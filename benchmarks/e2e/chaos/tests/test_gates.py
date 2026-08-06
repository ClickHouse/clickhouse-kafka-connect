#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Unit tests for chaos/gates.py (IC-4 recovery / quiescence / drain gates).

The gates are the load-bearing loop invariants of the monkey run (spec §3.6a/b):
they decide when the connector has RECOVERED between faults and when the whole
run has QUIESCED at the fence. Per IC-4 EVERY probe (Connect REST status, lag,
pod readiness, replication queue, DLQ depth) is injected as a `--probe-*-cmd`
string, and the decision logic is a set of PURE functions over probe-sample
sequences. So the bulk of these tests exercise the pure decision functions over
synthetic sample streams (no clock, no services); a thin band of CLI tests wire
stub probe commands end-to-end to pin the exit codes.

Fail-then-pass for every branch (plan T10):
  recovery happy path; momentary-lag-dip-not-progress rejected; transient
  FAILED->RUNNING tolerated + counted; pinned FAILED => 21 with trace; no-FAILED
  timeout => 22; quiescence happy path; dlq>0 blocks quiescence; sustained-W
  reset on a lag blip; T_settle => 23; drain-progress 50% trigger.

Run: python3 -m pytest benchmarks/e2e/chaos/tests/test_gates.py -q
"""
import json
import os
import stat
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gates  # noqa: E402

SCRIPT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "gates.py"
)


# --- sample builders -------------------------------------------------------- #


def _tasks(*states):
    """Build a Connect /status tasks list. A tuple (state, trace) attaches a
    trace; a bare string is a stateful task with no trace."""
    out = []
    for s in states:
        if isinstance(s, tuple):
            out.append({"state": s[0], "trace": s[1]})
        else:
            out.append({"state": s})
    return out


def smp(t, states=("RUNNING",), lag=None, pod=None, repl=None, dlq=None):
    return {
        "t": t,
        "tasks": _tasks(*states),
        "lag": lag,
        "pod_ready": pod,
        "repl_queue": repl,
        "dlq": dlq,
    }


# --- pure: task-state helpers (REST getFirstTaskFailureOpt equivalent) ------ #


def test_parse_status_accepts_wrapped_and_bare():
    wrapped = gates.parse_status('{"tasks":[{"state":"RUNNING"}]}')
    bare = gates.parse_status('[{"state":"RUNNING"}]')
    assert [t["state"] for t in wrapped] == ["RUNNING"]
    assert [t["state"] for t in bare] == ["RUNNING"]


def test_all_running_true_only_when_every_task_running():
    assert gates.all_running(_tasks("RUNNING", "RUNNING")) is True
    assert gates.all_running(_tasks("RUNNING", "FAILED")) is False
    assert gates.all_running(_tasks("RUNNING", "UNASSIGNED")) is False


def test_all_running_empty_is_false():
    # Empty tasks means "cannot confirm N tasks running" -> not recovered.
    assert gates.all_running([]) is False


def test_first_failure_trace_returns_first_failed_trace():
    trace = gates.first_failure_trace(
        _tasks("RUNNING", ("FAILED", "state machine boom"))
    )
    assert trace == "state machine boom"


def test_first_failure_trace_none_when_all_running():
    assert gates.first_failure_trace(_tasks("RUNNING", "RUNNING")) is None


def test_first_failure_trace_placeholder_when_no_trace_key():
    trace = gates.first_failure_trace(_tasks("FAILED"))
    assert trace == "No trace available"


# --- pure: recovery decision ------------------------------------------------ #


def test_recovery_happy_path_recovered():
    samples = [
        smp(0.0, lag=30, pod=True),
        smp(0.5, lag=20, pod=True),
        smp(1.0, lag=10, pod=True),
    ]
    ev = gates.evaluate_recovery(samples, q_seconds=1.0, require_pod=True, require_repl=False)
    assert ev.recovered is True
    assert ev.streak_start_t == 0.0
    assert ev.task_restart_count == 0


def test_recovery_lag_already_zero_counts_as_progress():
    samples = [smp(0.0, lag=0, pod=True), smp(0.6, lag=0, pod=True), smp(1.2, lag=0, pod=True)]
    ev = gates.evaluate_recovery(samples, q_seconds=1.0, require_pod=True, require_repl=False)
    assert ev.recovered is True


def test_recovery_momentary_lag_dip_not_progress_rejected():
    # 100 -> 90 (progress) -> 100 (regress: resets the streak) -> 90 again.
    samples = [
        smp(0.0, lag=100, pod=True),
        smp(0.5, lag=90, pod=True),
        smp(1.0, lag=100, pod=True),
        smp(1.5, lag=90, pod=True),
    ]
    ev = gates.evaluate_recovery(samples, q_seconds=1.0, require_pod=True, require_repl=False)
    assert ev.recovered is False


def test_recovery_transient_failed_tolerated_and_counted():
    # A FAILED sample resets the quiet window but the connector recovers and
    # then sustains Q; the restart is tolerated (still recovers) AND counted.
    samples = [
        smp(0.0, states=(("FAILED", "retry"),), lag=60),
        smp(0.5, lag=50),
        smp(1.0, lag=40),
        smp(1.5, lag=30),
    ]
    ev = gates.evaluate_recovery(samples, q_seconds=1.0, require_pod=False, require_repl=False)
    assert ev.recovered is True
    assert ev.task_restart_count == 1


def test_recovery_ch_fault_requires_replica_caught_up():
    # repl_queue must be empty (0) for CH faults; a non-empty queue blocks.
    lagging = [smp(t, lag=0, pod=True, repl=3) for t in (0.0, 0.5, 1.0)]
    ev = gates.evaluate_recovery(lagging, q_seconds=1.0, require_pod=True, require_repl=True)
    assert ev.recovered is False
    caught = [smp(t, lag=0, pod=True, repl=0) for t in (0.0, 0.5, 1.0)]
    ev2 = gates.evaluate_recovery(caught, q_seconds=1.0, require_pod=True, require_repl=True)
    assert ev2.recovered is True


def test_count_task_restarts_multiple_episodes():
    samples = [
        smp(0.0, states=(("FAILED", "x"),)),
        smp(0.5),  # RUNNING
        smp(1.0, states=(("FAILED", "y"),)),
        smp(1.5),  # RUNNING
    ]
    assert gates.count_task_restarts(samples) == 2


# --- pure: timeout classification (stuck vs infra stall) -------------------- #


def test_classify_timeout_stuck_when_task_pinned_failed():
    samples = [smp(0.0), smp(0.5, states=(("FAILED", "pinned trace"),))]
    kind, trace = gates.classify_timeout(samples)
    assert kind == "stuck"
    assert trace == "pinned trace"


def test_classify_timeout_infra_stall_when_no_task_failed():
    samples = [smp(0.0, lag=100, pod=False), smp(0.5, lag=100, pod=False)]
    kind, trace = gates.classify_timeout(samples)
    assert kind == "infra_stall"
    assert trace is None


# --- pure: quiescence decision ---------------------------------------------- #


def test_quiescence_happy_path():
    samples = [smp(t, lag=0, repl=0, dlq=0) for t in (0.0, 0.5, 1.0)]
    ev = gates.evaluate_quiescence(samples, w_seconds=1.0, require_repl=True, require_dlq=True)
    assert ev.quiesced is True


def test_quiescence_blocked_by_nonzero_dlq():
    samples = [smp(t, lag=0, repl=0, dlq=7) for t in (0.0, 0.5, 1.0)]
    ev = gates.evaluate_quiescence(samples, w_seconds=1.0, require_repl=True, require_dlq=True)
    assert ev.quiesced is False


def test_quiescence_sustained_w_reset_on_lag_blip():
    # lag 0,0,5,0,0 : the blip at t=1.0 resets the W window; the trailing 0.5s
    # of quiet is shorter than W, so not quiesced.
    samples = [
        smp(0.0, lag=0, repl=0, dlq=0),
        smp(0.5, lag=0, repl=0, dlq=0),
        smp(1.0, lag=5, repl=0, dlq=0),
        smp(1.5, lag=0, repl=0, dlq=0),
        smp(2.0, lag=0, repl=0, dlq=0),
    ]
    ev = gates.evaluate_quiescence(samples, w_seconds=1.0, require_repl=True, require_dlq=True)
    assert ev.quiesced is False


def test_quiescence_blocked_by_failed_task():
    samples = [smp(t, states=(("FAILED", "x"),), lag=0, repl=0, dlq=0) for t in (0.0, 0.5, 1.0)]
    ev = gates.evaluate_quiescence(samples, w_seconds=1.0, require_repl=True, require_dlq=True)
    assert ev.quiesced is False


# --- pure: drain progress --------------------------------------------------- #


def test_drain_pct_and_reached():
    assert gates.drain_pct(100, 40) == pytest.approx(60.0)
    assert gates.drain_reached(100, 40, 50) is True
    assert gates.drain_reached(100, 60, 50) is False


def test_drain_zero_baseline_is_fully_drained():
    assert gates.drain_reached(0, 0, 50) is True


# --- CLI end-to-end (stub probes, tiny windows) ----------------------------- #

RUNNING_STATUS = "echo '{\"tasks\":[{\"state\":\"RUNNING\"}]}'"
FAILED_STATUS = "echo '{\"tasks\":[{\"state\":\"FAILED\",\"trace\":\"boom-stacktrace\"}]}'"


def run_cli(*args, env=None):
    full_env = dict(os.environ)
    if env:
        full_env.update(env)
    proc = subprocess.run(
        [sys.executable, SCRIPT, *args],
        capture_output=True,
        text=True,
        env=full_env,
    )
    return proc.returncode, proc.stdout, proc.stderr


def _seq_stub(tmp_path, name, values):
    """Create a bash stub that returns successive lines of `values` across calls
    (clamping to the last), so a single injected probe string yields a stream."""
    counter = tmp_path / f"{name}.count"
    counter.write_text("0")
    valuesf = tmp_path / f"{name}.values"
    valuesf.write_text("\n".join(values) + "\n")
    script = tmp_path / f"{name}.sh"
    script.write_text(
        "#!/usr/bin/env bash\n"
        'c=$(cat "$1"); vf="$2"\n'
        'n=$(wc -l < "$vf"); idx=$c\n'
        'if [ "$idx" -ge "$n" ]; then idx=$(( n - 1 )); fi\n'
        'sed -n "$(( idx + 1 ))p" "$vf"\n'
        'echo $(( c + 1 )) > "$1"\n'
    )
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    return f'bash {script} {counter} {valuesf}'


def test_cli_recovery_happy_exit_0():
    rc, out, err = run_cli(
        "recovery",
        "--probe-status-cmd", RUNNING_STATUS,
        "--probe-lag-cmd", "echo 0",
        "--probe-pod-ready-cmd", "echo 1",
        "--q-seconds", "0.05",
        "--poll-interval", "0.01",
        "--t-recover", "5",
    )
    assert rc == 0, err
    doc = json.loads(out)
    assert doc["gate"] == "recovery"
    assert doc["status"] == "recovered"
    assert "task_restart_count" in doc
    assert "recovery_seconds" in doc


def test_cli_recovery_pinned_failed_exit_21_with_trace():
    rc, out, err = run_cli(
        "recovery",
        "--probe-status-cmd", FAILED_STATUS,
        "--probe-lag-cmd", "echo 100",
        "--q-seconds", "0.05",
        "--poll-interval", "0.01",
        "--t-recover", "0.1",
    )
    assert rc == 21, (out, err)
    doc = json.loads(out)
    assert doc["classification"] == "stuck"
    assert "boom-stacktrace" in (doc.get("trace") or "")
    # trace also surfaced on stderr per IC-4
    assert "boom-stacktrace" in err


def test_cli_recovery_no_failed_infra_stall_exit_22():
    rc, out, err = run_cli(
        "recovery",
        "--probe-status-cmd", RUNNING_STATUS,
        "--probe-lag-cmd", "echo 100",
        "--probe-pod-ready-cmd", "echo 0",  # pod never Ready -> infra stall
        "--q-seconds", "0.05",
        "--poll-interval", "0.01",
        "--t-recover", "0.1",
    )
    assert rc == 22, (out, err)
    doc = json.loads(out)
    assert doc["classification"] == "infra_stall"


def test_cli_quiescence_happy_exit_0():
    rc, out, err = run_cli(
        "quiescence",
        "--probe-status-cmd", RUNNING_STATUS,
        "--probe-lag-cmd", "echo 0",
        "--probe-repl-queue-cmd", "echo 0",
        "--probe-dlq-cmd", "echo 0",
        "--w-seconds", "0.05",
        "--poll-interval", "0.01",
        "--t-settle", "5",
    )
    assert rc == 0, err
    doc = json.loads(out)
    assert doc["gate"] == "quiescence"
    assert doc["status"] == "quiesced"


def test_cli_quiescence_dlq_nonzero_times_out_exit_23():
    rc, out, err = run_cli(
        "quiescence",
        "--probe-status-cmd", RUNNING_STATUS,
        "--probe-lag-cmd", "echo 0",
        "--probe-repl-queue-cmd", "echo 0",
        "--probe-dlq-cmd", "echo 5",  # DLQ never drains
        "--w-seconds", "0.05",
        "--poll-interval", "0.01",
        "--t-settle", "0.1",
    )
    assert rc == 23, (out, err)
    doc = json.loads(out)
    assert doc["status"] == "t_settle_timeout"


def test_cli_drain_progress_50pct_trigger(tmp_path):
    lag_stub = _seq_stub(tmp_path, "lag", ["100", "40"])
    rc, out, err = run_cli(
        "drain-progress",
        "--target-pct", "50",
        "--probe-lag-cmd", lag_stub,
        "--poll-interval", "0.01",
        "--timeout", "5",
    )
    assert rc == 0, err
    doc = json.loads(out)
    assert doc["gate"] == "drain-progress"
    assert doc["status"] == "reached"
    assert doc["drained_pct"] >= 50.0


def test_cli_unknown_subcommand_is_usage_error():
    rc, _, err = run_cli("nonsense")
    assert rc == 2
    assert err.strip()
