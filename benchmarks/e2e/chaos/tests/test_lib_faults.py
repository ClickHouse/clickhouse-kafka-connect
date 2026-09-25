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
"""Offline unit tests for chaos/lib_faults.sh (T9, spec §3.1/§3.4/§3.7).

No cluster, no creds, no network: a PATH-stubbed `kubectl` records every
invocation and returns canned output driven by STUB_* env vars. The
`resolve_keeper_leader` contract owned by T8 (lib_ch_cluster.sh) is STUBbed as a
bash function — this suite never sources or edits T8's file.

Covers:
  * lib_faults.sh sources cleanly under `set -u`, is `bash -n` clean, and
    defines one inject_<fault> per primitive (C1..C5 + quorum-loss),
  * each injector emits a well-formed fault-took-effect evidence JSON fragment
    (IC-4 `evidence`) as its ONLY stdout,
  * C4 REFUSES while any CH pod is NotReady (the <=1-CH cap at the injector),
  * the quorum-loss injector requires the explicit gate,
  * the nested self-cleaning trap runs on return,
  * C5 resolves the keeper leader and kills exactly that pod (C4 path).

Run: python3 -m pytest benchmarks/e2e/chaos/tests/test_lib_faults.py -q
"""
import json
import os
import subprocess

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
CHAOS = os.path.dirname(HERE)
ORCH = os.path.abspath(os.path.join(CHAOS, "..", "orchestration"))
LIB_FAULTS = os.path.join(CHAOS, "lib_faults.sh")
LIB_BENCH = os.path.join(ORCH, "lib_bench.sh")

# A PATH-stub kubectl that logs every call and dispatches canned output on the
# substring of its argument vector. STUB_* env vars steer each branch.
KUBECTL_STUB = r"""#!/usr/bin/env bash
echo "kubectl $*" >> "$KCALLS"
args="$*"
case "$args" in
  *"delete pod"*) exit 0 ;;
esac
case "$args" in
  *".metadata.uid"*)
    n=0; [ -f "$KUIDN" ] && n="$(cat "$KUIDN")"; n=$((n+1)); echo "$n" > "$KUIDN"
    if [ "$n" -le 1 ]; then echo "${STUB_UID_1:-uid-old}"; else echo "${STUB_UID_2:-uid-new}"; fi
    exit 0 ;;
esac
case "$args" in
  *"conditions"*) echo "${STUB_CH_READY:-ch-chaos-0=True;ch-chaos-1=True;ch-chaos-2=True;}"; exit 0 ;;
esac
case "$args" in
  *".items[0].metadata.name"*) echo "${STUB_CONNECT_POD:-bench-connect-connect-0}"; exit 0 ;;
esac
case "$args" in
  *"restart"*) echo "${STUB_HTTP_CODE:-200}"; exit 0 ;;
  *"/status"*) echo "${STUB_TASK_STATE:-RESTARTING}"; exit 0 ;;
esac
case "$args" in
  *"clickhouse-client"*) echo "${STUB_CH_QUERY:-2}"; exit 0 ;;
esac
case "$args" in
  *"kafka-get-offsets"*)
    if [ "${STUB_LAG_READ:-ok}" = "fail" ]; then exit 1; fi
    echo "hits-chaos:0:100"; exit 0 ;;
esac
exit 0
"""


@pytest.fixture
def stub_env(tmp_path):
    """Return (env, calls_path) with a PATH-stubbed kubectl installed."""
    binp = tmp_path / "bin"
    binp.mkdir()
    stub = binp / "kubectl"
    stub.write_text(KUBECTL_STUB)
    stub.chmod(0o755)
    calls = tmp_path / "kubectl.calls"
    env = dict(os.environ)
    env["PATH"] = f"{binp}{os.pathsep}{env['PATH']}"
    env["KCALLS"] = str(calls)
    env["KUIDN"] = str(tmp_path / "uid.n")
    # Keep the injectors fast: no real waits.
    env["FAULT_RECREATE_TIMEOUT"] = "5"
    env["FAULT_POLL_INTERVAL"] = "0"
    env["QUORUM_LOSS_HOLD"] = "0"
    env["CH_RECREATE_TIMEOUT"] = "5"
    return env, calls


def run_bash(snippet, env, define_leader=None):
    """Source the libs (+ optional resolve_keeper_leader stub) and run snippet."""
    leader = ""
    if define_leader is not None:
        leader = f'resolve_keeper_leader() {{ echo "{define_leader}"; }}\n'
    prog = (
        "set -uo pipefail\n"
        f'source "{LIB_BENCH}" >/dev/null 2>&1\n'
        f'source "{LIB_FAULTS}"\n'
        f"{leader}"
        f"{snippet}\n"
    )
    return subprocess.run(
        ["bash", "-c", prog], capture_output=True, text=True, env=env
    )


# --------------------------------------------------------------------------- #
# structure
# --------------------------------------------------------------------------- #
def test_lib_faults_exists():
    assert os.path.isfile(LIB_FAULTS)


def test_syntax_ok():
    r = subprocess.run(["bash", "-n", LIB_FAULTS], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_sources_cleanly_under_set_u():
    r = subprocess.run(
        ["bash", "-c", f'set -u; source "{LIB_BENCH}" >/dev/null 2>&1; '
                       f'source "{LIB_FAULTS}"; echo OK'],
        capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip().endswith("OK")


INJECTORS = [
    "inject_connect_pod_kill",
    "inject_task_restart",
    "inject_broker_pod_kill",
    "inject_ch_node_kill",
    "inject_ch_keeper_leader_kill",
    "inject_quorum_loss",
]


@pytest.mark.parametrize("fn", INJECTORS)
def test_defines_injector(fn, stub_env):
    env, _ = stub_env
    r = run_bash(f'declare -F {fn} >/dev/null && echo yes || echo no', env)
    assert r.stdout.strip().endswith("yes"), f"{fn} must be defined ({r.stderr})"


def test_sourcing_executes_no_injection(stub_env):
    """Sourcing lib_faults.sh must NOT run kubectl (no top-level side effect)."""
    env, calls = stub_env
    r = run_bash("echo SOURCED", env)
    assert r.returncode == 0, r.stderr
    assert "SOURCED" in r.stdout
    assert not calls.exists(), \
        f"sourcing executed kubectl:\n{calls.read_text() if calls.exists() else ''}"


# --------------------------------------------------------------------------- #
# evidence JSON shape (IC-4) — the injector's ONLY stdout
# --------------------------------------------------------------------------- #
def _evidence(r):
    """Parse the last stdout line as the evidence JSON fragment."""
    lines = [ln for ln in r.stdout.splitlines() if ln.strip()]
    assert lines, f"no stdout evidence (stderr: {r.stderr})"
    doc = json.loads(lines[-1])
    assert "kind" in doc and "detail" in doc, doc
    return doc


def test_c1_connect_pod_kill_evidence(stub_env):
    env, calls = stub_env
    r = run_bash("inject_connect_pod_kill", env)
    assert r.returncode == 0, r.stderr
    ev = _evidence(r)
    assert ev["kind"] == "pod_recreated"
    # a hard kill: --grace-period=0 --force
    log = calls.read_text()
    assert "delete pod" in log and "--grace-period=0" in log and "--force" in log
    # the new pod UID is observed and recorded
    assert "uid-old" in ev["detail"] and "uid-new" in ev["detail"]


def test_c2_task_restart_evidence(stub_env):
    env, calls = stub_env
    r = run_bash("inject_task_restart 1", env)
    assert r.returncode == 0, r.stderr
    ev = _evidence(r)
    assert ev["kind"] == "task_restart_accepted"
    log = calls.read_text()
    assert "tasks/1/restart" in log
    assert "200" in ev["detail"]  # 2xx acceptance recorded


def test_c2_task_restart_non_2xx_fails(stub_env):
    env, _ = stub_env
    env["STUB_HTTP_CODE"] = "500"
    r = run_bash("inject_task_restart 0", env)
    assert r.returncode != 0


def test_c3_broker_pod_kill_evidence(stub_env):
    env, calls = stub_env
    env["STUB_LAG_READ"] = "fail"  # transient lag-read failure during the fault
    r = run_bash("inject_broker_pod_kill", env)
    assert r.returncode == 0, r.stderr
    ev = _evidence(r)
    assert ev["kind"] == "broker_recreated"
    assert "delete pod" in calls.read_text()
    assert "uid-new" in ev["detail"]
    assert "failed" in ev["detail"]  # lag_read=failed recorded


def test_c4_ch_node_kill_evidence(stub_env):
    env, calls = stub_env
    env["CH_KILL_TARGET"] = "ch-chaos-2"
    r = run_bash("inject_ch_node_kill", env)
    assert r.returncode == 0, r.stderr
    ev = _evidence(r)
    assert ev["kind"] == "pod_recreated"
    log = calls.read_text()
    assert "delete pod ch-chaos-2" in log
    assert "insert_errors=2" in ev["detail"]  # from system.query_log stub


def test_c4_refuses_while_ch_pod_not_ready(stub_env):
    """The <=1-CH cap lives at the injector: refuse while any CH pod NotReady."""
    env, calls = stub_env
    env["STUB_CH_READY"] = "ch-chaos-0=True;ch-chaos-1=True;ch-chaos-2=False;"
    env["CH_KILL_TARGET"] = "ch-chaos-0"
    r = run_bash("inject_ch_node_kill; echo rc=$?", env)
    assert "rc=0" not in r.stdout, "must refuse (non-zero) while a CH pod is NotReady"
    # and it must NOT have killed anything
    log = calls.read_text() if calls.exists() else ""
    assert "delete pod" not in log, "refusal must not delete a CH pod"
    assert "refus" in r.stderr.lower() or "notready" in r.stderr.lower().replace(" ", "")


def test_c5_keeper_leader_kill_targets_the_leader(stub_env):
    env, calls = stub_env
    r = run_bash("inject_ch_keeper_leader_kill", env, define_leader="ch-chaos-1")
    assert r.returncode == 0, r.stderr
    ev = _evidence(r)
    assert ev["kind"] == "pod_recreated"
    assert "delete pod ch-chaos-1" in calls.read_text()


def test_c5_fails_when_leader_unresolved(stub_env):
    env, _ = stub_env
    # resolve_keeper_leader present but echoes empty
    r = run_bash("inject_ch_keeper_leader_kill", env, define_leader="")
    assert r.returncode != 0


# --------------------------------------------------------------------------- #
# quorum-loss gate (§3.7)
# --------------------------------------------------------------------------- #
def test_quorum_loss_requires_explicit_gate(stub_env):
    env, calls = stub_env
    env.pop("QUORUM_LOSS", None)
    r = run_bash("inject_quorum_loss", env)
    assert r.returncode != 0, "quorum-loss must refuse without the explicit gate"
    log = calls.read_text() if calls.exists() else ""
    assert "delete pod" not in log, "gated injector must not kill pods"


def test_quorum_loss_runs_under_gate(stub_env):
    env, calls = stub_env
    env["QUORUM_LOSS"] = "1"
    env["QUORUM_KILL_PODS"] = "ch-chaos-0 ch-chaos-1"
    env["STUB_CH_QUERY"] = "5"  # inserts fail -> stall observed
    r = run_bash("inject_quorum_loss", env)
    assert r.returncode == 0, r.stderr
    ev = _evidence(r)
    assert ev["kind"] == "quorum_stall"
    log = calls.read_text()
    # both keeper pods killed at once (cap deliberately overridden)
    assert "ch-chaos-0" in log and "ch-chaos-1" in log


# --------------------------------------------------------------------------- #
# nested self-cleaning trap
# --------------------------------------------------------------------------- #
def test_nested_trap_cleans_up_on_return(stub_env, tmp_path):
    env, _ = stub_env
    cleanup_log = tmp_path / "cleanup.log"
    env["FAULT_CLEANUP_LOG"] = str(cleanup_log)
    r = run_bash("inject_connect_pod_kill", env)
    assert r.returncode == 0, r.stderr
    assert cleanup_log.exists(), "the nested trap must run its cleanup on return"
    assert "cleaned" in cleanup_log.read_text()


if __name__ == "__main__":
    import sys
    sys.exit(subprocess.call(["python3", "-m", "pytest", __file__, "-q"]))
