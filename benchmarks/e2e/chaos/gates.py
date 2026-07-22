#!/usr/bin/env python3
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
"""Recovery / quiescence / drain gates for the chaos monkey loop (IC-4, §3.6a/b).

These are the load-bearing loop invariants: without them the monkey loop
silently conflates faults and runs its oracle on an unsettled system. Three
subcommands, each bounded so a monkey run can NEVER hang:

  recovery       (§3.6a) blocks until ALL signals hold sustained for Q_SECONDS:
                 every task RUNNING (Connect REST /status; the failing task's
                 `trace` captured on FAILED -- the REST equivalent of
                 ConfluentPlatform.getFirstTaskFailureOpt), the killed pod back
                 Ready, consumer lag STRICTLY decreasing again (forward progress
                 AFTER the fault, not a momentary dip -- or already 0), no new
                 FAILED during Q, and (CH faults only) the rejoined replica
                 caught up (replication queue empty). Bounded by T_RECOVER.
                 Transient FAILED->RUNNING cycles inside Q (the legitimate
                 W1-W3 CONTAINS/ERROR retry shapes) are TOLERATED and counted
                 into task_restart_count -- not treated as gate failures.
                 Exit 0 recovered; 21 t_recover_timeout STUCK (a task pinned
                 FAILED, trace to stderr + JSON); 22 t_recover_timeout INFRA
                 STALL (nothing FAILED, CH/pod slow).

  quiescence     (§3.6b) after the production fence, asserts lag=0 sustained
                 W_SECONDS + tasks RUNNING + replication queues empty + DLQ
                 depth == 0 (a non-zero DLQ is silent loss, §3.6b). Bounded by
                 T_SETTLE. Exit 0 quiesced; 23 t_settle_timeout.

  drain-progress the smoke gate's "50% drained" trigger (§3.5): exit 0 once the
                 backlog has drained past --target-pct of its first-observed
                 baseline. Bounded by --timeout (exit 24 on timeout).

ALL probes (Connect REST status, lag, pod readiness, replication queue, DLQ
depth) are INJECTED as `--probe-*-cmd` argument strings, run each poll via the
shell; their stdout is parsed into a sample. The DECISION LOGIC is a set of PURE
functions over the accumulated sample sequence (evaluate_recovery /
evaluate_quiescence / drain_reached) so it is testable without any live service.

stdout = one JSON result object (machine output, consumed by T11); the recovery
gate's object carries the IC-4 round fields `recovery_seconds` and
`task_restart_count`. stderr = logging.

CLI (common): --poll-interval S (env GATE_POLL_INTERVAL, default 5)
  recovery       --probe-status-cmd --probe-lag-cmd [--probe-pod-ready-cmd]
                 [--probe-repl-queue-cmd] [--ch-fault]
                 --q-seconds (env Q_SECONDS, default 30)
                 --t-recover (env T_RECOVER, default 600)
  quiescence     --probe-status-cmd --probe-lag-cmd [--probe-repl-queue-cmd]
                 --probe-dlq-cmd
                 --w-seconds (env W_SECONDS, default 60)
                 --t-settle  (env T_SETTLE, default 900)
  drain-progress --target-pct P --probe-lag-cmd --timeout (env T_RECOVER)
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass

# Exit codes (IC-4). 0 is success for every subcommand; the failure codes are
# distinct so the orchestrator (T11) maps each to an IC-3 run_conclusion.
EXIT_OK = 0
EXIT_USAGE = 2
EXIT_RECOVER_STUCK = 21
EXIT_RECOVER_INFRA = 22
EXIT_SETTLE_TIMEOUT = 23
EXIT_DRAIN_TIMEOUT = 24

_RUNNING = "RUNNING"
_FAILED = "FAILED"
_NO_TRACE = "No trace available"


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


# --- probe parsing (pure) --------------------------------------------------- #


def parse_status(text: str) -> list:
    """Parse a Connect /status payload into its tasks list. Accepts either the
    full object ({"tasks": [...]}) or a bare tasks array. Unparseable/empty text
    yields an empty list (treated as "cannot confirm running")."""
    text = (text or "").strip()
    if not text:
        return []
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        return []
    if isinstance(obj, dict):
        tasks = obj.get("tasks")
        return tasks if isinstance(tasks, list) else []
    if isinstance(obj, list):
        return obj
    return []


def parse_int(text: str):
    """Parse a single integer from probe stdout; None if absent/unparseable."""
    text = (text or "").strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        try:
            return int(float(text))
        except ValueError:
            return None


def parse_bool(text: str) -> bool:
    """Parse a readiness probe: 1/true/ready => True, everything else False."""
    return (text or "").strip().lower() in ("1", "true", "ready", "yes")


# --- task-state helpers (REST getFirstTaskFailureOpt equivalent) ------------ #


def _state(task: dict) -> str:
    return str(task.get("state", "")).upper()


def all_running(tasks: list) -> bool:
    """True iff there is at least one task and EVERY task is RUNNING. An empty
    list is False: we cannot confirm the N tasks are up (mirrors treating a
    missing status as not-yet-recovered)."""
    if not tasks:
        return False
    return all(_state(t) == _RUNNING for t in tasks)


def any_failed(tasks: list) -> bool:
    return any(_state(t) == _FAILED for t in tasks)


def first_failure_trace(tasks: list):
    """The FIRST FAILED task's trace, or None if no task is FAILED -- the REST
    equivalent of ConfluentPlatform.getFirstTaskFailureOpt. A FAILED task with
    no `trace` key yields the same placeholder the Java helper uses."""
    for t in tasks:
        if _state(t) == _FAILED:
            trace = t.get("trace")
            return str(trace) if trace is not None else _NO_TRACE
    return None


# --- recovery decision (pure) ----------------------------------------------- #


@dataclass
class RecoveryEval:
    recovered: bool
    streak_seconds: float
    streak_start_t: float
    task_restart_count: int


def lag_progressing(prev_lag, cur_lag) -> bool:
    """Forward progress between two consecutive lag samples: strictly decreasing,
    or already drained to 0. A flat or rising lag is NOT progress (rejects a
    momentary dip that bounces back)."""
    if cur_lag == 0:
        return True
    if prev_lag is None or cur_lag is None:
        return False
    return cur_lag < prev_lag


def recovery_instant_ok(sample: dict, require_pod: bool, require_repl: bool) -> bool:
    """The per-sample (instantaneous) recovery conditions, minus the pairwise
    lag-progress check (which needs the previous sample)."""
    if not all_running(sample.get("tasks") or []):
        return False
    if require_pod and sample.get("pod_ready") is not True:
        return False
    if require_repl and sample.get("repl_queue") != 0:
        return False
    return True


def count_task_restarts(samples: list) -> int:
    """Number of FAILED->(all RUNNING) recovery episodes across the stream. A
    transient FAILED that returns to RUNNING is tolerated by the gate but
    recorded here (IC-4 task_restart_count, §3.6a W1-W3 retry shapes)."""
    count = 0
    in_failure = False
    for s in samples:
        tasks = s.get("tasks") or []
        if any_failed(tasks):
            in_failure = True
        elif in_failure and all_running(tasks):
            count += 1
            in_failure = False
    return count


def evaluate_recovery(
    samples: list, q_seconds: float, require_pod: bool, require_repl: bool
) -> RecoveryEval:
    """Pure recovery verdict over the full sample sequence. Finds the trailing
    quiet window: every sample meets the instantaneous conditions AND lag makes
    forward progress at each step. Any break (a FAILED task, pod not Ready,
    replica behind, or lag stalling/rising) resets the window; recovery is
    reached when the ongoing window spans >= Q_SECONDS."""
    restart_count = count_task_restarts(samples)
    start_idx = None
    for i, s in enumerate(samples):
        if not recovery_instant_ok(s, require_pod, require_repl):
            start_idx = None
            continue
        if start_idx is None:
            start_idx = i
            continue
        # Continuing an open window: the previous sample was also instant-ok
        # (else start_idx would be None), so judge pairwise lag progress.
        if not lag_progressing(samples[i - 1].get("lag"), s.get("lag")):
            # Lag stalled/rose -> restart the quiet window at the current sample
            # (still a valid fresh baseline since it is instant-ok).
            start_idx = i
    if start_idx is None:
        return RecoveryEval(False, 0.0, samples[-1]["t"] if samples else 0.0, restart_count)
    streak_seconds = samples[-1]["t"] - samples[start_idx]["t"]
    return RecoveryEval(
        recovered=streak_seconds >= q_seconds,
        streak_seconds=streak_seconds,
        streak_start_t=samples[start_idx]["t"],
        task_restart_count=restart_count,
    )


def classify_timeout(samples: list):
    """Classify a T_RECOVER timeout: STUCK (a task pinned FAILED, with its
    trace -> exit 21, a real connector bug) vs INFRA STALL (nothing FAILED, the
    infra is just slow to reschedule/sync -> exit 22, integrity_unverified)."""
    if not samples:
        return ("infra_stall", None)
    trace = first_failure_trace(samples[-1].get("tasks") or [])
    if trace is not None:
        return ("stuck", trace)
    return ("infra_stall", None)


# --- quiescence decision (pure) --------------------------------------------- #


@dataclass
class QuiesceEval:
    quiesced: bool
    streak_seconds: float
    streak_start_t: float


def quiescence_instant_ok(sample: dict, require_repl: bool, require_dlq: bool) -> bool:
    """The per-sample quiescence conditions: every task RUNNING, lag == 0, and
    (when required) replication queues empty and DLQ depth == 0."""
    if not all_running(sample.get("tasks") or []):
        return False
    if sample.get("lag") != 0:
        return False
    if require_repl and sample.get("repl_queue") != 0:
        return False
    if require_dlq and sample.get("dlq") != 0:
        return False
    return True


def evaluate_quiescence(
    samples: list, w_seconds: float, require_repl: bool, require_dlq: bool
) -> QuiesceEval:
    """Pure quiescence verdict: the trailing window in which every sample meets
    the instantaneous conditions must span >= W_SECONDS. Any blip (lag != 0,
    task not RUNNING, non-empty queue, DLQ depth > 0) resets the window."""
    start_idx = None
    for i, s in enumerate(samples):
        if quiescence_instant_ok(s, require_repl, require_dlq):
            if start_idx is None:
                start_idx = i
        else:
            start_idx = None
    if start_idx is None:
        return QuiesceEval(False, 0.0, samples[-1]["t"] if samples else 0.0)
    streak_seconds = samples[-1]["t"] - samples[start_idx]["t"]
    return QuiesceEval(
        quiesced=streak_seconds >= w_seconds,
        streak_seconds=streak_seconds,
        streak_start_t=samples[start_idx]["t"],
    )


# --- drain-progress decision (pure) ----------------------------------------- #


def drain_pct(baseline, current) -> float:
    """Percentage of the baseline backlog that has drained. A zero (or unknown)
    baseline is fully drained by definition."""
    if not baseline or baseline <= 0:
        return 100.0
    if current is None:
        return 0.0
    return (baseline - current) / baseline * 100.0


def drain_reached(baseline, current, target_pct: float) -> bool:
    return drain_pct(baseline, current) >= target_pct


# --- probe runner (impure edge) --------------------------------------------- #


def run_probe(cmd: str) -> str:
    """Run an injected probe command via the shell and return its stdout. A
    non-zero exit or exception yields "" (a transient probe failure -- e.g. a
    lag read during a broker kill -- is not fatal; it just fails to confirm a
    condition this sample)."""
    if not cmd:
        return ""
    try:
        proc = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=30
        )
    except (subprocess.SubprocessError, OSError) as exc:
        log(f"probe error ({cmd!r}): {exc}")
        return ""
    if proc.returncode != 0:
        log(f"probe nonzero ({cmd!r}): rc={proc.returncode} {proc.stderr.strip()}")
        return ""
    return proc.stdout


def _sample_recovery(args, t: float) -> dict:
    return {
        "t": t,
        "tasks": parse_status(run_probe(args.probe_status_cmd)),
        "lag": parse_int(run_probe(args.probe_lag_cmd)),
        "pod_ready": parse_bool(run_probe(args.probe_pod_ready_cmd))
        if args.probe_pod_ready_cmd
        else None,
        "repl_queue": parse_int(run_probe(args.probe_repl_queue_cmd))
        if args.probe_repl_queue_cmd
        else None,
    }


def _sample_quiescence(args, t: float) -> dict:
    return {
        "t": t,
        "tasks": parse_status(run_probe(args.probe_status_cmd)),
        "lag": parse_int(run_probe(args.probe_lag_cmd)),
        "repl_queue": parse_int(run_probe(args.probe_repl_queue_cmd))
        if args.probe_repl_queue_cmd
        else None,
        "dlq": parse_int(run_probe(args.probe_dlq_cmd)),
    }


def _emit(obj: dict) -> None:
    print(json.dumps(obj))


# --- subcommand runners ----------------------------------------------------- #


def run_recovery(args) -> int:
    require_pod = bool(args.probe_pod_ready_cmd)
    require_repl = bool(args.ch_fault)
    if require_repl and not args.probe_repl_queue_cmd:
        log("recovery: --ch-fault requires --probe-repl-queue-cmd")
        return EXIT_USAGE
    start = time.monotonic()
    samples: list = []
    while True:
        now = time.monotonic() - start
        samples.append(_sample_recovery(args, now))
        ev = evaluate_recovery(samples, args.q_seconds, require_pod, require_repl)
        if ev.recovered:
            _emit({
                "gate": "recovery",
                "status": "recovered",
                "recovery_seconds": round(ev.streak_start_t, 3),
                "task_restart_count": ev.task_restart_count,
            })
            log(f"recovery: recovered after {ev.streak_start_t:.2f}s "
                f"(restarts={ev.task_restart_count})")
            return EXIT_OK
        if now >= args.t_recover:
            kind, trace = classify_timeout(samples)
            result = {
                "gate": "recovery",
                "status": "t_recover_timeout",
                "classification": kind,
                "task_restart_count": ev.task_restart_count,
            }
            if kind == "stuck":
                result["trace"] = trace
                log(f"recovery: STUCK -- task pinned FAILED. trace:\n{trace}")
                _emit(result)
                return EXIT_RECOVER_STUCK
            log("recovery: INFRA STALL -- no task FAILED within T_recover")
            _emit(result)
            return EXIT_RECOVER_INFRA
        time.sleep(args.poll_interval)


def run_quiescence(args) -> int:
    require_repl = bool(args.probe_repl_queue_cmd)
    start = time.monotonic()
    samples: list = []
    while True:
        now = time.monotonic() - start
        samples.append(_sample_quiescence(args, now))
        ev = evaluate_quiescence(samples, args.w_seconds, require_repl, require_dlq=True)
        if ev.quiesced:
            _emit({
                "gate": "quiescence",
                "status": "quiesced",
                "quiesce_seconds": round(ev.streak_start_t, 3),
            })
            log(f"quiescence: quiesced after {ev.streak_start_t:.2f}s")
            return EXIT_OK
        if now >= args.t_settle:
            _emit({"gate": "quiescence", "status": "t_settle_timeout"})
            log("quiescence: T_settle timeout -- system did not settle")
            return EXIT_SETTLE_TIMEOUT
        time.sleep(args.poll_interval)


def run_drain_progress(args) -> int:
    start = time.monotonic()
    baseline = None
    while True:
        now = time.monotonic() - start
        current = parse_int(run_probe(args.probe_lag_cmd))
        if baseline is None:
            baseline = current
        if drain_reached(baseline, current, args.target_pct):
            pct = drain_pct(baseline, current)
            _emit({
                "gate": "drain-progress",
                "status": "reached",
                "target_pct": args.target_pct,
                "drained_pct": round(pct, 3),
            })
            log(f"drain-progress: reached {pct:.1f}% (target {args.target_pct}%)")
            return EXIT_OK
        if now >= args.timeout:
            _emit({
                "gate": "drain-progress",
                "status": "drain_timeout",
                "target_pct": args.target_pct,
                "drained_pct": round(drain_pct(baseline, current), 3),
            })
            log("drain-progress: timeout before reaching target")
            return EXIT_DRAIN_TIMEOUT
        time.sleep(args.poll_interval)


# --- CLI -------------------------------------------------------------------- #


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name) or default)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gates.py",
        description="Recovery / quiescence / drain gates for the chaos loop (IC-4).",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    def add_common(p):
        p.add_argument("--poll-interval", type=float,
                       default=_env_float("GATE_POLL_INTERVAL", 5.0),
                       help="seconds between probe samples")

    rec = sub.add_parser("recovery", help="§3.6a recovery gate")
    add_common(rec)
    rec.add_argument("--probe-status-cmd", required=True,
                     help="Connect REST /status probe (emits the status JSON)")
    rec.add_argument("--probe-lag-cmd", required=True,
                     help="consumer-lag probe (emits total lag as an integer)")
    rec.add_argument("--probe-pod-ready-cmd", default="",
                     help="killed-pod readiness probe (emits 1/0)")
    rec.add_argument("--probe-repl-queue-cmd", default="",
                     help="replication-queue probe (emits queue size; required with --ch-fault)")
    rec.add_argument("--ch-fault", action="store_true",
                     help="CH-side fault: also require the replica caught up")
    rec.add_argument("--q-seconds", type=float, default=_env_float("Q_SECONDS", 30.0),
                     help="quiet window all signals must hold (s)")
    rec.add_argument("--t-recover", type=float, default=_env_float("T_RECOVER", 600.0),
                     help="recovery gate bound (s)")

    qui = sub.add_parser("quiescence", help="§3.6b fence quiescence gate")
    add_common(qui)
    qui.add_argument("--probe-status-cmd", required=True)
    qui.add_argument("--probe-lag-cmd", required=True)
    qui.add_argument("--probe-repl-queue-cmd", default="")
    qui.add_argument("--probe-dlq-cmd", required=True,
                     help="DLQ-depth probe (emits depth; MUST be 0 to quiesce)")
    qui.add_argument("--w-seconds", type=float, default=_env_float("W_SECONDS", 60.0),
                     help="settle window all signals must hold (s)")
    qui.add_argument("--t-settle", type=float, default=_env_float("T_SETTLE", 900.0),
                     help="quiescence gate bound (s)")

    dr = sub.add_parser("drain-progress", help="§3.5 smoke-gate drain trigger")
    add_common(dr)
    dr.add_argument("--target-pct", type=float, required=True,
                    help="drained-percent trigger (of first-observed backlog)")
    dr.add_argument("--probe-lag-cmd", required=True)
    dr.add_argument("--timeout", type=float, default=_env_float("T_RECOVER", 600.0),
                    help="drain gate bound (s)")
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.cmd == "recovery":
        return run_recovery(args)
    if args.cmd == "quiescence":
        return run_quiescence(args)
    if args.cmd == "drain-progress":
        return run_drain_progress(args)
    parser.error(f"unknown subcommand: {args.cmd}")
    return EXIT_USAGE


if __name__ == "__main__":
    sys.exit(main())
