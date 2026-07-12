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
"""Finalizer — turn a stream of raw poller samples into per-run scalars.

Pure functions only (no I/O, no clock, no network) so the math is unit-testable
against synthetic sample streams with known answers. The sampler writes JSONL;
:func:`load_samples` reads it; :func:`finalize` computes the pinned-name scalar
set (see ``metric_names``) plus a guards summary.

Sample shape (one dict per poll, see sampler.Sample.to_dict):
  {
    "t": <float epoch seconds, wall clock of the sample>,
    "offsets": {                       # per-partition, from Kafka admin API
        "0": {"committed": <int|null>, "end": <int>},
        ...
    },
    "connect": {                       # from Connect REST /status; None if down
        "connector_state": "RUNNING",
        "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "..."}],
        "unavailable": false
    },
    "jmx": {                           # from prometheus endpoint; None if absent
        "put_batch_avg_time_ms": <float|null>,
        "records_consumed_rate": <float|null>,
        "fetch_latency_avg_ms": <float|null>,
        "jvm_heap_used_bytes": <float|null>,
        "jvm_heap_max_bytes": <float|null>,
        "gc_collection_seconds_sum": <float|null>,  # cumulative
        "unavailable": false
    },
    "pod": {                           # from kubelet metrics.k8s.io; None absent
        "cpu_seconds_total": <float|null>,   # cumulative container CPU seconds
        "memory_working_set_bytes": <float|null>,
        "unavailable": false
    }
  }

drain_seconds anchor (plan §7 / §6): the window is first-sample-after-
connector-start -> the sample at which total lag first reaches 0. The caller of
`sample` starts polling only after the connector is deployed, so sample[0] is
already "after connector start"; finalize uses the first sample's timestamp as
the start anchor and the lag-0 sample's timestamp as the end anchor.
"""
import math
import os
from typing import Any, Dict, List, Optional, Tuple

import metric_names as mn


# drain_rate_stability bucket width (seconds). The committed-offset counter only
# advances on the sink's periodic offset commit (a ~25k-record lump), so at the
# 10s poll cadence the per-interval delta is commit-cadence-dominated: a visibly
# steady drain shows CoV ≈ 1.2–1.4 (live pairs, 2026-07-09) purely because some
# 10s windows straddle a commit and others do not. Aggregating deltas into WIDER
# buckets (default 60s) averages the lumpy commits out, so the CoV reflects the
# genuine drain-rate variation rather than the commit rhythm. Parameterizable via
# DRAIN_RATE_STABILITY_BUCKET_SECONDS. See README "drain_rate_stability" note:
# values recorded before 2026-07-10 used 10s buckets and are NOT comparable.
# `or "60"` (not the 2-arg default): a SET-BUT-EMPTY env var makes
# os.environ.get(k, "60") return "" and float("") raises ValueError. `get(k)
# or "60"` collapses both unset and empty to the default (same class fixed in
# 083e836).
BUCKET_SECONDS_DEFAULT = float(
    os.environ.get("DRAIN_RATE_STABILITY_BUCKET_SECONDS") or "60")


# --------------------------------------------------------------------------- #
# small numeric helpers
# --------------------------------------------------------------------------- #
def _coefficient_of_variation(values: List[float]) -> Optional[float]:
    """CoV = stddev / mean (population stddev). None if undefined.

    Returns None for < 2 points (no variation defined) or a non-positive mean
    (rate CoV is only meaningful about a positive mean). 0.0 for a flat stream.
    """
    pts = [v for v in values if v is not None]
    if len(pts) < 2:
        return None
    mean = sum(pts) / len(pts)
    if mean <= 0:
        return None
    var = sum((v - mean) ** 2 for v in pts) / len(pts)
    return math.sqrt(var) / mean


def _time_weighted_average(
    pairs: List[Tuple[float, Optional[float]]]
) -> Optional[float]:
    """Time-weighted mean of a sampled moving-average MBean (plan §7).

    ``pairs`` is [(t, value), ...] in sample order. Each value is weighted by
    the interval to the NEXT sample (left-Riemann over the drain), so a value
    that held for longer counts more — the correct way to integrate a moving
    average that is scraped at intervals rather than once. Samples whose value
    is None (source unavailable that tick) are skipped; the previous valid
    value's weight then extends across the gap to the next valid sample. A
    single valid sample returns that sample's value.
    """
    valid = [(t, v) for (t, v) in pairs if v is not None]
    if not valid:
        return None
    if len(valid) == 1:
        return valid[0][1]
    num = 0.0
    den = 0.0
    for i in range(len(valid) - 1):
        t0, v0 = valid[i]
        t1, _ = valid[i + 1]
        dt = t1 - t0
        if dt <= 0:  # non-monotonic clock guard: ignore this interval
            continue
        num += v0 * dt
        den += dt
    if den <= 0:
        # all intervals collapsed (clock went backwards / duplicate stamps):
        # fall back to a plain mean of valid values.
        return sum(v for _, v in valid) / len(valid)
    return num / den


def _total_lag(sample: Dict[str, Any]) -> Optional[int]:
    """Sum of per-partition lag (end - committed). None if any partition has no
    committed offset yet (consumer group not fully assigned)."""
    offs = sample.get("offsets") or {}
    if not offs:
        return None
    total = 0
    for _, po in offs.items():
        committed = po.get("committed")
        end = po.get("end")
        if committed is None or end is None:
            return None
        total += max(0, end - committed)
    return total


def _delivered_position(sample: Dict[str, Any]) -> Optional[int]:
    """Sum of per-partition committed offsets = rows delivered so far. None if
    incomplete."""
    offs = sample.get("offsets") or {}
    if not offs:
        return None
    total = 0
    for _, po in offs.items():
        committed = po.get("committed")
        if committed is None:
            return None
        total += committed
    return total


def _per_partition_progress(sample: Dict[str, Any]) -> Optional[Dict[str, int]]:
    """committed offset per partition (progress). Returns None if ANY partition
    has no committed offset yet (consumer group still starting): treating a
    not-yet-committed partition as 0 would fabricate a huge max-min spread and
    inflate partition_skew during startup — the same startup-grace idea as
    compute_guards."""
    offs = sample.get("offsets") or {}
    out = {}
    for p, po in offs.items():
        c = po.get("committed")
        if c is None:
            return None
        out[p] = c
    return out


# --------------------------------------------------------------------------- #
# individual scalar computations (each guards its own inputs)
# --------------------------------------------------------------------------- #
def compute_drain_seconds(samples: List[Dict[str, Any]]) -> Tuple[float, bool]:
    """(drain_seconds, lag_reached_zero).

    Window: samples[0].t -> the first sample whose total lag == 0. If lag never
    reaches 0, the window is samples[0].t -> last sample .t and lag_reached_zero
    is False (drain incomplete / timeout). Non-monotonic clock is guarded: the
    end time is max(end, start) so drain_seconds is never negative.
    single-sample stream -> 0.0 seconds.
    """
    if not samples:
        return 0.0, False
    start_t = samples[0]["t"]
    lag_zero_t = None
    for s in samples:
        lag = _total_lag(s)
        if lag is not None and lag <= 0:
            lag_zero_t = s["t"]
            break
    if lag_zero_t is None:
        end_t = samples[-1]["t"]
        reached = False
    else:
        end_t = lag_zero_t
        reached = True
    drain = max(0.0, end_t - start_t)
    return drain, reached


def compute_drain_rate_stability(
    samples: List[Dict[str, Any]],
    bucket_seconds: Optional[float] = None,
) -> Optional[float]:
    """CoV of the delivered-rows rate over fixed-width time buckets (plan §7).
    Lower = steadier plateau; higher = sawtooth (flush stalls). None if < 2
    usable buckets.

    Bucketing (2026-07-10 refinement): the committed-offset position only jumps
    on the sink's periodic offset commit (a ~25k-record lump), so a per-poll
    (10s) delta is dominated by whether that window happened to contain a commit,
    not by the true drain rate — a visibly steady drain measured CoV ≈ 1.2–1.4
    on the live pairs. We therefore aggregate the per-poll delivered-row deltas
    into ``bucket_seconds``-wide windows (default 60s, from
    BUCKET_SECONDS_DEFAULT / DRAIN_RATE_STABILITY_BUCKET_SECONDS) and take the
    CoV of the per-bucket rates. A 60s bucket spans several commit lumps, so the
    commit cadence averages out and the CoV reflects genuine rate variation.
    The metric NAME is unchanged; only its computation changed — pre-2026-07-10
    values used 10s (per-poll) buckets and are not comparable (README).

    Method: walk consecutive samples with a valid delivered position, dropping
    intervals with a non-positive clock step or a negative row delta (rebalance
    offset regression). Assign each interval's row delta to the bucket its START
    timestamp falls in (bucket index = floor((t0 - t_origin) / bucket_seconds),
    t_origin = first usable sample). Per bucket, rate = Σrows / Σdt over the
    intervals landing in it (Σdt, not the nominal width, so a bucket with gaps is
    still divided by the time actually observed). CoV over the per-bucket rates.
    """
    if bucket_seconds is None:
        bucket_seconds = BUCKET_SECONDS_DEFAULT
    if bucket_seconds <= 0:
        bucket_seconds = BUCKET_SECONDS_DEFAULT

    # 1) reduce samples to usable (t_start, dt, drows) interval records.
    intervals: List[Tuple[float, float, float]] = []
    prev_t = None
    prev_pos = None
    for s in samples:
        pos = _delivered_position(s)
        t = s["t"]
        if pos is None:
            prev_t, prev_pos = None, None
            continue
        if prev_pos is not None and prev_t is not None:
            dt = t - prev_t
            if dt > 0:  # monotonic clock guard
                drows = pos - prev_pos
                if drows >= 0:  # ignore offset regressions (rebalance reset)
                    intervals.append((prev_t, dt, float(drows)))
        prev_t, prev_pos = t, pos

    if not intervals:
        return None

    # 2) aggregate row deltas + observed time into fixed-width buckets keyed off
    #    the first usable interval's start (so buckets are run-relative).
    t_origin = intervals[0][0]
    rows_by_bucket: Dict[int, float] = {}
    dt_by_bucket: Dict[int, float] = {}
    for t0, dt, drows in intervals:
        b = int((t0 - t_origin) // bucket_seconds)
        rows_by_bucket[b] = rows_by_bucket.get(b, 0.0) + drows
        dt_by_bucket[b] = dt_by_bucket.get(b, 0.0) + dt

    # 3) per-bucket rate = rows / observed-seconds; CoV across the buckets.
    rates = [rows_by_bucket[b] / dt_by_bucket[b]
             for b in sorted(rows_by_bucket)
             if dt_by_bucket[b] > 0]
    return _coefficient_of_variation(rates)


def compute_partition_skew(samples: List[Dict[str, Any]]) -> Optional[float]:
    """Aggregate of the per-sample (max - min) per-partition progress spread
    (plan §7: "max-min per-partition offset progress ... aggregated"). We take
    the PEAK spread across the drain — the worst straggler gap — because a
    single idle task is a parallelism defect even if the average hides it.
    Samples where any partition has not committed yet (startup) are skipped —
    see _per_partition_progress. None if no usable sample had >= 2 partitions."""
    peak = None
    for s in samples:
        prog = _per_partition_progress(s)
        if prog is None or len(prog) < 2:
            continue
        spread = max(prog.values()) - min(prog.values())
        if peak is None or spread > peak:
            peak = spread
    return float(peak) if peak is not None else None


def compute_put_batch_avg_time_ms(samples: List[Dict[str, Any]]) -> Optional[float]:
    """Time-weighted average of the sink/Connect put-batch moving-average MBean
    (plan §7: never scrape once)."""
    pairs = [
        (s["t"], (s.get("jmx") or {}).get("put_batch_avg_time_ms"))
        for s in samples
    ]
    return _time_weighted_average(pairs)


def compute_fetch_latency_avg(samples: List[Dict[str, Any]]) -> Optional[float]:
    pairs = [
        (s["t"], (s.get("jmx") or {}).get("fetch_latency_avg_ms"))
        for s in samples
    ]
    return _time_weighted_average(pairs)


def compute_records_consumed_rate(samples: List[Dict[str, Any]]) -> Optional[float]:
    pairs = [
        (s["t"], (s.get("jmx") or {}).get("records_consumed_rate"))
        for s in samples
    ]
    return _time_weighted_average(pairs)


def compute_jvm_heap_peak(samples: List[Dict[str, Any]]) -> Optional[float]:
    peak = None
    for s in samples:
        used = (s.get("jmx") or {}).get("jvm_heap_used_bytes")
        if used is None:
            continue
        if peak is None or used > peak:
            peak = used
    return peak


def compute_gc_time_share(samples: List[Dict[str, Any]]) -> Optional[float]:
    """GC time as a fraction of wall-clock over the drain: (last cumulative GC
    seconds - first) / wall_seconds. gc_collection_seconds_sum is cumulative, so
    the delta over the window is the GC time spent during the drain. None if the
    endpoint never reported GC, if the counter reset mid-run (task restart drops
    it), or if the window has no positive duration."""
    obs = [
        (s["t"], (s.get("jmx") or {}).get("gc_collection_seconds_sum"))
        for s in samples
    ]
    valid = [(t, v) for (t, v) in obs if v is not None]
    if len(valid) < 2:
        return None
    t0, gc0 = valid[0]
    t1, gc1 = valid[-1]
    wall = t1 - t0
    if wall <= 0:
        return None
    gc_delta = gc1 - gc0
    if gc_delta < 0:  # counter reset (task restart) -> not computable
        return None
    return gc_delta / wall


def compute_connect_cpu_seconds_per_Mrows(
    samples: List[Dict[str, Any]], rows_expected: float
) -> Optional[float]:
    """Integrated pod CPU seconds over the drain / (rows / 1e6). cpu_seconds_total
    is a cumulative container counter (kubelet/cadvisor). Delta over the window is
    the CPU spent draining. None if the source was absent, if the counter reset,
    or if rows_expected <= 0."""
    if rows_expected <= 0:
        return None
    obs = [
        (s["t"], (s.get("pod") or {}).get("cpu_seconds_total"))
        for s in samples
    ]
    valid = [(t, v) for (t, v) in obs if v is not None]
    if len(valid) < 2:
        return None
    cpu_delta = valid[-1][1] - valid[0][1]
    if cpu_delta < 0:  # container restarted -> counter reset
        return None
    mrows = rows_expected / 1e6
    if mrows <= 0:
        return None
    return cpu_delta / mrows


# --------------------------------------------------------------------------- #
# validity guards -> counts + contract §1.3 flag tokens
# --------------------------------------------------------------------------- #
def compute_guards(
    samples: List[Dict[str, Any]],
    lag_reached_zero: bool,
    expected_tasks: Optional[int] = None,
) -> Dict[str, Any]:
    """Guard counters + the contract §1.3 flag tokens they map to.

    Sources:
      * rebalance_count       — post-startup task-count changes vs the prior
                                sample (a rebalance reassigns partitions), plus
                                any explicit REBALANCING connector state.
      * connect_task_restarts — post-startup: a task's state transitioned through
                                UNASSIGNED/FAILED/RESTARTING->RUNNING, or the
                                worker_id for a task changed (MBean per-task
                                counter resets on restart — plan §7 JMX reality
                                check).
      * task_failed_count     — count of tasks ever observed in state FAILED
                                (counted ALWAYS, startup included — never
                                false-clean).
      * lag_reached_zero      — 0/1 passed in from compute_drain_seconds.

    Startup grace: a normal connector startup walks 0 -> N tasks and
    UNASSIGNED -> RUNNING, which is not a rebalance or a restart. Transition
    detection (task-count changes, down-state->RUNNING, worker-id churn) is
    therefore suppressed until startup completes: the first sample where the
    task set is non-empty, every task is RUNNING, and the count is >=
    ``expected_tasks`` (when given; when None, the first all-RUNNING count IS
    the expected count — "first-stable-count"). That sample seeds the baseline;
    every change after it counts. Safe direction (never false-clean): FAILED
    tasks and explicit REBALANCING states count even during the grace window,
    and if startup never completes the drain cannot finish, so the run is
    flagged ``drain_incomplete`` anyway.

    Token mapping (contract §1.3):
      rebalance_count       > 0 -> 'rebalance'
      connect_task_restarts > 0 -> 'task_restart'
      task_failed_count     > 0 -> 'task_retries'   (retried work; §1.3 token)
      lag_reached_zero    == 0  -> 'drain_incomplete'
    """
    rebalance_count = 0
    connect_task_restarts = 0
    failed_tasks = set()

    startup_complete = False
    prev_worker_by_task: Dict[int, str] = {}
    prev_state_by_task: Dict[int, str] = {}
    prev_task_count: Optional[int] = None

    for s in samples:
        c = s.get("connect")
        if not c or c.get("unavailable"):
            # cannot observe -> do not fabricate a guard trip; skip.
            continue
        # explicit rebalancing state counts always (not a startup pattern).
        if (c.get("connector_state") or "").upper() == "REBALANCING":
            rebalance_count += 1
        tasks = c.get("tasks") or []

        # FAILED counts always — startup included (never false-clean).
        for t in tasks:
            if (t.get("state") or "").upper() == "FAILED":
                failed_tasks.add(t.get("id"))

        if not startup_complete:
            all_running = bool(tasks) and all(
                (t.get("state") or "").upper() == "RUNNING" for t in tasks)
            count_ok = (expected_tasks is None
                        or len(tasks) >= expected_tasks)
            if all_running and count_ok:
                # startup done: seed the baseline from THIS sample; the
                # transition into it is startup, not a restart/rebalance.
                startup_complete = True
                prev_task_count = len(tasks)
                for t in tasks:
                    tid = t.get("id")
                    prev_worker_by_task[tid] = t.get("worker_id") or ""
                    prev_state_by_task[tid] = (t.get("state") or "").upper()
            continue

        # ---- post-startup: full transition detection -----------------------
        this_task_count = len(tasks)
        if prev_task_count is not None and this_task_count != prev_task_count:
            rebalance_count += 1
        prev_task_count = this_task_count

        for t in tasks:
            tid = t.get("id")
            state = (t.get("state") or "").upper()
            worker = t.get("worker_id") or ""
            # restart: worker id for a task changed, or state came back to
            # RUNNING from a down state.
            prev_worker = prev_worker_by_task.get(tid)
            prev_state = prev_state_by_task.get(tid)
            if prev_worker is not None and worker and worker != prev_worker:
                connect_task_restarts += 1
            elif (
                prev_state in ("FAILED", "UNASSIGNED", "RESTARTING")
                and state == "RUNNING"
            ):
                connect_task_restarts += 1
            prev_worker_by_task[tid] = worker
            prev_state_by_task[tid] = state

    task_failed_count = len(failed_tasks)
    lag_zero = 1 if lag_reached_zero else 0

    tokens = []
    if rebalance_count > 0:
        tokens.append("rebalance")
    if connect_task_restarts > 0:
        tokens.append("task_restart")
    if task_failed_count > 0:
        tokens.append("task_retries")
    if lag_zero == 0:
        tokens.append("drain_incomplete")

    return {
        "counts": {
            mn.REBALANCE_COUNT: float(rebalance_count),
            mn.CONNECT_TASK_RESTARTS: float(connect_task_restarts),
            mn.TASK_FAILED_COUNT: float(task_failed_count),
            mn.LAG_REACHED_ZERO: float(lag_zero),
        },
        "flagged": 1 if tokens else 0,
        # contract §1.3: multiple tokens joined with a single pipe, no other sep.
        "flag_reason": "|".join(tokens),
    }


# --------------------------------------------------------------------------- #
# top-level finalize
# --------------------------------------------------------------------------- #
def finalize(
    samples: List[Dict[str, Any]],
    tier: int,
    rows_expected: float,
    expected_tasks: Optional[int] = None,
) -> Dict[str, Any]:
    """Compute the full per-run scalar set + guards for one (arm, tier) run.

    ``expected_tasks``: the configured tasks.max, used by the guard startup
    grace (see compute_guards); None = infer from the first all-RUNNING sample.

    Returns:
      {
        "tier": <0|1>,
        "rows_expected": <float>,
        "scalars": { <pinned metric name>: <float|None>, ... },
        "guards": { "counts": {...}, "flagged": 0|1, "flag_reason": "" },
      }
    Scalars whose source was unavailable are present with value None; the
    inserter skips None values (a missing sample is not a zero).
    """
    if tier not in (0, 1):
        raise ValueError(f"tier must be 0 or 1, got {tier!r}")

    drain_seconds, lag_reached_zero = compute_drain_seconds(samples)

    if drain_seconds > 0 and rows_expected > 0:
        throughput = rows_expected / drain_seconds
    else:
        throughput = None

    scalars: Dict[str, Optional[float]] = {}
    # tier-exclusive headline (contract §2.2)
    scalars[mn.headline_name(tier)] = throughput
    scalars[mn.DRAIN_SECONDS] = drain_seconds
    scalars[mn.DRAIN_RATE_STABILITY] = compute_drain_rate_stability(samples)
    scalars[mn.PARTITION_SKEW] = compute_partition_skew(samples)
    scalars[mn.CONNECT_CPU_SECONDS_PER_MROWS] = (
        compute_connect_cpu_seconds_per_Mrows(samples, rows_expected)
    )
    scalars[mn.CONNECT_JVM_HEAP_PEAK] = compute_jvm_heap_peak(samples)
    scalars[mn.GC_TIME_SHARE] = compute_gc_time_share(samples)
    scalars[mn.PUT_BATCH_AVG_TIME_MS] = compute_put_batch_avg_time_ms(samples)
    scalars[mn.FETCH_LATENCY_AVG] = compute_fetch_latency_avg(samples)
    scalars[mn.RECORDS_CONSUMED_RATE] = compute_records_consumed_rate(samples)

    guards = compute_guards(samples, lag_reached_zero,
                            expected_tasks=expected_tasks)
    # the guard counters are metrics too
    for name, val in guards["counts"].items():
        scalars[name] = val

    # contract §1.2 self-check: no name outside this tier's allowed set.
    allowed = mn.METRICS_BY_TIER[tier]
    emitted = set(scalars.keys())
    stray = emitted - allowed
    if stray:
        raise AssertionError(
            f"finalize emitted names not allowed for tier {tier}: {sorted(stray)}"
        )

    return {
        "tier": tier,
        "rows_expected": rows_expected,
        "scalars": scalars,
        "guards": guards,
    }
