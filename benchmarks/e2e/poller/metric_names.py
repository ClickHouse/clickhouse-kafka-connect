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
"""Pinned metric-name + unit + tier vocabulary (Benchmark v2 contract §2, plan §7).

Spelling IS the series identity (contract §2). This module is the single source
of truth for the names the poller emits into ``perf.metrics`` so the finalizer,
the inserter, and the tests all agree — a typo anywhere is a different series.

CRITICAL contract rules encoded here:
  * The connector-specific throughput headlines stay Kafka-specific
    (``null_drain_rows_per_sec`` for Tier 0, ``drain_rows_per_sec`` for Tier 1);
    they are NOT stored under the cross-connector alias (contract §2.2).
  * Where the plan §7 and the contract §2 disagree, the CONTRACT wins.
  * A ``perf.metrics`` row inherits (arm, tier) solely through its ``run_id``.
    Therefore each metric belongs to exactly one tier; ``METRICS_BY_TIER`` pins
    which names may attach to a tier-0 vs a tier-1 run_id. Emitting a tier-0
    name onto a tier-1 run row (or vice versa) is PROHIBITED (contract §1.2).

Metrics NOT emitted by the poller (documented, not a gap in this module):
  * ``sink_overhead_share`` — needs a query_log join (task 30/31 computes it).
  * All CH-server-side names (parts_per_insert, merge_amplification,
    rows_delivered, ch_avg_rows_per_insert, ch_insert_cpu_*, settle_seconds,
    run_cost_usd, covariates …) come from the reused Spark capture SQL, not the
    poller.
The poller owns exactly the client-side (offsets / Connect REST / JMX / kubelet)
scalars below.
"""

# --- unit strings written to perf.metrics.unit -----------------------------
U_ROWS_PER_SEC = "rows/s"
U_SECONDS = "seconds"
U_MS = "ms"
U_RATIO = "ratio"
U_PERCENT = "percent"
U_COUNT = "count"
U_BOOL = "bool"
U_ROWS_PER_SEC_PER_CONSUMED = "records/s"
U_BYTES = "bytes"
U_S_PER_MROWS = "s/Mrows"

# --- Tier-0-only headline (contract §2.2 — Kafka-specific spelling) ---------
NULL_DRAIN_ROWS_PER_SEC = "null_drain_rows_per_sec"
# --- Tier-1-only headline (contract §2.2 — Kafka-specific spelling) ---------
DRAIN_ROWS_PER_SEC = "drain_rows_per_sec"

# --- shared poller scalars (both tiers; distinct run_id per tier) -----------
DRAIN_SECONDS = "drain_seconds"
DRAIN_RATE_STABILITY = "drain_rate_stability"
PARTITION_SKEW = "partition_skew"
CONNECT_CPU_SECONDS_PER_MROWS = "connect_cpu_seconds_per_Mrows"
CONNECT_JVM_HEAP_PEAK = "connect_jvm_heap_peak"
GC_TIME_SHARE = "gc_time_share"
PUT_BATCH_AVG_TIME_MS = "put_batch_avg_time_ms"
FETCH_LATENCY_AVG = "fetch_latency_avg"
RECORDS_CONSUMED_RATE = "records_consumed_rate"

# --- validity-guard counters (emitted as metrics too; value 1/0 or count) ---
REBALANCE_COUNT = "rebalance_count"
CONNECT_TASK_RESTARTS = "connect_task_restarts"
TASK_FAILED_COUNT = "task_failed_count"
LAG_REACHED_ZERO = "lag_reached_zero"

# Unit for every emitted scalar. Presence in this dict == "the poller emits it".
UNITS = {
    NULL_DRAIN_ROWS_PER_SEC: U_ROWS_PER_SEC,
    DRAIN_ROWS_PER_SEC: U_ROWS_PER_SEC,
    DRAIN_SECONDS: U_SECONDS,
    DRAIN_RATE_STABILITY: U_RATIO,
    PARTITION_SKEW: U_COUNT,
    CONNECT_CPU_SECONDS_PER_MROWS: U_S_PER_MROWS,
    CONNECT_JVM_HEAP_PEAK: U_BYTES,
    GC_TIME_SHARE: U_RATIO,
    PUT_BATCH_AVG_TIME_MS: U_MS,
    FETCH_LATENCY_AVG: U_MS,
    RECORDS_CONSUMED_RATE: U_ROWS_PER_SEC_PER_CONSUMED,
    REBALANCE_COUNT: U_COUNT,
    CONNECT_TASK_RESTARTS: U_COUNT,
    TASK_FAILED_COUNT: U_COUNT,
    LAG_REACHED_ZERO: U_BOOL,
}

# The headline throughput name is the ONLY name that differs by tier. Everything
# else is shared but still attaches to the correct-tier run_id (the run_id, not
# the name, carries the tier — contract §1.2).
_SHARED = [
    DRAIN_SECONDS,
    DRAIN_RATE_STABILITY,
    PARTITION_SKEW,
    CONNECT_CPU_SECONDS_PER_MROWS,
    CONNECT_JVM_HEAP_PEAK,
    GC_TIME_SHARE,
    PUT_BATCH_AVG_TIME_MS,
    FETCH_LATENCY_AVG,
    RECORDS_CONSUMED_RATE,
    REBALANCE_COUNT,
    CONNECT_TASK_RESTARTS,
    TASK_FAILED_COUNT,
    LAG_REACHED_ZERO,
]

# The full set of names the poller may attach to a run of each tier. The
# headline is tier-exclusive: attaching null_drain_rows_per_sec to a tier-1
# run_id, or drain_rows_per_sec to a tier-0 run_id, is PROHIBITED (contract §1.2).
METRICS_BY_TIER = {
    0: set([NULL_DRAIN_ROWS_PER_SEC] + _SHARED),
    1: set([DRAIN_ROWS_PER_SEC] + _SHARED),
}


def headline_name(tier: int) -> str:
    """The tier-correct throughput headline name (contract §2.2)."""
    if tier == 0:
        return NULL_DRAIN_ROWS_PER_SEC
    if tier == 1:
        return DRAIN_ROWS_PER_SEC
    raise ValueError(f"tier must be 0 or 1, got {tier!r}")


def unit_for(name: str) -> str:
    try:
        return UNITS[name]
    except KeyError:
        raise KeyError(f"no pinned unit for metric {name!r}; add it to metric_names.UNITS")
