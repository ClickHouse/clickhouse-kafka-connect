<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Benchmark v2 — metrics poller (task 29)

Lightweight, no-Prometheus-server poller (plan decision 7, plan §7). During a
backlog drain it samples the **client side** — consumer-group offsets, the
Connect REST `/status` endpoint, sink/consumer JMX via a Prometheus exporter,
and Connect-pod CPU/memory — every `POLL_INTERVAL` (default 10s). At drain end it
computes the per-run scalar set and (optionally) lands it into `perf.metrics`.

The poller owns **only** the client-side metrics. The ClickHouse-server-side
metrics (`parts_per_insert`, `merge_amplification`, `rows_delivered`,
`ch_avg_rows_per_insert`, `ch_insert_cpu_seconds_per_Mrows`, `settle_seconds`,
`run_cost_usd`, the covariates …) come from the **reused Spark capture SQL** and
are inserted by the orchestrator (task 30/31), not here.

## Layout

| File | Role |
|------|------|
| `poller.py` | CLI (`sample`, `finalize`) |
| `sampler.py` | the four sample sources + the sampling loop + JSONL I/O |
| `finalizer.py` | pure per-run scalar math (unit-tested; no I/O) |
| `ch_insert.py` | atomic `perf.metrics` inserter with rollback (creds from env) |
| `metric_names.py` | the pinned metric-name/unit/tier vocabulary (contract §2) |
| `tests/` | pytest: finalizer math, contract-name validation, inserter, loop |
| `requirements.txt` | `confluent-kafka`, `requests` (finalizer needs neither) |

## Install & test

```bash
cd benchmarks/e2e/poller
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt pytest
python -m pytest tests/ -q          # 54 tests, all pure/offline
python -m py_compile *.py tests/*.py
```

## Usage

### `sample` — run until lag 0 or timeout

```bash
python poller.py sample \
  --group   ch-sink-t0 \
  --topic   hits \
  --connector clickhouse-sink \
  --out     samples-<run_id>.jsonl \
  --bootstrap  bench-kafka-bootstrap.kafka-bench.svc:9092 \
  --connect-url http://<connect-svc>:8083 \
  --jmx-url     http://<connect-pod>:9404/metrics \
  --cadvisor-url https://<node>:10250/metrics/cadvisor \
  --pod-name <connect-pod> --pod-container <container> \
  --poll-interval 10 --timeout 3600
```

Exit codes: **0** = drained (lag reached 0), **2** = timed out. Raw samples are
appended to `--out` as JSONL, flushed each tick (a crash leaves a usable partial
file the orchestrator can still archive + finalize).

`--jmx-url` / `--cadvisor-url` may be omitted (or left empty) — the corresponding
source is then marked *unavailable* and its scalars are simply not emitted
(tolerated absence; the drain still measures via offsets).

### `finalize` — compute scalars, optionally insert

```bash
# print scalars JSON only
python poller.py finalize --samples samples.jsonl \
  --run-id 2026-07-07T04-15-32Z-91ac2dd-head-t1 --tier 1 --rows-expected 100000000

# also land into perf.metrics (creds strictly from env)
TARGET_CH_HOST=... TARGET_CH_USER=... TARGET_CH_PASSWORD=... \
python poller.py finalize --samples samples.jsonl \
  --run-id 2026-07-07T04-15-32Z-91ac2dd-head-t1 --tier 1 \
  --rows-expected 100000000 --insert
```

`--run-id` **MUST** be the `run_id` of the correct `(arm, tier)` `perf.runs` row
(contract §1.2 form `<pair_id>-<arm>-t<tier>`). `--tier` selects the tier and the
headline metric name. The finalizer refuses to emit a name outside the tier's
allowed set (a tier-0 metric on a tier-1 run_id is a hard error, not a silent
mixed series).

The poller **never** writes `perf.runs` rows. It emits a `guards` block
(`flagged`, `flag_reason`) in the `finalize` JSON that the orchestrator uses to
set `flagged` / `flag_reason` on the runs row.

## Metric-by-metric source table

Names/units are pinned in `metric_names.py`; spelling is the contract (§2).
`d/dt`-source counters are integrated over the drain window (delta of a
cumulative counter), never scraped once — see the plan §7 note on moving-average
MBeans.

| Metric (pinned name) | Unit | Tier | Source | Computation | Ref |
|----------------------|------|------|--------|-------------|-----|
| `null_drain_rows_per_sec` | rows/s | 0 only | poller offsets | `rows_expected / drain_seconds` | plan §7, contract §2.2 |
| `drain_rows_per_sec` | rows/s | 1 only | poller offsets | `rows_expected / drain_seconds` | plan §7, contract §2.2 |
| `drain_seconds` | seconds | both | poller offsets | first-sample-after-connector-start → first lag-0 sample; clock-guarded ≥0 | plan §7 |
| `drain_rate_stability` | ratio | both | poller offsets | CoV of per-interval delivered-rows rate; offset-regression deltas dropped | plan §7 |
| `partition_skew` | count | both | poller offsets | peak of per-sample (max−min) per-partition committed-offset spread | plan §7 |
| `connect_cpu_seconds_per_Mrows` | s/Mrows | both | kubelet cadvisor | Δ`container_cpu_usage_seconds_total` / (rows/1e6); None if source absent or counter reset | plan §7, contract §2.1 |
| `connect_jvm_heap_peak` | bytes | both | Connect JMX | max `jvm_memory_bytes_used{area="heap"}` across samples | plan §7 |
| `gc_time_share` | ratio | both | Connect JMX | Δ`jvm_gc_collection_seconds_sum` / wall-seconds; None on counter reset | plan §7 |
| `put_batch_avg_time_ms` | ms | both | sink/Connect JMX | **time-weighted** average of the put-batch moving-average MBean across samples | plan §7 (never scrape once) |
| `fetch_latency_avg` | ms | both | consumer JMX | time-weighted average of `fetch-latency-avg` | plan §7 |
| `records_consumed_rate` | records/s | both | consumer JMX | time-weighted average of `records-consumed-rate` (summed across tasks) | plan §7 |
| `rebalance_count` **[guard]** | count | both | Connect REST | count of REBALANCING states + task-count changes across samples | plan §7 guards |
| `connect_task_restarts` **[guard]** | count | both | Connect REST | per-task worker-id change, or state FAILED/UNASSIGNED→RUNNING | plan §7 guards |
| `task_failed_count` **[guard]** | count | both | Connect REST | distinct tasks ever observed FAILED | plan §7 guards |
| `lag_reached_zero` **[guard]** | bool | both | poller offsets | 1 if total lag hit 0, else 0 | plan §7 guards |

**Guard → `flag_reason` token mapping** (contract §1.3; multiple tokens joined
with a single `|`):

| Guard tripped | Token |
|---------------|-------|
| `rebalance_count > 0` | `rebalance` |
| `connect_task_restarts > 0` | `task_restart` |
| `task_failed_count > 0` | `task_retries` |
| `lag_reached_zero == 0` | `drain_incomplete` |

### Left out of the poller (documented, by design)

- **`sink_overhead_share`** — `(put_batch_time − ch_insert_duration)/put_batch_time`
  needs a `query_log` join. The poller supplies `put_batch_avg_time_ms`; the
  orchestrator (task 30/31) joins it to the CH-side insert duration and computes
  `sink_overhead_share` there. Plan §7 open decision 2 governs whether it is
  gated or diagnostic.
- All **CH-server-side** names (see top of this README) — from the reused Spark
  capture SQL.

## Prerequisites the orchestration / CR (task 31) MUST wire

The poller **codes against** the documented endpoints below; it does **not**
edit any `benchmarks/e2e/infra/` file. Task 31 must provide:

1. **JMX Prometheus endpoint on the Connect worker** (for the JMX-sourced
   metrics: `put_batch_avg_time_ms`, `records_consumed_rate`, `fetch_latency_avg`,
   `connect_jvm_heap_peak`, `gc_time_share`). The Strimzi `KafkaConnect` CR MUST
   enable `spec.metricsConfig` of type `jmxPrometheusExporter`, using the repo's
   existing `jmx-export-connector.yml` rule set (referenced via a
   `ConfigMapKeyRef`), on the **standard port 9404**. That YAML already maps:
   - the sink's own MBeans → `clickhouse_kafka_connect{attribute="…"}` (cumulative
     `ReceivedRecords`, `InsertedRecords`, `InsertedBytes`, `RecordProcessingTime`,
     `TaskProcessingTime`, `ReceivedBatches`, `MeanReceiveLag`, `FailedRecords`,
     `MessagesSentToDLQ` — **no distributions, no retry counters**, per plan §7
     JMX reality check; and they **reset on task restart** — the restart guard
     exists partly for this reason);
   - Connect `sink-task-metrics` (put-batch time), consumer
     `consumer-fetch-manager-metrics` (records-consumed-rate, fetch-latency-avg),
     and worker/rebalance metrics.
   Point the poller at it with `--jmx-url http://<connect-pod-or-svc>:9404/metrics`
   (or `JMX_METRICS_URL`). If absent, JMX scalars are simply not emitted.
   The default `jvm_*` series (`jvm_memory_bytes_used{area="heap"}`,
   `jvm_gc_collection_seconds_sum`) are emitted by the jmx_exporter agent by
   default — confirm the CR does not disable them.

2. **metrics-server + kubelet cadvisor scrape** (for
   `connect_cpu_seconds_per_Mrows`). The metric integrates the **cumulative**
   cadvisor counter `container_cpu_usage_seconds_total{pod,container}` from the
   kubelet `/metrics/cadvisor` endpoint (an instantaneous `kubectl top`
   millicore reading cannot be integrated — see `sampler.sample_pod` docstring).
   Task 31 MUST ensure metrics-server is installed and grant the poller RBAC to
   read the kubelet cadvisor endpoint (or a cadvisor scrape proxy), then pass
   `--cadvisor-url` + `--pod-name` + `--pod-container`. If absent, the CPU metric
   is not emitted (plan open decision 1 keeps `null_drain_rows_per_sec` the sole
   Tier-0 gate meanwhile, with CPU as diagnostic).

3. **`run_id` provenance.** Task 31 generates the `pair_id`/`run_id` per contract
   §1.2 (`<pair_id>-<arm>-t<tier>`) and passes the correct per-(arm,tier) `run_id`
   to `finalize`. The poller trusts and validates the tier, never invents ids.

4. **CH credentials via env only** — `TARGET_CH_HOST`, `TARGET_CH_USER`,
   `TARGET_CH_PASSWORD` (never on the CLI, never logged). The inserter puts them
   in HTTP headers, never in the URL/body.

## Rollback discipline (mirrors the Spark pipeline)

`insert_metrics` is idempotent + atomic: it (1) builds rows (skipping `None`
scalars — a missing source is not a zero), (2) pre-deletes any existing rows for
`(run_id, these metric names)` so a retry is clean, (3) does one batch `INSERT`,
and (4) on insert failure issues a `DELETE`-by-`run_id` scoped to the names it
owns, rolling back any partial landing before re-raising. It only ever touches
its own metric names for the `run_id`, so a concurrent CH-side capture writing
other names for the same `run_id` is untouched.

## Testing gap (explicit)

There is **no live Kafka / ClickHouse integration test** — no cluster and no
credentials are available in this environment. All 54 tests are offline:
finalizer math against synthetic streams with hand-computed answers, the
contract metric-name-set assertion, the JSONL round-trip, the Prometheus parser,
the sampling loop against injected fake sources + a fake clock, and the inserter
against a fake `requests` (row building, tier-ownership rejection, rollback
sequence, credential hygiene). The live paths (`sampler.build_sources`,
`sampler.sample_offsets`, and the real HTTP calls) are exercised for the first
time by task 31's end-to-end run.
