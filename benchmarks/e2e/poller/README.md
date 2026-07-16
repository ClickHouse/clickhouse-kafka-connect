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
python -m pytest tests/ -q          # 79 tests, all pure/offline
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
  --cadvisor-url https://kubernetes.default.svc/api/v1/nodes/<node>/proxy/metrics/cadvisor \
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
| `drain_rate_stability` | ratio | both | poller offsets | CoV of the delivered-rows rate over fixed **60s** buckets (`DRAIN_RATE_STABILITY_BUCKET_SECONDS`); offset-regression deltas dropped. See the bucketing note below | plan §7 |
| `partition_skew` | count | both | poller offsets | peak of per-sample (max−min) per-partition committed-offset spread | plan §7 |
| `connect_cpu_seconds_per_Mrows` | s/Mrows | both | kubelet cadvisor | Δ`container_cpu_usage_seconds_total` / (rows/1e6); None if source absent or counter reset. Without an explicit container filter, the pod-aggregate (`container=""`) and pause (`container="POD"`) series are excluded from the sum (double-count guard) | plan §7 (client-side; NOT the contract-§2.1 server-side `ch_insert_cpu_seconds_per_Mrows`) |
| `connect_jvm_heap_peak` | bytes | both | Connect JMX | max heap-`used` across samples; matched under **both** exporter spellings `jvm_memory_bytes_used{area="heap"}` (older jmx_exporter) and `jvm_memory_used_bytes{area="heap"}` (newer client_java) | plan §7 |
| `gc_time_share` | ratio | both | Connect JMX | Δ`jvm_gc_collection_seconds_sum` / wall-seconds; None on counter reset | plan §7 |
| `put_batch_avg_time_ms` | ms | both | sink/Connect JMX | **time-weighted** average of the put-batch moving-average MBean across samples | plan §7 (never scrape once) |
| `fetch_latency_avg` | ms | both | consumer JMX | time-weighted average of `fetch-latency-avg` | plan §7 |
| `records_consumed_rate` | records/s | both | consumer JMX | time-weighted average of `records-consumed-rate` (summed across tasks). **Requires the extra `-rate` exporter rule** (prerequisite #1a below) — the stock `jmx-export-connector.yml` has no rule matching `-rate` attributes, so without it this metric is simply not emitted | plan §7 |
| `rebalance_count` **[guard]** | count | both | Connect REST | count of REBALANCING states (always) + post-startup task-count changes (startup grace: the initial 0→N ramp is not a rebalance) | plan §7 guards |
| `connect_task_restarts` **[guard]** | count | both | Connect REST | post-startup: per-task worker-id change, or state FAILED/UNASSIGNED→RUNNING (initial UNASSIGNED→RUNNING is startup, not a restart) | plan §7 guards |
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

**Startup grace (guards only):** a normal connector startup walks `0 → N` tasks
and `UNASSIGNED → RUNNING`; counting that as a rebalance/restart would flag every
clean run. Transition detection is therefore suppressed until the first sample
where all tasks (≥ `--expected-tasks`, when given; otherwise the first
all-RUNNING count) report RUNNING. Safe direction — never false-clean: FAILED
tasks and explicit REBALANCING states count even during the grace window, and a
startup that never completes cannot drain, so `drain_incomplete` flags the run
regardless. The same idea protects `partition_skew`: samples where a partition
has not committed yet are skipped instead of being treated as offset 0.

### `drain_rate_stability` bucketing (changed 2026-07-10 — not comparable across the boundary)

`drain_rate_stability` is the CoV of the delivered-rows rate, i.e. how steady the
drain plateau is. The committed-offset position (its only input) advances **only
on the sink's periodic offset commit** — a large lump (~25k+ records) every few
seconds — so a delta measured at the 10s poll cadence is dominated by *whether
that particular window contained a commit*, not by the real drain rate. On the
live pairs a visibly steady drain measured CoV ≈ **1.2–1.4** purely from this
commit rhythm.

**Refinement (2026-07-10):** the per-poll delivered-row deltas are now aggregated
into fixed **60s buckets** (`DRAIN_RATE_STABILITY_BUCKET_SECONDS`, default 60)
before the CoV is taken. A 60s bucket spans several commit lumps, so the commit
cadence averages out and the CoV reflects genuine rate variation. Same metric
**name**, changed computation.

> **Comparability:** values recorded **before 2026-07-10 used 10s (per-poll)
> buckets** and are **not comparable** to values recorded after. The unit tests
> pin both: the synthetic steady-drain-with-lumpy-commits stream yields CoV ≈
> 1.41 at 10s buckets and ≈ 0 at 60s, proving the fix.

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
   - Connect `sink-task-metrics` (put-batch time, via the `.+-avg` rule) and
     consumer `consumer-fetch-manager-metrics` `fetch-latency-avg` (same rule),
     and worker/rebalance metrics.
   It does **NOT** map `records-consumed-rate`: none of its attribute patterns
   matches a `-rate` suffix. See 1a.
   Point the poller at it with `--jmx-url http://<connect-pod-or-svc>:9404/metrics`
   (or `JMX_METRICS_URL`). If absent, JMX scalars are simply not emitted.
   The default `jvm_*` series (heap-used, `jvm_gc_collection_seconds_sum`) are
   emitted by the jmx_exporter agent by default — confirm the CR does not disable
   them. **Heap naming differs across agent versions:** older jmx_exporter emits
   `jvm_memory_bytes_used{area="heap"}`, newer client_java emits
   `jvm_memory_used_bytes{area="heap"}`. The poller matches **both** spellings
   (pair-2, 2026-07-09, emitted the newer one, so `connect_jvm_heap_peak` was
   null while `gc_time_share` — unaffected — was present); no CR change is needed
   for either.

   **1a. Extra exporter rule for `-rate` attributes (REQUIRED for
   `records_consumed_rate`).** The CR's `metricsConfig` rule list MUST include,
   in addition to the `jmx-export-connector.yml` rules, a rule matching the
   consumer fetch-manager rate attributes — paste-able:

   ```yaml
   # consumer-fetch-manager rate attributes (records-consumed-rate, ...) —
   # required by the benchmark poller (task 29); the stock
   # jmx-export-connector.yml rules match no `-rate` suffix.
   - pattern: kafka.consumer<type=consumer-fetch-manager-metrics, client-id=(.*)><>(.+-rate)
     name: kafka_consumer_fetch_manager_$2
     labels:
       clientId: "$1"
     help: "Kafka consumer fetch-manager rate metric"
     type: GAUGE
   ```

   With the exporter's name sanitization (dashes → underscores,
   `lowercaseOutputName: true`) this surfaces as
   `kafka_consumer_fetch_manager_records_consumed_rate{clientid="…"}`, which the
   poller matches by the `records_consumed_rate` substring. Without this rule
   the poller tolerates the absence and the metric is not emitted.

2. **kubelet cadvisor scrape via the API-server node proxy** (for
   `connect_cpu_seconds_per_Mrows` and the tier-0 CPU gate share
   `kafka_worker_cpu_share_t0`) — **DONE (wired, sighted gate).** The metric
   integrates the **cumulative** cadvisor counter
   `container_cpu_usage_seconds_total{pod,container}` from the kubelet
   `/metrics/cadvisor` endpoint (an instantaneous `kubectl top` millicore reading
   cannot be integrated — see `sampler.sample_pod` docstring).

   **Mechanism (as wired):**
   - **URL shape:** the poller reads the cadvisor endpoint of the node hosting
     the Connect worker *through the API-server proxy* —
     `https://kubernetes.default.svc/api/v1/nodes/<node>/proxy/metrics/cadvisor`.
     `run_pair.sh:deploy_connect` resolves `<node>` per-arm (the node the Connect
     pod landed on) and exports it as `POD_CADVISOR_URL`; the same function
     resolves the pod's main container name into `CONNECT_POD_CONTAINER`.
     `run_poller_sample` passes both as `--cadvisor-url` / `--pod-container`
     (plus the already-resolved `--pod-name ${CONNECT_POD}`).
   - **Auth/TLS:** the poller pod runs as ServiceAccount `bench-poller-sa`
     (`benchmarks/e2e/infra/poller-rbac.yaml`), which holds a **least-privilege**
     `ClusterRole` granting exactly `get` on `nodes/proxy` (no secrets, no pods,
     no exec). `sampler._cadvisor_auth` activates on the `https://` scheme: it
     sends `Authorization: Bearer <token>` from the projected SA token
     (`/var/run/secrets/kubernetes.io/serviceaccount/token`) and verifies TLS
     against the mounted cluster CA
     (`/var/run/secrets/kubernetes.io/serviceaccount/ca.crt`). A plain `http://`
     URL (tests / a pre-authorized proxy) is scraped bare, unchanged.
   - **Apply path:** `benchmarks/e2e/infra/scale-up.sh` applies
     `poller-rbac.yaml` (idempotent, cluster-scoped) alongside the Kafka /
     Schema-Registry manifests, so the operator does **nothing extra** — a normal
     scale-up provisions the gate. `run_pair.sh`'s poller pod sets
     `serviceAccountName: bench-poller-sa`.

   If the node cannot be resolved (unusual), `POD_CADVISOR_URL` is left empty and
   the CPU metric/gate degrade to UNAVAILABLE (tolerated absence, plan open
   decision 1) rather than failing the run.

3. **`run_id` provenance.** Task 31 generates the `pair_id`/`run_id` per contract
   §1.2 (`<pair_id>-<arm>-t<tier>`) and passes the correct per-(arm,tier) `run_id`
   to `finalize`. The poller trusts and validates the tier, never invents ids.

4. **CH credentials via env only** — `TARGET_CH_HOST`, `TARGET_CH_USER`,
   `TARGET_CH_PASSWORD` (never on the CLI, never logged). The inserter puts them
   in HTTP headers, never in the URL/body.

5. **Poller start ordering.** Task 31 MUST start `poller.py sample` only AFTER
   the connector is created and `/connectors/<name>/status` reports the
   connector and all `tasks.max` tasks RUNNING. The guard startup grace (above)
   makes the guards robust to observing the tail of the ramp anyway, but the
   `drain_seconds` start anchor is the FIRST sample — polling long before the
   connector exists would inflate the measured drain. Pass the configured
   `tasks.max` to `finalize --expected-tasks` so the grace window is bounded by
   the real expectation instead of inferred.

## Rollback discipline (mirrors the Spark pipeline)

`insert_metrics` is idempotent + atomic + verified: it (1) builds rows (skipping
`None` scalars — a missing source is not a zero), (2) pre-deletes any existing
rows for `(run_id, these metric names)` so a retry is clean, (3) does one batch
`INSERT`, (4) **verifies** the landing (`SELECT count()` for the run_id + name
set must equal the inserted row count), and (5) on insert **or verify** failure
issues a `DELETE`-by-`run_id` scoped to the names it owns, rolling back any
partial landing before raising. It only ever touches its own metric names for
the `run_id`, so a concurrent CH-side capture writing other names for the same
`run_id` is untouched.

## Testing gap (explicit)

There is **no live Kafka / ClickHouse integration test** — no cluster and no
credentials are available in this environment. All 79 tests are offline:
finalizer math against synthetic streams with hand-computed answers (including
the guard startup grace, the cadvisor double-count exclusion, and the
`drain_rate_stability` bucketing refinement — the lumpy-commit stream pinned at
CoV ≈ 1.41 @10s buckets vs ≈ 0 @60s), the contract metric-name-set assertion,
the JSONL round-trip, the Prometheus parser + heap dual-spelling
(`jvm_memory_bytes_used` / `jvm_memory_used_bytes`), the cadvisor
API-server-proxy auth (`_cadvisor_auth` http/https/no-token paths + the
Bearer-header/CA `verify` forwarding into a capturing fake `requests`), the
cadvisor startup self-check (`probe_cadvisor`: armed / 403-RBAC / empty-body /
no-matching-series / connection-error log lines), the sampling loop against
injected fake sources + a fake clock, and the inserter against a fake `requests`
(row building, tier-ownership rejection, rollback sequence, post-insert verify +
verify-mismatch rollback, credential hygiene).
The live paths (`sampler.build_sources`,
`sampler.sample_offsets`, and the real HTTP calls) are exercised for the first
time by task 31's end-to-end run.
