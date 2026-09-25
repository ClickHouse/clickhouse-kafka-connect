# Kafka Connect Sink Benchmark v2 — Top-Level Plan

Status: DRAFT for review. Top-level plan; to be split into concrete tasks later.
Scope: the ClickHouse Kafka Connect sink connector (this repo). The Spark connector
benchmark is a sibling plan (`spark-clickhouse-connector/docs/benchmark-v2-plan.md`);
both share the Benchmark v2 architecture, the `perf.*` metrics landing, and the DWH
pipeline — the Kafka benchmark reuses that infrastructure rather than rebuilding it.
The Kafka benchmark runs against its **own dedicated ClickHouse Cloud target** with
the same spec as the Spark target (3 vCPU / 12 GiB), so the two benchmarks never
contend and need no cross-scheduling.

---

## 1. What are we benchmarking?

Two distinct questions, each with its own instrument (a single blended number cannot
answer both):

| Q | Question | Bottleneck | Instrument |
|---|----------|-----------|------------|
| A | Is the **sink connector client** getting slower or hungrier? (conversion CPU, batching pipeline, memory, protocol efficiency) | client | Tier 0 — drain into an `ENGINE=Null` table |
| B | Is the sink sending the server **more expensive work per row**? (insert sizes, part counts, merge cost) | server | Tier 1 — drain into real MergeTree on the dedicated Cloud target |

Explicitly NOT goals: benchmarking Kafka brokers (topology is minimized on purpose),
benchmarking ClickHouse Cloud (capacity numbers are context), or measuring streaming
latency (deferred to a later rate-controlled profile — see Appendix A).

Design principle (shared with the Spark v2 plan): **a regression signal must move
when the connector changes and stay flat when the environment changes.**

---

## 2. Decision record

Decisions made interactively during planning (2026-07-06/07):

| # | Decision | Choice | Rationale / consequences |
|---|----------|--------|--------------------------|
| 1 | Architecture | **Full v2 from day one** | Two-arm HEAD-vs-pinned pairs, Tier 0 + Tier 1, ratio-based regression detection from run #1 |
| 2 | Workload model | **Backlog-drain primary** | Topic pre-loaded, connector always saturated -> rows/s is a true connector ceiling; `drain_seconds` is Spark-comparable fixed-volume wall-clock; producer speed decoupled from the measurement. Rate-controlled latency profile deferred |
| 3 | Semantics x batching | **At-least-once + tuned poll, per-partition batching** | `exactlyOnce=false`; per-partition batching keeps the insert_deduplication_token active (clean retries dedup server-side); insert size controlled via `consumer.override.max.poll.records` / fetch sizes; `tasks.max` = partition count so each task owns one partition -> predictable insert size |
| 4 | Parallelism | **3 partitions / 3 tasks** | Modest over the ~1-2 guideline for a 3 vCPU target (blog rule: insert parallelism ~= half the server's cores) but allows client/server pipelining. One-time sweep {1,2,3,6,12} later validates |
| 5 | Java client | **Keep sink default (V1)** | Benchmark what ships. If the sink's default flips to V2, the pinned arm keeps the old default and the H/P ratio records the flip as a real, annotated connector change |
| 6 | Backlog pre-load | **Re-produce fresh topic per run** | Cleanest broker state; producer runs to completion BEFORE the measured drain starts, so producer speed only costs wall-clock, never accuracy |
| 7 | Metrics infra | **Lightweight poller, no Prometheus** | Orchestrator (or sidecar) samples consumer-group offsets + Connect REST + JMX every ~10s during the drain; computes per-run scalars at drain end; inserts `perf.metrics` |
| 8 | Broker topology | **Single broker, RF=1, KRaft** | The broker only serves sequential fetches to 3 tasks and is not under test; cheapest, least variance |
| 9 | Tier 0 target | **`ENGINE=Null` table on the benchmark's own Cloud target** | Zero new infra. The H/P ratio still cancels Cloud drift (both arms hit the same table in the same hour). Known limitations recorded below (section 4, Tier 0) |
| 10 | Repo | **This repo (`clickhouse-kafka-connect`)** | Benchmark lives next to the connector it tests; CI builds the plugin from the same sha it benchmarks; natural git_sha provenance |
| 11 | Cadence | **Nightly pairs** | Trend density matches Spark; regressions surface within a day |
| 12 | EKS idle policy | **Scale node group to zero between runs** | Only the EKS control plane (~$73/mo) and operator config persist; compute is paid for run hours only; ~2-5 min node spin-up per run |

---

## 3. The two-arm protocol

Every nightly run executes the same drains **twice**:

- **Arm H** — sink plugin built from HEAD of this repo.
- **Arm P** — pinned reference build (last tagged release; bumped only deliberately,
  every bump an annotated dashboard event).

Same night, same EKS nodes, same broker, same pre-loaded topic content, same Cloud
target state. Arm order alternates by day (even/odd) so first-run advantage cannot
systematically favor one arm.

Why: the environment (ClickHouse Cloud auto-upgrades/restarts, EKS/Strimzi substrate)
cannot be pinned, so we pin a reference point inside it. Environment noise hits both
arms and cancels in the ratio H/P; only connector changes move the ratio.

Data model: both arms are ordinary rows in `perf.runs` sharing
`runtime['pair_id']`, distinguished by `runtime['arm'] = 'head' | 'pinned'`, with
`runs.connector = 'kafka-connect'` and `runtime['tier'] = '0' | '1'`.

---

## 4. Tiers

### Tier 0 — sink pipeline ceiling (`ENGINE=Null`) — nightly; the gate for Q-A

- Target: an `ENGINE=Null` table (hits schema) on the **Kafka benchmark's own Cloud
  target**. The server parses each insert and discards it — no parts, no merges, no
  storage, no memory pressure.
- Exercises the full sink path: consume/fetch, Struct conversion, batching, insert
  serialization, HTTPS, server parse, ack wait. Removes server storage noise.
- **Known limitations (accepted with decision 9):**
  1. The absolute Tier 0 trend drifts with Cloud versions (the instrument is not
     pinned). The H/P ratio remains a clean gate — that is what is gated.
  2. Parse still costs CPU; on a 3 vCPU box the Null-table ceiling may be
     parse-bound rather than connector-bound, muting client regressions. Mitigation:
     `ch_insert_cpu_share_tier0` is captured every run and charted with a threshold
     (dashboard Tab 3); if it camps near 100%, revisit this decision (fallback: CH
     pod on EKS, pinned version).

### Tier 1 — realistic ingest (real MergeTree) — nightly; the gate for Q-B

- Target: a **dedicated 3 vCPU / 12 GiB Cloud service with the same spec as the
  Spark benchmark's target**. Same spec + same dataset + same schema = honest
  cross-connector comparison (dashboard Tab 5), while dedicated instances mean the
  two benchmarks never contend and need no cross-scheduling. Caveat recorded for
  Tab 5: comparisons are same-spec, not same-instance — the two services can differ
  in Cloud version or internal state at any moment; `clickhouse_version` and the
  uptime/state covariates are recorded per run on both sides so comparisons can be
  filtered to matched conditions.
- The CH-side capture SQL from the Spark benchmark is reused (query_log / metric_log /
  part_log families), bounded by the drain window (connector start -> lag 0) plus the
  settle phase.
- Headline is a *server-bound* drain rate; gating emphasizes server-cost-per-row
  (`parts_per_insert`, `merge_amplification`) which stays meaningful when the
  capacity ceiling moves.

---

## 5. Anatomy of a nightly run

1. **Scale up**: node group 0 -> N (~2-5 min); Strimzi broker + Connect worker come up.
2. **Pre-load**: create fresh topic (3 partitions, RF=1); producer Job publishes the
   full dataset (~100M ClickBench hits rows, Avro + Schema Registry); producer
   completes; the produced row count is recorded as `rows_expected`. Production
   finishes entirely before any measured drain begins.
3. **Per arm** (order alternating nightly), deploy Connect with that arm's plugin:
   a. **Tier 0 drain**: connector configured at the Null table; consumer group starts
      at offset 0; poller samples every ~10s; drain ends when lag == 0.
   b. **Tier 1 drain**: truncate the real `hits` table; new consumer group from
      offset 0; drain into MergeTree; wait-for-settle; integrity check
      (`count()` + `uniqExact(WatchID)` vs `rows_expected`).
   c. Delete the connector + consumer groups (topic persists within the night so both
      arms drain identical bytes).
4. **Capture**: poller scalars + CH-side capture SQL -> `perf.metrics` /
   `perf.ch_inserts`, run records -> `perf.runs` (gated + atomic, with rollback —
   same discipline as the Spark pipeline).
5. **Export**: same gated S3-Parquet -> ClickPipe -> DWH path.
6. **Teardown**: delete topic; scale node group to zero.

Per night: 1 pre-load + 4 drains (2 arms x 2 tiers). At Spark-comparable rates each
drain is ~7-10 min.

Scheduling: the Kafka and Spark benchmarks run against dedicated targets, so no
cross-system scheduling is needed. The Kafka benchmark keeps its own single-run
concurrency guard (no two Kafka runs overlap on its target).

---

## 6. Operating configuration

| Setting | Value | Why |
|---|---|---|
| `exactlyOnce` | `false` | Decision 3; keeps dedup token active, bufferCount path stays available for later experiments |
| Batching | per-partition (default path) | dedup token = topic-partition-minOffset-maxOffset -> clean retry dedup |
| `consumer.override.max.poll.records` (+ `max.partition.fetch.bytes`, `fetch.max.bytes`) | sized so each insert >= 50k rows | The sink inserts what one poll delivers per topic-partition; Connect's default 500-row polls would generate ~200k tiny inserts and trip TOO_MANY_PARTS on this target. **First functional milestone: `ch_avg_rows_per_insert` >= 50k, verified from query_log** |
| Topic partitions / `tasks.max` | 3 / 3 | Decision 4; one task per partition -> insert size predictable |
| `client_version` | sink default (V1) | Decision 5; recorded in `runtime` map every run |
| Insert timeout | `clickhouseClientInsertTimeoutMs` set well below `consumer.override.max.poll.interval.ms` with margin | The sink's own docs warn a hung insert can outlive the poll interval and trigger a rebalance mid-insert; margin sized off the target's observed multi-second throttled p99s |
| Format | Avro + Schema Registry (Confluent AvroConverter), SCHEMA path -> RowBinary | Most common real typed path and the sink's best-performing insert path |
| `EventTime` encoding | epoch-seconds (int/long or Connect Timestamp), NOT bare `timestamp-micros` | Sink type-mapping constraint;全 105-column mapping validated once in CI, not discovered at 200K rows/s |
| async | none needed — the sink already forces `async_insert=0` + `wait_end_of_query=1` | Matches the Spark benchmark's async-OFF methodology by construction |
| Config provenance | every setting above echoed into `runtime` map per run | The config under test is version-controlled in this repo and visible per run |

Producer: K8s Job, reads staged hits data, publishes Avro at full speed (no rate
control needed in drain mode), records the exact produced count. Broker EBS sized for
~70 GB x RF1 transient per run; topic deleted on teardown.

---

## 7. Metrics catalog

Every metric is a **[gate]** (moves when the connector changes), a diagnostic
(explains why), or a **[guard]** (proves the run was a fair measurement). No Spark
event log exists here; client-side sources are the poller (offsets), Connect
REST/JMX, and Connect pod resource stats (kubelet/cadvisor).

### Tier 0 — drain into Null (sink pipeline ceiling)

| Metric | Source | What it tells us |
|---|---|---|
| `null_drain_rows_per_sec` **[gate]** | poller: rows_expected / (offset-0 -> lag-0) | The sink's raw ceiling — headline for Q-A |
| `connect_cpu_seconds_per_Mrows` **[gate]** | Connect pod CPU / rows | Kafka analog of Spark's executor-CPU-per-row; purest client-regression signal (kubelet-sourced, slightly noisier — see open decision 3) |
| `put_batch_avg_time_ms` | sink JMX, time-weighted across poller samples | Core sink cost per batch. NOTE: the MBean value is a moving average over recent batches — it must be sampled through the run, never scraped once at the end |
| `sink_overhead_share` | (put_batch_time - ch_insert_duration) / put_batch_time (JMX vs query_log) | Share of batch time spent in the sink pipeline vs the server — the decomposition that says WHERE a regression lives |
| `fetch_latency_avg`, `records_consumed_rate` | consumer JMX | Consume-side health; separates slow sink from slow fetch |
| `connect_jvm_heap_peak`, `gc_time_share` | Connect JMX | Buffer/allocation regressions (classic converter failure mode) |
| `bytes_on_wire_per_row` | CH network_receive_bytes / rows | Wire/format efficiency of the insert path |
| `ch_insert_cpu_share_tier0` **[caveat watch]** | query_log CPU / wall | The decision-9 guard: if the 3 vCPU box is parse-bound, Tier 0 is measuring the server — revisit |

### Tier 1 — drain into MergeTree (server cost + integrity)

| Metric | Source | What it tells us |
|---|---|---|
| `drain_rows_per_sec` (verified) **[gate]** | poller + integrity | Headline; comparable to Spark's rows/s on a same-spec target |
| `drain_seconds` | poller | The e2e_duration analog — fixed-volume wall-clock, the Spark-comparability number |
| `rows_delivered` vs `rows_expected`, `duplicate_rows` **[gate]** | count(), uniqExact(WatchID) vs producer count | Whether at-least-once + dedup-token delivers exactly-once-in-practice |
| `ch_dedup_dropped_blocks` | ProfileEvents (deduplicated blocks) | How often the dedup token actually fired — retries happened and were absorbed |
| `ch_avg_rows_per_insert` **[gate + milestone]** | query_log | The batching contract: >= 50k or the config is wrong (the 500-row default trap) |
| `parts_per_insert`, `merge_amplification` **[gate]** | part_log | Server cost-per-row — same Q-B gate as Spark, cross-connector comparable |
| insert p50/p99, delayed fraction, 241/252 counts, merge-pool peak, memory peaks | query_log / metric_log (reused Spark capture family) | The full server-side picture, unchanged |
| `drain_rate_stability` (CoV of per-interval rate) | poller curve | Steady plateau vs sawtooth (flush stalls) — invisible in the average |
| `partition_skew` | max-min per-partition offset progress | With 3 tasks, skew = idle tasks = wasted parallelism |
| `settle_seconds`; covariates `ch_uptime`, pre-run RSS / active parts; `run_cost_usd` | as the Spark v2 plan | Trend hygiene |

### Validity guards (either clean, or the run is FLAGGED non-comparable — not failed)

| Guard | Why |
|---|---|
| `rebalance_count == 0` during drain | A mid-drain rebalance reshuffles partitions and pollutes everything |
| `connect_task_restarts == 0` (+ task_failed_count) | Connect REST /status; a silently restarted task means the drain includes recovery time |
| insert timeout < poll interval margin respected | Recorded from config; keeps the rebalance-mid-insert trap closed |
| Producer completed before connector start; `rows_expected` recorded | Backlog-drain precondition |
| Offset lag reached 0 | Drain actually completed; no right-censoring |

### What the statistics teach us

- **H/P ratios** on the gates (`null_drain_rows_per_sec`, `connect_cpu_per_Mrows`,
  verified `drain_rows_per_sec`, `parts_per_insert`, `merge_amplification`) — the
  nightly regression verdict.
- **`sink_overhead_share` trend** — over months: is sink cost in conversion, in
  batching, or in the server? Directs optimization effort.
- **Cross-connector panel** — Spark vs Kafka on same-spec targets, same dataset,
  same metrics: rows/s, parts_per_insert, merge_amplification, server-CPU-per-Mrows.
  The first honest answer to "which connector is more efficient per row, and is the
  gap client-side or interaction-side?" (Same-spec, not same-instance — filter to
  matched clickhouse_version/state via the recorded covariates.)
- **Dedup-in-practice** (`duplicate_rows == 0` while `ch_dedup_dropped_blocks > 0`)
  — running evidence that the at-least-once + token design holds under real retries.
- **Drain curve shape** — plateau vs sawtooth, and whether the tail (last partition)
  drags the average; something Spark's single number can never show.

JMX reality check (from source review): the sink's MBeans expose cumulative
received/inserted/failed records+batches, bytes, DLQ count, processing times, and
moving-average lag/insert time — there are **no retry counters and no distributions**.
Distributions come from `perf.ch_inserts` (query_log), which the reused capture
already provides. MBean names include a per-process task counter and change on task
restart — another reason the restart guard exists.

---

## 8. Dashboards — exact layout

A **separate, dedicated Kafka dashboard** (kept distinct from the Spark dashboard),
mirroring the Spark v2 4-tab structure plus one Kafka-only tab. Superset on the DWH.

### Data foundation (virtual datasets, no DDL)

| Dataset | SQL sketch | Feeds |
|---|---|---|
| `v_kc_runs` | runs WHERE connector='kafka-connect' + unnested runtime (arm, tier, pair_id, partitions, poll size, ...) joined to pivoted metrics | everything |
| `v_kc_pair_ratios` | self-join on pair_id: H/P per (pair, tier, metric) | Tab 1 |
| `v_kc_drain_curve` | ch_inserts bucketed per minute: sum(written_rows)/60; cumulative sum vs rows_expected -> remaining-lag curve. **No new table needed** — per-insert event_time already exists | Tabs 2, 4 |
| `v_env_annotations` | shared with Spark (CH upgrades, restarts, pin bumps) | Tabs 1-3 |
| `v_xconn` | both connectors, arm=head, tier=1, common metric names | Tab 5 |

Global filters: date range, tier, config (runtime key/value, multi-select). Arm is
scoped per-tab (Tab 2 = head, Tab 3 = pinned).

### Tab 1 — REGRESSION (default: "did last night's pair pass?")

```
+------------+------------+------------+------------+--------------+
| TIER 0     | TIER 1     | INTEGRITY  | VALIDITY   | PAIRS IN     |
| verdict    | verdict    | dup=0 OK   | rebal=0 OK | BAND (20)    |
|  OK +1.1%  |  OK -0.4%  | count OK   | restarts=0 |   20/20      |
+------------+------------+------------+------------+--------------+
| LATEST PAIR - gated metrics (table)                              |
| tier metric                    HEAD   pinned   d%    band  OK?   |
| T0   null_drain_rows_per_sec   ...    ...      ...   +-3%        |
| T0   connect_cpu_s_per_Mrows   ...    ...      ...   +-3%        |
| T1   drain_rows_per_sec (ver.) ...    ...      ...   +-5%        |
| T1   ch_avg_rows_per_insert    ...    ...      ...   contract>=50k|
| T1   parts_per_insert          ...    ...      ...   +-5%        |
| T1   merge_amplification       ...    ...      ...   +-5%        |
+--------------------------------+---------------------------------+
| RATIO TREND Tier 0 (line, H/P, | RATIO TREND Tier 1 (line, H/P,  |
| band +-3%, pin-bump annots)    | band +-5%, config annots)       |
+--------------------------------+---------------------------------+
| EXCURSION + FLAGGED-RUN LOG: date, tier, metric/guard, d%, pair  |
+------------------------------------------------------------------+
```

8 charts. Kafka twist: a fifth tile for **validity guards** — a guard failure shows
as FLAGGED, not as a regression; the log carries both kinds. Tab 1 tile SQL is shared
with the alerting queries (GitHub issue / Slack on band excursion or integrity
failure), so dashboard and alerts cannot drift apart.

### Tab 2 — PERFORMANCE (absolute history, arm=head)

```
+-- HEADLINES -----------------------------------------------------+
| [BigNum] med drain rows/s (30d) [BigNum] med drain_seconds       |
| [BigNum] cost/run                                                |
+-- TIER 0 . SINK PIPELINE ----------------------------------------+
| null_drain_rows/s trend   | connect_cpu_per_Mrows + gc_share     |
| SINK OVERHEAD DECOMPOSITION (stacked area %): server insert time |
| vs sink-pipeline time (from sink_overhead_share) per run         |
+-- TIER 1 . THROUGHPUT & COST-PER-ROW ----------------------------+
| verified drain rows/s     | parts_per_insert + merge_amp         |
| (integrity marks)         | (dual axis)                          |
| drain_rate_stability trend| partition_skew trend                 |
+-- TIER 1 . SERVER INTERACTION (Spark-familiar family) -----------+
| insert p50/p99, delayed fraction, 241/252, merge-pool %, memory  |
| vs cap, batch-size distribution (~6 charts)                      |
+-- CONSUME SIDE --------------------------------------------------+
| fetch_latency + records_consumed_rate | put_batch_avg_time trend |
+------------------------------------------------------------------+
```

~16 charts. Centerpiece: the sink overhead decomposition — the months-long answer to
"is sink cost in conversion, batching, or the server."

### Tab 3 — ENVIRONMENT (arm=pinned; instrument health)

Six charts: pinned drain-rate trend with upgrade/restart annotations; CH version
timeline; throughput-vs-uptime scatter (+ decay); pre-run state trends (RSS, active
parts); CoV noise gauge per tier (Tier 0 must be << Tier 1 or the instrument is
broken); and Kafka-specific — **Tier 0 parse-watch**: `ch_insert_cpu_share_tier0`
trend with a threshold line. If that line camps near 100%, Tier 0 has become a server
benchmark and decision 9 is revisited.

### Tab 4 — RUN DRILL (one pair)

```
+ [Filter: pair_id] -----------------------------------------------+
| [BigNum] rows verified  [BigNum] drain_s  [BigNum] dup=0         |
| [BigNum] guards OK  [BigNum] $                                   |
+------------------------------------------------------------------+
| ARM COMPARISON (table): all metrics | HEAD | pinned | d%         |
+-----------------------------+------------------------------------+
| DRAIN CURVE: rows/s per min,| REMAINING LAG: rows_expected minus |
| H vs P overlaid (plateau or | cumulative delivered, H vs P -     |
| sawtooth view)              | shows tail/straggler pattern       |
+-----------------------------+------------------------------------+
| per-insert latency sequence | batch-size distribution +          |
| (ch_inserts, H vs P)        | covariates/config panel            |
+------------------------------------------------------------------+
```

The drain-curve + remaining-lag pair is the streaming-sink shape diagnostic Spark's
drill never had — straight out of `v_kc_drain_curve`, no new capture.

### Tab 5 — CROSS-CONNECTOR (the payoff tab; both connectors, arm=head, tier=1)

```
+------------------------------------------------------------------+
| SAME SPEC, SAME DATASET - Spark vs Kafka Connect                 |
+-----------------------------+------------------------------------+
| rows/s trend, 2 series      | server_cpu_per_Mrows, 2 series     |
+-----------------------------+------------------------------------+
| parts_per_insert, 2 series  | merge_amplification, 2 series      |
+-----------------------------+------------------------------------+
| EFFICIENCY TABLE (latest 30d medians):                           |
| metric | Spark | Kafka | ratio - which connector costs the       |
| server more per row, and is the gap client or interaction        |
+------------------------------------------------------------------+
```

5 charts, all reading the shared perf tables — zero extra capture work. This tab is
the argument for having kept the same spec, dataset, and schema across both
benchmarks (same-spec, not same-instance — see the Tier 1 caveat; charts default to
matched-version filtering).

Totals: ~36 charts, 5 virtual datasets, shared annotation layer.
Build order: `v_kc_runs` + Tab 1 skeleton (works from the first pair) -> Tab 4
(needed to debug the pipeline while it stabilizes) -> Tab 2 -> Tab 3 -> Tab 5 (needs
~a week of data).
Bands: Tier 0 +-3% / Tier 1 +-5% starting points, recalibrated from the first ~20
pairs' measured CoV. Kafka caution: Tier 0's band may need to be looser than Spark's
if the parse-watch shows the Null path partially server-bound.

---

## 9. Schema impact

**None — by design** (same argument as the Spark v2 plan):

- `arm` / `tier` / `pair_id` / all config -> `runs.runtime` map keys.
- All new metric names -> rows in tall/narrow `perf.metrics`.
- Integrity verdicts and guards -> metrics with value 1/0.
- Drain/lag curves -> derived from existing `perf.ch_inserts` (per-insert event_time)
  at query time; partition-level skew stays a poller-computed scalar. (Optional
  future extension if per-partition curves are ever wanted: a `perf.samples` table —
  not needed now.)
- Ratios/annotations -> Superset virtual datasets.
- DWH: ClickPipe mirrors the tables; new keys/names flow with zero pipeline changes.
- Tier 1 latency-profile schema deviation (`produce_ts` + `inserted_at` columns) is
  deferred with the latency profile itself (Appendix A).

---

## 10. Build phases (to be split into tasks)

| Phase | What | Exit criterion |
|---|---|---|
| 1 | EKS + Strimzi (KRaft, 1 broker RF1) + Schema Registry + scale-to-zero automation | cluster up/down cleanly from CI |
| 2 | Connect image build (plugin baked from git sha), HEAD + pinned variants; provenance into runtime map | both arm images build in CI |
| 3 | Avro schema for hits (EventTime as epoch-seconds) + producer Job (parquet -> Avro -> topic, counts recorded) + CI validation of all 105 column mappings | topic pre-load produces exact `rows_expected` |
| 4 | Poller (offsets + Connect REST + JMX sampling -> per-run scalars) | drain metrics land in perf.metrics for a manual run |
| 5 | Orchestration: full nightly anatomy (section 5), including truncate/settle/capture/rollback ported from the Spark pipeline, integrity check, and a single-run concurrency guard (no two Kafka runs overlap) | one green end-to-end pair |
| 6 | **Milestone gate: `ch_avg_rows_per_insert` >= 50k** on the Tier 1 drain (poll-size tuning until met) | contract met and recorded |
| 7 | Dashboard: v_kc_runs + Tab 1 + Tab 4, alerts | first pair visible + alertable |
| 8 | Tabs 2/3/5; band calibration after ~20 pairs | full dashboard live |
| 9 | Campaigns: parallelism sweep {1,2,3,6,12}; then deferred profiles (Appendix A) as decided | sweep report |

---

## 11. Open decisions

1. **Client-cost gate**: is kubelet-sourced `connect_cpu_seconds_per_Mrows` clean
   enough to gate on, or gate only `null_drain_rows_per_sec` and keep CPU as
   diagnostic? (Measure its CoV during phase 7 band calibration, then decide.)
2. **`sink_overhead_share`**: gated or diagnostic-only?
3. **Pinned reference**: which release starts as arm P, and the bump policy.
4. **Alert channel**: GitHub issue vs Slack (align with the Spark decision).
5. **Dedicated target provisioning**: create the Kafka benchmark's own 3 vCPU /
   12 GiB Cloud service (same spec as Spark's), same region as the EKS cluster;
   record its identity in CI secrets like the Spark target.
6. **Schema Registry**: Confluent vs Apicurio pod (either works with AvroConverter;
   pick during phase 1).

---

## Appendix A — Deferred profiles (design retained)

### Rate-controlled latency profile
Producer publishes at a fixed rate ~50% of the measured ceiling; measures
e2e per-record latency percentiles (produce -> row visible). Requires the schema
deviation: `produce_ts` stamped by the producer + `inserted_at DEFAULT now()` on the
target table (105+2 columns — an accepted, documented deviation from "same table as
Spark"); percentiles computed by one post-run aggregation query
(`quantiles(...)(inserted_at - produce_ts)`) so measurement never distorts the run.
Latency is only meaningful well below the ceiling; the plan forbids reporting latency
from saturated (drain) runs. Alternative without schema change: per-insert latency =
insert event_time - batch max produce_ts joined via query_id (coarser).

### Exactly-once comparison profile (weekly)
`exactlyOnce=true` (KeeperMap state). Measures the cost of the guarantee: throughput
delta vs the at-least-once baseline, plus the state-store load it adds to the target
(the KeeperMap table lives ON the target ClickHouse, contributing SELECT+INSERT per
batch and contaminating server-wide metrics — captured and attributed, not ignored).
Constraints to respect: bufferCount and ignorePartitionsWhenBatching are unavailable
under exactlyOnce; insert size is bounded by poll mechanics alone.

### Parallelism sweep campaign (one-time, then re-freeze)
Grid {1, 2, 3, 6, 12} partitions/tasks at fixed poll size, interleaved order,
>= 5 pairs per cell; validates the 3/3 operating point against the
half-the-server-cores guideline and locates the knee where added tasks stop paying.

### V1 -> V2 client comparison
One-time paired comparison on identical config whenever the default flips or the
team wants the migration number; then the nightly trend continues on the (new)
default.

---

## Appendix B — References

- Sibling plan: `spark-clickhouse-connector/docs/benchmark-v2-plan.md` (Benchmark v2
  architecture: two-arm protocol, tier rationale, dashboard patterns, hygiene).
- Build-critique findings driving sections 6-7 (batching model, delivery semantics,
  JMX surface, insert-timeout/poll-interval, EventTime encoding, V1 default):
  source review of this repo (`ProxySinkTask`, `ClickHouseSinkConfig`, `Processing`,
  `QueryIdentifier`, `ClickHouseWriter`, `KeeperStateProvider`, `TopicStatistics`,
  `jmx-export-connector.yml`).
- ClickHouse blog, "Supercharge your ClickHouse data loads" parts 1-3 (insert
  mechanics, threads x block-size joint optimum, memory formula, half-the-cores
  guideline, retry-safety patterns).
- Load-test report (Spark Part I measured + Kafka Part II plan): Notion, "ClickHouse
  Connector Load-Test / Benchmark Report". This plan supersedes Part II's K1-K8
  where they conflict (K3's poll/flush framing, K4's orchestration approach detail).
