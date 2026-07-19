# CH-side capture / integrity / export / rollback — port from the Spark pipeline

Task 30. This directory ports the ClickHouse-side capture pipeline from
`spark-clickhouse-connector/benchmarks/` (scripts + `sql/perf/1*.sql`, `2*.sql`)
to the Kafka Connect sink benchmark. The scripts live here
(`benchmarks/e2e/capture/`); the ported capture SQL lives in
`benchmarks/e2e/sql/capture/` (a NEW dir — the byte-locked
`benchmarks/e2e/sql/perf/` DDL is not touched).

This document records, per source file: **ported / dropped-why / adapted-how**,
the pinned-name mapping applied, the chosen query_log filter, and the
gating/rollback discipline.

## Gating & rollback discipline (ported VERBATIM from the Spark pipeline)

Ordering (Spark commits 8a185f98, f6eca292, cbba227b, 91ac2ddd):

1. **Gated capture.** Each capture SQL is an independent INSERT into
   `perf.metrics` / `perf.ch_inserts`. Run them in numeric order (15 before 19,
   since 19 reads `perf.ch_inserts`).
2. **Roll back partial metrics on failure.** If ANY capture step fails,
   `rollback_run_metrics.py` deletes this `run_id`'s rows from all three perf
   tables (`runs`, `metrics`, `ch_inserts`) — the source never holds metrics for
   a run that was never fully recorded.
3. **`perf.runs` insert only after complete metrics.** `insert_run_record.py`
   runs ONLY after capture fully succeeded. If it fails, roll back too (no runs
   row without complete metrics; no metrics without a runs row).
4. **Export gated on the runs insert.** `export_metrics_to_dwh.py` runs ONLY
   after the runs row is written. A rolled-back / never-recorded run is never
   exported.
5. **Integrity decides the verdict LAST.** `check_integrity.py` runs AFTER
   capture + run-record + export, so the integrity evidence is already persisted
   and exported before the run's fate is decided. Per contract §3 an integrity
   mismatch FAILS the run (no headline) but the metrics must survive for the
   dashboard — hence the failure lives here, not in a rollback-triggering
   capture failure.

## Pinned-name mapping applied (contract §2 / §7)

The Spark capture SQL on disk had already been renamed to the pinned spellings
(Spark task #40), so the ported SQL emits the PINNED names directly (no legacy
coalesce needed on the Kafka side — no Kafka history exists):

| Pinned name (emitted here) | Old Spark spelling (§7) | File |
|---|---|---|
| `parts_per_insert` | `ch_parts_per_insert` | `13_insert_from_part_log.sql` |
| `merge_amplification` | `ch_merge_amplification` | `13_insert_from_part_log.sql` |
| `inserts_delayed_fraction` | `ch_inserts_delayed_fraction` | `16_insert_throttling.sql` |
| `merge_pool_peak_pct` | `ch_merge_pool_peak_pct` | `16_insert_throttling.sql` |
| `settle_seconds` | `ch_settle_seconds` | `14_insert_settle_seconds.sql` |
| `ch_insert_cpu_seconds_per_Mrows` | (was missing) | `11_insert_from_query_log.sql` |

Already conformant, ported as-is: `rows_delivered`, `rows_expected`,
`unique_delivered`, `unique_expected`, `duplicate_rows`, `ch_dedup_dropped_blocks`,
`bytes_on_wire_per_row`, `ch_parts_active_peak`, `ch_memory_limit_errors`,
`ch_avg_rows_per_insert`, `settle_timed_out`, `ch_uptime`, `pre_run_rss`,
`pre_run_active_parts`.

Plan §7 Kafka additions checked against the Spark 1x SQL: `ch_avg_rows_per_insert`
(present, file 11), `parts_per_insert` + `merge_amplification` (present, file 13),
`ch_insert_cpu_share_tier0` (Tier 0 — Kafka-native SQL 23, no Spark source; see
the Tier 0 note below), `bytes_on_wire_per_row` (present, file 18).

## The query_log filter (Kafka-specific)

Spark scoped its inserts in `system.query_log` with Spark-specific predicates.
The sink's inserts are identified differently, so the filter is parameterized:

- **Primary:** `has(tables, {table_qualified:String})` — the target table
  (`clickbench.hits` / `clickbench.hits_null`). Ported unchanged; it already
  excludes CH-Cloud-internal billing inserts.
- **Optional narrowing:** `AND ({query_user:String} = '' OR user = {query_user:String})`.
  `{query_user}` is bound from `QUERY_LOG_USER` (config.env, default
  `kafka_benchmark`). Empty => the predicate is a no-op and the table scope alone
  applies. Set, it restricts the capture to the sink's ClickHouse user, excluding
  any co-tenant inserts on the same table.

This is a bound parameter (not string-interpolated SQL), so it rides the exact
same `clickhouse_connect` substitution mechanism as every other parameter.
**Task 31's first manual run validates this filter against live sink query_log
rows** (see the deferred-validation note in the report).

## Per-source-file disposition

### Scripts (`spark-clickhouse-connector/benchmarks/scripts/`)

| Source | Disposition | Notes |
|---|---|---|
| `ch_common.py` | **ported** verbatim | connect helper (port note only). |
| `run_metrics_sql.py` | **adapted** | Same `{name:Type}` substitution. Added `{query_user}` param (QUERY_LOG_USER). Dropped `EVENT_LOG_URI`/`target_addr`-of-Spark specifics kept; `resolve_expected` now also honours the pinned constants when there is NO glob (Kafka backlog-drain: `rows_expected` = producer committed-offset count). |
| `truncate_target.py` | **ported** verbatim | per-run reset before the Tier 1 drain. |
| `wait_for_settle.py` | **ported** verbatim | reads `system.parts` + `system.merges` on the target. |
| `insert_run_record.py` | **adapted** | `connector` defaults to `kafka-connect`; mandatory `target_region`/`environment_class` sourced from config.env and HARD-checked (never inline); nogit HARD-FAIL on RUN_ID/PAIR_ID (contract §1.2); arm/tier/pair_id + §1.4 config keys arrive via the RUNTIME JSON passthrough. |
| `rollback_run_metrics.py` | **ported** verbatim | deletes run_id rows from all three perf tables. |
| `check_integrity.py` | **ported** verbatim | reads back integrity_ok and fails on mismatch (contract §3). |
| `export_metrics_to_dwh.py` | **adapted** | `DWH_BUCKET` is now a REQUIRED param (no hardcoded default) — bucket/region is a pending decision (see README). Gating unchanged. |
| `lib_runid.sh` | **adapted** | Added the contract §1.2 nogit HARD-FAIL (was a silent `-nogit` fallback in Spark). RUN_ID format unchanged. |
| `compute_run_cost.py` | **dropped (Kafka-inapplicable)** | EMR-instance pricing. Kafka runs on EKS; `run_cost_usd` is a separate Kafka cost concern (plan §7 covariate) computed elsewhere, not in this port. |
| `read_task_failed_count.py` | **dropped (Spark event-log)** | reads `task_failed_count`, a Spark-task-attempt metric emitted by the event-log SQL (file 10). The Kafka analogue (Connect task restarts / failed count) is the poller's job (task 29) and feeds the same `task_retries`/`task_restart` flags there. |

### Capture SQL (`spark-clickhouse-connector/benchmarks/sql/perf/`)

| Source | Disposition | Notes |
|---|---|---|
| `10_insert_from_event_log.sql` | **dropped (Spark-only)** | reads the Spark event log (S3). No event log exists for Kafka; client-side metrics come from the poller (task 29). |
| `11_insert_from_query_log.sql` | **ported + adapted** | Kafka query_log filter (above). `ch_insert_cpu_seconds_per_Mrows` pinned. |
| `12_insert_from_metric_log.sql` | **ported + adapted** | one query_log denominator gets the query_user filter. |
| `13_insert_from_part_log.sql` | **ported + adapted** | `parts_per_insert` / `merge_amplification` pinned; query_log denominators get the filter. |
| `14_insert_settle_seconds.sql` | **ported** | `settle_seconds` pinned. |
| `15_capture_raw_inserts.sql` | **ported + adapted** | query_user filter; feeds `perf.ch_inserts` (and the drain-curve virtual dataset, plan §8). |
| `16_insert_throttling.sql` | **ported + adapted** | `inserts_delayed_fraction` / `merge_pool_peak_pct` pinned; query_log reads get the filter. |
| `17_insert_memory_peaks.sql` | **ported** verbatim | server-wide gauges, no query_log scope. |
| `18_insert_derived_metrics.sql` | **adapted (partial port)** | ONLY `bytes_on_wire_per_row` is portable (server-side). The client-side per-row metrics it references live in the Spark event-log SQL (file 10) and are DROPPED; those come from the poller (task 29). |
| `19_insert_rate_stability.sql` | **ported** verbatim | reads `perf.ch_inserts` for this run_id → `ingest_rate_stability` (server-side CoV). The client-side `drain_rate_stability` is the poller's (task 29). |
| `20_insert_integrity.sql` | **ported** verbatim | target-vs-SOURCE deltas on BOTH count() and uniqExact(WatchID); source constants recorded as metrics; `ch_dedup_dropped_blocks` from ProfileEvents. The ClickBench-hits duplicate-WatchID semantics (do NOT use count()−uniqExact() on the target) are preserved exactly. For Kafka, `rows_expected` = the producer's committed-offset count. |
| `21_pre_run_covariates.sql` | **ported** verbatim | pre-drain covariates; reads `system.asynchronous_metrics`, `system.parts`. |
| `22_insert_settle_timed_out.sql` | **ported** verbatim | `settle_timed_out` flag (contract §1.3 token settle_timeout). |
| `23_insert_tier0_cpu_share.sql` | **Kafka-native (no Spark source)** | `ch_insert_cpu_share_tier0` (unit `percent`); tier-0-only parse-watch, wired into run_pair.sh's capture loop on tier 0 ONLY (contract §2.1, amendment 3298da9b). |

### Tier 0 note

`ch_insert_cpu_share_tier0` (plan §7 Tier 0 parse-watch; contract §2.1) is NOT
emitted by any Spark capture SQL (Spark has no Tier 0 build on disk); it is a
Kafka-native capture file (`23_insert_tier0_cpu_share.sql`). It is a
`query_log` insert-CPU / wall-clock share captured on the Tier 0 Null run;
because the Kafka Tier 0 target is a **Cloud-hosted** `ENGINE=Null` table on the
SAME Cloud service (plan decision 9), it is derived from the same
`system.query_log` capture window as `ch_insert_cpu_seconds` (SQL 11) — the CPU
numerator matches SQL 11's accounting exactly. Per the contract §1.1
`tier0_ch_version` scoping note, Cloud-hosted Null tier-0 rows omit the pinned
`tier0_ch_version` and this parse-watch metric is MANDATORY for them instead. It
is emitted on TIER-0 runs ONLY (run_pair.sh's capture loop gates it by tier,
mirroring how SQL 20 integrity is gated to tier 1).

## Drain window (Kafka adaptation)

Spark bounds capture by its event-log window. Kafka bounds by the
connector-start timestamp → lag-0 timestamp (`{run_start}`/`{run_end}`), plus the
settle end (`{settle_end}` from `wait_for_settle.py`) for the merge/settle-window
SQL. All parameters are supplied by the orchestrator/poller (task 29/31); the SQL
consumes them through the unchanged substitution mechanism.
