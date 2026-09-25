-- =============================================================================
-- v_kc_runs  —  DWH Superset virtual dataset (Benchmark v2 Kafka)
-- =============================================================================
-- WHAT THIS IS
--   The base fact table for the Kafka dashboard: one row per raw_connectors_
--   load_testing.runs row (i.e. per (arm, tier) run) for connector='kafka-connect',
--   with the runtime Map unnested into typed columns and the tall metrics pivoted
--   to one wide column per metric. Modeled on the Spark v2 v_runs_enriched
--   (benchmarks/dashboard/v2/v_runs_enriched.sql) with the Kafka connector scope
--   and the plan §8 "v_kc_runs" sketch (runs + unnested runtime joined to pivoted
--   metrics). Feeds Tab 1's tiles/integrity/validity and the flagged log; will
--   feed later tabs (#34+).
--
-- CONTRACT GROUNDING (docs/benchmark-v2-contract.md)
--   §1.1 runtime coalesce defaults: arm -> 'head', tier -> '1', outcome ->
--     'success' (legacy rows stay first-class).
--   §1.3 flagged/flag_reason; §1 config + scope keys.
--   §3 integrity: prefer the emitted integrity_ok metric, else
--     rows_delivered==rows_expected AND unique_delivered==unique_expected, else
--     NULL (unknown, NOT failed). headline_ok default-excludes integrity-FAILED.
--   §7 legacy->contract metric-name coalesce (cutover 2026-07-07): pinned name
--     preferred, ch_-prefixed legacy name as fallback, so a series spans the
--     cutover. max(if(...,value,NULL)) NEVER maxIf (absent must be NULL, not 0.0).
--
-- MATCHED-DATASET RULE (contract §5, Amendment 2026-07-09d): scoped by connector
--   VALUE = 'kafka-connect', never by instance — Tab 5 cross-connector (#36) reads
--   the same mirror filtered to the OTHER connector value, so this scope does not
--   preclude the matched-dataset join.
--
-- SOURCE: raw_connectors_load_testing.{runs,metrics} (ClickPipe DWH mirror of
--   perf.{runs,metrics}). Expected to have rows from the first Kafka pair onward;
--   returns zero rows without erroring if none exist yet.
-- =============================================================================
CREATE OR REPLACE VIEW raw_connectors_load_testing.v_kc_runs AS
WITH
  m AS (
    SELECT run_id, metric_name, argMax(value, recorded_at) AS value
    FROM raw_connectors_load_testing.metrics
    GROUP BY run_id, metric_name
  ),
  pivot AS (
    SELECT
      run_id,
      -- integrity (contract §2.1 / §3)
      max(if(metric_name = 'integrity_ok',     value, NULL)) AS integrity_ok_metric,
      max(if(metric_name = 'rows_delivered',   value, NULL)) AS rows_delivered,
      max(if(metric_name = 'rows_expected',    value, NULL)) AS rows_expected,
      max(if(metric_name = 'unique_delivered', value, NULL)) AS unique_delivered,
      max(if(metric_name = 'unique_expected',  value, NULL)) AS unique_expected,
      max(if(metric_name = 'duplicate_rows',   value, NULL)) AS duplicate_rows,
      -- Tier 0 / Tier 1 gated + covariate metrics (pinned names; legacy coalesced)
      max(if(metric_name = 'null_drain_rows_per_sec',       value, NULL)) AS null_drain_rows_per_sec,
      max(if(metric_name = 'connect_cpu_seconds_per_Mrows', value, NULL)) AS connect_cpu_seconds_per_Mrows,
      max(if(metric_name = 'drain_rows_per_sec',            value, NULL)) AS drain_rows_per_sec,
      coalesce(
        max(if(metric_name = 'parts_per_insert',    value, NULL)),
        max(if(metric_name = 'ch_parts_per_insert', value, NULL))
      )                                                     AS parts_per_insert,
      coalesce(
        max(if(metric_name = 'merge_amplification',    value, NULL)),
        max(if(metric_name = 'ch_merge_amplification', value, NULL))
      )                                                     AS merge_amplification,
      max(if(metric_name = 'ch_avg_rows_per_insert', value, NULL)) AS ch_avg_rows_per_insert,
      -- validity-guard covariates surfaced on Tab 1's validity tile
      max(if(metric_name = 'task_retries',   value, NULL)) AS task_retries,
      max(if(metric_name = 'task_restarts',  value, NULL)) AS task_restarts,
      max(if(metric_name = 'rebalances',     value, NULL)) AS rebalances
    FROM m
    GROUP BY run_id
  )
SELECT
  r.run_id                                               AS run_id,
  r.run_started_at                                       AS run_started_at,
  r.run_ended_at                                         AS run_ended_at,
  r.git_sha                                              AS git_sha,
  r.connector                                            AS connector,
  r.connector_version                                    AS connector_version,
  r.clickhouse_version                                   AS clickhouse_version,
  r.notes                                                AS notes,

  -- ---- runtime map unnested with contract coalesce defaults (§1) ----
  coalesce(nullIf(r.runtime['arm'], ''),  'head')        AS arm,
  coalesce(nullIf(r.runtime['tier'], ''), '1')           AS tier,
  r.runtime['pair_id']                                   AS pair_id,
  coalesce(nullIf(r.runtime['outcome'], ''), 'success')  AS outcome,
  (r.runtime['flagged'] = '1')                           AS flagged,
  r.runtime['flag_reason']                               AS flag_reason,
  -- config keys (§1.4)
  r.runtime['batch_size']                                AS batch_size,
  r.runtime['write_parallelism']                         AS write_parallelism,
  r.runtime['async_insert']                              AS async_insert,
  r.runtime['partition_scheme']                          AS partition_scheme,
  r.runtime['dataset']                                   AS dataset,
  -- scope keys (§1.1)
  r.runtime['environment_class']                         AS environment_class,
  r.runtime['target_region']                             AS target_region,
  r.runtime['compute_region']                            AS compute_region,

  -- ---- pivoted metrics ----
  p.null_drain_rows_per_sec,
  p.connect_cpu_seconds_per_Mrows,
  p.drain_rows_per_sec,
  p.parts_per_insert,
  p.merge_amplification,
  p.ch_avg_rows_per_insert,
  p.rows_delivered,
  p.rows_expected,
  p.unique_delivered,
  p.unique_expected,
  p.duplicate_rows,
  p.task_retries,
  p.task_restarts,
  p.rebalances,

  -- ---- integrity (contract §3) ----
  multiIf(
    p.integrity_ok_metric IS NOT NULL, p.integrity_ok_metric = 1,
    p.rows_delivered IS NOT NULL AND p.rows_expected IS NOT NULL
      AND p.unique_delivered IS NOT NULL AND p.unique_expected IS NOT NULL,
      (p.rows_delivered = p.rows_expected AND p.unique_delivered = p.unique_expected),
    NULL
  )                                                      AS integrity_ok,
  (integrity_ok IS NULL OR integrity_ok = 1)             AS headline_ok,

  -- ---- pair recency presentation columns (§ Tab-4 fix #2: latest-pair default) ----
  -- pair_ts parsed from the pair_id 'YYYY-MM-DDTHH-MM-SSZ' prefix, NULL-safe, exactly
  -- as v_kc_pair_ratios / v_kc_run_drill derive it. ADDITIVE columns: existing Tab 1/2/3
  -- consumers ignore them; Tab-4 tiles use pair_seq = 1 to self-scope to the latest
  -- pair so an unset native filter still shows the newest pair (not an all-pair sum).
  if(
    extract(pair_id, '^(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2})Z') = '',
    NULL,
    parseDateTimeBestEffortOrNull(
      replaceRegexpOne(
        extract(pair_id, '^(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2})Z'),
        'T(\\d{2})-(\\d{2})-(\\d{2})$', 'T\\1:\\2:\\3'
      )
    )
  )                                                      AS pair_ts,
  -- dense rank over pair_ts DESC (1 = newest pair), tab-wide (NOT partitioned by
  -- tier — a Tab-4 tile sums a whole pair across arm+tier).
  dense_rank() OVER (ORDER BY pair_ts DESC)              AS pair_seq

FROM raw_connectors_load_testing.runs AS r
LEFT JOIN pivot AS p ON r.run_id = p.run_id
-- Kafka scope + contract §3 fixture exclusion.
WHERE r.connector = 'kafka-connect'
  AND r.connector != '__verdict_fixture__'
