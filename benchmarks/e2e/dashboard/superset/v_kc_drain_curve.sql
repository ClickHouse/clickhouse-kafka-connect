-- =============================================================================
-- v_kc_drain_curve  —  DWH Superset virtual dataset (Benchmark v2 Kafka, Tab 4/2)
-- =============================================================================
-- WHAT THIS IS
--   The per-minute drain shape + remaining-lag curve, DERIVED from perf.ch_inserts
--   (the per-insert query_log capture) with NO new table (plan §9: "Drain/lag
--   curves -> derived from existing perf.ch_inserts (per-insert event_time) at
--   query time"). Exactly the plan §8 data-foundation sketch:
--     "v_kc_drain_curve: ch_inserts bucketed per minute: sum(written_rows)/60;
--      cumulative sum vs rows_expected -> remaining-lag curve."
--   One row per (run, minute-bucket): the per-minute drain RATE (rows/s), the
--   cumulative rows delivered so far, and the REMAINING LAG (rows_expected minus
--   cumulative). Carries arm/tier/pair_id so Tab 4 can overlay the HEAD and pinned
--   curves of one pair (plan §8 "DRAIN CURVE: rows/s per min, H vs P overlaid";
--   "REMAINING LAG: rows_expected minus cumulative delivered, H vs P").
--
-- BUCKETING MATH (the load-bearing part — sanity-checked in verify_tab4.sh)
--   minute      = toStartOfMinute(event_time)                     -- 60s bucket key
--   minute_rows = sum(written_rows) within the bucket             -- rows in minute
--   rows_per_sec= minute_rows / 60.0                              -- plan: "/60"
--   cumulative  = running sum of minute_rows over minutes ASC     -- window sum
--                   (sum(minute_rows) OVER (PARTITION BY run_id ORDER BY minute
--                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))
--   remaining_lag = rows_expected - cumulative                    -- tail/straggler
--   minute_index  = (minute - firstMinute)/60  (0-based)          -- so H and P
--                   curves align on a common "minutes since drain start" x-axis
--                   even when their wall-clock start times differ.
--   The last bucket is partial (< 60s of wall clock); its rows/s is therefore a
--   lower bound for that minute — documented, not corrected (the shape read is
--   about the plateau/sawtooth, contract does not gate this curve).
--
-- rows_expected JOIN: the run's rows_expected metric (contract §2.1) gives the
--   remaining-lag denominator target. Pulled from perf.metrics with argMax by
--   recorded_at (latest wins). If a run has no rows_expected (e.g. a Tier-0 Null
--   run where the "expected" is the source count all the same, or a legacy row
--   missing it), remaining_lag is NULL for that run's rows (NULL-safe: the curve
--   simply omits the lag line; the rate + cumulative lines still render).
--
-- SCOPE: kafka-connect only (fixture excluded; failed-outcome runs excluded by
--   value). Inner-joined to runs so ch_inserts rows from a non-kafka or fixture or
--   failed run never leak in.
--
-- DEPLOYMENT DEVIATION (REPLICATED): flag tested as IN ('1','true') to match the
--   deployed Tab-1 datasets (see DASHBOARD.md). Here `flagged` is carried as a
--   presentation column so the drill can gray a flagged pair's curve; it does NOT
--   exclude the run (an operator drilling a flagged pair still wants to SEE its
--   shape — that is the whole point of the drill). Failed-OUTCOME runs ARE
--   excluded (no meaningful drain curve).
--
-- SOURCE: raw_connectors_load_testing.{ch_inserts,runs,metrics} (ClickPipe DWH
--   mirror of perf.*). ch_inserts EXISTS in the DWH (~380-420 rows/run per Task 34
--   data reality). Zero rows without error when a run has no captured inserts.
-- SETTINGS join_use_nulls = 1: a run without rows_expected => NULL lag, not 0.
-- =============================================================================
CREATE OR REPLACE VIEW raw_connectors_load_testing.v_kc_drain_curve AS
WITH
  runs_scoped AS
  (
    SELECT
      run_id,
      if(mapContains(runtime, 'arm'),  runtime['arm'],  'head') AS arm,
      if(mapContains(runtime, 'tier'), runtime['tier'], '1')    AS tier,
      runtime['pair_id']                                        AS pair_id,
      (mapContains(runtime, 'flagged') AND runtime['flagged'] IN ('1','true')) AS flagged
    FROM raw_connectors_load_testing.runs
    WHERE connector = 'kafka-connect'
      AND connector != '__verdict_fixture__'
      AND NOT (mapContains(runtime, 'outcome') AND runtime['outcome'] = 'failed')
      AND pair_id != ''
  ),

  -- rows_expected per run (remaining-lag denominator). One value per run.
  rows_expected AS
  (
    SELECT run_id, argMax(value, recorded_at) AS rows_expected
    FROM raw_connectors_load_testing.metrics
    WHERE metric_name = 'rows_expected'
    GROUP BY run_id
  ),

  -- per-minute aggregation of ch_inserts (the "bucket per minute; sum/60" step).
  per_minute AS
  (
    SELECT
      run_id,
      toStartOfMinute(event_time)          AS minute,
      sum(written_rows)                    AS minute_rows,
      count()                              AS minute_inserts,
      -- keep a couple of per-minute covariates cheap for the rate panel tooltips.
      avg(query_duration_ms)               AS avg_insert_ms,
      sum(exception_code != 0)             AS minute_errors
    FROM raw_connectors_load_testing.ch_inserts
    GROUP BY run_id, minute
  )

SELECT
  r.pair_id                                                AS pair_id,
  r.arm                                                    AS arm,
  r.tier                                                   AS tier,
  r.flagged                                                AS flagged,
  pm.run_id                                                AS run_id,
  pm.minute                                                AS minute,
  -- 0-based "minutes since this run's drain start" so H and P align on x.
  intDiv(
    toUInt32(pm.minute) - toUInt32(min(pm.minute) OVER (PARTITION BY pm.run_id)),
    60
  )                                                        AS minute_index,
  pm.minute_rows                                           AS minute_rows,
  pm.minute_inserts                                        AS minute_inserts,
  pm.avg_insert_ms                                         AS avg_insert_ms,
  pm.minute_errors                                         AS minute_errors,
  -- plan §8: rows/s per minute = sum(written_rows)/60.
  pm.minute_rows / 60.0                                    AS rows_per_sec,
  -- cumulative delivered so far (running window sum over minutes ASC).
  sum(pm.minute_rows) OVER (
    PARTITION BY pm.run_id ORDER BY pm.minute
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )                                                        AS cumulative_rows,
  re.rows_expected                                         AS rows_expected,
  -- remaining lag = rows_expected - cumulative (NULL-safe: no rows_expected => NULL).
  if(isNull(re.rows_expected), NULL,
     re.rows_expected - sum(pm.minute_rows) OVER (
       PARTITION BY pm.run_id ORDER BY pm.minute
       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
     ))                                                    AS remaining_lag,
  -- ---- pair recency presentation columns (Tab-4 fix #2: latest-pair default) ----
  -- pair_ts parsed from the pair_id 'YYYY-MM-DDTHH-MM-SSZ' prefix, NULL-safe, exactly
  -- as v_kc_run_drill / v_kc_pair_ratios derive it. ADDITIVE: existing Tab 2/4 curve
  -- charts ignore them; the Tab-4 drain_s tile uses pair_seq = 1 to self-scope to the
  -- latest pair so an unset native filter still shows the newest pair.
  if(
    extract(r.pair_id, '^(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2})Z') = '',
    NULL,
    parseDateTimeBestEffortOrNull(
      replaceRegexpOne(
        extract(r.pair_id, '^(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2})Z'),
        'T(\\d{2})-(\\d{2})-(\\d{2})$', 'T\\1:\\2:\\3'
      )
    )
  )                                                        AS pair_ts,
  -- dense rank over pair_ts DESC (1 = newest pair), tab-wide (NOT partitioned by tier).
  dense_rank() OVER (ORDER BY pair_ts DESC)                AS pair_seq
FROM per_minute AS pm
INNER JOIN runs_scoped AS r ON r.run_id = pm.run_id
LEFT  JOIN rows_expected AS re ON re.run_id = pm.run_id
ORDER BY pm.run_id, pm.minute
SETTINGS join_use_nulls = 1;
