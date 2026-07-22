-- =============================================================================
-- v_xconn  —  DWH Superset virtual dataset (Benchmark v2, Tab 5 CROSS-CONNECTOR)
-- =============================================================================
-- WHAT THIS IS
--   The ONE cross-connector comparison surface: Spark vs Kafka Connect on the
--   SHARED perf schema, tall per-run (one row per run × canonical metric), carrying
--   BOTH connectors and the columns Tab 5's payoff charts + efficiency table read.
--   This is the only view in the package that is NOT scoped to a single connector
--   (contract §5 Amendment 2026-07-09d: cross-connector lives on the Kafka
--   dashboard's Tab 5; the Spark dashboard does not duplicate it — see the Spark v2
--   view headers "cross-connector lives on [the Kafka] Tab 5").
--
-- CONNECTOR SCOPE (GROUNDED, not guessed)
--   * Spark  = connector VALUE 'spark'  — grounded in the DEPLOYED Spark dashboard
--     SQL: spark-clickhouse-connector/benchmarks/dashboard/v2/v_runs_enriched.sql:293
--     (WHERE r.connector = 'spark'), v_pair_ratios.sql:141, v_trailing_windows.sql:176;
--     write-side default benchmarks/scripts/insert_run_record.py:70
--     ("connector": os.environ.get("CONNECTOR", "spark")) and the schema DEFAULT
--     benchmarks/sql/perf/02_create_runs.sql:19 (connector String DEFAULT 'spark').
--     NOT 'clickhouse-spark' (that string is only a Grafana title / superset tag).
--   * Kafka  = connector VALUE 'kafka-connect' (this repo's own scope value; see the
--     sibling v_kc_* views' WHERE connector = 'kafka-connect').
--   The view emits a friendly `connector_label` ('Spark' | 'Kafka Connect') as the
--   chart SERIES key so the two overlaid lines are self-describing.
--
-- ARM / TIER SCOPE (plan §8 Tab-5 sketch): both connectors, arm = head, tier = 1.
--   arm=head = the code-under-test comparison (Tab 5 is the payoff, not the
--   instrument-health arm); tier=1 = the verified end-to-end throughput both
--   pipelines share. The server-cost + parts/merge family are all tier-1 too.
--   arm/tier are carried as columns so a native filter could widen scope later, but
--   the charts default-filter arm='head' AND tier='1'.
--
-- HEADLINE ALIASING (contract §2.2 — the alias lives HERE, in SQL, never a stored
--   rename): the two connectors' throughput headlines measure different operations
--   and keep DISTINCT stored names; this view folds them to ONE `rows_per_sec`
--   series so a single chart overlays both:
--       Spark  throughput_rows_per_sec  ─┐
--       Kafka  drain_rows_per_sec        ├─►  rows_per_sec   (tier 1, verified)
--       Spark  null_rows_per_sec        ─┐
--       Kafka  null_drain_rows_per_sec   ├─►  rows_per_sec   (tier 0 analogue, aliased
--                                                              here for completeness)
--   Neither pipeline MAY store its headline under 'rows_per_sec' — that would erase
--   which operation produced it (§2.2). The fold is metric-name only; values pass
--   through untouched.
--
-- SERVER-COST HEADLINE (contract §2.1 / Appendix): the cross-connector server-cost
--   metric MUST be spelled `ch_insert_cpu_seconds_per_Mrows` — NOT the plan's
--   sketch name `server_cpu_per_Mrows`. This view surfaces the pinned name (and
--   folds the ch_-prefixed legacy spelling for it, same §7 cutover rule as the
--   sibling views).
--
-- LEGACY→CONTRACT COALESCE (§7 cutover 2026-07-07): the ch_-prefixed legacy names
--   that had a rename (ch_parts_per_insert, ch_merge_amplification,
--   ch_insert_cpu_seconds_per_Mrows was never ch_-prefixed but the parts/merge ones
--   were) are folded into the pinned name so a series spans the cutover unbroken —
--   identical rewrite intent to v_kc_metric_trends.metric_canon.
--
-- ============================ THE MATCHED-DATASET RULE ========================
-- (contract §5 / §6 Amendment 2026-07-09d, enforcement concurred 2026-07-09e —
--  NORMATIVE). Cross-connector comparisons are valid ONLY on a MATCHED dataset:
--      matched  ⇔  equal (dataset, rows_expected)
--  where `dataset` is the runtime logical-shape key (§1.4, correctly 'hits' on both
--  pipelines today) and `rows_expected` is the volume-truth integrity metric (§2.1).
--  `dataset` alone cannot discriminate volume variants ('hits'@10M would "match"
--  'hits'@100M — TODAY'S ACTUAL STATE: Kafka 10,000,000 vs Spark 99,997,497), so the
--  bucket key is the PAIR (dataset, rows_expected). A comparison bucket is MATCHED
--  iff it contains ≥2 DISTINCT connectors at equal (dataset, rows_expected).
--
--  Implementation: each run carries its dataset + rows_expected; `comp_bucket` =
--  (dataset, rows_expected); a window counts distinct connectors per bucket, and
--  `matched_dataset` = (that count ≥ 2). The Tab-5 comparison charts DEFAULT-filter
--  matched_dataset = 1. A mismatched row MUST NEVER render as a comparable
--  efficiency number (§6) — it may appear only in the clearly-labelled UNMATCHED
--  context table (matched_dataset = 0), never on a shared comparison series.
--
--  Rows missing rows_expected are UNMATCHED by default (safe failure mode, §6):
--  rows_expected is left NULL, the bucket cannot reach a 2-connector match, so the
--  row falls to the context side.
--
--  HONEST-EMPTY TODAY: because Kafka runs 10M and Spark runs ~100M, NO bucket has
--  two connectors at equal (dataset, rows_expected) — so the matched side is
--  EMPTY. That is CORRECT and ACCEPTED (structure first; a Kafka 100M graduation is
--  a deliberate future instrument event that will populate it automatically — the
--  rule self-maintains, no view edit needed). Tab 5 renders gracefully empty with a
--  markdown note explaining WHY (see tab5_charts.json + DASHBOARD.md banner).
-- =============================================================================
--
-- ENVIRONMENT-CLASS CAVEAT (contract §6): environment_class is surfaced BESIDE
--   clickhouse_version on every row so each chart tooltip / the efficiency table can
--   show it, and the reader can see the classes differ (Spark = production, Kafka =
--   staging today). H/P ratio GATES are unaffected (both arms share the target), but
--   ABSOLUTE cross-connector numbers straddle a production/staging boundary — the
--   permanent markdown banner on the tab tells the reader. This view does not hide
--   or correct the difference; it exposes the columns so the caveat is data-backed.
--
-- EXCLUSIONS (verbatim intent from the sibling views): fixture rows for BOTH
--   spellings — Kafka's reserved '__verdict_fixture__' AND Spark's 'verdict_fixture'
--   (grounded: spark v_verdict_fixture_check.sql:60 WHERE r.connector =
--   'verdict_fixture'); failed runs excluded BY OUTCOME VALUE (never by absence);
--   empty pair_id excluded. flagged is CARRIED as a presentation column (integrity
--   mark), tested with the deployed IN ('1','true') predicate to match the deployed
--   Tab-1/Tab-2 datasets (see DASHBOARD.md "Deployed flagged predicate"); a flagged
--   run is shown-and-marked here, not silently dropped (this is descriptive history,
--   the verdict/exclusion truth stays on the per-connector Tab 1s).
--
-- SETTINGS join_use_nulls = 1 REQUIRED: an absent rows_expected / metric value must
--   surface as NULL (→ UNMATCHED / NO_DATA), not the Float64 default 0.
--
-- SOURCE: raw_connectors_load_testing.{runs,metrics} (ClickPipe DWH mirror of
--   perf.*, carries BOTH connectors). Zero rows without error when either connector
--   has no runs yet; matched side legitimately empty today (see above).
-- =============================================================================
CREATE OR REPLACE VIEW raw_connectors_load_testing.v_xconn AS
WITH
  -- Runs in scope: BOTH connectors, arm=head, tier=1, fixtures (both spellings) and
  -- failed-outcome runs excluded by value, empty pair_id excluded.
  runs_scoped AS
  (
    SELECT
      run_id,
      run_started_at,
      run_ended_at,
      git_sha,
      connector,
      multiIf(connector = 'kafka-connect', 'Kafka Connect',
              connector = 'spark',         'Spark',
              connector)                                        AS connector_label,
      connector_version,
      clickhouse_version,
      if(mapContains(runtime, 'arm'),  runtime['arm'],  'head') AS arm,
      if(mapContains(runtime, 'tier'), runtime['tier'], '1')    AS tier,
      runtime['pair_id']                                        AS pair_id,
      runtime['dataset']                                        AS dataset,
      (mapContains(runtime, 'flagged') AND runtime['flagged'] IN ('1','true')) AS flagged,
      runtime['flag_reason']                                    AS flag_reason,
      runtime['environment_class']                              AS environment_class,
      runtime['target_region']                                  AS target_region,
      runtime['compute_region']                                 AS compute_region
    FROM raw_connectors_load_testing.runs
    WHERE connector IN ('spark', 'kafka-connect')
      AND connector NOT IN ('__verdict_fixture__', 'verdict_fixture')
      AND NOT (mapContains(runtime, 'outcome') AND runtime['outcome'] = 'failed')
      AND pair_id != ''
      AND if(mapContains(runtime, 'arm'),  runtime['arm'],  'head') = 'head'
      AND if(mapContains(runtime, 'tier'), runtime['tier'], '1')    = '1'
  ),

  -- One value per (run, metric). Latest by recorded_at (argMax) for determinism.
  metric_vals AS
  (
    SELECT run_id, metric_name, any(unit) AS unit, argMax(value, recorded_at) AS value
    FROM raw_connectors_load_testing.metrics
    GROUP BY run_id, metric_name
  ),

  -- CANONICALISE metric names for the cross-connector series.
  --   1. §2.2 headline aliasing: fold the four connector-specific throughput
  --      spellings into ONE 'rows_per_sec' series (view-only alias; the stored name
  --      is never rows_per_sec).
  --   2. §7 legacy fold: ch_-prefixed renamed names → pinned name.
  -- Preference (argMax on is_pinned) picks the pinned spelling if both a legacy and
  -- pinned row exist for the same run.
  metric_canon AS
  (
    SELECT
      run_id,
      canon_name AS metric_name,
      argMax(value, is_pinned) AS value,
      argMax(unit,  is_pinned) AS unit
    FROM (
      SELECT run_id, value, unit,
        multiIf(
          -- §2.2 headline aliases → unified series ------------------------------
          metric_name IN ('throughput_rows_per_sec','drain_rows_per_sec',
                          'null_rows_per_sec','null_drain_rows_per_sec'), 'rows_per_sec',
          -- §7 legacy → pinned folds -------------------------------------------
          metric_name = 'ch_parts_per_insert',               'parts_per_insert',
          metric_name = 'ch_merge_amplification',             'merge_amplification',
          metric_name = 'ch_insert_cpu_seconds_per_Mrows',    'ch_insert_cpu_seconds_per_Mrows',
          metric_name
        )                                                          AS canon_name,
        -- is_pinned: prefer the row already carrying the pinned/aliased spelling.
        (metric_name IN (
          'rows_per_sec','parts_per_insert','merge_amplification',
          'ch_insert_cpu_seconds_per_Mrows'
        ))                                                         AS is_pinned
      FROM metric_vals
    )
    GROUP BY run_id, canon_name
  ),

  -- rows_expected (volume truth, §2.1) per run — the matched-bucket denominator.
  -- LEFT-joined below so a run missing it surfaces NULL (→ UNMATCHED, safe default).
  rows_expected AS
  (
    SELECT run_id, argMax(value, recorded_at) AS rows_expected
    FROM raw_connectors_load_testing.metrics
    WHERE metric_name = 'rows_expected'
    GROUP BY run_id
  ),

  -- Per-run rows enriched with the comparison bucket + rows_expected. One row per
  -- (run × canonical metric).
  enriched AS
  (
    SELECT
      r.pair_id                    AS pair_id,
      r.run_id                     AS run_id,
      r.connector                  AS connector,
      r.connector_label            AS connector_label,
      r.arm                        AS arm,
      r.tier                       AS tier,
      r.dataset                    AS dataset,
      re.rows_expected             AS rows_expected,
      r.run_started_at             AS run_started_at,
      r.run_ended_at               AS run_ended_at,
      r.git_sha                    AS git_sha,
      r.connector_version          AS connector_version,
      r.clickhouse_version         AS clickhouse_version,
      r.environment_class          AS environment_class,
      r.target_region              AS target_region,
      r.compute_region             AS compute_region,
      r.flagged                    AS flagged,
      r.flag_reason                AS flag_reason,
      mc.metric_name               AS metric_name,
      mc.value                     AS value,
      mc.unit                      AS unit
    FROM runs_scoped AS r
    INNER JOIN metric_canon AS mc ON mc.run_id = r.run_id
    LEFT  JOIN rows_expected AS re ON re.run_id = r.run_id
  )

SELECT
  e.pair_id                                                AS pair_id,
  e.run_id                                                 AS run_id,
  e.connector                                              AS connector,
  e.connector_label                                        AS connector_label,
  e.arm                                                    AS arm,
  e.tier                                                   AS tier,
  e.dataset                                                AS dataset,
  e.rows_expected                                          AS rows_expected,
  e.metric_name                                            AS metric_name,
  e.value                                                  AS value,
  e.unit                                                   AS unit,
  e.run_started_at                                         AS run_started_at,
  e.run_ended_at                                           AS run_ended_at,
  e.git_sha                                                AS git_sha,
  e.connector_version                                      AS connector_version,
  -- §6 covariates surfaced BESIDE each other on every row (tooltip/table columns).
  e.clickhouse_version                                     AS clickhouse_version,
  e.environment_class                                      AS environment_class,
  e.target_region                                          AS target_region,
  e.compute_region                                         AS compute_region,
  e.flagged                                                AS flagged,
  e.flag_reason                                            AS flag_reason,

  -- ---------------- MATCHED-DATASET RULE (§5/§6, normative) -------------------
  -- comp_bucket = the (dataset, rows_expected) pair. NULL rows_expected → a bucket
  -- that can never reach 2 connectors (safe-default UNMATCHED).
  concat(e.dataset, '@', ifNull(toString(toUInt64(e.rows_expected)), 'unknown')) AS comp_bucket,
  -- distinct connectors present in this bucket (across ALL metrics/rows of the view).
  uniqExact(e.connector) OVER (PARTITION BY e.dataset, e.rows_expected)        AS bucket_connectors,
  -- matched_dataset: the bucket compares ≥2 distinct connectors at equal
  -- (dataset, rows_expected). rows_expected NULL → NOT matched.
  (isNotNull(e.rows_expected)
   AND uniqExact(e.connector) OVER (PARTITION BY e.dataset, e.rows_expected) >= 2) AS matched_dataset,

  -- ---------------- presentation time axis (mirrors the sibling views) --------
  if(
    extract(e.pair_id, '^(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2})Z') = '',
    NULL,
    parseDateTimeBestEffortOrNull(
      replaceRegexpOne(
        extract(e.pair_id, '^(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2})Z'),
        'T(\\d{2})-(\\d{2})-(\\d{2})$', 'T\\1:\\2:\\3'
      )
    )
  )                                                        AS pair_ts,
  -- pair_seq: dense rank within (connector, metric_name) by pair_ts DESC (1 = newest)
  -- so each connector's own trend numbers its pairs 1..N (used by the 30d window).
  dense_rank() OVER (PARTITION BY e.connector, e.metric_name ORDER BY pair_ts DESC) AS pair_seq
FROM enriched AS e
ORDER BY metric_name, connector, run_started_at
-- REQUIRED: absent rows_expected / metric must be NULL, not 0 (see header).
SETTINGS join_use_nulls = 1;
