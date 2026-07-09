-- =============================================================================
-- v_kc_pair_ratios  —  DWH Superset virtual dataset (Benchmark v2 Kafka, Tab 1)
-- =============================================================================
-- WHAT THIS IS
--   The DWH-adapted twin of benchmarks/e2e/dashboard/sql/v_kc_pair_ratios.sql
--   (the fixture-ACCEPTED verdict map — 20/20 via test_verdicts.sh). This file is
--   the SQL body of the Superset virtual dataset that Tab 1's gated-metrics table,
--   verdict tiles, ratio-trend lines and excursion log all read.
--
-- ADAPTATION DELTA vs the accepted sql/v_kc_pair_ratios.sql (issue #33, Stage A)
--   The verdict semantics are PRESERVED BYTE-FOR-BYTE (the `gated` registry, the
--   NULL-safe ratio, the verdict multiIf precedence, and the calibration-hold
--   window are copied verbatim so the 20/20 acceptance still describes THIS SQL —
--   see verify_verdict_dwh.sh, which re-runs the fixture against exactly this body
--   with only the table names swapped back to perf.*). The DWH twin differs ONLY
--   by:
--     1. SOURCE TABLES: perf.runs/perf.metrics -> the ClickPipe DWH mirror
--        raw_connectors_load_testing.runs / .metrics (per the Spark v2 datasets;
--        see benchmarks/dashboard/v2/v_runs_enriched.sql header "DWH mirror").
--     2. CONNECTOR SCOPE: adds connector = 'kafka-connect' so this dataset is
--        Kafka-only (the DWH mirror carries BOTH connectors; matched-dataset rule,
--        contract §5 Amendment 2026-07-09d — scoped by connector VALUE, never by
--        instance, so Tab 5 cross-connector (#36) is not precluded).
--     3. PRESENTATION COLUMNS for the charts (NOT consumed by the verdict): a
--        NULL-safe pair_ts (parsed from the pair_id timestamp prefix, exactly as
--        the Spark v_pair_ratios does) and pair_seq (dense_rank within tier by
--        pair_ts DESC) so the two ratio-trend lines and the "latest pair" table
--        can scope by time/recency; and flag_reason (pipe-joined tokens) carried
--        for the excursion+flagged log. These are additive SELECT columns; they do
--        not touch the verdict expression.
--
-- ⚠ CONTRACT-DRIFT NOTE (raised to the manager for a Stage-B ruling — do NOT
--   silently "fix" here). This verdict map encodes the OLD contract that was in
--   force when the fixture was accepted (commit 3b7cc59): flat bands (T0 ±3% /
--   T1 ±5%), null_drain higher_better + parts_per_insert lower_better as RATIO
--   bands, and ch_avg_rows_per_insert as a >=50k threshold. Contract §3 was since
--   re-vendored (commits 22ed529 / f2c008f / 85c5e73, Amendment 2026-07-09b) to
--   CALIBRATED per-metric bands (throughput ±9%, drain/null ±8.5%, cpu ±6%,
--   serialize ±8.5%, same on both tiers), merge_amplification DEMOTED to
--   watch-only, and parts_per_insert as a BINARY TRIPWIRE (head==1.0 => OK, else
--   TRIPWIRE) — see benchmarks/dashboard/v2/v_verdict_fixture_check.sql on the
--   Spark side for the current-contract map. Task #33 instructs "PRESERVE the
--   verdict semantics exactly (it passed 20/20)", so Stage A ships the accepted
--   bytes UNCHANGED; the amendment requires a NEW fixture + re-acceptance before a
--   contract-current view ships. Which map goes live on the dashboard is a
--   principal decision, flagged in the Stage-A report. This file changes in ONE
--   place (the `gated` CTE) if the ruling is "adopt the amendment".
--
-- CALIBRATION HOLD (unchanged): provisional while comparable_pairs < 20; band
--   alerts suppressed while provisional. Kafka n=1 today (only pair-2 is a clean
--   calibration point; pair-1 is flagged instrument_resize and excluded), so every
--   emitted verdict is provisional ("calibrating, n=X/20").
--
-- FIXTURE / FAILED-RUN GUARDS (unchanged, verbatim): connector != the reserved
--   fixture connector; failed runs excluded BY OUTCOME VALUE (never by absence).
--   NOTE the fixture connector spelling: the accepted view and its fixture use
--   '__verdict_fixture__'; that literal is preserved here so the same guard the
--   acceptance test asserts remains effective on the DWH.
--
-- SETTINGS join_use_nulls = 1 is REQUIRED (an absent metric must surface as NULL
--   => NO_DATA, not the Float64 default 0 => false REGRESSION / 0-denominator).
-- =============================================================================
CREATE OR REPLACE VIEW raw_connectors_load_testing.v_kc_pair_ratios AS
WITH
  -- ============ VERBATIM from the accepted verdict map (do not edit) ============
  -- The gated-metric registry: metric name, direction, and band fraction.
  -- Bands: Tier 0 +-3% (0.03), Tier 1 +-5% (0.05) per the accepted map.
  -- ch_avg_rows_per_insert carries a sentinel band (unused; threshold rule instead).
  gated AS
  (
    SELECT metric_name, tier, direction, band, is_threshold
    FROM values(
      'metric_name String, tier String, direction String, band Float64, is_threshold UInt8',
      -- Tier 0 gates (+-3%)
      ('null_drain_rows_per_sec',           '0', 'higher_better', 0.03, 0),
      ('connect_cpu_seconds_per_Mrows',     '0', 'lower_better',  0.03, 0),
      -- Tier 1 gates (+-5%)
      ('drain_rows_per_sec',                '1', 'higher_better', 0.05, 0),
      ('parts_per_insert',                  '1', 'lower_better',  0.05, 0),
      ('merge_amplification',               '1', 'lower_better',  0.05, 0),
      -- Tier 1 batching contract: threshold check (>=50k), not a ratio band.
      ('ch_avg_rows_per_insert',            '1', 'higher_better', 0.00, 1)
    )
  ),
  -- ==============================================================================

  -- Runs in scope: THIS connector's benchmark (DWH mirror carries both), fixture
  -- excluded, failed excluded BY OUTCOME VALUE (not by key absence — legacy rows
  -- default to 'success'). Only the source table and the connector predicate
  -- differ from the accepted view; the projected columns are identical, plus
  -- flag_reason carried for the log.
  runs_scoped AS
  (
    SELECT
      run_id,
      connector,
      if(mapContains(runtime, 'arm'),  runtime['arm'],  'head') AS arm,
      if(mapContains(runtime, 'tier'), runtime['tier'], '1')    AS tier,
      runtime['pair_id']                                        AS pair_id,
      (mapContains(runtime, 'flagged') AND runtime['flagged'] = '1') AS flagged,
      runtime['flag_reason']                                    AS flag_reason
    FROM raw_connectors_load_testing.runs
    WHERE connector = 'kafka-connect'
      AND connector != '__verdict_fixture__'
      AND NOT (mapContains(runtime, 'outcome') AND runtime['outcome'] = 'failed')
      AND pair_id != ''
  ),

  -- One value per (run, metric). Latest by recorded_at (argMax) for determinism.
  metric_vals AS
  (
    SELECT run_id, metric_name, argMax(value, recorded_at) AS value
    FROM raw_connectors_load_testing.metrics
    GROUP BY run_id, metric_name
  ),

  head_side AS
  (
    SELECT r.pair_id AS pair_id, r.tier AS tier, g.metric_name AS metric_name,
           g.direction AS direction, g.band AS band, g.is_threshold AS is_threshold,
           r.flagged AS head_flagged, r.flag_reason AS head_flag_reason,
           mv.value AS head_value
    FROM runs_scoped AS r
    CROSS JOIN gated AS g
    LEFT JOIN metric_vals AS mv ON mv.run_id = r.run_id AND mv.metric_name = g.metric_name
    WHERE r.arm = 'head' AND r.tier = g.tier
  ),

  pinned_side AS
  (
    SELECT r.pair_id AS pair_id, r.tier AS tier, g.metric_name AS metric_name,
           r.flagged AS pinned_flagged, r.flag_reason AS pinned_flag_reason,
           mv.value AS pinned_value
    FROM runs_scoped AS r
    CROSS JOIN gated AS g
    LEFT JOIN metric_vals AS mv ON mv.run_id = r.run_id AND mv.metric_name = g.metric_name
    WHERE r.arm = 'pinned' AND r.tier = g.tier
  ),

  pairs AS
  (
    SELECT
      h.pair_id      AS pair_id,
      h.tier         AS tier,
      h.metric_name  AS metric_name,
      h.direction    AS direction,
      h.band         AS band,
      h.is_threshold AS is_threshold,
      h.head_value   AS head_value,
      p.pinned_value AS pinned_value,
      (h.head_flagged OR p.pinned_flagged) AS flagged,
      -- pipe-joined non-empty flag_reason(s) across the two arms (for the log).
      arrayStringConcat(
        arrayFilter(x -> x != '', [h.head_flag_reason, p.pinned_flag_reason]), '|'
      )                                    AS flag_reason,
      -- NULL-safe ratio: NULL if either side missing OR pinned = 0 (0-denominator).
      if(isNull(h.head_value) OR isNull(p.pinned_value) OR p.pinned_value = 0,
         NULL,
         h.head_value / p.pinned_value) AS ratio
    FROM head_side AS h
    INNER JOIN pinned_side AS p
      ON p.pair_id = h.pair_id AND p.tier = h.tier AND p.metric_name = h.metric_name
  )

SELECT
  pair_id,
  tier,
  metric_name,
  direction,
  band,
  head_value,
  pinned_value,
  ratio,
  flagged,
  flag_reason,

  -- ================ VERDICT — VERBATIM from the accepted map ==================
  -- Precedence exactly per the accepted verdict map:
  --   NO_DATA -> FLAGGED -> (threshold rule | band rule) -> OK
  multiIf(
    is_threshold = 1,
      multiIf(
        isNull(head_value), 'NO_DATA',
        flagged,            'FLAGGED',
        head_value >= 50000, 'OK',
                             'REGRESSION'
      ),
    isNull(ratio), 'NO_DATA',
    flagged,       'FLAGGED',
    (direction = 'higher_better' AND ratio > 1 + band), 'IMPROVEMENT',
    (direction = 'higher_better' AND ratio < 1 - band), 'REGRESSION',
    (direction = 'lower_better'  AND ratio < 1 - band), 'IMPROVEMENT',
    (direction = 'lower_better'  AND ratio > 1 + band), 'REGRESSION',
    'OK'
  ) AS verdict,

  -- CALIBRATION HOLD — VERBATIM: count of unflagged, comparable (non-NO_DATA)
  -- pairs seen so far for this (tier, metric). While < 20 the verdict is
  -- provisional and alerts are suppressed.
  countIf(
    (NOT flagged)
    AND if(is_threshold = 1, isNotNull(head_value), isNotNull(ratio))
  ) OVER (PARTITION BY tier, metric_name) AS comparable_pairs,

  (comparable_pairs < 20) AS provisional,
  (NOT provisional)       AS alerts_enabled,
  -- ===========================================================================

  -- ---- PRESENTATION-ONLY columns (NOT consumed by the verdict) --------------
  -- pair_ts: pair start time parsed from the pair_id 'YYYY-MM-DDTHH-MM-SSZ' prefix
  --   exactly as the Spark v_pair_ratios derives it. NULL-safe: the anchored regex
  --   only matches a real timestamp prefix; any other id yields '' -> NULL without
  --   erroring (parseDateTimeBestEffortOrNull never throws).
  if(
    extract(pair_id, '^(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2})Z') = '',
    NULL,
    parseDateTimeBestEffortOrNull(
      replaceRegexpOne(
        extract(pair_id, '^(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2})Z'),
        'T(\\d{2})-(\\d{2})-(\\d{2})$', 'T\\1:\\2:\\3'
      )
    )
  )                                        AS pair_ts,
  -- pair_seq: dense rank within tier by pair_ts DESC (1 = newest pair). dense_rank
  -- so every metric row of a pair shares one sequence number (tiles filter
  -- pair_seq = 1 for latest-pair, pair_seq <= 20 for the trailing window).
  dense_rank() OVER (PARTITION BY tier ORDER BY pair_ts DESC) AS pair_seq

FROM pairs
ORDER BY pair_id, tier, metric_name
-- REQUIRED for correctness (see header).
SETTINGS join_use_nulls = 1;
