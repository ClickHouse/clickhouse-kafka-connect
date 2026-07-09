-- v_kc_pair_ratios.sql — Benchmark v2 Kafka dashboard, Tab 1 pair-ratio verdicts.
--
-- WHAT THIS IS
--   The per-(pair, tier, gated-metric) H/P self-join that produces the nightly
--   regression verdict for the Kafka Connect sink benchmark (plan §8 Tab 1,
--   contract §3 verdict semantics). Issue #33's Superset virtual dataset consumes
--   this file VERBATIM (see benchmarks/e2e/dashboard/README.md).
--
-- WHY IT EXISTS AS A BUILD-TIME ACCEPTANCE ARTIFACT
--   The Spark dashboard shipped verdict logic that mislabeled 0/0 (no-data) and
--   good-direction excursions as REGRESSION. Per the principal mandate, any
--   verdict-emitting artifact requires fixture-based acceptance before it ships.
--   The verdict map below is exercised end-to-end by
--   benchmarks/e2e/dashboard/test_verdicts.sh against
--   benchmarks/e2e/dashboard/sql/fixture_verdict_truth_table.sql.
--
-- DATA MODEL (contract §1, §2; DDL benchmarks/e2e/sql/perf/02,03)
--   perf.runs   : ONE row per (arm, tier). runtime Map(String,String) carries
--                 arm ('head'|'pinned', absent => 'head'), tier ('0'|'1', absent
--                 => '1'), pair_id, outcome ('success'|'failed', absent =>
--                 'success'), flagged ('1' iff a validity guard tripped).
--   perf.metrics: tall/narrow (run_id, metric_name, unit, value, recorded_at).
--                 NO runtime map — inherits (arm,tier) solely via run_id join.
--
-- VERDICT MAP (contract §3; spec pinned by the principal). Per gated metric:
--   ratio NULL or pinned=0 (0-denominator)      -> NO_DATA
--   either arm's run flagged (runtime flagged=1) -> FLAGGED (excluded from bands)
--   ratio excursion beyond band, GOOD direction  -> IMPROVEMENT
--   ratio excursion beyond band, BAD direction   -> REGRESSION
--   otherwise                                    -> OK
-- ch_avg_rows_per_insert is NOT a ratio band: it is the >=50k batching contract
--   (plan §6 milestone), modeled as its own verdict rule on the HEAD value.
--
-- DIRECTION SPELLINGS (ASSUMPTION — pending contract amendment):
--   direction values are the literal strings 'higher_better' and 'lower_better'.
--   The principal flagged these as refinable by a pending contract amendment; they
--   are centralized in the `gated` CTE below so an amendment is a one-line change.
--
-- CALIBRATION HOLD (plan §8 bands "recalibrated from the first ~20 pairs"):
--   until >= 20 unflagged, comparable pairs exist for a (tier, metric), the verdict
--   is PROVISIONAL and band alerts are suppressed. Encoded as columns:
--     provisional     UInt8  (1 = still calibrating)
--     alerts_enabled  UInt8  (1 = NOT provisional; Tab 1 alert queries gate on this)
--
-- FIXTURE EXCLUSION GUARD (contract §3; acceptance requirement):
--   the WHERE clause excludes connector='__verdict_fixture__' so the synthetic
--   truth-table rows NEVER mix into production verdicts. test_verdicts.sh asserts
--   this guard is present and effective.
--
-- FAILED-RUN EXCLUSION (contract §1.3, §3):
--   runs are excluded by outcome VALUE (runtime['outcome']='failed'), never by
--   absence — legacy rows have no 'outcome' key and are 'success' by default.

CREATE OR REPLACE VIEW perf.v_kc_pair_ratios AS
WITH
  -- The gated-metric registry: metric name, direction, and band fraction.
  -- Bands: Tier 0 +-3% (0.03), Tier 1 +-5% (0.05) per plan §8 / contract §3.
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

  -- Runs in scope: this connector's benchmark, fixture excluded, failed excluded
  -- BY OUTCOME VALUE (not by key absence — legacy rows default to 'success').
  runs_scoped AS
  (
    SELECT
      run_id,
      connector,
      if(mapContains(runtime, 'arm'),  runtime['arm'],  'head') AS arm,
      if(mapContains(runtime, 'tier'), runtime['tier'], '1')    AS tier,
      runtime['pair_id']                                        AS pair_id,
      (mapContains(runtime, 'flagged') AND runtime['flagged'] = '1') AS flagged
    FROM perf.runs
    WHERE connector != '__verdict_fixture__'
      AND NOT (mapContains(runtime, 'outcome') AND runtime['outcome'] = 'failed')
      AND pair_id != ''
  ),

  -- One value per (run, metric). If a metric were emitted twice for a run we take
  -- the latest by recorded_at (argMax) to stay deterministic.
  metric_vals AS
  (
    SELECT run_id, metric_name, argMax(value, recorded_at) AS value
    FROM perf.metrics
    GROUP BY run_id, metric_name
  ),

  -- Head arm per (pair, tier, gated metric) with its value (NULL if the metric row
  -- is absent for that run).
  head_side AS
  (
    SELECT r.pair_id AS pair_id, r.tier AS tier, g.metric_name AS metric_name,
           g.direction AS direction, g.band AS band, g.is_threshold AS is_threshold,
           r.flagged AS head_flagged, mv.value AS head_value
    FROM runs_scoped AS r
    CROSS JOIN gated AS g
    LEFT JOIN metric_vals AS mv ON mv.run_id = r.run_id AND mv.metric_name = g.metric_name
    WHERE r.arm = 'head' AND r.tier = g.tier
  ),

  pinned_side AS
  (
    SELECT r.pair_id AS pair_id, r.tier AS tier, g.metric_name AS metric_name,
           r.flagged AS pinned_flagged, mv.value AS pinned_value
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

  -- VERDICT (precedence exactly per contract §3 verdict map):
  --   NO_DATA -> FLAGGED -> (threshold rule | band rule) -> OK
  multiIf(
    -- ch_avg_rows_per_insert threshold rule (>=50k). NO_DATA if head missing;
    -- FLAGGED wins over the threshold; else OK/REGRESSION on the 50k contract.
    is_threshold = 1,
      multiIf(
        isNull(head_value), 'NO_DATA',
        flagged,            'FLAGGED',
        head_value >= 50000, 'OK',
                             'REGRESSION'
      ),
    -- ratio-band gated metrics
    isNull(ratio), 'NO_DATA',
    flagged,       'FLAGGED',
    -- excursion beyond band?
    (direction = 'higher_better' AND ratio > 1 + band), 'IMPROVEMENT',
    (direction = 'higher_better' AND ratio < 1 - band), 'REGRESSION',
    (direction = 'lower_better'  AND ratio < 1 - band), 'IMPROVEMENT',
    (direction = 'lower_better'  AND ratio > 1 + band), 'REGRESSION',
    'OK'
  ) AS verdict,

  -- CALIBRATION HOLD: count of unflagged, comparable (non-NO_DATA) pairs seen so
  -- far for this (tier, metric). While < 20 the verdict is provisional and alerts
  -- are suppressed. Threshold-rule comparability = head_value present; ratio-rule
  -- comparability = ratio present.
  countIf(
    (NOT flagged)
    AND if(is_threshold = 1, isNotNull(head_value), isNotNull(ratio))
  ) OVER (PARTITION BY tier, metric_name) AS comparable_pairs,

  (comparable_pairs < 20) AS provisional,
  (NOT provisional)       AS alerts_enabled

FROM pairs
ORDER BY pair_id, tier, metric_name
-- join_use_nulls=1 is REQUIRED for correctness: a metric that is ABSENT for a run
-- must surface as NULL head_value/pinned_value (=> NO_DATA), not as the Float64
-- default 0 (which would false-verdict a missing metric as REGRESSION / 0-denom).
SETTINGS join_use_nulls = 1;
