-- v_kc_pair_ratios.sql — Benchmark v2 Kafka dashboard, Tab 1 pair-ratio verdicts.
--
-- WHAT THIS IS
--   The per-(pair, tier, gated-metric) H/P self-join that produces the nightly
--   regression verdict for the Kafka Connect sink benchmark (plan §8 Tab 1,
--   contract §3 verdict semantics). Issue #33's Superset virtual dataset is the
--   DWH twin of this file (benchmarks/e2e/dashboard/superset/v_kc_pair_ratios.sql);
--   the verdict logic below is the canonical copy.
--
-- CONTRACT LEVEL: Amendment 2026-07-09b (CONFORMED — #33 Stage A2, manager ruling
--   option B). Supersedes the original flat-band map (commit 3b7cc59, T0 ±3% /
--   T1 ±5%, merge_amplification gated, ch_avg_rows_per_insert >=50k threshold).
--   The amended semantics implemented here (docs/benchmark-v2-contract.md §3):
--     * CALIBRATED per-metric bands, same on both tiers (2x measured noise floor):
--         null_drain_rows_per_sec / drain_rows_per_sec   ±8.5%  (=> 0.085)
--         connect_cpu_seconds_per_Mrows                  ±6%    (=> 0.06)
--       (throughput_rows_per_sec ±9% and serialize_seconds_per_Mrows ±8.5% are
--        Spark-side spellings; the Kafka pipeline does not emit them and per
--        Amendment 2026-07-09f correctly omits them — see the REGISTRY NOTE.)
--     * parts_per_insert is a binary TRIPWIRE on the HEAD arm's ABSOLUTE value:
--       exactly 1.0 => OK; ANY deviation => TRIPWIRE (structural invariant break,
--       alerts regardless of calibration); NULL/absent => NO_DATA as usual.
--     * merge_amplification is DEMOTED TO WATCH-ONLY: not gated, never a verdict
--       row here (it stays a reported covariate — v_kc_runs / Tab 2). The fixture
--       asserts this view emits ZERO merge_amplification rows.
--     * ch_avg_rows_per_insert is DEGATED: the amended gate composition (which
--       "replaces the prior flat-band gated set") does not include it; it is a
--       plain §2.1 covariate now. The fixture asserts zero rows for it too.
--     * PRECEDENCE (PINNED): FAIL (integrity; excluded upstream by outcome) >
--       FLAG > NO_DATA / TRIPWIRE / IMPROVEMENT / REGRESSION / OK. NOTE: this
--       REVERSES the original map's NO_DATA-before-FLAGGED ordering — a flagged
--       pair is FLAGGED even when its ratio is NULL/0-denominator or its tripwire
--       is armed. The fixture covers this cell explicitly.
--     * ALERT RULE: during calibration only integrity failures and the parts
--       TRIPWIRE alert. Encoded as `alert_now` so alert queries share the view.
--   Direction spellings 'higher_better'/'lower_better' are now CONTRACT-PINNED
--   (§3 direction table) — the former "pending amendment" assumption is resolved.
--
-- REGISTRY NOTE — connect_cpu_seconds_per_Mrows band (RATIFIED — Amendment
--   2026-07-09f, contract re-vendored at bd249f2): the pinned band table now
--   names the Kafka client-cpu spelling `connect_cpu_seconds_per_Mrows` in the
--   cpu-per-Mrows family row (±6%), and the Tier-0 gate composition marks
--   `serialize_seconds_per_Mrows` as Spark-specific ("a pipeline gates only the
--   metrics it emits") — both exactly as this registry implements. The Stage-A2
--   assumption note is resolved; no code change was needed.
--
-- WHY IT EXISTS AS A BUILD-TIME ACCEPTANCE ARTIFACT
--   Per the principal mandate, any verdict-emitting artifact requires fixture-
--   based acceptance before it ships. The map below is exercised end-to-end by
--   benchmarks/e2e/dashboard/test_verdicts.sh against
--   benchmarks/e2e/dashboard/sql/fixture_verdict_truth_table.sql (extended for
--   the amendment: band edges in both directions, tripwire cells, watch-only and
--   degated exclusions, FLAG>NO_DATA precedence).
--
-- DATA MODEL (contract §1, §2; DDL benchmarks/e2e/sql/perf/02,03)
--   perf.runs   : ONE row per (arm, tier). runtime Map(String,String) carries
--                 arm ('head'|'pinned', absent => 'head'), tier ('0'|'1', absent
--                 => '1'), pair_id, outcome ('success'|'failed', absent =>
--                 'success'), flagged ('1' iff a validity guard tripped).
--   perf.metrics: tall/narrow (run_id, metric_name, unit, value, recorded_at).
--                 NO runtime map — inherits (arm,tier) solely via run_id join.
--
-- CALIBRATION HOLD (contract §3): until >= 20 unflagged, comparable pairs exist
--   for a (tier, metric), the verdict is PROVISIONAL ("calibrating, n=X/20") and
--   band alerts are suppressed. Columns: provisional, alerts_enabled, alert_now
--   (alert_now fires for TRIPWIRE even while provisional — contract §3).
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
  -- The gated-metric registry (contract §3 gate composition, Amendment
  -- 2026-07-09b, Kafka spellings — see the header REGISTRY NOTE):
  --   Tier 0 gate: null_drain_rows_per_sec (banded ±8.5%, higher_better)
  --                connect_cpu_seconds_per_Mrows (banded ±6%, lower_better)
  --   Tier 1 gate: drain_rows_per_sec (banded ±8.5%, higher_better)
  --                parts_per_insert (TRIPWIRE — direction/band unused)
  -- merge_amplification (watch-only) and ch_avg_rows_per_insert (degated
  -- covariate) are DELIBERATELY absent: no registry row => no verdict row.
  gated AS
  (
    SELECT metric_name, tier, direction, band, is_tripwire
    FROM values(
      'metric_name String, tier String, direction String, band Float64, is_tripwire UInt8',
      ('null_drain_rows_per_sec',       '0', 'higher_better', 0.085, 0),
      ('connect_cpu_seconds_per_Mrows', '0', 'lower_better',  0.06,  0),
      ('drain_rows_per_sec',            '1', 'higher_better', 0.085, 0),
      ('parts_per_insert',              '1', 'tripwire',      0.0,   1)
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
           g.direction AS direction, g.band AS band, g.is_tripwire AS is_tripwire,
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
      h.is_tripwire  AS is_tripwire,
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
  is_tripwire,
  head_value,
  pinned_value,
  ratio,
  flagged,

  -- VERDICT (contract §3, Amendment 2026-07-09b; precedence PINNED):
  --   FLAG > NO_DATA / TRIPWIRE / IMPROVEMENT / REGRESSION / OK.
  --   (FAIL > FLAG is honoured upstream: failed-outcome runs never reach here.)
  multiIf(
    -- FLAG overrides everything below, including an armed TRIPWIRE and a
    -- NULL/0-denominator ratio (contract §3 precedence line).
    flagged, 'FLAGGED',
    -- TRIPWIRE metric (parts_per_insert): binary on the HEAD arm's ABSOLUTE
    -- value, NO ratio/band comparison. NULL/absent head => NO_DATA as usual.
    is_tripwire = 1,
      multiIf(
        isNull(head_value),  'NO_DATA',
        head_value = 1.0,    'OK',
                             'TRIPWIRE'
      ),
    -- banded metrics: NO_DATA on NULL ratio (either side missing / pinned=0)
    isNull(ratio), 'NO_DATA',
    -- excursion beyond the calibrated band?
    (direction = 'higher_better' AND ratio > 1 + band), 'IMPROVEMENT',
    (direction = 'higher_better' AND ratio < 1 - band), 'REGRESSION',
    (direction = 'lower_better'  AND ratio < 1 - band), 'IMPROVEMENT',
    (direction = 'lower_better'  AND ratio > 1 + band), 'REGRESSION',
    'OK'
  ) AS verdict,

  -- CALIBRATION HOLD: count of unflagged, comparable (non-NO_DATA) pairs seen so
  -- far for this (tier, metric). While < 20 the verdict is provisional and BAND
  -- alerts are suppressed. Tripwire comparability = head_value present; banded
  -- comparability = ratio present.
  countIf(
    (NOT flagged)
    AND if(is_tripwire = 1, isNotNull(head_value), isNotNull(ratio))
  ) OVER (PARTITION BY tier, metric_name) AS comparable_pairs,

  (comparable_pairs < 20) AS provisional,
  (NOT provisional)       AS alerts_enabled,

  -- ALERT RULE (contract §3): a TRIPWIRE alerts REGARDLESS of calibration state
  -- (structural invariant break, not a noise excursion); band REGRESSIONs alert
  -- only once calibration completes. Alert queries MUST fire on alert_now = 1
  -- (never re-derive this predicate — that is how dashboard and alerts drift).
  ((verdict = 'TRIPWIRE') OR (verdict = 'REGRESSION' AND alerts_enabled)) AS alert_now

FROM pairs
ORDER BY pair_id, tier, metric_name
-- join_use_nulls=1 is REQUIRED for correctness: a metric that is ABSENT for a run
-- must surface as NULL head_value/pinned_value (=> NO_DATA), not as the Float64
-- default 0 (which would false-verdict a missing metric as REGRESSION / 0-denom).
SETTINGS join_use_nulls = 1;
