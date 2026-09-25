-- =============================================================================
-- v_kc_run_drill  —  DWH Superset virtual dataset (Benchmark v2 Kafka, Tab 4)
-- =============================================================================
-- WHAT THIS IS
--   The ARM-COMPARISON long-format view for Tab 4 (RUN DRILL): one row per
--   (pair_id, tier, metric_name) carrying the HEAD value, the pinned value, their
--   ratio and delta%, plus the pair's identity/flag/config context. Unlike
--   v_kc_pair_ratios (which is scoped to the FOUR gated/tripwire metrics and
--   carries verdict semantics), this view is UN-GATED: it pivots EVERY metric the
--   pair emitted so the drill's "ARM COMPARISON (table): all metrics | HEAD |
--   pinned | d%" panel (plan §8 Tab 4 sketch) shows the full picture, not just the
--   verdict-bearing subset. It deliberately carries NO verdict/band/alert column —
--   the drill is descriptive, not a gate (verdicts live on Tab 1 via
--   v_kc_pair_ratios, which stays the single source of alert truth).
--
-- WHY A NEW VIEW (not a reuse of v_kc_runs / v_kc_pair_ratios)
--   * v_kc_runs is one WIDE row per run (arm+tier); a Superset table cannot pivot
--     it to an all-metrics HEAD|pinned|delta grid without hard-coding every metric.
--   * v_kc_pair_ratios is the RIGHT shape (long, H vs P) but is intentionally
--     scoped to the 4 gated metrics + verdict registry — extending it to all
--     metrics would perturb the byte-locked verdict acceptance (verify_verdict_dwh
--     31/31). So this is a sibling long view: same H/P self-join idea, no registry,
--     no verdict, all metrics.
--   The Tab 4 BIG-NUMBER row (rows verified, drain_s, dup=0, guards OK, cost) and
--   the covariates/config panel are built by REUSING v_kc_runs (see tab4_charts.json
--   and DASHBOARD.md) — no new view for those. Only the all-metrics arm table and
--   the ch_inserts drill (v_kc_inserts_drill / v_kc_drain_curve) are new.
--
-- CONTRACT GROUNDING
--   §1.1 runtime coalesce defaults (arm->'head', tier->'1', outcome->'success').
--   §1.3 flagged/flag_reason; §1.4 config keys echoed for the covariate panel.
--   §2 metric spellings (pinned name preferred; legacy ch_-prefixed coalesced per
--     §7 for the names that had a cutover — parts_per_insert, merge_amplification,
--     connections_per_insert). Absent metric => NULL (max(if(...)) NEVER maxIf).
--   §2.1 run_cost_usd two-arm attribution: the FULL pair cost is charged once on
--     one arm's row; the other arm omits it. So per-arm cost is undefined by
--     design — cost is surfaced as a per-PAIR big number in v_kc_runs, and this
--     arm table shows it as-is (one side NULL) without inventing a ratio.
--
-- MATCHED-DATASET RULE (contract §5, Amendment 2026-07-09d): scoped by connector
--   VALUE = 'kafka-connect', never by instance — does not preclude Tab 5 (#36).
--
-- DEPLOYMENT DEVIATION (REPLICATED — see DASHBOARD.md "Deployed flagged predicate"):
--   The DEPLOYED Tab-1 datasets on the DWH test the flag as
--   runtime['flagged'] IN ('1','true') (the harness has emitted both spellings),
--   whereas the repo Tab-1 SQL tests = '1'. Tab 4 is built to sit beside the
--   deployed assets, so this view REPLICATES the deployed predicate IN ('1','true')
--   for consistency. (v_kc_pair_ratios still owns verdict truth; here `flagged` is
--   presentation/exclusion context only.)
--
-- SANITY CHECK (pair-4 story): the head-t1 code advantage is expected to surface
--   here as drain_rows_per_sec ratio ~1.032 (HEAD ~3.2% faster than pinned) on the
--   t1 row of the latest clean pair — i.e. head_value/pinned_value ≈ 1.032,
--   delta_pct ≈ +3.2. The arm table is the place the operator reads that story
--   metric-by-metric; the verdict (OK within the ±8.5% band) is on Tab 1.
--
-- SOURCE: raw_connectors_load_testing.{runs,metrics} (ClickPipe DWH mirror). Zero
--   rows without error when no kafka pairs exist.
-- SETTINGS join_use_nulls = 1 REQUIRED: an absent arm/metric must surface as NULL
--   (=> NULL ratio), not the Float64 default 0 (false delta / 0-denominator).
-- =============================================================================
CREATE OR REPLACE VIEW raw_connectors_load_testing.v_kc_run_drill AS
WITH
  runs_scoped AS
  (
    SELECT
      run_id,
      if(mapContains(runtime, 'arm'),  runtime['arm'],  'head') AS arm,
      if(mapContains(runtime, 'tier'), runtime['tier'], '1')    AS tier,
      runtime['pair_id']                                        AS pair_id,
      (mapContains(runtime, 'flagged') AND runtime['flagged'] IN ('1','true')) AS flagged,
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

  -- Legacy->contract coalesce (§7 cutover 2026-07-07): fold the ch_-prefixed legacy
  -- name into the pinned name so a metric series spans the cutover. Done as a
  -- per-(run,metric) rewrite BEFORE the arm join, so the arm table shows one row
  -- per pinned metric name regardless of which spelling a given run emitted.
  metric_canon AS
  (
    SELECT
      run_id,
      multiIf(
        metric_name = 'ch_parts_per_insert',       'parts_per_insert',
        metric_name = 'ch_merge_amplification',     'merge_amplification',
        metric_name = 'ch_connections_per_insert',  'connections_per_insert',
        metric_name = 'ch_settle_seconds',          'settle_seconds',
        metric_name
      ) AS metric_name,
      argMax(value, is_pinned) AS value  -- prefer the pinned-name row when both exist
    FROM (
      SELECT run_id, metric_name, value,
             (metric_name NOT LIKE 'ch\\_%') AS is_pinned
      FROM metric_vals
    )
    GROUP BY run_id, metric_name
  ),

  head_side AS
  (
    SELECT r.pair_id AS pair_id, r.tier AS tier,
           mc.metric_name AS metric_name, mc.value AS head_value,
           r.flagged AS head_flagged, r.flag_reason AS head_flag_reason
    FROM runs_scoped AS r
    LEFT JOIN metric_canon AS mc ON mc.run_id = r.run_id
    WHERE r.arm = 'head'
  ),

  pinned_side AS
  (
    SELECT r.pair_id AS pair_id, r.tier AS tier,
           mc.metric_name AS metric_name, mc.value AS pinned_value,
           r.flagged AS pinned_flagged, r.flag_reason AS pinned_flag_reason
    FROM runs_scoped AS r
    LEFT JOIN metric_canon AS mc ON mc.run_id = r.run_id
    WHERE r.arm = 'pinned'
  ),

  -- FULL outer join on (pair,tier,metric): a metric present on only ONE arm must
  -- still produce a row (with the other side NULL) so the drill shows it (e.g.
  -- run_cost_usd, charged to one arm by §2.1; or a metric a single arm dropped).
  arm_pairs AS
  (
    SELECT
      coalesce(nullIf(h.pair_id, ''),     p.pair_id)     AS pair_id,
      coalesce(nullIf(h.tier, ''),        p.tier)        AS tier,
      coalesce(nullIf(h.metric_name, ''), p.metric_name) AS metric_name,
      h.head_value                                        AS head_value,
      p.pinned_value                                      AS pinned_value,
      (coalesce(h.head_flagged, 0) OR coalesce(p.pinned_flagged, 0)) AS flagged,
      arrayStringConcat(
        arrayFilter(x -> x != '',
          [coalesce(h.head_flag_reason, ''), coalesce(p.pinned_flag_reason, '')]),
        '|'
      )                                                   AS flag_reason
    FROM head_side AS h
    FULL OUTER JOIN pinned_side AS p
      ON  p.pair_id     = h.pair_id
      AND p.tier        = h.tier
      AND p.metric_name = h.metric_name
  )

SELECT
  pair_id,
  tier,
  metric_name,
  head_value,
  pinned_value,
  -- NULL-safe ratio + delta%: NULL if either side missing or pinned = 0.
  if(isNull(head_value) OR isNull(pinned_value) OR pinned_value = 0,
     NULL,
     head_value / pinned_value)                          AS ratio,
  if(isNull(head_value) OR isNull(pinned_value) OR pinned_value = 0,
     NULL,
     round((head_value / pinned_value - 1) * 100, 2))     AS delta_pct,
  flagged,
  flag_reason,
  -- ---- presentation columns (mirror v_kc_pair_ratios so Tab 4's pair filter and
  --      "latest pair" default match the rest of the dashboard byte-for-byte) ----
  if(
    extract(pair_id, '^(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2})Z') = '',
    NULL,
    parseDateTimeBestEffortOrNull(
      replaceRegexpOne(
        extract(pair_id, '^(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2})Z'),
        'T(\\d{2})-(\\d{2})-(\\d{2})$', 'T\\1:\\2:\\3'
      )
    )
  )                                                       AS pair_ts,
  dense_rank() OVER (ORDER BY pair_ts DESC)               AS pair_seq
FROM arm_pairs
WHERE metric_name != ''
ORDER BY pair_id, tier, metric_name
SETTINGS join_use_nulls = 1;
