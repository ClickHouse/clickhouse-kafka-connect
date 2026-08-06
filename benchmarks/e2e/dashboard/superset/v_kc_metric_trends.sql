-- =============================================================================
-- v_kc_metric_trends  —  DWH Superset virtual dataset (Benchmark v2 Kafka, Tabs 2/3)
-- =============================================================================
-- WHAT THIS IS
--   The TALL per-run metric series the absolute-history tabs need: one row per
--   (run, metric_name) carrying value + unit and the run's scope columns
--   (arm, tier, pair_id, run_started_at, clickhouse_version, connector_version,
--    environment_class, target_region, pre-parsed pair_ts / pair_seq). This is the
--   dataset every Tab-2 (arm=head) and Tab-3 (arm=pinned) TREND chart reads: a
--   Superset time-series line filters this view to `metric_name = '<name>' AND arm =
--   '<tab arm>' AND tier = '<n>'` and plots value over run_started_at (or pair_ts).
--
-- WHY A NEW VIEW (not v_kc_runs / v_kc_run_drill / v_kc_pair_ratios)
--   * v_kc_runs is one WIDE row per run — a time-series line over an arbitrary
--     metric would need one hard-coded column per chart, and every NEW metric (the
--     Spark server-interaction family, the consume-side JMX metrics, gc_share, ...)
--     would require editing the base view. A TALL (run × metric) shape lets one
--     dataset feed ~20 trend charts and absorb new metric names with zero DDL.
--   * v_kc_run_drill / v_kc_pair_ratios are H-vs-P PAIR views (self-joined on
--     pair_id) — the wrong grain for an absolute single-arm history trend, and
--     v_kc_pair_ratios is byte-locked to the 4 gated metrics + verdict registry.
--   This view carries NO verdict/band/alert column — the trend tabs are descriptive
--   absolute history; Tab 1's v_kc_pair_ratios stays the single source of verdict
--   and alert truth, so the trend tabs cannot drift from the alerting logic.
--
-- ARM SCOPING (plan §8 global-filter note): the view carries BOTH arms; the tab
--   scopes it. Tab 2 (PERFORMANCE) filters arm='head' (absolute code-under-test
--   history); Tab 3 (ENVIRONMENT) filters arm='pinned' (instrument health). The arm
--   column is exposed so the Superset native/dashboard filter does the scoping — the
--   same value is never stored twice.
--
-- CONTRACT GROUNDING (docs/benchmark-v2-contract.md)
--   §2 metric spellings ARE the series identity. §7 legacy->contract cutover
--     (2026-07-07): the ch_-prefixed legacy names (ch_parts_per_insert,
--     ch_merge_amplification, ch_connections_per_insert, ch_settle_seconds,
--     ch_inserts_delayed_fraction, ch_merge_pool_peak_pct) are FOLDED into their
--     pinned name so a series spans the cutover unbroken (same coalesce as
--     v_kc_run_drill.metric_canon). The capture-family names that were already
--     conformant (ch_insert_duration_p50_ms/p99_ms, ch_memory_limit_errors,
--     ch_too_many_parts_errors, ch_parts_active_peak, ch_peak_server_memory_bytes,
--     bytes_on_wire_per_row, ...) pass through unchanged — they are the pinned
--     spellings (§7 "already conformant on disk").
--   §2.1 run_cost_usd two-arm attribution: charged once per pair on ONE arm's row;
--     the other arm omits it. A cost TREND therefore reads per-PAIR, not per-arm —
--     documented on the headline tile, not corrected here (the row is emitted as-is).
--   §1.1 runtime coalesce defaults (arm->'head', tier->'1', outcome->'success').
--
-- SIGHTED-GATE HISTORY (plan §7 / task-35 constraint 5): the Tier-0 client-cost
--   metrics connect_cpu_seconds_per_Mrows and ch_insert_cpu_share_tier0 only exist
--   from pair 4 (2026-07-12) onward — before the Tier-0 build landed there are no
--   rows for those names, so their trend lines simply START at 2026-07-12. That is a
--   data fact of this view, surfaced honestly in the chart descriptions (tab2/tab3
--   _charts.json), not something this SQL back-fills.
--
-- WATCH-ONLY / COVARIATE METRICS: merge_amplification is WATCH-ONLY (contract §2 /
--   §3 — no verdict row; charted here as a trend with a watch-only description).
--   drain_rate_stability is DELIBERATELY not surfaced to any chart yet (task-35
--   constraint 2 / principal ruling: pre-2026-07-13 values are 10s-bucket, not
--   comparable to the post-d2aff99 bucketed values) — the trend slot for it is an
--   honest markdown placeholder on Tab 2, NOT a line off this view. This view does
--   not special-case it (it would appear if queried), but no chart queries it.
--
-- MATCHED-DATASET RULE (contract §5, Amendment 2026-07-09d): scoped by connector
--   VALUE = 'kafka-connect', never by instance — does not preclude Tab 5 (#36).
--
-- DEPLOYMENT DEVIATION (REPLICATED — see DASHBOARD.md "Deployed flagged predicate"):
--   flag tested as runtime['flagged'] IN ('1','true') to match the deployed Tab-1
--   datasets. `flagged` is carried as a presentation column so a trend chart can mark
--   a flagged run (an integrity-mark overlay); it does NOT exclude the run here (the
--   absolute-history tabs show the point and mark it — excluding silently would hide
--   a real measurement). Failed-OUTCOME runs ARE excluded by value (no comparable
--   measurement). The verdict/exclusion truth stays on Tab 1.
--
-- SOURCE: raw_connectors_load_testing.{runs,metrics} (ClickPipe DWH mirror of
--   perf.*). Zero rows without error when no kafka runs exist yet.
-- =============================================================================
CREATE OR REPLACE VIEW raw_connectors_load_testing.v_kc_metric_trends AS
WITH
  runs_scoped AS
  (
    SELECT
      run_id,
      run_started_at,
      run_ended_at,
      git_sha,
      connector_version,
      clickhouse_version,
      if(mapContains(runtime, 'arm'),  runtime['arm'],  'head') AS arm,
      if(mapContains(runtime, 'tier'), runtime['tier'], '1')    AS tier,
      runtime['pair_id']                                        AS pair_id,
      (mapContains(runtime, 'flagged') AND runtime['flagged'] IN ('1','true')) AS flagged,
      runtime['flag_reason']                                    AS flag_reason,
      runtime['environment_class']                              AS environment_class,
      runtime['target_region']                                  AS target_region
    FROM raw_connectors_load_testing.runs
    WHERE connector = 'kafka-connect'
      AND connector != '__verdict_fixture__'
      AND NOT (mapContains(runtime, 'outcome') AND runtime['outcome'] = 'failed')
      AND pair_id != ''
  ),

  -- One value per (run, metric). Latest by recorded_at (argMax) for determinism.
  metric_vals AS
  (
    SELECT run_id, metric_name, any(unit) AS unit, argMax(value, recorded_at) AS value
    FROM raw_connectors_load_testing.metrics
    GROUP BY run_id, metric_name
  ),

  -- Legacy->contract coalesce (§7 cutover 2026-07-07): fold the ch_-prefixed legacy
  -- names that had a rename into the pinned name so a series spans the cutover
  -- unbroken (identical rewrite list to v_kc_run_drill.metric_canon). Only the names
  -- §7 lists as renamed are folded; every other ch_-prefixed name (the capture
  -- family that is ALREADY the pinned spelling) passes through untouched.
  metric_canon AS
  (
    SELECT
      run_id,
      multiIf(
        metric_name = 'ch_parts_per_insert',          'parts_per_insert',
        metric_name = 'ch_merge_amplification',        'merge_amplification',
        metric_name = 'ch_connections_per_insert',     'connections_per_insert',
        metric_name = 'ch_settle_seconds',             'settle_seconds',
        metric_name = 'ch_inserts_delayed_fraction',   'inserts_delayed_fraction',
        metric_name = 'ch_merge_pool_peak_pct',        'merge_pool_peak_pct',
        metric_name
      ) AS metric_name,
      argMax(value, is_pinned) AS value,  -- prefer the pinned-name row when both exist
      argMax(unit,  is_pinned) AS unit
    FROM (
      SELECT run_id, metric_name, value, unit,
             -- a folded name's pinned spelling never starts 'ch_'; the capture-family
             -- names DO start 'ch_' and are already pinned, so key the preference on
             -- membership in the fold list, not on the 'ch_' prefix.
             (metric_name IN (
               'parts_per_insert','merge_amplification','connections_per_insert',
               'settle_seconds','inserts_delayed_fraction','merge_pool_peak_pct'
             )) AS is_pinned
      FROM metric_vals
    )
    GROUP BY run_id, metric_name
  )

SELECT
  r.pair_id                                                AS pair_id,
  r.arm                                                    AS arm,
  r.tier                                                   AS tier,
  r.run_id                                                 AS run_id,
  r.run_started_at                                         AS run_started_at,
  r.run_ended_at                                           AS run_ended_at,
  r.git_sha                                                AS git_sha,
  r.connector_version                                      AS connector_version,
  r.clickhouse_version                                     AS clickhouse_version,
  r.environment_class                                      AS environment_class,
  r.target_region                                          AS target_region,
  r.flagged                                                AS flagged,
  r.flag_reason                                            AS flag_reason,
  mc.metric_name                                           AS metric_name,
  mc.value                                                 AS value,
  mc.unit                                                  AS unit,
  -- ---- presentation columns (mirror v_kc_pair_ratios / v_kc_run_drill so the trend
  --      tabs share the dashboard's pair time axis + recency ranking byte-for-byte).
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
  -- pair_seq: dense rank within (arm,tier) by pair_ts DESC (1 = newest). Per-arm/tier
  -- so a head-t1 trend and a pinned-t0 trend each number their own pairs 1..N.
  dense_rank() OVER (PARTITION BY r.arm, r.tier ORDER BY pair_ts DESC) AS pair_seq
FROM runs_scoped AS r
INNER JOIN metric_canon AS mc ON mc.run_id = r.run_id
ORDER BY metric_name, tier, arm, run_started_at;
