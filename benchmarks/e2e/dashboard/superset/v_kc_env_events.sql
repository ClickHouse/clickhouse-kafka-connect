-- =============================================================================
-- v_kc_env_events  —  DWH Superset virtual dataset (Benchmark v2 Kafka, Tab 3)
-- =============================================================================
-- WHAT THIS IS
--   The KAFKA-LOCAL environment-annotation source for Tab 3 (ENVIRONMENT). It
--   derives, inline from consecutive PINNED-arm runs, the two environment events the
--   plan's Tab-3 annotation layer needs:
--     * CH UPGRADE  — clickhouse_version changed vs the previous pinned run.
--     * SERVER RESTART — ch_uptime DROPPED vs the previous pinned run (the service
--       restarted between the two runs; contract §2.1 "a drop vs the previous run =>
--       the service restarted").
--   One row per detected event, carrying when it happened (the run it was first seen
--   on), what changed (from/to value), and the MANDATORY scope tuple so the
--   annotation lands only on the right charts.
--
-- WHY THIS EXISTS (and why it is Kafka-local, not the shared v_env_annotations)
--   The plan §8 data-foundation sketches a SHARED v_env_annotations across Spark and
--   Kafka. That shared view DOES NOT EXIST YET for Kafka (task-35 constraint 6). Per
--   contract §4, an UNSCOPED shared annotation view is PROHIBITED — it would paint a
--   Spark-target restart (production, us-east-2) onto Kafka charts (staging) and vice
--   versa, manufacturing false correlations. So Tab 3's annotation layer is derived
--   HERE, inline, from THIS connector's runs only, and every emitted row carries the
--   §4 scope tuple (connector, target_service, environment_class). A shared
--   cross-connector annotation view that UNIONs both connectors' scoped events is
--   explicitly FUTURE WORK (documented in DASHBOARD.md Tab-3 section); when it lands,
--   this view becomes its kafka-connect branch unchanged (it is already scoped).
--
-- CONTRACT §4 SCOPE TUPLE (MANDATORY on every annotation row):
--     (connector, target_service, environment_class)
--   connector          = 'kafka-connect' (this view's scope).
--   environment_class  = runtime['environment_class'] (contract §1.1 mandatory).
--   target_service     = the concrete service identity. The contract names this key
--     but the runtime map does not (yet) carry a dedicated 'target_service' key — the
--     mandatory identity keys today are target_region + environment_class (§1.1). So
--     target_service is resolved defensively: runtime['target_service'] if the
--     pipeline ever emits it, else a stable composite of environment_class +
--     target_region (e.g. 'staging/us-east-2'), which uniquely identifies THIS
--     benchmark's dedicated target among the connectors' targets. When a dedicated
--     'target_service' key is added upstream this coalesce picks it up with no edit.
--     (The composite is sufficient for scoping BECAUSE the two benchmarks run against
--     different (class, region) targets — contract §1.1 / §4.)
--
-- ARM SCOPING: PINNED only. Tab 3 is the arm=pinned instrument-health tab, and the
--   pinned arm is the stable reference whose environment drift is the story (a HEAD
--   clickhouse_version change is a code-under-test config move, not an environment
--   event). CH-version and uptime deltas are therefore computed over consecutive
--   PINNED runs of the same (tier, scope) only.
--
-- EVENT DETECTION (window over consecutive pinned runs, ordered by run_started_at):
--   prev_ch_version = lagInFrame(clickhouse_version) OVER (per scope+tier, by time)
--   prev_uptime     = lagInFrame(ch_uptime)          OVER (per scope+tier, by time)
--   A ch_version_change row is emitted when prev is non-empty AND differs.
--   A server_restart  row is emitted when prev uptime is present AND current < prev
--     (uptime went backwards => the box restarted between the two runs).
--   The FIRST run in a series has no predecessor => emits no event (nothing to diff).
--
-- WHY tier IS IN THE PARTITION: t0 and t1 pinned runs of one night hit the SAME
--   target but are separate runs; partitioning the lag by tier keeps the version/
--   uptime series per-tier coherent and avoids a spurious "restart" when the natural
--   t0->t1 ch_uptime increase within a night is compared across tiers. The scope
--   tuple (connector, target_service, environment_class) is the same for both tiers
--   of a pair, so the emitted annotation still scopes correctly to the target.
--
-- CONSUMERS MUST FILTER THE SCOPE TUPLE (contract §4): Tab-3's annotation overlay,
--   and any future shared view, MUST filter (connector, target_service,
--   environment_class) — never render an unscoped union.
--
-- SOURCE: raw_connectors_load_testing.{runs,metrics} (ClickPipe DWH mirror of
--   perf.*). ch_uptime / clickhouse_version are covariates present from the first
--   pair; zero rows without error when <2 pinned runs exist (nothing to diff).
-- =============================================================================
CREATE OR REPLACE VIEW raw_connectors_load_testing.v_kc_env_events AS
WITH
  pinned_runs AS
  (
    SELECT
      run_id,
      run_started_at,
      clickhouse_version,
      if(mapContains(runtime, 'tier'), runtime['tier'], '1')    AS tier,
      runtime['pair_id']                                        AS pair_id,
      coalesce(nullIf(runtime['environment_class'], ''), 'unknown') AS environment_class,
      runtime['target_region']                                  AS target_region,
      -- contract §4 target_service: dedicated key if present, else a stable
      -- (class/region) composite that uniquely names this benchmark's target.
      coalesce(
        nullIf(runtime['target_service'], ''),
        concat(
          coalesce(nullIf(runtime['environment_class'], ''), 'unknown'),
          '/',
          coalesce(nullIf(runtime['target_region'], ''), 'unknown')
        )
      )                                                         AS target_service
    FROM raw_connectors_load_testing.runs
    WHERE connector = 'kafka-connect'
      AND connector != '__verdict_fixture__'
      AND NOT (mapContains(runtime, 'outcome') AND runtime['outcome'] = 'failed')
      AND pair_id != ''
      AND if(mapContains(runtime, 'arm'), runtime['arm'], 'head') = 'pinned'
  ),

  -- ch_uptime per pinned run (contract §2.1 covariate). One value per run.
  uptime AS
  (
    SELECT run_id, argMax(value, recorded_at) AS ch_uptime
    FROM raw_connectors_load_testing.metrics
    WHERE metric_name = 'ch_uptime'
    GROUP BY run_id
  ),

  -- consecutive-run diffs over the pinned series, per (scope tuple, tier).
  seq AS
  (
    SELECT
      pr.run_id                                                AS run_id,
      pr.run_started_at                                        AS run_started_at,
      pr.pair_id                                               AS pair_id,
      pr.tier                                                  AS tier,
      pr.environment_class                                     AS environment_class,
      pr.target_service                                        AS target_service,
      pr.target_region                                         AS target_region,
      pr.clickhouse_version                                    AS ch_version,
      u.ch_uptime                                              AS ch_uptime,
      lagInFrame(pr.clickhouse_version) OVER w                 AS prev_ch_version,
      lagInFrame(u.ch_uptime)           OVER w                 AS prev_ch_uptime
    FROM pinned_runs AS pr
    LEFT JOIN uptime AS u ON u.run_id = pr.run_id
    WINDOW w AS (
      PARTITION BY pr.target_service, pr.environment_class, pr.tier
      ORDER BY pr.run_started_at
      ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    )
  ),

  -- one row per (run, tier) per event KIND that fired; arrayJoin flattens the kinds.
  events AS
  (
    SELECT
      run_id, run_started_at, pair_id, tier,
      environment_class, target_service, target_region,
      ch_version, prev_ch_version, ch_uptime, prev_ch_uptime,
      kind
    FROM seq
    ARRAY JOIN
      arrayFilter(x -> x != '', [
        -- CH upgrade: previous version present AND different.
        if(prev_ch_version != '' AND prev_ch_version != ch_version, 'ch_version_change', ''),
        -- server restart: previous uptime present AND uptime went backwards.
        if(isNotNull(prev_ch_uptime) AND ch_uptime < prev_ch_uptime, 'server_restart', '')
      ]) AS kind
  )

SELECT
  'kafka-connect'                                            AS connector,
  target_service,
  environment_class,
  target_region,
  tier,
  run_id,
  run_started_at                                            AS event_time,
  pair_id,
  -- parsed pair timestamp so the annotation can be placed on the trend charts'
  -- pair_ts x-axis (same derivation as the trend view).
  if(
    extract(pair_id, '^(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2})Z') = '',
    NULL,
    parseDateTimeBestEffortOrNull(
      replaceRegexpOne(
        extract(pair_id, '^(\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2})Z'),
        'T(\\d{2})-(\\d{2})-(\\d{2})$', 'T\\1:\\2:\\3'
      )
    )
  )                                                          AS pair_ts,
  kind                                                       AS event_kind,
  -- from/to detail for the annotation label.
  multiIf(
    kind = 'ch_version_change', prev_ch_version,
    kind = 'server_restart',    toString(toUInt64(prev_ch_uptime)),
    ''
  )                                                          AS from_value,
  multiIf(
    kind = 'ch_version_change', ch_version,
    kind = 'server_restart',    toString(toUInt64(ch_uptime)),
    ''
  )                                                          AS to_value,
  multiIf(
    kind = 'ch_version_change',
      concat('CH upgrade ', prev_ch_version, ' -> ', ch_version),
    kind = 'server_restart',
      concat('server restart (uptime ', toString(toUInt64(prev_ch_uptime)),
             's -> ', toString(toUInt64(ch_uptime)), 's)'),
    ''
  )                                                          AS label
FROM events
ORDER BY event_time, tier, event_kind;
