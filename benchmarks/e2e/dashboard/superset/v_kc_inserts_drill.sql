-- =============================================================================
-- v_kc_inserts_drill  —  DWH Superset virtual dataset (Benchmark v2 Kafka, Tab 4)
-- =============================================================================
-- WHAT THIS IS
--   The RAW per-insert grain for Tab 4's two bottom-left/right panels (plan §8):
--     * "per-insert latency sequence (ch_inserts, H vs P)" — event_time x
--       query_duration_ms, one point per insert, colored by arm; shows how insert
--       latency evolves across the drain (warm-up spike, steady state, tail).
--     * "batch-size distribution" — a histogram over written_rows (rows per
--       insert), one distribution per arm; shows whether the connector is batching
--       to its configured target or fragmenting.
--   This is a THIN pass-through of perf.ch_inserts joined to the run's scope
--   (arm/tier/pair_id) — one row per captured insert. No aggregation: Superset does
--   the histogram bucketing (batch-size dist) and the scatter/line (latency
--   sequence) from these rows, so the same dataset feeds both panels.
--
-- WHY SEPARATE FROM v_kc_drain_curve
--   v_kc_drain_curve is minute-BUCKETED (shape/lag). This view is the UN-bucketed
--   per-insert grain the distribution + latency-sequence panels need (a histogram
--   over per-minute sums would be wrong — it must be over per-insert written_rows).
--   Same source table, different grain; kept apart so each panel reads the grain it
--   actually wants.
--
-- COLUMNS surfaced (all from ch_inserts unless noted):
--   event_time, seq (per-run insert ordinal by event_time), elapsed_s (seconds
--   since the run's first insert, so H and P latency sequences align on x),
--   query_duration_ms (latency), written_rows (batch size), written_rows_bucket
--   (pre-binned batch-size range for a version-proof bar chart — see below),
--   written_bytes, memory_usage, cpu_microseconds, exception_code (+ is_error
--   flag), and the arm/tier/pair_id/flagged scope from runs, plus the pair_ts /
--   pair_seq presentation columns (so tiles/panels can self-scope to pair_seq=1).
--
-- BATCH-SIZE BUCKET (Task-34 Tab-4 fix #1): Superset's NATIVE histogram viz
--   (viz_type 'histogram', all_columns_x + link_length) errored on the deployed
--   chart and is finicky across Superset versions. Instead this view now emits a
--   deterministic pre-binned written_rows_bucket (0-10k / 10-25k / 25-50k /
--   50-100k / 100k+) so the batch-size panel is a plain BAR chart (dimension =
--   written_rows_bucket, metric = COUNT(*), series = arm) — version-proof and
--   render-stable. written_rows itself is still surfaced (the latency sequence and
--   any ad-hoc analysis still read the raw value); the bucket is purely additive.
--   written_rows_bucket_sort carries the numeric lower edge so the bar categories
--   order correctly (Superset sorts a string dimension lexicographically otherwise).
--
-- PAIR SCOPING (Task-34 Tab-4 fix #2): this view now carries pair_ts + pair_seq
--   (dense_rank over pair_ts DESC, 1 = newest pair) exactly like v_kc_run_drill /
--   v_kc_pair_ratios, so a chart with no explicit pair filter can self-scope to the
--   LATEST pair via pair_seq = 1 (an unset native filter then still shows the newest
--   pair instead of aggregating across all pairs).
--
-- SCOPE: kafka-connect only; fixture excluded; failed-OUTCOME runs excluded (no
--   meaningful insert sequence). flagged carried as presentation (deployment
--   deviation IN ('1','true') REPLICATED — see DASHBOARD.md); a flagged pair is
--   still drillable (that is the point of the drill), so flagged does NOT exclude.
--
-- SANITY CHECK (data reality, Task 34): ch_inserts carries ~380-420 rows per run,
--   so a 4-run clean pair yields ~1.5-1.7k rows here — dense enough for both a
--   readable latency sequence and a real batch-size histogram.
--
-- SOURCE: raw_connectors_load_testing.{ch_inserts,runs} (ClickPipe DWH mirror of
--   perf.*). Zero rows without error when a run has no captured inserts.
-- =============================================================================
CREATE OR REPLACE VIEW raw_connectors_load_testing.v_kc_inserts_drill AS
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
  )
SELECT
  r.pair_id                                                AS pair_id,
  r.arm                                                    AS arm,
  r.tier                                                   AS tier,
  r.flagged                                                AS flagged,
  i.run_id                                                 AS run_id,
  i.event_time                                             AS event_time,
  -- per-run insert ordinal (1-based) by event_time, ties broken by written_rows.
  row_number() OVER (
    PARTITION BY i.run_id ORDER BY i.event_time, i.written_rows
  )                                                        AS seq,
  -- seconds since this run's FIRST insert, so H and P sequences share an x-origin.
  toUInt32(i.event_time)
    - toUInt32(min(i.event_time) OVER (PARTITION BY i.run_id)) AS elapsed_s,
  i.query_duration_ms                                      AS query_duration_ms,
  i.written_rows                                           AS written_rows,
  -- pre-binned batch-size range (Tab-4 fix #1): a deterministic label so the
  -- batch-size panel is a version-proof BAR chart (count per bucket, per arm)
  -- instead of Superset's finicky native histogram viz.
  multiIf(
    i.written_rows <  10000,  '0-10k',
    i.written_rows <  25000,  '10-25k',
    i.written_rows <  50000,  '25-50k',
    i.written_rows < 100000,  '50-100k',
                              '100k+'
  )                                                        AS written_rows_bucket,
  -- numeric lower edge of the bucket, so the bar categories sort in size order
  -- (a string dimension otherwise sorts lexicographically: '100k+' before '25-50k').
  multiIf(
    i.written_rows <  10000,  0,
    i.written_rows <  25000,  10000,
    i.written_rows <  50000,  25000,
    i.written_rows < 100000,  50000,
                              100000
  )                                                        AS written_rows_bucket_sort,
  i.written_bytes                                          AS written_bytes,
  i.memory_usage                                           AS memory_usage,
  i.cpu_microseconds                                       AS cpu_microseconds,
  i.exception_code                                         AS exception_code,
  (i.exception_code != 0)                                  AS is_error,
  -- ---- pair recency presentation columns (Tab-4 fix #2: latest-pair default) ----
  -- pair_ts parsed from the pair_id 'YYYY-MM-DDTHH-MM-SSZ' prefix, NULL-safe, exactly
  -- as v_kc_run_drill / v_kc_pair_ratios derive it (so the "latest pair" agrees
  -- tab-wide byte-for-byte).
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
  -- pair_seq: dense rank over pair_ts DESC (1 = newest pair), tab-wide (NOT
  -- partitioned by tier — the drill scopes a whole pair). A panel with no explicit
  -- pair filter self-scopes to the latest pair via pair_seq = 1.
  dense_rank() OVER (ORDER BY pair_ts DESC)                AS pair_seq
FROM raw_connectors_load_testing.ch_inserts AS i
INNER JOIN runs_scoped AS r ON r.run_id = i.run_id
ORDER BY i.run_id, i.event_time;
