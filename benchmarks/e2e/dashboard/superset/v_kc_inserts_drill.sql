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
--   query_duration_ms (latency), written_rows (batch size), written_bytes,
--   memory_usage, cpu_microseconds, exception_code (+ is_error flag), and the
--   arm/tier/pair_id/flagged scope from runs.
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
  i.written_bytes                                          AS written_bytes,
  i.memory_usage                                           AS memory_usage,
  i.cpu_microseconds                                       AS cpu_microseconds,
  i.exception_code                                         AS exception_code,
  (i.exception_code != 0)                                  AS is_error
FROM raw_connectors_load_testing.ch_inserts AS i
INNER JOIN runs_scoped AS r ON r.run_id = i.run_id
ORDER BY i.run_id, i.event_time;
