-- Kafka Connect sink benchmark v2 — perf.ch_inserts (per-insert raw stats).
-- Ported from the Spark benchmark (spark-clickhouse-connector/benchmarks/sql/perf/04_create_ch_inserts.sql).
-- MUST stay schema-identical with the Spark benchmark: drain/lag curves are derived
-- from these per-insert event_time rows at query time (plan §9).
-- Per-insert raw stats for distribution analysis (batch-size / duration
-- histograms) without re-querying CH after the fact.
CREATE TABLE IF NOT EXISTS perf.ch_inserts (
  run_id           String,
  event_time       DateTime,
  query_duration_ms UInt64,
  written_rows     UInt64,
  written_bytes    UInt64,
  memory_usage     UInt64,
  network_bytes    UInt64,
  cpu_microseconds UInt64,
  exception_code   Int32
) ENGINE = MergeTree
ORDER BY (run_id, event_time);
