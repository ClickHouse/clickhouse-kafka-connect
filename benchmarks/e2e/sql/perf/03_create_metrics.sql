-- Kafka Connect sink benchmark v2 — perf.metrics (tall/narrow per-run scalars).
-- Ported from the Spark benchmark (spark-clickhouse-connector/benchmarks/sql/perf/03_create_metrics.sql).
-- MUST stay schema-identical with the Spark benchmark: new metric names are just rows,
-- never columns (plan §9), so the DWH pipeline and dashboards flow with zero changes.
CREATE TABLE IF NOT EXISTS perf.metrics (
  run_id      String,
  metric_name LowCardinality(String),
  unit        LowCardinality(String),
  value       Float64,
  recorded_at DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY (metric_name, run_id);
