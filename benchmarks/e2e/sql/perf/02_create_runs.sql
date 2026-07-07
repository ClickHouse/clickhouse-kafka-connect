-- Kafka Connect sink benchmark v2 — perf.runs (one row per benchmark run).
-- Ported from the Spark benchmark (spark-clickhouse-connector/benchmarks/sql/perf/02_create_runs.sql).
-- MUST stay schema-identical with the Spark benchmark (plan §9: "schema impact — none by design";
-- all per-connector attributes go into the runtime map, arm/tier/pair_id are runtime keys).
CREATE TABLE IF NOT EXISTS perf.runs (
  run_id            String,
  run_started_at    DateTime,
  run_ended_at      DateTime,
  git_sha           String,
  connector          String DEFAULT 'spark',
  run_profile        String DEFAULT '',
  connector_version  String,
  clickhouse_version String DEFAULT '',
  -- Generic per-connector attributes so new connectors need no schema change
  -- (Spark fills spark_version, scala_version, emr_release; Kafka fills
  --  kafka_version, connect_version, strimzi_version, arm, tier, pair_id; etc.).
  runtime            Map(String, String),
  notes              String DEFAULT ''
) ENGINE = MergeTree
ORDER BY (run_started_at, run_id);
