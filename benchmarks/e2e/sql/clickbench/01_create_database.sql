-- Kafka Connect sink benchmark v2 — clickbench target database.
-- Ported from the Spark benchmark (spark-clickhouse-connector/benchmarks/sql/clickbench/01_create_database.sql).
-- MUST stay schema-identical with the Spark target: this is the cross-connector comparability contract (plan §4 Tier 1, dashboard Tab 5).
CREATE DATABASE IF NOT EXISTS clickbench;
