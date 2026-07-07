-- Kafka Connect sink benchmark v2 — perf.* metrics landing database.
-- Ported from the Spark benchmark (spark-clickhouse-connector/benchmarks/sql/perf/01_create_database.sql).
-- MUST stay schema-identical with the Spark benchmark: the DWH pipeline and the
-- cross-connector dashboard (plan §8/§9) read both connectors from the same perf.* shape.
CREATE DATABASE IF NOT EXISTS perf;
