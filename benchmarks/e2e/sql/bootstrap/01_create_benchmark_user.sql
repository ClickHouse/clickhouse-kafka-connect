-- Kafka Connect sink benchmark v2 — benchmark user + role for the dedicated Cloud target.
--
-- Cloud-only DDL: CREATE USER / CREATE ROLE / GRANT require a running server with
-- access control enabled. This does NOT parse under clickhouse-local (no user
-- directory), so it is intentionally excluded from the local parse-check — run it
-- against the live Cloud service after it is created (see ../README.md).
--
-- Password: do NOT commit a real secret. Replace REPLACE_ME_BENCHMARK_USER_PASSWORD
-- with the value stored in the KAFKA_TARGET_CH_PASSWORD CI secret at bootstrap time
-- (e.g. envsubst / sed before piping to clickhouse-client), or run this statement
-- interactively and paste the generated password into the secret.
--
-- Least-privilege model, mirroring what the capture pipeline (task 30) needs:
--   * clickbench.*  : INSERT + SELECT (drain + integrity row-count checks)
--   * clickbench.hits : TRUNCATE     (per-run reset before the measured drain)
--   * perf.*        : INSERT + SELECT (metrics landing + read-back)
--   * system.*      : SELECT on the tables the CH-side capture reads

CREATE ROLE IF NOT EXISTS kafka_benchmark;

-- clickbench: drain target + integrity checks + per-run reset.
GRANT INSERT, SELECT ON clickbench.* TO kafka_benchmark;
GRANT TRUNCATE ON clickbench.hits TO kafka_benchmark;

-- perf: metrics landing (capture writes, dashboard/read-back reads).
GRANT INSERT, SELECT ON perf.* TO kafka_benchmark;

-- system tables read by the capture pipeline (query_log / part_log / metric_log /
-- parts / asynchronous_metric_log families — plan §4 Tier 1, §7 metrics catalog).
GRANT SELECT ON system.query_log TO kafka_benchmark;
GRANT SELECT ON system.part_log TO kafka_benchmark;
GRANT SELECT ON system.metric_log TO kafka_benchmark;
GRANT SELECT ON system.parts TO kafka_benchmark;
GRANT SELECT ON system.asynchronous_metric_log TO kafka_benchmark;

CREATE USER IF NOT EXISTS kafka_benchmark
  IDENTIFIED WITH sha256_password BY 'REPLACE_ME_BENCHMARK_USER_PASSWORD'
  DEFAULT ROLE kafka_benchmark;

GRANT kafka_benchmark TO kafka_benchmark;
