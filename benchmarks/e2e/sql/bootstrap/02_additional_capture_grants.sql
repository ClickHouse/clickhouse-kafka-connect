-- Kafka Connect sink benchmark v2 — additional system-table grants for the
-- CH-side capture pipeline (task 30).
--
-- 01_create_benchmark_user.sql grants SELECT on the system tables that were
-- known when the user was authored: query_log, part_log, metric_log, parts,
-- asynchronous_metric_log. Porting the full capture family (task 30) revealed
-- the capture SQL + wait_for_settle.py read three MORE system tables that the
-- kafka_benchmark role does not yet have. This file adds exactly those, so the
-- byte-locked bootstrap/01 file (and the Spark-owned perf.* DDL) stays untouched.
--
-- Cloud-only DDL: GRANT requires a running server with access control enabled;
-- this does NOT parse under clickhouse-local (no user directory), so it is
-- intentionally excluded from the local parse-check — run it against the live
-- Cloud service after 01_create_benchmark_user.sql (see ../README.md).
--
-- The three additional tables and where they are read:
--   * system.merge_tree_settings  -- 16_insert_throttling.sql: reads
--                                    parts_to_delay_insert / parts_to_throw_insert
--                                    (the throttle thresholds) via remoteSecure.
--   * system.asynchronous_metrics -- 21_pre_run_covariates.sql: point-in-time
--                                    Uptime / MemoryResident gauges (the current
--                                    snapshot table, distinct from the *_log
--                                    already granted in 01) via remoteSecure.
--   * system.merges               -- wait_for_settle.py: in-flight merge count on
--                                    the target, read directly with the benchmark
--                                    user's own credentials (not via remoteSecure).

GRANT SELECT ON system.merge_tree_settings TO kafka_benchmark;
GRANT SELECT ON system.asynchronous_metrics TO kafka_benchmark;
GRANT SELECT ON system.merges TO kafka_benchmark;
