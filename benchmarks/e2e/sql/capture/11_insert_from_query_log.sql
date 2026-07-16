--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
-- Parameters ({name:Type}) are bound by run_metrics_sql.py.
--
-- Ported from spark-clickhouse-connector benchmarks/sql/perf/11_insert_from_query_log.sql.
-- Metric names are the PINNED spellings of docs/benchmark-v2-contract.md §2.1;
-- see benchmarks/e2e/capture/PORTING.md for the per-metric rename mapping.
--
-- remoteSecure()/9440 reaches the ClickHouse Cloud target (no plain 9000);
-- for self-hosted use remote()/9000. Single-replica only; for multi-replica
-- swap in clusterAllReplicas('default', system.X).
--
-- Window: [run_start, run_end] is the drain window. For the Kafka sink the
-- orchestrator/poller (task 29) bounds it by connector-start timestamp ->
-- lag-0 timestamp (plus settle end for the settle-window SQL). The substitution
-- mechanism is identical to Spark's — the SQL just consumes {run_start}/{run_end}.
--
-- QUERY_LOG FILTER (Kafka adaptation, plan §7 / contract): the sink's inserts
-- are NOT identified by Spark's Spark-specific predicates. They are identified by
--   (a) target table   : has(tables, {table_qualified:String})   [primary]
--   (b) connector user  : optional AND user = {query_user:String}
-- The user predicate is a no-op when {query_user} is '' (the default), so the
-- table-scope filter alone works out of the box; setting QUERY_LOG_USER in the
-- orchestrator env narrows to the sink's CH user (e.g. 'kafka_benchmark') to
-- exclude any co-tenant inserts on the same table. Task 31's first manual run
-- validates the live filter against real sink query_log rows.
--
-- Async inserts: the sink forces async_insert=0 + wait_end_of_query=1 (plan §6),
-- so only 'Insert' receipts carry rows here. The mode-agnostic
-- Insert/AsyncInsertFlush UNION on `written_rows > 0` is retained verbatim from
-- Spark so the same SQL still works should async ever be enabled in a profile.
-- has(tables) excludes CH-Cloud-internal billing inserts.
--
-- ch_insert_cpu_seconds_per_Mrows (contract §2.1): the cross-connector Tab-5
-- server-cost headline. Server insert CPU (OSCPUVirtualTime on the Insert
-- receipts) / (delivered rows / 1e6), delivered rows being the mode-agnostic
-- server-side written-rows total, guarded with nullIf.

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
SELECT {run_id:String}, metric_name, unit, value FROM (
  -- ====== From `Insert` receipts (what the connector sees) ======
  SELECT 'ch_insert_count' AS metric_name, 'count' AS unit, toFloat64(count()) AS value
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND has(tables, {table_qualified:String})
    AND ({query_user:String} = '' OR user = {query_user:String})
  UNION ALL
  SELECT 'ch_insert_duration_p50_ms', 'ms', quantile(0.50)(query_duration_ms)
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND has(tables, {table_qualified:String})
    AND ({query_user:String} = '' OR user = {query_user:String})
  UNION ALL
  SELECT 'ch_insert_duration_p99_ms', 'ms', quantile(0.99)(query_duration_ms)
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND has(tables, {table_qualified:String})
    AND ({query_user:String} = '' OR user = {query_user:String})
  UNION ALL
  SELECT 'ch_peak_memory_usage_bytes', 'bytes', toFloat64(max(memory_usage))
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND has(tables, {table_qualified:String})
    AND ({query_user:String} = '' OR user = {query_user:String})
  UNION ALL
  SELECT 'ch_avg_memory_per_insert_bytes', 'bytes', toFloat64(avg(memory_usage))
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND has(tables, {table_qualified:String})
    AND ({query_user:String} = '' OR user = {query_user:String})
  UNION ALL
  -- Network + CPU on the receipt side: bytes the connector sent, server CPU spent parsing.
  SELECT 'ch_network_receive_bytes', 'bytes',
         toFloat64(sum(ProfileEvents['NetworkReceiveBytes']))
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND has(tables, {table_qualified:String})
    AND ({query_user:String} = '' OR user = {query_user:String})
  UNION ALL
  SELECT 'ch_insert_cpu_seconds', 'seconds',
         toFloat64(sum(ProfileEvents['OSCPUVirtualTimeMicroseconds'])) / 1e6
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND has(tables, {table_qualified:String})
    AND ({query_user:String} = '' OR user = {query_user:String})
  UNION ALL
  -- Server insert CPU seconds per million delivered rows: the cross-connector
  -- Tab-5 server-cost headline (contract §2.1). Numerator matches
  -- ch_insert_cpu_seconds above; denominator is the mode-agnostic written-rows
  -- total / 1e6, guarded with nullIf.
  SELECT 'ch_insert_cpu_seconds_per_Mrows', 's/Mrows',
         (SELECT toFloat64(sum(ProfileEvents['OSCPUVirtualTimeMicroseconds'])) / 1e6
          FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
          WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
            AND type = 'QueryFinish' AND query_kind = 'Insert'
            AND has(tables, {table_qualified:String})
            AND ({query_user:String} = '' OR user = {query_user:String}))
         / nullIf(
           (SELECT toFloat64(sum(written_rows))
            FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
            WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
              AND type = 'QueryFinish' AND query_kind IN ('Insert', 'AsyncInsertFlush')
              AND written_rows > 0
              AND has(tables, {table_qualified:String})
              AND ({query_user:String} = '' OR user = {query_user:String})) / 1e6,
           0)
  UNION ALL
  -- ====== Real write rows/bytes (mode-agnostic) ======
  -- `written_rows > 0` selects AsyncInsertFlush rows in async mode and Insert
  -- rows in sync mode, so the same aggregate works in both.
  SELECT 'ch_avg_rows_per_insert', 'rows', toFloat64(avg(written_rows))
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind IN ('Insert', 'AsyncInsertFlush')
    AND written_rows > 0
    AND has(tables, {table_qualified:String})
    AND ({query_user:String} = '' OR user = {query_user:String})
  UNION ALL
  SELECT 'ch_p50_rows_per_insert', 'rows', quantile(0.50)(written_rows)
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind IN ('Insert', 'AsyncInsertFlush')
    AND written_rows > 0
    AND has(tables, {table_qualified:String})
    AND ({query_user:String} = '' OR user = {query_user:String})
  UNION ALL
  SELECT 'ch_avg_uncompressed_bytes_per_insert', 'bytes', toFloat64(avg(written_bytes))
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind IN ('Insert', 'AsyncInsertFlush')
    AND written_bytes > 0
    AND has(tables, {table_qualified:String})
    AND ({query_user:String} = '' OR user = {query_user:String})
  UNION ALL
  -- ch_flush_duration_* describes the duration of the query that actually wrote
  -- data. In async mode that's the AsyncInsertFlush; in sync mode the Insert.
  SELECT 'ch_flush_duration_p50_ms', 'ms', quantile(0.50)(query_duration_ms)
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind IN ('Insert', 'AsyncInsertFlush')
    AND written_rows > 0
    AND has(tables, {table_qualified:String})
    AND ({query_user:String} = '' OR user = {query_user:String})
  UNION ALL
  SELECT 'ch_flush_duration_p99_ms', 'ms', quantile(0.99)(query_duration_ms)
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind IN ('Insert', 'AsyncInsertFlush')
    AND written_rows > 0
    AND has(tables, {table_qualified:String})
    AND ({query_user:String} = '' OR user = {query_user:String})
  UNION ALL
  -- ====== Windowed error counters ======
  SELECT 'ch_failed_inserts', 'count', toFloat64(count())
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND query_kind IN ('Insert', 'AsyncInsertFlush')
    AND type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
    AND has(tables, {table_qualified:String})
    AND ({query_user:String} = '' OR user = {query_user:String})
  UNION ALL
  -- TOO_MANY_PARTS (252): the canonical "batches too small" error. Scoped to
  -- insert/flush kinds so unrelated queries hitting 252 don't inflate the count.
  SELECT 'ch_too_many_parts_errors', 'count', toFloat64(count())
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND query_kind IN ('Insert', 'AsyncInsertFlush')
    AND exception_code = 252
    AND has(tables, {table_qualified:String})
    AND ({query_user:String} = '' OR user = {query_user:String})
  UNION ALL
  -- NETWORK_ERROR (209), SOCKET_TIMEOUT (210): connector-side timeout symptoms.
  SELECT 'ch_socket_errors', 'count', toFloat64(count())
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND query_kind IN ('Insert', 'AsyncInsertFlush')
    AND exception_code IN (209, 210)
    AND has(tables, {table_qualified:String})
    AND ({query_user:String} = '' OR user = {query_user:String})
);
