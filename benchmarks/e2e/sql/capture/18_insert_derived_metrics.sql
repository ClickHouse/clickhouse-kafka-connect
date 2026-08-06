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
-- ADAPTED from spark-clickhouse-connector benchmarks/sql/perf/18_insert_derived_metrics.sql.
-- The Spark file 18 held only server-side derived metrics; the CLIENT-side
-- per-row cost metrics it referenced live in Spark's event-log SQL (file 10),
-- which is Spark-specific and DROPPED here (client-side Kafka metrics come from
-- the poller, task 29). The one server-side derived metric — bytes_on_wire_per_row
-- (contract §2.1, plan §7) — is PORTED. Its name was already conformant on disk.
--
--   bytes_on_wire_per_row [bytes/row]
--       = server NetworkReceiveBytes (post-compression ingress on Insert
--         receipts) / rows written. Wire efficiency of the sink's insert path:
--         catches compression/encoding regressions.
--
-- Both numerator and denominator come from the SAME target system.query_log
-- window and the SAME table + Kafka query_user scope used everywhere else, so
-- this is self-consistent with ch_network_receive_bytes (file 11).

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
SELECT {run_id:String}, 'bytes_on_wire_per_row' AS metric_name, 'bytes/row' AS unit,
       (SELECT toFloat64(sum(ProfileEvents['NetworkReceiveBytes']))
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
            AND ({query_user:String} = '' OR user = {query_user:String})),
         0);
