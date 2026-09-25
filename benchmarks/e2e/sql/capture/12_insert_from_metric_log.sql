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
-- Ported from spark-clickhouse-connector benchmarks/sql/perf/12_insert_from_metric_log.sql.
-- Metric names are the PINNED spellings of docs/benchmark-v2-contract.md §2.1.
-- connections_per_insert is the cross-connector shared metric (§2.1); it uses
-- the same spelling on ALL tiers (Amendment 2026-07-07 / contract §7). The Tier-1
-- rename ch_connections_per_insert -> connections_per_insert landed 2026-07-07;
-- no Kafka history exists, so no legacy-name coalesce is needed here. The
-- remaining ch_http_connections_created/preserved/peak names are connector-
-- specific diagnostics (correctly NOT shared, not renamed by §7).
--
-- Connection-churn metrics from system.metric_log: connections_per_insert
-- is ~1.0 when the sink opens one connection per insert; << 1.0 once an HTTP
-- client is cached per task. The single query_log subquery (the insert-count
-- denominator) carries the same Kafka query_user filter as file 11.

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
SELECT {run_id:String}, metric_name, unit, value FROM (
  SELECT 'ch_http_connections_created' AS metric_name, 'count' AS unit,
         toFloat64(max(ProfileEvent_HTTPConnectionsCreated) - min(ProfileEvent_HTTPConnectionsCreated)) AS value
  FROM remoteSecure({target_addr:String}, system.metric_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
  UNION ALL
  SELECT 'ch_http_connections_preserved', 'count',
         toFloat64(max(ProfileEvent_HTTPConnectionsPreserved) - min(ProfileEvent_HTTPConnectionsPreserved))
  FROM remoteSecure({target_addr:String}, system.metric_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
  UNION ALL
  -- Ratio: connections opened / inserts. 1.0 = one connection per insert;
  -- << 1.0 = one connection serves many inserts.
  SELECT 'connections_per_insert', 'ratio',
         (SELECT toFloat64(max(ProfileEvent_HTTPConnectionsCreated) - min(ProfileEvent_HTTPConnectionsCreated))
          FROM remoteSecure({target_addr:String}, system.metric_log, {target_user:String}, {target_password:String})
          WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String}))
         /
         greatest(
           (SELECT toFloat64(count())
            FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
            WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
              AND type = 'QueryFinish' AND query_kind = 'Insert'
              -- Scope to our table; without this, CH-Cloud-internal billing
              -- inserts inflate the denominator and deflate the ratio.
              AND has(tables, {table_qualified:String})
              AND ({query_user:String} = '' OR user = {query_user:String})),
           1.0)
  UNION ALL
  -- Peak concurrent HTTP connections. Verifies the server isn't saturated.
  SELECT 'ch_http_connections_peak', 'count',
         toFloat64(max(CurrentMetric_HTTPConnection))
  FROM remoteSecure({target_addr:String}, system.metric_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
  UNION ALL
  -- Background merge queue: anything > 100 means merges are falling behind ingest.
  SELECT 'ch_bg_merge_queue_peak', 'count',
         toFloat64(max(CurrentMetric_BackgroundMergesAndMutationsPoolTask))
  FROM remoteSecure({target_addr:String}, system.metric_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
);
