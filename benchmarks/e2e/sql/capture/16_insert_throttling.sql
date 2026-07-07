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
-- Ported from spark-clickhouse-connector benchmarks/sql/perf/16_insert_throttling.sql.
-- inserts_delayed_fraction, merge_pool_peak_pct, ch_parts_active_peak and
-- ch_memory_limit_errors are the PINNED spellings (contract §2.1); the Spark
-- source already renamed inserts_delayed_fraction / merge_pool_peak_pct per §7.
--
-- Throttling visibility. ClickHouse's three escalating merge-pressure defences:
--   1. parts_to_delay_insert (default 150): inserts get artificially slept
--   2. parts_to_throw_insert (default 3000): inserts rejected w/ code 252
--   3. background pool exhaustion: new merges can't be scheduled
-- This SQL captures evidence of all three. query_log reads carry the Kafka
-- query_user filter (file 11); metric_log reads are server-wide gauges.

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
SELECT {run_id:String}, metric_name, unit, value FROM (
  -- ====== Counter deltas from metric_log over the full settle window ======
  -- ProfileEvent_* in system.metric_log are cumulative since server start, so
  -- the window delta is max()-min(), not sum().
  SELECT 'ch_delayed_inserts_count' AS metric_name, 'count' AS unit,
         toFloat64(max(ProfileEvent_DelayedInserts) - min(ProfileEvent_DelayedInserts)) AS value
  FROM remoteSecure({target_addr:String}, system.metric_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({settle_end:String})
  UNION ALL
  SELECT 'ch_delayed_inserts_total_ms', 'ms',
         toFloat64(max(ProfileEvent_DelayedInsertsMilliseconds) - min(ProfileEvent_DelayedInsertsMilliseconds))
  FROM remoteSecure({target_addr:String}, system.metric_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({settle_end:String})
  UNION ALL
  -- TOO_MANY_PARTS rejections (server-side, broader than 11's per-query count).
  SELECT 'ch_rejected_inserts_count', 'count',
         toFloat64(max(ProfileEvent_RejectedInserts) - min(ProfileEvent_RejectedInserts))
  FROM remoteSecure({target_addr:String}, system.metric_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({settle_end:String})
  UNION ALL
  -- MEMORY_LIMIT_EXCEEDED (241): the OTHER way inserts fail under pressure. Big
  -- batches x high write concurrency on a small server OOM here rather than
  -- tripping TOO_MANY_PARTS. Counted over the insert window (query_log), scoped
  -- to our table + Kafka query_user + insert/flush kinds. Code 33
  -- (CANNOT_READ_ALL_DATA) is usually a downstream symptom of the same OOM.
  SELECT 'ch_memory_limit_errors', 'count', toFloat64(count())
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND query_kind IN ('Insert', 'AsyncInsertFlush')
    AND exception_code IN (241, 33)
    AND has(tables, {table_qualified:String})
    AND ({query_user:String} = '' OR user = {query_user:String})
  UNION ALL
  -- Peak active part count during the window. CH 25.x removed
  -- CurrentMetric_MaxPartCountForPartition; CurrentMetric_PartsActive is the
  -- closest proxy (coincides for a single-partition table).
  SELECT 'ch_parts_active_peak', 'count',
         toFloat64(max(CurrentMetric_PartsActive))
  FROM remoteSecure({target_addr:String}, system.metric_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({settle_end:String})
  UNION ALL
  -- Merge pool utilisation. 100% = the merger is the bottleneck. Peak of the
  -- per-sample ratio, not max(task)/max(size), so a mid-run rescale can't
  -- misstate the peak.
  SELECT 'merge_pool_peak_pct', 'percent',
         max(toFloat64(CurrentMetric_BackgroundMergesAndMutationsPoolTask) /
             greatest(toFloat64(CurrentMetric_BackgroundMergesAndMutationsPoolSize), 1.0)) * 100
  FROM remoteSecure({target_addr:String}, system.metric_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({settle_end:String})
  UNION ALL
  -- ====== Actual server thresholds (so the numbers above can be interpreted) ======
  SELECT 'ch_throttle_threshold_delay', 'parts',
         toFloat64(any(toUInt64OrZero(value)))
  FROM remoteSecure({target_addr:String}, system.merge_tree_settings, {target_user:String}, {target_password:String})
  WHERE name = 'parts_to_delay_insert'
  UNION ALL
  SELECT 'ch_throttle_threshold_throw', 'parts',
         toFloat64(any(toUInt64OrZero(value)))
  FROM remoteSecure({target_addr:String}, system.merge_tree_settings, {target_user:String}, {target_password:String})
  WHERE name = 'parts_to_throw_insert'
  UNION ALL
  -- ====== Per-insert visibility from query_log ======
  -- Fraction of inserts that experienced any artificial delay. The delay
  -- attaches to whatever query did the actual write (written_rows > 0).
  SELECT 'inserts_delayed_fraction', 'ratio',
         toFloat64(countIf(ProfileEvents['DelayedInserts'] > 0)) /
         greatest(toFloat64(count()), 1.0)
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind IN ('Insert', 'AsyncInsertFlush')
    AND written_rows > 0
    AND has(tables, {table_qualified:String})
    AND ({query_user:String} = '' OR user = {query_user:String})
  UNION ALL
  -- p99 latency of throttled vs un-throttled inserts. The gap quantifies the
  -- throttling tax independent of normal server load.
  SELECT 'ch_throttled_insert_p99_ms', 'ms', quantile(0.99)(query_duration_ms)
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind IN ('Insert', 'AsyncInsertFlush')
    AND written_rows > 0
    AND has(tables, {table_qualified:String})
    AND ({query_user:String} = '' OR user = {query_user:String})
    AND ProfileEvents['DelayedInserts'] > 0
  UNION ALL
  SELECT 'ch_unthrottled_insert_p99_ms', 'ms', quantile(0.99)(query_duration_ms)
  FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
  WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
    AND type = 'QueryFinish' AND query_kind IN ('Insert', 'AsyncInsertFlush')
    AND written_rows > 0
    AND has(tables, {table_qualified:String})
    AND ({query_user:String} = '' OR user = {query_user:String})
    AND ProfileEvents['DelayedInserts'] = 0
);
