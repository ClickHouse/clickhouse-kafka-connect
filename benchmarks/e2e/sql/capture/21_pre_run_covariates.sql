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
-- Ported verbatim from spark-clickhouse-connector benchmarks/sql/perf/21_pre_run_covariates.sql.
-- ch_uptime / pre_run_rss / pre_run_active_parts are conformant on disk (§7).
--
-- Pre-run server covariates (contract §2.1). Captured on the target BEFORE the
-- drain (pre-truncate) so environment noise becomes a filterable fact:
--   ch_uptime             server uptime in seconds. Lower than the previous run
--                         => the service restarted between runs (cleared memory
--                         pressure, cold caches).
--   pre_run_rss           resident memory (MemoryResident) at run start.
--   pre_run_active_parts  active parts on the target table at run start. A
--                         cleanliness VERIFIER (~0 by design, since the pipeline
--                         truncates at end-of-run), NOT a variance explainer.
--
-- Point-in-time reads from system.asynchronous_metrics + system.parts — no time
-- window, so they run correctly before RUN_END exists (pre-drain capture).

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
SELECT {run_id:String} AS run_id, metric_name, unit, value FROM (
  SELECT 'ch_uptime' AS metric_name, 'seconds' AS unit,
         toFloat64(any(if(metric = 'Uptime', value, NULL))) AS value
  FROM remoteSecure({target_addr:String}, system.asynchronous_metrics, {target_user:String}, {target_password:String})
  WHERE metric = 'Uptime'
  UNION ALL
  SELECT 'pre_run_rss', 'bytes',
         toFloat64(any(if(metric = 'MemoryResident', value, NULL)))
  FROM remoteSecure({target_addr:String}, system.asynchronous_metrics, {target_user:String}, {target_password:String})
  WHERE metric = 'MemoryResident'
  UNION ALL
  SELECT 'pre_run_active_parts', 'count',
         toFloat64(count())
  FROM remoteSecure({target_addr:String}, system.parts, {target_user:String}, {target_password:String})
  WHERE database = {ch_database:String} AND table = {ch_table:String} AND active
);
