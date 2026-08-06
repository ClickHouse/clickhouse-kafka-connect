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
-- ch_insert_cpu_share_tier0 (contract §2.1, unit 'percent'):
-- CONTRACT-MANDATORY for Cloud-hosted Null tier-0 rows (amendment 3298da9b).
-- The Kafka Tier 0 target is an ENGINE=Null table on the SAME Cloud service
-- (plan decision 9); such rows have no pinned instrument version (the Cloud
-- version drifts) so they OMIT tier0_ch_version and MUST emit this parse-watch
-- metric instead (contract §1.1 tier0_ch_version scoping note). This is the
-- decision-9 parse-watch: if it camps near 100%, the Null target is parse-bound
-- and Tier 0 is measuring the server, not the connector.
--
-- Definition: 100 * (server insert CPU seconds from query_log ProfileEvents
-- OSCPUVirtualTimeMicroseconds) / (wall-clock drain window seconds). The
-- numerator MATCHES SQL 11's ch_insert_cpu_seconds CPU accounting EXACTLY (same
-- ProfileEvent, same Insert-receipt scope, same table/user filter) so the two
-- are consistent. The denominator is the drain window [run_start, run_end] in
-- seconds; the caller passes the hits_null table on tier 0 via {table_qualified}
-- and the connector user via {query_user} (same substitution mechanism as the
-- other capture files). Wired into run_pair.sh's capture loop on TIER-0 runs
-- ONLY (mirrors how SQL 20 integrity is skipped on tier 0).

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
SELECT {run_id:String}, metric_name, unit, value FROM (
  SELECT 'ch_insert_cpu_share_tier0' AS metric_name, 'percent' AS unit,
         100 *
         (SELECT toFloat64(sum(ProfileEvents['OSCPUVirtualTimeMicroseconds'])) / 1e6
          FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
          WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
            AND type = 'QueryFinish' AND query_kind = 'Insert'
            AND has(tables, {table_qualified:String})
            AND ({query_user:String} = '' OR user = {query_user:String}))
         / nullIf(
           dateDiff('second',
                    parseDateTimeBestEffort({run_start:String}),
                    parseDateTimeBestEffort({run_end:String})),
           0) AS value
);
