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
-- Ported from spark-clickhouse-connector benchmarks/sql/perf/14_insert_settle_seconds.sql.
-- settle_seconds is the PINNED spelling (contract §2.1); the Spark source
-- already renamed it from ch_settle_seconds per §7.
--
-- Writes the settle time (seconds from drain end until active parts stabilise,
-- measured by wait_for_settle.py) into perf.metrics.

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
VALUES ({run_id:String}, 'settle_seconds', 'seconds', toFloat64({settle_seconds:Float64}));
