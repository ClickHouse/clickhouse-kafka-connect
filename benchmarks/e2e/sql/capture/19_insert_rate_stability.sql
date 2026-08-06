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
-- Ported verbatim from spark-clickhouse-connector benchmarks/sql/perf/19_insert_rate_stability.sql
-- (reads this run's rows from perf.ch_inserts — no remote/target access, so no
-- query_user filter). This is the server-side CoV; the poller (task 29) also
-- computes a client-side drain_rate_stability from the offset curve (plan §7).
--
--   ingest_rate_stability [CoV, ratio]
--       Coefficient of Variation of the per-minute insert rate:
--         CoV = stddevPop(rows_per_minute) / median(rows_per_minute)
--       rows_per_minute = sum(written_rows) bucketed by toStartOfMinute(event_time)
--       over this run's rows in perf.ch_inserts. Distinguishes a flat plateau
--       (low CoV) from a sawtooth (high CoV) — the throttling/stall pattern a
--       run-average throughput number hides. Lower is steadier.
--
-- Zero new capture: reads perf.ch_inserts, which 15_capture_raw_inserts.sql
-- populates for this run_id earlier in the same capture loop (numeric order:
-- 15 lands before 19). A run with < 2 populated minute-buckets yields NULL and
-- the row is simply omitted.

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
WITH per_minute AS (
  SELECT toStartOfMinute(event_time) AS minute,
         sum(written_rows)           AS rows_per_minute
  FROM perf.ch_inserts
  WHERE run_id = {run_id:String}
  GROUP BY minute
)
SELECT {run_id:String} AS run_id,
       'ingest_rate_stability' AS metric_name,
       'ratio' AS unit,
       toFloat64(stddevPop(rows_per_minute) / nullIf(median(rows_per_minute), 0)) AS value
FROM per_minute
HAVING count() >= 2;
