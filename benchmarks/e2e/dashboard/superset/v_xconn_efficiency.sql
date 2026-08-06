-- =============================================================================
-- v_xconn_efficiency  —  DWH Superset virtual dataset (Benchmark v2, Tab 5 table)
-- =============================================================================
-- WHAT THIS IS
--   The EFFICIENCY TABLE dataset for Tab 5 (plan §8 sketch): latest-30d MEDIANS
--   per (metric, connector) plus the Spark-vs-Kafka RATIO column — "which connector
--   costs the server more per row / delivers more rows/s, and how big is the gap".
--   One row per metric_name; columns metric | Spark | Kafka | ratio (+ the §6
--   covariate columns per side + the matched flag).
--
-- BUILT ON v_xconn (do not re-derive scope): reads the canonical cross-connector
--   view, so the connector scope, arm/tier scope, §2.2 aliasing, §7 folds, fixture/
--   failed/flagged handling and the MATCHED-DATASET rule are all inherited verbatim
--   (single source of truth). This view only adds the 30d-median aggregation + ratio.
--
-- MATCHED-ONLY BY DEFAULT (§5/§6): the ratio column is a COMPARABLE efficiency
--   number, so it is computed ONLY on matched buckets (matched_dataset = 1). Because
--   Kafka runs 10M and Spark ~100M today, NO bucket is matched, so this table is
--   HONESTLY EMPTY today — CORRECT and ACCEPTED (see v_xconn header + DASHBOARD.md).
--   The UNMATCHED context lives in v_xconn (matched_dataset = 0) surfaced as a
--   separate, clearly-labelled context table on the tab — never as a ratio here.
--
-- 30-DAY WINDOW: rows whose run_started_at is within the latest 30 days
--   (run_started_at >= now() - INTERVAL 30 DAY). Median via medianExact for
--   determinism (small n; exact, not approximate). At n<20 clean pairs the median
--   is NOISY/PROVISIONAL — the chart description carries the calibration caveat
--   (task-35 constraint 9 style); this SQL does not gate on n, it reports the level.
--
-- DIRECTION-OF-GOODNESS is NOT baked into the ratio (the ratio is always
--   spark_median / kafka_median); the chart description tells the reader how to read
--   each metric (rows_per_sec higher_better; ch_insert_cpu_seconds_per_Mrows,
--   parts_per_insert, merge_amplification lower_better). Keeping the ratio direction
--   uniform avoids a per-metric sign flip that would confuse the table.
--
-- SETTINGS join_use_nulls = 1: an absent side (a metric only one connector emitted)
--   must surface NULL, so the ratio is NULL (not 0) — a metric present on only one
--   connector is not a comparable efficiency number.
-- =============================================================================
CREATE OR REPLACE VIEW raw_connectors_load_testing.v_xconn_efficiency AS
WITH
  -- Matched, in-window, one median per (metric, connector). value is already the
  -- canonical cross-connector series from v_xconn (rows_per_sec folded, etc.).
  medians AS
  (
    SELECT
      metric_name,
      connector,
      any(connector_label)              AS connector_label,
      medianExact(value)                AS median_value,
      any(unit)                         AS unit,
      -- §6 covariates per connector side (a matched bucket shares dataset/volume;
      -- environment_class/clickhouse_version may still differ across connectors —
      -- that is exactly the caveat, surfaced here per side).
      any(environment_class)            AS environment_class,
      any(clickhouse_version)           AS clickhouse_version,
      any(dataset)                      AS dataset,
      max(rows_expected)                AS rows_expected,
      count()                           AS n_rows
    FROM raw_connectors_load_testing.v_xconn
    WHERE matched_dataset = 1
      AND value IS NOT NULL
      AND run_started_at >= now() - INTERVAL 30 DAY
    GROUP BY metric_name, connector
  ),

  spark_side AS
  (
    SELECT metric_name, median_value AS spark_median, unit AS spark_unit,
           environment_class AS spark_env_class, clickhouse_version AS spark_ch_version,
           n_rows AS spark_n
    FROM medians WHERE connector = 'spark'
  ),

  kafka_side AS
  (
    SELECT metric_name, median_value AS kafka_median, unit AS kafka_unit,
           environment_class AS kafka_env_class, clickhouse_version AS kafka_ch_version,
           n_rows AS kafka_n
    FROM medians WHERE connector = 'kafka-connect'
  )

SELECT
  coalesce(s.metric_name, k.metric_name)                   AS metric_name,
  coalesce(s.spark_unit,  k.kafka_unit)                    AS unit,
  s.spark_median                                           AS spark_median,
  k.kafka_median                                           AS kafka_median,
  -- Spark ÷ Kafka. NULL-safe: NULL if either side missing OR kafka = 0.
  if(isNull(s.spark_median) OR isNull(k.kafka_median) OR k.kafka_median = 0,
     NULL,
     s.spark_median / k.kafka_median)                      AS ratio_spark_over_kafka,
  -- §6 covariates, both sides, so the reader sees the class/version difference that
  -- the tab-level caveat banner explains.
  s.spark_env_class                                        AS spark_environment_class,
  k.kafka_env_class                                        AS kafka_environment_class,
  s.spark_ch_version                                       AS spark_clickhouse_version,
  k.kafka_ch_version                                       AS kafka_clickhouse_version,
  s.spark_n                                                AS spark_n,
  k.kafka_n                                                AS kafka_n
FROM spark_side AS s
FULL OUTER JOIN kafka_side AS k ON k.metric_name = s.metric_name
ORDER BY metric_name
SETTINGS join_use_nulls = 1;
