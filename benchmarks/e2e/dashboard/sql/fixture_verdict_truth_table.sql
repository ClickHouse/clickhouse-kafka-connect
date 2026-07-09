-- fixture_verdict_truth_table.sql — synthetic acceptance fixture for the verdict
-- map in v_kc_pair_ratios.sql (Benchmark v2 Kafka dashboard, #33 prep).
--
-- PURPOSE
--   The principal mandate: any verdict-emitting artifact requires fixture-based
--   acceptance BEFORE it ships. The Spark dashboard mislabeled 0/0 (no-data) and
--   good-direction excursions as REGRESSION; this fixture pins every branch of the
--   verdict map so that regression can never re-enter the Kafka dashboard silently.
--
-- HOW IT IS USED
--   benchmarks/e2e/dashboard/test_verdicts.sh loads the byte-locked perf.* DDL,
--   applies this fixture, creates the view, and asserts each case's actual verdict
--   equals the EXPECTED verdict documented inline below (case -> expected).
--
-- ISOLATION
--   All rows use connector='__verdict_fixture__' and run_id prefix 'fixture-'.
--   The view (v_kc_pair_ratios.sql) EXCLUDES connector='__verdict_fixture__', so
--   these rows never mix into production verdicts. This file is IDEMPOTENT: it
--   deletes all fixture rows (by connector + run_id prefix) before re-inserting.
--
-- DIRECTION CARRIERS
--   higher_better  -> null_drain_rows_per_sec (Tier 0, band +-3% => 0.03)
--   lower_better   -> parts_per_insert        (Tier 1, band +-5% => 0.05)
--   threshold rule -> ch_avg_rows_per_insert  (Tier 1, >=50k contract)
--
-- RATIO CONSTRUCTION: ratio = head_value / pinned_value.
--   0.90 => head=90,  pinned=100
--   1.00 => head=100, pinned=100
--   1.10 => head=110, pinned=100
--   NULL (one side missing) => head metric row present, pinned metric row ABSENT
--   0-denominator          => pinned_value = 0 (=> ratio NULL by the NULL-safe rule)
--
-- VERDICT PRECEDENCE (v_kc_pair_ratios.sql): NO_DATA -> FLAGGED -> band/threshold -> OK.
--   Therefore a NULL/0-denominator case is NO_DATA even when flagged; the flagged
--   cases below are all built on comparable ratios so FLAGGED is the live outcome.
--
-- =====================================================================================
--  TRUTH TABLE (case pair_id -> expected verdict). test_verdicts.sh asserts each.
-- -------------------------------------------------------------------------------------
--  pair_id (fixture-*)                 tier metric                       expect
-- -------------------------------------------------------------------------------------
--  hb-090-unflagged                    0    null_drain_rows_per_sec      REGRESSION
--  hb-100-unflagged                    0    null_drain_rows_per_sec      OK
--  hb-110-unflagged                    0    null_drain_rows_per_sec      IMPROVEMENT
--  hb-null-unflagged                   0    null_drain_rows_per_sec      NO_DATA
--  hb-zerodenom-unflagged              0    null_drain_rows_per_sec      NO_DATA
--  hb-090-flagged                      0    null_drain_rows_per_sec      FLAGGED
--  hb-100-flagged                      0    null_drain_rows_per_sec      FLAGGED
--  hb-110-flagged                      0    null_drain_rows_per_sec      FLAGGED
--  lb-090-unflagged                    1    parts_per_insert             IMPROVEMENT
--  lb-100-unflagged                    1    parts_per_insert             OK
--  lb-110-unflagged                    1    parts_per_insert             REGRESSION
--  lb-null-unflagged                   1    parts_per_insert             NO_DATA
--  lb-zerodenom-unflagged              1    parts_per_insert             NO_DATA
--  lb-090-flagged                      1    parts_per_insert             IMPROVEMENT->FLAGGED
--  lb-100-flagged                      1    parts_per_insert             OK->FLAGGED
--  lb-110-flagged                      1    parts_per_insert             REGRESSION->FLAGGED
--  thr-above-unflagged                 1    ch_avg_rows_per_insert       OK        (head=60000)
--  thr-below-unflagged                 1    ch_avg_rows_per_insert       REGRESSION(head=40000)
--  thr-null-unflagged                  1    ch_avg_rows_per_insert       NO_DATA   (head missing)
--  thr-above-flagged                   1    ch_avg_rows_per_insert       FLAGGED   (head=60000)
--  failed-run                          1    parts_per_insert             (EXCLUDED — no row)
-- =====================================================================================
--
-- The 'flagged' cases above document both the underlying band outcome and the
-- FLAGGED override the view must apply; the asserted expectation is FLAGGED.
-- The 'failed-run' case sets runtime['outcome']='failed' on BOTH arms; the view
-- excludes it BY OUTCOME VALUE, so it must produce NO pairs row at all.

-- ---- idempotency: remove any prior fixture rows first ----
DELETE FROM perf.runs    WHERE connector = '__verdict_fixture__' OR run_id LIKE 'fixture-%';
DELETE FROM perf.metrics WHERE run_id LIKE 'fixture-%';

-- =====================================================================================
--  perf.runs — one (arm, tier) row per fixture run. run_id = fixture-<pair>-<arm>-t<tier>
-- =====================================================================================
INSERT INTO perf.runs
  (run_id, run_started_at, run_ended_at, git_sha, connector, connector_version, runtime)
VALUES
  ('fixture-hb-090-unflagged-head-t0',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-hb-090-unflagged','outcome':'success'}),
  ('fixture-hb-090-unflagged-pinned-t0', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-hb-090-unflagged','outcome':'success'}),
  ('fixture-hb-100-unflagged-head-t0',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-hb-100-unflagged','outcome':'success'}),
  ('fixture-hb-100-unflagged-pinned-t0', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-hb-100-unflagged','outcome':'success'}),
  ('fixture-hb-110-unflagged-head-t0',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-hb-110-unflagged','outcome':'success'}),
  ('fixture-hb-110-unflagged-pinned-t0', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-hb-110-unflagged','outcome':'success'}),
  ('fixture-hb-null-unflagged-head-t0',       now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-hb-null-unflagged','outcome':'success'}),
  ('fixture-hb-null-unflagged-pinned-t0',     now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-hb-null-unflagged','outcome':'success'}),
  ('fixture-hb-zerodenom-unflagged-head-t0',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-hb-zerodenom-unflagged','outcome':'success'}),
  ('fixture-hb-zerodenom-unflagged-pinned-t0', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-hb-zerodenom-unflagged','outcome':'success'}),
  ('fixture-hb-090-flagged-head-t0',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-hb-090-flagged','outcome':'success','flagged':'1','flag_reason':'rebalance'}),
  ('fixture-hb-090-flagged-pinned-t0', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-hb-090-flagged','outcome':'success'}),
  ('fixture-hb-100-flagged-head-t0',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-hb-100-flagged','outcome':'success','flagged':'1','flag_reason':'task_restart'}),
  ('fixture-hb-100-flagged-pinned-t0', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-hb-100-flagged','outcome':'success'}),
  ('fixture-hb-110-flagged-head-t0',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-hb-110-flagged','outcome':'success','flagged':'1','flag_reason':'settle_timeout'}),
  ('fixture-hb-110-flagged-pinned-t0', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-hb-110-flagged','outcome':'success'}),

  ('fixture-lb-090-unflagged-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-lb-090-unflagged','outcome':'success'}),
  ('fixture-lb-090-unflagged-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-lb-090-unflagged','outcome':'success'}),
  ('fixture-lb-100-unflagged-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-lb-100-unflagged','outcome':'success'}),
  ('fixture-lb-100-unflagged-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-lb-100-unflagged','outcome':'success'}),
  ('fixture-lb-110-unflagged-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-lb-110-unflagged','outcome':'success'}),
  ('fixture-lb-110-unflagged-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-lb-110-unflagged','outcome':'success'}),
  ('fixture-lb-null-unflagged-head-t1',        now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-lb-null-unflagged','outcome':'success'}),
  ('fixture-lb-null-unflagged-pinned-t1',      now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-lb-null-unflagged','outcome':'success'}),
  ('fixture-lb-zerodenom-unflagged-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-lb-zerodenom-unflagged','outcome':'success'}),
  ('fixture-lb-zerodenom-unflagged-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-lb-zerodenom-unflagged','outcome':'success'}),
  ('fixture-lb-090-flagged-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-lb-090-flagged','outcome':'success'}),
  ('fixture-lb-090-flagged-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-lb-090-flagged','outcome':'success','flagged':'1','flag_reason':'rebalance'}),
  ('fixture-lb-100-flagged-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-lb-100-flagged','outcome':'success'}),
  ('fixture-lb-100-flagged-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-lb-100-flagged','outcome':'success','flagged':'1','flag_reason':'drain_incomplete'}),
  ('fixture-lb-110-flagged-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-lb-110-flagged','outcome':'success'}),
  ('fixture-lb-110-flagged-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-lb-110-flagged','outcome':'success','flagged':'1','flag_reason':'task_retries'}),

  ('fixture-thr-above-unflagged-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-thr-above-unflagged','outcome':'success'}),
  ('fixture-thr-above-unflagged-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-thr-above-unflagged','outcome':'success'}),
  ('fixture-thr-below-unflagged-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-thr-below-unflagged','outcome':'success'}),
  ('fixture-thr-below-unflagged-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-thr-below-unflagged','outcome':'success'}),
  ('fixture-thr-null-unflagged-head-t1',    now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-thr-null-unflagged','outcome':'success'}),
  ('fixture-thr-null-unflagged-pinned-t1',  now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-thr-null-unflagged','outcome':'success'}),
  ('fixture-thr-above-flagged-head-t1',     now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-thr-above-flagged','outcome':'success','flagged':'1','flag_reason':'task_restart'}),
  ('fixture-thr-above-flagged-pinned-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-thr-above-flagged','outcome':'success'}),

  ('fixture-failed-run-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-failed-run','outcome':'failed'}),
  ('fixture-failed-run-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-failed-run','outcome':'failed'});

-- =====================================================================================
--  perf.metrics — the values that drive each ratio / threshold.
--  head=90/100/110 vs pinned=100 gives ratios 0.90/1.00/1.10.
--  NULL case: head row present, pinned row ABSENT. 0-denom: pinned value = 0.
-- =====================================================================================
INSERT INTO perf.metrics (run_id, metric_name, unit, value) VALUES
  ('fixture-hb-090-unflagged-head-t0',   'null_drain_rows_per_sec', 'rows/s',  90),
  ('fixture-hb-090-unflagged-pinned-t0', 'null_drain_rows_per_sec', 'rows/s', 100),
  ('fixture-hb-100-unflagged-head-t0',   'null_drain_rows_per_sec', 'rows/s', 100),
  ('fixture-hb-100-unflagged-pinned-t0', 'null_drain_rows_per_sec', 'rows/s', 100),
  ('fixture-hb-110-unflagged-head-t0',   'null_drain_rows_per_sec', 'rows/s', 110),
  ('fixture-hb-110-unflagged-pinned-t0', 'null_drain_rows_per_sec', 'rows/s', 100),
  ('fixture-hb-null-unflagged-head-t0',  'null_drain_rows_per_sec', 'rows/s', 100),
  ('fixture-hb-zerodenom-unflagged-head-t0',   'null_drain_rows_per_sec', 'rows/s', 100),
  ('fixture-hb-zerodenom-unflagged-pinned-t0', 'null_drain_rows_per_sec', 'rows/s',   0),
  ('fixture-hb-090-flagged-head-t0',   'null_drain_rows_per_sec', 'rows/s',  90),
  ('fixture-hb-090-flagged-pinned-t0', 'null_drain_rows_per_sec', 'rows/s', 100),
  ('fixture-hb-100-flagged-head-t0',   'null_drain_rows_per_sec', 'rows/s', 100),
  ('fixture-hb-100-flagged-pinned-t0', 'null_drain_rows_per_sec', 'rows/s', 100),
  ('fixture-hb-110-flagged-head-t0',   'null_drain_rows_per_sec', 'rows/s', 110),
  ('fixture-hb-110-flagged-pinned-t0', 'null_drain_rows_per_sec', 'rows/s', 100),

  ('fixture-lb-090-unflagged-head-t1',   'parts_per_insert', 'ratio',  90),
  ('fixture-lb-090-unflagged-pinned-t1', 'parts_per_insert', 'ratio', 100),
  ('fixture-lb-100-unflagged-head-t1',   'parts_per_insert', 'ratio', 100),
  ('fixture-lb-100-unflagged-pinned-t1', 'parts_per_insert', 'ratio', 100),
  ('fixture-lb-110-unflagged-head-t1',   'parts_per_insert', 'ratio', 110),
  ('fixture-lb-110-unflagged-pinned-t1', 'parts_per_insert', 'ratio', 100),
  ('fixture-lb-null-unflagged-head-t1',  'parts_per_insert', 'ratio', 100),
  ('fixture-lb-zerodenom-unflagged-head-t1',   'parts_per_insert', 'ratio', 100),
  ('fixture-lb-zerodenom-unflagged-pinned-t1', 'parts_per_insert', 'ratio',   0),
  ('fixture-lb-090-flagged-head-t1',   'parts_per_insert', 'ratio',  90),
  ('fixture-lb-090-flagged-pinned-t1', 'parts_per_insert', 'ratio', 100),
  ('fixture-lb-100-flagged-head-t1',   'parts_per_insert', 'ratio', 100),
  ('fixture-lb-100-flagged-pinned-t1', 'parts_per_insert', 'ratio', 100),
  ('fixture-lb-110-flagged-head-t1',   'parts_per_insert', 'ratio', 110),
  ('fixture-lb-110-flagged-pinned-t1', 'parts_per_insert', 'ratio', 100),

  ('fixture-thr-above-unflagged-head-t1',   'ch_avg_rows_per_insert', 'rows', 60000),
  ('fixture-thr-above-unflagged-pinned-t1', 'ch_avg_rows_per_insert', 'rows', 60000),
  ('fixture-thr-below-unflagged-head-t1',   'ch_avg_rows_per_insert', 'rows', 40000),
  ('fixture-thr-below-unflagged-pinned-t1', 'ch_avg_rows_per_insert', 'rows', 60000),
  ('fixture-thr-null-unflagged-pinned-t1',  'ch_avg_rows_per_insert', 'rows', 60000),
  ('fixture-thr-above-flagged-head-t1',     'ch_avg_rows_per_insert', 'rows', 60000),
  ('fixture-thr-above-flagged-pinned-t1',   'ch_avg_rows_per_insert', 'rows', 60000),

  ('fixture-failed-run-head-t1',   'parts_per_insert', 'ratio', 110),
  ('fixture-failed-run-pinned-t1', 'parts_per_insert', 'ratio', 100);
