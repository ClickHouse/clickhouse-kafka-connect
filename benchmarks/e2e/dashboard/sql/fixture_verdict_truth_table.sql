-- fixture_verdict_truth_table.sql — synthetic acceptance fixture for the verdict
-- map in v_kc_pair_ratios.sql (Benchmark v2 Kafka dashboard, #33; extended for
-- contract Amendment 2026-07-09b in Stage A2).
--
-- PURPOSE
--   The principal mandate: any verdict-emitting artifact requires fixture-based
--   acceptance BEFORE it ships. This fixture pins every branch of the AMENDED
--   verdict map: calibrated per-metric bands (edges tested just-inside and
--   just-outside in both directions), the parts_per_insert TRIPWIRE, the
--   merge_amplification watch-only and ch_avg_rows_per_insert degated exclusions,
--   and the PINNED precedence FLAG > NO_DATA/TRIPWIRE/band.
--
-- HOW IT IS USED
--   benchmarks/e2e/dashboard/test_verdicts.sh loads the byte-locked perf.* DDL,
--   applies this fixture, creates the view, and asserts each case's actual verdict
--   equals the EXPECTED verdict documented inline below. The DWH twin is asserted
--   by benchmarks/e2e/dashboard/superset/verify_verdict_dwh.sh with the same
--   truth table.
--
-- ISOLATION
--   All rows use connector='__verdict_fixture__' and run_id prefix 'fixture-'.
--   The view EXCLUDES connector='__verdict_fixture__', so these rows never mix
--   into production verdicts. This file is IDEMPOTENT: it deletes all fixture
--   rows (by connector + run_id prefix) before re-inserting.
--
-- CARRIERS (Amendment 2026-07-09b registry, Kafka spellings)
--   higher_better banded  -> null_drain_rows_per_sec  (Tier 0, ±8.5% => [0.915, 1.085])
--   lower_better  banded  -> connect_cpu_seconds_per_Mrows (Tier 0, ±6% => [0.94, 1.06])
--   band-edge cells       -> drain_rows_per_sec        (Tier 1, ±8.5%)
--   TRIPWIRE              -> parts_per_insert          (Tier 1, head==1.0 OK else TRIPWIRE)
--   watch-only exclusion  -> merge_amplification       (must emit NO verdict row)
--   degated exclusion     -> ch_avg_rows_per_insert    (must emit NO verdict row)
--
-- RATIO CONSTRUCTION: ratio = head_value / pinned_value (pinned = 100 unless noted).
--   0.90 => head=90 … 1.10 => head=110. Edge cells: 1.08 / 1.09 / 0.92 / 0.91
--   around the ±8.5% band; 1.05 / 1.07 / 0.95 / 0.93 around the ±6% band.
--   NULL (one side missing) => that side's metric row ABSENT.
--   0-denominator          => pinned_value = 0 (=> ratio NULL by the NULL-safe rule).
--
-- VERDICT PRECEDENCE (PINNED, Amendment 2026-07-09b): FLAG > NO_DATA / TRIPWIRE /
--   IMPROVEMENT / REGRESSION / OK. NOTE: FLAG now beats NO_DATA (reversed from the
--   pre-amendment map) — the hb-null-flagged cell asserts exactly this.
--
-- =====================================================================================
--  TRUTH TABLE (case pair_id -> expected verdict). test_verdicts.sh asserts each.
-- -------------------------------------------------------------------------------------
--  pair_id (fixture-*)          tier metric                          expect
-- -------------------------------------------------------------------------------------
--  hb-090-unflagged             0    null_drain_rows_per_sec         REGRESSION  (0.90 < 0.915)
--  hb-100-unflagged             0    null_drain_rows_per_sec         OK
--  hb-110-unflagged             0    null_drain_rows_per_sec         IMPROVEMENT (1.10 > 1.085)
--  hb-null-unflagged            0    null_drain_rows_per_sec         NO_DATA (pinned absent)
--  hb-zerodenom-unflagged       0    null_drain_rows_per_sec         NO_DATA (pinned = 0)
--  hb-090-flagged               0    null_drain_rows_per_sec         FLAGGED
--  hb-100-flagged               0    null_drain_rows_per_sec         FLAGGED
--  hb-110-flagged               0    null_drain_rows_per_sec         FLAGGED
--  hb-null-flagged              0    null_drain_rows_per_sec         FLAGGED (flag > NO_DATA)
--  drain-1080-unflagged         1    drain_rows_per_sec              OK          (1.08  < 1.085, edge inside)
--  drain-1090-unflagged         1    drain_rows_per_sec              IMPROVEMENT (1.09  > 1.085, edge outside)
--  drain-0920-unflagged         1    drain_rows_per_sec              OK          (0.92  > 0.915, edge inside)
--  drain-0910-unflagged         1    drain_rows_per_sec              REGRESSION  (0.91  < 0.915, edge outside)
--  cpu-090-unflagged            0    connect_cpu_seconds_per_Mrows   IMPROVEMENT (lb, 0.90 < 0.94)
--  cpu-100-unflagged            0    connect_cpu_seconds_per_Mrows   OK
--  cpu-110-unflagged            0    connect_cpu_seconds_per_Mrows   REGRESSION  (lb, 1.10 > 1.06)
--  cpu-null-unflagged           0    connect_cpu_seconds_per_Mrows   NO_DATA (pinned absent)
--  cpu-zerodenom-unflagged      0    connect_cpu_seconds_per_Mrows   NO_DATA (pinned = 0)
--  cpu-090-flagged              0    connect_cpu_seconds_per_Mrows   FLAGGED
--  cpu-100-flagged              0    connect_cpu_seconds_per_Mrows   FLAGGED
--  cpu-110-flagged              0    connect_cpu_seconds_per_Mrows   FLAGGED
--  cpu-1050-unflagged           0    connect_cpu_seconds_per_Mrows   OK          (1.05 < 1.06, edge inside)
--  cpu-1070-unflagged           0    connect_cpu_seconds_per_Mrows   REGRESSION  (1.07 > 1.06, edge outside, bad dir)
--  cpu-0950-unflagged           0    connect_cpu_seconds_per_Mrows   OK          (0.95 > 0.94, edge inside)
--  cpu-0930-unflagged           0    connect_cpu_seconds_per_Mrows   IMPROVEMENT (0.93 < 0.94, edge outside, good dir)
--  tw-ok-unflagged              1    parts_per_insert                OK          (head = 1.0)
--  tw-armed-high-unflagged      1    parts_per_insert                TRIPWIRE    (head = 1.02)
--  tw-armed-low-unflagged       1    parts_per_insert                TRIPWIRE    (head = 0.98)
--  tw-null-unflagged            1    parts_per_insert                NO_DATA     (head absent)
--  tw-armed-flagged             1    parts_per_insert                FLAGGED     (flag > armed tripwire)
--  tw-ok-pinnedmissing-unflagged 1   parts_per_insert                OK          (head = 1.0; pinned metric ABSENT —
--                                                                                 tripwire ignores pinned/ratio)
--  watch-pair-unflagged         1    merge_amplification             (NO ROW — watch-only)
--  watch-pair-unflagged         1    ch_avg_rows_per_insert          (NO ROW — degated)
--  failed-run                   1    parts_per_insert                (EXCLUDED — no row; outcome='failed')
-- =====================================================================================
--
-- The 'flagged' band cases are built on comparable ratios so the FLAGGED override —
-- not NO_DATA — is what is asserted there; hb-null-flagged is the one deliberate
-- flag-with-NULL cell (asserting FLAG > NO_DATA). tw-armed-flagged asserts
-- FLAG > armed TRIPWIRE. The 'failed-run' case sets runtime['outcome']='failed' on
-- BOTH arms; the view excludes it BY OUTCOME VALUE, so it must produce NO row.
-- watch-pair-unflagged carries an enormous merge_amplification excursion (2.5x
-- ratio) and a sub-50k ch_avg_rows_per_insert (40000) — values that would have
-- verdicted under the PRE-amendment map — to prove both are now excluded.

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
  ('fixture-hb-null-flagged-head-t0',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-hb-null-flagged','outcome':'success','flagged':'1','flag_reason':'drain_incomplete'}),
  ('fixture-hb-null-flagged-pinned-t0', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-hb-null-flagged','outcome':'success'}),

  ('fixture-drain-1080-unflagged-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-drain-1080-unflagged','outcome':'success'}),
  ('fixture-drain-1080-unflagged-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-drain-1080-unflagged','outcome':'success'}),
  ('fixture-drain-1090-unflagged-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-drain-1090-unflagged','outcome':'success'}),
  ('fixture-drain-1090-unflagged-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-drain-1090-unflagged','outcome':'success'}),
  ('fixture-drain-0920-unflagged-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-drain-0920-unflagged','outcome':'success'}),
  ('fixture-drain-0920-unflagged-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-drain-0920-unflagged','outcome':'success'}),
  ('fixture-drain-0910-unflagged-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-drain-0910-unflagged','outcome':'success'}),
  ('fixture-drain-0910-unflagged-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-drain-0910-unflagged','outcome':'success'}),

  ('fixture-cpu-090-unflagged-head-t0',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-cpu-090-unflagged','outcome':'success'}),
  ('fixture-cpu-090-unflagged-pinned-t0', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-cpu-090-unflagged','outcome':'success'}),
  ('fixture-cpu-100-unflagged-head-t0',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-cpu-100-unflagged','outcome':'success'}),
  ('fixture-cpu-100-unflagged-pinned-t0', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-cpu-100-unflagged','outcome':'success'}),
  ('fixture-cpu-110-unflagged-head-t0',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-cpu-110-unflagged','outcome':'success'}),
  ('fixture-cpu-110-unflagged-pinned-t0', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-cpu-110-unflagged','outcome':'success'}),
  ('fixture-cpu-null-unflagged-head-t0',       now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-cpu-null-unflagged','outcome':'success'}),
  ('fixture-cpu-null-unflagged-pinned-t0',     now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-cpu-null-unflagged','outcome':'success'}),
  ('fixture-cpu-zerodenom-unflagged-head-t0',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-cpu-zerodenom-unflagged','outcome':'success'}),
  ('fixture-cpu-zerodenom-unflagged-pinned-t0', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-cpu-zerodenom-unflagged','outcome':'success'}),
  ('fixture-cpu-090-flagged-head-t0',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-cpu-090-flagged','outcome':'success'}),
  ('fixture-cpu-090-flagged-pinned-t0', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-cpu-090-flagged','outcome':'success','flagged':'1','flag_reason':'rebalance'}),
  ('fixture-cpu-100-flagged-head-t0',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-cpu-100-flagged','outcome':'success'}),
  ('fixture-cpu-100-flagged-pinned-t0', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-cpu-100-flagged','outcome':'success','flagged':'1','flag_reason':'drain_incomplete'}),
  ('fixture-cpu-110-flagged-head-t0',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-cpu-110-flagged','outcome':'success'}),
  ('fixture-cpu-110-flagged-pinned-t0', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-cpu-110-flagged','outcome':'success','flagged':'1','flag_reason':'task_retries'}),
  ('fixture-cpu-1050-unflagged-head-t0',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-cpu-1050-unflagged','outcome':'success'}),
  ('fixture-cpu-1050-unflagged-pinned-t0', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-cpu-1050-unflagged','outcome':'success'}),
  ('fixture-cpu-1070-unflagged-head-t0',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-cpu-1070-unflagged','outcome':'success'}),
  ('fixture-cpu-1070-unflagged-pinned-t0', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-cpu-1070-unflagged','outcome':'success'}),
  ('fixture-cpu-0950-unflagged-head-t0',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-cpu-0950-unflagged','outcome':'success'}),
  ('fixture-cpu-0950-unflagged-pinned-t0', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-cpu-0950-unflagged','outcome':'success'}),
  ('fixture-cpu-0930-unflagged-head-t0',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'0','pair_id':'fixture-cpu-0930-unflagged','outcome':'success'}),
  ('fixture-cpu-0930-unflagged-pinned-t0', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'0','pair_id':'fixture-cpu-0930-unflagged','outcome':'success'}),

  ('fixture-tw-ok-unflagged-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-tw-ok-unflagged','outcome':'success'}),
  ('fixture-tw-ok-unflagged-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-tw-ok-unflagged','outcome':'success'}),
  ('fixture-tw-armed-high-unflagged-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-tw-armed-high-unflagged','outcome':'success'}),
  ('fixture-tw-armed-high-unflagged-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-tw-armed-high-unflagged','outcome':'success'}),
  ('fixture-tw-armed-low-unflagged-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-tw-armed-low-unflagged','outcome':'success'}),
  ('fixture-tw-armed-low-unflagged-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-tw-armed-low-unflagged','outcome':'success'}),
  ('fixture-tw-null-unflagged-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-tw-null-unflagged','outcome':'success'}),
  ('fixture-tw-null-unflagged-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-tw-null-unflagged','outcome':'success'}),
  ('fixture-tw-armed-flagged-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-tw-armed-flagged','outcome':'success','flagged':'1','flag_reason':'task_restart'}),
  ('fixture-tw-armed-flagged-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-tw-armed-flagged','outcome':'success'}),
  ('fixture-tw-ok-pinnedmissing-unflagged-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-tw-ok-pinnedmissing-unflagged','outcome':'success'}),
  ('fixture-tw-ok-pinnedmissing-unflagged-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-tw-ok-pinnedmissing-unflagged','outcome':'success'}),

  ('fixture-watch-pair-unflagged-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-watch-pair-unflagged','outcome':'success'}),
  ('fixture-watch-pair-unflagged-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-watch-pair-unflagged','outcome':'success'}),

  ('fixture-failed-run-head-t1',   now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'head','tier':'1','pair_id':'fixture-failed-run','outcome':'failed'}),
  ('fixture-failed-run-pinned-t1', now(), now(), 'fix', '__verdict_fixture__', 'v', {'arm':'pinned','tier':'1','pair_id':'fixture-failed-run','outcome':'failed'});

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
  ('fixture-hb-null-flagged-head-t0',  'null_drain_rows_per_sec', 'rows/s', 100),

  ('fixture-drain-1080-unflagged-head-t1',   'drain_rows_per_sec', 'rows/s', 1080),
  ('fixture-drain-1080-unflagged-pinned-t1', 'drain_rows_per_sec', 'rows/s', 1000),
  ('fixture-drain-1090-unflagged-head-t1',   'drain_rows_per_sec', 'rows/s', 1090),
  ('fixture-drain-1090-unflagged-pinned-t1', 'drain_rows_per_sec', 'rows/s', 1000),
  ('fixture-drain-0920-unflagged-head-t1',   'drain_rows_per_sec', 'rows/s',  920),
  ('fixture-drain-0920-unflagged-pinned-t1', 'drain_rows_per_sec', 'rows/s', 1000),
  ('fixture-drain-0910-unflagged-head-t1',   'drain_rows_per_sec', 'rows/s',  910),
  ('fixture-drain-0910-unflagged-pinned-t1', 'drain_rows_per_sec', 'rows/s', 1000),

  ('fixture-cpu-090-unflagged-head-t0',   'connect_cpu_seconds_per_Mrows', 's/Mrows',  90),
  ('fixture-cpu-090-unflagged-pinned-t0', 'connect_cpu_seconds_per_Mrows', 's/Mrows', 100),
  ('fixture-cpu-100-unflagged-head-t0',   'connect_cpu_seconds_per_Mrows', 's/Mrows', 100),
  ('fixture-cpu-100-unflagged-pinned-t0', 'connect_cpu_seconds_per_Mrows', 's/Mrows', 100),
  ('fixture-cpu-110-unflagged-head-t0',   'connect_cpu_seconds_per_Mrows', 's/Mrows', 110),
  ('fixture-cpu-110-unflagged-pinned-t0', 'connect_cpu_seconds_per_Mrows', 's/Mrows', 100),
  ('fixture-cpu-null-unflagged-head-t0',  'connect_cpu_seconds_per_Mrows', 's/Mrows', 100),
  ('fixture-cpu-zerodenom-unflagged-head-t0',   'connect_cpu_seconds_per_Mrows', 's/Mrows', 100),
  ('fixture-cpu-zerodenom-unflagged-pinned-t0', 'connect_cpu_seconds_per_Mrows', 's/Mrows',   0),
  ('fixture-cpu-090-flagged-head-t0',   'connect_cpu_seconds_per_Mrows', 's/Mrows',  90),
  ('fixture-cpu-090-flagged-pinned-t0', 'connect_cpu_seconds_per_Mrows', 's/Mrows', 100),
  ('fixture-cpu-100-flagged-head-t0',   'connect_cpu_seconds_per_Mrows', 's/Mrows', 100),
  ('fixture-cpu-100-flagged-pinned-t0', 'connect_cpu_seconds_per_Mrows', 's/Mrows', 100),
  ('fixture-cpu-110-flagged-head-t0',   'connect_cpu_seconds_per_Mrows', 's/Mrows', 110),
  ('fixture-cpu-110-flagged-pinned-t0', 'connect_cpu_seconds_per_Mrows', 's/Mrows', 100),
  ('fixture-cpu-1050-unflagged-head-t0',   'connect_cpu_seconds_per_Mrows', 's/Mrows', 1050),
  ('fixture-cpu-1050-unflagged-pinned-t0', 'connect_cpu_seconds_per_Mrows', 's/Mrows', 1000),
  ('fixture-cpu-1070-unflagged-head-t0',   'connect_cpu_seconds_per_Mrows', 's/Mrows', 1070),
  ('fixture-cpu-1070-unflagged-pinned-t0', 'connect_cpu_seconds_per_Mrows', 's/Mrows', 1000),
  ('fixture-cpu-0950-unflagged-head-t0',   'connect_cpu_seconds_per_Mrows', 's/Mrows',  950),
  ('fixture-cpu-0950-unflagged-pinned-t0', 'connect_cpu_seconds_per_Mrows', 's/Mrows', 1000),
  ('fixture-cpu-0930-unflagged-head-t0',   'connect_cpu_seconds_per_Mrows', 's/Mrows',  930),
  ('fixture-cpu-0930-unflagged-pinned-t0', 'connect_cpu_seconds_per_Mrows', 's/Mrows', 1000),

  ('fixture-tw-ok-unflagged-head-t1',           'parts_per_insert', 'ratio', 1.0),
  ('fixture-tw-ok-unflagged-pinned-t1',         'parts_per_insert', 'ratio', 1.0),
  ('fixture-tw-armed-high-unflagged-head-t1',   'parts_per_insert', 'ratio', 1.02),
  ('fixture-tw-armed-high-unflagged-pinned-t1', 'parts_per_insert', 'ratio', 1.0),
  ('fixture-tw-armed-low-unflagged-head-t1',    'parts_per_insert', 'ratio', 0.98),
  ('fixture-tw-armed-low-unflagged-pinned-t1',  'parts_per_insert', 'ratio', 1.0),
  ('fixture-tw-null-unflagged-pinned-t1',       'parts_per_insert', 'ratio', 1.0),
  ('fixture-tw-armed-flagged-head-t1',          'parts_per_insert', 'ratio', 1.02),
  ('fixture-tw-armed-flagged-pinned-t1',        'parts_per_insert', 'ratio', 1.0),
  ('fixture-tw-ok-pinnedmissing-unflagged-head-t1', 'parts_per_insert', 'ratio', 1.0),

  ('fixture-watch-pair-unflagged-head-t1',   'merge_amplification',    'ratio',     5.0),
  ('fixture-watch-pair-unflagged-pinned-t1', 'merge_amplification',    'ratio',     2.0),
  ('fixture-watch-pair-unflagged-head-t1',   'ch_avg_rows_per_insert', 'rows',  40000),
  ('fixture-watch-pair-unflagged-pinned-t1', 'ch_avg_rows_per_insert', 'rows',  60000),

  ('fixture-failed-run-head-t1',   'parts_per_insert', 'ratio', 1.02),
  ('fixture-failed-run-pinned-t1', 'parts_per_insert', 'ratio', 1.0);
