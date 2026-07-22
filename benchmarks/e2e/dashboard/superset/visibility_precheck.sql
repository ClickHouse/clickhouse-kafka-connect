-- =============================================================================
-- visibility_precheck.sql  —  Stage B FIRST ACTION (issue #33)
-- =============================================================================
-- Run these against the DWH ("DWH" connection, uuid dc93cd97, READ-ONLY) via the
-- authorized Stage-B mechanism BEFORE creating any dataset. If Q1 does not return
-- the 8 expected run_ids, or Q2/Q3 quarantine checks fail, ABORT LOUDLY and do not
-- proceed — the dashboard would render empty or wrong.
--
-- Expected topology (task #33):
--   Pair 1 = 2026-07-08T11-52-46Z-e140231  -> FLAGGED instrument_resize
--            (excluded from ratios/bands; visible in v_kc_flagged_log)
--   Pair 2 = 2026-07-09T10-04-24Z-e4b1f7a  -> CLEAN; the sole n=1 calibration point
--   8 runs total = 2 pairs x 2 arms (head, pinned) x 2 tiers (0, 1).
-- The exact run_id spelling is <pair_id>-<arm>-t<tier> (contract §1.2), e.g.
--   2026-07-08T11-52-46Z-e140231-head-t0 ... -pinned-t1.
-- =============================================================================

-- ---- Q1: the 8 kafka run_ids across the two pairs are present -----------------
-- EXPECT: 8 rows; 4 per pair_id; arms {head,pinned} x tiers {0,1} per pair.
SELECT
  runtime['pair_id']                            AS pair_id,
  runtime['arm']                                AS arm,
  runtime['tier']                               AS tier,
  run_id,
  runtime['outcome']                            AS outcome,
  runtime['flagged']                            AS flagged,
  runtime['flag_reason']                        AS flag_reason
FROM raw_connectors_load_testing.runs
WHERE connector = 'kafka-connect'
  AND runtime['pair_id'] IN (
    '2026-07-08T11-52-46Z-e140231',
    '2026-07-09T10-04-24Z-e4b1f7a'
  )
ORDER BY pair_id, tier, arm;

-- ---- Q1b: run counts per pair (both should be 4) ------------------------------
SELECT runtime['pair_id'] AS pair_id, count() AS n_runs
FROM raw_connectors_load_testing.runs
WHERE connector = 'kafka-connect'
  AND runtime['pair_id'] IN ('2026-07-08T11-52-46Z-e140231','2026-07-09T10-04-24Z-e4b1f7a')
GROUP BY pair_id;

-- ---- Q2: pair-1 quarantine verification (the instrument_resize flag) ----------
-- EXPECT: every pair-1 row has flagged='1', flag_reason contains 'instrument_resize'.
-- The task notes the THREE CORRECTED rows carry batch_size='100000'; surfaced here.
SELECT
  run_id,
  runtime['flagged']                            AS flagged,
  runtime['flag_reason']                        AS flag_reason,
  runtime['batch_size']                         AS batch_size,
  (runtime['flagged'] = '1'
   AND position(runtime['flag_reason'], 'instrument_resize') > 0) AS quarantine_ok
FROM raw_connectors_load_testing.runs
WHERE connector = 'kafka-connect'
  AND runtime['pair_id'] = '2026-07-08T11-52-46Z-e140231'
ORDER BY run_id;

-- ---- Q2b: assertion form — must return 0 (no un-quarantined pair-1 rows) ------
-- If this is > 0, pair-1 is NOT fully flagged -> ABORT (it would leak into ratios).
SELECT count() AS pair1_unquarantined_rows
FROM raw_connectors_load_testing.runs
WHERE connector = 'kafka-connect'
  AND runtime['pair_id'] = '2026-07-08T11-52-46Z-e140231'
  AND NOT (runtime['flagged'] = '1'
           AND position(runtime['flag_reason'], 'instrument_resize') > 0);

-- ---- Q2c: the three corrected rows carry batch_size='100000' ------------------
-- EXPECT: 3 (informational — confirms the correction landed on the DWH).
SELECT count() AS pair1_rows_batchsize_100k
FROM raw_connectors_load_testing.runs
WHERE connector = 'kafka-connect'
  AND runtime['pair_id'] = '2026-07-08T11-52-46Z-e140231'
  AND runtime['batch_size'] = '100000';

-- ---- Q3: pair-2 rows are CLEAN (no flagged key) -------------------------------
-- EXPECT: clean_ok = 1 for all 4 rows; mapContains(runtime,'flagged') = 0.
SELECT
  run_id,
  mapContains(runtime, 'flagged')               AS has_flagged_key,
  runtime['outcome']                            AS outcome,
  (mapContains(runtime, 'flagged') = 0
   AND coalesce(nullIf(runtime['outcome'], ''), 'success') != 'failed') AS clean_ok
FROM raw_connectors_load_testing.runs
WHERE connector = 'kafka-connect'
  AND runtime['pair_id'] = '2026-07-09T10-04-24Z-e4b1f7a'
ORDER BY run_id;

-- ---- Q3b: assertion form — must return 0 (no non-clean pair-2 rows) -----------
SELECT count() AS pair2_nonclean_rows
FROM raw_connectors_load_testing.runs
WHERE connector = 'kafka-connect'
  AND runtime['pair_id'] = '2026-07-09T10-04-24Z-e4b1f7a'
  AND mapContains(runtime, 'flagged') = 1;

-- ---- Q4: the created views resolve and pair-2 produces verdict rows -----------
-- Run AFTER creating the datasets. EXPECT: pair-2 present in v_kc_pair_ratios with
-- provisional=1 (n=1 calibration); pair-1 ABSENT (flag-excluded); pair-1 present in
-- v_kc_flagged_log with log_class='FLAGGED'.
SELECT pair_id, tier, metric_name, verdict, provisional, comparable_pairs
FROM raw_connectors_load_testing.v_kc_pair_ratios
ORDER BY pair_id, tier, metric_name;

SELECT count() AS pair1_in_ratios_should_be_0
FROM raw_connectors_load_testing.v_kc_pair_ratios
WHERE pair_id = '2026-07-08T11-52-46Z-e140231';

SELECT pair_id, arm, tier, log_class, flag_reason
FROM raw_connectors_load_testing.v_kc_flagged_log
WHERE pair_id = '2026-07-08T11-52-46Z-e140231'
ORDER BY tier, arm;
