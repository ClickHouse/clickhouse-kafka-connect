-- =============================================================================
-- v_kc_flagged_log  —  DWH Superset virtual dataset (Benchmark v2 Kafka, Tab 1)
-- =============================================================================
-- WHAT THIS IS
--   The "flagged + failed run log" for Tab 1's bottom panel (plan §8: "EXCURSION +
--   FLAGGED-RUN LOG"). One row per NON-CLEAN run of connector='kafka-connect':
--   either FLAGGED (a validity guard tripped, runtime['flagged']='1') or FAILED
--   (runtime['outcome']='failed'). These are the runs the verdict view
--   (v_kc_pair_ratios) EXCLUDES from ratios/bands — this log is where they stay
--   VISIBLE (task #33 constraint 5: pair-1's instrument_resize rows are excluded
--   from ratios by flag, but must appear here).
--
-- WHY A SEPARATE VIEW
--   v_kc_pair_ratios drops flagged/failed runs before the ratio (they never reach
--   a verdict). The dashboard still owes the operator a reason why a pair is
--   missing from the trend. This view is the audit trail: it does NOT gate, it
--   only lists — so it is deliberately permissive (no fixture rows, but flagged
--   AND failed included).
--
-- CONTRACT GROUNDING
--   §1.3 flag_reason fixed token vocabulary (task_retries, settle_timeout,
--     rebalance, task_restart, drain_incomplete, integrity_unverified,
--     instrument_resize, instrument_shift — '|'-joined when several trip).
--   §1.1 outcome ('failed' by value, never by absence).
--   §3 fixture connector excluded from all real trends.
--
-- SOURCE: raw_connectors_load_testing.runs (DWH mirror). Returns zero rows without
--   erroring when there are no flagged/failed runs.
-- =============================================================================
CREATE OR REPLACE VIEW raw_connectors_load_testing.v_kc_flagged_log AS
SELECT
  r.run_id                                               AS run_id,
  r.runtime['pair_id']                                   AS pair_id,
  r.run_started_at                                       AS run_started_at,
  coalesce(nullIf(r.runtime['arm'], ''),  'head')        AS arm,
  coalesce(nullIf(r.runtime['tier'], ''), '1')           AS tier,
  coalesce(nullIf(r.runtime['outcome'], ''), 'success')  AS outcome,
  (r.runtime['flagged'] = '1')                           AS flagged,
  r.runtime['flag_reason']                               AS flag_reason,
  -- classify the log row so the panel can group/color: FAILED (outcome) vs
  -- FLAGGED (guard) — FAILED wins the label when both are true (contract §3
  -- precedence FAIL > FLAG).
  multiIf(
    coalesce(nullIf(r.runtime['outcome'], ''), 'success') = 'failed', 'FAILED',
    r.runtime['flagged'] = '1',                                        'FLAGGED',
    'CLEAN'
  )                                                      AS log_class,
  -- config echoed for triage (e.g. batch_size on the instrument_resize rows).
  r.runtime['batch_size']                                AS batch_size,
  r.connector_version                                    AS connector_version,
  r.notes                                                AS notes
FROM raw_connectors_load_testing.runs AS r
WHERE r.connector = 'kafka-connect'
  AND r.connector != '__verdict_fixture__'
  AND (
    r.runtime['flagged'] = '1'
    OR coalesce(nullIf(r.runtime['outcome'], ''), 'success') = 'failed'
  )
ORDER BY r.run_started_at DESC
