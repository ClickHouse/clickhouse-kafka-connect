#!/usr/bin/env bash
#
# verify_verdict_dwh.sh — fixture acceptance for the DWH-ADAPTED verdict view
#   (superset/v_kc_pair_ratios.sql), proving the adaptation preserved the verdict
#   semantics that the accepted sql/v_kc_pair_ratios.sql passed 20/20.
#
# WHY THIS EXISTS
#   Task #33: "if the adaptation is more than table-name substitution, re-run the
#   fixture acceptance locally against the ADAPTED SQL." The DWH twin adds
#   connector scoping and presentation columns (pair_ts, pair_seq, flag_reason), so
#   it is more than a rename — this script re-runs the SAME 20-case truth table
#   against the DWH view body with only the source tables swapped back to perf.*.
#
# HOW IT WORKS
#   clickhouse-local, byte-locked perf.* DDL + the SAME fixture the accepted test
#   uses. The DWH view is transformed for local execution by:
#     * raw_connectors_load_testing.{runs,metrics} -> perf.{runs,metrics}
#     * the two guard lines "connector = 'kafka-connect'" and
#       "connector != '__verdict_fixture__'" -> a fixture-INCLUSIVE "1=1" (the
#       fixture rows carry connector='__verdict_fixture__', so we must let them in
#       to assert the verdict logic — exactly the pattern the accepted test uses).
#     * view name -> v_kc_pair_ratios_fixture (to sit beside anything else).
#   The verdict multiIf, gated registry and calibration columns are UNCHANGED bytes
#   between the two files; this proves the swap did not perturb them.
#
# USAGE: benchmarks/e2e/dashboard/superset/verify_verdict_dwh.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DASH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$DASH_DIR/../../.." && pwd)"
PERF_DDL_DIR="$REPO_ROOT/benchmarks/e2e/sql/perf"
FIXTURE_SQL="$DASH_DIR/sql/fixture_verdict_truth_table.sql"
DWH_VIEW_SQL="$SCRIPT_DIR/v_kc_pair_ratios.sql"

CH_BIN="${CLICKHOUSE_BIN:-}"
if [ -z "$CH_BIN" ]; then
  if command -v clickhouse >/dev/null 2>&1; then CH_BIN="$(command -v clickhouse)"
  else CH_BIN="$(ls -1 /opt/homebrew/Caskroom/clickhouse/*/clickhouse-macos-* 2>/dev/null | head -1 || true)"; fi
fi
[ -n "$CH_BIN" ] && [ -x "$CH_BIN" ] || { echo "FATAL: clickhouse binary not found (set CLICKHOUSE_BIN)"; exit 2; }

CH_STATE="$(mktemp -d)"; trap 'rm -rf "$CH_STATE"' EXIT
ch() { "$CH_BIN" local --path "$CH_STATE" "$@"; }

echo "== DWH-adapted verdict acceptance (superset/v_kc_pair_ratios.sql) =="
echo "clickhouse: $CH_BIN"; echo ""

for f in 01_create_database.sql 02_create_runs.sql 03_create_metrics.sql; do
  ch --multiquery < "$PERF_DDL_DIR/$f"
done
ch --multiquery < "$FIXTURE_SQL"

# ---- static guard: the DWH view must still carry the fixture-exclusion clause ----
if ! grep -Eq "connector[[:space:]]*!=[[:space:]]*'__verdict_fixture__'" "$DWH_VIEW_SQL"; then
  echo "FAIL: superset/v_kc_pair_ratios.sql is MISSING the fixture-exclusion guard"; exit 1
fi
echo "OK  : fixture-exclusion guard present in DWH view SQL"

# ---- transform the DWH view for local, fixture-inclusive execution ----
VIEW_INCLUSIVE="$CH_STATE/dwh_view_inclusive.sql"
sed -e "s/raw_connectors_load_testing\.runs/perf.runs/g" \
    -e "s/raw_connectors_load_testing\.metrics/perf.metrics/g" \
    -e "s/raw_connectors_load_testing\.v_kc_pair_ratios/perf.v_kc_pair_ratios_fixture/g" \
    -e "s/connector = 'kafka-connect'/1=1/" \
    -e "s/connector != '__verdict_fixture__'/1=1/" \
    "$DWH_VIEW_SQL" > "$VIEW_INCLUSIVE"
ch --multiquery < "$VIEW_INCLUSIVE"

declare -a CASES=(
  "fixture-hb-090-unflagged 0 null_drain_rows_per_sec|REGRESSION"
  "fixture-hb-100-unflagged 0 null_drain_rows_per_sec|OK"
  "fixture-hb-110-unflagged 0 null_drain_rows_per_sec|IMPROVEMENT"
  "fixture-hb-null-unflagged 0 null_drain_rows_per_sec|NO_DATA"
  "fixture-hb-zerodenom-unflagged 0 null_drain_rows_per_sec|NO_DATA"
  "fixture-hb-090-flagged 0 null_drain_rows_per_sec|FLAGGED"
  "fixture-hb-100-flagged 0 null_drain_rows_per_sec|FLAGGED"
  "fixture-hb-110-flagged 0 null_drain_rows_per_sec|FLAGGED"
  "fixture-lb-090-unflagged 1 parts_per_insert|IMPROVEMENT"
  "fixture-lb-100-unflagged 1 parts_per_insert|OK"
  "fixture-lb-110-unflagged 1 parts_per_insert|REGRESSION"
  "fixture-lb-null-unflagged 1 parts_per_insert|NO_DATA"
  "fixture-lb-zerodenom-unflagged 1 parts_per_insert|NO_DATA"
  "fixture-lb-090-flagged 1 parts_per_insert|FLAGGED"
  "fixture-lb-100-flagged 1 parts_per_insert|FLAGGED"
  "fixture-lb-110-flagged 1 parts_per_insert|FLAGGED"
  "fixture-thr-above-unflagged 1 ch_avg_rows_per_insert|OK"
  "fixture-thr-below-unflagged 1 ch_avg_rows_per_insert|REGRESSION"
  "fixture-thr-null-unflagged 1 ch_avg_rows_per_insert|NO_DATA"
  "fixture-thr-above-flagged 1 ch_avg_rows_per_insert|FLAGGED"
)

fails=0
printf "%-34s %-4s %-26s %-12s %-12s %s\n" "PAIR" "TIER" "METRIC" "EXPECTED" "ACTUAL" "RESULT"
printf '%s\n' "--------------------------------------------------------------------------------------------------------"
for entry in "${CASES[@]}"; do
  key="${entry%%|*}"; expected="${entry##*|}"
  read -r pair tier metric <<< "$key"
  actual="$(ch --query "SELECT verdict FROM perf.v_kc_pair_ratios_fixture WHERE pair_id='$pair' AND tier='$tier' AND metric_name='$metric'")"
  [ -z "$actual" ] && actual="<no-row>"
  if [ "$actual" = "$expected" ]; then result="PASS"; else result="FAIL"; fails=$((fails+1)); fi
  printf "%-34s %-4s %-26s %-12s %-12s %s\n" "$pair" "$tier" "$metric" "$expected" "$actual" "$result"
done
echo ""

failed_rows="$(ch --query "SELECT count() FROM perf.v_kc_pair_ratios_fixture WHERE pair_id='fixture-failed-run'")"
[ "$failed_rows" = "0" ] && echo "PASS: failed-run excluded by outcome value (0 rows)" || { echo "FAIL: failed-run produced $failed_rows rows"; fails=$((fails+1)); }

bad_calib="$(ch --query "SELECT count() FROM perf.v_kc_pair_ratios_fixture WHERE (provisional != 1 OR alerts_enabled != 0)")"
[ "$bad_calib" = "0" ] && echo "PASS: calibration hold active (all provisional, alerts disabled)" || { echo "FAIL: calibration hold broken on $bad_calib rows"; fails=$((fails+1)); }

echo ""
total=${#CASES[@]}
if [ "$fails" -eq 0 ]; then
  echo "==================================================================="
  echo "DWH ADAPTATION ACCEPTED: $total/$total verdict cases + structural assertions"
  echo "==================================================================="
  exit 0
else
  echo "==================================================================="
  echo "DWH ADAPTATION FAILED: $fails assertion(s) failed"
  echo "==================================================================="
  exit 1
fi
