#!/usr/bin/env bash
#
# test_verdicts.sh — fixture-based acceptance for the Benchmark v2 Kafka verdict map.
#
# THE POINT OF THIS SCRIPT (principal mandate, #33 prep):
#   Any verdict-emitting artifact requires fixture-based acceptance BEFORE it ships.
#   The Spark dashboard shipped verdict logic that mislabeled 0/0 (no-data) and
#   good-direction excursions as REGRESSION. This test proves the Kafka verdict map
#   (benchmarks/e2e/dashboard/sql/v_kc_pair_ratios.sql) handles every branch, by:
#     1. creating perf.* from the BYTE-LOCKED DDL at benchmarks/e2e/sql/perf/,
#     2. applying benchmarks/e2e/dashboard/sql/fixture_verdict_truth_table.sql,
#     3. creating the view v_kc_pair_ratios,
#     4. asserting each fixture case's verdict == the documented expectation,
#     5. asserting the view excludes the fixture connector (production rows and
#        fixture rows never mix), AND that the WHERE connector guard is present.
#   Any mismatch prints loudly and the script exits non-zero.
#
# REQUIREMENTS: clickhouse (clickhouse-local). On this machine:
#   which clickhouse  ->  /opt/homebrew/Caskroom/clickhouse/.../clickhouse-macos-aarch64
#
# USAGE: benchmarks/e2e/dashboard/test_verdicts.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
PERF_DDL_DIR="$REPO_ROOT/benchmarks/e2e/sql/perf"
DASH_SQL_DIR="$SCRIPT_DIR/sql"
VIEW_SQL="$DASH_SQL_DIR/v_kc_pair_ratios.sql"
FIXTURE_SQL="$DASH_SQL_DIR/fixture_verdict_truth_table.sql"

# Resolve the clickhouse binary (handles the zsh alias not being present in bash).
CH_BIN="${CLICKHOUSE_BIN:-}"
if [ -z "$CH_BIN" ]; then
  if command -v clickhouse >/dev/null 2>&1; then
    CH_BIN="$(command -v clickhouse)"
  else
    CH_BIN="$(ls -1 /opt/homebrew/Caskroom/clickhouse/*/clickhouse-macos-* 2>/dev/null | head -1 || true)"
  fi
fi
[ -n "$CH_BIN" ] && [ -x "$CH_BIN" ] || { echo "FATAL: clickhouse binary not found (set CLICKHOUSE_BIN)"; exit 2; }

CH_STATE="$(mktemp -d)"
trap 'rm -rf "$CH_STATE"' EXIT
ch() { "$CH_BIN" local --path "$CH_STATE" "$@"; }

echo "== Benchmark v2 Kafka verdict acceptance =="
echo "clickhouse: $CH_BIN"
echo "state dir : $CH_STATE"
echo ""

# ---- 1. byte-locked perf.* DDL (read-only to this test) ----
echo "-- creating perf.* from byte-locked DDL --"
for f in 01_create_database.sql 02_create_runs.sql 03_create_metrics.sql; do
  ch --multiquery < "$PERF_DDL_DIR/$f"
done

# ---- 2. apply the fixture ----
echo "-- applying fixture --"
ch --multiquery < "$FIXTURE_SQL"

# ---- 2b. prove idempotency: applying twice must not change counts ----
ch --multiquery < "$FIXTURE_SQL"

# ---- 3. create the view under test ----
echo "-- creating v_kc_pair_ratios --"
ch --multiquery < "$VIEW_SQL"
echo ""

# ---- static guard: the view SQL must carry the fixture-exclusion WHERE clause ----
if ! grep -Eq "connector[[:space:]]*!=[[:space:]]*'__verdict_fixture__'" "$VIEW_SQL"; then
  echo "FAIL: v_kc_pair_ratios.sql is MISSING the WHERE connector != '__verdict_fixture__' guard"
  exit 1
fi
echo "OK  : fixture-exclusion guard present in view SQL"

# =====================================================================================
#  Expected truth table: "<pair_id> <tier> <metric>|<expected_verdict>"
# =====================================================================================
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

# NOTE: the view EXCLUDES connector='__verdict_fixture__'. To ASSERT the verdict
# logic against fixture rows we re-run the view body against a fixture-INCLUSIVE
# variant produced by stripping that one guard line — proving the logic is correct
# AND that the only thing standing between fixture rows and production is that guard.
VIEW_INCLUSIVE="$CH_STATE/view_inclusive.sql"
sed "s/WHERE connector != '__verdict_fixture__'/WHERE 1=1/; s/CREATE OR REPLACE VIEW perf.v_kc_pair_ratios AS/CREATE OR REPLACE VIEW perf.v_kc_pair_ratios_fixture AS/" "$VIEW_SQL" > "$VIEW_INCLUSIVE"
ch --multiquery < "$VIEW_INCLUSIVE"

for entry in "${CASES[@]}"; do
  key="${entry%%|*}"; expected="${entry##*|}"
  read -r pair tier metric <<< "$key"
  actual="$(ch --query "SELECT verdict FROM perf.v_kc_pair_ratios_fixture WHERE pair_id='$pair' AND tier='$tier' AND metric_name='$metric'")"
  [ -z "$actual" ] && actual="<no-row>"
  if [ "$actual" = "$expected" ]; then
    result="PASS"
  else
    result="FAIL"; fails=$((fails+1))
  fi
  printf "%-34s %-4s %-26s %-12s %-12s %s\n" "$pair" "$tier" "$metric" "$expected" "$actual" "$result"
done

echo ""

# ---- failed-run case: must produce NO row (excluded by outcome value) ----
failed_rows="$(ch --query "SELECT count() FROM perf.v_kc_pair_ratios_fixture WHERE pair_id='fixture-failed-run'")"
if [ "$failed_rows" = "0" ]; then
  echo "PASS: failed-run (outcome='failed') produced 0 rows (excluded by outcome value)"
else
  echo "FAIL: failed-run produced $failed_rows rows (expected 0 — must be excluded by outcome value)"
  fails=$((fails+1))
fi

# ---- fixture-exclusion is EFFECTIVE: production view must see 0 fixture rows ----
prod_fixture_rows="$(ch --query "SELECT count() FROM perf.v_kc_pair_ratios WHERE pair_id LIKE 'fixture-%'")"
if [ "$prod_fixture_rows" = "0" ]; then
  echo "PASS: production view v_kc_pair_ratios exposes 0 fixture rows (guard is effective)"
else
  echo "FAIL: production view exposed $prod_fixture_rows fixture rows (guard NOT effective)"
  fails=$((fails+1))
fi

# ---- prove production + fixture do not mix: seed a real production pair, confirm
#      it appears in the production view while fixture rows still do not ----
ch --multiquery <<'SQL'
INSERT INTO perf.runs (run_id, run_started_at, run_ended_at, git_sha, connector, connector_version, runtime) VALUES
  ('prod-pair-head-t1',   now(), now(), 'abc', 'kafka-connect', 'v1', {'arm':'head','tier':'1','pair_id':'prod-pair','outcome':'success'}),
  ('prod-pair-pinned-t1', now(), now(), 'abc', 'kafka-connect', 'v1', {'arm':'pinned','tier':'1','pair_id':'prod-pair','outcome':'success'});
INSERT INTO perf.metrics (run_id, metric_name, unit, value) VALUES
  ('prod-pair-head-t1',   'parts_per_insert', 'ratio', 100),
  ('prod-pair-pinned-t1', 'parts_per_insert', 'ratio', 100);
SQL
prod_visible="$(ch --query "SELECT count() FROM perf.v_kc_pair_ratios WHERE pair_id='prod-pair'")"
still_no_fixture="$(ch --query "SELECT count() FROM perf.v_kc_pair_ratios WHERE pair_id LIKE 'fixture-%'")"
if [ "$prod_visible" -ge 1 ] && [ "$still_no_fixture" = "0" ]; then
  echo "PASS: production rows visible ($prod_visible), fixture rows absent ($still_no_fixture) — no mixing"
else
  echo "FAIL: mixing check — prod_visible=$prod_visible fixture_visible=$still_no_fixture"
  fails=$((fails+1))
fi

# ---- idempotency assertion: fixture applied twice, run-count must be single ----
dup="$(ch --query "SELECT count() FROM perf.runs WHERE run_id='fixture-hb-100-unflagged-head-t0'")"
if [ "$dup" = "1" ]; then
  echo "PASS: fixture is idempotent (applied twice, 1 row for a sample run_id)"
else
  echo "FAIL: fixture not idempotent (found $dup rows for a sample run_id, expected 1)"
  fails=$((fails+1))
fi

# ---- calibration hold: all fixture (tier,metric) have <20 comparable pairs, so
#      every emitted verdict must be provisional with alerts disabled ----
bad_calib="$(ch --query "SELECT count() FROM perf.v_kc_pair_ratios_fixture WHERE pair_id LIKE 'fixture-%' AND (provisional != 1 OR alerts_enabled != 0)")"
if [ "$bad_calib" = "0" ]; then
  echo "PASS: calibration hold active (all fixture verdicts provisional, alerts disabled)"
else
  echo "FAIL: calibration hold broken on $bad_calib fixture rows"
  fails=$((fails+1))
fi

echo ""
total=${#CASES[@]}
if [ "$fails" -eq 0 ]; then
  echo "==================================================================="
  echo "ACCEPTANCE PASSED: $total/$total verdict cases + all structural assertions"
  echo "==================================================================="
  exit 0
else
  echo "==================================================================="
  echo "ACCEPTANCE FAILED: $fails assertion(s) failed"
  echo "==================================================================="
  exit 1
fi
