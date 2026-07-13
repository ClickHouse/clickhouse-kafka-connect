#!/usr/bin/env bash
#
# verify_tab4.sh — render-shape acceptance for the Tab 4 (RUN DRILL) datasets
#   (superset/v_kc_run_drill.sql, v_kc_drain_curve.sql, v_kc_inserts_drill.sql).
#
# WHY THIS EXISTS
#   Task #34: verify every Tab-4 dataset SQL parses/EXPLAINs and produces the RIGHT
#   SHAPE against synthetic perf.* rows through the REAL view bodies (only the table
#   names swapped perf.* <- raw_connectors_load_testing.*, the same adaptation
#   verify_verdict_dwh.sh uses). The drill has NO verdicts, so — unlike the verdict
#   view — the gold standard here is a RENDER-SHAPE test:
#     1. every view CREATEs and SELECTs without error (parse + type check);
#     2. the arm view emits the H|P|delta shape with a NULL-safe ratio and the
#        pair-4 head-t1 drain ratio lands at ~1.032 (the known-value sanity check);
#     3. the drain-curve BUCKETING MATH is correct on synthetic inserts:
#        rows_per_sec = minute_rows/60, cumulative is a monotone running sum, and
#        remaining_lag = rows_expected - cumulative;
#     4. the inserts-drill grain is one row per synthetic insert with a per-run seq.
#   No fixture-verdict logic is involved (there is none here), so no truth table.
#
# HOW IT WORKS
#   clickhouse-local, the byte-locked perf.* DDL, a tiny synthetic pair seeded
#   inline (one clean pair, head+pinned, t0+t1; ch_inserts across 3 minutes), then
#   the three view bodies transformed for local execution by swapping the DWH mirror
#   table names back to perf.* (exactly verify_verdict_dwh.sh's sed approach).
#
# USAGE: benchmarks/e2e/dashboard/superset/verify_tab4.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DASH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$DASH_DIR/../../.." && pwd)"
PERF_DDL_DIR="$REPO_ROOT/benchmarks/e2e/sql/perf"

CH_BIN="${CLICKHOUSE_BIN:-}"
if [ -z "$CH_BIN" ]; then
  if command -v clickhouse >/dev/null 2>&1; then CH_BIN="$(command -v clickhouse)"
  else CH_BIN="$(ls -1 /opt/homebrew/Caskroom/clickhouse/*/clickhouse-macos-* 2>/dev/null | head -1 || true)"; fi
fi
[ -n "$CH_BIN" ] && [ -x "$CH_BIN" ] || { echo "FATAL: clickhouse binary not found (set CLICKHOUSE_BIN)"; exit 2; }

CH_STATE="$(mktemp -d)"; trap 'rm -rf "$CH_STATE"' EXIT
ch() { "$CH_BIN" local --path "$CH_STATE" "$@"; }

echo "== Tab 4 render-shape acceptance (run_drill / drain_curve / inserts_drill) =="
echo "clickhouse: $CH_BIN"; echo ""

for f in 01_create_database.sql 02_create_runs.sql 03_create_metrics.sql 04_create_ch_inserts.sql; do
  ch --multiquery < "$PERF_DDL_DIR/$f"
done

# ---- synthetic clean pair: pair_id 2026-07-11T09-00-00Z-pair4 ----------------
#  head-t1 drain_rows_per_sec = 1032000, pinned-t1 = 1000000 => ratio 1.032
#  (the known pair-4 head-t1 code advantage the drill must expose).
ch --multiquery <<'SQL'
INSERT INTO perf.runs (run_id, run_started_at, run_ended_at, git_sha, connector, connector_version, clickhouse_version, runtime, notes) VALUES
 ('2026-07-11T09-00-00Z-pair4-head-t1',   '2026-07-11 09:00:00','2026-07-11 09:05:00','pair4','kafka-connect','v2','25.1', {'arm':'head','tier':'1','pair_id':'2026-07-11T09-00-00Z-pair4'}, ''),
 ('2026-07-11T09-00-00Z-pair4-pinned-t1', '2026-07-11 09:00:00','2026-07-11 09:05:00','pair4','kafka-connect','v2','25.1', {'arm':'pinned','tier':'1','pair_id':'2026-07-11T09-00-00Z-pair4'}, ''),
 ('2026-07-11T09-00-00Z-pair4-head-t0',   '2026-07-11 09:00:00','2026-07-11 09:05:00','pair4','kafka-connect','v2','25.1', {'arm':'head','tier':'0','pair_id':'2026-07-11T09-00-00Z-pair4'}, ''),
 ('2026-07-11T09-00-00Z-pair4-pinned-t0', '2026-07-11 09:00:00','2026-07-11 09:05:00','pair4','kafka-connect','v2','25.1', {'arm':'pinned','tier':'0','pair_id':'2026-07-11T09-00-00Z-pair4'}, '');

-- metrics: drain ratio 1.032 on t1; rows_expected for the lag denominator; a
-- cost charged to ONE arm only (§2.1 two-arm attribution) to exercise NULL ratio.
INSERT INTO perf.metrics (run_id, metric_name, value, recorded_at) VALUES
 ('2026-07-11T09-00-00Z-pair4-head-t1',   'drain_rows_per_sec', 1032000, now()),
 ('2026-07-11T09-00-00Z-pair4-pinned-t1', 'drain_rows_per_sec', 1000000, now()),
 ('2026-07-11T09-00-00Z-pair4-head-t1',   'rows_expected',      3000000, now()),
 ('2026-07-11T09-00-00Z-pair4-pinned-t1', 'rows_expected',      3000000, now()),
 ('2026-07-11T09-00-00Z-pair4-head-t1',   'run_cost_usd',       4.20, now()),
 ('2026-07-11T09-00-00Z-pair4-head-t1',   'parts_per_insert',    1.0, now()),
 ('2026-07-11T09-00-00Z-pair4-pinned-t1', 'ch_parts_per_insert', 1.0, now());

-- ch_inserts: 3 minutes of inserts on the two t1 runs. Per minute totals:
--   min0: 1,000,000 rows (2 inserts)  min1: 1,000,000 (2)  min2: 1,000,000 (2)
-- => rows_per_sec 1e6/60 per minute; cumulative 1e6,2e6,3e6; lag 2e6,1e6,0.
INSERT INTO perf.ch_inserts (run_id, event_time, query_duration_ms, written_rows, written_bytes, memory_usage, network_bytes, cpu_microseconds, exception_code) VALUES
 ('2026-07-11T09-00-00Z-pair4-head-t1',   '2026-07-11 09:00:10', 120, 500000, 5000000, 1000, 1000, 100, 0),
 ('2026-07-11T09-00-00Z-pair4-head-t1',   '2026-07-11 09:00:40', 130, 500000, 5000000, 1000, 1000, 100, 0),
 ('2026-07-11T09-00-00Z-pair4-head-t1',   '2026-07-11 09:01:10', 110, 500000, 5000000, 1000, 1000, 100, 0),
 ('2026-07-11T09-00-00Z-pair4-head-t1',   '2026-07-11 09:01:40', 115, 500000, 5000000, 1000, 1000, 100, 0),
 ('2026-07-11T09-00-00Z-pair4-head-t1',   '2026-07-11 09:02:10', 118, 500000, 5000000, 1000, 1000, 100, 0),
 ('2026-07-11T09-00-00Z-pair4-head-t1',   '2026-07-11 09:02:40', 119, 500000, 5000000, 1000, 1000, 100, 0),
 ('2026-07-11T09-00-00Z-pair4-pinned-t1', '2026-07-11 09:00:15', 140, 500000, 5000000, 1000, 1000, 100, 0),
 ('2026-07-11T09-00-00Z-pair4-pinned-t1', '2026-07-11 09:00:45', 150, 500000, 5000000, 1000, 1000, 100, 0),
 ('2026-07-11T09-00-00Z-pair4-pinned-t1', '2026-07-11 09:01:15', 145, 500000, 5000000, 1000, 1000, 100, 0);
SQL

# ---- transform + create the three views (perf.* substitution) ----------------
mk_local() {
  local src="$1" out="$2"
  sed -e "s/raw_connectors_load_testing\./perf./g" "$src" > "$out"
}
mk_local "$SCRIPT_DIR/v_kc_run_drill.sql"     "$CH_STATE/run_drill.sql"
mk_local "$SCRIPT_DIR/v_kc_drain_curve.sql"   "$CH_STATE/drain_curve.sql"
mk_local "$SCRIPT_DIR/v_kc_inserts_drill.sql" "$CH_STATE/inserts_drill.sql"
ch --multiquery < "$CH_STATE/run_drill.sql"
ch --multiquery < "$CH_STATE/drain_curve.sql"
ch --multiquery < "$CH_STATE/inserts_drill.sql"
echo "OK  : all three views CREATE without error"

fails=0
check() { # label expected actual
  if [ "$2" = "$3" ]; then printf "PASS: %-52s %s\n" "$1" "$3"
  else printf "FAIL: %-52s expected=%s actual=%s\n" "$1" "$2" "$3"; fails=$((fails+1)); fi
}

# --- 1. arm view: pair-4 head-t1 drain ratio ~1.032 (known value) -------------
ratio="$(ch --query "SELECT round(ratio,3) FROM perf.v_kc_run_drill WHERE metric_name='drain_rows_per_sec' AND tier='1'")"
check "arm view drain_rows_per_sec ratio (pair-4 head-t1 story)" "1.032" "$ratio"
delta="$(ch --query "SELECT round(delta_pct,1) FROM perf.v_kc_run_drill WHERE metric_name='drain_rows_per_sec' AND tier='1'")"
check "arm view delta_pct = +3.2" "3.2" "$delta"

# NULL-safe ratio: cost charged to one arm only => ratio NULL (not 0/false).
cost_ratio_null="$(ch --query "SELECT ratio IS NULL FROM perf.v_kc_run_drill WHERE metric_name='run_cost_usd' AND tier='1'")"
check "arm view one-sided metric (run_cost_usd) => NULL ratio" "1" "$cost_ratio_null"

# legacy-name coalesce: head parts_per_insert + pinned ch_parts_per_insert fold
# into ONE metric row named parts_per_insert with a ratio of 1.0.
parts_row="$(ch --query "SELECT count() FROM perf.v_kc_run_drill WHERE metric_name='parts_per_insert' AND tier='1'")"
check "arm view legacy coalesce -> single parts_per_insert row" "1" "$parts_row"
parts_ratio="$(ch --query "SELECT round(ratio,3) FROM perf.v_kc_run_drill WHERE metric_name='parts_per_insert' AND tier='1'")"
check "arm view parts_per_insert ratio (both sides 1.0)" "1" "$parts_ratio"
no_legacy="$(ch --query "SELECT count() FROM perf.v_kc_run_drill WHERE metric_name='ch_parts_per_insert'")"
check "arm view leaks NO ch_-prefixed legacy metric name" "0" "$no_legacy"

# pair_seq present and = 1 for the (only) pair.
pseq="$(ch --query "SELECT DISTINCT pair_seq FROM perf.v_kc_run_drill")"
check "arm view pair_seq = 1 for the latest (only) pair" "1" "$pseq"

# --- 2. drain-curve bucketing math on the head-t1 run -------------------------
rps0="$(ch --query "SELECT toUInt64(rows_per_sec) FROM perf.v_kc_drain_curve WHERE run_id='2026-07-11T09-00-00Z-pair4-head-t1' AND minute_index=0")"
check "drain rows_per_sec min0 = 1e6/60 = 16666" "16666" "$rps0"
cum="$(ch --query "SELECT groupArray(toUInt64(cumulative_rows)) FROM (SELECT cumulative_rows FROM perf.v_kc_drain_curve WHERE run_id='2026-07-11T09-00-00Z-pair4-head-t1' ORDER BY minute_index)")"
check "drain cumulative is monotone running sum" "[1000000,2000000,3000000]" "$cum"
lag="$(ch --query "SELECT groupArray(toInt64(remaining_lag)) FROM (SELECT remaining_lag FROM perf.v_kc_drain_curve WHERE run_id='2026-07-11T09-00-00Z-pair4-head-t1' ORDER BY minute_index)")"
check "drain remaining_lag = expected - cumulative -> 0" "[2000000,1000000,0]" "$lag"
midx="$(ch --query "SELECT groupArray(minute_index) FROM (SELECT DISTINCT minute_index FROM perf.v_kc_drain_curve WHERE run_id='2026-07-11T09-00-00Z-pair4-head-t1' ORDER BY minute_index)")"
check "drain minute_index is 0-based, dense" "[0,1,2]" "$midx"
# both arms overlaid: two run_ids for the t1 pair.
arms="$(ch --query "SELECT uniqExact(run_id) FROM perf.v_kc_drain_curve WHERE tier='1'")"
check "drain curve carries BOTH arms of the t1 pair" "2" "$arms"

# --- 3. inserts-drill grain: one row per synthetic insert ---------------------
n_head="$(ch --query "SELECT count() FROM perf.v_kc_inserts_drill WHERE run_id='2026-07-11T09-00-00Z-pair4-head-t1'")"
check "inserts drill grain = one row per insert (head-t1 = 6)" "6" "$n_head"
seqmax="$(ch --query "SELECT max(seq) FROM perf.v_kc_inserts_drill WHERE run_id='2026-07-11T09-00-00Z-pair4-head-t1'")"
check "inserts drill per-run seq 1..6" "6" "$seqmax"
el0="$(ch --query "SELECT elapsed_s FROM perf.v_kc_inserts_drill WHERE run_id='2026-07-11T09-00-00Z-pair4-head-t1' AND seq=1")"
check "inserts drill first insert elapsed_s = 0" "0" "$el0"
wr="$(ch --query "SELECT DISTINCT written_rows FROM perf.v_kc_inserts_drill WHERE run_id='2026-07-11T09-00-00Z-pair4-head-t1'")"
check "inserts drill written_rows (batch size) passthrough" "500000" "$wr"

# --- 4. scope guards: fixture / failed / non-kafka never leak ------------------
ch --query "INSERT INTO perf.runs (run_id, run_started_at, run_ended_at, git_sha, connector, connector_version, runtime) VALUES ('fix-1','2026-07-11 09:00:00','2026-07-11 09:05:00','x','__verdict_fixture__','v2', {'arm':'head','tier':'1','pair_id':'fixp'})"
ch --query "INSERT INTO perf.runs (run_id, run_started_at, run_ended_at, git_sha, connector, connector_version, runtime) VALUES ('spark-1','2026-07-11 09:00:00','2026-07-11 09:05:00','x','spark','v2', {'arm':'head','tier':'1','pair_id':'sparkp'})"
ch --query "INSERT INTO perf.runs (run_id, run_started_at, run_ended_at, git_sha, connector, connector_version, runtime) VALUES ('failrun','2026-07-11 09:00:00','2026-07-11 09:05:00','x','kafka-connect','v2', {'arm':'head','tier':'1','pair_id':'failp','outcome':'failed'})"
leak="$(ch --query "SELECT count() FROM perf.v_kc_run_drill WHERE pair_id IN ('fixp','sparkp','failp')")"
check "arm view excludes fixture/non-kafka/failed runs" "0" "$leak"

echo ""
if [ "$fails" -eq 0 ]; then
  echo "==================================================================="
  echo "TAB 4 DATASETS ACCEPTED: all render-shape assertions passed"
  echo "==================================================================="
  exit 0
else
  echo "==================================================================="
  echo "TAB 4 DATASETS FAILED: $fails assertion(s) failed"
  echo "==================================================================="
  exit 1
fi
