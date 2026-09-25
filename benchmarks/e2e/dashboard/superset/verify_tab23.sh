#!/usr/bin/env bash
#
# verify_tab23.sh — render-shape acceptance for the Tab 2 (PERFORMANCE) + Tab 3
#   (ENVIRONMENT) datasets (superset/v_kc_metric_trends.sql, v_kc_env_events.sql).
#
# WHY THIS EXISTS
#   Task #35: verify the two NEW Tab-2/Tab-3 dataset SQL bodies parse/EXPLAIN and
#   produce the RIGHT SHAPE against synthetic perf.* rows through the REAL view
#   bodies (only the table names swapped perf.* <- raw_connectors_load_testing.*,
#   the same adaptation verify_verdict_dwh.sh / verify_tab4.sh use). Tabs 2/3 carry
#   NO verdicts (absolute history + instrument health), so — like the drill — the
#   gold standard here is a RENDER-SHAPE test, not a verdict truth table:
#     1. both views CREATE + SELECT without error (parse + type check), 0 rows empty;
#     2. v_kc_metric_trends returns the pair-4 connect_cpu_seconds_per_Mrows value on
#        the RIGHT arm/tier (head / t0), the sighted-gate Tier-0 series (constraint 5);
#     3. legacy ch_-prefixed names fold to the pinned name (§7) with no leak, and the
#        already-conformant capture-family names pass through untouched;
#     4. v_kc_env_events emits a ch_version_change row when consecutive pinned
#        versions differ, and a server_restart row when ch_uptime drops — each with
#        the contract §4 scope tuple (connector, target_service, environment_class);
#        the FIRST pinned run (no predecessor) emits nothing;
#     5. fixture / failed-outcome / non-kafka runs never leak into either view;
#     6. Tab-3 pinned scoping: v_kc_env_events contains NO head-arm-derived rows
#        (head runs do not drive environment annotations).
#
# HOW IT WORKS
#   clickhouse-local, the byte-locked perf.* DDL, a small synthetic set of runs
#   seeded inline (head+pinned, t0+t1; two pinned pairs so env-events has a
#   predecessor to diff), then the two view bodies transformed for local execution by
#   swapping the DWH mirror table names back to perf.* (verify_tab4.sh's sed approach).
#
# USAGE: benchmarks/e2e/dashboard/superset/verify_tab23.sh
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

echo "== Tab 2/3 render-shape acceptance (metric_trends / env_events) =="
echo "clickhouse: $CH_BIN"; echo ""

for f in 01_create_database.sql 02_create_runs.sql 03_create_metrics.sql 04_create_ch_inserts.sql; do
  ch --multiquery < "$PERF_DDL_DIR/$f"
done

# ---- synthetic runs -----------------------------------------------------------
#  Two clean pinned pairs (p3 25.1, p4 25.2) so env-events has a predecessor to
#  diff; a head-t0 run carrying the pair-4 sighted-gate connect_cpu value; a
#  legacy-named metric to exercise the §7 fold; ch_uptime drop p3->p4 (restart).
ch --multiquery <<'SQL'
INSERT INTO perf.runs (run_id, run_started_at, run_ended_at, git_sha, connector, connector_version, clickhouse_version, runtime, notes) VALUES
 ('p3-pinned-t0','2026-07-11 09:00:00','2026-07-11 09:05:00','p3','kafka-connect','v1','25.1', {'arm':'pinned','tier':'0','pair_id':'2026-07-11T09-00-00Z-p3','environment_class':'staging','target_region':'us-east-2'}, ''),
 ('p3-pinned-t1','2026-07-11 09:05:00','2026-07-11 09:12:00','p3','kafka-connect','v1','25.1', {'arm':'pinned','tier':'1','pair_id':'2026-07-11T09-00-00Z-p3','environment_class':'staging','target_region':'us-east-2'}, ''),
 ('p4-pinned-t0','2026-07-12 09:00:00','2026-07-12 09:05:00','p4','kafka-connect','v1','25.2', {'arm':'pinned','tier':'0','pair_id':'2026-07-12T09-00-00Z-p4','environment_class':'staging','target_region':'us-east-2'}, ''),
 ('p4-pinned-t1','2026-07-12 09:05:00','2026-07-12 09:12:00','p4','kafka-connect','v1','25.2', {'arm':'pinned','tier':'1','pair_id':'2026-07-12T09-00-00Z-p4','environment_class':'staging','target_region':'us-east-2'}, ''),
 ('p4-head-t0','2026-07-12 09:00:00','2026-07-12 09:05:00','p4','kafka-connect','v2','25.2', {'arm':'head','tier':'0','pair_id':'2026-07-12T09-00-00Z-p4','environment_class':'staging','target_region':'us-east-2'}, ''),
 ('p4-head-t1','2026-07-12 09:05:00','2026-07-12 09:12:00','p4','kafka-connect','v2','25.2', {'arm':'head','tier':'1','pair_id':'2026-07-12T09-00-00Z-p4','environment_class':'staging','target_region':'us-east-2'}, '');

INSERT INTO perf.metrics (run_id, metric_name, unit, value, recorded_at) VALUES
 ('p4-head-t0','connect_cpu_seconds_per_Mrows','s/Mrows', 42.5, now()),
 ('p4-head-t0','null_drain_rows_per_sec','rows/s', 900000, now()),
 ('p4-head-t0','ch_insert_cpu_share_tier0','percent', 2.1, now()),
 ('p4-head-t1','ch_parts_per_insert','ratio', 1.0, now()),
 ('p4-head-t1','ch_insert_duration_p99_ms','ms', 120, now()),
 ('p4-head-t1','drain_rows_per_sec','rows/s', 1032000, now()),
 ('p3-pinned-t0','ch_uptime','seconds', 100000, now()),
 ('p3-pinned-t1','ch_uptime','seconds', 100420, now()),
 ('p4-pinned-t0','ch_uptime','seconds', 500, now()),
 ('p4-pinned-t1','ch_uptime','seconds', 920, now()),
 ('p4-pinned-t1','drain_rows_per_sec','rows/s', 1000000, now());
SQL

# ---- transform + create the two views (perf.* substitution) -------------------
mk_local() { sed -e "s/raw_connectors_load_testing\./perf./g" "$1" > "$2"; }
mk_local "$SCRIPT_DIR/v_kc_metric_trends.sql" "$CH_STATE/metric_trends.sql"
mk_local "$SCRIPT_DIR/v_kc_env_events.sql"    "$CH_STATE/env_events.sql"
ch --multiquery < "$CH_STATE/metric_trends.sql"
ch --multiquery < "$CH_STATE/env_events.sql"
echo "OK  : both views CREATE without error"

fails=0
check() { # label expected actual
  if [ "$2" = "$3" ]; then printf "PASS: %-58s %s\n" "$1" "$3"
  else printf "FAIL: %-58s expected=%s actual=%s\n" "$1" "$2" "$3"; fails=$((fails+1)); fi
}

# --- 1. metric_trends: pair-4 connect_cpu on the RIGHT arm/tier (head/t0) ------
cpu="$(ch --query "SELECT toString(value) FROM perf.v_kc_metric_trends WHERE metric_name='connect_cpu_seconds_per_Mrows' AND arm='head' AND tier='0'")"
check "trends: pair-4 connect_cpu value on head/t0 (sighted-gate)" "42.5" "$cpu"
# the value must NOT appear on any other arm/tier (right scope only).
cpu_wrong="$(ch --query "SELECT count() FROM perf.v_kc_metric_trends WHERE metric_name='connect_cpu_seconds_per_Mrows' AND NOT (arm='head' AND tier='0')")"
check "trends: connect_cpu scoped to head/t0 only (no leak)" "0" "$cpu_wrong"

# --- 2. legacy fold (§7) + conformant passthrough -----------------------------
parts_folded="$(ch --query "SELECT count() FROM perf.v_kc_metric_trends WHERE metric_name='parts_per_insert' AND arm='head' AND tier='1'")"
check "trends: legacy ch_parts_per_insert folds to parts_per_insert" "1" "$parts_folded"
no_leak="$(ch --query "SELECT count() FROM perf.v_kc_metric_trends WHERE metric_name LIKE 'ch_%parts_per_insert%'")"
check "trends: no ch_-prefixed parts_per_insert leaks" "0" "$no_leak"
p99_through="$(ch --query "SELECT toString(value) FROM perf.v_kc_metric_trends WHERE metric_name='ch_insert_duration_p99_ms' AND arm='head' AND tier='1'")"
check "trends: conformant capture-family name passes through" "120" "$p99_through"

# --- 3. arm exposure + pair_seq present ---------------------------------------
arms="$(ch --query "SELECT arrayStringConcat(arraySort(groupUniqArray(arm)), ',') FROM perf.v_kc_metric_trends")"
check "trends: both arms exposed (tab scopes, view carries both)" "head,pinned" "$arms"
pseq="$(ch --query "SELECT max(pair_seq) FROM perf.v_kc_metric_trends WHERE arm='pinned' AND tier='1'")"
check "trends: pair_seq dense-ranked per arm/tier (2 pinned t1 pairs)" "2" "$pseq"

# --- 4. env_events: version-change + restart, with §4 scope tuple -------------
verchg="$(ch --query "SELECT count() FROM perf.v_kc_env_events WHERE event_kind='ch_version_change'")"
check "env: ch_version_change emitted when versions differ" "2" "$verchg"
verchg_val="$(ch --query "SELECT concat(from_value,'->',to_value) FROM perf.v_kc_env_events WHERE event_kind='ch_version_change' AND tier='0'")"
check "env: version-change from/to detail (t0) = 25.1->25.2" "25.1->25.2" "$verchg_val"
restart="$(ch --query "SELECT count() FROM perf.v_kc_env_events WHERE event_kind='server_restart'")"
check "env: server_restart emitted when ch_uptime drops (both tiers)" "2" "$restart"
scope="$(ch --query "SELECT concat(connector,'|',target_service,'|',environment_class) FROM perf.v_kc_env_events WHERE event_kind='ch_version_change' AND tier='0'")"
check "env: contract §4 scope tuple present + composed" "kafka-connect|staging/us-east-2|staging" "$scope"
# the FIRST pinned run (p3, no predecessor) must emit NO event.
first_run_events="$(ch --query "SELECT count() FROM perf.v_kc_env_events WHERE run_id IN ('p3-pinned-t0','p3-pinned-t1')")"
check "env: first pinned run (no predecessor) emits no event" "0" "$first_run_events"

# --- 5. Tab-3 pinned scoping: env_events has NO head-derived rows -------------
head_events="$(ch --query "SELECT count() FROM perf.v_kc_env_events WHERE run_id LIKE 'p%-head-%'")"
check "env: pinned-only (no head run drives an annotation)" "0" "$head_events"

# --- 6. scope guards: fixture / failed / non-kafka never leak -----------------
ch --query "INSERT INTO perf.runs (run_id, run_started_at, run_ended_at, git_sha, connector, connector_version, clickhouse_version, runtime) VALUES ('fix-1','2026-07-12 09:00:00','2026-07-12 09:05:00','x','__verdict_fixture__','v2','25.2', {'arm':'pinned','tier':'0','pair_id':'fixp','environment_class':'staging'})"
ch --query "INSERT INTO perf.runs (run_id, run_started_at, run_ended_at, git_sha, connector, connector_version, clickhouse_version, runtime) VALUES ('spark-1','2026-07-12 09:00:00','2026-07-12 09:05:00','x','spark','v2','25.2', {'arm':'pinned','tier':'0','pair_id':'sparkp','environment_class':'production'})"
ch --query "INSERT INTO perf.runs (run_id, run_started_at, run_ended_at, git_sha, connector, connector_version, clickhouse_version, runtime) VALUES ('failrun','2026-07-12 09:00:00','2026-07-12 09:05:00','x','kafka-connect','v2','25.2', {'arm':'pinned','tier':'0','pair_id':'failp','outcome':'failed','environment_class':'staging'})"
ch --query "INSERT INTO perf.metrics (run_id, metric_name, unit, value, recorded_at) VALUES ('spark-1','drain_rows_per_sec','rows/s',9,now())"
trends_leak="$(ch --query "SELECT count() FROM perf.v_kc_metric_trends WHERE pair_id IN ('fixp','sparkp','failp')")"
check "trends: excludes fixture/non-kafka/failed runs" "0" "$trends_leak"
env_leak="$(ch --query "SELECT count() FROM perf.v_kc_env_events WHERE pair_id IN ('fixp','sparkp','failp')")"
check "env: excludes fixture/non-kafka/failed runs" "0" "$env_leak"

# --- 7. both views empty-safe (0 rows, no error) on a fresh db ----------------
ES="$(mktemp -d)"; "$CH_BIN" local --path "$ES" --multiquery < "$PERF_DDL_DIR/01_create_database.sql"
for f in 02_create_runs.sql 03_create_metrics.sql 04_create_ch_inserts.sql; do "$CH_BIN" local --path "$ES" --multiquery < "$PERF_DDL_DIR/$f"; done
"$CH_BIN" local --path "$ES" --multiquery < "$CH_STATE/metric_trends.sql"
"$CH_BIN" local --path "$ES" --multiquery < "$CH_STATE/env_events.sql"
empty_mt="$("$CH_BIN" local --path "$ES" --query "SELECT count() FROM perf.v_kc_metric_trends")"
empty_ev="$("$CH_BIN" local --path "$ES" --query "SELECT count() FROM perf.v_kc_env_events")"
rm -rf "$ES"
check "empty-safe: v_kc_metric_trends 0 rows, no error" "0" "$empty_mt"
check "empty-safe: v_kc_env_events 0 rows, no error" "0" "$empty_ev"

echo ""
if [ "$fails" -eq 0 ]; then
  echo "==================================================================="
  echo "TAB 2/3 DATASETS ACCEPTED: all render-shape assertions passed"
  echo "==================================================================="
  exit 0
else
  echo "==================================================================="
  echo "TAB 2/3 DATASETS FAILED: $fails assertion(s) failed"
  echo "==================================================================="
  exit 1
fi
