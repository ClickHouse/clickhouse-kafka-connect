#!/usr/bin/env bash
#
# verify_tab5.sh — render-shape acceptance for the Tab 5 (CROSS-CONNECTOR) datasets
#   (superset/v_xconn.sql + superset/v_xconn_efficiency.sql).
#
# WHY THIS EXISTS
#   Task #36: verify the cross-connector view bodies parse/EXPLAIN and produce the
#   RIGHT SHAPE against synthetic perf.* rows for BOTH connectors through the REAL
#   view bodies (only table names swapped perf.* <- raw_connectors_load_testing.*,
#   the same adaptation verify_tab23.sh / verify_tab4.sh use). Tab 5 carries NO
#   verdict (it is the descriptive payoff comparison), so the gold standard is a
#   RENDER-SHAPE + RULE test, not a verdict truth table:
#     1. both views CREATE + SELECT without error (parse + type check), 0 rows empty;
#     2. §2.2 ALIASING: the four headline spellings (Spark throughput_rows_per_sec,
#        Kafka drain_rows_per_sec + the tier-0 null_* pair) FOLD to ONE 'rows_per_sec'
#        series; no headline leaks under its stored name;
#     3. MATCHED-DATASET RULE: matched_dataset = 1 ONLY when two connectors share
#        equal (dataset, rows_expected); mismatched volumes (10M vs 100M) are UNMATCHED
#        and excluded from the efficiency (ratio) table by default;
#     4. a genuinely matched bucket (both connectors at hits@10M) DOES produce a
#        comparable ratio in v_xconn_efficiency (proves the mechanism, not just the
#        empty state);
#     5. §6 covariates environment_class + clickhouse_version are present columns on
#        v_xconn and both-sided on v_xconn_efficiency;
#     6. fixture (BOTH spellings) / failed-outcome / flagged / off-scope (pinned,
#        tier!=1) runs handled correctly — fixtures & failed EXCLUDED, flagged CARRIED
#        (marked, not dropped), off-scope excluded.
#
# HOW IT WORKS
#   clickhouse-local, the byte-locked perf.* DDL, a synthetic set of runs for BOTH
#   connectors seeded inline, then the two view bodies transformed for local execution
#   by swapping the DWH mirror table names back to perf.* (verify_tab4.sh's sed
#   approach). v_xconn_efficiency reads v_xconn, so both are created in dependency
#   order.
#
# USAGE: benchmarks/e2e/dashboard/superset/verify_tab5.sh
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

echo "== Tab 5 render-shape acceptance (v_xconn / v_xconn_efficiency) =="
echo "clickhouse: $CH_BIN"; echo ""

for f in 01_create_database.sql 02_create_runs.sql 03_create_metrics.sql 04_create_ch_inserts.sql; do
  ch --multiquery < "$PERF_DDL_DIR/$f"
done

# ---- synthetic runs -----------------------------------------------------------
#  Cross-connector head/tier-1 runs. TWO comparison buckets:
#   * hits@100M  : Spark ONLY (Spark's real volume today)  -> UNMATCHED (1 connector)
#   * hits@10M   : BOTH Spark + Kafka                       -> MATCHED (2 connectors)
#  The hits@10M bucket is SYNTHETIC (today Spark does not run 10M) — it exists to
#  prove the matched mechanism produces a ratio; the hits@100M / kafka-10M split
#  reproduces TODAY'S honestly-empty reality on the shared series.
#  Scope guards: a pinned run, a tier-0 run, a failed run, a fixture run (each
#  spelling), a flagged run — all seeded to prove the WHERE handling.
#  Spark uses throughput_rows_per_sec; Kafka uses drain_rows_per_sec (§2.2 aliases).
#  environment_class differs across connectors (spark=production, kafka=staging, §6).
# Row map (see comment above for the intent of each run_id):
#   sp-100m-head-t1  Spark @100M real volume, UNMATCHED (no kafka at 100M)
#   sp-10m-head-t1   Spark @10M synthetic matched partner
#   kc-10m-head-t1   Kafka @10M real volume, MATCHED with spark@10M
#   kc-10m-pinned-t1 OFF-SCOPE (arm=pinned) -> excluded
#   kc-10m-head-t0   OFF-SCOPE (tier=0) -> excluded
#   kc-fail-head-t1  EXCLUDED (outcome=failed)
#   fix-kc-head-t1   EXCLUDED (fixture, kafka spelling __verdict_fixture__)
#   fix-sp-head-t1   EXCLUDED (fixture, spark spelling verdict_fixture)
#   kc-flag-head-t1  FLAGGED (value '1') -> CARRIED+marked; @55M (no partner) UNMATCHED
ch --multiquery <<'SQL'
INSERT INTO perf.runs (run_id, run_started_at, run_ended_at, git_sha, connector, connector_version, clickhouse_version, runtime, notes) VALUES
 ('sp-100m-head-t1','2026-07-12 09:00:00','2026-07-12 09:20:00','sha1','spark','sv1','25.4', {'arm':'head','tier':'1','pair_id':'2026-07-12T09-00-00Z-sp100','dataset':'hits','environment_class':'production','target_region':'us-east-2','compute_region':'us-east-2'}, ''),
 ('sp-10m-head-t1','2026-07-12 10:00:00','2026-07-12 10:10:00','sha1','spark','sv1','25.4', {'arm':'head','tier':'1','pair_id':'2026-07-12T10-00-00Z-sp10','dataset':'hits','environment_class':'production','target_region':'us-east-2','compute_region':'us-east-2'}, ''),
 ('kc-10m-head-t1','2026-07-12 11:00:00','2026-07-12 11:15:00','sha2','kafka-connect','kv1','25.3', {'arm':'head','tier':'1','pair_id':'2026-07-12T11-00-00Z-kc10','dataset':'hits','environment_class':'staging','target_region':'us-east-2','compute_region':'us-east-2'}, ''),
 ('kc-10m-pinned-t1','2026-07-12 11:00:00','2026-07-12 11:15:00','sha2','kafka-connect','kv1','25.3', {'arm':'pinned','tier':'1','pair_id':'2026-07-12T11-00-00Z-kc10','dataset':'hits'}, ''),
 ('kc-10m-head-t0','2026-07-12 11:00:00','2026-07-12 11:15:00','sha2','kafka-connect','kv1','25.3', {'arm':'head','tier':'0','pair_id':'2026-07-12T11-00-00Z-kc10','dataset':'hits'}, ''),
 ('kc-fail-head-t1','2026-07-12 12:00:00','2026-07-12 12:15:00','sha3','kafka-connect','kv1','25.3', {'arm':'head','tier':'1','pair_id':'2026-07-12T12-00-00Z-fail','dataset':'hits','outcome':'failed'}, ''),
 ('fix-kc-head-t1','2026-07-12 13:00:00','2026-07-12 13:15:00','sha4','__verdict_fixture__','fv1','25.3', {'arm':'head','tier':'1','pair_id':'2026-07-12T13-00-00Z-fixk','dataset':'hits'}, ''),
 ('fix-sp-head-t1','2026-07-12 13:30:00','2026-07-12 13:45:00','sha5','verdict_fixture','fv1','25.3', {'arm':'head','tier':'1','pair_id':'2026-07-12T13-30-00Z-fixs','dataset':'hits'}, ''),
 ('kc-flag-head-t1','2026-07-12 14:00:00','2026-07-12 14:15:00','sha6','kafka-connect','kv1','25.3', {'arm':'head','tier':'1','pair_id':'2026-07-12T14-00-00Z-flag','dataset':'hits','flagged':'1','flag_reason':'rebalance'}, '');

INSERT INTO perf.metrics (run_id, metric_name, unit, value, recorded_at) VALUES
 ('sp-100m-head-t1','throughput_rows_per_sec','rows/s', 2000000, now()),
 ('sp-100m-head-t1','rows_expected','count', 100000000, now()),
 ('sp-100m-head-t1','ch_insert_cpu_seconds_per_Mrows','s/Mrows', 5.0, now()),
 ('sp-100m-head-t1','parts_per_insert','ratio', 1.2, now()),
 ('sp-10m-head-t1','throughput_rows_per_sec','rows/s', 1800000, now()),
 ('sp-10m-head-t1','rows_expected','count', 10000000, now()),
 ('sp-10m-head-t1','ch_insert_cpu_seconds_per_Mrows','s/Mrows', 6.0, now()),
 ('sp-10m-head-t1','parts_per_insert','ratio', 1.1, now()),
 ('kc-10m-head-t1','drain_rows_per_sec','rows/s', 900000, now()),
 ('kc-10m-head-t1','rows_expected','count', 10000000, now()),
 ('kc-10m-head-t1','ch_insert_cpu_seconds_per_Mrows','s/Mrows', 3.0, now()),
 ('kc-10m-head-t1','ch_parts_per_insert','ratio', 1.0, now()),
 ('kc-10m-pinned-t1','drain_rows_per_sec','rows/s', 850000, now()),
 ('kc-10m-head-t0','null_drain_rows_per_sec','rows/s', 3000000, now()),
 ('kc-fail-head-t1','drain_rows_per_sec','rows/s', 100, now()),
 ('kc-flag-head-t1','drain_rows_per_sec','rows/s', 500000, now()),
 ('kc-flag-head-t1','rows_expected','count', 55000000, now());
SQL

# ---- transform + create the two views (perf.* substitution) -------------------
#  v_xconn_efficiency reads v_xconn, so create v_xconn FIRST.
mk_local() { sed -e "s/raw_connectors_load_testing\./perf./g" "$1" > "$2"; }
mk_local "$SCRIPT_DIR/v_xconn.sql"            "$CH_STATE/xconn.sql"
mk_local "$SCRIPT_DIR/v_xconn_efficiency.sql" "$CH_STATE/xconn_eff.sql"
ch --multiquery < "$CH_STATE/xconn.sql"
ch --multiquery < "$CH_STATE/xconn_eff.sql"
echo "OK  : both views CREATE without error"

fails=0
check() { # label expected actual
  if [ "$2" = "$3" ]; then printf "PASS: %-62s %s\n" "$1" "$3"
  else printf "FAIL: %-62s expected=%s actual=%s\n" "$1" "$2" "$3"; fails=$((fails+1)); fi
}

# --- 1. §2.2 ALIASING: the four headline spellings fold to ONE rows_per_sec -----
# Spark throughput + Kafka drain both surface as metric_name='rows_per_sec'.
# in-scope head/t1 rows_per_sec: spark@100M, spark@10M, kafka@10M, kafka-flag@55M = 4
rps_rows="$(ch --query "SELECT count() FROM perf.v_xconn WHERE metric_name='rows_per_sec'")"
check "alias: rows_per_sec present for BOTH connectors (3 spark/kafka in-scope + 1 flagged)" "4" "$rps_rows"
sp_rps="$(ch --query "SELECT toString(value) FROM perf.v_xconn WHERE metric_name='rows_per_sec' AND connector='spark' AND rows_expected=100000000")"
check "alias: spark throughput_rows_per_sec -> rows_per_sec value (100M row)" "2000000" "$sp_rps"
kc_rps="$(ch --query "SELECT toString(value) FROM perf.v_xconn WHERE metric_name='rows_per_sec' AND connector='kafka-connect' AND rows_expected=10000000")"
check "alias: kafka drain_rows_per_sec -> rows_per_sec value" "900000" "$kc_rps"
# no headline leaks under its stored name
leak="$(ch --query "SELECT count() FROM perf.v_xconn WHERE metric_name IN ('throughput_rows_per_sec','drain_rows_per_sec','null_rows_per_sec','null_drain_rows_per_sec')")"
check "alias: NO stored headline name leaks (all folded)" "0" "$leak"

# --- 2. §7 legacy fold: ch_parts_per_insert -> parts_per_insert -----------------
kc_parts="$(ch --query "SELECT toString(value) FROM perf.v_xconn WHERE metric_name='parts_per_insert' AND connector='kafka-connect'")"
check "fold: kafka ch_parts_per_insert -> parts_per_insert value" "1" "$kc_parts"
no_legacy="$(ch --query "SELECT count() FROM perf.v_xconn WHERE metric_name='ch_parts_per_insert'")"
check "fold: NO ch_-prefixed legacy parts name leaks" "0" "$no_legacy"
# pinned server-cost name is present unchanged (must be ch_insert_cpu_seconds_per_Mrows)
cpu_name="$(ch --query "SELECT count() FROM perf.v_xconn WHERE metric_name='ch_insert_cpu_seconds_per_Mrows'")"
check "pinned: ch_insert_cpu_seconds_per_Mrows present (NOT server_cpu_per_Mrows)" "3" "$cpu_name"
bad_name="$(ch --query "SELECT count() FROM perf.v_xconn WHERE metric_name='server_cpu_per_Mrows'")"
check "pinned: server_cpu_per_Mrows NEVER appears" "0" "$bad_name"

# --- 3. MATCHED-DATASET RULE ----------------------------------------------------
# hits@10M: both connectors present -> matched=1. hits@100M: spark only -> matched=0.
matched_10m="$(ch --query "SELECT DISTINCT matched_dataset FROM perf.v_xconn WHERE rows_expected=10000000 AND metric_name='rows_per_sec'")"
check "matched: hits@10M (both connectors) => matched_dataset=1" "1" "$matched_10m"
unmatched_100m="$(ch --query "SELECT DISTINCT matched_dataset FROM perf.v_xconn WHERE rows_expected=100000000 AND connector='spark' AND metric_name='rows_per_sec'")"
check "matched: hits@100M (spark only) => matched_dataset=0" "0" "$unmatched_100m"
bucket_10m="$(ch --query "SELECT DISTINCT bucket_connectors FROM perf.v_xconn WHERE rows_expected=10000000")"
check "matched: hits@10M bucket has 2 distinct connectors" "2" "$bucket_10m"
comp_bucket="$(ch --query "SELECT DISTINCT comp_bucket FROM perf.v_xconn WHERE rows_expected=10000000 AND metric_name='rows_per_sec'")"
check "matched: comp_bucket key format (dataset@rows_expected)" "hits@10000000" "$comp_bucket"

# --- 4. EFFICIENCY TABLE: ratio computed only on matched, and it IS computed -----
# matched hits@10M rows_per_sec: spark 1.8M / kafka 0.9M = 2.0
eff_ratio="$(ch --query "SELECT toString(round(ratio_spark_over_kafka,3)) FROM perf.v_xconn_efficiency WHERE metric_name='rows_per_sec'")"
check "efficiency: matched rows_per_sec ratio spark/kafka = 1.8M/0.9M" "2" "$eff_ratio"
# server cost matched: spark 6.0 / kafka 3.0 = 2.0
eff_cpu="$(ch --query "SELECT toString(round(ratio_spark_over_kafka,3)) FROM perf.v_xconn_efficiency WHERE metric_name='ch_insert_cpu_seconds_per_Mrows'")"
check "efficiency: matched ch_insert_cpu ratio spark/kafka = 6.0/3.0" "2" "$eff_cpu"
# the UNMATCHED 100M spark row must NOT contribute to the efficiency medians:
# only the 10M (matched) spark value (1.8M) is medianed, NOT the 100M (2.0M).
eff_sp_median="$(ch --query "SELECT toString(spark_median) FROM perf.v_xconn_efficiency WHERE metric_name='rows_per_sec'")"
check "efficiency: spark median uses ONLY matched (10M=1.8M), excludes 100M" "1800000" "$eff_sp_median"

# --- 5. §6 COVARIATES present (environment_class + clickhouse_version) -----------
sp_env="$(ch --query "SELECT DISTINCT environment_class FROM perf.v_xconn WHERE connector='spark'")"
check "covariate: spark environment_class = production" "production" "$sp_env"
kc_env="$(ch --query "SELECT DISTINCT environment_class FROM perf.v_xconn WHERE connector='kafka-connect' AND NOT flagged")"
check "covariate: kafka environment_class = staging" "staging" "$kc_env"
eff_both_env="$(ch --query "SELECT concat(spark_environment_class,'|',kafka_environment_class) FROM perf.v_xconn_efficiency WHERE metric_name='rows_per_sec'")"
check "covariate: efficiency table carries BOTH sides' env_class (the §6 caveat)" "production|staging" "$eff_both_env"
ch_ver_present="$(ch --query "SELECT if(count()=countIf(clickhouse_version!=''),1,0) FROM perf.v_xconn")"
check "covariate: clickhouse_version populated on every row" "1" "$ch_ver_present"

# --- 6. SCOPE GUARDS: fixtures/failed/off-scope excluded; flagged carried --------
leak_excluded="$(ch --query "SELECT count() FROM perf.v_xconn WHERE run_id IN ('kc-10m-pinned-t1','kc-10m-head-t0','kc-fail-head-t1','fix-kc-head-t1','fix-sp-head-t1')")"
check "scope: pinned/tier0/failed/fixture(both spellings) NEVER leak" "0" "$leak_excluded"
flag_carried="$(ch --query "SELECT toString(flagged) FROM perf.v_xconn WHERE run_id='kc-flag-head-t1' AND metric_name='rows_per_sec'")"
check "scope: flagged run CARRIED (marked), not dropped" "1" "$flag_carried"
flag_reason="$(ch --query "SELECT DISTINCT flag_reason FROM perf.v_xconn WHERE run_id='kc-flag-head-t1'")"
check "scope: flagged run carries flag_reason" "rebalance" "$flag_reason"
# a flagged @100M unmatched run must not sneak onto the matched comparison side
flag_unmatched="$(ch --query "SELECT DISTINCT matched_dataset FROM perf.v_xconn WHERE run_id='kc-flag-head-t1' AND metric_name='rows_per_sec'")"
check "scope: flagged @55M run is UNMATCHED (not a comparable number)" "0" "$flag_unmatched"

# --- 7. HONEST-EMPTY reproduction: if we DROP the synthetic 10M bucket, the -------
#        matched side (and thus the efficiency ratio) is EMPTY — today's reality.
ch --query "TRUNCATE TABLE perf.metrics"
ch --query "TRUNCATE TABLE perf.runs"
ch --multiquery <<'SQL'
-- Today's ACTUAL state: Kafka only at 10M, Spark only at 100M — no shared bucket.
INSERT INTO perf.runs (run_id, run_started_at, run_ended_at, git_sha, connector, connector_version, clickhouse_version, runtime, notes) VALUES
 ('sp-real-head-t1','2026-07-12 09:00:00','2026-07-12 09:20:00','shaA','spark','sv1','25.4',
   {'arm':'head','tier':'1','pair_id':'2026-07-12T09-00-00Z-spR','dataset':'hits','environment_class':'production'}, ''),
 ('kc-real-head-t1','2026-07-12 11:00:00','2026-07-12 11:15:00','shaB','kafka-connect','kv1','25.3',
   {'arm':'head','tier':'1','pair_id':'2026-07-12T11-00-00Z-kcR','dataset':'hits','environment_class':'staging'}, '');
INSERT INTO perf.metrics (run_id, metric_name, unit, value, recorded_at) VALUES
 ('sp-real-head-t1','throughput_rows_per_sec','rows/s', 2000000, now()),
 ('sp-real-head-t1','rows_expected','count', 100000000, now()),
 ('kc-real-head-t1','drain_rows_per_sec','rows/s', 900000, now()),
 ('kc-real-head-t1','rows_expected','count', 10000000, now());
SQL
matched_today="$(ch --query "SELECT countIf(matched_dataset=1) FROM perf.v_xconn")"
check "honest-empty: TODAY (10M vs 100M) => ZERO matched rows" "0" "$matched_today"
eff_today="$(ch --query "SELECT count() FROM perf.v_xconn_efficiency")"
check "honest-empty: efficiency table EMPTY today (no matched bucket)" "0" "$eff_today"
# but the view is still informative on the UNMATCHED (context) side:
context_today="$(ch --query "SELECT count() FROM perf.v_xconn WHERE matched_dataset=0 AND metric_name='rows_per_sec'")"
check "honest-empty: UNMATCHED context still shows both connectors (informative)" "2" "$context_today"

echo ""
if [ "$fails" -eq 0 ]; then
  echo "==================================================================="
  echo "TAB 5 DATASETS ACCEPTED: all cross-connector render-shape + rule assertions passed"
  echo "==================================================================="
  exit 0
else
  echo "==================================================================="
  echo "TAB 5 DATASETS FAILED: $fails assertion(s) failed"
  echo "==================================================================="
  exit 1
fi
