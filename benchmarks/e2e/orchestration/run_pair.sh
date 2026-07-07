#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# =============================================================================
# Benchmark v2 — nightly two-arm pair orchestrator (task 31).
#
# Implements the full nightly anatomy (plan §5 steps 1-6) as composable phase
# functions: scale up -> pre-load (topic + producer) -> per-arm (day-parity
# order) [Tier 0 drain -> Tier 1 drain, each: deploy Connect+connector, wait
# tasks RUNNING, poller sample -> lag 0, finalize+insert metrics, capture SQL,
# integrity, insert_run_record, export] -> teardown topic -> scale down.
#
# STATUS: CODE-COMPLETE / VALIDATION-PENDING. No live cluster or AWS/CH creds
# were available; this is validated OFFLINE only (bash -n, --plan dry-run, unit
# tests). The acceptance criterion (a green e2e pair) is NOT claimed. See
# README.md "Known gaps".
#
# Two run modes:
#   --plan / -n   Print the phase sequence for the resolved arm order and exit 0.
#                 Executes NOTHING (no cluster/creds needed). Used in review + CI
#                 self-check to make the future live run auditable.
#   (default)     Execute the full pair. Requires the CI workflow's env (creds,
#                 image refs, kubeconfig). When run standalone a trap ensures the
#                 always-cleanup path (topic delete + scale down) still runs on
#                 any failure; in CI those are ALSO separate `if: always()` steps
#                 (overseer directive b) so cleanup survives even a hard kill of
#                 this script.
#
# Arm order (overseer directive g): alternates by UTC day-of-year parity.
#   EVEN day -> head first ; ODD day -> pinned first.
#   (Documented, matching the Spark CI convention's parity switch; DOY not DOM
#    to avoid month-boundary parity repeats.)
#
# run_id construction (contract §1.2): pair_id = RUN_ID from lib_runid.sh;
#   per (arm,tier) run_id = <pair_id>-<arm>-t<tier>. 4 runs rows per night.
#
# run_cost_usd (contract §2.1 amendment, overseer directive d): FULL pair cost
#   charged ONCE on the FIRST-run arm's row; the other arm omits it.
#
# Single-run concurrency (overseer directive f): enforced by the GitHub Actions
#   `concurrency:` group in benchmark-nightly.yml (documented limits in README).
#   This script does not add a second lock.
# =============================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
INFRA_DIR="${E2E_DIR}/infra"
CAPTURE_DIR="${E2E_DIR}/capture"
POLLER_DIR="${E2E_DIR}/poller"
PRODUCER_DIR="${E2E_DIR}/producer"
TEMPLATES_DIR="${SCRIPT_DIR}/templates"

NS="${K8S_NAMESPACE:-kafka-bench}"
TOPIC="${TOPIC:-hits}"
CONNECT_NAME="bench-connect"
CONNECTOR_NAME="bench-clickhouse-sink"
CONNECT_REST="http://${CONNECT_NAME}-connect-api.${NS}.svc:8083"
EXPECTED_PARTITIONS=3
EXPECTED_TASKS=3
CONNECT_HEAP="${CONNECT_HEAP:-2048m}"
POLL_INTERVAL="${POLL_INTERVAL:-10}"
POLL_TIMEOUT="${POLL_TIMEOUT:-3600}"
ARTIFACT_DIR="${ARTIFACT_DIR:-${SCRIPT_DIR}/artifacts}"

# m6i.large us-east-2 on-demand pricing for the pair-cost calc (see phase_scale_down
# / emit_run_cost). Kept here (not an AWS Pricing API call) per plan "no new AWS
# calls"; bump deliberately when prices drift.
M6I_LARGE_USD_PER_HR="${M6I_LARGE_USD_PER_HR:-0.096}"   # us-east-2 on-demand
EBS_GP3_USD_PER_GB_MO="${EBS_GP3_USD_PER_GB_MO:-0.08}"
BROKER_EBS_GB="${BROKER_EBS_GB:-70}"
SCALE_UP_NODES="${SCALE_UP_NODES:-2}"

log()  { printf '\033[1;34m[run_pair]\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[1;33m[run_pair:warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[run_pair:error]\033[0m %s\n' "$*" >&2; exit 1; }

# --------------------------------------------------------------------------- #
# Arm order — UTC day-of-year parity (overseer directive g).
# --------------------------------------------------------------------------- #
resolve_arm_order() {
  local doy
  doy="$(date -u +%j)"
  # 10# forces base-10 (leading-zero DOY like 007 would otherwise be octal).
  if [ $((10#$doy % 2)) -eq 0 ]; then
    ARM_ORDER=(head pinned)   # even day: head first
  else
    ARM_ORDER=(pinned head)   # odd day: pinned first
  fi
  echo "${ARM_ORDER[@]}"
}

# --------------------------------------------------------------------------- #
# --plan / -n : print the phase sequence for the resolved order; execute nothing.
# --------------------------------------------------------------------------- #
print_plan() {
  local order=("$@")
  local doy; doy="$(date -u +%j)"
  local parity="odd -> pinned first"
  [ $((10#$doy % 2)) -eq 0 ] && parity="even -> head first"
  cat <<EOF
=== Benchmark v2 nightly pair — PHASE PLAN (dry-run, nothing executed) ===
UTC day-of-year : ${doy} (${parity})
arm order       : ${order[0]} then ${order[1]}
pair_id         : <RUN_ID from lib_runid.sh: YYYY-MM-DDTHH-MM-SSZ-<shortsha>>
run rows/night  : 4 -> (${order[0]},t0) (${order[0]},t1) (${order[1]},t0) (${order[1]},t1)
                  all sharing one pair_id (contract §1.2)
run_cost_usd    : charged ONCE on FIRST-run arm (${order[0]}) row (contract §2.1)

PHASE 1  scale up          eksctl node group 0->${SCALE_UP_NODES}; Kafka CR + Schema Registry Ready
PHASE 2  pre-load          create topic (${EXPECTED_PARTITIONS} partitions RF1) -> ASSERT count==${EXPECTED_PARTITIONS}
                           -> producer Job -> capture rows_expected from JSON summary (fail on exit!=0)
PHASE 3  per arm, in order [ ${order[0]} , ${order[1]} ]:
  for arm in ${order[0]} ${order[1]}:
    apply CH-creds Secret + connect-metrics ConfigMap
    deploy KafkaConnect CR (arm image) -> wait Ready
    --- Tier 0 (hits_null) ---
      pre-run covariates (21) ; NO truncate (Null engine)
      deploy connector (topic2TableMap=hits=hits_null, group ch-sink-<arm>-t0)
      wait connector + ${EXPECTED_TASKS} tasks RUNNING   (poller prereq 5)
      poller sample -> lag 0 (RUN_END = lag-0 ts)
      finalize --insert (run_id <pair_id>-<arm>-t0, tier 0)
      capture SQL [11..19,20,22] window ; on any fail -> rollback + abort
      insert_run_record (runs row AFTER metrics) ; on fail -> rollback + abort
      export (gated on runs insert)
      integrity check LAST (Tier 0: rows-only sanity)
      delete connector + consumer group
    --- Tier 1 (hits) ---
      truncate target ; pre-run covariates (21)
      deploy connector (topic2TableMap=hits=hits, group ch-sink-<arm>-t1)
      wait connector + ${EXPECTED_TASKS} tasks RUNNING
      poller sample -> lag 0 (RUN_END = lag-0 ts)
      wait_for_settle -> SETTLE_END / settle_timed_out
      finalize --insert (run_id <pair_id>-<arm>-t1, tier 1)
      capture SQL [11..19,20,22] window ; on any fail -> rollback + abort
      (first-run arm ${order[0]} only: emit_run_cost) insert_run_record ; on fail -> rollback + abort
      export (gated) ; integrity check LAST (FAIL run on mismatch, evidence already exported)
      delete connector + consumer group
    delete KafkaConnect CR + CH-creds Secret
PHASE 4  teardown topic    delete KafkaTopic CR   (if: always in CI)
PHASE 5  scale down        scale-down.sh -> node group 0   (if: always in CI)
=== END PHASE PLAN ===
EOF
}

# --------------------------------------------------------------------------- #
# runtime-map echo (contract §1 + §1.4 + overseer directive c). Every plan §6
# setting appears here AND in kafkaconnector.json.tmpl (cross-checked by
# tests/test_config_crosscheck.py). Emits a JSON object on stdout.
#
# Args: arm tier
# Env : PAIR_ID, GIT_SHA, CONNECTOR_VERSION, KAFKA_CONNECT_VERSION,
#       STRIMZI_VERSION, TARGET_REGION, ENVIRONMENT_CLASS, COMPUTE_REGION,
#       PLUGIN_SHA256 (all set by build_runtime_env / the workflow).
# --------------------------------------------------------------------------- #
build_runtime_json() {
  local arm="$1" tier="$2"
  python3 - "$arm" "$tier" <<'PY'
import json, os, sys
arm, tier = sys.argv[1], sys.argv[2]
rt = {
    # --- scope & identity (contract §1.1/§1.2) ---
    "arm": arm,
    "tier": tier,
    "pair_id": os.environ.get("PAIR_ID", ""),
    "target_region": os.environ.get("TARGET_REGION", ""),
    "environment_class": os.environ.get("ENVIRONMENT_CLASS", ""),
    # NEW contract key (overseer directive c): EKS region, recorded from day one.
    "compute_region": os.environ.get("COMPUTE_REGION", ""),
    # --- shared config keys (contract §1.4) ---
    "batch_size": os.environ.get("CFG_MAX_POLL_RECORDS", ""),   # sink flush size = max.poll.records
    "write_parallelism": os.environ.get("CFG_TASKS_MAX", ""),   # tasks.max
    "async_insert": "0",                                        # sink forces async OFF
    "partition_scheme": os.environ.get("CFG_PARTITION_SCHEME", ""),
    "dataset": "hits",
    # NOTE: warm_up is OMITTED by design (kafka has no priming step; absent => no warm-up).
    # --- plan §6 config echo (every setting) ---
    "exactlyOnce": "false",
    "consumer_max_poll_records": os.environ.get("CFG_MAX_POLL_RECORDS", ""),
    "consumer_max_partition_fetch_bytes": os.environ.get("CFG_MAX_PARTITION_FETCH_BYTES", ""),
    "consumer_fetch_max_bytes": os.environ.get("CFG_FETCH_MAX_BYTES", ""),
    "consumer_max_poll_interval_ms": os.environ.get("CFG_MAX_POLL_INTERVAL_MS", ""),
    "clickhouse_client_insert_timeout_ms": os.environ.get("CFG_INSERT_TIMEOUT_MS", ""),
    "client_version": "V1",                                     # plan §6 (decision 5) sink default
    "insert_format": "Avro+SchemaRegistry->RowBinary",          # plan §6 format
    "clickhouse_settings": "async_insert=0,wait_end_of_query=1",
    # --- Kafka-namespaced provenance (contract §1.4) ---
    "kafka_connect_version": os.environ.get("KAFKA_CONNECT_VERSION", ""),
    "strimzi_version": os.environ.get("STRIMZI_VERSION", ""),
    "plugin_sha256": os.environ.get("PLUGIN_SHA256", ""),
}
# drop empties so a missing provenance value is absent, not "" (a real value)
rt = {k: v for k, v in rt.items() if v != ""}
print(json.dumps(rt, sort_keys=True))
PY
}

# =========================================================================== #
# EXECUTION PHASES (each a function; only run when not in --plan mode).
# =========================================================================== #

phase_scale_up() {
  log "PHASE 1: scale up to ${SCALE_UP_NODES} node(s)"
  "${INFRA_DIR}/scale-up.sh" "${SCALE_UP_NODES}" || die "scale-up failed"
}

phase_preload() {
  log "PHASE 2: pre-load topic + producer"
  # Fresh topic (3 partitions RF1) via the KafkaTopic CR (topicOperator).
  kubectl delete kafkatopic "${TOPIC}" -n "${NS}" --ignore-not-found --wait=true 2>/dev/null || true
  envsubst < "${TEMPLATES_DIR}/kafkatopic.yaml.tmpl" | kubectl apply -f - || die "topic apply failed"
  kubectl -n "${NS}" wait kafkatopic/"${TOPIC}" --for=condition=Ready --timeout=120s \
    || die "topic did not become Ready"

  # Overseer directive e: ASSERT actual partition count == 3 before producing.
  local parts
  parts="$(kubectl -n "${NS}" get kafkatopic "${TOPIC}" -o jsonpath='{.spec.partitions}' 2>/dev/null)"
  [ "${parts}" = "${EXPECTED_PARTITIONS}" ] \
    || die "topic partition count is '${parts}', expected ${EXPECTED_PARTITIONS} (one-task-per-partition invariant broken)"
  log "  topic ${TOPIC} Ready with ${parts} partitions"

  # Producer Job (fresh; backoffLimit 0). Overseer directive e: parameterize an
  # IRSA serviceAccountName for S3 read (PRODUCER_SA). IAM role is a provisioning
  # TODO (see README). Image ref + parquet source come from the workflow env.
  kubectl -n "${NS}" delete job hits-producer --ignore-not-found --wait=true 2>/dev/null || true
  local job_yaml="${ARTIFACT_DIR}/producer-job.yaml"
  mkdir -p "${ARTIFACT_DIR}"
  # Patch image + serviceAccountName + PARQUET_SOURCE into a copy of job.yaml.
  python3 "${SCRIPT_DIR}/render_producer_job.py" \
      --job "${PRODUCER_DIR}/job.yaml" \
      --image "${PRODUCER_IMAGE:?PRODUCER_IMAGE required}" \
      --service-account "${PRODUCER_SA:-hits-producer}" \
      --parquet-source "${PARQUET_SOURCE:?PARQUET_SOURCE required}" \
      --out "${job_yaml}" || die "producer job render failed"
  kubectl apply -f "${job_yaml}" || die "producer job apply failed"

  log "  waiting for producer Job to complete"
  kubectl -n "${NS}" wait --for=condition=complete job/hits-producer --timeout="${PRODUCER_TIMEOUT:-21600}s" \
    || die "producer Job did not complete (see logs)"

  # rows_expected from the producer's JSON summary (last stdout line; exit 0 only).
  local plog; plog="$(kubectl -n "${NS}" logs job/hits-producer --tail=-1 2>/dev/null)"
  echo "${plog}" > "${ARTIFACT_DIR}/producer.log"
  ROWS_EXPECTED="$(echo "${plog}" | tail -1 | python3 -c 'import sys,json; print(json.load(sys.stdin)["rows_expected"])' 2>/dev/null)"
  [ -n "${ROWS_EXPECTED}" ] && [ "${ROWS_EXPECTED}" != "None" ] \
    || die "could not parse rows_expected from producer summary"
  export ROWS_EXPECTED
  export SOURCE_ROWS_EXPECTED="${ROWS_EXPECTED}"
  log "  rows_expected = ${ROWS_EXPECTED}"
}

# Apply the per-run CH-creds Secret (from CI-secret env) + the metrics ConfigMap.
apply_secret_and_metrics() {
  kubectl -n "${NS}" create secret generic bench-ch-creds \
    --from-literal=hostname="${TARGET_CH_HOST:?}" \
    --from-literal=username="${TARGET_CH_USER:?}" \
    --from-literal=password="${TARGET_CH_PASSWORD:?}" \
    --dry-run=client -o yaml | kubectl apply -f - || die "secret apply failed"
  kubectl apply -f "${TEMPLATES_DIR}/connect-metrics-configmap.yaml" || die "metrics configmap apply failed"
}

deploy_connect() {
  local arm_image="$1"
  log "  deploying KafkaConnect (image=${arm_image})"
  ARM_IMAGE="${arm_image}" CONNECT_HEAP="${CONNECT_HEAP}" \
    envsubst '${ARM_IMAGE} ${CONNECT_HEAP}' < "${TEMPLATES_DIR}/kafkaconnect.yaml.tmpl" \
    | kubectl apply -f - || die "KafkaConnect apply failed"
  kubectl -n "${NS}" wait kafkaconnect/"${CONNECT_NAME}" --for=condition=Ready --timeout=600s \
    || die "KafkaConnect did not become Ready"

  # Read this arm's provenance from the baked-in /opt/benchmark/provenance.json
  # (Dockerfile) so GIT_SHA / CONNECTOR_VERSION / KAFKA_CONNECT_VERSION /
  # STRIMZI_VERSION / PLUGIN_SHA256 come from the IMAGE UNDER TEST, per arm
  # (overseer directive c) — not from the checkout (which is HEAD for both arms).
  local prov
  prov="$(kubectl -n "${NS}" exec deploy/"${CONNECT_NAME}"-connect -- \
          cat /opt/benchmark/provenance.json 2>/dev/null || echo '{}')"
  GIT_SHA="$(echo "${prov}" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("git_sha",""))' 2>/dev/null)"
  CONNECTOR_VERSION="$(echo "${prov}" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("connector_version",""))' 2>/dev/null)"
  KAFKA_CONNECT_VERSION="$(echo "${prov}" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("kafka_version",""))' 2>/dev/null)"
  STRIMZI_VERSION="$(echo "${prov}" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("strimzi_version",""))' 2>/dev/null)"
  PLUGIN_SHA256="$(echo "${prov}" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("plugin_sha256",""))' 2>/dev/null)"
  export GIT_SHA CONNECTOR_VERSION KAFKA_CONNECT_VERSION STRIMZI_VERSION PLUGIN_SHA256
  [ -n "${GIT_SHA}" ] || warn "provenance.json git_sha empty — insert_run_record will fail its GIT_SHA require()"
  log "  arm provenance: git_sha=${GIT_SHA:0:12} connector_version=${CONNECTOR_VERSION}"
}

delete_connect() {
  kubectl -n "${NS}" delete kafkaconnect "${CONNECT_NAME}" --ignore-not-found --wait=true 2>/dev/null || true
  kubectl -n "${NS}" delete secret bench-ch-creds --ignore-not-found 2>/dev/null || true
}

deploy_connector() {
  local ch_table="$1" group="$2"
  log "  deploying connector (table=${ch_table}, group=${group})"
  CH_TABLE="${ch_table}" CONSUMER_GROUP="${group}" \
    python3 "${SCRIPT_DIR}/render_connector.py" \
      --template "${TEMPLATES_DIR}/kafkaconnector.json.tmpl" \
      --ch-table "${ch_table}" --group "${group}" --out - \
    | kubectl apply -f - || die "connector apply failed"
}

delete_connector() {
  local group="$1"
  kubectl -n "${NS}" delete kafkaconnector "${CONNECTOR_NAME}" --ignore-not-found --wait=true 2>/dev/null || true
  # Delete the consumer group so the next drain starts fresh at offset 0
  # (plan §5 step 3c). Best-effort via a kafka client pod.
  "${SCRIPT_DIR}/delete_consumer_group.sh" "${group}" 2>/dev/null || warn "consumer-group delete best-effort failed for ${group}"
}

wait_tasks_running() {
  # Poller prerequisite 5: start the poller only AFTER connector + all tasks
  # report RUNNING. Poll the Connect REST /status.
  local deadline=$(( $(date +%s) + ${TASKS_RUNNING_TIMEOUT:-600} ))
  log "  waiting for connector + ${EXPECTED_TASKS} tasks RUNNING"
  while :; do
    local status running conn_state
    status="$(kubectl -n "${NS}" exec deploy/"${CONNECT_NAME}"-connect -- \
              curl -sf "${CONNECT_REST}/connectors/${CONNECTOR_NAME}/status" 2>/dev/null || echo '')"
    if [ -n "${status}" ]; then
      conn_state="$(echo "${status}" | python3 -c 'import sys,json;print(json.load(sys.stdin)["connector"]["state"])' 2>/dev/null || echo '')"
      running="$(echo "${status}" | python3 -c 'import sys,json;d=json.load(sys.stdin);print(sum(1 for t in d.get("tasks",[]) if t["state"]=="RUNNING"))' 2>/dev/null || echo 0)"
      if [ "${conn_state}" = "RUNNING" ] && [ "${running}" -ge "${EXPECTED_TASKS}" ]; then
        log "  connector RUNNING with ${running}/${EXPECTED_TASKS} tasks RUNNING"
        return 0
      fi
    fi
    [ "$(date +%s)" -ge "${deadline}" ] && die "connector/tasks did not all reach RUNNING in time"
    sleep 5
  done
}

# Run the poller sample loop for a (arm,tier); returns via $SAMPLES_FILE.
run_poller_sample() {
  local group="$1"
  SAMPLES_FILE="${ARTIFACT_DIR}/samples-${RUN_ID}.jsonl"
  log "  poller sample (group=${group}) -> ${SAMPLES_FILE}"
  ( cd "${POLLER_DIR}" && python3 poller.py sample \
      --group "${group}" --topic "${TOPIC}" --connector "${CONNECTOR_NAME}" \
      --out "${SAMPLES_FILE}" \
      --bootstrap "${KAFKA_BOOTSTRAP:-bench-kafka-bootstrap.${NS}.svc:9092}" \
      --connect-url "${CONNECT_REST}" \
      --jmx-url "${JMX_METRICS_URL:-}" \
      --cadvisor-url "${POD_CADVISOR_URL:-}" \
      --pod-name "${CONNECT_POD_NAME:-}" --pod-container "${CONNECT_POD_CONTAINER:-}" \
      --poll-interval "${POLL_INTERVAL}" --timeout "${POLL_TIMEOUT}" ) \
    && POLLER_RC=0 || POLLER_RC=$?
  # exit 0 drained, 2 timeout. A timeout means lag never hit 0 -> the finalizer
  # records lag_reached_zero=0 -> flag_reason drain_incomplete (contract §1.3).
  RUN_END="$(date -u +%Y-%m-%dT%H:%M:%S)"
  export RUN_END
  if [ "${POLLER_RC}" = "2" ]; then
    warn "  poller TIMED OUT (drain incomplete) — continuing to capture (run will be FLAGGED)"
  elif [ "${POLLER_RC}" != "0" ]; then
    die "poller sample failed (rc=${POLLER_RC})"
  fi
}

# finalize + insert the poller scalars into perf.metrics for a (arm,tier); also
# emit the guards JSON so insert_run_record can set flagged/flag_reason
# (overseer directive h). Sets FLAGGED / FLAG_REASON in the env.
finalize_and_insert_metrics() {
  local tier="$1"
  local out="${ARTIFACT_DIR}/finalize-${RUN_ID}.json"
  log "  finalize + insert metrics (tier ${tier}, run_id ${RUN_ID})"
  ( cd "${POLLER_DIR}" && python3 poller.py finalize \
      --samples "${SAMPLES_FILE}" --run-id "${RUN_ID}" --tier "${tier}" \
      --rows-expected "${ROWS_EXPECTED}" --expected-tasks "${EXPECTED_TASKS}" \
      --insert ) > "${out}" || die "poller finalize/insert failed"
  # Wire the poller guards JSON into flagged/flag_reason (overseer directive h).
  FLAGGED="$(python3 -c 'import sys,json;print(json.load(open(sys.argv[1]))["guards"]["flagged"])' "${out}")"
  FLAG_REASON="$(python3 -c 'import sys,json;print(json.load(open(sys.argv[1]))["guards"]["flag_reason"])' "${out}")"
  export FLAGGED FLAG_REASON
  log "  guards: flagged=${FLAGGED} flag_reason='${FLAG_REASON}'"
}

# Gated CH-side capture + run-record + export + integrity for a (arm,tier).
# Mirrors capture/README "Order of operations" (BINDING gating discipline).
capture_and_record() {
  local arm="$1" tier="$2" emit_cost="$3"
  log "  gated capture + run record (arm=${arm}, tier=${tier})"

  # Numeric-ordered gated capture; ANY failure -> rollback + abort (README).
  local f
  for f in 11 12 13 14 15 16 17 18 19 20 22; do
    local sqlfile
    sqlfile="$(ls "${E2E_DIR}"/sql/capture/${f}_*.sql 2>/dev/null | head -1)"
    [ -n "${sqlfile}" ] || die "capture SQL ${f}_* not found"
    ( cd "${CAPTURE_DIR}" && python3 run_metrics_sql.py "${sqlfile}" ) \
      || { warn "capture ${f} failed -> rollback"; ( cd "${CAPTURE_DIR}" && python3 rollback_run_metrics.py ); die "capture aborted"; }
  done

  # run_cost_usd (contract §2.1): FULL pair cost on the FIRST-run arm only.
  if [ "${emit_cost}" = "1" ]; then
    emit_run_cost || { warn "run_cost emit failed -> rollback"; ( cd "${CAPTURE_DIR}" && python3 rollback_run_metrics.py ); die "cost emit aborted"; }
  fi

  # runs row AFTER metrics (gated); failure -> rollback + abort (README).
  local runtime_json
  runtime_json="$(build_runtime_json "${arm}" "${tier}")"
  # Fold the poller guards into the runtime map (overseer directive h): flagged
  # and flag_reason are runtime keys (contract §1.3).
  if [ "${FLAGGED:-0}" = "1" ]; then
    runtime_json="$(python3 -c 'import json,sys;d=json.loads(sys.argv[1]);d["flagged"]="1";d["flag_reason"]=sys.argv[2];print(json.dumps(d,sort_keys=True))' "${runtime_json}" "${FLAG_REASON}")"
  fi
  ( cd "${CAPTURE_DIR}" && RUNTIME="${runtime_json}" python3 insert_run_record.py ) \
    || { warn "insert_run_record failed -> rollback"; ( cd "${CAPTURE_DIR}" && python3 rollback_run_metrics.py ); die "run record aborted"; }

  # export ONLY after runs insert succeeded (gated, README).
  ( cd "${CAPTURE_DIR}" && python3 export_metrics_to_dwh.py ) || warn "DWH export failed (metrics persisted; export can be retried)"

  # integrity verdict LAST (README). Tier 1: a mismatch FAILS the run AFTER the
  # evidence is exported (contract §3). Tier 0 (Null engine) has no delivered
  # rows to count, so the integrity SQL records integrity_unverified there — the
  # check tolerates that (flagged, not failed).
  ( cd "${CAPTURE_DIR}" && python3 check_integrity.py ) || INTEGRITY_RC=$?
  if [ "${INTEGRITY_RC:-0}" != "0" ]; then
    if [ "${tier}" = "1" ]; then
      die "TIER 1 INTEGRITY MISMATCH for ${RUN_ID} — run FAILS (evidence already exported, contract §3)"
    else
      warn "tier 0 integrity not verifiable (Null engine) — expected; not failing"
    fi
  fi
  INTEGRITY_RC=0
}

# Compute the FULL pair cost once and record run_cost_usd on this arm's run_id.
# node-hours x m6i.large price + broker EBS. Uses PAIR_RUN_START -> now.
emit_run_cost() {
  log "  emitting run_cost_usd (pair cost, once) on run_id ${RUN_ID}"
  python3 "${SCRIPT_DIR}/emit_run_cost.py" \
    --run-id "${RUN_ID}" \
    --pair-start "${PAIR_RUN_START:?}" \
    --nodes "${SCALE_UP_NODES}" \
    --node-usd-per-hr "${M6I_LARGE_USD_PER_HR}" \
    --ebs-gb "${BROKER_EBS_GB}" --ebs-usd-per-gb-mo "${EBS_GP3_USD_PER_GB_MO}"
}

phase_arm() {
  local arm="$1" arm_image="$2" first_run="$3"
  log "PHASE 3: arm=${arm} (image=${arm_image}, first_run=${first_run})"
  apply_secret_and_metrics
  deploy_connect "${arm_image}"

  # ---- Tier 0 (hits_null) ----
  export CH_TABLE="hits_null"
  RUN_ID="${PAIR_ID}-${arm}-t0"; export RUN_ID
  RUN_START="$(date -u +%Y-%m-%dT%H:%M:%S)"; export RUN_START
  local g0="ch-sink-${arm}-t0"
  ( cd "${CAPTURE_DIR}" && python3 run_metrics_sql.py "${E2E_DIR}/sql/capture/21_pre_run_covariates.sql" ) \
    || warn "tier0 pre-run covariates failed (continuing)"
  deploy_connector "hits_null" "${g0}"
  wait_tasks_running
  run_poller_sample "${g0}"
  finalize_and_insert_metrics 0
  # Tier 0 has no settle (Null engine): SETTLE_END = RUN_END, settle 0.
  export SETTLE_END="${RUN_END}" SETTLE_SECONDS=0 SETTLE_TIMED_OUT=0
  capture_and_record "${arm}" "0" "0"   # cost only on the tier-1 first-arm row
  delete_connector "${g0}"

  # ---- Tier 1 (hits) ----
  export CH_TABLE="hits"
  RUN_ID="${PAIR_ID}-${arm}-t1"; export RUN_ID
  RUN_START="$(date -u +%Y-%m-%dT%H:%M:%S)"; export RUN_START
  local g1="ch-sink-${arm}-t1"
  ( cd "${CAPTURE_DIR}" && python3 truncate_target.py ) || die "tier1 truncate failed"
  ( cd "${CAPTURE_DIR}" && python3 run_metrics_sql.py "${E2E_DIR}/sql/capture/21_pre_run_covariates.sql" ) \
    || warn "tier1 pre-run covariates failed (continuing)"
  deploy_connector "hits" "${g1}"
  wait_tasks_running
  run_poller_sample "${g1}"
  # settle after lag 0 (plan §5 step 3b).
  local settle_status="${ARTIFACT_DIR}/settle-${RUN_ID}.status"
  SETTLE_END="$( cd "${CAPTURE_DIR}" && SETTLE_STATUS_FILE="${settle_status}" python3 wait_for_settle.py )" \
    || warn "wait_for_settle errored (continuing with RUN_END)"
  export SETTLE_END="${SETTLE_END:-${RUN_END}}"
  export SETTLE_TIMED_OUT="$(cat "${settle_status}" 2>/dev/null || echo 0)"
  export SETTLE_SECONDS=0   # settle_seconds is emitted by capture SQL 14 from the window
  finalize_and_insert_metrics 1
  # first-run arm carries the once-per-pair run_cost_usd (on its tier-1 row).
  local emit_cost=0; [ "${first_run}" = "1" ] && emit_cost=1
  capture_and_record "${arm}" "1" "${emit_cost}"
  delete_connector "${g1}"

  delete_connect
}

phase_teardown_topic() {
  log "PHASE 4: teardown topic (always)"
  kubectl -n "${NS}" delete kafkatopic "${TOPIC}" --ignore-not-found --wait=false 2>/dev/null || true
}

phase_scale_down() {
  log "PHASE 5: scale down (always)"
  "${INFRA_DIR}/scale-down.sh" || warn "scale-down reported non-zero — check for cost leak"
}

# Trap-based cleanup for STANDALONE runs (in CI these are also `if: always()`
# steps in benchmark-nightly.yml — overseer directive b).
cleanup_trap() {
  local rc=$?
  if [ "${CLEANUP_DONE:-0}" != "1" ]; then
    warn "cleanup trap firing (rc=${rc}); running teardown topic + scale down"
    phase_teardown_topic || true
    phase_scale_down || true
  fi
  exit "${rc}"
}

# =========================================================================== #
# main
# =========================================================================== #
main() {
  local plan_only=0
  while [ $# -gt 0 ]; do
    case "$1" in
      --plan|-n) plan_only=1; shift ;;
      -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
      *) die "unknown arg: $1" ;;
    esac
  done

  # Resolve arm order (both modes).
  read -r -a ARM_ORDER <<< "$(resolve_arm_order)"

  if [ "${plan_only}" -eq 1 ]; then
    print_plan "${ARM_ORDER[@]}"
    exit 0
  fi

  # ---- live execution from here ----
  command -v kubectl >/dev/null 2>&1 || die "kubectl required"
  command -v envsubst >/dev/null 2>&1 || die "envsubst required (gettext)"

  # pair_id = RUN_ID from lib_runid.sh (contract §1.2); nogit MUST fail.
  # shellcheck source=/dev/null
  . "${CAPTURE_DIR}/lib_runid.sh" || die "lib_runid.sh refused (nogit?) — aborting"
  export PAIR_ID="${RUN_ID}"
  export PAIR_RUN_START="${RUN_START}"
  case "${PAIR_ID}" in
    *-nogit|nogit) die "PAIR_ID '${PAIR_ID}' does not pin the commit under test (contract §1.2)" ;;
  esac
  log "pair_id=${PAIR_ID} ; arm order: ${ARM_ORDER[0]} then ${ARM_ORDER[1]}"

  # Config-echo env for build_runtime_json (single source of truth = the §6
  # values below; cross-checked against kafkaconnector.json.tmpl by the tests).
  export CFG_MAX_POLL_RECORDS="${CFG_MAX_POLL_RECORDS:-100000}"
  export CFG_MAX_PARTITION_FETCH_BYTES="${CFG_MAX_PARTITION_FETCH_BYTES:-104857600}"
  export CFG_FETCH_MAX_BYTES="${CFG_FETCH_MAX_BYTES:-209715200}"
  export CFG_MAX_POLL_INTERVAL_MS="${CFG_MAX_POLL_INTERVAL_MS:-600000}"
  export CFG_INSERT_TIMEOUT_MS="${CFG_INSERT_TIMEOUT_MS:-180000}"
  export CFG_TASKS_MAX="${CFG_TASKS_MAX:-3}"
  export CFG_PARTITION_SCHEME="${CFG_PARTITION_SCHEME:-toYYYYMM(EventDate)}"

  # source config.env for the capture scope keys / connector identity.
  # shellcheck source=/dev/null
  . "${CAPTURE_DIR}/config.env"

  trap cleanup_trap EXIT INT TERM

  phase_scale_up
  phase_preload

  # Arm 0 is the first-run arm (carries run_cost_usd).
  phase_arm "${ARM_ORDER[0]}" "${ARM0_IMAGE:?ARM0_IMAGE required}" "1"
  phase_arm "${ARM_ORDER[1]}" "${ARM1_IMAGE:?ARM1_IMAGE required}" "0"

  phase_teardown_topic
  phase_scale_down
  CLEANUP_DONE=1
  trap - EXIT INT TERM
  log "pair complete: ${PAIR_ID}"
}

main "$@"
