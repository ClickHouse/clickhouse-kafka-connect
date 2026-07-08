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
# Benchmark v2 — nightly two-arm pair orchestrator (task 31 + review fixes).
#
# Implements the full nightly anatomy (plan §5 steps 1-6) as composable phase
# functions: scale up -> pre-load (topic + producer) -> per-arm (alternating
# order) [Tier 0 drain -> Tier 1 drain, each: deploy Connect+connector, wait
# tasks RUNNING, poller sample -> lag 0, finalize+insert metrics, capture SQL,
# insert_run_record, export, integrity] -> pair cost -> teardown topic -> scale
# down.
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
#                 always-cleanup path (CRs + topic delete + scale down) still
#                 runs on any failure; in CI the topic-delete + scale-down are
#                 ALSO separate `if: always()` steps (overseer directive b) so
#                 cleanup survives even a hard kill of this script.
#
# Arm order (overseer directive g, review F10): computed ONCE. In CI the
#   workflow resolves the UTC day-of-year parity a single time and passes it in
#   via ARM_ORDER_SPEC ("head pinned" | "pinned head") together with the
#   matching ARM0_IMAGE/ARM1_IMAGE — this script then NEVER re-derives it (a
#   second derivation straddling UTC midnight would silently mislabel the arms
#   vs the image slots). Standalone (no ARM_ORDER_SPEC) it falls back to the
#   same parity rule: EVEN day -> head first ; ODD day -> pinned first.
#
# Poller placement (review F4): the sampler runs IN-CLUSTER, inside a dedicated
#   pod (`bench-poller`, producer image + the poller package copied in), because
#   the broker bootstrap, Connect REST, and JMX endpoints are internal-only by
#   design (plan decision 8) and unreachable from a GitHub runner. `finalize`
#   (pure math + perf.metrics insert over the Cloud HTTPS endpoint) runs on the
#   runner against the copied-back JSONL.
#
# run_id construction (contract §1.2): pair_id = RUN_ID from lib_runid.sh;
#   per (arm,tier) run_id = <pair_id>-<arm>-t<tier>. 4 runs rows per night.
#
# run_cost_usd (contract §2.1 amendment, directive d, review F6): FULL pair
#   cost charged ONCE, on the FIRST-run arm's tier-1 row, computed at END of
#   pair so the window covers both arms. See phase_pair_cost for why this one
#   metric sits outside the capture gate.
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
POLLER_POD="${POLLER_POD:-bench-poller}"
# Strimzi node-pool pod naming (<cluster>-<pool>-<ordinal>); see README known gaps.
BROKER_POD="${BROKER_POD:-bench-combined-0}"
EXPECTED_PARTITIONS=3
EXPECTED_TASKS=3
CONNECT_HEAP="${CONNECT_HEAP:-2048m}"
POLL_INTERVAL="${POLL_INTERVAL:-10}"
POLL_TIMEOUT="${POLL_TIMEOUT:-3600}"
ARTIFACT_DIR="${ARTIFACT_DIR:-${SCRIPT_DIR}/artifacts}"

# m6i.large us-east-2 on-demand pricing for the pair-cost calc (phase_pair_cost
# / emit_run_cost.py). Kept here (not an AWS Pricing API call) per plan "no new
# AWS calls"; bump deliberately when prices drift.
M6I_LARGE_USD_PER_HR="${M6I_LARGE_USD_PER_HR:-0.096}"   # us-east-2 on-demand
EBS_GP3_USD_PER_GB_MO="${EBS_GP3_USD_PER_GB_MO:-0.08}"
BROKER_EBS_GB="${BROKER_EBS_GB:-70}"
SCALE_UP_NODES="${SCALE_UP_NODES:-2}"

log()  { printf '\033[1;34m[run_pair]\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[1;33m[run_pair:warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[run_pair:error]\033[0m %s\n' "$*" >&2; exit 1; }

# Rollback-then-die (review F13): for failures that occur AFTER this run_id has
# started landing rows (SQL 21 pre-run covariates onward), roll the run's
# partial perf rows back before dying so a truncate/poller/finalize failure
# never orphans metrics. rollback_run_metrics.py is idempotent and scoped to
# RUN_ID; when RUN_ID has landed nothing yet it is a no-op.
fail_run() {
  warn "failing run ${RUN_ID:-<unset>}: $*"
  if [ -n "${RUN_ID:-}" ]; then
    ( cd "${CAPTURE_DIR}" && python3 rollback_run_metrics.py ) \
      || warn "rollback_run_metrics failed for ${RUN_ID} — perf rows may be orphaned"
  fi
  die "$@"
}

# --------------------------------------------------------------------------- #
# Arm order (overseer directive g, review F10) — single computation.
# CI passes ARM_ORDER_SPEC (resolved once, alongside the image slots); the
# parity fallback below exists for standalone runs only.
# --------------------------------------------------------------------------- #
resolve_arm_order() {
  if [ -n "${ARM_ORDER_SPEC:-}" ]; then
    echo "${ARM_ORDER_SPEC}"
    return 0
  fi
  local doy
  doy="$(date -u +%j)"
  # 10# forces base-10 (leading-zero DOY like 007 would otherwise be octal).
  if [ $((10#$doy % 2)) -eq 0 ]; then
    echo "head pinned"    # even day: head first
  else
    echo "pinned head"    # odd day: pinned first
  fi
}

# --------------------------------------------------------------------------- #
# --plan / -n : print the phase sequence for the resolved order; execute nothing.
# --------------------------------------------------------------------------- #
print_plan() {
  local order=("$@")
  local order_src
  if [ -n "${ARM_ORDER_SPEC:-}" ]; then
    order_src="ARM_ORDER_SPEC (workflow-resolved, single parity computation)"
  else
    local doy; doy="$(date -u +%j)"
    local parity="odd -> pinned first"
    [ $((10#$doy % 2)) -eq 0 ] && parity="even -> head first"
    order_src="UTC day-of-year ${doy} (${parity}) [standalone fallback]"
  fi
  cat <<EOF
=== Benchmark v2 nightly pair — PHASE PLAN (dry-run, nothing executed) ===
arm order source: ${order_src}
arm order       : ${order[0]} then ${order[1]}
pair_id         : <RUN_ID from lib_runid.sh: YYYY-MM-DDTHH-MM-SSZ-<shortsha>>
run rows/night  : 4 -> (${order[0]},t0) (${order[0]},t1) (${order[1]},t0) (${order[1]},t1)
                  all sharing one pair_id (contract §1.2)
run_cost_usd    : FULL pair cost, computed at END of pair, charged ONCE on the
                  FIRST-run arm's (${order[0]}) tier-1 row (contract §2.1)

PHASE 1  scale up          eksctl node group 0->${SCALE_UP_NODES}; Kafka CR + Schema Registry Ready
PHASE 2  pre-load          create topic (${EXPECTED_PARTITIONS} partitions RF1)
                           -> ASSERT broker-reported partition count==${EXPECTED_PARTITIONS} (kafka-topics.sh --describe)
                           -> producer Job -> watch BOTH terminal conditions (Complete|Failed;
                              Failed dies fast + dumps last logs — backoffLimit=0 is terminal)
                           -> capture rows_expected from JSON summary
PHASE 2b poller host       in-cluster poller pod (${POLLER_POD}) — broker/REST/JMX are internal-only
PHASE 3  per arm, in order [ ${order[0]} , ${order[1]} ]:
  for arm in ${order[0]} ${order[1]}:
    apply CH-creds Secret + connect-metrics ConfigMap
    deploy KafkaConnect CR (arm image) -> wait Ready -> read provenance from pod
    --- Tier 0 (hits_null) ---
      pre-run covariates (21) ; NO truncate (Null engine)
      deploy connector (topic2TableMap=hits=hits_null, group ch-sink-<arm>-t0)
      wait connector + ${EXPECTED_TASKS} tasks RUNNING (fast-fail on FAILED; REST via poller pod)
      poller sample (in-cluster) -> lag 0 (RUN_END = lag-0 ts) -> copy samples back
      finalize --insert (run_id <pair_id>-<arm>-t0, tier 0; lands on METRICS service)
      capture SQL [11..19,22,23] gated window ; on any fail -> rollback + abort
      (tier 0: SQL 20 integrity NOT run — integrity is a tier-1 concept, contract §3;
       SQL 23 ch_insert_cpu_share_tier0 parse-watch runs on tier 0 ONLY, contract §2.1)
      insert_run_record (runs row AFTER metrics) ; on fail -> rollback + abort
      export (gated on runs insert)
      delete connector + consumer group
    --- Tier 1 (hits) ---
      pre-run covariates (21, PRE-truncate semantics) ; truncate target
      deploy connector (topic2TableMap=hits=hits, group ch-sink-<arm>-t1)
      wait connector + ${EXPECTED_TASKS} tasks RUNNING
      poller sample (in-cluster) -> lag 0 (RUN_END = lag-0 ts)
      wait_for_settle -> SETTLE_END / settle_timed_out
      finalize --insert (run_id <pair_id>-<arm>-t1, tier 1; lands on METRICS service)
      capture SQL [11..20,22] gated ; on any fail -> rollback + abort
      (tier 1: SQL 23 ch_insert_cpu_share_tier0 NOT run — tier-0-only, contract §2.1)
      SQL 20 integrity: computation failure -> FLAG integrity_unverified (not rollback)
      insert_run_record ; on fail -> rollback + abort
      export (gated) ; integrity check LAST (MISMATCH FAILS run, evidence already exported)
      delete connector + consumer group
    delete KafkaConnect CR + CH-creds Secret
PHASE 3c pair cost         emit run_cost_usd once, end-of-pair window, on (${order[0]},t1)
PHASE 4  teardown topic    delete KafkaTopic CR   (if: always in CI)
PHASE 5  scale down        scale-down.sh -> node group 0   (if: always in CI)
=== END PHASE PLAN ===
EOF
}

# --------------------------------------------------------------------------- #
# runtime-map echo (contract §1 + §1.4 + overseer directive c). Every plan §6
# setting appears here AND in kafkaconnector.json.tmpl (cross-checked by
# tests/test_orchestration.py). Emits a JSON object on stdout.
#
# Review F9: the mandatory identity/scope keys (arm, tier, pair_id,
# target_region, environment_class, compute_region) HARD-FAIL when empty —
# they must never silently vanish from a runs row. Optional provenance keys
# are still dropped when empty (absent, not "").
#
# Args: arm tier
# Env : PAIR_ID, TARGET_REGION, ENVIRONMENT_CLASS, COMPUTE_REGION, CFG_*,
#       KAFKA_CONNECT_VERSION, STRIMZI_VERSION, PLUGIN_SHA256.
# --------------------------------------------------------------------------- #
build_runtime_json() {
  local arm="$1" tier="$2"
  python3 - "$arm" "$tier" <<'PY'
import json, os, sys
arm, tier = sys.argv[1], sys.argv[2]
rt = {
    # --- scope & identity (contract §1.1/§1.2) — MANDATORY, fail-on-empty ---
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
# Review F9: mandatory keys hard-fail when empty (never silently dropped).
MANDATORY = ("arm", "tier", "pair_id", "target_region",
             "environment_class", "compute_region")
missing = [k for k in MANDATORY if not rt.get(k)]
if missing:
    print(f"ERROR: runtime map missing mandatory key(s): {missing} — "
          f"refusing to build a runs-row runtime without them (contract §1.1).",
          file=sys.stderr)
    sys.exit(1)
# drop empty OPTIONAL values so a missing provenance value is absent, not "".
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
  # --timeout bounds the deletion wait (wait-audit: a stuck finalizer would
  # otherwise block forever); on timeout the subsequent apply fails loudly.
  kubectl delete kafkatopic "${TOPIC}" -n "${NS}" --ignore-not-found --wait=true --timeout=120s 2>/dev/null || true
  envsubst < "${TEMPLATES_DIR}/kafkatopic.yaml.tmpl" | kubectl apply -f - || die "topic apply failed"
  kubectl -n "${NS}" wait kafkatopic/"${TOPIC}" --for=condition=Ready --timeout=120s \
    || die "topic did not become Ready"

  # Overseer directive e (review F14): ASSERT the partition count the BROKER
  # actually reports — not the CR spec, which only echoes what we asked for.
  local parts
  parts="$(kubectl -n "${NS}" exec "${BROKER_POD}" -- \
             bin/kafka-topics.sh --bootstrap-server localhost:9092 \
             --describe --topic "${TOPIC}" 2>/dev/null \
           | sed -n 's/.*PartitionCount:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -1)"
  [ "${parts}" = "${EXPECTED_PARTITIONS}" ] \
    || die "broker reports partition count '${parts}' for ${TOPIC}, expected ${EXPECTED_PARTITIONS} (one-task-per-partition invariant broken)"
  log "  topic ${TOPIC} Ready with ${parts} partitions (broker-verified)"

  # Producer Job (fresh; backoffLimit 0). Overseer directive e: parameterize an
  # IRSA serviceAccountName for S3 read (PRODUCER_SA). IAM role is a provisioning
  # TODO (see README). Image ref + parquet source come from the workflow env.
  kubectl -n "${NS}" delete job hits-producer --ignore-not-found --wait=true --timeout=120s 2>/dev/null || true
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

  # Wait for a TERMINAL state — Complete OR Failed (live fix). The previous
  # one-sided `kubectl wait --for=condition=complete` could not see Failed:
  # with backoffLimit=0 the FIRST failure is terminal by design, and waiting
  # out the full PRODUCER_TIMEOUT (6h default) on an already-failed pre-load
  # burns cluster-hours for nothing. Poll both Job conditions and die fast on
  # Failed, dumping the producer pod's last log lines for diagnosis.
  log "  waiting for producer Job (terminal: Complete OR Failed; timeout ${PRODUCER_TIMEOUT:-21600}s)"
  local jdeadline=$(( $(date +%s) + ${PRODUCER_TIMEOUT:-21600} ))
  while :; do
    local jconds
    jconds="$(kubectl -n "${NS}" get job hits-producer \
                -o jsonpath='{range .status.conditions[*]}{.type}={.status} {end}' 2>/dev/null)"
    case " ${jconds}" in
      *" Complete=True"*)
        log "  producer Job Complete"
        break ;;
      *" Failed=True"*)
        warn "producer Job FAILED (backoffLimit=0: first failure is terminal). Last log lines:"
        kubectl -n "${NS}" logs job/hits-producer --tail=40 2>/dev/null | sed 's/^/    | /' >&2 || true
        die "producer Job failed — pre-load invalid, aborting the pair before any measured drain" ;;
    esac
    [ "$(date +%s)" -ge "${jdeadline}" ] \
      && die "producer Job reached neither Complete nor Failed within ${PRODUCER_TIMEOUT:-21600}s"
    sleep 15
  done

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

# --------------------------------------------------------------------------- #
# In-cluster poller host (review F4). The broker bootstrap (9092), Connect REST
# (8083), and the JMX exporter (9404) are internal-only by design (plan
# decision 8) — a GitHub runner cannot reach any of them. The sampler therefore
# runs inside the cluster, in a long-lived pod built from the producer image
# (python + confluent-kafka baked in; `requests` rides in with the
# schema-registry client) with the poller package copied in via kubectl cp.
# The JSONL is copied back after each drain; `finalize` runs on the runner.
# --------------------------------------------------------------------------- #
phase_poller_host() {
  log "PHASE 2b: in-cluster poller host pod (${POLLER_POD})"
  kubectl -n "${NS}" delete pod "${POLLER_POD}" --ignore-not-found --wait=true --timeout=120s 2>/dev/null || true
  kubectl -n "${NS}" run "${POLLER_POD}" --image="${PRODUCER_IMAGE:?PRODUCER_IMAGE required}" \
    --restart=Never --command \
    --labels="app.kubernetes.io/part-of=kafka-connect-benchmark-v2" \
    -- sleep infinity || die "poller pod create failed"
  kubectl -n "${NS}" wait --for=condition=Ready pod/"${POLLER_POD}" --timeout=300s \
    || die "poller pod not Ready"
  kubectl -n "${NS}" exec "${POLLER_POD}" -- mkdir -p /opt/poller || die "poller pod mkdir failed"
  local f
  for f in poller.py sampler.py finalizer.py ch_insert.py metric_names.py requirements.txt; do
    kubectl cp "${POLLER_DIR}/${f}" "${NS}/${POLLER_POD}:/opt/poller/${f}" \
      || die "kubectl cp ${f} into poller pod failed"
  done
  # Deps: confluent-kafka is baked into the producer image; requests normally
  # rides in with it. Verify, and best-effort pip install only if missing
  # (needs PyPI egress — nodes already pull public images).
  kubectl -n "${NS}" exec "${POLLER_POD}" -- python -c "import confluent_kafka, requests" 2>/dev/null \
    || kubectl -n "${NS}" exec "${POLLER_POD}" -- pip install --no-cache-dir -r /opt/poller/requirements.txt \
    || die "poller pod lacks confluent-kafka/requests and pip install failed"
  log "  poller host ready"
}

teardown_poller_pod() {
  kubectl -n "${NS}" delete pod "${POLLER_POD}" --ignore-not-found --wait=false 2>/dev/null || true
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

# The Connect worker pod (review F3): Strimzi 0.46 manages Connect via
# StrimziPodSets, NOT Deployments — `kubectl exec deploy/...` matches nothing.
# Target the pod through the Strimzi labels instead.
connect_pod() {
  kubectl -n "${NS}" get pod \
    -l "strimzi.io/cluster=${CONNECT_NAME},strimzi.io/kind=KafkaConnect" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null
}

deploy_connect() {
  local arm_image="$1"
  log "  deploying KafkaConnect (image=${arm_image})"
  ARM_IMAGE="${arm_image}" CONNECT_HEAP="${CONNECT_HEAP}" \
    envsubst '${ARM_IMAGE} ${CONNECT_HEAP}' < "${TEMPLATES_DIR}/kafkaconnect.yaml.tmpl" \
    | kubectl apply -f - || die "KafkaConnect apply failed"
  kubectl -n "${NS}" wait kafkaconnect/"${CONNECT_NAME}" --for=condition=Ready --timeout=600s \
    || die "KafkaConnect did not become Ready"

  # Pod-targeted access (review F3): resolve the worker pod once per arm.
  CONNECT_POD="$(connect_pod)"
  [ -n "${CONNECT_POD}" ] || die "could not resolve the Connect worker pod (StrimziPodSet labels)"
  CONNECT_POD_IP="$(kubectl -n "${NS}" get pod "${CONNECT_POD}" -o jsonpath='{.status.podIP}' 2>/dev/null)"
  export CONNECT_POD CONNECT_POD_IP
  # JMX exporter lives on the worker POD (9404), no dedicated Service — point
  # the in-cluster sampler at the pod IP (poller README prerequisite 1).
  export JMX_METRICS_URL="${JMX_METRICS_URL_OVERRIDE:-http://${CONNECT_POD_IP}:9404/metrics}"

  # Read this arm's provenance from the baked-in /opt/benchmark/provenance.json
  # (Dockerfile) so GIT_SHA / CONNECTOR_VERSION / KAFKA_CONNECT_VERSION /
  # STRIMZI_VERSION / PLUGIN_SHA256 come from the IMAGE UNDER TEST, per arm
  # (overseer directive c) — not from the checkout (which is HEAD for both arms).
  local prov
  prov="$(kubectl -n "${NS}" exec "${CONNECT_POD}" -- \
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
  kubectl -n "${NS}" delete kafkaconnect "${CONNECT_NAME}" --ignore-not-found --wait=true --timeout=300s 2>/dev/null || true
  kubectl -n "${NS}" delete secret bench-ch-creds --ignore-not-found 2>/dev/null || true
}

deploy_connector() {
  local ch_table="$1" group="$2"
  log "  deploying connector (table=${ch_table}, group=${group})"
  python3 "${SCRIPT_DIR}/render_connector.py" \
      --template "${TEMPLATES_DIR}/kafkaconnector.json.tmpl" \
      --ch-table "${ch_table}" --group "${group}" --out - \
    | kubectl apply -f - || fail_run "connector apply failed"
}

delete_connector() {
  local group="$1"
  kubectl -n "${NS}" delete kafkaconnector "${CONNECTOR_NAME}" --ignore-not-found --wait=true --timeout=120s 2>/dev/null || true
  # Delete the consumer group so the next drain starts fresh at offset 0
  # (plan §5 step 3c). Best-effort via the broker pod.
  "${SCRIPT_DIR}/delete_consumer_group.sh" "${group}" 2>/dev/null || warn "consumer-group delete best-effort failed for ${group}"
}

# Poll Connect REST /status THROUGH the in-cluster poller pod (review F3+F4:
# the REST service is internal-only, and the Strimzi image is not guaranteed
# to ship curl — the poller pod has python+requests for exactly this).
wait_tasks_running() {
  # Poller prerequisite 5: start the sampler only AFTER connector + all tasks
  # report RUNNING.
  local deadline=$(( $(date +%s) + ${TASKS_RUNNING_TIMEOUT:-600} ))
  log "  waiting for connector + ${EXPECTED_TASKS} tasks RUNNING (fast-fails on FAILED)"
  while :; do
    local out conn_state running failed
    out="$(kubectl -n "${NS}" exec "${POLLER_POD}" -- python -c "
import requests, sys
try:
    d = requests.get('${CONNECT_REST}/connectors/${CONNECTOR_NAME}/status', timeout=10).json()
    tasks = d.get('tasks', [])
    print(d['connector']['state'],
          sum(1 for t in tasks if t['state'] == 'RUNNING'),
          sum(1 for t in tasks if t['state'] == 'FAILED'))
except Exception:
    print('UNAVAILABLE 0 0')
" 2>/dev/null || echo 'UNAVAILABLE 0 0')"
    read -r conn_state running failed <<< "${out}"
    # Wait-audit (live-fix class): FAILED is TERMINAL here — Connect does not
    # auto-restart FAILED tasks, and errors.tolerance=none fails the task on
    # the first bad record — so waiting out the timeout would only burn
    # cluster-time. Die fast with the status dump.
    if [ "${conn_state}" = "FAILED" ] || [ "${failed:-0}" -gt 0 ] 2>/dev/null; then
      warn "connector/task FAILED — /status dump:"
      kubectl -n "${NS}" exec "${POLLER_POD}" -- python -c "
import requests
print(requests.get('${CONNECT_REST}/connectors/${CONNECTOR_NAME}/status', timeout=10).text)
" 2>/dev/null | sed 's/^/    | /' >&2 || true
      fail_run "connector state=${conn_state}, failed_tasks=${failed:-?} (terminal — not waiting for the ${TASKS_RUNNING_TIMEOUT:-600}s timeout)"
    fi
    if [ "${conn_state}" = "RUNNING" ] && [ "${running}" -ge "${EXPECTED_TASKS}" ] 2>/dev/null; then
      log "  connector RUNNING with ${running}/${EXPECTED_TASKS} tasks RUNNING"
      return 0
    fi
    [ "$(date +%s)" -ge "${deadline}" ] && fail_run "connector/tasks did not all reach RUNNING in time (last: ${out})"
    sleep 5
  done
}

# Run the poller sample loop IN-CLUSTER for a (arm,tier) (review F4); copies
# the JSONL back to ${ARTIFACT_DIR} afterwards. Returns via $SAMPLES_FILE.
run_poller_sample() {
  local group="$1"
  local pod_out="/tmp/samples-${RUN_ID}.jsonl"
  SAMPLES_FILE="${ARTIFACT_DIR}/samples-${RUN_ID}.jsonl"
  log "  poller sample in-cluster (group=${group}) -> ${SAMPLES_FILE}"
  kubectl -n "${NS}" exec "${POLLER_POD}" -- python /opt/poller/poller.py sample \
      --group "${group}" --topic "${TOPIC}" --connector "${CONNECTOR_NAME}" \
      --out "${pod_out}" \
      --bootstrap "${KAFKA_BOOTSTRAP:-bench-kafka-bootstrap.${NS}.svc:9092}" \
      --connect-url "${CONNECT_REST}" \
      --jmx-url "${JMX_METRICS_URL:-}" \
      --cadvisor-url "${POD_CADVISOR_URL:-}" \
      --pod-name "${CONNECT_POD:-}" --pod-container "${CONNECT_POD_CONTAINER:-}" \
      --poll-interval "${POLL_INTERVAL}" --timeout "${POLL_TIMEOUT}" \
    && POLLER_RC=0 || POLLER_RC=$?
  # RUN_END anchors the capture window at lag-0 (or at give-up on timeout).
  RUN_END="$(date -u +%Y-%m-%dT%H:%M:%S)"
  export RUN_END
  # Copy the samples back REGARDLESS of rc — the JSONL is flushed per tick, so
  # even a partial file is archived + finalizable.
  kubectl cp "${NS}/${POLLER_POD}:${pod_out}" "${SAMPLES_FILE}" 2>/dev/null \
    || warn "could not copy samples back from the poller pod"
  # exit 0 drained, 2 timeout. A timeout means lag never hit 0 -> the finalizer
  # records lag_reached_zero=0 -> flag_reason drain_incomplete (contract §1.3).
  if [ "${POLLER_RC}" = "2" ]; then
    warn "  poller TIMED OUT (drain incomplete) — continuing to capture (run will be FLAGGED)"
  elif [ "${POLLER_RC}" != "0" ]; then
    fail_run "poller sample failed (rc=${POLLER_RC})"
  fi
  [ -s "${SAMPLES_FILE}" ] || fail_run "no samples were retrieved for ${RUN_ID}"
}

# finalize + insert the poller scalars into perf.metrics for a (arm,tier); also
# emit the guards JSON so insert_run_record can set flagged/flag_reason
# (overseer directive h). Sets FLAGGED / FLAG_REASON in the env.
#
# Review F5 (split-brain landing): the poller's inserter reads TARGET_CH_* by
# its own contract, but the metrics LANDING is the METRICS service (the same
# service capture/rollback/export use). Remap the env for this one invocation —
# the poller itself is not modified.
finalize_and_insert_metrics() {
  local tier="$1"
  local out="${ARTIFACT_DIR}/finalize-${RUN_ID}.json"
  log "  finalize + insert metrics (tier ${tier}, run_id ${RUN_ID})"
  log "  poller metrics landing -> METRICS service (env METRICS_CH_HOST; same landing as capture/rollback/export)"
  ( cd "${POLLER_DIR}" && \
    TARGET_CH_HOST="${METRICS_CH_HOST:?METRICS_CH_HOST required}" \
    TARGET_CH_USER="${METRICS_CH_USER:?METRICS_CH_USER required}" \
    TARGET_CH_PASSWORD="${METRICS_CH_PASSWORD:?METRICS_CH_PASSWORD required}" \
    python3 poller.py finalize \
      --samples "${SAMPLES_FILE}" --run-id "${RUN_ID}" --tier "${tier}" \
      --rows-expected "${ROWS_EXPECTED}" --expected-tasks "${EXPECTED_TASKS}" \
      --insert ) > "${out}" || fail_run "poller finalize/insert failed"
  # Wire the poller guards JSON into flagged/flag_reason (overseer directive h).
  FLAGGED="$(python3 -c 'import sys,json;print(json.load(open(sys.argv[1]))["guards"]["flagged"])' "${out}")"
  FLAG_REASON="$(python3 -c 'import sys,json;print(json.load(open(sys.argv[1]))["guards"]["flag_reason"])' "${out}")"
  export FLAGGED FLAG_REASON
  log "  guards: flagged=${FLAGGED} flag_reason='${FLAG_REASON}'"
}

# Append a contract §1.3 token to FLAG_REASON (single-pipe-joined) + set FLAGGED.
append_flag() {
  local token="$1"
  FLAGGED=1
  FLAG_REASON="${FLAG_REASON:+${FLAG_REASON}|}${token}"
  export FLAGGED FLAG_REASON
}

# Gated CH-side capture + run-record + export + integrity for a (arm,tier).
# Mirrors capture/README "Order of operations" (BINDING gating discipline).
#
# Review F7 — integrity handling per contract §3/§1.3:
#   * Tier 0: SQL 20 is NOT run and no integrity check happens — integrity is a
#     tier-1 concept (Null engine delivers no countable rows); "not applicable"
#     is neither a flag nor a failure.
#   * Tier 1, SQL 20 COMPUTATION failure (SQL error / missing constants): the
#     run is FLAGGED integrity_unverified (contract §1.3) — not rolled back,
#     not failed. The rest of the metrics are legitimate.
#   * Tier 1, integrity MISMATCH: the run FAILS — but only AFTER the runs row
#     is inserted and the evidence exported (contract §3: flagged/failed runs
#     are still fully captured).
capture_and_record() {
  local arm="$1" tier="$2"
  log "  gated capture + run record (arm=${arm}, tier=${tier})"

  local integrity_unverified=0
  local f
  for f in 11 12 13 14 15 16 17 18 19 20 22 23; do
    if [ "${f}" = "20" ] && [ "${tier}" = "0" ]; then
      continue   # tier 0: integrity not applicable (review F7)
    fi
    if [ "${f}" = "23" ] && [ "${tier}" != "0" ]; then
      continue   # ch_insert_cpu_share_tier0: tier-0-only parse-watch (contract §2.1)
    fi
    local sqlfile
    sqlfile="$(ls "${E2E_DIR}"/sql/capture/${f}_*.sql 2>/dev/null | head -1)"
    [ -n "${sqlfile}" ] || fail_run "capture SQL ${f}_* not found"
    if ! ( cd "${CAPTURE_DIR}" && python3 run_metrics_sql.py "${sqlfile}" ); then
      if [ "${f}" = "20" ]; then
        # Integrity COMPUTATION failed (tier 1): flag, don't roll back (F7).
        warn "capture 20 (integrity) computation failed -> FLAG integrity_unverified"
        append_flag "integrity_unverified"
        integrity_unverified=1
      else
        warn "capture ${f} failed -> rollback"
        ( cd "${CAPTURE_DIR}" && python3 rollback_run_metrics.py )
        die "capture aborted for ${RUN_ID}"
      fi
    fi
  done

  # runs row AFTER metrics (gated); failure -> rollback + abort (README).
  # NOTE (review F6): run_cost_usd is NOT emitted here — it is a whole-pair
  # metric emitted once at end-of-pair (phase_pair_cost).
  local runtime_json
  runtime_json="$(build_runtime_json "${arm}" "${tier}")" \
    || { warn "runtime map build failed -> rollback"; ( cd "${CAPTURE_DIR}" && python3 rollback_run_metrics.py ); die "runtime map aborted for ${RUN_ID}"; }
  # Fold the guards into the runtime map (overseer directive h): flagged and
  # flag_reason are runtime keys (contract §1.3).
  if [ "${FLAGGED:-0}" = "1" ]; then
    runtime_json="$(python3 -c 'import json,sys;d=json.loads(sys.argv[1]);d["flagged"]="1";d["flag_reason"]=sys.argv[2];print(json.dumps(d,sort_keys=True))' "${runtime_json}" "${FLAG_REASON}")"
  fi
  ( cd "${CAPTURE_DIR}" && RUNTIME="${runtime_json}" python3 insert_run_record.py ) \
    || { warn "insert_run_record failed -> rollback"; ( cd "${CAPTURE_DIR}" && python3 rollback_run_metrics.py ); die "run record aborted for ${RUN_ID}"; }

  # export ONLY after runs insert succeeded (gated, README).
  ( cd "${CAPTURE_DIR}" && python3 export_metrics_to_dwh.py ) || warn "DWH export failed (metrics persisted; export can be retried)"

  # integrity verdict LAST (README, contract §3) — tier 1 only, and only when
  # the integrity metrics were actually computed.
  if [ "${tier}" = "1" ]; then
    if [ "${integrity_unverified}" = "1" ]; then
      warn "tier 1 integrity UNVERIFIED for ${RUN_ID} — run FLAGGED (integrity_unverified), not failed (contract §1.3)"
    else
      ( cd "${CAPTURE_DIR}" && python3 check_integrity.py ) \
        || die "TIER 1 INTEGRITY MISMATCH for ${RUN_ID} — run FAILS (evidence already exported, contract §3)"
    fi
  fi
}

# --------------------------------------------------------------------------- #
# Pair cost (review F6 + contract §2.1 amendment): the shared infra is
# provisioned once per pair, so the FULL pair cost is charged ONCE, on the
# FIRST-run arm's tier-1 row — and it can only be computed at END of pair
# (charging it mid-pair would halve the node-hours). This single late INSERT
# deliberately sits OUTSIDE the capture gate: the target runs row already
# exists (its capture+insert+export completed), the metric is additive and
# idempotent (emit_run_cost.py pre-deletes any prior run_cost_usd row for the
# run_id), and rolling back a completed run over a failed cost write would
# destroy good evidence — so a failure here is a WARN, not a rollback.
# --------------------------------------------------------------------------- #
phase_pair_cost() {
  local first_arm="$1"
  local cost_run_id="${PAIR_ID}-${first_arm}-t1"
  log "PHASE 3c: pair cost -> run_cost_usd on ${cost_run_id} (end-of-pair window)"
  python3 "${SCRIPT_DIR}/emit_run_cost.py" \
    --run-id "${cost_run_id}" \
    --pair-start "${PAIR_RUN_START:?}" \
    --nodes "${SCALE_UP_NODES}" \
    --node-usd-per-hr "${M6I_LARGE_USD_PER_HR}" \
    --ebs-gb "${BROKER_EBS_GB}" --ebs-usd-per-gb-mo "${EBS_GP3_USD_PER_GB_MO}" \
    || warn "run_cost_usd emit failed (retryable; run rows remain valid)"
}

phase_arm() {
  local arm="$1" arm_image="$2"
  log "PHASE 3: arm=${arm} (image=${arm_image})"
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
  capture_and_record "${arm}" "0"
  delete_connector "${g0}"

  # ---- Tier 1 (hits) ----
  export CH_TABLE="hits"
  RUN_ID="${PAIR_ID}-${arm}-t1"; export RUN_ID
  RUN_START="$(date -u +%Y-%m-%dT%H:%M:%S)"; export RUN_START
  local g1="ch-sink-${arm}-t1"
  # Review F8: pre-run covariates BEFORE truncate (capture/README binding
  # order) — pre_run_active_parts / pre_run_rss have PRE-truncate semantics
  # (the contract's cleanliness verifier reads the state the run INHERITED).
  ( cd "${CAPTURE_DIR}" && python3 run_metrics_sql.py "${E2E_DIR}/sql/capture/21_pre_run_covariates.sql" ) \
    || warn "tier1 pre-run covariates failed (continuing)"
  ( cd "${CAPTURE_DIR}" && python3 truncate_target.py ) || fail_run "tier1 truncate failed"
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
  capture_and_record "${arm}" "1"
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

# Trap-based cleanup for STANDALONE runs (in CI topic-delete + scale-down are
# also `if: always()` steps in benchmark-nightly.yml — overseer directive b).
# Review F11: also deletes the KafkaConnector/KafkaConnect CRs, the creds
# Secret, the poller pod, and (best-effort) all four per-(arm,tier) consumer
# groups — a mid-arm crash must not leave a live connector that revives next
# night and pre-consumes the fresh topic.
cleanup_trap() {
  local rc=$?
  if [ "${CLEANUP_DONE:-0}" != "1" ]; then
    warn "cleanup trap firing (rc=${rc}); tearing down CRs, groups, topic, nodes"
    kubectl -n "${NS}" delete kafkaconnector "${CONNECTOR_NAME}" --ignore-not-found --wait=false 2>/dev/null || true
    kubectl -n "${NS}" delete kafkaconnect "${CONNECT_NAME}" --ignore-not-found --wait=true --timeout=300s 2>/dev/null || true
    kubectl -n "${NS}" delete secret bench-ch-creds --ignore-not-found 2>/dev/null || true
    local arm tier
    for arm in head pinned; do
      for tier in 0 1; do
        "${SCRIPT_DIR}/delete_consumer_group.sh" "ch-sink-${arm}-t${tier}" 2>/dev/null || true
      done
    done
    teardown_poller_pod
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

  # Resolve arm order (both modes) — single source (review F10): CI supplies
  # ARM_ORDER_SPEC; the parity fallback is standalone-only. Validate the shape
  # here (resolve_arm_order runs in a subshell, so it cannot die for us).
  read -r -a ARM_ORDER <<< "$(resolve_arm_order)"
  case "${ARM_ORDER[0]:-}-${ARM_ORDER[1]:-}" in
    head-pinned|pinned-head) ;;
    *) die "invalid arm order '${ARM_ORDER[*]:-}' (ARM_ORDER_SPEC must be 'head pinned' or 'pinned head')" ;;
  esac

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
  # values below; cross-checked against kafkaconnector.json.tmpl by the tests
  # in tests/test_orchestration.py).
  export CFG_MAX_POLL_RECORDS="${CFG_MAX_POLL_RECORDS:-100000}"
  export CFG_MAX_PARTITION_FETCH_BYTES="${CFG_MAX_PARTITION_FETCH_BYTES:-104857600}"
  export CFG_FETCH_MAX_BYTES="${CFG_FETCH_MAX_BYTES:-209715200}"
  export CFG_MAX_POLL_INTERVAL_MS="${CFG_MAX_POLL_INTERVAL_MS:-600000}"
  export CFG_INSERT_TIMEOUT_MS="${CFG_INSERT_TIMEOUT_MS:-180000}"
  export CFG_TASKS_MAX="${CFG_TASKS_MAX:-3}"
  # Matches the Tier 1 DDL (sql/clickbench/02_create_hits.sql PARTITION BY).
  export CFG_PARTITION_SCHEME="${CFG_PARTITION_SCHEME:-toYear(EventDate)}"
  # Review F9: compute_region is a mandatory runtime key — default it from repo
  # config here (the EKS region, infra/env.sh AWS_REGION default) so it can
  # never silently vanish; build_runtime_json still hard-fails if empty.
  export COMPUTE_REGION="${COMPUTE_REGION:-us-east-2}"

  # source config.env for the capture scope keys / connector identity.
  # shellcheck source=/dev/null
  . "${CAPTURE_DIR}/config.env"

  trap cleanup_trap EXIT INT TERM

  phase_scale_up
  phase_preload
  phase_poller_host

  phase_arm "${ARM_ORDER[0]}" "${ARM0_IMAGE:?ARM0_IMAGE required}"
  phase_arm "${ARM_ORDER[1]}" "${ARM1_IMAGE:?ARM1_IMAGE required}"

  # Pair cost last (review F6): the window must cover both arms.
  phase_pair_cost "${ARM_ORDER[0]}"

  teardown_poller_pod
  phase_teardown_topic
  phase_scale_down
  CLEANUP_DONE=1
  trap - EXIT INT TERM
  log "pair complete: ${PAIR_ID}"
}

main "$@"
