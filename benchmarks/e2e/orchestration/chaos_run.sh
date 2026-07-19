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
# chaos_run.sh — chaos / monkey test orchestrator (#771, task T11, IC-3).
#
# The sibling of run_pair.sh: it reuses lib_bench.sh (scale/preload/poller/
# Connect deploy family, digest-pin law, broker offset helpers) and adds the
# chaos-specific machinery from chaos/lib_ch_cluster.sh (self-hosted CH cluster
# lifecycle + keeper quorum) and chaos/lib_faults.sh (fault injectors C1..C5 +
# quorum-loss). It drives the spec §3.3/§3.5/§3.6 phase sequence:
#
#   isolation guard -> image validation -> config validation ->
#   scale-up (5+1) -> CH cluster deploy + DDL + keeper_map smoke ->
#   keeper_map_reset (eo=1) -> topic + DLQ -> preload (smoke: one-shot;
#   monkey: base preload + --stream producer) -> poller host ->
#   Connect + chaos connector ->
#     smoke : drain-progress 50% -> single fault -> recovery gate -> fence
#     monkey: schedule.py -> per-round inject (window racer for crash-class) ->
#             recovery gate -> record round
#   -> fence (SIGTERM the stream producer; rows_expected := broker offsets) ->
#   quiescence -> oracle (check_integrity.py --direct) -> write_artifact.py ->
#   teardown (verify CH StatefulSet gone AND nodegroups at 0, §9.8).
#
# Decision §10.2: the chaos path writes a per-run JSON artifact (§11 / IC-9). It
# NEVER inserts into the metrics-CH tables and never exports to a DWH — that
# capture/export family is pair-only and stays in run_pair.sh.
#
# STATUS: CODE-COMPLETE / VALIDATION-PENDING. Phase-1 is validated OFFLINE only
# (bash -n, shellcheck, --plan dry-run, pytest). No live EKS/CH deploy is claimed
# here; the live smoke gate + monkey runs are the downstream, principal-gated
# phase.
#
# Two run modes:
#   --plan / -n   Print the resolved phase plan and exit 0. Executes NOTHING.
#   (default)     Execute the full chaos run. The artifact is written on EVERY
#                 path (including every gate/oracle timeout) BEFORE exit, and a
#                 cleanup trap tears the cluster down on any failure.
#
# Exit codes (IC-3, consumed by the T12 workflow):
#   0  PASS          integrity_ok, quiesced
#   1  FAIL          MISMATCH, or a stuck-connector t_recover_timeout
#   3  UNVERIFIED    CHECK_ERROR / fault-not-observed / infra-stall /
#                    t_settle_timeout
# =============================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"

# --------------------------------------------------------------------------- #
# Chaos identities (IC-3 / spec §4). Set BEFORE sourcing lib_bench.sh so its
# overridable `${VAR:-default}` constants pick up the chaos targets instead of
# the pair defaults. The pair keeps hits/bench-clickhouse-sink/run_pair; chaos
# retargets topic/connector/log-prefix/scale here and nowhere else.
# --------------------------------------------------------------------------- #
export BENCH_LOG_PREFIX="${BENCH_LOG_PREFIX:-chaos_run}"
export SCALE_UP_NODES="${SCALE_UP_NODES:-5}"          # decision §10.1 (bench-ng max 5)
export TOPIC="${TOPIC:-hits-chaos}"                   # §4 distinct topic
export CONNECTOR_NAME="${CONNECTOR_NAME:-chaos-clickhouse-sink}"  # chaos KafkaConnector CR
export EXPECTED_PARTITIONS="${EXPECTED_PARTITIONS:-3}"
export EXPECTED_TASKS="${EXPECTED_TASKS:-3}"
# KafkaConnect CR is REUSED (bench-connect, same worker template) — CONNECT_NAME
# keeps its lib_bench default, so CONNECT_REST resolves to the shared worker.
# Affirmed here (before the source) so it is self-documenting and overridable.
export CONNECT_NAME="${CONNECT_NAME:-bench-connect}"

# Shared, run-mode-agnostic helpers + overridable constants (spec §8, IC-1):
# log/warn/die, the digest-pin family (validate_image_ref, ...), phase_scale_up/
# phase_scale_down, phase_preload, phase_poller_host, the Connect deploy family,
# broker_topic_row_count, build_runtime_json. Sourced FIRST; side-effect-free.
# shellcheck source=lib_bench.sh disable=SC1091
. "${SCRIPT_DIR}/lib_bench.sh"

CHAOS_DIR="$(cd "${SCRIPT_DIR}/../chaos" && pwd)"
# CH-cluster lifecycle (T8): deploy/teardown_ch_cluster, resolve_keeper_leader,
# keeper_map_reset/smoke, apply_chaos_ddl, await_replica_sync, read_ch_image_digest.
# shellcheck source=../chaos/lib_ch_cluster.sh disable=SC1091
. "${CHAOS_DIR}/lib_ch_cluster.sh"
# Fault injectors (T9): inject_connect_pod_kill/task_restart/broker_pod_kill/
# ch_node_kill/ch_keeper_leader_kill/quorum_loss (+ FAULT_REFUSED, QUORUM_LOSS gate).
# shellcheck source=../chaos/lib_faults.sh disable=SC1091
. "${CHAOS_DIR}/lib_faults.sh"

# --------------------------------------------------------------------------- #
# Chaos-only constants (all overridable for tests; §4 / IC-3 identities).
# --------------------------------------------------------------------------- #
CHAOS_TEMPLATES_DIR="${CHAOS_TEMPLATES_DIR:-${CHAOS_DIR}/templates}"
CHAOS_CONNECTOR_TMPL="${CHAOS_CONNECTOR_TMPL:-${CHAOS_TEMPLATES_DIR}/chaos-connector.json.tmpl}"
DLQ_TOPIC="${DLQ_TOPIC:-hits-chaos-dlq}"              # §3.6b: DLQ depth must be 0
# The pair's sink connector — its presence means a benchmark pair is ACTIVE.
# Chaos uses a DISTINCT connector (chaos-clickhouse-sink), so this is the clean
# "pair is running" signal for the §4-rule-1 isolation guard.
PAIR_CONNECTOR_NAME="${PAIR_CONNECTOR_NAME:-bench-clickhouse-sink}"

# Artifact / result vocabulary (IC-9 / spec §4, top-level columns).
CONNECTOR_LABEL="${CONNECTOR_LABEL:-kafka-connect-chaos}"
ENVIRONMENT_CLASS_CHAOS="${ENVIRONMENT_CLASS_CHAOS:-self_hosted}"
# zkDatabase (exactly-once KeeperMap state db) is defined in lib_ch_cluster.sh;
# affirm it here so it is honored + self-documenting in the plan/teardown.
ZK_DATABASE="${ZK_DATABASE:-connect_state_chaos}"

# --------------------------------------------------------------------------- #
# Self-hosted CH creds + wire (IC-2 / T7). The chaos cluster (chaos/ch-cluster.yaml
# users.xml, derived from the docker fixture) serves user `default` with an EMPTY
# password over plaintext 8123, ssl=false (pinned in the T7 chaos connector
# template). TARGET_CH_HOST/PORT come from lib_ch_cluster.sh; USER/PASSWORD/SECURE
# are set HERE so they reach every consumer of the chaos target:
#   * apply_secret_and_metrics (lib_bench) builds the bench-ch-creds Secret the
#     Connect worker resolves ${env:CH_USERNAME}/${env:CH_PASSWORD} from;
#   * the oracle (check_integrity.py --direct) reads the SAME target directly.
# The empty password is LEGITIMATE, so it is exported SET-but-empty (`-`, not
# `:-`) and lib_bench requires it SET-but-allows-empty (${TARGET_CH_PASSWORD?}).
# SECURE=false + PORT=8123 keep the oracle off the pair's Cloud 8443/TLS default.
# HOST/PORT default to the in-cluster Service already resolved by lib_ch_cluster.sh
# (value-preserving :- defaults; deploy_ch_cluster re-exports them post-deploy).
export TARGET_CH_HOST="${TARGET_CH_HOST:-${CH_SVC}.${NS}.svc}"
export TARGET_CH_PORT="${TARGET_CH_PORT:-8123}"
export TARGET_CH_USER="${TARGET_CH_USER:-default}"
export TARGET_CH_PASSWORD="${TARGET_CH_PASSWORD-}"
export TARGET_CH_SECURE="${TARGET_CH_SECURE:-false}"

# Gate timeouts / rates (IC-3 env; passed through to chaos/gates.py + producer).
T_RECOVER="${T_RECOVER:-600}"
T_SETTLE="${T_SETTLE:-900}"
Q_SECONDS="${Q_SECONDS:-30}"
W_SECONDS="${W_SECONDS:-60}"
STREAM_RATE="${STREAM_RATE:-5000}"
CHAOS_CH_VERSION="${CHAOS_CH_VERSION:-latest}"        # IC-8 (version-selected CH)
STREAM_PRODUCER_POD="${STREAM_PRODUCER_POD:-hits-chaos-producer}"
DRAIN_TARGET_PCT="${DRAIN_TARGET_PCT:-50}"            # §3.5 smoke-gate trigger
# F-L2b: the one-shot base-preload wait bound. The pair's PRODUCER_TIMEOUT
# default is 6h (21600s) — right for a 10M-row pair preload, catastrophic for a
# failed smoke/base preload that should fail fast. Scope it to 900s here and
# export it as PRODUCER_TIMEOUT before the one-shot wait (phase_chaos_preload).
CHAOS_PRODUCER_TIMEOUT="${CHAOS_PRODUCER_TIMEOUT:-900}"

# CLI-resolved run parameters (defaults per IC-3).
MODE=""
SEED=""
ROUNDS=20
EXACTLY_ONCE=""
T_MIN=30
T_MAX=180
FAULTS="C1,C2,C3,C4,C5"
QUORUM_LOSS=0
AGGRESSIVE=0
WATCH_CELL=0
OUT_DIR=""
PLAN_ONLY=0
REAP=0

# Run-state (set as the run proceeds; written into the artifact on every path).
CHAOS_ID=""
CHAOS_MODE=""
DELIVERY_MODE=""
GROUP=""
IPWB="false"
IPWB_FIELD="0"
ROUNDS_COMPLETED=0
CH_VERSION=""
ROUNDS_FILE=""
ORACLE_FILE=""
CHAOS_CONNECTOR_RENDERED=""
ARTIFACT_WRITTEN=0
CLEANUP_DONE=0

# =========================================================================== #
# ARG PARSING + FLAG VALIDATION (IC-3).
# =========================================================================== #
parse_args() {
  while [ $# -gt 0 ]; do
    case "$1" in
      --mode)          MODE="${2:?--mode needs a value}"; shift 2 ;;
      --seed)          SEED="${2:?--seed needs a value}"; shift 2 ;;
      --rounds)        ROUNDS="${2:?--rounds needs a value}"; shift 2 ;;
      --exactly-once)  EXACTLY_ONCE="${2:?--exactly-once needs a value}"; shift 2 ;;
      --t-min)         T_MIN="${2:?--t-min needs a value}"; shift 2 ;;
      --t-max)         T_MAX="${2:?--t-max needs a value}"; shift 2 ;;
      --faults)        FAULTS="${2:?--faults needs a value}"; shift 2 ;;
      --quorum-loss)   QUORUM_LOSS=1; shift ;;
      --aggressive)    AGGRESSIVE=1; shift ;;
      --watch-cell)    WATCH_CELL=1; shift ;;
      --out)           OUT_DIR="${2:?--out needs a value}"; shift 2 ;;
      --plan|-n)       PLAN_ONLY=1; shift ;;
      # --reap (T15): state-free, idempotent teardown-only mode. Needs no
      # --mode/--exactly-once/--seed; if combined with a run, reap wins (main
      # branches on REAP before validate_flags). The kill-safety net for the L2
      # incident (SIGKILL/CI-timeout leaks the cluster; nothing can trap SIGKILL).
      --reap)          REAP=1; shift ;;
      # --allow-tag: local-hacking escape hatch (== KAFKA_ALLOW_TAG=1), as
      # run_pair.sh. Default is STRICT (digest-pinned); never use it for a real run.
      --allow-tag)     export KAFKA_ALLOW_TAG=1; shift ;;
      -h|--help)       grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
      *)               die "unknown arg: $1" ;;
    esac
  done
}

validate_flags() {
  case "${MODE}" in
    smoke|monkey) ;;
    *) die "--mode must be 'smoke' or 'monkey' (got '${MODE:-<unset>}')" ;;
  esac

  # --quorum-loss forces exactly-once (§3.7: at-least-once has no KeeperMap
  # dependency, so quorum loss degenerates to a plain C4 availability blip).
  if [ "${QUORUM_LOSS}" = "1" ] && [ "${EXACTLY_ONCE}" != "1" ]; then
    warn "--quorum-loss forces --exactly-once 1 (§3.7); overriding requested '${EXACTLY_ONCE:-<unset>}'"
    EXACTLY_ONCE="1"
  fi

  case "${EXACTLY_ONCE}" in
    0|1) ;;
    *) die "--exactly-once must be 0 or 1 (got '${EXACTLY_ONCE:-<unset>}')" ;;
  esac

  if [ "${MODE}" = "monkey" ] && [ -z "${SEED}" ]; then
    die "monkey mode requires --seed <int> (the replay key, IC-3/§3.3)"
  fi
  if [ -n "${SEED}" ]; then
    case "${SEED}" in *[!0-9]*) die "--seed must be a non-negative integer (got '${SEED}')" ;; esac
  fi
  case "${ROUNDS}" in ''|*[!0-9]*) die "--rounds must be a positive integer (got '${ROUNDS}')" ;; esac
  [ "${ROUNDS}" -ge 1 ] 2>/dev/null || die "--rounds must be >= 1"

  # --watch-cell (ignorePartitionsWhenBatching=true WATCH cell, §3.2) is
  # at-least-once ONLY: eo=1 + ipwb is silently ignored by the sink
  # (ClickHouseSinkTask.java:150), so a run would test a config its runtime keys
  # do not reflect. Reject loudly here as well as in validate_chaos_config.
  if [ "${WATCH_CELL}" = "1" ] && [ "${EXACTLY_ONCE}" = "1" ]; then
    die "--watch-cell (ignorePartitionsWhenBatching=true) is incompatible with --exactly-once 1 (silently ignored by the sink, §3.2); it is an at-least-once WATCH cell only"
  fi
  if [ "${WATCH_CELL}" = "1" ]; then
    IPWB="true"; IPWB_FIELD="1"
  fi
}

# Derive the run-shape identities from the validated flags (both plan + live).
resolve_run_shape() {
  if [ "${EXACTLY_ONCE}" = "1" ]; then
    DELIVERY_MODE="exactly_once"
  else
    DELIVERY_MODE="at_least_once"
  fi
  GROUP="ch-sink-chaos-eo${EXACTLY_ONCE}"
  if [ "${QUORUM_LOSS}" = "1" ]; then
    CHAOS_MODE="quorum_loss"       # IC-9 filterable; never averaged into defaults
  else
    CHAOS_MODE="${MODE}"
  fi
  CH_VERSION="${CHAOS_CH_VERSION}"
  OUT_DIR="${OUT_DIR:-${SCRIPT_DIR}/artifacts/chaos}"
}

_profile() { [ "${AGGRESSIVE}" = "1" ] && echo "aggressive" || echo "default"; }
_faults_resolved() { [ "${QUORUM_LOSS}" = "1" ] && echo "quorum_loss" || echo "${FAULTS}"; }
_yesno() { [ "$1" = "1" ] && echo "yes" || echo "no"; }

# =========================================================================== #
# --plan / -n : print the resolved phase plan; execute nothing.
# =========================================================================== #
print_plan() {
  local seed_disp="${SEED:-<none>}"
  local mode_branch
  if [ "${MODE}" = "smoke" ]; then
    mode_branch="  smoke : drain-progress ${DRAIN_TARGET_PCT}% -> single fault -> recovery gate -> fence"
  else
    mode_branch="  monkey: schedule.py(seed=${seed_disp},rounds=${ROUNDS}) -> per-round inject (window racer for crash-class) -> recovery gate -> record round"
  fi
  local km_line
  if [ "${EXACTLY_ONCE}" = "1" ]; then
    km_line="PHASE 2b keeper_map reset  DROP zkDatabase connect_state_chaos (§2.4 stale-state)   [eo=1: YES]"
  else
    km_line="PHASE 2b keeper_map reset  (exactly-once only)                                        [eo=0: SKIPPED]"
  fi
  cat <<EOF
=== Chaos / monkey test (#771) — PHASE PLAN (dry-run, nothing executed) ===
mode            : ${MODE}
delivery mode   : ${DELIVERY_MODE} (exactly_once=${EXACTLY_ONCE})
chaos_mode      : ${CHAOS_MODE}
seed            : ${seed_disp}
rounds          : ${ROUNDS}
faults          : $(_faults_resolved)
profile         : $(_profile)
quorum-loss     : $(_yesno "${QUORUM_LOSS}")
watch-cell      : $(_yesno "${WATCH_CELL}") (ignore_partitions_when_batching=${IPWB_FIELD})
identities      : topic=${TOPIC} dlq=${DLQ_TOPIC} table=${CH_DATABASE}.${CH_TABLE}
                  group=${GROUP} connector_cr=${CONNECTOR_NAME} connect_cr=${CONNECT_NAME}
                  connector=${CONNECTOR_LABEL} environment_class=${ENVIRONMENT_CLASS_CHAOS}
                  zkPath=/kafka-connect-chaos zkDatabase=${ZK_DATABASE}
scale           : bench-ng 0->${SCALE_UP_NODES} + connect-ng 0->1 (SCALE_UP_NODES=${SCALE_UP_NODES})
CH image        : clickhouse/clickhouse-server:${CHAOS_CH_VERSION} (IC-8: version-selected, digest read back post-deploy)
out dir         : ${OUT_DIR}
exit codes      : 0 PASS(integrity_ok,quiesced) | 1 FAIL(MISMATCH / t_recover_timeout stuck) | 3 UNVERIFIED(CHECK_ERROR / fault-not-observed / infra-stall / t_settle_timeout)

PHASE 0  isolation guard   refuse to start if the pair connector ${PAIR_CONNECTOR_NAME} exists (§4 rule 1)
PHASE 0  image validation  digest-law on ARM_IMAGE + PRODUCER_IMAGE (validate_image_ref); CH version-selected (IC-8)
PHASE 0  config validation validate_chaos_config via render_connector.py (exactlyOnce/ipwb/bufferCount §3.2)
PHASE 1  scale up          bench-ng 0->${SCALE_UP_NODES} + connect-ng 0->1
PHASE 2  CH cluster        deploy_ch_cluster (3 replicas + keeper quorum) + apply_chaos_ddl + keeper_map_smoke + read_ch_image_digest
${km_line}
PHASE 3  topics            create ${TOPIC} (${EXPECTED_PARTITIONS} partitions) + ${DLQ_TOPIC} (1 partition)
PHASE 4  preload           $( [ "${MODE}" = "smoke" ] && echo "one-shot producer (clean/dup dataset)" || echo "base preload + --stream producer (rate ${STREAM_RATE}/s)" )
PHASE 4b poller host       in-cluster poller pod
PHASE 5  Connect+connector deploy ${CONNECT_NAME} + chaos connector (render_connector.py --exactly-once ${EXACTLY_ONCE} --ipwb ${IPWB}) + wait tasks RUNNING
PHASE 6  ${MODE} loop
${mode_branch}
PHASE 7  fence             SIGTERM stream producer; rows_expected := broker offsets ${TOPIC}; SOURCE_UNIQUE_EXPECTED (IC-7)
PHASE 8  quiescence        lag=0 sustained W=${W_SECONDS}s + tasks RUNNING + repl queue empty + DLQ depth 0 (bounded T_settle=${T_SETTLE}s)
PHASE 9  oracle            check_integrity.py --direct (SETTINGS select_sequential_consistency=1)
PHASE 10 artifact          write_artifact.py -> ${OUT_DIR} (written on EVERY path incl. timeouts)
PHASE 11 teardown          connector/Connect/groups/topics/poller + teardown_ch_cluster (verify STS gone) + scale-down (verify nodegroups 0, §9.8)
=== END PHASE PLAN ===
EOF
}

# --plan --reap : print the state-free teardown sequence; execute nothing (T15).
print_reap_plan() {
  cat <<EOF
=== Chaos / monkey test (#771) — REAP PLAN (--reap dry-run, nothing executed) ===
mode            : reap (state-free, idempotent teardown-only; T15 kill-safety net)
why             : the cleanup trap cannot fire on SIGKILL/CI-timeout — --reap is the
                  belt-and-suspenders that force-removes a leaked cluster (§9.8)
identities      : connector_cr=${CONNECTOR_NAME} connect_cr=${CONNECT_NAME}
                  groups=ch-sink-chaos-eo0,ch-sink-chaos-eo1
                  topic=${TOPIC} dlq=${DLQ_TOPIC} stream_pod=${STREAM_PRODUCER_POD}
                  ch_sts=${CH_STS}
gates skipped   : NO isolation guard / image validation / config validation
                  (those gate LIVE runs, not a teardown-only reap)

REAP 1  delete chaos connector CR ${CONNECTOR_NAME} + Connect CR ${CONNECT_NAME}
REAP 2  delete chaos consumer groups ch-sink-chaos-eo0 + ch-sink-chaos-eo1
REAP 3  delete stream producer pod ${STREAM_PRODUCER_POD} + poller pod
REAP 4  delete topics ${TOPIC} + ${DLQ_TOPIC}
REAP 5  teardown_ch_cluster (STS/Services/ConfigMap/PVCs; verify ${CH_STS} GONE, §9.8)
REAP 6  phase_scale_down (bench-ng + connect-ng -> desired 0)
REAP 7  ASSERT §9.8: CH StatefulSet ${CH_STS} gone AND both nodegroups desired=0
        -> exit 0 if clean, non-zero (loud) on any cost leak
        (every step ignore-not-found: nothing deployed still exits 0)
=== END REAP PLAN ===
EOF
}

# =========================================================================== #
# PHASE 0 — pre-flight gates (before any cluster mutation).
# =========================================================================== #

# §4 rule 1: never run concurrent with a benchmark pair. The pair's DISTINCT
# sink connector (bench-clickhouse-sink) being present is the "pair active"
# signal (chaos uses chaos-clickhouse-sink, so this cannot false-trip on us).
isolation_guard() {
  log "PHASE 0: isolation guard — refuse to start if a benchmark pair is active (§4 rule 1)"
  local existing
  existing="$(kubectl -n "${NS}" get kafkaconnector "${PAIR_CONNECTOR_NAME}" \
                --ignore-not-found -o name 2>/dev/null)"
  [ -z "${existing}" ] \
    || die "isolation guard: pair connector ${PAIR_CONNECTOR_NAME} is present (${existing}) — a benchmark pair is active; refusing to start a chaos run concurrently (§4 rule 1)"
  log "  no active pair detected"
}

# Digest-law (§4 rule 3): every SUT/harness image must be a digest ref. The CH
# TARGET is the IC-8-scoped exception (version-selected), so ONLY ARM_IMAGE +
# PRODUCER_IMAGE are validated here, in place, before any phase.
phase_validate_images() {
  log "PHASE 0: image validation (digest law on ARM_IMAGE + PRODUCER_IMAGE; CH is version-selected, IC-8)"
  ARM_IMAGE="$(validate_image_ref ARM_IMAGE "${ARM_IMAGE:?ARM_IMAGE required (connect image, digest)}")" \
    || die "ARM_IMAGE failed digest validation"
  PRODUCER_IMAGE="$(validate_image_ref PRODUCER_IMAGE "${PRODUCER_IMAGE:?PRODUCER_IMAGE required (producer/poller image, digest)}")" \
    || die "PRODUCER_IMAGE failed digest validation"
  export ARM_IMAGE PRODUCER_IMAGE
  log "  images validated (digest-pinned): arm=${ARM_IMAGE} producer=${PRODUCER_IMAGE}"
  # Non-fatal newer-push eyeball for ECR refs (same gate as run_pair.sh).
  check_image_provenance ARM_IMAGE "${ARM_IMAGE}"
  check_image_provenance PRODUCER_IMAGE "${PRODUCER_IMAGE}"
}

# Render the chaos connector CR NOW (before scale-up). render_connector.py runs
# validate_chaos_config() (§3.2), so this doubles as the config-combination gate
# and produces the CR the deploy phase applies.
render_chaos_connector() {
  local out="$1"
  local wc=()
  [ "${WATCH_CELL}" = "1" ] && wc=(--watch-cell)
  python3 "${SCRIPT_DIR}/render_connector.py" \
    --template "${CHAOS_CONNECTOR_TMPL}" \
    --exactly-once "${EXACTLY_ONCE}" --ipwb "${IPWB}" "${wc[@]}" \
    --group "${GROUP}" --out "${out}"
}

phase_validate_config() {
  log "PHASE 0: config validation — validate_chaos_config via render_connector.py (§3.2)"
  CHAOS_CONNECTOR_RENDERED="${OUT_DIR}/chaos-connector-${CHAOS_ID}.json"
  mkdir -p "${OUT_DIR}"
  render_chaos_connector "${CHAOS_CONNECTOR_RENDERED}" \
    || die "chaos connector config rejected (validate_chaos_config, §3.2) or render failed"
  log "  config valid; rendered chaos connector -> ${CHAOS_CONNECTOR_RENDERED}"
}

# =========================================================================== #
# PHASE 2/3 — CH cluster + chaos topics.
# =========================================================================== #
phase_ch_cluster() {
  log "PHASE 2: self-hosted CH cluster (3 replicas + keeper quorum) + DDL + keeper_map smoke"
  deploy_ch_cluster
  apply_chaos_ddl
  keeper_map_smoke                 # acceptance §9.2 probe
  read_ch_image_digest >/dev/null  # IC-8 read-back -> exports CH_IMAGE_DIGEST
  log "  CH image digest (deployed truth): ${CH_IMAGE_DIGEST:-<unread>}"
  # Exactly-once ONLY: drop the stale KeeperMap state table (§2.4 stale-state
  # trap; recreated lazily by KeeperStateProvider.init). NEVER on at-least-once.
  if [ "${EXACTLY_ONCE}" = "1" ]; then
    log "PHASE 2b: KeeperMap state reset (exactly-once)"
    keeper_map_reset
  else
    log "PHASE 2b: KeeperMap state reset SKIPPED (at-least-once has no KeeperMap state)"
  fi
}

# Apply a KafkaTopic CR from an inline manifest (chaos owns its topics; the pair
# topic template is hardcoded to `hits`, so it is not reused here).
_apply_topic() {
  local name="$1" parts="$2"
  kubectl -n "${NS}" delete kafkatopic "${name}" --ignore-not-found --wait=true --timeout=120s 2>/dev/null || true
  cat <<EOF | kubectl apply -f - || die "topic ${name} apply failed"
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: ${name}
  namespace: ${NS}
  labels:
    strimzi.io/cluster: bench
    app.kubernetes.io/part-of: kafka-connect-benchmark-v2
spec:
  partitions: ${parts}
  replicas: 1
  config:
    retention.ms: "86400000"
EOF
  kubectl -n "${NS}" wait kafkatopic/"${name}" --for=condition=Ready --timeout=120s \
    || die "topic ${name} did not become Ready"
}

phase_topics() {
  log "PHASE 3: create ${TOPIC} (${EXPECTED_PARTITIONS} partitions) + DLQ ${DLQ_TOPIC} (1 partition)"
  _apply_topic "${TOPIC}" "${EXPECTED_PARTITIONS}"
  # Pre-create the DLQ so quiescence's DLQ-depth==0 probe reads a real topic even
  # before Connect would auto-create it (RF=1, §3.6b DLQ pin).
  _apply_topic "${DLQ_TOPIC}" "1"
}

# =========================================================================== #
# PHASE 4 — preload (smoke one-shot; monkey base + continuous stream, IC-7).
# =========================================================================== #
phase_chaos_preload() {
  log "PHASE 4: preload (mode=${MODE})"
  # Base preload: render + apply the one-shot indexed producer Job (reuse the
  # pair renderer), targeting the chaos topic + dataset. broker offsets are the
  # authoritative count (frozen later at the fence).
  local job_yaml="${OUT_DIR}/chaos-producer-job.yaml"
  mkdir -p "${OUT_DIR}"
  python3 "${SCRIPT_DIR}/render_producer_job.py" \
    --job "${PRODUCER_DIR}/job.yaml" \
    --image "${PRODUCER_IMAGE:?PRODUCER_IMAGE required}" \
    --service-account "${PRODUCER_SA:-hits-producer}" \
    --parquet-source "${PARQUET_SOURCE:?PARQUET_SOURCE required}" \
    --shard-count "${SHARD_COUNT:-3}" \
    --topic "${TOPIC}" \
    --out "${job_yaml}" || die "chaos producer job render failed"
  kubectl -n "${NS}" delete job hits-producer --ignore-not-found --wait=true --timeout=120s 2>/dev/null || true
  kubectl apply -f "${job_yaml}" || die "chaos producer job apply failed"
  # F-L2b: scope the one-shot wait to CHAOS_PRODUCER_TIMEOUT (default 900s) rather
  # than the pair's 6h PRODUCER_TIMEOUT default, so a failed smoke/base preload
  # fails FAST. NOTE: monkey's continuous --stream producer (below) is NOT bound
  # by this — it is SIGTERM-fenced at phase_fence (IC-7), never by a wait timeout.
  export PRODUCER_TIMEOUT="${CHAOS_PRODUCER_TIMEOUT}"
  kubectl -n "${NS}" wait --for=condition=complete job/hits-producer --timeout="${PRODUCER_TIMEOUT}s" \
    || die "chaos base preload Job did not complete within ${PRODUCER_TIMEOUT}s (CHAOS_PRODUCER_TIMEOUT)"

  if [ "${MODE}" = "monkey" ]; then
    # Continuous, bounded-rate producer (IC-7): long-lived, single-pass, never
    # wraps. SIGTERM (the fence, phase_fence) stops it and prints unique_sent.
    log "  starting continuous --stream producer (${STREAM_PRODUCER_POD}, rate=${STREAM_RATE}/s)"
    kubectl -n "${NS}" delete pod "${STREAM_PRODUCER_POD}" --ignore-not-found --wait=true --timeout=120s 2>/dev/null || true
    kubectl -n "${NS}" run "${STREAM_PRODUCER_POD}" --image="${PRODUCER_IMAGE}" \
      --restart=Never \
      --labels="app.kubernetes.io/part-of=kafka-connect-benchmark-v2" \
      --overrides='{"spec":{"nodeSelector":{"role":"bench"}}}' \
      --command -- python producer.py --stream --rate-limit "${STREAM_RATE}" \
        --topic "${TOPIC}" --parquet-source "${PARQUET_SOURCE}" \
      || die "continuous stream producer create failed"
  fi
}

# =========================================================================== #
# PHASE 5 — Connect worker + chaos connector.
# =========================================================================== #
phase_connect() {
  log "PHASE 5: deploy Connect worker (${CONNECT_NAME}) + chaos connector (${CONNECTOR_NAME})"
  apply_secret_and_metrics
  deploy_connect "${ARM_IMAGE}"
  kubectl apply -f "${CHAOS_CONNECTOR_RENDERED}" || fail_run "chaos connector apply failed"
  wait_tasks_running
}

# =========================================================================== #
# GATE PROBES — written to OUT_DIR as tiny scripts so chaos/gates.py can run
# them via the shell (--probe-*-cmd). They read the exported identity env vars.
# =========================================================================== #
_write_probe_scripts() {
  export NS POLLER_POD CONNECT_REST CONNECTOR_NAME BROKER_POD DLQ_TOPIC \
         CH_QUERY_POD CH_DATABASE CH_TABLE
  mkdir -p "${OUT_DIR}"
  cat > "${OUT_DIR}/probe-status.sh" <<'EOF'
#!/usr/bin/env bash
kubectl -n "$NS" exec "$POLLER_POD" -- python -c \
  "import requests;print(requests.get('$CONNECT_REST/connectors/$CONNECTOR_NAME/status',timeout=10).text)" 2>/dev/null
EOF
  cat > "${OUT_DIR}/probe-lag.sh" <<'EOF'
#!/usr/bin/env bash
kubectl -n "$NS" exec "$BROKER_POD" -- bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group "$GATE_GROUP" 2>/dev/null \
  | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END{print s+0}'
EOF
  cat > "${OUT_DIR}/probe-dlq.sh" <<'EOF'
#!/usr/bin/env bash
kubectl -n "$NS" exec "$BROKER_POD" -- bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic "$DLQ_TOPIC" --time -1 2>/dev/null \
  | awk -F: '{s+=$3} END{print s+0}'
EOF
  cat > "${OUT_DIR}/probe-repl-queue.sh" <<'EOF'
#!/usr/bin/env bash
kubectl -n "$NS" exec "$CH_QUERY_POD" -- clickhouse-client --query \
  "SELECT sum(queue_size) FROM system.replicas WHERE database='$CH_DATABASE' AND table='$CH_TABLE'" 2>/dev/null
EOF
  cat > "${OUT_DIR}/probe-pod-ready.sh" <<'EOF'
#!/usr/bin/env bash
st="$(kubectl -n "$NS" get pod "$GATE_POD" \
       -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)"
[ "$st" = "True" ] && echo 1 || echo 0
EOF
  chmod +x "${OUT_DIR}"/probe-*.sh
}

# =========================================================================== #
# FAULT DISPATCH — maps a fault token to its injector (evidence JSON on stdout).
# Reused by the monkey loop AND the internal --inject-once re-exec (window racer
# --kill-cmd), so the racer can time the kill into a target window (§3.4).
# =========================================================================== #
_inject_fault() {
  local fault="$1"
  case "${fault}" in
    C1) inject_connect_pod_kill ;;
    C2) inject_task_restart ;;
    C3) inject_broker_pod_kill ;;
    C4) inject_ch_node_kill ;;
    C5) inject_ch_keeper_leader_kill ;;
    quorum_loss) inject_quorum_loss ;;
    *) die "_inject_fault: unknown fault '${fault}'" ;;
  esac
}

# Crash-class faults (pod kills) are raced into a target crash window; C2/C3 are
# injected directly (a REST restart / broker kill has no doLogic window to hit).
_is_crash_class() { case "$1" in C1|C4|C5) return 0 ;; *) return 1 ;; esac; }

# The recovery gate for one round (chaos/gates.py recovery). CH-side faults also
# require the rejoined replica caught up (--ch-fault + repl-queue probe).
_run_recovery_gate() {
  local fault="$1" out="$2" ch_fault=() pod_ready=()
  case "${fault}" in C4|C5|quorum_loss) ch_fault=(--ch-fault --probe-repl-queue-cmd "sh ${OUT_DIR}/probe-repl-queue.sh") ;; esac
  [ -n "${GATE_POD:-}" ] && pod_ready=(--probe-pod-ready-cmd "sh ${OUT_DIR}/probe-pod-ready.sh")
  GATE_GROUP="${GROUP}" python3 "${CHAOS_DIR}/gates.py" recovery \
    --probe-status-cmd "sh ${OUT_DIR}/probe-status.sh" \
    --probe-lag-cmd "sh ${OUT_DIR}/probe-lag.sh" \
    "${pod_ready[@]}" "${ch_fault[@]}" \
    --q-seconds "${Q_SECONDS}" --t-recover "${T_RECOVER}" > "${out}"
}

# Append one IC-4 round record to ROUNDS_FILE, assembled from the injector
# evidence + the recovery-gate JSON.
_record_round() {
  local round="$1" fault="$2" window="$3" inject_ts="$4" evidence="$5" recovery_json="$6"
  python3 - "$round" "$fault" "$window" "$inject_ts" "$evidence" "$recovery_json" >> "${ROUNDS_FILE}" <<'PY'
import json, sys
round_no, fault, window, inject_ts, evidence_raw, recovery_raw = sys.argv[1:7]
try:
    ev = json.loads(evidence_raw) if evidence_raw.strip() else {}
except Exception:
    ev = {}
try:
    rec = json.loads(recovery_raw) if recovery_raw.strip() else {}
except Exception:
    rec = {}
detail = ev.get("detail", "")
ins_err = 0
for tok in str(detail).replace(";", " ").split():
    if tok.startswith("insert_errors="):
        try:
            ins_err = int(tok.split("=", 1)[1])
        except ValueError:
            ins_err = 0
rec_seconds = rec.get("recovery_seconds", 0.0)
restart_count = rec.get("task_restart_count", 0)
# Fault-took-effect (§5): any observed evidence => True.
fault_observed = bool(ev) and ev.get("kind") is not None
record = {
    "round": int(round_no),
    "fault_type": fault,
    "fault_window": window,
    "inject_ts": inject_ts,
    "recovery_seconds": rec_seconds,
    "task_restart_count": restart_count,
    "insert_errors_during_fault": ins_err,
    "ch_dedup_dropped_blocks": 0,
    "fault_observed": fault_observed,
    "evidence": ev,
}
print(json.dumps(record))
PY
}

# =========================================================================== #
# PHASE 6 — the run loop (smoke = one fault; monkey = R rounds).
# =========================================================================== #
run_smoke() {
  log "PHASE 6 (smoke): drain to ${DRAIN_TARGET_PCT}% -> single fault -> recovery gate"
  # Wait until 50% drained (the smoke-gate trigger, §3.5).
  GATE_GROUP="${GROUP}" python3 "${CHAOS_DIR}/gates.py" drain-progress \
    --target-pct "${DRAIN_TARGET_PCT}" \
    --probe-lag-cmd "sh ${OUT_DIR}/probe-lag.sh" --timeout "${T_RECOVER}" \
    || warn "smoke drain-progress gate did not reach ${DRAIN_TARGET_PCT}% (continuing to inject)"

  local fault="${SMOKE_FAULT:-C1}"       # smoke set is {C1, C4}; default C1
  local inject_ts evidence rc rec="${OUT_DIR}/recovery-1.json"
  inject_ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  evidence="$(_inject_fault "${fault}")" || warn "smoke fault ${fault} injector returned non-zero"
  rc=0; _run_recovery_gate "${fault}" "${rec}" || rc=$?
  _record_round 1 "${fault}" "na" "${inject_ts}" "${evidence}" "$(cat "${rec}" 2>/dev/null)"
  ROUNDS_COMPLETED=1
  _handle_recovery_rc "${rc}"
}

run_monkey() {
  log "PHASE 6 (monkey): schedule.py(seed=${SEED}, rounds=${ROUNDS}) -> per-round inject + recovery gate"
  local agg=() ql=()
  [ "${AGGRESSIVE}" = "1" ] && agg=(--aggressive)
  [ "${QUORUM_LOSS}" = "1" ] && ql=(--quorum-loss)
  local schedule
  schedule="$(python3 "${CHAOS_DIR}/schedule.py" --seed "${SEED}" --rounds "${ROUNDS}" \
                --t-min "${T_MIN}" --t-max "${T_MAX}" --faults "${FAULTS}" \
                "${agg[@]}" "${ql[@]}")" \
    || die "schedule.py failed to generate the seeded fault schedule"

  # Iterate rounds (tab-separated: round, wait, fault, window). Caps (§3.3) are
  # enforced by DELAYING a CH fault until the cluster is fully Ready — never by
  # re-rolling (IC-5): the seeded SEQUENCE is preserved, only timings drift.
  local round wait fault window
  while IFS=$'\t' read -r round wait fault window; do
    [ -n "${round}" ] || continue
    log "  round ${round}: wait=${wait}s fault=${fault} target_window=${window}"
    sleep "${wait}" 2>/dev/null || true
    case "${fault}" in
      C4|C5|quorum_loss)
        # ≤1-CH-down cap (§3.3): delay until every CH pod is Ready again.
        while ! _ch_all_ready; do
          warn "    cap: a CH pod is still NotReady — delaying ${fault} (not re-rolling, IC-5)"
          sleep "${FAULT_POLL_INTERVAL:-3}"
        done ;;
    esac

    local inject_ts evidence rc rec="${OUT_DIR}/recovery-${round}.json"
    inject_ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    if _is_crash_class "${fault}"; then
      evidence="$(_race_fault "${fault}" "${window}")"
    else
      evidence="$(_inject_fault "${fault}")" || warn "round ${round}: injector ${fault} returned non-zero"
      window="na"
    fi
    rc=0; _run_recovery_gate "${fault}" "${rec}" || rc=$?
    _record_round "${round}" "${fault}" "${_LANDED_WINDOW:-${window}}" "${inject_ts}" \
      "${evidence}" "$(cat "${rec}" 2>/dev/null)"
    ROUNDS_COMPLETED="${round}"
    _handle_recovery_rc "${rc}"
  done < <(printf '%s' "${schedule}" | python3 -c '
import json, sys
sched = json.load(sys.stdin)
for r in sched.get("rounds", []):
    print("\t".join(str(r[k]) for k in ("round", "wait_seconds", "fault_type", "target_window")))
')
}

# Race a crash-class kill into its target window (§3.4). The window racer tails
# the connector log and fires the injector (via the --inject-once re-exec)
# inside the target window, reporting the window it ACTUALLY landed in.
_race_fault() {
  local fault="$1" target="$2" racer_mode ev_file racer_out landed
  [ "${EXACTLY_ONCE}" = "1" ] && racer_mode="exactly-once" || racer_mode="at-least-once"
  ev_file="${OUT_DIR}/evidence-${fault}-$$.json"
  local connect_pod; connect_pod="$(connect_pod)"
  racer_out="$(python3 "${CHAOS_DIR}/window_racer.py" \
      --target-window "${target}" --mode "${racer_mode}" \
      --logs-cmd "kubectl -n ${NS} logs -f ${connect_pod}" \
      --kill-cmd "CHAOS_INJECT=${fault} CHAOS_EVIDENCE_OUT=${ev_file} bash '${SELF}' --inject-once" \
      2>/dev/null)" || true
  landed="$(printf '%s' "${racer_out}" | python3 -c 'import json,sys
try: print(json.load(sys.stdin)["fault_window"])
except Exception: print("na")' 2>/dev/null)"
  _LANDED_WINDOW="${landed:-na}"
  cat "${ev_file}" 2>/dev/null || echo '{}'
}

# Map a recovery-gate exit code to the IC-3 outcome; a timeout ends the run
# (artifact-on-every-path) instead of hanging (§3.6a).
_handle_recovery_rc() {
  local rc="$1"
  case "${rc}" in
    0) : ;;   # recovered
    21) abort_with_artifact 1 "failed" "t_recover_timeout" "recovery gate STUCK (a task pinned FAILED, §3.6a)" ;;
    22) abort_with_artifact 3 "integrity_unverified" "t_recover_timeout" "recovery gate INFRA STALL (no task FAILED, §3.6a)" ;;
    *)  abort_with_artifact 3 "integrity_unverified" "t_recover_timeout" "recovery gate returned unexpected exit ${rc}" ;;
  esac
}

# =========================================================================== #
# PHASE 7/8/9 — fence, quiescence, oracle.
# =========================================================================== #
phase_fence() {
  log "PHASE 7: fence — stop the continuous producer, freeze rows_expected"
  if [ "${MODE}" = "monkey" ]; then
    # SIGTERM = the fence signal (IC-7): the pod's producer flushes in-flight and
    # prints the final summary (rows_sent + unique_sent) before exiting.
    kubectl -n "${NS}" delete pod "${STREAM_PRODUCER_POD}" --grace-period=60 --wait=true --timeout=120s 2>/dev/null || true
    local summary
    summary="$(kubectl -n "${NS}" logs "${STREAM_PRODUCER_POD}" --tail=-1 2>/dev/null | grep '^{' | tail -1)"
    SOURCE_UNIQUE_EXPECTED="$(printf '%s' "${summary}" | python3 -c 'import json,sys
try: print(json.load(sys.stdin)["unique_sent"])
except Exception: print("")' 2>/dev/null)"
    [ -n "${SOURCE_UNIQUE_EXPECTED}" ] \
      || warn "could not read unique_sent from the stream producer summary — SOURCE_UNIQUE_EXPECTED must be supplied (IC-7 cross-check)"
    export SOURCE_UNIQUE_EXPECTED
  fi
  # rows_expected is ALWAYS offsets-derived (ECOSYSTEM §5), frozen here.
  ROWS_EXPECTED="$(broker_topic_row_count "${TOPIC}")" \
    || die "fence: could not derive rows_expected from broker offsets for ${TOPIC}"
  case "${ROWS_EXPECTED}" in ''|*[!0-9]*) die "fence: non-numeric rows_expected='${ROWS_EXPECTED}'" ;; esac
  export ROWS_EXPECTED
  log "  rows_expected frozen at ${ROWS_EXPECTED}; SOURCE_UNIQUE_EXPECTED=${SOURCE_UNIQUE_EXPECTED:-<env>}"
}

phase_quiescence() {
  log "PHASE 8: quiescence — lag=0 sustained W=${W_SECONDS}s + tasks RUNNING + repl queue empty + DLQ depth 0"
  local rc=0
  GATE_GROUP="${GROUP}" python3 "${CHAOS_DIR}/gates.py" quiescence \
    --probe-status-cmd "sh ${OUT_DIR}/probe-status.sh" \
    --probe-lag-cmd "sh ${OUT_DIR}/probe-lag.sh" \
    --probe-repl-queue-cmd "sh ${OUT_DIR}/probe-repl-queue.sh" \
    --probe-dlq-cmd "sh ${OUT_DIR}/probe-dlq.sh" \
    --w-seconds "${W_SECONDS}" --t-settle "${T_SETTLE}" || rc=$?
  if [ "${rc}" != "0" ]; then
    # 23 = t_settle_timeout (never a silent pass, §3.6b).
    abort_with_artifact 3 "integrity_unverified" "t_settle_timeout" "quiescence did not settle within T_settle=${T_SETTLE}s (gate exit ${rc})"
  fi
  # Ensure the read hits a caught-up replica before the oracle counts.
  await_replica_sync || warn "await_replica_sync returned non-zero (oracle uses select_sequential_consistency=1 as a backstop)"
}

# The oracle: check_integrity.py --direct reads the target directly and classifies
# via integrity_math.chaos_verdict (IC-6). Writes the oracle JSON consumed by the
# artifact writer. Exit 0 PASS / 1 MISMATCH / 3 CHECK_ERROR|unverified.
phase_oracle() {
  log "PHASE 9: oracle — check_integrity.py --direct (SETTINGS select_sequential_consistency=1)"
  local dlq_depth fault_observed rc=0
  dlq_depth="$(sh "${OUT_DIR}/probe-dlq.sh" 2>/dev/null)"; dlq_depth="${dlq_depth:-0}"
  # Fault-took-effect (§5): a run whose integrity is clean but shows NO observed
  # fault effect is UNVERIFIED, never PASS. Any round with fault_observed=true qualifies.
  fault_observed=0
  if [ -s "${ROUNDS_FILE}" ] && grep -q '"fault_observed": true' "${ROUNDS_FILE}"; then
    fault_observed=1
  fi
  ORACLE_FILE="${OUT_DIR}/oracle-${CHAOS_ID}.json"

  # The self-hosted CH client Service DNS (ch-chaos.kafka-bench.svc) is
  # in-cluster-only; the oracle (check_integrity.py) runs on the OPERATOR machine
  # and cannot resolve it (unlike the pair's public Cloud host). Port-forward the
  # Service to localhost so the local oracle can read the target, reusing
  # check_integrity.py --direct unchanged (its retry/backoff/CHECK_ERROR envelope
  # still applies). Torn down right after the read.
  local oracle_host="${TARGET_CH_HOST}" oracle_port="${TARGET_CH_PORT}" pf_pid="" pf_port="${ORACLE_PF_PORT:-18123}"
  case "${TARGET_CH_HOST}" in
    127.0.0.1|localhost) : ;;  # already local (tests / manual)
    *)
      log "  port-forward svc/${CH_SVC} ${pf_port}:8123 for the local oracle read"
      kubectl -n "${NS}" port-forward "svc/${CH_SVC}" "${pf_port}:8123" >/dev/null 2>&1 &
      pf_pid=$!
      local pf_deadline=$(( $(date +%s) + 30 ))
      until curl -sf "http://127.0.0.1:${pf_port}/ping" >/dev/null 2>&1; do
        [ "$(date +%s)" -ge "${pf_deadline}" ] && { warn "oracle port-forward not ready in 30s (oracle will retry/CHECK_ERROR)"; break; }
        sleep 1
      done
      oracle_host="127.0.0.1"; oracle_port="${pf_port}"
      ;;
  esac

  TARGET_CH_HOST="${oracle_host}" TARGET_CH_PORT="${oracle_port}" \
  TARGET_CH_SECURE="${TARGET_CH_SECURE:-false}" \
  TARGET_CH_USER="${TARGET_CH_USER:-default}" TARGET_CH_PASSWORD="${TARGET_CH_PASSWORD-}" \
  CH_DATABASE="${CH_DATABASE}" CH_TABLE="${CH_TABLE}" \
  ROWS_EXPECTED="${ROWS_EXPECTED}" SOURCE_UNIQUE_EXPECTED="${SOURCE_UNIQUE_EXPECTED:?SOURCE_UNIQUE_EXPECTED required (IC-7)}" \
  DLQ_DEPTH="${dlq_depth}" FAULT_OBSERVED="${fault_observed}" \
    python3 "${CAPTURE_DIR}/check_integrity.py" --direct > "${ORACLE_FILE}" || rc=$?

  [ -n "${pf_pid}" ] && kill "${pf_pid}" 2>/dev/null || true
  case "${rc}" in
    0) RUN_OUTCOME="passed" ;;
    1) RUN_OUTCOME="failed" ;;
    *) RUN_OUTCOME="integrity_unverified" ;;
  esac
  export RUN_OUTCOME
  log "  oracle exit=${rc} -> outcome=${RUN_OUTCOME}"
  return "${rc}"
}

# =========================================================================== #
# ARTIFACT — written on EVERY path (IC-3). abort_with_artifact synthesizes an
# oracle JSON when a gate ended the run before the real oracle could run.
# =========================================================================== #
_synth_oracle() {
  local verdict="$1" reason="$2"
  python3 - "$verdict" "$reason" "${ROWS_EXPECTED:-0}" "${SOURCE_UNIQUE_EXPECTED:-0}" <<'PY'
import json, sys
verdict, reason, rows_expected, unique_expected = sys.argv[1:5]
def _int(x):
    try: return int(float(x))
    except Exception: return 0
print(json.dumps({
    "rows_expected": _int(rows_expected),
    "rows_delivered": 0,
    "unique_delivered": 0,
    "unique_expected": _int(unique_expected),
    "duplicate_rows": 0,
    "loss": 0,
    "dlq_depth": 0,
    "verdict": verdict,
    "reason": reason,
}))
PY
}

write_result_artifact() {
  local outcome="$1" run_conclusion="$2"
  [ "${ARTIFACT_WRITTEN}" = "1" ] && return 0
  log "PHASE 10: write result artifact (outcome=${outcome}, run_conclusion=${run_conclusion})"
  mkdir -p "${OUT_DIR}"
  [ -f "${ROUNDS_FILE}" ] || : > "${ROUNDS_FILE}"
  # If no real oracle ran (a gate timeout ended the run), synthesize a CHECK_ERROR
  # oracle so the artifact is still complete (IC-9 mandatory fields).
  if [ ! -s "${ORACLE_FILE}" ]; then
    _synth_oracle "CHECK_ERROR" "run ended before the oracle ran (${run_conclusion})" > "${ORACLE_FILE}"
  fi
  CHAOS_ID="${CHAOS_ID}" CHAOS_MODE="${CHAOS_MODE}" CHAOS_SEED="${SEED:-}" \
  CHAOS_ROUNDS="${ROUNDS}" ROUNDS_COMPLETED="${ROUNDS_COMPLETED}" \
  DELIVERY_MODE="${DELIVERY_MODE}" EXACTLY_ONCE="${EXACTLY_ONCE}" \
  CONNECTOR="${CONNECTOR_LABEL}" ENVIRONMENT_CLASS="${ENVIRONMENT_CLASS_CHAOS}" \
  IGNORE_PARTITIONS_WHEN_BATCHING="${IPWB_FIELD}" \
  CH_VERSION="${CH_VERSION}" CH_IMAGE_DIGEST="${CH_IMAGE_DIGEST:-}" \
  ARM_IMAGE="${ARM_IMAGE:-}" PRODUCER_IMAGE="${PRODUCER_IMAGE:-}" \
  OUTCOME="${outcome}" RUN_CONCLUSION="${run_conclusion}" \
    python3 "${CHAOS_DIR}/write_artifact.py" \
      --rounds "${ROUNDS_FILE}" --oracle "${ORACLE_FILE}" --out "${OUT_DIR}" \
    || warn "write_artifact.py failed — the result artifact may be incomplete"
  ARTIFACT_WRITTEN=1
}

# Write the artifact for an early (gate-timeout) conclusion, tear down, and exit
# with the IC-3 code. The artifact is ALWAYS written before exit.
abort_with_artifact() {
  local code="$1" outcome="$2" run_conclusion="$3" reason="$4"
  warn "run ending early: ${reason} (exit ${code}, outcome=${outcome}, run_conclusion=${run_conclusion})"
  write_result_artifact "${outcome}" "${run_conclusion}"
  teardown_chaos
  CLEANUP_DONE=1
  trap - EXIT INT TERM
  exit "${code}"
}

# =========================================================================== #
# TEARDOWN — §4 rule 4 / §9.8: verify the CH StatefulSet is GONE and both
# nodegroups reach 0. teardown_ch_cluster verifies the STS; phase_scale_down ->
# scale-down.sh verifies both nodegroups at desired 0.
# =========================================================================== #
teardown_chaos() {
  [ "${CLEANUP_DONE}" = "1" ] && return 0
  log "PHASE 11: teardown (connector/Connect/groups/topics/poller + CH cluster + scale-down)"
  kubectl -n "${NS}" delete kafkaconnector "${CONNECTOR_NAME}" --ignore-not-found --wait=false 2>/dev/null || true
  delete_connect
  [ -n "${GROUP}" ] && { "${SCRIPT_DIR}/delete_consumer_group.sh" "${GROUP}" || true; }
  kubectl -n "${NS}" delete pod "${STREAM_PRODUCER_POD}" --ignore-not-found --wait=false 2>/dev/null || true
  teardown_poller_pod
  kubectl -n "${NS}" delete kafkatopic "${TOPIC}" "${DLQ_TOPIC}" --ignore-not-found --wait=false 2>/dev/null || true
  # §9.8: verify the CH StatefulSet is torn down (teardown_ch_cluster dies loud
  # if the STS survives) AND both nodegroups reach 0 (phase_scale_down ->
  # scale-down.sh exits non-zero on a cost leak).
  teardown_ch_cluster || warn "teardown_ch_cluster reported a problem — check for a surviving CH StatefulSet (cost/data leak, §9.8)"
  phase_scale_down || warn "phase_scale_down reported non-zero — a nodegroup did NOT reach 0 (cost leak, §9.8)"
}

# =========================================================================== #
# --reap (T15): state-free, idempotent teardown-only mode. The kill-safety net
# for the L2 incident — a SIGKILLed / CI-timed-out run leaks 5+1 nodes + the CH
# cluster because nothing can trap SIGKILL. --reap force-removes every chaos
# resource regardless of run state, then asserts the two cost-critical §9.8
# invariants: the CH StatefulSet is GONE and both nodegroups reach desired 0.
# Every step is ignore-not-found, so it is safe to run any number of times
# whether or not a run is live (nothing deployed -> still a clean exit 0).
# =========================================================================== #
run_reap() {
  log "REAP (T15): state-free idempotent chaos teardown + §9.8 cost-leak assertion"
  # Both delivery-mode consumer groups may exist (we do not know which eo ran, or
  # if a run got far enough to create one); delete BOTH, then let teardown_chaos
  # own the rest (DRY — it holds every other chaos identity + teardown function).
  # GROUP is cleared so teardown_chaos does not also re-issue a single-group delete.
  local eo
  for eo in 0 1; do
    "${SCRIPT_DIR}/delete_consumer_group.sh" "ch-sink-chaos-eo${eo}" 2>/dev/null || true
  done
  GROUP=""
  teardown_chaos
  # teardown_ch_cluster (inside teardown_chaos) dies loud if the STS survives, so
  # reaching here already implies it is gone; teardown_chaos's phase_scale_down,
  # however, SWALLOWS a nodegroup leak into a warn. Re-assert both invariants and
  # let their exit codes — not the swallowed warns — be the reap verdict (§9.8).
  local rc=0 sts_left
  sts_left="$(kubectl -n "${NS}" get statefulset "${CH_STS}" --ignore-not-found -o name 2>/dev/null)"
  [ -z "${sts_left}" ] \
    || { warn "reap: CH StatefulSet ${CH_STS} still present (${sts_left}) — cost/data leak (§9.8)"; rc=1; }
  # scale-down.sh is the authoritative nodegroup=0 verifier (exit non-zero iff a
  # nodegroup fails to reach desired 0); idempotent, so re-running it is safe.
  "${INFRA_DIR}/scale-down.sh" \
    || { warn "reap: a nodegroup did NOT reach desired 0 (cost leak, §9.8)"; rc=1; }
  if [ "${rc}" = "0" ]; then
    log "reap: CLEAN — CH StatefulSet ${CH_STS} gone AND both nodegroups desired=0"
  else
    warn "reap: INCOMPLETE — a chaos resource survived (see above); rerun --reap or investigate"
  fi
  return "${rc}"
}

# Cleanup trap for any abnormal exit (a lib `die`, a hard kill). Writes a
# best-effort artifact (if none yet) then tears down — mirrors run_pair.sh's trap.
cleanup_trap() {
  local rc=$?
  if [ "${CLEANUP_DONE:-0}" != "1" ]; then
    warn "cleanup trap firing (rc=${rc}); writing a best-effort artifact then tearing down"
    write_result_artifact "integrity_unverified" "t_recover_timeout" 2>/dev/null || true
    teardown_chaos
  fi
  exit "${rc}"
}

# =========================================================================== #
# main
# =========================================================================== #
main() {
  parse_args "$@"

  # --reap (T15) is teardown-only: it takes NO --mode/--exactly-once/--seed and
  # bypasses the PHASE-0 live-run gates (isolation guard / image / config
  # validation). Decided FIRST — if --reap is combined with a run, reap wins.
  if [ "${REAP}" -eq 1 ]; then
    if [ "${PLAN_ONLY}" -eq 1 ]; then
      print_reap_plan
      exit 0
    fi
    command -v kubectl >/dev/null 2>&1 || die "kubectl required"
    run_reap
    exit $?
  fi

  validate_flags
  resolve_run_shape

  if [ "${PLAN_ONLY}" -eq 1 ]; then
    print_plan
    exit 0
  fi

  # ---- live execution from here ----
  command -v kubectl >/dev/null 2>&1 || die "kubectl required"
  command -v envsubst >/dev/null 2>&1 || die "envsubst required (gettext)"

  CHAOS_ID="${CHAOS_ID_OVERRIDE:-$(date -u +%Y%m%d)-$$}"
  ROUNDS_FILE="${OUT_DIR}/rounds-${CHAOS_ID}.jsonl"
  ORACLE_FILE="${OUT_DIR}/oracle-${CHAOS_ID}.json"
  mkdir -p "${OUT_DIR}"
  : > "${ROUNDS_FILE}"
  log "chaos_id=${CHAOS_ID} mode=${MODE} chaos_mode=${CHAOS_MODE} delivery=${DELIVERY_MODE} seed=${SEED:-<none>} rounds=${ROUNDS}"

  # PHASE 0 — pre-flight gates BEFORE any cluster mutation: isolation guard
  # first (refuse if a pair is active), then image validation (digest law), then
  # config validation. NONE of these mutate the cluster.
  isolation_guard
  phase_validate_images
  phase_validate_config

  trap cleanup_trap EXIT INT TERM

  # ---- live phases ----
  phase_scale_up
  phase_ch_cluster
  phase_topics
  phase_chaos_preload
  phase_poller_host
  phase_connect
  _write_probe_scripts

  if [ "${MODE}" = "smoke" ]; then
    run_smoke
  else
    run_monkey
  fi

  phase_fence
  phase_quiescence

  local oracle_rc=0
  phase_oracle || oracle_rc=$?
  local exit_code
  case "${oracle_rc}" in
    0) exit_code=0 ;;   # PASS
    1) exit_code=1 ;;   # MISMATCH
    *) exit_code=3 ;;   # CHECK_ERROR / fault-not-observed / unverified
  esac
  write_result_artifact "${RUN_OUTCOME}" "quiesced"

  teardown_chaos
  CLEANUP_DONE=1
  trap - EXIT INT TERM
  log "chaos run complete: chaos_id=${CHAOS_ID} outcome=${RUN_OUTCOME} exit=${exit_code}"
  exit "${exit_code}"
}

# The absolute path to THIS script, for the window racer's --inject-once re-exec.
SELF="${SCRIPT_DIR}/$(basename "${BASH_SOURCE[0]:-$0}")"

# Internal re-exec used by the window racer (--kill-cmd): inject ONE fault named
# by CHAOS_INJECT, writing its evidence JSON to CHAOS_EVIDENCE_OUT. All libs are
# already sourced at file scope, so this path needs only the injector.
if [ "${1:-}" = "--inject-once" ]; then
  _ev="$(_inject_fault "${CHAOS_INJECT:?CHAOS_INJECT required for --inject-once}")" || true
  if [ -n "${CHAOS_EVIDENCE_OUT:-}" ]; then
    printf '%s\n' "${_ev}" > "${CHAOS_EVIDENCE_OUT}"
  else
    printf '%s\n' "${_ev}"
  fi
  exit 0
fi

# Run main() only when executed directly, not when sourced (tests source this
# file to exercise validate_image_ref / print_plan helpers in isolation).
if [ "${BASH_SOURCE[0]:-$0}" = "$0" ]; then
  main "$@"
fi
