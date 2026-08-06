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
# lib_ch_cluster.sh — self-hosted ClickHouse-cluster lifecycle (spec §2, §8; T8).
#
# The chaos target is an in-cluster CH cluster WE own and can kill pods of (no
# Cloud API). This library deploys/tears it down, drives its keeper quorum, and
# manages the chaos target objects (the hits_chaos ReplicatedMergeTree + the
# exactly-once KeeperMap state). Sourced by chaos_run.sh (T11); its
# resolve_keeper_leader is the C5 fault target consumed by lib_faults.sh (T9).
#
# Resource names ARE the IC-2 contract (chaos/ch-cluster.yaml): StatefulSet
# ch-chaos (pods ch-chaos-{0,1,2}), Services ch-chaos / ch-chaos-headless,
# ConfigMap ch-chaos-config, keeper client port 9181. The CH image is the
# IC-8-scoped exception to the digest law: version-selected via CHAOS_CH_VERSION.
#
# INVARIANT (mirrors lib_bench.sh IC-1): sourcing this file has NO top-level side
# effect beyond variable defaults — it NEVER executes a phase. Every constant is
# `${VAR:-default}` so a bare `set -u` shell can source it.
# =============================================================================

# Locate this file's own dir BEFORE sourcing lib_bench.sh (which resets
# SCRIPT_DIR to ITS own dir); chaos/ is a sibling of orchestration/.
CHAOS_LIB_DIR="${CHAOS_LIB_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)}"

# lib_bench.sh (IC-1): log/warn/die, the digest-pin family, broker helpers, the
# Connect deploy family, etc. Sourcing it is side-effect-free by contract.
# shellcheck source=../orchestration/lib_bench.sh disable=SC1091
source "${CHAOS_LIB_DIR}/../orchestration/lib_bench.sh"

# --------------------------------------------------------------------------- #
# Overridable resource-name + identity constants (IC-2 / IC-3). NS comes from
# lib_bench.sh (default kafka-bench).
# --------------------------------------------------------------------------- #
CH_CLUSTER_YAML="${CH_CLUSTER_YAML:-${CHAOS_LIB_DIR}/ch-cluster.yaml}"
CH_STS="${CH_STS:-ch-chaos}"
CH_SVC="${CH_SVC:-ch-chaos}"
CH_SVC_HEADLESS="${CH_SVC_HEADLESS:-ch-chaos-headless}"
CH_CONFIGMAP="${CH_CONFIGMAP:-ch-chaos-config}"
CH_APP_LABEL="${CH_APP_LABEL:-app=ch-chaos}"
CH_REPLICAS="${CH_REPLICAS:-3}"
KEEPER_CLIENT_PORT="${KEEPER_CLIENT_PORT:-9181}"

# Chaos target identity (IC-3). CH_DATABASE/CH_TABLE = clickbench.hits_chaos;
# ZK_DATABASE = the exactly-once KeeperMap state table (KeeperStateProvider
# names the table after zkDatabase, KeeperStateProvider.java:74-92).
CH_DATABASE="${CH_DATABASE:-clickbench}"
CH_TABLE="${CH_TABLE:-hits_chaos}"
ZK_DATABASE="${ZK_DATABASE:-connect_state_chaos}"
# ConfigMap key holding the hits_chaos DDL (dot escaped for jsonpath).
CH_DDL_KEY="${CH_DDL_KEY:-hits_chaos\.sql}"
# KeeperMap CREATE/DROP smoke probe (acceptance §9.2) — distinct name/path so it
# never collides with the exactly-once state table.
KEEPER_SMOKE_TABLE="${KEEPER_SMOKE_TABLE:-chaos_keeper_smoke}"
KEEPER_SMOKE_PATH="${KEEPER_SMOKE_PATH:-/chaos_keeper_smoke}"

# CH image (IC-8): version-selected by design, NOT digest-pinned (the scoped
# exception to the digest law). The digest is read back post-deploy from the
# running pods (read_ch_image_digest) and recorded as deployed truth.
CHAOS_CH_VERSION="${CHAOS_CH_VERSION:-latest}"

# In-cluster access (IC-2): plaintext 8123, no TLS.
TARGET_CH_HOST="${TARGET_CH_HOST:-${CH_SVC}.${NS}.svc}"
TARGET_CH_PORT="${TARGET_CH_PORT:-8123}"

# Wait bounds (overridable; tests set these small).
CH_READY_TIMEOUT="${CH_READY_TIMEOUT:-600}"
CH_QUORUM_TIMEOUT="${CH_QUORUM_TIMEOUT:-300}"
CH_REPLICA_SYNC_TIMEOUT="${CH_REPLICA_SYNC_TIMEOUT:-300}"
CH_POLL_INTERVAL="${CH_POLL_INTERVAL:-5}"

# --------------------------------------------------------------------------- #
# Small helpers.
# --------------------------------------------------------------------------- #
# Pod name for the i-th replica (ch-chaos-<i>).
_ch_pod() { printf '%s-%s' "${CH_STS}" "$1"; }

# Run a single SQL statement against the target via clickhouse-client in a CH
# pod (in-cluster, plaintext). $2 overrides the pod (default replica 0).
ch_query() {
  local sql="$1" pod="${2:-${CH_EXEC_POD:-$(_ch_pod 0)}}"
  kubectl -n "${NS}" exec "${pod}" -- clickhouse-client --query "${sql}"
}

# ClickHouse-keeper 4-letter-word (4lw) probe over the client port, via a bash
# /dev/tcp socket inside the pod (the image entrypoint is bash; no nc needed).
# Echoes the raw 4lw response on stdout.
_keeper_4lw() {
  local pod="$1" word="$2" cmd
  cmd="exec 3<>/dev/tcp/127.0.0.1/${KEEPER_CLIENT_PORT}; printf '${word}\n' >&3; cat <&3"
  kubectl -n "${NS}" exec "${pod}" -- bash -c "${cmd}" 2>/dev/null
}

# The pod's keeper role from `mntr` (zk_server_state: leader|follower|observer).
_keeper_state() {
  _keeper_4lw "$1" mntr | sed -n 's/^zk_server_state[[:space:]]*//p' | head -1
}

# True when the 3-node keeper quorum is formed: every pod answers ruok=imok and
# exactly 1 reports leader with the rest followers (4lw mntr/ruok).
_keeper_quorum_ok() {
  local i pod state leaders=0 followers=0
  for (( i=0; i<CH_REPLICAS; i++ )); do
    pod="$(_ch_pod "${i}")"
    [ "$(_keeper_ruok "${pod}")" = "imok" ] || return 1
    state="$(_keeper_state "${pod}")"
    case "${state}" in
      leader)   leaders=$(( leaders + 1 )) ;;
      follower) followers=$(( followers + 1 )) ;;
      *)        return 1 ;;
    esac
  done
  [ "${leaders}" -eq 1 ] && [ "${followers}" -eq $(( CH_REPLICAS - 1 )) ]
}

_keeper_ruok() { _keeper_4lw "$1" ruok; }

# --------------------------------------------------------------------------- #
# deploy_ch_cluster — render (IC-8) + apply (IC-2), then wait for BOTH 3/3
# readiness AND keeper quorum before returning (spec §2, §9.2).
# --------------------------------------------------------------------------- #
deploy_ch_cluster() {
  export CHAOS_CH_IMAGE="clickhouse/clickhouse-server:${CHAOS_CH_VERSION}"
  log "deploy_ch_cluster: applying ${CH_CLUSTER_YAML} (image=${CHAOS_CH_IMAGE})"
  # IC-8: only ${CHAOS_CH_IMAGE} is substituted; the container command's $( )/
  # $(( )) survive (ch-cluster.yaml comment). apply is server-side idempotent.
  envsubst '${CHAOS_CH_IMAGE}' < "${CH_CLUSTER_YAML}" | kubectl apply -f - \
    || die "deploy_ch_cluster: kubectl apply of ${CH_CLUSTER_YAML} failed"

  # (1) readiness gate: all replicas Ready (the /ping readinessProbe).
  log "deploy_ch_cluster: waiting for ${CH_REPLICAS}/${CH_REPLICAS} ${CH_STS} pods Ready (timeout ${CH_READY_TIMEOUT}s)"
  local deadline=$(( $(date +%s) + CH_READY_TIMEOUT ))
  while :; do
    local ready
    ready="$(kubectl -n "${NS}" get statefulset "${CH_STS}" \
               -o jsonpath='{.status.readyReplicas}' 2>/dev/null)"
    if [ "${ready:-0}" = "${CH_REPLICAS}" ]; then
      log "  ${CH_STS} ${ready}/${CH_REPLICAS} Ready"
      break
    fi
    [ "$(date +%s)" -ge "${deadline}" ] \
      && die "deploy_ch_cluster: ${CH_STS} did not reach ${CH_REPLICAS}/${CH_REPLICAS} Ready within ${CH_READY_TIMEOUT}s (last readyReplicas=${ready:-0})"
    sleep "${CH_POLL_INTERVAL}"
  done

  # (2) quorum gate: the embedded 3-node keeper forms (1 leader + N-1 followers)
  # via 4lw mntr/ruok — KeeperMap state (exactly-once) needs a live quorum (§2).
  log "deploy_ch_cluster: waiting for keeper quorum (1 leader + $(( CH_REPLICAS - 1 )) followers, timeout ${CH_QUORUM_TIMEOUT}s)"
  deadline=$(( $(date +%s) + CH_QUORUM_TIMEOUT ))
  while :; do
    if _keeper_quorum_ok; then
      log "  keeper quorum formed"
      break
    fi
    [ "$(date +%s)" -ge "${deadline}" ] \
      && die "deploy_ch_cluster: keeper quorum (1 leader + $(( CH_REPLICAS - 1 )) followers) did not form within ${CH_QUORUM_TIMEOUT}s"
    sleep "${CH_POLL_INTERVAL}"
  done

  export TARGET_CH_HOST TARGET_CH_PORT
  log "deploy_ch_cluster: ready — TARGET_CH_HOST=${TARGET_CH_HOST} TARGET_CH_PORT=${TARGET_CH_PORT}"
}

# --------------------------------------------------------------------------- #
# teardown_ch_cluster — delete STS/Services/ConfigMap/PVCs and VERIFY the STS is
# gone (§4 rule 4 / §9.8: no data/cost leak into the next run). PVCs are NOT
# reclaimed by the STS delete, so they are deleted explicitly.
# --------------------------------------------------------------------------- #
teardown_ch_cluster() {
  log "teardown_ch_cluster: deleting STS/Services/ConfigMap/PVCs for ${CH_STS}"
  kubectl -n "${NS}" delete statefulset "${CH_STS}" --ignore-not-found --wait=true --timeout=300s 2>/dev/null || true
  kubectl -n "${NS}" delete service "${CH_SVC}" "${CH_SVC_HEADLESS}" --ignore-not-found --wait=true --timeout=120s 2>/dev/null || true
  kubectl -n "${NS}" delete configmap "${CH_CONFIGMAP}" --ignore-not-found 2>/dev/null || true
  kubectl -n "${NS}" delete pvc -l "${CH_APP_LABEL}" --ignore-not-found --wait=true --timeout=120s 2>/dev/null || true

  # VERIFY gone — the STS must not survive (a surviving STS keeps pods alive and
  # leaks cost; §9.8 requires the teardown to confirm it, not assume it).
  local sts_left pvc_left
  sts_left="$(kubectl -n "${NS}" get statefulset "${CH_STS}" --ignore-not-found -o name 2>/dev/null)"
  [ -z "${sts_left}" ] \
    || die "teardown_ch_cluster: StatefulSet ${CH_STS} still present after delete (${sts_left}) — refusing to report a clean teardown"
  pvc_left="$(kubectl -n "${NS}" get pvc -l "${CH_APP_LABEL}" --ignore-not-found -o name 2>/dev/null)"
  [ -z "${pvc_left}" ] \
    || warn "teardown_ch_cluster: PVC(s) still present after delete: ${pvc_left} (data/cost leak — check reclaim policy)"
  log "teardown_ch_cluster: verified ${CH_STS} torn down"
}

# --------------------------------------------------------------------------- #
# resolve_keeper_leader — echo the pod whose `mntr` reports zk_server_state
# leader (C5's kill target, spec §3.1). Errors on 0 (no leader / quorum lost)
# or >1 (split-brain read) leaders. THIS NAME/BEHAVIOR is the T9 interface.
# --------------------------------------------------------------------------- #
resolve_keeper_leader() {
  local i pod state leader="" n=0
  for (( i=0; i<CH_REPLICAS; i++ )); do
    pod="$(_ch_pod "${i}")"
    state="$(_keeper_state "${pod}")"
    if [ "${state}" = "leader" ]; then
      leader="${pod}"
      n=$(( n + 1 ))
    fi
  done
  [ "${n}" -eq 1 ] \
    || die "resolve_keeper_leader: expected exactly 1 keeper leader among ${CH_REPLICAS} pods, found ${n} (0 => no leader / quorum lost; >1 => split-brain read)"
  printf '%s\n' "${leader}"
}

# --------------------------------------------------------------------------- #
# keeper_map_reset — drop the exactly-once KeeperMap state table before each
# exactly-once run (§2.4 stale-state trap). KeeperStateProvider.init() names the
# table after zkDatabase and recreates it lazily (KeeperStateProvider.java:74-92,
# 141), so a plain DROP is safe.
# --------------------------------------------------------------------------- #
keeper_map_reset() {
  log "keeper_map_reset: DROP TABLE IF EXISTS \`${ZK_DATABASE}\` (exactly-once KeeperMap state; recreated lazily by KeeperStateProvider.init)"
  ch_query "DROP TABLE IF EXISTS \`${ZK_DATABASE}\`" \
    || die "keeper_map_reset: failed to drop KeeperMap state table \`${ZK_DATABASE}\` on the target"
}

# --------------------------------------------------------------------------- #
# keeper_map_smoke — prove keeper_map_path_prefix is active and the quorum
# accepts a KeeperMap CREATE/DROP (acceptance §9.2). Mirrors the connector's
# table shape (KeeperStateProvider.init) but on a throwaway name/path.
# --------------------------------------------------------------------------- #
keeper_map_smoke() {
  log "keeper_map_smoke: CREATE/DROP a KeeperMap table (acceptance §9.2 probe)"
  ch_query "CREATE TABLE IF NOT EXISTS \`${KEEPER_SMOKE_TABLE}\` (\`key\` String, v Int64) ENGINE=KeeperMap('${KEEPER_SMOKE_PATH}') PRIMARY KEY \`key\`" \
    || die "keeper_map_smoke: KeeperMap CREATE failed — keeper_map_path_prefix not active or quorum down"
  ch_query "DROP TABLE IF EXISTS \`${KEEPER_SMOKE_TABLE}\`" \
    || die "keeper_map_smoke: KeeperMap DROP failed"
  log "keeper_map_smoke: OK"
}

# --------------------------------------------------------------------------- #
# apply_chaos_ddl — create clickbench.hits_chaos (ReplicatedMergeTree) from the
# ch-chaos-config ConfigMap DDL (spec §2 / T2). The DDL is intentionally plain
# (no ON CLUSTER), so it is applied on EACH replica — CREATE ... IF NOT EXISTS is
# idempotent and all three register with the same zk path.
# --------------------------------------------------------------------------- #
apply_chaos_ddl() {
  log "apply_chaos_ddl: creating ${CH_DATABASE}.${CH_TABLE} from ConfigMap ${CH_CONFIGMAP}"
  local ddl
  ddl="$(kubectl -n "${NS}" get configmap "${CH_CONFIGMAP}" \
           -o jsonpath="{.data.${CH_DDL_KEY}}" 2>/dev/null)" \
    || die "apply_chaos_ddl: could not read ${CH_DDL_KEY} from ConfigMap ${CH_CONFIGMAP}"
  [ -n "${ddl}" ] \
    || die "apply_chaos_ddl: ${CH_DDL_KEY} in ConfigMap ${CH_CONFIGMAP} is empty (DDL not carried into the manifest?)"
  local i pod
  for (( i=0; i<CH_REPLICAS; i++ )); do
    pod="$(_ch_pod "${i}")"
    printf '%s\n' "${ddl}" | kubectl -n "${NS}" exec -i "${pod}" -- clickhouse-client -n \
      || die "apply_chaos_ddl: DDL apply failed on ${pod}"
  done
  log "apply_chaos_ddl: ${CH_DATABASE}.${CH_TABLE} created on ${CH_REPLICAS} replica(s)"
}

# --------------------------------------------------------------------------- #
# await_replica_sync — force + await replication catch-up so the oracle reads a
# caught-up replica (spec §5 replica-read caveat). SYSTEM SYNC REPLICA on each
# replica, then poll system.replicas until the queue drains, bounded by
# CH_REPLICA_SYNC_TIMEOUT.
# --------------------------------------------------------------------------- #
await_replica_sync() {
  log "await_replica_sync: SYSTEM SYNC REPLICA + system.replicas queue drain for ${CH_DATABASE}.${CH_TABLE}"
  local i pod
  for (( i=0; i<CH_REPLICAS; i++ )); do
    pod="$(_ch_pod "${i}")"
    ch_query "SYSTEM SYNC REPLICA \`${CH_DATABASE}\`.\`${CH_TABLE}\`" "${pod}" \
      || warn "await_replica_sync: SYSTEM SYNC REPLICA returned non-zero on ${pod} (continuing to poll the queue)"
  done
  local deadline=$(( $(date +%s) + CH_REPLICA_SYNC_TIMEOUT ))
  while :; do
    local q
    q="$(ch_query "SELECT sum(queue_size) FROM system.replicas WHERE database='${CH_DATABASE}' AND table='${CH_TABLE}'")" \
      || die "await_replica_sync: could not read system.replicas on the target"
    q="${q//[[:space:]]/}"
    if [ "${q:-0}" = "0" ]; then
      log "  replication queue empty — replicas caught up"
      return 0
    fi
    [ "$(date +%s)" -ge "${deadline}" ] \
      && die "await_replica_sync: replication queue did not drain within ${CH_REPLICA_SYNC_TIMEOUT}s (last sum(queue_size)=${q})"
    sleep "${CH_POLL_INTERVAL}"
  done
}

# --------------------------------------------------------------------------- #
# read_ch_image_digest — IC-8 read-back: the RESOLVED CH image digest from a
# running pod's container status (deployed truth, not template intent). Echoes
# the digest and exports CH_IMAGE_DIGEST for the artifact writer.
# --------------------------------------------------------------------------- #
read_ch_image_digest() {
  local pod="${1:-$(_ch_pod 0)}"
  local digest
  digest="$(kubectl -n "${NS}" get pod "${pod}" \
              -o jsonpath='{.status.containerStatuses[0].imageID}' 2>/dev/null)"
  [ -n "${digest}" ] \
    || die "read_ch_image_digest: empty imageID for ${pod} (pod not running / no container status yet)"
  export CH_IMAGE_DIGEST="${digest}"
  printf '%s\n' "${digest}"
}
