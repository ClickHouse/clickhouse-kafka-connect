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
# lib_faults.sh — chaos fault injectors (T9, spec §3.1/§3.4/§3.7).
#
# Sourced by chaos_run.sh (T11) AFTER lib_bench.sh (log/warn/die, NS, POLLER_POD,
# CONNECT_REST, CONNECTOR_NAME, BROKER_POD, connect_pod, broker_topic_row_count)
# and chaos/lib_ch_cluster.sh (T8: resolve_keeper_leader).
#
# One inject_<fault> per primitive (C1..C5 + quorum-loss). Each injector:
#   * is self-cleaning via a nested RETURN trap (transient state removed on any
#     exit path),
#   * emits a fault-took-effect evidence JSON fragment (IC-4 `evidence`,
#     {"kind":..,"detail":..}) as its ONLY stdout — all logging goes to stderr,
#   * is driven entirely by kubectl / the Connect REST API — ZERO cloud API.
#
# INVARIANT (matches lib_bench.sh): sourcing this file has NO top-level side
# effect beyond variable defaults — it NEVER runs a kubectl call or injects a
# fault. `set -uo pipefail` compatible.
# =============================================================================

# --------------------------------------------------------------------------- #
# Overridable constants (IC-2 identities; all `${VAR:-default}` for `set -u`).
# --------------------------------------------------------------------------- #
CH_STS="${CH_STS:-ch-chaos}"
CH_POD_SELECTOR="${CH_POD_SELECTOR:-app=ch-chaos}"
CH_REPLICAS="${CH_REPLICAS:-3}"
# A CH pod to run read-only `clickhouse-client` queries against (query_log,
# stall probe). Any ready replica works; ch-chaos-0 by default.
CH_QUERY_POD="${CH_QUERY_POD:-ch-chaos-0}"

# Bounded waits — overridable so the offline tests run without real sleeps.
FAULT_RECREATE_TIMEOUT="${FAULT_RECREATE_TIMEOUT:-180}"
FAULT_POLL_INTERVAL="${FAULT_POLL_INTERVAL:-3}"
CH_RECREATE_TIMEOUT="${CH_RECREATE_TIMEOUT:-300}"
# §3.7 quorum-loss hold: keep quorum down this long before awaiting restore.
QUORUM_LOSS_HOLD="${QUORUM_LOSS_HOLD:-30}"

# Distinct non-zero return for a cap/gate REFUSAL (vs a hard failure) so the
# monkey loop can tell "declined by invariant" from "broke".
FAULT_REFUSED=3

# --------------------------------------------------------------------------- #
# Evidence emitter (IC-4). Prints a compact one-line JSON object on stdout;
# python3 does the escaping so a detail string can contain arbitrary chars.
# --------------------------------------------------------------------------- #
_fault_emit_evidence() {
  local kind="$1" detail="$2"
  python3 - "$kind" "$detail" <<'PY'
import json, sys
print(json.dumps({"kind": sys.argv[1], "detail": sys.argv[2]}))
PY
}

# Nested-trap cleanup: remove transient files and (best-effort) record that the
# trap fired so the harness/tests can observe self-cleaning.
#
# A bash RETURN trap set inside a function is GLOBAL: besides the injector's own
# return it ALSO fires when an ENCLOSING caller returns (e.g. C5 -> C4). In that
# outer scope the injector's `local wk` is unbound, so the trap passes it as
# "${wk:-}" (empty) and this cleanup no-ops on the empty path — it acts and logs
# exactly once, for the injector that actually created the temp file.
_fault_cleanup() {
  trap - RETURN
  local f had=0
  for f in "$@"; do
    [ -n "${f}" ] || continue
    had=1
    rm -f "${f}" 2>/dev/null || true
  done
  [ "${had}" = "1" ] || return 0
  if [ -n "${FAULT_CLEANUP_LOG:-}" ]; then
    echo "cleaned $*" >> "${FAULT_CLEANUP_LOG}"
  fi
  return 0
}

# Echo a pod's metadata.uid (empty if the pod is gone).
_pod_uid() {
  kubectl -n "${NS}" get pod "$1" -o jsonpath='{.metadata.uid}' 2>/dev/null
}

# Block until <pod> reports a UID different from <old_uid> (Strimzi/StatefulSet
# recreated it), bounded by FAULT_RECREATE_TIMEOUT. Echoes the observed UID;
# returns non-zero if the timeout elapses with no change.
_await_new_pod_uid() {
  local pod="$1" old_uid="$2" uid deadline
  deadline=$(( $(date +%s) + FAULT_RECREATE_TIMEOUT ))
  while :; do
    uid="$(_pod_uid "${pod}")"
    if [ -n "${uid}" ] && [ "${uid}" != "${old_uid}" ]; then
      echo "${uid}"
      return 0
    fi
    if [ "$(date +%s)" -ge "${deadline}" ]; then
      echo "${uid}"
      return 1
    fi
    sleep "${FAULT_POLL_INTERVAL}"
  done
}

# Return 0 iff exactly CH_REPLICAS CH pods exist AND every one reports Ready.
# This is the injector-side ≤1-CH-down cap (§3.3): a just-killed pod is either
# missing (fewer entries) or NotReady, so both break the equality.
_ch_all_ready() {
  local out n_ready
  out="$(kubectl -n "${NS}" get pods -l "${CH_POD_SELECTOR}" \
          -o jsonpath='{range .items[*]}{.metadata.name}={@.status.conditions[?(@.type=="Ready")].status};{end}' \
          2>/dev/null)" || return 2
  [ -n "${out}" ] || return 1
  case "${out}" in
    *"=False;"*|*"=Unknown;"*|*"=;"*) return 1 ;;
  esac
  n_ready="$(printf '%s' "${out}" | grep -o '=True;' | wc -l | tr -d ' ')"
  [ "${n_ready}" = "${CH_REPLICAS}" ] || return 1
  return 0
}

# Count insert exceptions on the target from system.query_log during the recent
# fault window (evidence for C4/C5). Echoes an integer (0 on any read failure).
_ch_insert_errors() {
  local n
  n="$(kubectl -n "${NS}" exec "${CH_QUERY_POD}" -- \
         clickhouse-client --query \
         "SELECT count() FROM system.query_log WHERE query_kind='Insert' AND exception_code != 0 AND event_time > now() - INTERVAL 5 MINUTE" \
         2>/dev/null)"
  case "${n}" in
    ''|*[!0-9]*) echo "0" ;;
    *) echo "${n}" ;;
  esac
}

# The idx'th connector task state via the in-cluster poller (mirrors
# wait_tasks_running's REST-through-the-poller pattern).
_connector_task_state() {
  local idx="$1"
  kubectl -n "${NS}" exec "${POLLER_POD}" -- python -c "
import requests, sys
try:
    d = requests.get('${CONNECT_REST}/connectors/${CONNECTOR_NAME}/status', timeout=10).json()
    tasks = d.get('tasks', [])
    print(tasks[${idx}]['state'] if len(tasks) > ${idx} else 'UNKNOWN')
except Exception:
    print('UNKNOWN')
" 2>/dev/null || echo "UNKNOWN"
}

# =========================================================================== #
# FAULT INJECTORS (run ONLY when a caller invokes them — sourcing runs none).
# =========================================================================== #

# C1 — connect_pod_kill (§3.1): hard-kill the Connect worker pod; Strimzi
# recreates it. Evidence = the new pod UID observed.
inject_connect_pod_kill() {
  local wk pod old_uid new_uid
  wk="$(mktemp)"
  trap '_fault_cleanup "${wk:-}"' RETURN
  pod="$(connect_pod)"
  [ -n "${pod}" ] || { warn "C1: could not resolve the Connect worker pod"; return 1; }
  old_uid="$(_pod_uid "${pod}")"
  log "C1 connect_pod_kill: hard-killing ${pod} (uid=${old_uid:-?})"
  kubectl -n "${NS}" delete pod "${pod}" --grace-period=0 --force >/dev/null 2>&1 \
    || { warn "C1: delete pod ${pod} failed"; return 1; }
  new_uid="$(_await_new_pod_uid "${pod}" "${old_uid}")" \
    || warn "C1: new pod UID not observed within ${FAULT_RECREATE_TIMEOUT}s"
  _fault_emit_evidence "pod_recreated" \
    "connect pod ${pod} uid ${old_uid:-none} -> ${new_uid:-none}"
}

# C2 — task_restart (§3.1): POST the Connect REST task-restart endpoint through
# the poller pod. Evidence = 2xx acceptance + the task-state signal.
inject_task_restart() {
  local wk idx code state
  idx="${1:-0}"
  wk="$(mktemp)"
  trap '_fault_cleanup "${wk:-}"' RETURN
  log "C2 task_restart: POST /connectors/${CONNECTOR_NAME}/tasks/${idx}/restart via ${POLLER_POD}"
  code="$(kubectl -n "${NS}" exec "${POLLER_POD}" -- python -c "
import requests, sys
try:
    r = requests.post('${CONNECT_REST}/connectors/${CONNECTOR_NAME}/tasks/${idx}/restart', timeout=10)
    print(r.status_code)
except Exception:
    print('000')
" 2>/dev/null || echo '000')"
  case "${code}" in
    2??) : ;;
    *) warn "C2: task-restart POST returned HTTP ${code}"; return 1 ;;
  esac
  state="$(_connector_task_state "${idx}")"
  _fault_emit_evidence "task_restart_accepted" \
    "POST tasks/${idx}/restart -> HTTP ${code}; task${idx} state=${state}"
}

# C3 — broker_pod_kill (§3.1): hard-kill the broker (RF=1 topic briefly
# unavailable; PVC persists). Evidence = broker UID change + a transient
# lag-read failure while the broker is down.
inject_broker_pod_kill() {
  local wk old_uid new_uid lag_read
  wk="$(mktemp)"
  trap '_fault_cleanup "${wk:-}"' RETURN
  old_uid="$(_pod_uid "${BROKER_POD}")"
  log "C3 broker_pod_kill: hard-killing ${BROKER_POD} (uid=${old_uid:-?})"
  kubectl -n "${NS}" delete pod "${BROKER_POD}" --grace-period=0 --force >/dev/null 2>&1 \
    || { warn "C3: delete pod ${BROKER_POD} failed"; return 1; }
  lag_read="ok"
  broker_topic_row_count "${TOPIC}" >/dev/null 2>&1 || lag_read="failed"
  new_uid="$(_await_new_pod_uid "${BROKER_POD}" "${old_uid}")" \
    || warn "C3: broker UID not observed within ${FAULT_RECREATE_TIMEOUT}s"
  _fault_emit_evidence "broker_recreated" \
    "broker ${BROKER_POD} uid ${old_uid:-none} -> ${new_uid:-none}; lag_read=${lag_read}"
}

# C4 — ch_node_kill (§3.1): kill a random self-hosted CH replica. REFUSES while
# any CH pod is NotReady — the ≤1-CH cap lives here (§3.3), not only in the
# loop. Evidence = pod recreated + insert errors from the target query_log.
inject_ch_node_kill() {
  local wk target old_uid new_uid ins_err
  wk="$(mktemp)"
  trap '_fault_cleanup "${wk:-}"' RETURN
  if ! _ch_all_ready; then
    warn "C4 REFUSED: a CH pod is NotReady — the ≤1-CH-down cap forbids a second CH kill (§3.3)"
    return "${FAULT_REFUSED}"
  fi
  target="${1:-${CH_KILL_TARGET:-}}"
  [ -n "${target}" ] || target="ch-chaos-$(( RANDOM % CH_REPLICAS ))"
  old_uid="$(_pod_uid "${target}")"
  log "C4 ch_node_kill: killing ${target} (uid=${old_uid:-?})"
  kubectl -n "${NS}" delete pod "${target}" --grace-period=0 --force >/dev/null 2>&1 \
    || { warn "C4: delete pod ${target} failed"; return 1; }
  new_uid="$(_await_new_pod_uid "${target}" "${old_uid}")" \
    || warn "C4: ${target} UID not observed within ${FAULT_RECREATE_TIMEOUT}s"
  ins_err="$(_ch_insert_errors)"
  _fault_emit_evidence "pod_recreated" \
    "ch pod ${target} uid ${old_uid:-none} -> ${new_uid:-none}; insert_errors=${ins_err}"
}

# C5 — ch_keeper_leader_kill (§3.1, #771 "the Keeper-hosting node"): resolve the
# current keeper leader (T8's resolve_keeper_leader) then take the C4 path on it.
inject_ch_keeper_leader_kill() {
  local leader
  declare -F resolve_keeper_leader >/dev/null 2>&1 \
    || die "C5: resolve_keeper_leader is undefined — source chaos/lib_ch_cluster.sh (T8) first"
  leader="$(resolve_keeper_leader)" \
    || { warn "C5: resolve_keeper_leader failed"; return 1; }
  [ -n "${leader}" ] || { warn "C5: keeper leader unresolved (empty)"; return 1; }
  log "C5 ch_keeper_leader_kill: leader=${leader} -> C4 path"
  inject_ch_node_kill "${leader}"
}

# quorum_loss (§3.7): kill 2 of the 3 keeper (CH) pods AT ONCE, deliberately
# breaking the 3-node quorum and OVERRIDING the ≤1-CH cap — ONLY under the
# explicit --quorum-loss gate (QUORUM_LOSS=1). Holds, then awaits STS restore,
# and records whether the stall was observed while quorum was down.
inject_quorum_loss() {
  if [ "${QUORUM_LOSS:-0}" != "1" ]; then
    die "inject_quorum_loss REFUSED: requires the explicit --quorum-loss gate (QUORUM_LOSS=1); it overrides the ≤1-CH cap and must never fire in the default sweep (§3.7)"
  fi
  local wk p1 p2 stall restored
  wk="$(mktemp)"
  trap '_fault_cleanup "${wk:-}"' RETURN
  # Default target = the first two replicas; overridable for replay/tests.
  read -r p1 p2 <<< "${QUORUM_KILL_PODS:-ch-chaos-0 ch-chaos-1}"
  log "quorum_loss: killing ${p1} AND ${p2} at once (breaking quorum, cap overridden)"
  kubectl -n "${NS}" delete pod "${p1}" "${p2}" --grace-period=0 --force >/dev/null 2>&1 \
    || { warn "quorum_loss: delete of ${p1}/${p2} failed"; return 1; }
  # Observe the stall: an insert against the target must FAIL while quorum is
  # down (exactly-once cannot write KeeperMap state). Non-zero rows_error => stall.
  stall="not_observed"
  if [ "$(_ch_insert_errors)" != "0" ]; then
    stall="observed"
  fi
  [ "${QUORUM_LOSS_HOLD}" = "0" ] || sleep "${QUORUM_LOSS_HOLD}"
  restored="no"
  if _await_ch_restore; then
    restored="yes"
  else
    warn "quorum_loss: CH quorum did not restore within ${CH_RECREATE_TIMEOUT}s"
  fi
  _fault_emit_evidence "quorum_stall" \
    "killed ${p1},${p2}; stall=${stall}; restored=${restored}"
}

# Await the StatefulSet restoring all CH replicas Ready (quorum reformed).
_await_ch_restore() {
  local deadline
  deadline=$(( $(date +%s) + CH_RECREATE_TIMEOUT ))
  while :; do
    _ch_all_ready && return 0
    [ "$(date +%s)" -ge "${deadline}" ] && return 1
    sleep "${FAULT_POLL_INTERVAL}"
  done
}
