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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
# Shared, run-mode-agnostic helpers + overridable constants (spec §8, IC-1):
# logging (log/warn/die), the digest-pin family, the preload/poller/Connect/
# scale phase functions, build_runtime_json, fail_run, and the path/identity/
# timeout constants (SCRIPT_DIR, E2E_DIR, CAPTURE_DIR, POLLER_DIR, PRODUCER_DIR,
# INFRA_DIR, TEMPLATES_DIR, NS, TOPIC, CONNECT_NAME, CONNECTOR_NAME, CONNECT_REST,
# POLLER_POD, BROKER_POD, EXPECTED_*, CONNECT_HEAP, POLL_*, ARTIFACT_DIR,
# SCALE_UP_NODES). Sourced FIRST so every pair-specific function/constant below
# can rely on it. Behavior is byte-identical to before the extraction:
# BENCH_LOG_PREFIX defaults to `run_pair`, and every constant keeps its pair
# default. chaos_run.sh sources the same file with retargeted names.
# shellcheck source=/dev/null
. "${SCRIPT_DIR}/lib_bench.sh"

# us-east-2 on-demand pricing for the pair-cost calc (phase_pair_cost /
# emit_run_cost.py). Kept here (not an AWS Pricing API call) per plan "no new
# AWS calls"; bump deliberately when prices drift. PAIR-ONLY (§10.2): the chaos
# run has no pair-cost phase, so these live in run_pair.sh, not lib_bench.sh.
# SCALE_UP_NODES is a shared scale constant in lib_bench.sh (phase_scale_up needs
# it; keep in sync with infra/env.sh SCALE_UP_NODES + cluster.yaml bench-ng
# maxSize); the phase_pair_cost calc below charges all ${SCALE_UP_NODES} honestly.
# The pair runs on TWO instance types (2026-07-08 rebuild): SCALE_UP_NODES
# x m6i.large (broker/registry/producer/poller) + CONNECT_NODES x m6i.xlarge
# (the dedicated, CPU-bound Connect worker). Both node-hour terms are billed.
M6I_LARGE_USD_PER_HR="${M6I_LARGE_USD_PER_HR:-0.096}"    # us-east-2 on-demand
M6I_XLARGE_USD_PER_HR="${M6I_XLARGE_USD_PER_HR:-0.192}"  # us-east-2 on-demand (2x large)
EBS_GP3_USD_PER_GB_MO="${EBS_GP3_USD_PER_GB_MO:-0.08}"
BROKER_EBS_GB="${BROKER_EBS_GB:-70}"
# Dedicated Connect nodegroup count (baseline 1 m6i.xlarge; keep in sync with
# infra/env.sh CONNECT_NODES). Scale-out is the #37 sweep's variable.
CONNECT_NODES="${CONNECT_NODES:-1}"

# Set to 1 by ingest_failed() (contract §1.3 outcome amendment): the pair
# CONTINUES after a failed-class run (the contract wants the data from every
# remaining run) but main() exits non-zero at the end for CI visibility.
PAIR_HAD_FAILURE=0

# Set to 1 by a NON-FATAL anomaly that must NOT fail the pair (pair-4 fix): a
# tier-1 integrity CHECK_ERROR (exit 3) — the redundant confirmation checker
# could not verify, but the capture-computed integrity_ok is authoritative and
# already recorded. The pair exit stays GREEN; main() logs the warning summary.
PAIR_HAD_WARNING=0

# Space-separated run_ids whose perf.runs row ACTUALLY landed (appended by
# capture_and_record after a successful insert_run_record). phase_pair_cost
# consults this before emitting run_cost_usd: a metric row for a run_id with
# no runs row would violate the contract §1.2 join rule (metrics inherit their
# tagging solely through the runs-row join).
RECORDED_RUNS=""

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
failure policy  : PRE-drain-start failures (scale-up/topic/producer/deploy/
                  tasks-never-RUNNING) -> hard abort + rollback (nothing to capture).
                  POST-drain-start ingest failures (poller timeout = lag never 0,
                  poller hard failure, finalize failure) -> run FULLY captured +
                  exported with runtime['outcome']='failed' (+ integrity_unverified
                  on tier 1; contract §1.3, no survivorship bias); the PAIR
                  CONTINUES to the remaining runs; the job exits non-zero at the
                  very end for CI visibility. Success rows OMIT the outcome key.

PHASE 1  scale up          eksctl node group 0->${SCALE_UP_NODES}; Kafka CR + Schema Registry Ready
PHASE 2  pre-load          create topic (${EXPECTED_PARTITIONS} partitions RF1)
                           -> ASSERT broker-reported partition count==${EXPECTED_PARTITIONS} (kafka-topics.sh --describe)
                           -> render + apply INDEXED producer Job (completions==parallelism==SHARD_COUNT=${SHARD_COUNT:-3}
                              shard pods; each produces a disjoint stride-N slice of the parquet files
                              — parallel preload, ~5-6min vs ~20min; UNMEASURED so no instrument impact)
                           -> watch BOTH terminal conditions (Complete|Failed; Failed dies fast +
                              dumps last logs — backoffLimit=0: first pod failure is terminal)
                           -> derive rows_expected from BROKER offsets (kafka-get-offsets.sh in broker
                              pod: Σ latest−earliest per partition; producer-count-agnostic, authoritative)
                           -> best-effort cross-check: Σ per-shard rows_sent (per completion-index pod log)
                              == broker offsets else FAIL (dup/lost records); parse problems only WARN
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
      export (gated) ; integrity check LAST — redundant confirmation over capture-
       computed integrity_ok (authoritative). exit 0 OK -> proceed; exit 1 MISMATCH
       -> run FAILS (row stands, mismatch detectable via integrity_ok=0); exit 3
       CHECK_ERROR (could not verify: infra/timeout) -> WARN, pair stays green
      delete connector + consumer group
    delete KafkaConnect CR + CH-creds Secret
PHASE 3c pair cost         emit run_cost_usd once, end-of-pair window, on (${order[0]},t1)
PHASE 4  teardown topic    delete KafkaTopic CR   (if: always in CI)
PHASE 5  scale down        scale-down.sh -> node group 0   (if: always in CI)
=== END PHASE PLAN ===
EOF
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
      --insert ) > "${out}" \
    || { warn "poller finalize/insert failed for ${RUN_ID}"; return 1; }
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

# --------------------------------------------------------------------------- #
# CPU acceptance gate (tier 0, principal directive): after the tier-0 drain's
# finalize, compute the worker's integrated CPU share of its CPU LIMIT over the
# drain window and print a loud verdict:
#   PASS                      share < 0.80
#   INSTRUMENT_RESIZE_SUSPECT share >= 0.80  (the instrument may be the
#                                             bottleneck, not the connector)
# LOG-ONLY verdict — the run is NOT auto-flagged (the flag decision is the
# manager's per the quarantine protocol). The share also lands as the runtime
# key kafka_worker_cpu_share_t0 on the tier-0 runs row.
#
# Source: the finalize JSON already integrates the cadvisor CPU counter into
# connect_cpu_seconds_per_Mrows; share = (cpu_per_Mrows x rows/1e6) /
# (drain_seconds x limit_cores). The cadvisor source is WIRED (poller
# prerequisite 2: bench-poller-sa + POD_CADVISOR_URL via the API-server node
# proxy), so the gate is normally SIGHTED. It reports UNAVAILABLE (key absent)
# only if the scrape failed at runtime (node unresolved, proxy 403/empty) or the
# CPU limit is unreadable — tolerated absence, never a run failure.
# Sets/exports KAFKA_WORKER_CPU_SHARE_T0 (a ratio of used-to-limit CPU-seconds,
# >=0, 4 decimals, or ""). NOTE: this is NOT bounded to 0..1 — a worker can burn
# more CPU-seconds than its limit x drain (bursting above request up to the
# limit, brief over-limit before throttling, multi-core accounting), so the
# ratio legitimately exceeds 1.0; the >=0.80 gate treats anything at/above the
# threshold (including >1) as INSTRUMENT_RESIZE_SUSPECT.
# M4: the cpu_seconds delta is integrated by the poller over the finalize
# window, which spans the FIRST valid cadvisor sample to the LAST valid sample
# of the tier-0 drain (connect_cpu_seconds_per_Mrows in the finalize JSON) —
# not the wall-clock run_start/run_end. drain_seconds below matches that same
# first->last-valid-sample span, so the ratio's numerator and denominator are
# taken over the identical window.
# --------------------------------------------------------------------------- #
compute_cpu_gate_t0() {
  local fin="${ARTIFACT_DIR}/finalize-${RUN_ID}.json"
  KAFKA_WORKER_CPU_SHARE_T0="$(python3 - "${fin}" "${KAFKA_WORKER_CPU_LIMIT:-}" <<'PY'
import json, sys
try:
    d = json.load(open(sys.argv[1]))
    s = d.get("scalars", {})
    cpu_per_m = s.get("connect_cpu_seconds_per_Mrows")
    drain = s.get("drain_seconds")
    rows = d.get("rows_expected")
    limit = (sys.argv[2] or "").strip()
    def cores(q):  # k8s cpu quantity: "2" | "2000m"
        return float(q[:-1]) / 1000.0 if q.endswith("m") else float(q)
    if not limit or cpu_per_m is None or not drain or not rows or float(drain) <= 0:
        print("")
    else:
        cpu_seconds = float(cpu_per_m) * (float(rows) / 1e6)
        print(f"{cpu_seconds / (float(drain) * cores(limit)):.4f}")
except Exception:
    print("")
PY
)"
  export KAFKA_WORKER_CPU_SHARE_T0
  if [ -z "${KAFKA_WORKER_CPU_SHARE_T0}" ]; then
    warn "CPU GATE (tier 0, ${RUN_ID}): UNAVAILABLE — cadvisor scrape failed at runtime (node unresolved / proxy 403 / empty body) or CPU limit unreadable; kafka_worker_cpu_share_t0 omitted"
    return 0
  fi
  if awk "BEGIN{exit !(${KAFKA_WORKER_CPU_SHARE_T0} >= 0.80)}"; then
    warn "CPU GATE (tier 0, ${RUN_ID}): INSTRUMENT_RESIZE_SUSPECT — worker used ${KAFKA_WORKER_CPU_SHARE_T0} of its CPU limit (${KAFKA_WORKER_CPU_LIMIT:-?}) over the drain (>=80%: the instrument may be the bottleneck). LOG-ONLY — no auto-flag (quarantine protocol: flag decision is the manager's)."
  else
    log "CPU GATE (tier 0, ${RUN_ID}): PASS — worker CPU share of limit = ${KAFKA_WORKER_CPU_SHARE_T0} (<80%)"
  fi
}

# Human token for the ingest-failure log/reason from run_poller_sample's rc
# (or 3 = finalize failed after a clean drain).
ingest_fail_reason() {
  case "$1" in
    2) echo "drain_incomplete_timeout" ;;
    3) echo "finalize_failed" ;;
    *) echo "poller_hard_failure" ;;
  esac
}

# --------------------------------------------------------------------------- #
# Capture-on-failure (contract §1.3 `outcome` amendment): an (arm,tier) run
# whose DRAIN STARTED (poller sampling began) but whose ingest then failed is
# still FULLY captured and exported, marked runtime['outcome']='failed' — no
# survivorship bias. This REPLACES the old rollback-and-die on post-drain-start
# failures. The boundary:
#   * PRE-DRAIN failures (scale-up, topic, producer, poller host, Connect
#     deploy, connector apply, tasks never all RUNNING) keep hard-die +
#     rollback — the drain never began, there is nothing meaningful to capture.
#   * POST-DRAIN-START failures land here: poller timeout (lag never 0),
#     poller hard failure, finalize failure. Design choice on the last one: a
#     run that cannot be FULLY measured (finalize crashed after a clean drain)
#     is also failed-class — a half-metriced "success" row silently entering
#     headlines is worse than a conservatively-failed row.
# What is captured: best-effort client-side scalars from whatever samples were
# copied back (JSONL is flushed per tick), the CH-side capture SQL over the
# actual window (RUN_START -> failure time), the runs row with outcome='failed'
# (+ integrity_unverified on tier 1 — by definition, contract §1.3: a failed
# run makes no integrity claim), and the export. Then the PAIR CONTINUES (the
# other runs are still worth capturing; ratios skip failed-class rows by
# outcome) and main() exits non-zero at the end.
# --------------------------------------------------------------------------- #
ingest_failed() {
  local arm="$1" tier="$2" reason="$3"
  warn "INGEST FAILED for ${RUN_ID} (${reason}) — capture-on-failure: full capture + export, outcome=failed (contract §1.3, no survivorship bias)"
  PAIR_HAD_FAILURE=1
  export OUTCOME="failed"
  # Window end = failure time when the poller never anchored it.
  RUN_END="${RUN_END:-$(date -u +%Y-%m-%dT%H:%M:%S)}"; export RUN_END
  # No settle on a failed run — the window closes at failure time (values kept
  # if the real settle already ran, e.g. finalize_failed after a clean drain).
  export SETTLE_END="${SETTLE_END:-${RUN_END}}"
  export SETTLE_SECONDS="${SETTLE_SECONDS:-0}"
  export SETTLE_TIMED_OUT="${SETTLE_TIMED_OUT:-0}"
  # Best-effort client-side scalars (skip the retry when finalize itself was
  # the failure — it would fail identically).
  if [ "${reason}" != "finalize_failed" ] && [ -s "${SAMPLES_FILE:-/nonexistent}" ]; then
    finalize_and_insert_metrics "${tier}" \
      || warn "finalize on the failed-class run did not land — client-side metrics absent"
  else
    warn "client-side metrics absent for the failed-class run (${reason})"
  fi
  capture_and_record "${arm}" "${tier}" "failed" || warn "failed-class capture incomplete for ${RUN_ID}"
  unset OUTCOME
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
  local arm="$1" tier="$2" mode="${3:-strict}"
  log "  gated capture + run record (arm=${arm}, tier=${tier}, mode=${mode})"

  local integrity_unverified=0
  local f
  for f in 11 12 13 14 15 16 17 18 19 20 22 23; do
    if [ "${f}" = "20" ] && { [ "${tier}" = "0" ] || [ "${mode}" = "failed" ]; }; then
      # tier 0: integrity not applicable (review F7). failed-class: a failed
      # run makes NO integrity claim (contract §1.3 — integrity_unverified by
      # definition, appended below for tier 1); running SQL 20 would compute a
      # meaningless verdict over a truncated drain.
      continue
    fi
    if [ "${f}" = "23" ] && [ "${tier}" != "0" ]; then
      continue   # ch_insert_cpu_share_tier0: tier-0-only parse-watch (contract §2.1)
    fi
    local sqlfile
    sqlfile="$(ls "${E2E_DIR}"/sql/capture/${f}_*.sql 2>/dev/null | head -1)"
    if [ -z "${sqlfile}" ]; then
      if [ "${mode}" = "failed" ]; then
        warn "capture SQL ${f}_* not found — skipping on failed-class run"
        continue
      fi
      fail_run "capture SQL ${f}_* not found"
    fi
    if ! ( cd "${CAPTURE_DIR}" && python3 run_metrics_sql.py "${sqlfile}" ); then
      if [ "${mode}" = "failed" ]; then
        # Capture-on-failure: keep whatever evidence lands. Rolling back a
        # failed-class run over one bad capture file would erase the very
        # evidence the outcome amendment exists to keep.
        warn "capture ${f} failed on a failed-class run — continuing (partial evidence beats none)"
        continue
      fi
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

  # Failed-class tier 1: integrity_unverified BY DEFINITION (contract §1.3 —
  # SQL 20 was skipped above, no integrity claim exists for this run).
  if [ "${mode}" = "failed" ] && [ "${tier}" = "1" ]; then
    append_flag "integrity_unverified"
  fi

  # runs row AFTER metrics (gated); failure -> rollback + abort (README).
  # NOTE (review F6): run_cost_usd is NOT emitted here — it is a whole-pair
  # metric emitted once at end-of-pair (phase_pair_cost).
  local runtime_json
  if ! runtime_json="$(build_runtime_json "${arm}" "${tier}")"; then
    warn "runtime map build failed -> rollback"
    ( cd "${CAPTURE_DIR}" && python3 rollback_run_metrics.py )
    # A runs row cannot land without its runtime map; on the failed-class path
    # this must not kill the rest of the pair (the outcome amendment wants the
    # remaining runs), so roll back this run's metrics and continue.
    [ "${mode}" = "failed" ] && { warn "failed-class run ${RUN_ID} could not be recorded"; return 1; }
    die "runtime map aborted for ${RUN_ID}"
  fi
  # Fold the guards into the runtime map (overseer directive h): flagged and
  # flag_reason are runtime keys (contract §1.3).
  if [ "${FLAGGED:-0}" = "1" ]; then
    runtime_json="$(python3 -c 'import json,sys;d=json.loads(sys.argv[1]);d["flagged"]="1";d["flag_reason"]=sys.argv[2];print(json.dumps(d,sort_keys=True))' "${runtime_json}" "${FLAG_REASON}")"
  fi
  if ! ( cd "${CAPTURE_DIR}" && RUNTIME="${runtime_json}" python3 insert_run_record.py ); then
    warn "insert_run_record failed -> rollback"
    ( cd "${CAPTURE_DIR}" && python3 rollback_run_metrics.py )
    # Same continue-the-pair rule on the failed-class path (no metrics may
    # outlive a missing runs row — rolled back above — but the OTHER runs of
    # the pair are still worth capturing).
    [ "${mode}" = "failed" ] && { warn "failed-class run ${RUN_ID} could not be recorded"; return 1; }
    die "run record aborted for ${RUN_ID}"
  fi
  # The runs row landed — record it so phase_pair_cost can verify its target
  # row exists before attaching run_cost_usd (contract §1.2 join rule).
  RECORDED_RUNS="${RECORDED_RUNS} ${RUN_ID}"

  # export ONLY after runs insert succeeded (gated, README). Failed-class runs
  # are exported too (contract §1.3: fully captured AND exported, marked).
  ( cd "${CAPTURE_DIR}" && python3 export_metrics_to_dwh.py ) || warn "DWH export failed (metrics persisted; export can be retried)"

  # integrity verdict LAST (README, contract §3) — tier 1 only, success-path
  # only (a failed-class run makes no integrity claim), and only when the
  # integrity metrics were actually computed.
  #
  # pair-4 crash-class fix: check_integrity.py now returns DISTINCT exit codes,
  # and this is a REDUNDANT CONFIRMATION LAYER over the capture-computed
  # integrity_ok (SQL 20), which is the AUTHORITATIVE verdict already on the row:
  #   0  verdict OK           -> proceed.
  #   1  RAN and MISMATCHED   -> the run FAILS (unchanged §3 semantics). The row
  #                             already stands (verdict runs post-insert/export
  #                             by design), so the mismatch is detectable via
  #                             integrity_ok=0; the job fails for CI visibility.
  #   3  CHECK_ERROR          -> could NOT verify (infra/connection/query — the
  #                             pair-4 transient read-timeout was THIS). WARN
  #                             loudly; do NOT die. The runs row stands clean
  #                             because capture-computed integrity_ok is
  #                             authoritative; a transient checker failure must
  #                             never turn a perfect run false-red.
  #
  # NOTE on why exit 3 does NOT retro-flag: the runs row is ALREADY INSERTED at
  # this point (post-insert/post-export by design), so a flag can no longer ride
  # the runtime map — and it must not, because there is nothing wrong to flag:
  # the authoritative capture verdict already recorded integrity_ok. We mark a
  # PAIR-level WARNING (not a failure) so the pair exit stays green.
  if [ "${mode}" = "strict" ] && [ "${tier}" = "1" ]; then
    if [ "${integrity_unverified}" = "1" ]; then
      warn "tier 1 integrity UNVERIFIED for ${RUN_ID} — run FLAGGED (integrity_unverified), not failed (contract §1.3)"
    else
      local ic_rc=0
      ( cd "${CAPTURE_DIR}" && python3 check_integrity.py ) || ic_rc=$?
      case "${ic_rc}" in
        0)
          : ;;  # verdict OK
        1)
          die "TIER 1 INTEGRITY MISMATCH for ${RUN_ID} — checker RAN and read integrity_ok!=1; run FAILS. The perf.runs row is already inserted and the metrics persisted (DWH export was attempted best-effort earlier — see its own log line for whether a DWH role was present); the mismatch is detectable via integrity_ok=0 on the persisted row (contract §3)." ;;
        3)
          PAIR_HAD_WARNING=1
          warn "TIER 1 integrity CHECK_ERROR for ${RUN_ID} (exit 3): the redundant confirmation checker could NOT verify (infra/connection/query failure after its own retries — e.g. a transient CH read-timeout). This is NOT a data verdict. The runs row STANDS CLEAN: the capture-computed integrity_ok (SQL 20) is the AUTHORITATIVE verdict and is already on the row + exported. Run is NOT failed; pair stays green with a WARNING." ;;
        *)
          PAIR_HAD_WARNING=1
          warn "TIER 1 integrity checker returned UNEXPECTED exit ${ic_rc} for ${RUN_ID} — treating as CHECK_ERROR (could not verify; capture-computed integrity_ok remains authoritative). Run NOT failed; pair WARNING." ;;
      esac
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
  # §1.2 join-rule gate (review of bf623b48, F2): run_cost_usd may only attach
  # to a run_id whose perf.runs row actually LANDED. A failed-class run whose
  # record insert failed was rolled back — emitting the cost metric onto it
  # would create an orphaned metric row with no runs-row join. We deliberately
  # SKIP with a loud warn rather than silently re-anchor to the other arm: a
  # re-anchor would blur the "first-run arm carries the pair cost" convention
  # and per-pair cost sums stay honest either way (the pair simply has no cost
  # row that night — rare, and the CI job is already red via PAIR_HAD_FAILURE).
  case " ${RECORDED_RUNS} " in
    *" ${cost_run_id} "*) ;;
    *)
      warn "PHASE 3c SKIPPED: run_cost_usd target ${cost_run_id} has NO recorded runs row (its record insert failed/rolled back) — refusing to orphan a metric (contract §1.2). This pair carries no run_cost_usd."
      return 0 ;;
  esac
  log "PHASE 3c: pair cost -> run_cost_usd on ${cost_run_id} (end-of-pair window)"
  # Cost spans BOTH instance types (2026-07-08 rebuild): --nodes/--node-usd-per-hr
  # is the m6i.large term (broker/registry/producer/poller); --connect-nodes/
  # --connect-usd-per-hr is the dedicated m6i.xlarge Connect-worker term. Both
  # accrue over the same pair window.
  python3 "${SCRIPT_DIR}/emit_run_cost.py" \
    --run-id "${cost_run_id}" \
    --pair-start "${PAIR_RUN_START:?}" \
    --nodes "${SCALE_UP_NODES}" \
    --node-usd-per-hr "${M6I_LARGE_USD_PER_HR}" \
    --connect-nodes "${CONNECT_NODES}" \
    --connect-usd-per-hr "${M6I_XLARGE_USD_PER_HR}" \
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
  # Per-run state reset: no flag/settle bleed from the previous (arm,tier) run
  # onto a failed-class run that skips finalize/settle.
  FLAGGED=0; FLAG_REASON=""; export FLAGGED FLAG_REASON
  SETTLE_END=""; SETTLE_SECONDS=""; SETTLE_TIMED_OUT=""
  KAFKA_WORKER_CPU_SHARE_T0=""; export KAFKA_WORKER_CPU_SHARE_T0
  local g0="ch-sink-${arm}-t0"
  ( cd "${CAPTURE_DIR}" && python3 run_metrics_sql.py "${E2E_DIR}/sql/capture/21_pre_run_covariates.sql" ) \
    || warn "tier0 pre-run covariates failed (continuing)"
  deploy_connector "hits_null" "${g0}"
  wait_tasks_running
  # Drain start boundary (outcome amendment): from here on, a failure is a
  # failed-class CAPTURED run, not a rollback-and-die.
  local rc0=0
  run_poller_sample "${g0}" || rc0=$?
  if [ "${rc0}" -eq 0 ]; then
    # Tier 0 has no settle (Null engine): SETTLE_END = RUN_END, settle 0.
    export SETTLE_END="${RUN_END}" SETTLE_SECONDS=0 SETTLE_TIMED_OUT=0
    finalize_and_insert_metrics 0 || rc0=3
  fi
  if [ "${rc0}" -eq 0 ]; then
    # CPU acceptance gate AFTER the tier-0 finalize (principal directive):
    # loud PASS / INSTRUMENT_RESIZE_SUSPECT verdict + kafka_worker_cpu_share_t0
    # runtime key on this run's row (log-only; never auto-flags).
    compute_cpu_gate_t0
    capture_and_record "${arm}" "0"
  else
    ingest_failed "${arm}" "0" "$(ingest_fail_reason "${rc0}")"
  fi
  delete_connector "${g0}"

  # ---- Tier 1 (hits) ----
  export CH_TABLE="hits"
  RUN_ID="${PAIR_ID}-${arm}-t1"; export RUN_ID
  RUN_START="$(date -u +%Y-%m-%dT%H:%M:%S)"; export RUN_START
  FLAGGED=0; FLAG_REASON=""; export FLAGGED FLAG_REASON
  SETTLE_END=""; SETTLE_SECONDS=""; SETTLE_TIMED_OUT=""
  local g1="ch-sink-${arm}-t1"
  # Review F8: pre-run covariates BEFORE truncate (capture/README binding
  # order) — pre_run_active_parts / pre_run_rss have PRE-truncate semantics
  # (the contract's cleanliness verifier reads the state the run INHERITED).
  ( cd "${CAPTURE_DIR}" && python3 run_metrics_sql.py "${E2E_DIR}/sql/capture/21_pre_run_covariates.sql" ) \
    || warn "tier1 pre-run covariates failed (continuing)"
  ( cd "${CAPTURE_DIR}" && python3 truncate_target.py ) || fail_run "tier1 truncate failed"
  deploy_connector "hits" "${g1}"
  wait_tasks_running
  local rc1=0
  run_poller_sample "${g1}" || rc1=$?
  if [ "${rc1}" -eq 0 ]; then
    # settle after lag 0 (plan §5 step 3b) — success path only; a failed run's
    # window closes at failure time (ingest_failed).
    local settle_status="${ARTIFACT_DIR}/settle-${RUN_ID}.status"
    SETTLE_END="$( cd "${CAPTURE_DIR}" && SETTLE_STATUS_FILE="${settle_status}" python3 wait_for_settle.py )" \
      || warn "wait_for_settle errored (continuing with RUN_END)"
    export SETTLE_END="${SETTLE_END:-${RUN_END}}"
    export SETTLE_TIMED_OUT="$(cat "${settle_status}" 2>/dev/null || echo 0)"
    export SETTLE_SECONDS=0   # settle_seconds is emitted by capture SQL 14 from the window
    finalize_and_insert_metrics 1 || rc1=3
  fi
  if [ "${rc1}" -eq 0 ]; then
    capture_and_record "${arm}" "1"
  else
    ingest_failed "${arm}" "1" "$(ingest_fail_reason "${rc1}")"
  fi
  delete_connector "${g1}"

  delete_connect
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
    # Route group deletion through delete_consumer_group.sh (single-point
    # maintenance) — same path as delete_connector(). pair-4 fix: the previous
    # `2>/dev/null` here swallowed the script's OWN B5 not-found silencing AND
    # any real error, so the not-found handling never got a chance to show and
    # the trap printed scary noise / hid genuine failures. The script already
    # treats GroupIdNotFoundException as a silent success (exit 0) and emits a
    # clean line otherwise; let its stderr through and keep it best-effort.
    local arm tier
    for arm in head pinned; do
      for tier in 0 1; do
        "${SCRIPT_DIR}/delete_consumer_group.sh" "ch-sink-${arm}-t${tier}" || true
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
      # --allow-tag: local-hacking escape hatch (equivalent to KAFKA_ALLOW_TAG=1)
      # that lets a mutable tag through image validation. Default is STRICT
      # (digest-pinned): never use this for a real benchmark pair.
      --allow-tag) export KAFKA_ALLOW_TAG=1; shift ;;
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
  # OUTCOME is owned exclusively by ingest_failed(); an ambient value would
  # mark every run failed (contract §1.3: success must OMIT the key).
  unset OUTCOME

  # Image validation FIRST (before any cluster mutation): every deployed image
  # must be a DIGEST ref (stale-tag class fix). A mutable tag is either resolved
  # to a digest here or hard-fails — unless KAFKA_ALLOW_TAG=1 / --allow-tag
  # (local-hacking only). Resolve in place so every downstream deploy pins the
  # digest. ARM0/ARM1 are validated here; ARM_IMAGE in kafkaconnect.yaml.tmpl is
  # fed from these. The producer image is the poller pod's image too.
  ARM0_IMAGE="$(validate_image_ref ARM0_IMAGE "${ARM0_IMAGE:?ARM0_IMAGE required}")" \
    || die "ARM0_IMAGE failed digest validation"
  ARM1_IMAGE="$(validate_image_ref ARM1_IMAGE "${ARM1_IMAGE:?ARM1_IMAGE required}")" \
    || die "ARM1_IMAGE failed digest validation"
  PRODUCER_IMAGE="$(validate_image_ref PRODUCER_IMAGE "${PRODUCER_IMAGE:?PRODUCER_IMAGE required}")" \
    || die "PRODUCER_IMAGE failed digest validation"
  export ARM0_IMAGE ARM1_IMAGE PRODUCER_IMAGE
  log "images validated (digest-pinned): arm0=${ARM0_IMAGE} arm1=${ARM1_IMAGE} producer=${PRODUCER_IMAGE}"

  # Pre-launch provenance gate: non-fatal newer-push eyeball for ECR refs.
  check_image_provenance ARM0_IMAGE "${ARM0_IMAGE}"
  check_image_provenance ARM1_IMAGE "${ARM1_IMAGE}"
  check_image_provenance PRODUCER_IMAGE "${PRODUCER_IMAGE}"

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
  # CFG_MAX_POLL_RECORDS is a REAL knob (review F2): render_connector.py writes
  # this same env var into consumer.override.max.poll.records, so the deployed
  # connector and the runtime echo agree by construction. Baseline 25000, the
  # only live-proven pairing with CONNECT_HEAP=4096m (100k GC-spiraled the 2G
  # heap — e140231). TODO(#32): co-tune poll+heap upward on the xlarge toward
  # the >=50k rows/insert milestone.
  export CFG_MAX_POLL_RECORDS="${CFG_MAX_POLL_RECORDS:-25000}"
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
  # Pair-level policy (contract §1.3 outcome amendment): a failed-class run
  # does NOT abort the pair — every remaining (arm,tier) run was still executed
  # and captured (the failed run's tier ratio is simply non-computable; the
  # other arm's rows keep their absolute-trend value). But CI must SEE it, so
  # the job exits non-zero after all capture + cleanup completed.
  if [ "${PAIR_HAD_FAILURE}" = "1" ]; then
    warn "pair ${PAIR_ID} completed WITH FAILED-CLASS RUN(S) — evidence captured + exported (runtime['outcome']='failed'); exiting non-zero for CI visibility"
    exit 1
  fi
  # A non-fatal warning (e.g. a tier-1 integrity CHECK_ERROR: the redundant
  # confirmation checker could not verify) does NOT fail the pair — the
  # capture-computed integrity_ok is authoritative and already recorded. Surface
  # it so the operator sees the checker did not run clean, but exit GREEN.
  if [ "${PAIR_HAD_WARNING}" = "1" ]; then
    warn "pair ${PAIR_ID} complete WITH WARNING(S) (e.g. integrity CHECK_ERROR — redundant checker could not verify; capture-computed integrity_ok authoritative). Pair is GREEN."
  fi
  log "pair complete: ${PAIR_ID}"
}

# Run main() only when executed directly, not when sourced (tests source this
# file to unit-test validate_image_ref / resolve_tag_to_digest in isolation).
# BASH_SOURCE[0] != $0 under `source`; equal under direct execution.
if [ "${BASH_SOURCE[0]:-$0}" = "$0" ]; then
  main "$@"
fi
