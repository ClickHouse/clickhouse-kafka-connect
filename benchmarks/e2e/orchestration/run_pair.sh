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
# CONNECT_HEAP baseline = 4096m (co-sized with max.poll.records; the 2G heap
# GC-spiraled on the first 100k poll — commit e140231). Runs on the dedicated
# connect-ng m6i.xlarge (16 GiB) with the CR's 5Gi/6Gi request/limit. #32 tunes
# heap + poll size upward together.
CONNECT_HEAP="${CONNECT_HEAP:-4096m}"
POLL_INTERVAL="${POLL_INTERVAL:-10}"
POLL_TIMEOUT="${POLL_TIMEOUT:-3600}"
ARTIFACT_DIR="${ARTIFACT_DIR:-${SCRIPT_DIR}/artifacts}"

# us-east-2 on-demand pricing for the pair-cost calc (phase_pair_cost /
# emit_run_cost.py). Kept here (not an AWS Pricing API call) per plan "no new
# AWS calls"; bump deliberately when prices drift.
# The pair now runs on TWO instance types (2026-07-08 rebuild): SCALE_UP_NODES
# x m6i.large (broker/registry/producer/poller) + CONNECT_NODES x m6i.xlarge
# (the dedicated, CPU-bound Connect worker). Both node-hour terms are billed.
M6I_LARGE_USD_PER_HR="${M6I_LARGE_USD_PER_HR:-0.096}"    # us-east-2 on-demand
M6I_XLARGE_USD_PER_HR="${M6I_XLARGE_USD_PER_HR:-0.192}"  # us-east-2 on-demand (2x large)
EBS_GP3_USD_PER_GB_MO="${EBS_GP3_USD_PER_GB_MO:-0.08}"
BROKER_EBS_GB="${BROKER_EBS_GB:-70}"
SCALE_UP_NODES="${SCALE_UP_NODES:-2}"
# Dedicated Connect nodegroup count (baseline 1 m6i.xlarge; keep in sync with
# infra/env.sh CONNECT_NODES). Scale-out is the #37 sweep's variable.
CONNECT_NODES="${CONNECT_NODES:-1}"

log()  { printf '\033[1;34m[run_pair]\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[1;33m[run_pair:warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[run_pair:error]\033[0m %s\n' "$*" >&2; exit 1; }

# Set to 1 by ingest_failed() (contract §1.3 outcome amendment): the pair
# CONTINUES after a failed-class run (the contract wants the data from every
# remaining run) but main() exits non-zero at the end for CI visibility.
PAIR_HAD_FAILURE=0

# Space-separated run_ids whose perf.runs row ACTUALLY landed (appended by
# capture_and_record after a successful insert_run_record). phase_pair_cost
# consults this before emitting run_cost_usd: a metric row for a run_id with
# no runs row would violate the contract §1.2 join rule (metrics inherit their
# tagging solely through the runs-row join).
RECORDED_RUNS=""

# --------------------------------------------------------------------------- #
# Image reference validation — DIGEST-PINNED BY DEFAULT (stale-tag class fix).
#
# Live-diagnosed failure: mutable tags (benchmark-head-<sha>, :latest) were
# served STALE twice during the first pair because a node/registry cache kept an
# older image behind a reused tag — the two arms silently ran the wrong bits.
# The operator recovered by hand-pinning digests; that is now the default.
#
# Every image the pair deploys (both arm images + the producer/poller image)
# MUST be a digest reference (repo@sha256:...). validate_image_ref:
#   * digest ref (contains @sha256:)   -> accept, echo unchanged
#   * mutable tag + KAFKA_ALLOW_TAG=1  -> WARN (local-hacking escape hatch),
#                                         echo unchanged (a laptop daemon may not
#                                         have registry access to resolve)
#   * mutable tag, strict (default)    -> try to RESOLVE the tag to a digest:
#         - ECR  (*.dkr.ecr.*.amazonaws.com/...) via `aws ecr describe-images`
#         - ghcr (ghcr.io/...)                    via `gh api` (packages)
#       resolved -> echo the digest ref; unresolvable -> FAIL LOUD with the
#       stale-tag explanation. (kubectl/docker cannot resolve a remote tag to a
#       digest without pulling; the registry APIs above are the reliable path.)
#
# Emits the (possibly resolved) digest ref on stdout; diagnostics on stderr.
# Returns non-zero on a hard failure. Self-contained (env + external CLIs only)
# so tests can exercise it directly.
#
# Args: <var-name-for-messages> <ref>
# Env : KAFKA_ALLOW_TAG (escape hatch, default strict).
# --------------------------------------------------------------------------- #
_ref_is_digest() { case "$1" in *@sha256:*) return 0 ;; *) return 1 ;; esac; }

# Resolve a mutable tag to repo@sha256:... using the registry API. Echoes the
# digest ref on success; returns non-zero (no output) when it cannot resolve.
resolve_tag_to_digest() {
  local ref="$1" repo tag registry digest
  repo="${ref%:*}"; tag="${ref##*:}"
  # A ref with no ":tag" (e.g. bare repo) has repo==ref; treat tag as "latest".
  [ "${repo}" = "${ref}" ] && { repo="${ref}"; tag="latest"; }
  registry="${ref%%/*}"
  case "${registry}" in
    *.dkr.ecr.*.amazonaws.com)
      command -v aws >/dev/null 2>&1 || return 1
      local region ecr_repo
      # registry = <acct>.dkr.ecr.<region>.amazonaws.com ; ecr repo = path after host.
      region="$(printf '%s' "${registry}" | sed -n 's/.*\.dkr\.ecr\.\([^.]*\)\.amazonaws\.com/\1/p')"
      ecr_repo="${repo#*/}"
      [ -n "${region}" ] && [ -n "${ecr_repo}" ] || return 1
      digest="$(aws ecr describe-images --region "${region}" \
                  --repository-name "${ecr_repo}" \
                  --image-ids imageTag="${tag}" \
                  --query 'imageDetails[0].imageDigest' --output text 2>/dev/null)"
      ;;
    ghcr.io)
      command -v gh >/dev/null 2>&1 || return 1
      # ghcr path = ghcr.io/<owner>/<package...>. The container package "name"
      # is everything after the owner (URL-encode the '/' as %2F for the API).
      local owner pkg
      owner="$(printf '%s' "${repo}" | cut -d/ -f2)"
      pkg="$(printf '%s' "${repo}" | cut -d/ -f3- | sed 's#/#%2F#g')"
      [ -n "${owner}" ] && [ -n "${pkg}" ] || return 1
      # Find the version whose tags contain ${tag}; take its digest name.
      digest="$(gh api --paginate \
                  "/users/${owner}/packages/container/${pkg}/versions" \
                  --jq ".[] | select(.metadata.container.tags[]? == \"${tag}\") | .name" \
                  2>/dev/null | head -1)"
      # Org packages live under /orgs/...; retry there if the user path was empty.
      if [ -z "${digest}" ]; then
        digest="$(gh api --paginate \
                    "/orgs/${owner}/packages/container/${pkg}/versions" \
                    --jq ".[] | select(.metadata.container.tags[]? == \"${tag}\") | .name" \
                    2>/dev/null | head -1)"
      fi
      ;;
    *)
      return 1 ;;
  esac
  case "${digest}" in
    sha256:*) printf '%s@%s\n' "${repo}" "${digest}"; return 0 ;;
    *) return 1 ;;
  esac
}

validate_image_ref() {
  local name="$1" ref="$2"
  if [ -z "${ref}" ]; then
    echo "[validate_image_ref] ${name} is empty" >&2
    return 1
  fi
  if _ref_is_digest "${ref}"; then
    printf '%s\n' "${ref}"
    return 0
  fi
  if [ "${KAFKA_ALLOW_TAG:-0}" = "1" ]; then
    echo "[validate_image_ref:warn] ${name}='${ref}' is a MUTABLE TAG; accepted only" \
         "because KAFKA_ALLOW_TAG=1 (local-hacking escape hatch). Node/registry" \
         "caches can serve this stale — never use a tag for a real benchmark pair." >&2
    printf '%s\n' "${ref}"
    return 0
  fi
  # Strict (default): try to resolve to a digest before giving up.
  local resolved
  if resolved="$(resolve_tag_to_digest "${ref}")" && _ref_is_digest "${resolved}"; then
    echo "[validate_image_ref] resolved ${name} tag '${ref}' -> '${resolved}'" >&2
    printf '%s\n' "${resolved}"
    return 0
  fi
  echo "[validate_image_ref:error] ${name}='${ref}' is a MUTABLE TAG and could not" \
       "be resolved to a digest (repo@sha256:...)." >&2
  echo "  The benchmark pins images by DIGEST by default: a mutable tag can be" >&2
  echo "  served STALE by node/registry caches (the stale-tag failure class that" >&2
  echo "  made the first pair run the wrong image twice)." >&2
  echo "  Fix: pass the digest ref (build-arm.sh --push prints IMAGE_DIGEST=...)," >&2
  echo "  or set KAFKA_ALLOW_TAG=1 to bypass for LOCAL hacking only." >&2
  return 1
}

# --------------------------------------------------------------------------- #
# Pre-launch provenance gate (stale-pin lesson): a pre-fix producer digest once
# wasted a full launch. AFTER digest validation, for each ECR-hosted image ref,
# WARN LOUDLY (never fail) if a NEWER image was pushed to that repo AFTER the
# pinned digest's own push time. Pinned-arm refs are LEGITIMATELY old, so this
# is an operator eyeball prompt, not a gate: the run proceeds regardless.
# Skips silently for non-ECR refs and when aws is unavailable. Kept fast (one
# describe-images per ref = 3 calls for arm0/arm1/producer).
#
# _ecr_newer_push_exists: PURE comparator (unit-testable offline). Reads an
# `aws ecr describe-images` JSON document on stdin and the pinned sha256 digest
# as $1. Echoes "1" if any image in the repo was pushed strictly AFTER the
# pinned digest's imagePushedAt; "0" otherwise (including when the pinned digest
# is not found, or timestamps are missing — fail safe to no-warning). No AWS.
_ecr_newer_push_exists() {
  local pinned_digest="$1"
  # The describe-images JSON arrives on STDIN, so the python program itself is
  # read from fd 3 (heredoc) — `python3 -` would consume stdin as the program.
  python3 /dev/fd/3 "${pinned_digest}" 3<<'PY'
import json, sys
pinned = sys.argv[1]
try:
    doc = json.load(sys.stdin)
    imgs = doc.get("imageDetails", []) if isinstance(doc, dict) else doc
    # Find the pinned image's push time.
    pinned_at = None
    for im in imgs:
        if im.get("imageDigest") == pinned:
            pinned_at = im.get("imagePushedAt")
            break
    if pinned_at is None:
        print("0"); sys.exit(0)
    newer = any(
        im.get("imageDigest") != pinned
        and im.get("imagePushedAt") is not None
        and str(im["imagePushedAt"]) > str(pinned_at)
        for im in imgs
    )
    print("1" if newer else "0")
except Exception:
    print("0")
PY
}

check_image_provenance() {
  local name="$1" ref="$2"
  command -v aws >/dev/null 2>&1 || return 0
  # digest ref = repo@sha256:...  ; only ECR-hosted repos are checked.
  case "${ref}" in *@sha256:*) : ;; *) return 0 ;; esac
  local repo="${ref%@*}" pinned="${ref##*@}"
  local registry="${repo%%/*}"
  case "${registry}" in
    *.dkr.ecr.*.amazonaws.com) : ;;
    *) return 0 ;;   # non-ECR ref: skip silently
  esac
  local region ecr_repo json newer
  region="$(printf '%s' "${registry}" | sed -n 's/.*\.dkr\.ecr\.\([^.]*\)\.amazonaws\.com/\1/p')"
  ecr_repo="${repo#*/}"
  [ -n "${region}" ] && [ -n "${ecr_repo}" ] || return 0
  json="$(aws ecr describe-images --region "${region}" \
            --repository-name "${ecr_repo}" \
            --query '{imageDetails: imageDetails[].{imageDigest: imageDigest, imagePushedAt: imagePushedAt}}' \
            --output json 2>/dev/null)" || return 0
  [ -n "${json}" ] || return 0
  newer="$(printf '%s' "${json}" | _ecr_newer_push_exists "${pinned}")"
  if [ "${newer}" = "1" ]; then
    warn "PROVENANCE (${name}): a NEWER image was pushed to ${ecr_repo} AFTER the pinned digest (${pinned}) — confirm your pin is intentional. (Pinned-arm refs are legitimately old; this is informational, the run proceeds.)"
  fi
}

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
    # --- deployed-instrument keys (principal directive, pair #2 onward) ---
    # kafka-NAMESPACED for now: the shared pinned spellings arrive in a pending
    # contract amendment; namespaced keys are contract-legal today (§1.4
    # connector-specific rule) and the later migration to the shared names is a
    # 1-line rename. Values are DEPLOYED truth read back from the live pod/node
    # by deploy_connect (never template intent — the provenance lesson).
    "kafka_compute_instance_type": os.environ.get("KAFKA_COMPUTE_INSTANCE_TYPE", ""),
    "kafka_worker_cpu_request": os.environ.get("KAFKA_WORKER_CPU_REQUEST", ""),
    "kafka_worker_cpu_limit": os.environ.get("KAFKA_WORKER_CPU_LIMIT", ""),
    "kafka_worker_mem_request": os.environ.get("KAFKA_WORKER_MEM_REQUEST", ""),
    "kafka_worker_mem_limit": os.environ.get("KAFKA_WORKER_MEM_LIMIT", ""),
}
# Tier-0-only: the CPU acceptance-gate share (compute_cpu_gate_t0). The cadvisor
# CPU source is wired (poller prerequisite 2), so this is normally present;
# absent only if the runtime scrape failed or the gate could not compute.
if tier == "0":
    _share = os.environ.get("KAFKA_WORKER_CPU_SHARE_T0", "")
    if _share:
        rt["kafka_worker_cpu_share_t0"] = _share
# Review F9: mandatory keys hard-fail when empty (never silently dropped).
MANDATORY = ("arm", "tier", "pair_id", "target_region",
             "environment_class", "compute_region")
missing = [k for k in MANDATORY if not rt.get(k)]
if missing:
    print(f"ERROR: runtime map missing mandatory key(s): {missing} — "
          f"refusing to build a runs-row runtime without them (contract §1.1).",
          file=sys.stderr)
    sys.exit(1)
# Contract §1.3 outcome amendment: the key is present ONLY when the ingest
# failed (ingest_failed() exports OUTCOME=failed). A success run MUST OMIT it
# (absent => 'success', Map semantics — legacy rows only ever landed on the
# success path), so writing outcome='success' explicitly is rejected.
outcome = os.environ.get("OUTCOME", "")
if outcome:
    if outcome != "failed":
        print(f"ERROR: OUTCOME={outcome!r} — the only writable value is "
              f"'failed'; a success run must OMIT the outcome key "
              f"(contract §1.3, absent => success).", file=sys.stderr)
        sys.exit(1)
    rt["outcome"] = outcome
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
  # Take the LAST JSON line, not the last line: the producer prints a final
  # human-readable "[producer] OK: ..." line AFTER the JSON summary (live-run
  # finding 2026-07-08 — tail -1 grabbed it and the parse died on a PERFECT
  # 10M/10M pre-load).
  ROWS_EXPECTED="$(echo "${plog}" | grep '^{' | tail -1 | python3 -c 'import sys,json; print(json.load(sys.stdin)["rows_expected"])' 2>/dev/null)"
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
  # Pin the poller pod to the bench nodegroup (2026-07-08 rebuild): the dedicated
  # connect-ng m6i.xlarge runs ONLY the Connect worker. `kubectl run` has no
  # --node-selector flag, so inject nodeSelector role=bench via --overrides.
  # serviceAccountName=bench-poller-sa (infra/poller-rbac.yaml, applied by
  # scale-up.sh) grants the projected token used to scrape the kubelet cadvisor
  # endpoint through the API-server node proxy (poller prerequisite 2 — the
  # sighted CPU source). Without it the pod runs as `default` (no nodes/proxy).
  kubectl -n "${NS}" run "${POLLER_POD}" --image="${PRODUCER_IMAGE:?PRODUCER_IMAGE required}" \
    --restart=Never --command \
    --labels="app.kubernetes.io/part-of=kafka-connect-benchmark-v2" \
    --overrides='{"spec":{"nodeSelector":{"role":"bench"},"serviceAccountName":"bench-poller-sa"}}' \
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

  # Instrument runtime keys — DEPLOYED truth, not template intent (the
  # provenance lesson: record what actually ran). Read the instance type from
  # the NODE the worker pod landed on, and the CPU/memory requests/limits from
  # the DEPLOYED pod spec (containers[0] — the Strimzi Connect pod has a single
  # main container; the JMX exporter is an agent, not a sidecar). Keys are
  # kafka-NAMESPACED for now: the shared pinned spellings arrive in a pending
  # contract amendment; namespaced keys are contract-legal today (§1.4), and
  # migrating to the shared names later is a 1-line rename here.
  # nodeName resolution runs here under the ORCHESTRATOR identity (not the
  # poller SA). Do NOT swallow stderr: the 07-09 blind-CPU-gate incident was a
  # silent failure here — 2>/dev/null hid the true cause, so a resolution
  # failure looked identical to an empty node. Capture stderr and warn-log it
  # loudly on failure; the gate degrades honestly (POD_CADVISOR_URL empty ->
  # CPU gate UNAVAILABLE) but the operator now sees WHY.
  local node node_err
  node_err="$(mktemp)"
  node="$(kubectl -n "${NS}" get pod "${CONNECT_POD}" -o jsonpath='{.spec.nodeName}' 2>"${node_err}")"
  if [ -z "${node}" ] && [ -s "${node_err}" ]; then
    warn "nodeName resolution failed for pod ${CONNECT_POD}: $(tr '\n' ' ' < "${node_err}")"
  fi
  rm -f "${node_err}"

  # Sighted CPU source (poller prerequisite 2): the cadvisor cumulative CPU
  # counter for the Connect pod, scraped from the kubelet /metrics/cadvisor of
  # the node the worker landed on, THROUGH the API-server node proxy. The poller
  # pod authenticates with its bench-poller-sa token (nodes/proxy get, granted by
  # infra/poller-rbac.yaml) and verifies TLS against the mounted cluster CA
  # (sampler._cadvisor_auth activates on the https:// scheme). Node is resolved
  # per-arm here (connect-ng has 1 node, but never hardcode — the pod may be
  # rescheduled between arm deploys). Empty node -> leave POD_CADVISOR_URL empty
  # so the poller tolerates absence rather than scraping a malformed URL.
  if [ -n "${node}" ]; then
    export POD_CADVISOR_URL="https://kubernetes.default.svc/api/v1/nodes/${node}/proxy/metrics/cadvisor"
  else
    export POD_CADVISOR_URL=""
    warn "could not resolve the Connect worker node — POD_CADVISOR_URL empty, CPU gate stays UNAVAILABLE"
  fi
  # Container-label filter for the cadvisor series: the Strimzi Connect pod's
  # single main container (containers[0].name). Empty -> the sampler sums real
  # containers only (double-count guard excludes the "" aggregate + "POD" pause).
  CONNECT_POD_CONTAINER="$(kubectl -n "${NS}" get pod "${CONNECT_POD}" \
      -o jsonpath='{.spec.containers[0].name}' 2>/dev/null)"
  export CONNECT_POD_CONTAINER

  KAFKA_COMPUTE_INSTANCE_TYPE="$(kubectl get node "${node}" \
      -o jsonpath='{.metadata.labels.node\.kubernetes\.io/instance-type}' 2>/dev/null)"
  KAFKA_WORKER_CPU_REQUEST="$(kubectl -n "${NS}" get pod "${CONNECT_POD}" \
      -o jsonpath='{.spec.containers[0].resources.requests.cpu}' 2>/dev/null)"
  KAFKA_WORKER_CPU_LIMIT="$(kubectl -n "${NS}" get pod "${CONNECT_POD}" \
      -o jsonpath='{.spec.containers[0].resources.limits.cpu}' 2>/dev/null)"
  KAFKA_WORKER_MEM_REQUEST="$(kubectl -n "${NS}" get pod "${CONNECT_POD}" \
      -o jsonpath='{.spec.containers[0].resources.requests.memory}' 2>/dev/null)"
  KAFKA_WORKER_MEM_LIMIT="$(kubectl -n "${NS}" get pod "${CONNECT_POD}" \
      -o jsonpath='{.spec.containers[0].resources.limits.memory}' 2>/dev/null)"
  export KAFKA_COMPUTE_INSTANCE_TYPE KAFKA_WORKER_CPU_REQUEST KAFKA_WORKER_CPU_LIMIT \
         KAFKA_WORKER_MEM_REQUEST KAFKA_WORKER_MEM_LIMIT
  [ -n "${KAFKA_COMPUTE_INSTANCE_TYPE}" ] \
    || warn "could not read the worker node's instance-type label — kafka_compute_instance_type will be absent"
  log "  instrument: node=${node} type=${KAFKA_COMPUTE_INSTANCE_TYPE:-?} cpu=${KAFKA_WORKER_CPU_REQUEST:-?}/${KAFKA_WORKER_CPU_LIMIT:-?} mem=${KAFKA_WORKER_MEM_REQUEST:-?}/${KAFKA_WORKER_MEM_LIMIT:-?}"
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
  # Return codes for the caller (contract §1.3 outcome amendment — the caller
  # decides between the success path and the capture-on-failure path; this
  # function no longer rolls back or dies):
  #   0 = drained (lag reached 0)
  #   2 = poller timeout — lag never reached 0: the INGEST did not complete
  #       (failed-class; the finalizer still lands the guards incl.
  #       drain_incomplete from the partial samples)
  #   1 = hard failure (poller crashed / exec dropped / no samples retrieved)
  if [ "${POLLER_RC}" = "2" ]; then
    warn "  poller TIMED OUT — lag never reached 0 (ingest did not complete)"
    return 2
  elif [ "${POLLER_RC}" != "0" ]; then
    warn "poller sample failed hard (rc=${POLLER_RC})"
    return 1
  fi
  [ -s "${SAMPLES_FILE}" ] || { warn "no samples were retrieved for ${RUN_ID}"; return 1; }
  return 0
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
  if [ "${mode}" = "strict" ] && [ "${tier}" = "1" ]; then
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
  log "pair complete: ${PAIR_ID}"
}

# Run main() only when executed directly, not when sourced (tests source this
# file to unit-test validate_image_ref / resolve_tag_to_digest in isolation).
# BASH_SOURCE[0] != $0 under `source`; equal under direct execution.
if [ "${BASH_SOURCE[0]:-$0}" = "$0" ]; then
  main "$@"
fi
