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
# lib_bench.sh — run-mode-agnostic benchmark helpers (spec §8, IC-1).
#
# Sourced by BOTH run_pair.sh (the nightly pair orchestrator) and chaos_run.sh
# (the chaos/monkey orchestrator). This file holds the functions and constants
# that are identical across run modes; everything pair-specific stays in
# run_pair.sh (§10.2 NOT-moved list: finalize_and_insert_metrics,
# capture_and_record, ingest_failed, ingest_fail_reason, compute_cpu_gate_t0,
# phase_pair_cost, resolve_arm_order, print_plan, cleanup_trap, main, phase_arm,
# append_flag, the perf.*/export path).
#
# INVARIANT (IC-1): sourcing this file must have NO top-level side effect beyond
# variable defaults — it must NEVER execute a phase. It is `set -uo pipefail`
# compatible: every constant uses the `${VAR:-default}` idiom so a bare `set -u`
# shell can source it. Constants are overridable so chaos can retarget the
# topic/group/table/connector names (and the log prefix) without touching pair
# behavior; run_pair.sh sources this with the defaults and is byte-identical to
# before the extraction.
# =============================================================================

# --------------------------------------------------------------------------- #
# Overridable path + identity constants. SCRIPT_DIR defaults to this file's own
# directory (run_pair.sh sets it BEFORE sourcing, to locate this file, so the
# default is a no-op there). All others are `${VAR:-default}` so chaos can
# retarget them from the environment.
# --------------------------------------------------------------------------- #
SCRIPT_DIR="${SCRIPT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)}"
E2E_DIR="${E2E_DIR:-$(cd "${SCRIPT_DIR}/.." && pwd)}"
INFRA_DIR="${INFRA_DIR:-${E2E_DIR}/infra}"
CAPTURE_DIR="${CAPTURE_DIR:-${E2E_DIR}/capture}"
POLLER_DIR="${POLLER_DIR:-${E2E_DIR}/poller}"
PRODUCER_DIR="${PRODUCER_DIR:-${E2E_DIR}/producer}"
TEMPLATES_DIR="${TEMPLATES_DIR:-${SCRIPT_DIR}/templates}"

NS="${K8S_NAMESPACE:-kafka-bench}"
TOPIC="${TOPIC:-hits}"
CONNECT_NAME="${CONNECT_NAME:-bench-connect}"
CONNECTOR_NAME="${CONNECTOR_NAME:-bench-clickhouse-sink}"
CONNECT_REST="${CONNECT_REST:-http://${CONNECT_NAME}-connect-api.${NS}.svc:8083}"
POLLER_POD="${POLLER_POD:-bench-poller}"
# Strimzi node-pool pod naming (<cluster>-<pool>-<ordinal>); see README known gaps.
BROKER_POD="${BROKER_POD:-bench-combined-0}"
EXPECTED_PARTITIONS="${EXPECTED_PARTITIONS:-3}"
EXPECTED_TASKS="${EXPECTED_TASKS:-3}"
# CONNECT_HEAP baseline = 4096m (co-sized with max.poll.records; the 2G heap
# GC-spiraled on the first 100k poll — commit e140231). Runs on the dedicated
# connect-ng m6i.xlarge (16 GiB) with the CR's 5Gi/6Gi request/limit. #32 tunes
# heap + poll size upward together.
CONNECT_HEAP="${CONNECT_HEAP:-4096m}"
POLL_INTERVAL="${POLL_INTERVAL:-10}"
POLL_TIMEOUT="${POLL_TIMEOUT:-3600}"
ARTIFACT_DIR="${ARTIFACT_DIR:-${SCRIPT_DIR}/artifacts}"
# Bench nodegroup size (sharded-preload capacity, review F5). Fit arithmetic on
# m6i.large (~7.1Gi allocatable): the broker+registry node has ~2.9Gi free (no
# 4Gi-request producer pod fits there); each OTHER bench node fits exactly ONE
# 4Gi-request shard pod — so SHARD_COUNT=3 needs 1 broker node + 3 producer
# nodes = 4. Consumed by phase_scale_up (and, in run_pair.sh, print_plan +
# phase_pair_cost). Chaos overrides it (T11: SCALE_UP_NODES=5). Keep in sync
# with infra/env.sh SCALE_UP_NODES + cluster.yaml bench-ng maxSize.
SCALE_UP_NODES="${SCALE_UP_NODES:-4}"

# Log prefix (IC-1): chaos sets BENCH_LOG_PREFIX=chaos_run; the pair keeps the
# historical `run_pair` prefix so its log lines are byte-identical.
BENCH_LOG_PREFIX="${BENCH_LOG_PREFIX:-run_pair}"

log()  { printf '\033[1;34m[%s]\033[0m %s\n' "${BENCH_LOG_PREFIX}" "$*" >&2; }
warn() { printf '\033[1;33m[%s:warn]\033[0m %s\n' "${BENCH_LOG_PREFIX}" "$*" >&2; }
die()  { printf '\033[1;31m[%s:error]\033[0m %s\n' "${BENCH_LOG_PREFIX}" "$*" >&2; exit 1; }

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
# runtime-map echo (contract §1 + §1.4 + overseer directive c). Every plan §6
# setting appears here AND in kafkaconnector.json.tmpl (cross-checked by
# tests/test_orchestration.py). Emits a JSON object on stdout.
#
# Review F9: the mandatory identity/scope keys (arm, tier, pair_id,
# target_region, environment_class, compute_region) HARD-FAIL when empty —
# they must never silently vanish from a runs row. Optional provenance keys
# are still dropped when empty (absent, not "").
#
# IC-1 parameterization: the optional env RUNTIME_EXTRA_KEYS_JSON (a flat JSON
# object) is merged into the map AFTER the mandatory-key check. Chaos passes the
# §4 vocabulary (connector, chaos_id, chaos_mode, chaos_seed, fault_type, …); a
# key that collides with a built-in is a hard error, and an empty/absent value
# leaves the pair output byte-identical.
#
# Args: arm tier
# Env : PAIR_ID, TARGET_REGION, ENVIRONMENT_CLASS, COMPUTE_REGION, CFG_*,
#       KAFKA_CONNECT_VERSION, STRIMZI_VERSION, PLUGIN_SHA256,
#       RUNTIME_EXTRA_KEYS_JSON (optional).
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
# IC-1: the built-in key namespace, snapshot BEFORE dropping empties so a
# RUNTIME_EXTRA_KEYS_JSON collision is caught even against an optional key that
# would otherwise be dropped (chaos must never overwrite a pair identity/config
# key, present or not).
_builtin_keys = set(rt.keys())
# drop empty OPTIONAL values so a missing provenance value is absent, not "".
rt = {k: v for k, v in rt.items() if v != ""}
# IC-1: merge RUNTIME_EXTRA_KEYS_JSON (the chaos §4 vocabulary) AFTER the
# mandatory-key check. Empty/absent => the map is untouched (byte-identical pair
# output). A flat JSON object is required; a collision with a built-in key is a
# hard error.
_extra_raw = os.environ.get("RUNTIME_EXTRA_KEYS_JSON", "")
if _extra_raw:
    try:
        _extra = json.loads(_extra_raw)
    except Exception as e:
        print(f"ERROR: RUNTIME_EXTRA_KEYS_JSON is not valid JSON: {e}",
              file=sys.stderr)
        sys.exit(1)
    if not isinstance(_extra, dict):
        print("ERROR: RUNTIME_EXTRA_KEYS_JSON must be a flat JSON object "
              "of string keys (got a non-object).", file=sys.stderr)
        sys.exit(1)
    _collisions = sorted(k for k in _extra if k in _builtin_keys)
    if _collisions:
        print(f"ERROR: RUNTIME_EXTRA_KEYS_JSON key(s) collide with built-in "
              f"runtime keys: {_collisions} — refusing to overwrite pair "
              f"identity/config keys (IC-1).", file=sys.stderr)
        sys.exit(1)
    rt.update(_extra)
print(json.dumps(rt, sort_keys=True))
PY
}

# =========================================================================== #
# EXECUTION PHASES (each a function; only run when a caller invokes them —
# sourcing this file NEVER executes any of them, IC-1).
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

  # Producer Job (fresh; backoffLimit 0). PARALLEL SHARDED PRELOAD: an INDEXED
  # Job of SHARD_COUNT pods, each producing a disjoint stride-N slice of the
  # parquet files (cuts the ~20min single-producer preload to ~5-6min). The
  # preload is UNMEASURED, so this parallelism never touches the instrument.
  # Overseer directive e: parameterize an IRSA serviceAccountName for S3 read
  # (PRODUCER_SA). Image ref + parquet source come from the workflow env.
  kubectl -n "${NS}" delete job hits-producer --ignore-not-found --wait=true --timeout=120s 2>/dev/null || true
  local job_yaml="${ARTIFACT_DIR}/producer-job.yaml"
  mkdir -p "${ARTIFACT_DIR}"
  local shard_count="${SHARD_COUNT:-3}"
  # Patch image + SA + PARQUET_SOURCE + completions/parallelism/SHARD_COUNT into
  # a copy of job.yaml (render enforces completions==parallelism==SHARD_COUNT).
  python3 "${SCRIPT_DIR}/render_producer_job.py" \
      --job "${PRODUCER_DIR}/job.yaml" \
      --image "${PRODUCER_IMAGE:?PRODUCER_IMAGE required}" \
      --service-account "${PRODUCER_SA:-hits-producer}" \
      --parquet-source "${PARQUET_SOURCE:?PARQUET_SOURCE required}" \
      --shard-count "${shard_count}" \
      --out "${job_yaml}" || die "producer job render failed"
  kubectl apply -f "${job_yaml}" || die "producer job apply failed"
  log "  producer Job applied (Indexed, ${shard_count} shard pod(s))"

  # Wait for a TERMINAL state — Complete OR Failed (live fix). The previous
  # one-sided `kubectl wait --for=condition=complete` could not see Failed:
  # with backoffLimit=0 the FIRST pod failure is terminal (Indexed Job), and
  # waiting out the full PRODUCER_TIMEOUT (6h default) on an already-failed
  # pre-load burns cluster-hours for nothing. Poll both Job conditions and die
  # fast on Failed, dumping the failed pods' last log lines for diagnosis.
  log "  waiting for producer Job (terminal: Complete OR Failed; timeout ${PRODUCER_TIMEOUT:-21600}s)"
  local jdeadline=$(( $(date +%s) + ${PRODUCER_TIMEOUT:-21600} ))
  while :; do
    local jconds
    jconds="$(kubectl -n "${NS}" get job hits-producer \
                -o jsonpath='{range .status.conditions[*]}{.type}={.status} {end}' 2>/dev/null)"
    case " ${jconds}" in
      *" Complete=True"*)
        log "  producer Job Complete (${shard_count} shard(s))"
        # Observability (best-effort): relay each pod's final peak_rss line into
        # the runner log so the operator sees every shard's memory high-water
        # mark. --prefix tags each line with its pod so the shards are separable.
        { kubectl -n "${NS}" logs job/hits-producer --tail=-1 --prefix 2>/dev/null \
            | grep -i "peak_rss" \
            | sed 's/^/  producer final: /' >&2; } || true
        break ;;
      *" Failed=True"*)
        warn "producer Job FAILED (backoffLimit=0: first pod failure is terminal). Last log lines:"
        kubectl -n "${NS}" logs job/hits-producer --tail=40 --prefix 2>/dev/null | sed 's/^/    | /' >&2 || true
        die "producer Job failed — pre-load invalid, aborting the pair before any measured drain" ;;
    esac
    [ "$(date +%s)" -ge "${jdeadline}" ] \
      && die "producer Job reached neither Complete nor Failed within ${PRODUCER_TIMEOUT:-21600}s"
    sleep 15
  done

  # ------------------------------------------------------------------------- #
  # GLOBAL rows_expected — the AUTHORITATIVE count contract (overseer
  # directive 2), computed by the ORCHESTRATOR from the BROKER after the whole
  # Job completes, NOT from producer send counts. Under sharding each pod sends
  # 1/N of the data but the topic END OFFSETS reflect the global total, so the
  # per-pod offset check was removed; this is the one true count.
  #
  #   rows_expected = Σ_partitions (end_offset − beginning_offset)
  #
  # via kafka-get-offsets.sh inside the broker pod — the SAME bin/ broker-exec
  # convention already used for kafka-topics.sh (partition assert above) and
  # kafka-consumer-groups.sh (delete_consumer_group.sh). The Strimzi 0.46 /
  # Kafka 3.9 image ships bin/kafka-get-offsets.sh. This is producer-count-
  # agnostic — the property that keeps preload SPEED irrelevant to accuracy.
  # ------------------------------------------------------------------------- #
  ROWS_EXPECTED="$(broker_topic_row_count "${TOPIC}")" \
    || die "could not derive rows_expected from broker offsets for ${TOPIC}"
  case "${ROWS_EXPECTED}" in
    ''|*[!0-9]*) die "broker offset derivation returned non-numeric rows_expected='${ROWS_EXPECTED}'" ;;
  esac
  [ "${ROWS_EXPECTED}" -gt 0 ] 2>/dev/null \
    || die "rows_expected=${ROWS_EXPECTED} from broker offsets — the pre-load produced nothing (empty/wrong parquet source?)"
  export ROWS_EXPECTED
  export SOURCE_ROWS_EXPECTED="${ROWS_EXPECTED}"
  log "  rows_expected = ${ROWS_EXPECTED} (broker end-minus-beginning offsets)"

  # Best-effort cross-check: sum the N pods' rows_sent (last JSON line PER
  # completion index) and WARN on mismatch. run_pair's old "last JSON line of
  # the whole Job log" cannot survive N interleaved pods, so parse per-index.
  # This is a diagnostic only: parse problems NEVER fail the pair (offsets are
  # authoritative). It fails the preload ONLY when a clean per-pod sum is
  # available AND disagrees with the broker offsets (that means duplicate or
  # lost records — the pre-load IS invalid).
  local plog; plog="$(kubectl -n "${NS}" logs job/hits-producer --tail=-1 --prefix 2>/dev/null)"
  echo "${plog}" > "${ARTIFACT_DIR}/producer.log"
  local sum_sent
  sum_sent="$(producer_rows_sent_sum "${shard_count}")"
  if [ -n "${sum_sent}" ] && [ "${sum_sent}" != "PARSE_FAIL" ]; then
    if [ "${sum_sent}" = "${ROWS_EXPECTED}" ]; then
      log "  cross-check OK: Σ rows_sent over ${shard_count} shard(s) = ${sum_sent} == broker offsets"
    else
      die "COUNT MISMATCH: Σ rows_sent over shards = ${sum_sent} != broker offsets = ${ROWS_EXPECTED}. Idempotence should make these equal; a mismatch means the pre-load is INVALID (duplicate or lost records)."
    fi
  else
    warn "could not cleanly sum per-shard rows_sent (log parse) — relying on broker offsets alone (authoritative); NOT failing the pair"
  fi
}

# --------------------------------------------------------------------------- #
# Broker-derived topic row count (the authoritative rows_expected). Sums
# (latest − earliest) over every partition using kafka-get-offsets.sh inside
# the broker pod. --time -1 = latest (end) offset, --time -2 = earliest
# (beginning) offset; output lines are "topic:partition:offset". A fresh RF1
# topic has earliest 0 on every partition, but we subtract it explicitly so a
# retained/compacted topic still counts correctly. Echoes the integer sum on
# stdout; returns non-zero (no output) if either offset read fails.
# --------------------------------------------------------------------------- #
broker_topic_row_count() {
  local topic="$1"
  local latest earliest
  latest="$(kubectl -n "${NS}" exec "${BROKER_POD}" -- \
              bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
              --topic "${topic}" --time -1 2>/dev/null)" || return 1
  earliest="$(kubectl -n "${NS}" exec "${BROKER_POD}" -- \
                bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
                --topic "${topic}" --time -2 2>/dev/null)" || return 1
  [ -n "${latest}" ] || return 1
  # Pure text math in python (portable; no bc). Sums latest, subtracts earliest,
  # keyed by partition so a differing line order between the two reads is safe.
  printf 'LATEST\n%s\nEARLIEST\n%s\n' "${latest}" "${earliest}" | python3 -c '
import sys
section = None
end = {}
beg = {}
for line in sys.stdin:
    line = line.strip()
    if line in ("LATEST", "EARLIEST"):
        section = line
        continue
    if not line:
        continue
    # topic:partition:offset  (offset may be empty for a partition with no leader)
    parts = line.split(":")
    if len(parts) != 3:
        continue
    _, p, off = parts
    if off == "":
        continue
    (end if section == "LATEST" else beg)[p] = int(off)
total = sum(end.get(p, 0) - beg.get(p, 0) for p in end)
print(total)
' || return 1
}

# --------------------------------------------------------------------------- #
# Sum the N shard pods rows_sent from the producer Job log. Each pod prints its
# summary as the LAST JSON line of its OWN log; with N interleaved pods in one
# `logs job/...` stream we cannot take "the last JSON line" globally. We fetch
# each completion index's pod log separately (label
# batch.kubernetes.io/job-completion-index=<i>) and parse that pod's last JSON
# line. Echoes the integer sum on success; echoes "PARSE_FAIL" (never fails the
# caller) if any pod's summary is unreadable — the broker offsets are
# authoritative, this is only a cross-check.
# --------------------------------------------------------------------------- #
producer_rows_sent_sum() {
  local shard_count="$1"
  local i total=0 pod plog rs
  for (( i=0; i<shard_count; i++ )); do
    pod="$(kubectl -n "${NS}" get pod \
             -l "job-name=hits-producer,batch.kubernetes.io/job-completion-index=${i}" \
             -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
    [ -n "${pod}" ] || { echo "PARSE_FAIL"; return 0; }
    plog="$(kubectl -n "${NS}" logs "${pod}" --tail=-1 2>/dev/null)"
    rs="$(printf '%s\n' "${plog}" | grep '^{' | tail -1 \
          | python3 -c 'import sys,json
try:
    print(json.load(sys.stdin)["rows_sent"])
except Exception:
    print("")' 2>/dev/null)"
    case "${rs}" in
      ''|*[!0-9]*) echo "PARSE_FAIL"; return 0 ;;
    esac
    total=$(( total + rs ))
  done
  echo "${total}"
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
  # hostname/username REQUIRE a non-empty value (${VAR:?}); password requires the
  # var to be SET but ALLOWS EMPTY (${VAR?}) — the self-hosted chaos CH uses user
  # `default` with an empty password (ch-cluster.yaml users.xml). The pair always
  # sets a non-empty password, so ${TARGET_CH_PASSWORD?} is a no-op there.
  kubectl -n "${NS}" create secret generic bench-ch-creds \
    --from-literal=hostname="${TARGET_CH_HOST:?}" \
    --from-literal=username="${TARGET_CH_USER:?}" \
    --from-literal=password="${TARGET_CH_PASSWORD?}" \
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
  # (plan §5 step 3c). Best-effort via the broker pod. Let the script's stderr
  # through: it already silences GroupIdNotFoundException (B5) itself, so a
  # blanket 2>/dev/null here would only hide a REAL delete error before the warn.
  "${SCRIPT_DIR}/delete_consumer_group.sh" "${group}" || warn "consumer-group delete best-effort failed for ${group}"
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

phase_teardown_topic() {
  log "PHASE 4: teardown topic (always)"
  kubectl -n "${NS}" delete kafkatopic "${TOPIC}" --ignore-not-found --wait=false 2>/dev/null || true
}

phase_scale_down() {
  log "PHASE 5: scale down (always)"
  "${INFRA_DIR}/scale-down.sh" || warn "scale-down reported non-zero — check for cost leak"
}
