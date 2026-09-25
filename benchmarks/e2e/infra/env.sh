#!/usr/bin/env bash
# Shared configuration + helpers for the Kafka-benchmark infra scripts.
# Sourced by provision.sh / scale-up.sh / scale-down.sh / teardown.sh /
# install-strimzi.sh. Contains NO credentials — the operator supplies AWS auth
# via their own profile / OIDC role before invoking any script.
#
# Deliberately sets NO shell flags: each script owns its own `set` line
# (scale-down.sh runs without -e by design; a `set -e` here would silently
# re-enable it in the sourcing shell and break that invariant).

# --- Cluster identity (keep in sync with cluster.yaml) ----------------------
export CLUSTER_NAME="${CLUSTER_NAME:-kafka-bench}"
# us-east-2: co-located with the dedicated ClickHouse Cloud target.
export AWS_REGION="${AWS_REGION:-us-east-2}"
export NODEGROUP_NAME="${NODEGROUP_NAME:-bench-ng}"
# Dedicated Connect nodegroup (2026-07-08 rebuild): the Connect worker was
# CPU-bound on the shared m6i.large, so it now runs alone on an m6i.xlarge
# node (cluster.yaml connect-ng). Scaled 0<->1 alongside bench-ng.
export CONNECT_NODEGROUP_NAME="${CONNECT_NODEGROUP_NAME:-connect-ng}"
# Node count the Connect nodegroup scales to for a run (baseline = 1 worker;
# scale-out is the #37 sweep's variable).
export CONNECT_NODES="${CONNECT_NODES:-1}"
export K8S_NAMESPACE="${K8S_NAMESPACE:-kafka-bench}"

# AWS account this benchmark lives in (context only — not used for auth here).
export AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-796575137974}"

# --- Pinned versions (bump deliberately; each bump is a substrate change) ---
# Strimzi operator release. The install manifest is fetched for this tag.
export STRIMZI_VERSION="${STRIMZI_VERSION:-0.46.0}"

# --- Scale defaults ---------------------------------------------------------
# Node count a run scales up to. FOUR bench nodes (sharded-preload capacity,
# review F5): broker+registry share one node (~2.9Gi free after them — no 4Gi
# producer pod fits), and each of the other 3 nodes fits exactly ONE 4Gi-request
# producer shard pod, so SHARD_COUNT=3 runs truly in parallel. Keep in sync with
# cluster.yaml bench-ng maxSize (4). Cost delta vs the old 2: +2 m6i.large x
# pair duration (~+$0.40/pair) — accepted cost-for-speed (preload ~20min ->
# ~5-6min); the extra nodes just idle during the drains (broker/registry/poller
# placement unchanged, Connect worker on its own connect-ng node), so the
# measured path is unaffected.
export SCALE_UP_NODES="${SCALE_UP_NODES:-4}"

# --- Directory of this script tree ------------------------------------------
INFRA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export INFRA_DIR

# --- Helpers ----------------------------------------------------------------
log()  { printf '\033[1;34m[infra]\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[1;33m[infra:warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[infra:error]\033[0m %s\n' "$*" >&2; exit 1; }

# Fail fast with a clear message if a required CLI is missing.
require() {
  local missing=0 c
  for c in "$@"; do
    command -v "$c" >/dev/null 2>&1 || { warn "missing required command: $c"; missing=1; }
  done
  [ "$missing" -eq 0 ] || die "install the missing command(s) above and retry"
}

# Verify live AWS credentials before any mutating operation. Every script that
# touches AWS calls this first so an expired token fails loudly and early
# rather than midway through provisioning.
require_aws_creds() {
  require aws
  if ! aws sts get-caller-identity >/dev/null 2>&1; then
    die "AWS credentials are missing or expired (sts get-caller-identity failed). Refresh them and retry."
  fi
  log "AWS identity OK: $(aws sts get-caller-identity --query Arn --output text)"
}
