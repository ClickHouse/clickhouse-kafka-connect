#!/usr/bin/env bash
# Scale the managed node group to ZERO and clean transient per-run K8s state,
# so between runs the only paid footprint is the EKS control plane (~$0.10/hr).
#
# HARDENED FOR FAILURE PATHS. The CI orchestrator calls this from an
# `if: always()` step after failed or aborted runs, so it MUST:
#   * be fully idempotent and succeed from ANY state — nodes NotReady, Kafka CR
#     not Ready or absent, nodegroup already at 0, operator missing, kubeconfig
#     stale. No step assumes a clean prior run.
#   * best-effort delete transient per-run state that would otherwise pin cost
#     once nodes go away. The broker's EBS volume is a PER-RUN PVC
#     (kafka.yaml node pool storage has deleteClaim: true, size 70Gi): deleting
#     the Kafka CR cascades PVC -> EBS deletion. A failed run that left the
#     broker up would otherwise leak a standing 70Gi gp3 volume. We therefore
#     tear the Kafka CR (and Schema Registry) down here, best-effort, before
#     dropping nodes. Nothing about the broker data is meant to survive a run.
#   * exit non-zero if the node group did NOT reach 0, so CI can alert on a
#     cost-leaking scale-down failure. Best-effort K8s cleanup failures are
#     WARNINGS (they do not block reaching zero); failing to reach zero is the
#     only hard error.
#
# Usage:  ./scale-down.sh
# Requires live AWS credentials, eksctl. kubectl cleanup is best-effort.

set -uo pipefail   # NOTE: not -e — this script must push through partial state
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

# require_aws_creds uses `set -e` semantics via die(); guard it so a creds
# failure is a clean hard error (we cannot scale without AWS).
require eksctl aws
require_aws_creds

# ---------------------------------------------------------------------------
# Best-effort transient cleanup (never blocks reaching zero).
# Guarded so a missing cluster / stale kubeconfig / absent CRD is a warning.
# ---------------------------------------------------------------------------
cleanup_transient() {
  command -v kubectl >/dev/null 2>&1 || { warn "kubectl not found; skipping K8s cleanup"; return 0; }
  if ! kubectl get ns "${K8S_NAMESPACE}" >/dev/null 2>&1; then
    warn "namespace ${K8S_NAMESPACE} not reachable; skipping K8s cleanup (nothing to clean or API down)"
    return 0
  fi

  log "best-effort: deleting per-run Kafka CR (cascades PVC -> EBS, deleteClaim=true)"
  kubectl -n "${K8S_NAMESPACE}" delete kafka bench --ignore-not-found --wait=false 2>/dev/null \
    || warn "could not delete Kafka CR (may be absent or CRD gone) — continuing"
  kubectl -n "${K8S_NAMESPACE}" delete kafkanodepool combined --ignore-not-found --wait=false 2>/dev/null \
    || warn "could not delete KafkaNodePool — continuing"

  log "best-effort: deleting Schema Registry"
  kubectl -n "${K8S_NAMESPACE}" delete -f "${INFRA_DIR}/schema-registry.yaml" --ignore-not-found --wait=false 2>/dev/null \
    || warn "could not delete Schema Registry — continuing"

  # Sweep any leftover PVCs in the namespace (belt-and-suspenders against a
  # PVC orphaned by a broker that died before the operator could reconcile).
  log "best-effort: sweeping leftover PVCs in ${K8S_NAMESPACE}"
  kubectl -n "${K8S_NAMESPACE}" delete pvc --all --ignore-not-found --wait=false 2>/dev/null \
    || warn "PVC sweep hit an error — continuing (EBS may need manual check)"
}

cleanup_transient

# ---------------------------------------------------------------------------
# Scale the node group to zero. This is the step whose success CI depends on.
# ---------------------------------------------------------------------------
log "scaling node group ${NODEGROUP_NAME} to 0"
if ! eksctl scale nodegroup \
      --cluster "${CLUSTER_NAME}" \
      --region "${AWS_REGION}" \
      --name "${NODEGROUP_NAME}" \
      --nodes 0 \
      --nodes-min 0 \
      --nodes-max 0 2>/dev/null; then
  warn "eksctl scale returned non-zero (nodegroup may already be at 0 or mid-transition); verifying actual state"
fi

# ---------------------------------------------------------------------------
# Verify the nodegroup actually reached (or is heading to) desired 0. Exit code
# reflects this so CI can alert on a real cost leak.
# ---------------------------------------------------------------------------
log "verifying node group desired capacity is 0"
desired="$(eksctl get nodegroup \
            --cluster "${CLUSTER_NAME}" \
            --region "${AWS_REGION}" \
            --name "${NODEGROUP_NAME}" \
            -o json 2>/dev/null \
          | python3 -c 'import sys,json
try:
    d=json.load(sys.stdin)
    print(d[0].get("DesiredCapacity", d[0].get("Desired", "unknown")))
except Exception:
    print("unknown")' 2>/dev/null || echo unknown)"

if [ "${desired}" = "0" ]; then
  log "scale-down complete: node group desired capacity is 0. Only the EKS control plane persists."
  exit 0
fi

die "node group did NOT reach desired 0 (desired='${desired}'). Compute may still be running — INVESTIGATE (potential cost leak)."
