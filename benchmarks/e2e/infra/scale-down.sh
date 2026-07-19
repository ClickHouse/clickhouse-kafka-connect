#!/usr/bin/env bash
# Scale BOTH managed node groups (bench-ng + connect-ng) to ZERO and clean
# transient per-run K8s state, so between runs the only paid footprint is the
# EKS control plane (~$0.10/hr). connect-ng is the dedicated Connect-worker
# nodegroup added in the 2026-07-08 rebuild (worker was CPU-bound on the shared
# m6i.large); it scales to zero exactly like bench-ng.
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
# Wait (bounded) for PVC deletion BEFORE dropping nodes. The EBS CSI
# controller runs on the nodes: if we scale to 0 while a PVC is still
# Terminating, nothing detaches/deletes the 70Gi EBS volume and it lingers
# (billed) until the next scale-up. Best-effort with a hard timeout — a
# timeout is a loud warning, never a scale-down blocker.
# ---------------------------------------------------------------------------
wait_for_pvc_deletion() {
  command -v kubectl >/dev/null 2>&1 || return 0
  kubectl get ns "${K8S_NAMESPACE}" >/dev/null 2>&1 || return 0

  local timeout="${PVC_DELETE_TIMEOUT_SECONDS:-300}"
  local deadline=$(( $(date +%s) + timeout ))
  local remaining
  log "waiting up to ${timeout}s for PVCs in ${K8S_NAMESPACE} to finish deleting (EBS CSI needs nodes to detach/delete volumes)"
  while :; do
    remaining="$(kubectl -n "${K8S_NAMESPACE}" get pvc --no-headers 2>/dev/null | wc -l | tr -d ' ')"
    remaining="${remaining:-0}"
    if [ "${remaining}" = "0" ]; then
      log "all PVCs gone — EBS volumes released"
      return 0
    fi
    if [ "$(date +%s)" -ge "${deadline}" ]; then
      warn "TIMEOUT: ${remaining} PVC(s) still present after ${timeout}s. Scaling down anyway —"
      warn "the backing EBS volume(s) (~70Gi gp3) may persist, billed, until the next scale-up"
      warn "finishes the deletion. Check 'kubectl -n ${K8S_NAMESPACE} get pvc' / the EC2 volumes console."
      return 0
    fi
    log "  ${remaining} PVC(s) still Terminating; waiting..."
    sleep 10
  done
}

wait_for_pvc_deletion

# ---------------------------------------------------------------------------
# Scale a node group to zero and verify it reached desired 0. Returns 0 on
# success, 1 on a real cost leak (did not reach 0). Covers BOTH nodegroups
# (bench-ng + the dedicated connect-ng, 2026-07-08 rebuild) — the exit-code
# contract is non-zero only if a nodegroup fails to reach 0.
# ---------------------------------------------------------------------------
scale_group_to_zero() {
  local ng="$1"
  log "scaling node group ${ng} to 0"
  # NOTE: EKS NodegroupScalingConfig.maxSize has an API minimum of 1, so
  # --nodes-max 0 would be rejected. desired=0 / min=0 / max=1 is the lowest
  # valid scaled-to-zero shape (scale-up.sh raises max again when scaling up).
  if ! eksctl scale nodegroup \
        --cluster "${CLUSTER_NAME}" \
        --region "${AWS_REGION}" \
        --name "${ng}" \
        --nodes 0 \
        --nodes-min 0 \
        --nodes-max 1 2>/dev/null; then
    warn "eksctl scale ${ng} returned non-zero (may already be at 0 or mid-transition); verifying actual state"
  fi

  # Read back desired capacity. `eksctl scale` returns before the ASG desired
  # is updated (async), so a single immediate read RACES the update and can see
  # the pre-scale value. Poll with a bounded retry (12x10s = 2min) and judge
  # only the final observed value.
  read_desired() {
    eksctl get nodegroup \
        --cluster "${CLUSTER_NAME}" \
        --region "${AWS_REGION}" \
        --name "${ng}" \
        -o json 2>/dev/null \
      | python3 -c 'import sys,json
try:
    d=json.load(sys.stdin)
    print(d[0].get("DesiredCapacity", d[0].get("Desired", "unknown")))
except Exception:
    print("unknown")' 2>/dev/null || echo unknown
  }

  log "verifying node group ${ng} desired capacity reaches 0 (bounded retry: eksctl scale is async)"
  local desired i
  for i in $(seq 1 12); do
    desired="$(read_desired)"
    if [ "${desired}" = "0" ]; then
      break
    fi
    log "  attempt ${i}/12: desired='${desired}' (not 0 yet); waiting 10s for async ASG update"
    sleep 10
  done

  if [ "${desired}" != "0" ]; then
    warn "node group ${ng} did NOT reach desired 0 after retries (desired='${desired}')."
    return 1
  fi

  # desired=0 is reached. The LAST node commonly lingers ~15min in
  # Terminating:Wait under the EKS-managed drain lifecycle hook: at zero the
  # kube-system coredns PodDisruptionBudget is unsatisfiable, so the hook holds
  # the instance until its heartbeat timeout, then force-terminates. This is
  # EXPECTED, not a leak — desired=0 means the ASG will not replace it and the
  # cost is bounded (~15min of one node, ~$0.02). Only desired!=0 is a leak.
  local instances
  instances="$(aws ec2 describe-instances \
        --region "${AWS_REGION}" \
        --filters "Name=tag:eks:nodegroup-name,Values=${ng}" \
                  "Name=instance-state-name,Values=pending,running,shutting-down,stopping" \
        --query 'length(Reservations[].Instances[])' \
        --output text 2>/dev/null || echo unknown)"
  if [ -n "${instances}" ] && [ "${instances}" != "0" ] && [ "${instances}" != "unknown" ]; then
    log "  node group ${ng}: desired=0 reached; last node(s) draining under lifecycle hook (expected, ~15min, ~\$0.02) — NOT a leak (${instances} instance(s) still Terminating)."
  else
    log "  node group ${ng} desired capacity is 0."
  fi
  return 0
}

# ---------------------------------------------------------------------------
# Scale BOTH node groups to zero. This is the step whose success CI depends on.
# Run both regardless of individual outcome (best-effort push-through), then
# fail non-zero if EITHER did not reach 0 so CI can alert on a real cost leak.
# ---------------------------------------------------------------------------
rc=0
scale_group_to_zero "${NODEGROUP_NAME}" || rc=1
scale_group_to_zero "${CONNECT_NODEGROUP_NAME}" || rc=1

if [ "${rc}" = "0" ]; then
  log "scale-down complete: both node groups (${NODEGROUP_NAME}, ${CONNECT_NODEGROUP_NAME}) at desired 0. Only the EKS control plane persists."
  exit 0
fi

die "at least one node group did NOT reach desired 0 (see warnings above). Compute may still be running — INVESTIGATE (potential cost leak)."
