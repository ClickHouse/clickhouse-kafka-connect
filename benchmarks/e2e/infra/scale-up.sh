#!/usr/bin/env bash
# Scale BOTH managed node groups up (bench-ng 0 -> N, connect-ng 0 -> 1), wait
# for nodes Ready across both, apply the Kafka + Schema Registry manifests, and
# wait for the Kafka CR to be Ready. Idempotent and CI-safe: re-running when
# already scaled just re-applies the manifests and re-waits (no error if up).
#
# The Connect worker is CPU-bound on the shared m6i.large, so it runs alone on
# the dedicated connect-ng m6i.xlarge node (2026-07-08 rebuild).
#
# Usage:  ./scale-up.sh [N]      (N = bench-ng nodes; defaults to $SCALE_UP_NODES,
#                                 i.e. 2. connect-ng scales to $CONNECT_NODES=1.)
#
# Requires live AWS credentials, eksctl, kubectl.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

NODES="${1:-${SCALE_UP_NODES}}"

require eksctl kubectl aws
require_aws_creds

# Total nodes we expect Ready across BOTH nodegroups: bench-ng (N) + connect-ng
# (CONNECT_NODES, baseline 1). The Connect worker is CPU-bound on the shared
# m6i.large, so it runs alone on the dedicated connect-ng m6i.xlarge node.
TOTAL_NODES=$(( NODES + CONNECT_NODES ))

log "scaling node group ${NODEGROUP_NAME} to ${NODES} node(s)"
# --nodes sets desired; keep min at 0 (scale-to-zero) and ensure max >= N.
eksctl scale nodegroup \
  --cluster "${CLUSTER_NAME}" \
  --region "${AWS_REGION}" \
  --name "${NODEGROUP_NAME}" \
  --nodes "${NODES}" \
  --nodes-min 0 \
  --nodes-max "${NODES}"

log "scaling connect node group ${CONNECT_NODEGROUP_NAME} to ${CONNECT_NODES} node(s)"
eksctl scale nodegroup \
  --cluster "${CLUSTER_NAME}" \
  --region "${AWS_REGION}" \
  --name "${CONNECT_NODEGROUP_NAME}" \
  --nodes "${CONNECT_NODES}" \
  --nodes-min 0 \
  --nodes-max "${CONNECT_NODES}"

# Count Ready, schedulable nodes matching a label selector. A cordoned node
# shows "Ready,SchedulingDisabled" — it must NOT satisfy the wait (pods cannot
# land on it), so exclude it. Empty/failed kubectl -> 0.
count_ready_nodes() {
  local n
  n=$(kubectl get nodes -l "$1" --no-headers 2>/dev/null \
        | grep -w Ready | grep -cv SchedulingDisabled || true)
  echo "${n:-0}"
}

log "waiting for ${TOTAL_NODES} node(s) to reach Ready (${NODES} role=bench + ${CONNECT_NODES} role=connect)"
# Per-nodegroup label-filtered counts (not a single total): a total of N+1 could
# be satisfied by N+1 bench nodes while connect-ng is still coming up, letting
# the Kafka apply race ahead of the node the worker requires. kubectl wait needs
# objects to exist, so poll counts first, then wait on readiness.
deadline=$(( $(date +%s) + 600 ))
while :; do
  bench_ready=$(count_ready_nodes "role=bench")
  connect_ready=$(count_ready_nodes "role=connect")
  [ "${bench_ready}" -ge "${NODES}" ] && [ "${connect_ready}" -ge "${CONNECT_NODES}" ] && break
  [ "$(date +%s)" -ge "${deadline}" ] && die "timed out waiting for nodes Ready (bench ${bench_ready}/${NODES}, connect ${connect_ready}/${CONNECT_NODES})"
  log "  bench ${bench_ready}/${NODES}, connect ${connect_ready}/${CONNECT_NODES} nodes Ready; waiting..."
  sleep 10
done
kubectl wait --for=condition=Ready nodes --all --timeout=300s

# Ensure the operator/storageclass are present (idempotent) in case the
# cluster was provisioned in a prior session.
log "ensuring Strimzi operator + storage class are installed"
"${INFRA_DIR}/install-strimzi.sh"
kubectl -n "${K8S_NAMESPACE}" rollout status deploy/strimzi-cluster-operator --timeout=300s

log "applying poller RBAC (bench-poller-sa + nodes/proxy get for the cadvisor CPU source)"
# Least-privilege ServiceAccount the in-cluster poller pod runs as, granting the
# kubelet cadvisor scrape via the API-server node proxy (poller prerequisite 2).
# Cluster-scoped + idempotent — safe to re-apply on every scale-up.
#
# A ClusterRoleBinding's roleRef is IMMUTABLE: `kubectl apply` cannot repoint a
# pre-existing binding of the same name, so a binding left over from an earlier
# (broken) manifest would silently survive the apply and the poller would keep
# getting 403 on the cadvisor scrape (pair-2 symptom: `can-i get nodes/proxy`
# NO despite the objects "looking correct"). Delete the binding first so the
# apply always installs the current roleRef. The ClusterRole/SA update in place.
#
# The NAMESPACED RoleBinding bench-poller-pods has the same immutable-roleRef
# constraint, so delete-before-apply it too — otherwise a stale roleRef survives
# the apply and the pods get/list grant silently fails to update.
kubectl delete clusterrolebinding bench-poller-cadvisor --ignore-not-found
kubectl -n "${K8S_NAMESPACE}" delete rolebinding bench-poller-pods --ignore-not-found
kubectl apply -f "${INFRA_DIR}/poller-rbac.yaml"

# Preflight (fail loud BEFORE a drain pays for it): assert the SA's REAL access
# with a live token + raw request. Do NOT use `kubectl auth can-i` in any form —
# on this EKS cluster SSAR-style checks (impersonated OR token'd) return a false
# "no" for nodes/proxy while the actual GET is authorized (verified 2026-07-12:
# raw request -> NotFound [authorized past authz], can-i -> no). The sampler does
# raw GETs, so the raw probe is the truth. NotFound on the dummy node = authorized
# (403 Forbidden = genuinely denied). pods get is probed the same way (nodeName
# resolution — the ACTUAL cause of the 2026-07-09 blind gate).
SA="system:serviceaccount:${K8S_NAMESPACE}:bench-poller-sa"
log "preflight: probing ${SA} real access (cadvisor CPU gate)"
SA_TOKEN="$(kubectl create token bench-poller-sa -n "${K8S_NAMESPACE}" --duration=10m)"
# --request-timeout so an API-server blip is a bounded failure, not a hang that
# stalls the whole pair before it even starts.
proxy_probe="$(kubectl --token="${SA_TOKEN}" --request-timeout=30s get --raw "/api/v1/nodes/preflight-dummy-node/proxy/metrics/cadvisor" 2>&1 || true)"
if echo "${proxy_probe}" | grep -qi "forbidden"; then
  kubectl describe clusterrolebinding bench-poller-cadvisor 2>&1 | sed 's/^/    | /' >&2 || true
  die "poller RBAC preflight FAILED: ${SA} raw GET nodes/proxy is Forbidden — the cadvisor CPU gate would be BLIND for the whole pair. Check poller-rbac.yaml before running run_pair.sh."
elif echo "${proxy_probe}" | grep -Eqi "notfound|not found|\"code\": *404|404"; then
  # POSITIVE authorization signature: the request went PAST authz and the dummy
  # node simply does not exist. This is the only outcome that proves the grant.
  :
else
  # Neither Forbidden nor the expected NotFound/404 — e.g. a timeout, a
  # connection error, or an unrecognized shape. Don't assume authorized (the old
  # code passed on ANY non-Forbidden output, which would mask a probe that never
  # actually reached authz). Warn loudly with the raw output and continue.
  warn "poller RBAC preflight: nodes/proxy probe returned neither Forbidden nor NotFound/404 — cannot positively confirm authorization; continuing but the CPU gate may be blind. Raw probe output: $(echo "${proxy_probe}" | tr '\n' ' ')"
fi
if ! kubectl --token="${SA_TOKEN}" --request-timeout=30s get pods -n "${K8S_NAMESPACE}" -o name >/dev/null 2>&1; then
  die "poller RBAC preflight FAILED: ${SA} cannot list pods in ${K8S_NAMESPACE} — nodeName resolution would fail and the CPU gate would be BLIND (the 2026-07-09 failure mode). Ensure the bench-poller-pods Role+RoleBinding exist (poller-rbac.yaml)."
fi
log "  preflight OK: ${SA} raw nodes/proxy authorized + pods listable"

log "applying Kafka (KRaft, 1 broker RF1) + Schema Registry manifests"
kubectl apply -f "${INFRA_DIR}/kafka.yaml"
kubectl apply -f "${INFRA_DIR}/schema-registry.yaml"

log "waiting for Kafka CR 'bench' to be Ready (operator provisions broker + PVC)"
kubectl -n "${K8S_NAMESPACE}" wait kafka/bench --for=condition=Ready --timeout=600s

log "waiting for Schema Registry to be Available"
kubectl -n "${K8S_NAMESPACE}" rollout status deploy/schema-registry --timeout=300s

log "scale-up complete: ${TOTAL_NODES} node(s) (${NODES} bench + ${CONNECT_NODES} connect), Kafka bench Ready, Schema Registry up."
log "  bootstrap: bench-kafka-bootstrap.${K8S_NAMESPACE}.svc:9092"
log "  registry : http://schema-registry.${K8S_NAMESPACE}.svc:8081"
