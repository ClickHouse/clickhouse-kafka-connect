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

log "waiting for ${TOTAL_NODES} node(s) to reach Ready (${NODES} bench + ${CONNECT_NODES} connect)"
# Wait until at least TOTAL_NODES nodes are Ready. kubectl wait needs objects to
# exist, so poll until the count is present, then wait on readiness.
deadline=$(( $(date +%s) + 600 ))
while :; do
  ready=$(kubectl get nodes --no-headers 2>/dev/null | grep -cw Ready || true)
  [ "${ready}" -ge "${TOTAL_NODES}" ] && break
  [ "$(date +%s)" -ge "${deadline}" ] && die "timed out waiting for ${TOTAL_NODES} Ready node(s) (have ${ready})"
  log "  ${ready}/${TOTAL_NODES} nodes Ready; waiting..."
  sleep 10
done
kubectl wait --for=condition=Ready nodes --all --timeout=300s

# Ensure the operator/storageclass are present (idempotent) in case the
# cluster was provisioned in a prior session.
log "ensuring Strimzi operator + storage class are installed"
"${INFRA_DIR}/install-strimzi.sh"
kubectl -n "${K8S_NAMESPACE}" rollout status deploy/strimzi-cluster-operator --timeout=300s

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
