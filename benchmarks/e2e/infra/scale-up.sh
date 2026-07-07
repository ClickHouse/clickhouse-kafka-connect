#!/usr/bin/env bash
# Scale the managed node group from 0 -> N, wait for nodes Ready, apply the
# Kafka + Schema Registry manifests, and wait for the Kafka CR to be Ready.
# Idempotent and CI-safe: re-running when already scaled just re-applies the
# manifests and re-waits (no error if already up).
#
# Usage:  ./scale-up.sh [N]      (N defaults to $SCALE_UP_NODES, i.e. 2)
#
# Requires live AWS credentials, eksctl, kubectl.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

NODES="${1:-${SCALE_UP_NODES}}"

require eksctl kubectl aws
require_aws_creds

log "scaling node group ${NODEGROUP_NAME} to ${NODES} node(s)"
# --nodes sets desired; keep min at 0 (scale-to-zero) and ensure max >= N.
eksctl scale nodegroup \
  --cluster "${CLUSTER_NAME}" \
  --region "${AWS_REGION}" \
  --name "${NODEGROUP_NAME}" \
  --nodes "${NODES}" \
  --nodes-min 0 \
  --nodes-max "${NODES}"

log "waiting for ${NODES} node(s) to reach Ready"
# Wait until at least N nodes are Ready. kubectl wait needs objects to exist,
# so poll until the count is present, then wait on readiness.
deadline=$(( $(date +%s) + 600 ))
while :; do
  ready=$(kubectl get nodes --no-headers 2>/dev/null | grep -cw Ready || true)
  [ "${ready}" -ge "${NODES}" ] && break
  [ "$(date +%s)" -ge "${deadline}" ] && die "timed out waiting for ${NODES} Ready node(s) (have ${ready})"
  log "  ${ready}/${NODES} nodes Ready; waiting..."
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

log "scale-up complete: ${NODES} node(s), Kafka bench Ready, Schema Registry up."
log "  bootstrap: bench-kafka-bootstrap.${K8S_NAMESPACE}.svc:9092"
log "  registry : http://schema-registry.${K8S_NAMESPACE}.svc:8081"
