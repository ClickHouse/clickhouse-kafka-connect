#!/usr/bin/env bash
# Install the pinned Strimzi operator (namespace-scoped: deployed into and
# watching the benchmark namespace) and apply the namespace + gp3 StorageClass.
# Idempotent: re-running re-applies the same pinned manifests.
#
# This step does NOT require nodes to be up — the operator Deployment schedules
# once nodes exist (scale-up.sh brings them up and waits for the Kafka CR).
#
# Requires: kubectl configured for the cluster (provision.sh does this via
# `eksctl utils write-kubeconfig`). No AWS calls here.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

require kubectl

log "creating namespace ${K8S_NAMESPACE}"
kubectl apply -f "${INFRA_DIR}/namespace.yaml"

log "installing Strimzi operator ${STRIMZI_VERSION} (watching ${K8S_NAMESPACE})"
# Pinned GitHub release bundle (the strimzi.io/install/<version> shortcut does
# NOT exist — only install/latest — and returns 404 for pinned versions). The
# bundle hardcodes `namespace: myproject`; rewrite every occurrence to our
# namespace before applying (this covers both metadata.namespace and the
# RoleBinding subject namespaces, all of which mean "the deploy namespace").
STRIMZI_URL="https://github.com/strimzi/strimzi-kafka-operator/releases/download/${STRIMZI_VERSION}/strimzi-cluster-operator-${STRIMZI_VERSION}.yaml"
require curl
curl -fsSL "${STRIMZI_URL}" \
  | sed "s/namespace: myproject/namespace: ${K8S_NAMESPACE}/g" \
  | kubectl apply -n "${K8S_NAMESPACE}" -f -

log "applying gp3 StorageClass"
kubectl apply -f "${INFRA_DIR}/storageclass-gp3.yaml"

log "waiting for the Strimzi cluster operator deployment to be Available"
# The operator pod itself needs a node; if the group is at zero this waits.
# scale-up.sh is the normal path that guarantees a node first, but tolerate
# both orders here.
kubectl -n "${K8S_NAMESPACE}" rollout status deploy/strimzi-cluster-operator --timeout=300s \
  || warn "operator not yet Available (likely no nodes — run scale-up.sh); manifests are applied and will reconcile"

log "Strimzi operator ${STRIMZI_VERSION} manifests applied"
