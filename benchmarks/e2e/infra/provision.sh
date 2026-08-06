#!/usr/bin/env bash
# One-shot: create the EKS cluster (node group starts at zero) and install the
# Strimzi operator + StorageClass. After this, the persistent footprint is the
# EKS control plane only (~$0.10/hr) — no paid compute until scale-up.sh.
#
# Idempotent-ish: `eksctl create cluster` is not itself idempotent, so this
# checks for an existing cluster first and skips creation if present, then
# always (re-)runs the operator install (which IS idempotent).
#
# THE single command a credentialed operator runs to provision:
#     ./provision.sh
#
# Requires live AWS credentials (checked up front) and: eksctl, kubectl, aws.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

require eksctl kubectl aws
require_aws_creds

if eksctl get cluster --name "${CLUSTER_NAME}" --region "${AWS_REGION}" >/dev/null 2>&1; then
  log "cluster ${CLUSTER_NAME} already exists; skipping create"
else
  log "creating cluster ${CLUSTER_NAME} from cluster.yaml (node group at desiredCapacity 0)"
  eksctl create cluster -f "${INFRA_DIR}/cluster.yaml"
fi

log "writing kubeconfig for ${CLUSTER_NAME}"
eksctl utils write-kubeconfig --cluster "${CLUSTER_NAME}" --region "${AWS_REGION}"

log "installing Strimzi operator + storage class"
"${INFRA_DIR}/install-strimzi.sh"

log "provision complete. Node group is at zero — run ./scale-up.sh before a benchmark run."
