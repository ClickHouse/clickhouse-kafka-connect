#!/usr/bin/env bash
# Full teardown: delete the entire EKS cluster (control plane, node group, VPC,
# and all eksctl-created resources). Use this to stop ALL cost, including the
# control plane — NOT the between-runs path (that is scale-down.sh, which keeps
# the control plane so the next run spins up in minutes).
#
# Idempotent: succeeds (with a warning) if the cluster does not exist.
#
# Usage:  ./teardown.sh
# Requires live AWS credentials, eksctl.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=env.sh
source "${SCRIPT_DIR}/env.sh"

require eksctl aws
require_aws_creds

if ! eksctl get cluster --name "${CLUSTER_NAME}" --region "${AWS_REGION}" >/dev/null 2>&1; then
  warn "cluster ${CLUSTER_NAME} not found in ${AWS_REGION}; nothing to tear down"
  exit 0
fi

log "deleting cluster ${CLUSTER_NAME} (control plane + node group + VPC). This can take ~15 min."
# --disable-nodegroup-eviction avoids hanging on PodDisruptionBudgets; the
# broker PVC (deleteClaim=true) and its EBS volume are removed with the stack.
if eksctl delete cluster --name "${CLUSTER_NAME}" --region "${AWS_REGION}" --disable-nodegroup-eviction; then
  log "teardown complete: cluster ${CLUSTER_NAME} deleted. No further EKS cost."
  exit 0
fi

die "cluster delete returned non-zero — verify in the AWS console that the CloudFormation stacks fully deleted (potential residual cost)."
