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
# Delete a Kafka consumer group so the next drain starts fresh at offset 0
# (plan §5 step 3c: "topic persists within the night so both arms drain
# identical bytes" — but each drain uses a fresh group). Best-effort: the
# orchestrator only warns if this fails (the per-(arm,tier) group name is
# already distinct, so a stale group would at worst be a no-op reuse; deleting
# it keeps system.consumer_groups clean).
#
# Runs kafka-consumer-groups.sh inside the broker pod (the Strimzi image ships
# it). The connector MUST already be deleted (a live member blocks deletion).
#
# Usage: delete_consumer_group.sh <group-id>
set -uo pipefail

GROUP="${1:?usage: delete_consumer_group.sh <group-id>}"
NS="${K8S_NAMESPACE:-kafka-bench}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-bench-kafka-bootstrap.${NS}.svc:9092}"
BROKER_POD="${BROKER_POD:-bench-combined-0}"

kubectl -n "${NS}" exec "${BROKER_POD}" -- \
  bin/kafka-consumer-groups.sh --bootstrap-server "${BOOTSTRAP}" \
  --delete --group "${GROUP}"
