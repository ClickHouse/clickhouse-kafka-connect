# Kafka Connect sink Benchmark v2 — EKS + Strimzi + Schema Registry infra (task 25)

Infrastructure-as-code for the benchmark substrate defined in
[`docs/benchmark-v2-plan.md`](../../../docs/benchmark-v2-plan.md) phase 1
(decisions 8 and 12): an EKS cluster hosting a **single-broker Strimzi Kafka
(KRaft, RF=1)**, a **Schema Registry**, and a Kafka Connect worker, with
**scale-to-zero** between nightly runs.

The broker is deliberately minimal and **not under test** — it only serves
sequential fetches to 3 Connect tasks (plan decision 8). This directory is the
complete, reproducible provisioning path: once fresh AWS credentials exist,
one command brings the cluster up.

> **Credentials status:** at authoring time the session AWS credentials were
> **expired** (`sts get-caller-identity` -> `ExpiredToken`). Nothing here was
> provisioned live. Everything was validated offline (see "Validation"). No
> credentials are embedded anywhere; every script fails fast via
> `require_aws_creds` if creds are missing/expired.

## AWS context (never hardcoded)

| | |
|---|---|
| Account | `796575137974` |
| Region  | **`us-east-2`** (co-located with the dedicated ClickHouse Cloud target) |
| Cluster | `kafka-bench` |
| Node group | `bench-ng` — 2× `m6i.large`, gp3, `minSize 0` (scale-to-zero) |
| Namespace | `kafka-bench` |
| Tags | `Project: clickbench-benchmark` on every resource |

The operator supplies AWS auth via their own profile / OIDC role **before**
running any script (see "CI secrets / roles"). No file here carries secrets.

### ClickHouse Cloud target — env-only, never committed

The benchmark's dedicated target is a **`clickhouse-staging.com`** service in
`us-east-2`. Its connection details are supplied to the run **only** via
environment variables and **must never appear in any committed file**:

```
TARGET_CH_HOST      # e.g. <service>.us-east-2.aws.clickhouse-staging.com
TARGET_CH_USER
TARGET_CH_PASSWORD
```

These are consumed by the connector/orchestration tasks (phases 2+), not by
this infra. They are listed here only so operators know they are env-sourced.

## Files

| File | Purpose |
|---|---|
| `cluster.yaml` | eksctl `ClusterConfig`: cluster + managed node group (desired 0), gp3, EBS CSI addon, tags, pinned k8s + AZs. |
| `namespace.yaml` | `kafka-bench` namespace. |
| `storageclass-gp3.yaml` | `gp3-bench` StorageClass (EBS CSI, WaitForFirstConsumer) for the broker PVC. |
| `kafka.yaml` | Strimzi `KafkaNodePool` + `Kafka` — KRaft, 1 combined node, RF=1 everywhere, 70Gi gp3 PVC (`deleteClaim: true`), ~2Gi broker heap. |
| `schema-registry.yaml` | Confluent `cp-schema-registry` Deployment + Service, pointing at the Strimzi bootstrap. |
| `env.sh` | Shared config + helpers (`require`, `require_aws_creds`); pins Strimzi version. Sourced by all scripts. NO credentials. |
| `install-strimzi.sh` | Applies namespace, pinned Strimzi operator, gp3 StorageClass. Idempotent, no AWS calls. |
| `provision.sh` | **One-shot**: create cluster (nodes at 0) + install operator. |
| `scale-up.sh` | Node group 0 -> N; wait nodes Ready; apply Kafka + Registry; wait Kafka CR Ready. Idempotent. |
| `scale-down.sh` | Node group -> 0 + best-effort transient cleanup. **Hardened for failure paths**; exit code reflects reaching 0. |
| `teardown.sh` | Full cluster delete (control plane included). Idempotent. |

## Schema Registry choice (plan open decision 6)

**Confluent `cp-schema-registry` (Community image), not Apicurio.** The insert
path uses the **Confluent `AvroConverter`** (plan section 6, decision-locked),
and cp-schema-registry is the reference implementation of exactly that wire
format (magic byte + 4-byte schema id). It is a 1:1 match — no compatibility
shim, no Apicurio `ccompat` endpoint, no alternate id-encoding. Wire
compatibility with the AvroConverter is the deciding criterion, and Confluent
satisfies it natively.

## Runbook

All commands run from this directory (`benchmarks/e2e/infra/`). Ensure
`eksctl`, `kubectl`, `aws`, and `python3` are installed and AWS creds are live
(`aws sts get-caller-identity` must succeed).

### 1. Provision (one-time; leaves only the control plane paid)

```bash
./provision.sh
```

Creates the cluster with the node group at **desiredCapacity 0** and installs
the Strimzi operator. After this the only cost is the EKS control plane.

### 2. Scale up (start of a run)

```bash
./scale-up.sh          # -> $SCALE_UP_NODES nodes (default 2)
# or ./scale-up.sh 2
```

Waits for nodes Ready, applies Kafka + Schema Registry, waits for the Kafka CR
`Ready`. On success it prints the in-cluster endpoints:

```
bootstrap: bench-kafka-bootstrap.kafka-bench.svc:9092
registry : http://schema-registry.kafka-bench.svc:8081
```

### 3. Scale down (end of a run — and on failure, see below)

```bash
./scale-down.sh
```

Best-effort deletes the per-run Kafka CR (cascading its 70Gi PVC/EBS),
Schema Registry, and any stray PVCs, then **waits (bounded, default 300s via
`PVC_DELETE_TIMEOUT_SECONDS`) for PVC deletion to finish before dropping
nodes** — the EBS CSI controller runs on the nodes, so scaling to 0 with a
PVC still Terminating would strand the EBS volume until the next scale-up.
Then scales the node group to 0. **Exits non-zero if the node group did not
reach 0** so CI can alert on a cost leak.

### 4. Teardown (stop ALL cost, including control plane)

```bash
./teardown.sh
```

## Scale-down on failure paths (CI `if: always()`)

The nightly orchestrator must call `scale-down.sh` from an **`if: always()`**
step so a failed/aborted run never leaves paid compute (or a standing EBS
volume) running:

```yaml
- name: Scale EKS node group to zero
  if: always()
  working-directory: benchmarks/e2e/infra
  run: ./scale-down.sh
```

`scale-down.sh` is built for this. It:

1. **Is fully idempotent and degraded-state-safe.** It succeeds from any state
   — nodes NotReady, Kafka CR not Ready or absent, node group already at 0,
   operator missing, stale kubeconfig. No step assumes a clean prior run
   (`set -uo pipefail`, not `-e`; every cleanup step is guarded / best-effort).
2. **Cleans transient per-run state that would otherwise pin cost.** The
   broker's EBS is a **per-run PVC** — `kafka.yaml` sets `deleteClaim: true`
   on the node pool storage (70Gi gp3), so deleting the Kafka CR cascades to
   PVC then EBS deletion. A failed run that left the broker up would otherwise
   **leak a standing 70Gi gp3 volume**; scale-down tears the Kafka CR down and
   sweeps leftover PVCs in the namespace to close that leak. **Broker data is
   not meant to survive a run** — the topic is re-produced fresh each run
   (plan section 5), so nothing of value is lost.
3. **Waits for PVC deletion before dropping nodes (bounded).** The EBS CSI
   controller lives on the nodes; if the group hits 0 while a PVC is still
   Terminating, nothing can detach/delete the volume and the 70Gi EBS lingers,
   billed, until the next scale-up. scale-down waits up to
   `PVC_DELETE_TIMEOUT_SECONDS` (default 300) for the namespace's PVCs to be
   gone; on timeout it warns loudly and proceeds (reaching node-zero still
   wins — see the cost note below).
4. **Exit code reflects whether the node group reached 0.** Best-effort K8s
   cleanup failures are warnings and never block reaching zero; only failing
   to reach desired-0 is a hard non-zero exit, which CI should alert on.

## Cost notes (plan decision 12)

- **Control plane** persists after `provision.sh`: ~$0.10/hr (~$73/mo). This is
  the deliberate trade for ~2-5 min run spin-up instead of a full create.
- **Nodes** (2× m6i.large) are paid **only during runs** — scaled to 0 between
  runs by `scale-down.sh`.
- **Broker EBS** (70Gi gp3) exists **only while a run's Kafka CR exists**
  (`deleteClaim: true`); `scale-down.sh` removes it every run, including after
  failures — so normally no standing EBS cost between runs. **Caveat:** after
  an abnormal teardown, if PVC deletion does not complete within the bounded
  wait (`PVC_DELETE_TIMEOUT_SECONDS`, default 300s), the volume can persist
  (billed, ~$5.60/mo for 70Gi gp3) until the next scale-up finishes the
  deletion — the timeout warning in the scale-down log flags this; check
  `kubectl -n kafka-bench get pvc` / the EC2 volumes console.
- **Full stop:** `teardown.sh` deletes everything including the control plane.

## CI secrets / roles (mirrors the Spark ClickBench CI OIDC pattern)

The Spark load-test workflow uses GitHub OIDC to assume an AWS role (no
long-lived keys), then reads run config from GitHub secrets. Mirror that here:

- **`id-token: write`** job permission + `aws-actions/configure-aws-credentials@v4`
  with `role-to-assume: ${{ secrets.KAFKA_BENCH_AWS_ROLE_ARN }}` and
  `aws-region: us-east-2`. The role trusts the repo's GitHub OIDC provider and
  is scoped to EKS/EC2/EBS/IAM for `Project=clickbench-benchmark` (analog of
  the Spark `CLICKBENCH_AWS_ROLE_ARN`).
- Expected GitHub secrets (analogous to the Spark `CLICKBENCH_*` set):

  | Secret | Use |
  |---|---|
  | `KAFKA_BENCH_AWS_ROLE_ARN` | OIDC role the CI job assumes (region us-east-2) |
  | `KAFKA_BENCH_TARGET_CH_HOST` | dedicated Cloud target host -> `TARGET_CH_HOST` |
  | `KAFKA_BENCH_TARGET_CH_USER` | -> `TARGET_CH_USER` |
  | `KAFKA_BENCH_TARGET_CH_PASSWORD` | -> `TARGET_CH_PASSWORD` |
  | `KAFKA_BENCH_METRICS_CH_HOST` / `_USER` / `_PASSWORD` | `perf.*` metrics landing (shared DWH pipeline) |

  CH connection values are injected as env at run time and **never committed**.
- The role and secrets are provisioned once (out of band, like the Spark CI
  infra) and referenced by name only.

## Validation (offline)

Performed at authoring time (no AWS calls; creds were expired):

- **YAML parse** (`python3 -c 'yaml.safe_load_all'`) on every `.yaml`,
  including the Strimzi CRs and eksctl config — all parse cleanly.
- **Structural check**: every K8s/eksctl document has a valid
  `apiVersion` + `kind` (+ `metadata.name`) — all pass.
- **kubectl client dry-run** was attempted on the plain-K8s manifests but
  `kubectl apply --dry-run=client` needs a reachable API server to resolve
  resource kinds (it hit `connection refused` — no cluster, creds expired), so
  it cannot run fully offline here; the YAML-parse + structural check stand in
  for it. `kubeconform` was unavailable. Re-run the client dry-run once a
  kubeconfig exists (after `provision.sh`).
- **`bash -n`** on every script (`shellcheck` was unavailable) — all pass.
- **Credential grep** (`ASIA`, `AKIA`, `aws_secret`, `PRIVATE KEY`,
  `SECRET_ACCESS`) over the whole tree — clean (only this README's own
  description of the grep matches).

Re-run offline validation any time with the commands in the plan's task-25
self-verification section.
