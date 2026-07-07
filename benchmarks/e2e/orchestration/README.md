# Benchmark v2 — nightly orchestration (task 31)

The assembly task: wires every component built so far (infra, images, producer,
poller, capture, sql) into ONE nightly end-to-end two-arm pair, per
[`docs/benchmark-v2-plan.md`](../../../docs/benchmark-v2-plan.md) §5 and the
[naming/semantics contract](../../../docs/benchmark-v2-contract.md).

> **STATUS: CODE-COMPLETE / VALIDATION-PENDING.** No AWS credentials and no
> provisioned cluster were available while authoring this. Nothing here has run
> against a live cluster. The plan phase-5 acceptance criterion — **one green
> end-to-end pair** — is **NOT** claimed. What is delivered is the complete
> orchestration code plus this manual-validation runbook. See
> [Known gaps](#known-gaps-everything-unvalidated-live).

## What this territory owns

| File | Role |
|------|------|
| `run_pair.sh` | The orchestrator. Implements plan §5 steps 1-6 as phase functions; `--plan`/`-n` prints the phase sequence without executing. Trap-based cleanup for standalone runs. |
| `templates/kafkaconnect.yaml.tmpl` | Strimzi `KafkaConnect` CR: single worker, arm image, JVM sizing, `metricsConfig` (jmxPrometheusExporter port 9404 → `connect-metrics` ConfigMap), external CH-creds Secret wiring. |
| `templates/connect-metrics-configmap.yaml` | JMX exporter rules = stock `jmx-export-connector.yml` **plus the mandatory `-rate` rule** (poller README prerequisite 1a). |
| `templates/kafkaconnector.json.tmpl` | `KafkaConnector` CR: `ClickHouseSinkConnector`, plan §6 values EXACTLY, `${CH_TABLE}` per tier, `${env:CH_*}` creds from the Secret. |
| `templates/kafkatopic.yaml.tmpl` | `KafkaTopic` CR: 3 partitions RF1. |
| `render_connector.py` / `render_producer_job.py` | Substitute per-run values into the connector CR / producer Job (never editing the components' committed files). |
| `emit_run_cost.py` | Full-pair `run_cost_usd`, charged once on the first-run arm (contract §2.1). |
| `delete_consumer_group.sh` | Fresh-group-per-drain cleanup. |
| `tests/test_orchestration.py` | Offline unit tests (arm order, run_id, config echo, producer parse, §6 cross-check). |
| `../../../.github/workflows/benchmark-nightly.yml` | Cron + dispatch; `workflow_call`s `benchmark-images.yml`; OIDC creds; concurrency; `if: always()` cleanup; artifact upload. |

## Anatomy mapping (plan §5 → phase functions)

| Plan §5 | run_pair.sh |
|---------|-------------|
| 1. Scale up | `phase_scale_up` → `infra/scale-up.sh` |
| 2. Pre-load | `phase_preload` → KafkaTopic CR (assert 3 partitions) → producer Job → parse `rows_expected` |
| 3. Per arm (day-parity order) | `phase_arm` × 2, Tier 0 then Tier 1 each |
| 3c. Delete connector + group | `delete_connector` |
| 4. Capture | `capture_and_record` (poller finalize+insert, then gated CH-side capture) |
| 5. Export | `capture_and_record` → `export_metrics_to_dwh.py` (gated on runs insert) |
| 6. Teardown | `phase_teardown_topic` + `phase_scale_down` (also CI `if: always()`) |

**Arm order (directive g):** UTC day-of-year parity — **even → head first, odd →
pinned first**. The workflow's image-slot resolver uses the identical parity
expression; a unit test pins the one-liner so the two cannot drift.

**Gating discipline (capture/README, BINDING):** capture SQL runs in numeric
order; any failure → `rollback_run_metrics.py` + abort; `insert_run_record.py`
only after full capture; runs-insert failure → rollback; export only after runs
insert; integrity verdict LAST (Tier 1 mismatch **FAILS** the run after evidence
is exported, contract §3).

## Manual-validation runbook (the first e2e pair)

A credentialed operator runs these once the cluster is provisioned and the
`KAFKA_*` CI secrets exist. **This is the validation this task could not perform.**

### 0. Prerequisites (one-time, out of band)

1. `benchmarks/e2e/infra/provision.sh` has been run (control plane + node group at 0).
2. Target Cloud service bootstrapped: run `benchmarks/e2e/sql/` files 1-8 (see
   `sql/README.md`), including `bootstrap/01_create_benchmark_user.sql` with the
   real `kafka_benchmark` password.
3. CI secrets / repo vars set (see [Secrets](#secrets--repo-vars-binding)).
4. **IAM role for the producer IRSA (provisioning TODO):** create an IAM role
   trusting the EKS OIDC provider, granting `s3:GetObject`/`ListBucket` on the
   parquet staging prefix, and annotate the `hits-producer` ServiceAccount in
   `kafka-bench` with it (`eks.amazonaws.com/role-arn`). `render_producer_job.py`
   only wires the SA **name** (`--service-account`); the role/annotation is not
   yet automated.
5. Parquet staging location chosen (producer README "Parquet staging (TBD)") and
   set as `KAFKA_PARQUET_SOURCE`.

### 1. Dry-run the phase plan (no creds needed)

```bash
benchmarks/e2e/orchestration/run_pair.sh --plan
```

Confirm the arm order and the 4 run rows match today's parity.

### 2. Bring the cluster up + build images

```bash
export AWS_PROFILE=...            # or assume the KAFKA_BENCH_AWS_ROLE_ARN role
aws eks update-kubeconfig --name kafka-bench --region us-east-2
# Build both arm images (or let the workflow do it):
./benchmarks/e2e/docker/build-arm.sh --arm head   --image-tag ghcr.io/<owner>/clickhouse-kafka-connect:benchmark-head-<sha>
./benchmarks/e2e/docker/build-arm.sh --arm pinned  --image-tag ghcr.io/<owner>/clickhouse-kafka-connect:benchmark-pinned-<ref>
# (push both to a registry the cluster can pull)
```

### 3. Run one pair by hand

```bash
export AWS_REGION=us-east-2 COMPUTE_REGION=us-east-2 K8S_NAMESPACE=kafka-bench
export ARM0_IMAGE=<first-run arm image for today's parity>
export ARM1_IMAGE=<second-run arm image>
export PRODUCER_IMAGE=<producer image>  PRODUCER_SA=hits-producer
export PARQUET_SOURCE=s3://<staging>/clickbench/hits/
export TARGET_CH_HOST=... TARGET_CH_USER=... TARGET_CH_PASSWORD=...
export METRICS_CH_HOST=... METRICS_CH_USER=... METRICS_CH_PASSWORD=...
export DWH_ROLE_ARN=... DWH_BUCKET=...
benchmarks/e2e/orchestration/run_pair.sh
```

### 4. Verify the run landed (the acceptance evidence)

```sql
-- 4 rows, one pair_id, distinct run_ids (contract §1.2)
SELECT run_id, runtime['arm'], runtime['tier'], runtime['pair_id']
FROM perf.runs WHERE runtime['pair_id'] = '<pair_id>' ORDER BY run_id;

-- mandatory scope keys present on every row (contract §1.1)
SELECT run_id, runtime['target_region'], runtime['environment_class'],
       runtime['compute_region'] FROM perf.runs WHERE runtime['pair_id']='<pair_id>';

-- run_cost_usd on exactly ONE arm (first-run) row (contract §2.1)
SELECT run_id, value FROM perf.metrics
WHERE metric_name='run_cost_usd' AND run_id LIKE '<pair_id>%';

-- FIRST FUNCTIONAL MILESTONE (plan §6): ch_avg_rows_per_insert >= 50k on Tier 1
SELECT run_id, value FROM perf.metrics
WHERE metric_name='ch_avg_rows_per_insert' AND run_id LIKE '<pair_id>%-t1';

-- Tier 1 integrity (contract §3): rows_delivered == rows_expected, duplicate_rows == 0
SELECT metric_name, value FROM perf.metrics
WHERE run_id='<pair_id>-<first-arm>-t1'
AND metric_name IN ('integrity_ok','rows_delivered','rows_expected','duplicate_rows');
```

### 5. Confirm cleanup

```bash
kubectl -n kafka-bench get kafkatopic,kafkaconnect,kafkaconnector   # empty
eksctl get nodegroup --cluster kafka-bench --region us-east-2       # desired 0
```

## Acceptance checklist (maps to the plan exit criterion)

Plan phase 5 exit criterion: **one green end-to-end pair.** A pair is green when:

- [ ] Cluster scaled up; producer Job completed; `rows_expected` recorded.
- [ ] Topic came up with **exactly 3 partitions** (asserted before producing).
- [ ] Both arms ran, in the day-parity order, Tier 0 then Tier 1 each.
- [ ] Connector + all 3 tasks reached RUNNING before the poller started (prereq 5).
- [ ] Poller drained to lag 0 (`lag_reached_zero == 1`); no guard tripped
      (`rebalance_count == 0`, `connect_task_restarts == 0`, `task_failed_count == 0`)
      → run **not** FLAGGED.
- [ ] `perf.runs` has **4 rows**, one `pair_id`, distinct `run_id`s, mandatory
      scope keys set, `warm_up` **absent**.
- [ ] `run_cost_usd` on exactly the first-run arm's row.
- [ ] Tier 1 integrity OK (`rows_delivered == rows_expected`, `duplicate_rows == 0`).
- [ ] `ch_avg_rows_per_insert >= 50000` on Tier 1 (plan §6 first milestone; if
      below, task 32 tunes the poll size — not a code defect here).
- [ ] Metrics exported to the DWH; topic deleted; node group at 0.

## Secrets / repo vars (BINDING)

**GitHub Secrets** (mirror the Spark `CLICKBENCH_*` set):

| Secret | Use |
|--------|-----|
| `KAFKA_BENCH_AWS_ROLE_ARN` | OIDC role the CI job assumes (infra/README.md) |
| `KAFKA_TARGET_CH_HOST` / `_USER` / `_PASSWORD` | drain target (sql/README.md) |
| `KAFKA_METRICS_CH_HOST` / `_USER` / `_PASSWORD` | `perf.*` landing (sql/README.md) |
| `KAFKA_DWH_ROLE_ARN` | cross-account DWH export role |

**GitHub repo Variables** (non-secret):

| Var | Use |
|-----|-----|
| `KAFKA_PRODUCER_IMAGE` | producer image ref |
| `KAFKA_PARQUET_SOURCE` | resolved parquet staging location |
| `KAFKA_PRODUCER_SA` | producer IRSA ServiceAccount name (default `hits-producer`) |
| `KAFKA_DWH_BUCKET` | DWH S3 bucket (pending region decision, capture/README) |

## Concurrency guard + its limits (directive f)

`benchmark-nightly.yml` sets `concurrency: { group: benchmark-v2-kafka-nightly,
cancel-in-progress: false }`. This serialises runs of **this workflow file**.

**Limits (documented, not hidden):**
- It is a **per-workflow-file** lock, not a target-wide lock. It does NOT prevent
  a manual `run_pair.sh` on a laptop from overlapping a CI run. Operators must
  not run the script by hand while the nightly could fire.
- It does **not** coordinate with the Spark benchmark — by design: the two
  benchmarks hit **different** dedicated targets (plan §3/§5), so no
  cross-workflow lock is needed.
- A queued run waits for the in-flight one; it is not cancelled
  (`cancel-in-progress: false`), so a long run delays but never truncates the next.

## Self-verification performed (offline only)

- `bash -n` on `run_pair.sh`, `delete_consumer_group.sh` — pass.
- `py_compile` on all `.py` — pass.
- `yaml.safe_load` on every template + the workflow — pass (CR templates parsed
  after substituting dummy placeholders).
- `run_pair.sh --plan` for **both** parities — correct arm order + cost attribution.
- `render_connector.py` / `render_producer_job.py` smoke — correct substitution,
  doc-keys stripped, `${env:CH_*}` creds left untouched.
- `pytest tests/` — **18 passed** (arm parity vs shell, run_id form, runtime-map
  scope/config keys, `warm_up` omitted, empty-provenance drop, producer parse,
  §6 cross-check template↔echo + insert-timeout-<-poll-interval margin).
- Every path/secret/workflow referenced by `benchmark-nightly.yml` verified to
  exist (workflow_call target, scripts, capture SQL 11-22, requirements.txt).

## Known gaps (everything unvalidated live)

1. **No green e2e pair.** Nothing ran against a cluster/target. Every live path
   below is exercised for the first time by the runbook above.
2. **Poller live paths** (`sampler.build_sources`, real Kafka admin/REST/JMX/
   cadvisor HTTP) — never hit; poller README flags this too.
3. **Capture query_log filter** (`has(tables) [+ user=kafka_benchmark]`),
   `remoteSecure()` reachability on 9440, and the bootstrap grants — validated
   for the first time here (capture/README defers this to task 31).
4. **JMX `-rate` rule** — the exporter output name
   (`kafka_consumer_fetch_manager_records_consumed_rate`) is asserted by the
   poller README but not confirmed against a live exporter.
5. **Connect REST service DNS** — `bench-connect-connect-api.kafka-bench.svc:8083`
   and the pod exec path assume Strimzi's default service naming; confirm on the
   first Ready CR.
6. **Producer IRSA IAM role** — only the SA name is wired; the role + trust
   policy + SA annotation are a provisioning TODO (runbook step 0.4).
7. **cadvisor RBAC / metrics-server** — poller prerequisite 2; the CPU metric is
   simply absent until wired (open decision 1 keeps `null_drain_rows_per_sec` the
   sole Tier-0 gate meanwhile).
8. **run_cost pricing** — the m6i.large us-east-2 rate is a hardcoded table
   value; verify against the current on-demand price and bump if drifted.
9. **Broker pod name** (`bench-combined-0`) in `delete_consumer_group.sh` assumes
   the Strimzi node-pool naming; confirm and parameterize if it differs.

## Interface gaps found in other components (not patched — reported)

- **CH-secret naming disagreement between docs.** `infra/README.md` names the CH
  secrets `KAFKA_BENCH_TARGET_CH_*` / `KAFKA_BENCH_METRICS_CH_*`; `sql/README.md`
  names them `KAFKA_TARGET_CH_*` / `KAFKA_METRICS_CH_*`. This workflow pins the
  **sql/README.md** spelling (the more complete table, and the doc closest to the
  capture scripts that consume them) and uses `KAFKA_BENCH_AWS_ROLE_ARN` for the
  AWS role (only infra/README documents it). One of the two component READMEs
  should be reconciled; not patched here (not this task's territory).
- **Consumer-group deletion needs the broker pod name.** No component exposes a
  helper to run `kafka-consumer-groups.sh`; `delete_consumer_group.sh` assumes
  the pod `bench-combined-0`. A small infra-provided exec helper would be more
  robust than hardcoding the pod name.
