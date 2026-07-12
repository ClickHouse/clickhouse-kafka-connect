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
| 2. Pre-load | `phase_preload` → KafkaTopic CR (broker-verified 3-partition assert) → **Indexed** producer Job (`completions == parallelism == SHARD_COUNT`, default 3 — parallel sharded preload, ~5-6 min vs ~20 min; **terminal watch: Complete OR Failed** — Failed dies fast with the pods' last logs) → derive `rows_expected` from **broker offsets** (`broker_topic_row_count`: `kafka-get-offsets.sh` in the broker pod, Σ latest−earliest per partition — producer-count-agnostic, authoritative) → best-effort cross-check `producer_rows_sent_sum` (Σ per-shard `rows_sent`, per completion-index pod log) == broker offsets else FAIL; then `phase_poller_host` (in-cluster sampler pod) |
| 3. Per arm (alternating order) | `phase_arm` × 2, Tier 0 then Tier 1 each |
| 3c. Delete connector + group | `delete_connector` |
| 4. Capture | `finalize_and_insert_metrics` (poller scalars → METRICS service) then `capture_and_record` (gated CH-side capture SQL → runs row) |
| 5. Export | `capture_and_record` → `export_metrics_to_dwh.py` (gated on runs insert) |
| — Pair cost | `phase_pair_cost` (end-of-pair; see below) |
| 6. Teardown | `phase_teardown_topic` + `phase_scale_down` (also CI `if: always()`) |

**Arm order (directive g, single-source — review F10):** the workflow computes
the UTC day-of-year parity **once** (even → head first, odd → pinned first) and
passes it to `run_pair.sh` as `ARM_ORDER_SPEC` together with the matching
`ARM0_IMAGE`/`ARM1_IMAGE` slots; the script never re-derives it in CI (a second
derivation straddling UTC midnight would mislabel arms vs images). The parity
fallback inside `run_pair.sh` is standalone-only; unit tests pin both paths.

**Poller placement (review F4):** the broker bootstrap (9092), Connect REST
(8083) and the JMX exporter (9404) are internal-only by design (plan decision
8), so the sampler runs **in-cluster** in a dedicated pod (`bench-poller`,
producer image + the poller package `kubectl cp`-ed in; deps verified, pip
fallback). Samples JSONL is copied back per drain (flushed per tick, so even a
dropped exec leaves an archivable partial). `finalize` (pure math + the
perf.metrics insert over the Cloud HTTPS endpoint) runs on the runner — with
the env remapped so the poller's inserter lands on the **METRICS** service,
the same landing capture/rollback/export use (review F5).

**Image pinning (digest by default — stale-tag class fix):** every image the
pair deploys (both arm images + the producer/poller image) is pinned by
**digest** (`repo@sha256:...`), never a mutable tag. A live pair once ran the
wrong image *twice* because a reused tag was served stale by a node/registry
cache; the operator recovered by hand-pinning digests, and that is now the
default. `build-arm.sh --push` resolves and emits the registry digest
(`IMAGE_DIGEST=...`); `benchmark-images.yml`'s `workflow_call` outputs are
digests, not tags; and `run_pair.sh` re-validates every image ref up front
(`validate_image_ref`) — a digest is accepted, a bare tag is resolved to a
digest (ECR via `aws ecr describe-images`, ghcr via `gh api`) or **hard-fails**,
unless `KAFKA_ALLOW_TAG=1` / `--allow-tag` (a local-hacking escape hatch, strict
by default).

**Wait audit (live fix):** every wait has a *failure-side* exit, not just a
success condition plus a long timeout. The first live run exposed the trap: the
producer Job failed (backoffLimit=0 → first failure is terminal) but the
one-sided `kubectl wait --for=condition=complete` kept waiting toward the 6h
`PRODUCER_TIMEOUT`, burning cluster-hours. Now: the producer wait polls **both**
Job conditions (`Complete`/`Failed`) and dies fast on Failed, dumping the pod's
last log lines; `wait_tasks_running` fast-fails (`fail_run` + status dump) on a
FAILED connector or task instead of burning its 600s timeout (Connect never
auto-restarts FAILED tasks, and `errors.tolerance=none` fails on the first bad
record); and every `kubectl delete --wait=true` carries a `--timeout` so a
stuck finalizer cannot block forever. Structural tests pin all three shapes.
Remaining bounded-but-slow waits (topic Ready 120s, poller pod Ready 300s,
KafkaConnect Ready 600s, settle 1800s cap) all die/flag on timeout by design.

**Gating discipline (capture/README, BINDING):** capture SQL runs in numeric
order; any failure → `rollback_run_metrics.py` + abort — and early per-run
failures after the pre-run covariates landed (truncate/deploy/poller/finalize)
also roll back first (`fail_run`, review F13); `insert_run_record.py` only
after full capture; runs-insert failure → rollback; export only after runs
insert; integrity verdict LAST.

**Integrity semantics (contract §3/§1.3 — review F7):**
- **Tier 0:** integrity is *not applicable* (Null engine delivers no countable
  rows) — SQL 20 is not run, nothing is flagged, nothing fails.
- **Tier 1, computation failure** (SQL 20 errors / constants missing): run is
  **FLAGGED** `integrity_unverified`, not rolled back, not failed.
- **Tier 1, mismatch:** run **FAILS** — after the runs row is inserted and the
  evidence exported (flagged/failed runs are still fully captured).

**The `check_integrity.py` verdict is a REDUNDANT CONFIRMATION LAYER, not the
source of truth (pair-4 crash-class fix).** The authoritative verdict is the
capture-computed `integrity_ok` written by SQL 20 during the gated capture step;
it is already on the `perf.runs`/`perf.metrics` row before the checker runs.
`check_integrity.py` merely *reads it back* at the very end and now returns
**distinct exit codes** that `capture_and_record` maps as:

| exit | meaning | `run_pair.sh` action |
|------|---------|----------------------|
| `0`  | ran, verdict OK (`integrity_ok == 1`) | proceed |
| `1`  | **RAN and MISMATCHED** (`integrity_ok != 1`, or metrics missing) — the ONLY code that may fail a run | run **FAILS** (`die`); the row already stands, mismatch detectable via `integrity_ok=0` |
| `3`  | **CHECK_ERROR**: any infra/connection/query exception — *could not verify* | **WARN loudly, do NOT die**; set `PAIR_HAD_WARNING`, pair stays **green** |

The checker wraps both the client acquisition and the query and **retries with
backoff** (3 attempts, 5/15/30s; `CH_INTEGRITY_ATTEMPTS` / `CH_INTEGRITY_BACKOFFS`)
before conceding exit 3. The staging-service handshake stall that triggered the
pair-4 false-red (a transient `clickhouse_connect` read-timeout on the connect
`SELECT version()`) is a **CHECK_ERROR (exit 3)**, not a mismatch — the old code
treated any non-zero as a mismatch and failed a **perfect** run.

**Why exit 3 does NOT retro-flag the row.** The verdict runs *post-insert /
post-export by design*, so the `perf.runs` row is **already inserted** when the
checker fires — a flag can no longer ride the runtime map. More importantly, it
**must not** flag: there is nothing wrong to record. The authoritative
capture-computed `integrity_ok` (SQL 20) already verified the data and is on the
row; `check_integrity.py` failing to *re-read* it is a property of the redundant
confirmation layer, not of the data. So an exit-3 leaves the row **clean**,
warns the operator that the confirmation checker could not run, and keeps the
pair green (`PAIR_HAD_WARNING`, surfaced in `main()`'s completion summary — not
`PAIR_HAD_FAILURE`). Only an unambiguous exit-1 mismatch fails the run.

**Capture-on-failure — `outcome` (contract §1.3 amendment): FAILED runs are
fully captured and exported, marked — no survivorship bias.**
- **Drain-start boundary:** the poller sample beginning. **Pre-drain failures**
  (scale-up, topic create, producer, poller host, Connect deploy, connector
  apply, tasks never all RUNNING) keep the old **hard-abort + rollback** — the
  drain never began, there is nothing meaningful to capture.
- **Post-drain-start ingest failures** — poller timeout (lag never reached 0),
  poller hard failure/no samples, finalize failure — route to `ingest_failed()`:
  best-effort client scalars from the partial JSONL, CH-side capture SQL over
  the actual window (`RUN_START` → failure time; per-file failures tolerated —
  partial evidence beats none), runs row with `runtime['outcome']='failed'`
  (+ `integrity_unverified` on tier 1, **by definition** — SQL 20 is skipped, a
  failed run makes no integrity claim), then export. A finalize failure after a
  clean drain is deliberately failed-class too: a half-metriced "success" row
  silently entering headlines is worse than a conservatively-failed row.
- **Success rows OMIT the key** (absent ⇒ `success`, Map semantics); writing
  `outcome='success'` explicitly is rejected by the runtime-map builder.
- **Pair-level policy:** a failed-class run does **not** abort the pair — every
  remaining (arm,tier) run still executes and captures (the failed run's tier
  ratio is simply non-computable; the other arm keeps its absolute-trend value;
  dashboards exclude failed rows **by outcome, not by absence**). After all
  capture + teardown, `run_pair.sh` **exits non-zero** so CI surfaces the
  failure.
- **Not reclassified:** a tier-1 integrity **mismatch** keeps its §3 semantics
  (run FAILS after evidence export, detectable via `integrity_ok=0`) — its runs
  row is inserted before the verdict and is not retroactively marked; and a
  poller **timeout** now lands as failed-class (previously flagged-only
  `drain_incomplete`) because "lag never reached 0" means the ingest step did
  not complete — the finalize guards (incl. `drain_incomplete`) still land
  alongside.

**Pair cost (contract §2.1 — review F6):** `run_cost_usd` is computed at
**end of pair** (the window must cover both arms; charging mid-pair would halve
the node-hours) and attached to the **first-run arm's tier-1** run_id. This one
metric deliberately sits **outside the capture gate**: the target runs row is
already complete and exported, the insert is idempotent (pre-delete + insert),
and rolling back a completed run over a failed cost write would destroy good
evidence — a failure there is a warn, not a rollback.
**§1.2 join-rule gate:** `phase_pair_cost` only emits when the target run's
`perf.runs` row actually **landed** (tracked via `RECORDED_RUNS`, appended by
`capture_and_record` after a successful record insert). If the first-arm t1
record was rolled back (failed-class run whose insert failed), the cost emit is
**skipped with a loud warn** — deliberately **not** silently re-anchored to the
other arm (that would blur the first-run-arm cost convention); per-pair cost
sums stay honest: the pair simply carries no `run_cost_usd` that night, and the
job is already red via `PAIR_HAD_FAILURE`.
The gate keys on the row **landing**, not on the run's outcome: a first-arm t1
run that is **failed-class but whose `perf.runs` row still landed** (e.g. a
tier-1 integrity mismatch, which inserts its row before the verdict) **does**
receive the pair cost — the cost anchors to a real, present row, and the pair's
node-hours are attributable regardless of that run's pass/fail verdict.

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
# Build BOTH arm images, push, and CAPTURE THE DIGEST (or let the workflow do
# it — its outputs are already digests). Digest-pinning is the default: the tag
# is a build handle, the digest (repo@sha256:...) is what you deploy — a mutable
# tag can be served stale by node/registry caches (the first pair ran the wrong
# image twice from a reused tag).
./benchmarks/e2e/docker/build-arm.sh --arm head \
  --image-tag ghcr.io/<owner>/clickhouse-kafka-connect:benchmark-head-<sha> \
  --push --digest-out /tmp/head.digest
HEAD_IMAGE="$(cat /tmp/head.digest)"     # repo@sha256:...
./benchmarks/e2e/docker/build-arm.sh --arm pinned \
  --image-tag ghcr.io/<owner>/clickhouse-kafka-connect:benchmark-pinned-<ref> \
  --push --digest-out /tmp/pinned.digest
PINNED_IMAGE="$(cat /tmp/pinned.digest)" # repo@sha256:...
```

### 3. Run one pair by hand

```bash
export AWS_REGION=us-east-2 COMPUTE_REGION=us-east-2 K8S_NAMESPACE=kafka-bench
# RECOMMENDED: pin the order explicitly so the image slots cannot disagree with
# a parity re-derivation (this is what CI does — review F10):
export ARM_ORDER_SPEC="head pinned"        # or "pinned head"
# ARM0/ARM1/PRODUCER MUST be DIGEST refs (repo@sha256:...) — run_pair.sh
# validates and rejects a bare tag by default (KAFKA_ALLOW_TAG=1/--allow-tag is
# the local-hacking escape hatch). Use the digests captured above.
export ARM0_IMAGE="$HEAD_IMAGE"            # digest of ARM_ORDER_SPEC's FIRST arm
export ARM1_IMAGE="$PINNED_IMAGE"          # digest of the second arm
export PRODUCER_IMAGE=<producer image DIGEST>  PRODUCER_SA=hits-producer
export PARQUET_SOURCE=s3://<staging>/clickbench/hits/
export TARGET_CH_HOST=... TARGET_CH_USER=... TARGET_CH_PASSWORD=...
export METRICS_CH_HOST=... METRICS_CH_USER=... METRICS_CH_PASSWORD=...
export DWH_ROLE_ARN=... DWH_BUCKET=...
benchmarks/e2e/orchestration/run_pair.sh
```

**Baseline tuning knobs (first-baseline pair).** The baseline runs
`CFG_MAX_POLL_RECORDS=25000` + `CONNECT_HEAP=4096m` — a *co-sized* pair (a 100k
first poll GC-spiraled the 2G-heap worker: 17 restarts, zero commits; heap and
poll size must move together — commit e140231), and the only live-proven
pairing. Both are the DEFAULTS: `run_pair.sh` defaults `CONNECT_HEAP=4096m` and
`CFG_MAX_POLL_RECORDS=25000`, and the connector template
(`kafkaconnector.json.tmpl`) carries `25000`. `CFG_MAX_POLL_RECORDS` is a real
knob: `render_connector.py` writes the same env var into
`consumer.override.max.poll.records`, so the deployed connector and the runtime
echo (provenance) agree by construction. **TODO(#32):** 25k is *knowingly below*
the plan-§6 `>=50k ch_avg_rows_per_insert` milestone; #32 co-tunes poll+heap
*upward together* on the `connect-ng` m6i.xlarge. Worker `replicas` stays 1 here
(scale-out is the **#37** sweep's variable).

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

### 4b. CPU acceptance gate + deployed-instrument keys

**CPU acceptance gate (tier 0 only).** After the tier-0 drain finalizes,
`compute_cpu_gate_t0` integrates the Connect worker's cadvisor CPU counter over
the drain window and computes its **share of the CPU limit** =
`(connect_cpu_seconds_per_Mrows × rows/1e6) / (drain_seconds × limit_cores)`.
The verdict is printed loudly and is **LOG-ONLY** (never auto-flags — the
quarantine flag decision is the manager's):

| Verdict | Condition |
| --- | --- |
| `PASS` | share `< 0.80` |
| `INSTRUMENT_RESIZE_SUSPECT` | share `>= 0.80` (the instrument, not the connector, may be the bottleneck) |
| `UNAVAILABLE` | cadvisor scrape failed at runtime (node unresolved, proxy 403/empty) or the CPU limit is unreadable — tolerated, never a run failure |

The share is **not bounded to 0..1**: a worker can burn more CPU-seconds than
`limit × drain` (bursting, brief over-limit before throttling, multi-core
accounting), so values `> 1.0` are legitimate and still classify as
`INSTRUMENT_RESIZE_SUSPECT`. When sighted, the share also lands as the tier-0
runtime key `kafka_worker_cpu_share_t0` (fixed to 4 decimals).

**Deployed-instrument keys** (kafka-namespaced until the shared pinned spellings
land) describe the substrate the run executed on, so a result is reproducible
and comparable only against a matched instrument:

| Key | Meaning |
| --- | --- |
| `kafka_compute_instance_type` | Connect worker node instance type (e.g. `m6i.xlarge`) |
| `kafka_worker_cpu_request` / `kafka_worker_cpu_limit` | Connect worker pod CPU request / limit |
| `kafka_worker_mem_request` / `kafka_worker_mem_limit` | Connect worker pod memory request / limit |
| `kafka_worker_cpu_share_t0` | tier-0 CPU-gate share of limit (see above; tier 0 only, sighted-only) |

```sql
SELECT run_id,
       runtime['kafka_compute_instance_type'] AS instance,
       runtime['kafka_worker_cpu_limit']      AS cpu_limit,
       runtime['kafka_worker_cpu_share_t0']   AS cpu_share_t0
FROM perf.runs WHERE runtime['pair_id']='<pair_id>' ORDER BY run_id;
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

- `bash -n` on `run_pair.sh`, `delete_consumer_group.sh`, `docker/build-arm.sh`
  — pass.
- `py_compile` on all `.py` — pass.
- `yaml.safe_load` on every template + the workflow — pass (CR templates parsed
  after substituting dummy placeholders).
- `run_pair.sh --plan` for both `ARM_ORDER_SPEC` orders **and** the standalone
  parity fallback — correct arm order, poller-host phase, end-of-pair cost.
- `render_connector.py` / `render_producer_job.py` smoke — correct substitution,
  doc-keys stripped, `${env:CH_*}` creds left untouched.
- `pytest tests/` — **43 passed** (arm parity vs shell, `ARM_ORDER_SPEC`
  override + invalid-spec rejection + fallback, run_id form, runtime-map
  scope/config keys, mandatory-key fail-on-empty, `warm_up` omitted,
  empty-provenance drop, producer parse, §6 cross-check template↔echo,
  insert-timeout-<-poll-interval margin, partition_scheme = Tier-1 DDL,
  run_cost end-of-pair placement, `config.providers` wiring, `-rate` rule
  over-match guard, **digest-pinned image validation** — digest accepted / tag
  rejected by default / `KAFKA_ALLOW_TAG` escape hatch / all three images
  validated before any phase / workflow outputs are digests / build-arm.sh
  `--push` emits `IMAGE_DIGEST`).
- Confluent Avro converter archive (7.7.1) downloaded from the Hub and its
  sha256 pinned in `docker/build-arm.sh` (verified against the real download).
- Every path/secret/workflow referenced by `benchmark-nightly.yml` verified to
  exist (workflow_call target, scripts, capture SQL 11-23, requirements.txt).

## Known gaps (everything unvalidated live)

1. **No green e2e pair.** Nothing ran against a cluster/target. Every live path
   below is exercised for the first time by the runbook above.
2. **Poller live paths** (`sampler.build_sources`, real Kafka admin/REST/JMX/
   cadvisor HTTP) — never hit; poller README flags this too.
3. **In-cluster sampler transport** — the drain-long `kubectl exec` into the
   `bench-poller` pod holds one API-server connection for up to `POLL_TIMEOUT`.
   The JSONL is flushed per tick and copied back even on failure, so a dropped
   exec loses the live rc but not the samples; if drops prove common, promote
   the sampler to a K8s Job + PVC. Also: the poller pod verifies
   `confluent_kafka`+`requests` and falls back to `pip install` (needs PyPI
   egress) — confirm on first run.
4. **Capture query_log filter** (`has(tables) [+ user=kafka_benchmark]`),
   `remoteSecure()` reachability on 9440, and the bootstrap grants — validated
   for the first time here (capture/README defers this to task 31).
5. **JMX `-rate` rule** — the exporter output name
   (`kafka_consumer_fetch_manager_records_consumed_rate`) is asserted by the
   poller README but not confirmed against a live exporter.
6. **Connect REST service DNS + pod labels** —
   `bench-connect-connect-api.kafka-bench.svc:8083` and the StrimziPodSet pod
   labels (`strimzi.io/cluster` + `strimzi.io/kind=KafkaConnect`) assume
   Strimzi 0.46 defaults; confirm on the first Ready CR.
7. **Producer IRSA IAM role** — only the SA name is wired; the role + trust
   policy + SA annotation are a provisioning TODO (runbook step 0.4).
8. **cadvisor RBAC / metrics-server** — poller prerequisite 2; the CPU metric is
   simply absent until wired (open decision 1 keeps `null_drain_rows_per_sec` the
   sole Tier-0 gate meanwhile).
9. **run_cost pricing** — the m6i.large + m6i.xlarge us-east-2 rates are
   hardcoded table values (`M6I_LARGE_USD_PER_HR`, `M6I_XLARGE_USD_PER_HR` in
   `run_pair.sh`); the pair-cost calc bills BOTH node-hour terms (bench-ng
   m6i.large + the dedicated connect-ng m6i.xlarge). Verify against the current
   on-demand prices and bump if drifted.
10. **Broker pod name** (`bench-combined-0`) used by `delete_consumer_group.sh`
    and the broker-side partition assert assumes the Strimzi node-pool naming;
    confirm and parameterize if it differs.
11. **Confluent Hub download URL** (`hub-downloads.confluent.io/...`) — the
    archive + sha256 were fetched and pinned offline-of-cluster; the URL shape
    is re-exercised on every image build (a 404 fails the build loudly).

## Interface gaps found in other components (not patched — reported)

- **CH-secret naming disagreement between docs.** `infra/README.md` names the CH
  secrets `KAFKA_BENCH_TARGET_CH_*` / `KAFKA_BENCH_METRICS_CH_*`; `sql/README.md`
  names them `KAFKA_TARGET_CH_*` / `KAFKA_METRICS_CH_*`. This workflow pins the
  **sql/README.md** spelling (the more complete table, and the doc closest to the
  capture scripts that consume them) and uses `KAFKA_BENCH_AWS_ROLE_ARN` for the
  AWS role (only infra/README documents it). One of the two component READMEs
  should be reconciled; not patched here (not this task's territory).
- **Poller README's example `-rate` exporter rule over-matches.** Its
  `client-id=(.*)` capture also matches the per-topic fetch-manager MBean
  (`client-id=..., topic=...`), which would emit `records_consumed_rate` twice
  and ~double the poller's summed value. The ConfigMap in this territory uses
  `client-id=([^,]+)` instead (review F12); the poller README example itself is
  not edited (not this task's territory) — it should be corrected to match.
- **Consumer-group deletion needs the broker pod name.** No component exposes a
  helper to run `kafka-consumer-groups.sh`; `delete_consumer_group.sh` assumes
  the pod `bench-combined-0`. A small infra-provided exec helper would be more
  robust than hardcoding the pod name.
