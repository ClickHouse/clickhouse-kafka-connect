# Benchmark v2 — the Kafka e2e ecosystem (infrastructure handbook)

**Audience**: anyone building a new workload on this infrastructure — written
specifically as the handoff for the **chaos / "monkey" exactly-once test**
(kill ClickHouse ingestion and Connect workers mid-processing and prove no row
is lost or duplicated), but general to any e2e task.

**State as of 2026-07-13.** Everything below is live and battle-tested across five benchmark pairs —
eight tier-1 drains × 10M rows each with PERFECT integrity (pairs 2–4 clean +
quarantined pair 1; pair 5 in flight), plus the same volumes through Tier-0.

---

## 1. The one-paragraph architecture

A scale-to-zero EKS cluster (`kafka-bench`, us-east-2) runs a single-broker
KRaft Kafka (Strimzi), a Confluent Schema Registry, a sharded producer Job that
pre-loads a topic with an exactly-counted dataset, and a Kafka Connect worker
(on a dedicated node) running the ClickHouse sink connector against a ClickHouse
Cloud staging service. A poller pod samples consumer lag, Connect REST/JMX, and
per-container CPU (cadvisor). An orchestrator script on the operator's machine
(`orchestration/run_pair.sh`) sequences everything, captures server-side metrics
into a `perf.*` schema on the same CH service, verifies **integrity** (exact row
count + exact uniqueness vs. broker offsets), and exports results to a DWH that
feeds Superset dashboards. Every deployed image is **digest-pinned**; every run
is identified by `<pair_id>-<arm>-t<tier>`; every anomaly is **flagged, not
deleted**.

```
 operator machine (run_pair.sh)────────────────────────────────┐
   │ kubectl / eksctl (role-chained AWS creds)                 │ curl/python
   ▼                                                           ▼
 EKS kafka-bench                                        ClickHouse Cloud staging
 ┌────────────────────────────────────────────┐         gams6lhck3.us-east-2....
 │ bench-ng (m6i.large ×4, scale-to-zero)     │         ┌──────────────────────┐
 │   bench-combined-0   (Kafka broker, KRaft) │  8443   │ clickbench.hits      │
 │   schema-registry                          │ ───────►│ clickbench.hits_null │
 │   hits-producer-{0,1,2}  (Indexed Job)     │         │ perf.runs/metrics/   │
 │   bench-poller       (lag/JMX/cadvisor)    │         │      ch_inserts      │
 │ connect-ng (m6i.xlarge ×1, tainted)        │         └──────────┬───────────┘
 │   bench-connect-connect-0 (the SUT)        │                    │ export bridge (CI)
 └────────────────────────────────────────────┘                    ▼
                                                     DWH → Superset dashboard 432
```

---

## 2. AWS + access

| Thing | Value |
|---|---|
| Cluster | `kafka-bench`, EKS 1.36, us-east-2, account 796575137974 |
| Nodegroups | `bench-ng` m6i.large, min 0 / max 4 (broker+registry+producer shards+poller); `connect-ng` m6i.xlarge, min 0 / max 1, **taint `dedicated=connect:NoSchedule`** — the Connect worker runs alone |
| Cost | ~$0.58/hr compute fully scaled (4×m6i.large $0.384 + 1×m6i.xlarge $0.192) + gp3, **control plane only (~$0.10/hr) at rest**. A pair ≈ $0.87 at the pre-2026-07-13 2+1 shape; ≈ $1.10–1.25 at 4+1 (accepted cost-for-speed; preload 3× faster). |
| Registry | ECR `796575137974.dkr.ecr.us-east-2.amazonaws.com/{connect-bench,producer-bench}` |
| Parquet staging | `s3://shimons/clickbench-kafka-bench/hits-10m/` — 10 files, exactly 10,000,000 rows, 10,000,000 unique `WatchID`s |

**Credentials model** (until merge-to-main enables OIDC everywhere):
- The operator pastes a fresh `IntegrationsTester` SSO session (~12h life) into
  profile `clickbench-ops`.
- **Direct EKS API is DENIED to IntegrationsTester.** All `aws`/`eksctl` calls
  go through profile `clickbench-provisioner` (role-chain:
  `role_arn=arn:aws:iam::796575137974:role/clickbench-eks-provisioner`,
  `source_profile=clickbench-ops` — auto-refreshing).
- `kubectl` auth is pinned in the kubeconfig's exec block to profile
  `clickbench-ops` (the tester has a cluster-admin *access entry*; the
  provisioner role does not).
- Always `unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN`
  before any call — stale ambient env vars shadow profiles and have burned us
  repeatedly.

**⚠ EKS RBAC quirk (load-bearing)**: on this cluster, `kubectl auth can-i`
(impersonated *or* token-based SSAR) returns **false negatives** for permissions
that real requests are granted. Never gate anything on `can-i`; probe with a
real token + raw request (see the preflight in `infra/scale-up.sh`). This cost
us a full day of "blind CPU gate" debugging.

---

## 3. Infra scripts (`benchmarks/e2e/infra/`)

| Script | What it does |
|---|---|
| `provision.sh` | One-shot cluster create from `cluster.yaml` (nodegroups at 0) + Strimzi operator + `gp3-bench` StorageClass. Persistent footprint = control plane only. |
| `scale-up.sh [N]` | Scales both nodegroups (default 4 bench + 1 connect), waits per-nodegroup label-filtered Ready, re-applies Strimzi + **poller RBAC** (delete-before-apply — roleRefs are immutable), runs the **real-probe preflight**, applies `kafka.yaml` + `schema-registry.yaml`, waits for the Kafka CR Ready. Idempotent. |
| `scale-down.sh` | Both nodegroups → 0, verifies, prints calm lifecycle-hook messaging (a lingering `Terminating` instance for ~15 min is expected, ~$0.02, not a leak). |
| `teardown.sh` | Deletes the whole cluster (only for rebuilds). |
| `poller-rbac.yaml` | `bench-poller-sa` + ClusterRole `nodes/proxy get` (cadvisor scrape via API-server proxy) + namespace Role `pods get/list` (nodeName resolution — without it the CPU source never arms). |

**Kafka**: Strimzi 0.46.0, KRaft, single combined broker/controller
(`bench-combined-0`), RF=1, 70Gi gp3 PVC, plaintext 9092 in-cluster
(`bench-kafka-bootstrap.kafka-bench.svc:9092`). Schema Registry at
`http://schema-registry.kafka-bench.svc:8081`.

---

## 4. Images (digest-pinning is law)

- **connect-bench**: two arms — `head` (plugin built from the checked-out sha)
  and `pinned` (the released artifact named in `docker/PINNED_REF`,
  sha256-verified, never rebuilt). Built by `docker/build-arm.sh`.
- **producer-bench**: `producer/Dockerfile`, build context `benchmarks/e2e/`
  (COPYs `schema/`).
- **Deploy by digest (`repo@sha256:...`), never by tag.** A mutable tag once
  served a stale image twice; `run_pair.sh` hard-rejects bare tags
  (`KAFKA_ALLOW_TAG=1` is the local-hacking escape hatch only).
- Build paths: (a) locally via buildx `--platform linux/amd64` + push to ECR —
  if Docker Hub is unreachable from the Docker VM, pull the base via
  `public.ecr.aws/docker/library/python:3.11-slim` and retag; (b) **CI (PR
  #789)**: `.github/workflows/benchmark-images.yml` builds all three images via
  OIDC → ECR (pending an IAM trust+ECR-policy change on
  `clickbench-load-test-ci`). Always run the **arch gate** (manifest platform
  must be linux/amd64 — Apple Silicon builds arm64 by default and the pods
  crash with `exec format error`).

---

## 5. The producer (pre-load with an exact count)

`producer/producer.py`, run as a **K8s Indexed Job** (`SHARD_COUNT` pods, 3 by
default; each pod takes files where `i % SHARD_COUNT == JOB_COMPLETION_INDEX`
from the stable-sorted listing). ~9.5k rows/s per shard ⇒ 10M rows in ~6–7 min.

Count contract (the part a chaos test must not break):
- `enable.idempotence=true` per pod — producer retries never duplicate records.
- **`rows_expected` is derived by the orchestrator from broker END OFFSETS
  after the Job completes** (`Σ end−beginning` over partitions) — never from
  producer-side counts. A cross-check sums the shards' `rows_sent` and warns on
  mismatch.
- `backoffLimit: 0` — a half-complete pre-load is never silently retried (a K8s
  restart would double end-offsets); the orchestrator recreates topic + Job.
- Memory is explicitly bounded (pyarrow readahead 1×1, rdkafka queue 100k
  msgs/128MiB); peak_rss telemetry on every progress line. Pitfall fixed
  2026-07-13: `ds.dataset(...).files` returns **bucket-relative** paths for S3
  — rebuilding a dataset from that list requires `filesystem=dataset.filesystem`.

---

## 6. The system under test (Connect + connector)

Deployed per run by `run_pair.sh` from templates in `orchestration/templates/`:
- `kafkaconnect.yaml.tmpl` — Strimzi `KafkaConnect` CR `bench-connect`:
  1 replica on connect-ng (nodeSelector + toleration), image = the arm's
  **digest**, `CONNECT_HEAP=4096m`, requests 2cpu/5Gi, limits 3.5cpu/6Gi.
  CH creds come from Secret `bench-ch-creds` via `externalConfiguration`
  (never in CRs or logs).
- `kafkaconnector.json.tmpl` — `KafkaConnector` CR `bench-clickhouse-sink`:
  `com.clickhouse.kafka.connect.ClickHouseSinkConnector`, `tasksMax: 3`
  (= topic partitions), `topic2TableMap: hits=<hits_null|hits>` per tier,
  **`exactlyOnce: "false"`** (benchmark decision 3: at-least-once with the
  connector's dedup token active), `consumer.override.max.poll.records`
  = `CFG_MAX_POLL_RECORDS` (25000 baseline — co-sized with the heap; a 100k
  poll on a 2G heap once GC-spiraled the worker to death).
- The worker is **deliberately CPU-quota-bounded** (~0.96–0.99 of its 3.5-core
  limit during drains, symmetric across arms — this is the signed-off
  instrument definition, not a bug).

> **Chaos-test note**: exactly-once mode (`exactlyOnce: "true"`, KeeperMap
> state store on the CH side) is NOT what the benchmark exercises. The chaos
> task should parameterize this and test both modes — at-least-once+dedup is
> the mode with five pairs of zero-duplicate evidence behind it.

---

## 7. Orchestration (`orchestration/run_pair.sh`)

Phase anatomy (each phase fails loud; cleanup trap always tears down):
1. **Scale up** (4+1 nodes, preflight, broker Ready).
2. **Pre-load**: fresh topic (3 partitions) → sharded producer Job →
   `rows_expected` from offsets.
   2b. **Poller host pod** (`bench-poller`, runs `poller/sampler.py`).
3. **Per (arm, tier)** ×4: deploy Connect+connector → wait 3/3 tasks RUNNING
   (fast-fail on FAILED) → poller samples until **lag=0** (the drain) →
   finalize (client metrics insert, ~14 metrics) → capture SQL (server-side
   from `system.query_log` etc.) → **integrity** (tier 1) → run record →
   export (WARNs without a DWH role; the bridge re-exports later).
   3c. **Pair cost** → `run_cost_usd` on the first-run arm only.
4/5. **Teardown topic + scale down** (always, even on failure).

Key env contract (see `orchestration/README.md` §3 for the full list):
`ARM_ORDER_SPEC` ("pinned head" / "head pinned", alternating by day parity),
`ARM0_IMAGE`/`ARM1_IMAGE`/`PRODUCER_IMAGE` (digests), `PARQUET_SOURCE`,
`TARGET_CH_*` + `METRICS_CH_*` (both currently the same staging service),
`SOURCE_ROWS_EXPECTED`/`SOURCE_UNIQUE_EXPECTED` (10M/10M).

Operator conventions: launch detached (`nohup … & disown`) via a launcher
script that holds **no secrets** (env-passed), log to a scratchpad file,
watch with a filtered `tail -f` monitor for `PHASE|error|FAILED|ARMED|integrity`.

---

## 8. Verification & data model (the chaos test's oracle)

**`perf.*` on the metrics CH** (`sql/perf/*.sql`, mirrored to the DWH):
- `perf.runs` — one row per run: `run_id`, `connector`, versions, and the
  `runtime` Map (arm, tier, pair_id, config echo, **instrument truth**
  (instance type, cpu/mem limits), `flagged`/`flag_reason`/`outcome` per the
  contract).
- `perf.metrics` — tall (run_id, metric_name, value): throughput, drain
  seconds, CPU/heap, integrity numbers, covariates.
- `perf.ch_inserts` — one row per server-side insert (from `query_log`):
  batch sizes, durations, exception codes. ~400 rows per 10M-row drain.

**Integrity check** (`capture/check_integrity.py` + SQL 20) — *the
exactly-once oracle*:
```
rows_delivered   = count() FROM target table      == rows_expected (offsets)
unique_delivered = uniqExact(WatchID)             == SOURCE_UNIQUE_EXPECTED
duplicate_rows   = rows_delivered − rows_expected     (must be 0)
```
**⚠ Formula law (contract + SQL 20 header): `duplicate_rows` is the
target-vs-SOURCE count delta — NEVER `count() − uniqExact()` on the target.**
The source dataset may legitimately contain duplicate row-ids (full ClickBench
hits does; our 10M subset happens not to), and the banned formula would
false-positive every run on such data. Uniqueness is asserted separately
against the *source* constant. A chaos dataset with duplicate WatchIDs works
fine under the correct formula and breaks under the banned one.
Result lands as metrics (`integrity_ok`, `duplicate_rows`, `rows_delivered`)
on the run's row. **Loss** shows as `rows_delivered < expected`; **duplication**
as `duplicate_rows > 0`. This is precisely the pass/fail for a chaos run.
(Checker hardening in progress: an infra exception during the check must
classify as CHECK_ERROR/`integrity_unverified`, never as MISMATCH — a CH
read-timeout once mislabeled a perfect run.)

**Contract** (`docs/benchmark-v2-contract.md`, vendored from the spark repo,
sha-checked by CI): run identity, runtime keys, flag vocabulary
(`instrument_resize`, `instrument_shift`, …), verdict map, calibration rules.
A chaos run that must not pollute benchmark trends should either use a
**distinct `connector` value** (e.g. `kafka-connect-chaos`) — every dashboard
dataset filters by connector value — or carry a flag token. Distinct connector
is cleaner; if chaos data should stay entirely out of the DWH, simply don't
run the export.

**Export bridge**: spark repo workflow `export-run-to-dwh.yml`
(`workflow_dispatch`, inputs `run_ids` + `source=kafka`) → parquet to the DWH
ingestion bucket. ⚠ The DWH mirror is **append-only and never re-ingests a
rewritten same-key file** — get the data right *before* exporting.

**Dashboard**: Superset dashboard **432** (datasets 1537/1538/1539 on the DWH
connection). Standards: plain-English titles, every chart carries a
description with direction-of-goodness, verdict logic is fixture-accepted.

---

## 9. Chaos/"monkey" test — how to reuse all of this

**What transfers unchanged**: cluster + scale scripts, broker + registry,
producer (any row count via a staged prefix — `rows_expected` is
offsets-derived so any subset works), Connect/connector templates, poller,
`perf.*` capture, and above all the **integrity check as the oracle**.

**Kill primitives available on this infra**:
| Target | Primitive | Notes |
|---|---|---|
| Connect worker | `kubectl delete pod bench-connect-connect-0` mid-drain | Strimzi recreates it; tasks rebalance; measure re-delivery window + verify integrity after re-drain |
| Individual tasks | Connect REST (`/connectors/.../tasks/<n>/restart`) via the poller host | finer-grained than pod kill |
| Kafka broker | `kubectl delete pod bench-combined-0` | RF=1: the topic is briefly unavailable; PVC persists — data survives, clients must reconnect |
| Network | NetworkPolicy deny between Connect↔CH or Connect↔broker for T seconds | cleaner "partition" than pod kill |
| ClickHouse ingestion | **You cannot kill nodes of ClickHouse Cloud** (managed). Options: (a) revoke/alter the CH user mid-run, (b) `SYSTEM STOP MERGES`/quotas to induce pressure, (c) rely on the service's own idling/wake stalls (observed: connection hangs, read timeouts — the exact failure class that already bit `check_integrity`), or (d) for true node-kill semantics, deploy a small self-hosted CH **in-cluster** (a StatefulSet on bench-ng) as the chaos target — the connector doesn't care which CH it points at (`TARGET_CH_HOST`). Contract note: runs against an in-cluster CH record `environment_class='self_hosted'` (the 3298da9b scoping amendment covers exactly this). |

**Ground rules learned the hard way**:
1. Never run chaos concurrently with a benchmark pair (shared broker/target).
2. Use a distinct topic (`hits-chaos`), distinct consumer groups, distinct
   target tables (`clickbench.hits_chaos`), distinct `connector` value.
3. Digest-pin the images you test; record them in the runtime map.
4. Scale down when done; verify both nodegroups report 0.
5. Fail loud and classify honestly: infra error ≠ assertion failure
   (the CHECK_ERROR vs MISMATCH lesson).
6. Expect the CH staging service to idle (~15 s wake) and to stall
   occasionally — chaos assertions must retry/timeout accordingly, and that
   flakiness is itself chaos-relevant signal.

**Suggested chaos-run shape** (mirrors the pair anatomy so all tooling fits):
pre-load N rows (offsets-count) → start drain → at k% drained, inject fault →
allow recovery → drain to lag 0 → settle → **integrity**: `count()==N`,
`uniqExact==N_unique`, duplicates==0 → record a `perf.runs` row with
`connector='kafka-connect-chaos'` and runtime keys
(`fault_type`, `fault_at_pct`, `recovery_seconds`, `exactly_once=<bool>`) →
repeat across the fault matrix × both delivery modes.

---

## 10. Pointers

| Doc | What's in it |
|---|---|
| `orchestration/README.md` | full env contract, launch-by-hand guide, failure-policy semantics |
| `orchestration/PLAYBOOK.md` | S0–S7 operational gates for a pair |
| `docs/benchmark-v2-plan.md` | the benchmark's design decisions (§5 anatomy, §6 connector config, §8 dashboards) |
| `docs/benchmark-v2-contract.md` | naming/semantics law (runtime keys, flags, verdicts) |
| `producer/README.md` | sharding, count contract, memory bounds, OOM diagnosis |
| `poller/README.md` | sampler sources incl. the cadvisor/RBAC prerequisites |
| PR #789 | CI image builds via OIDC → ECR (pending IAM) |
