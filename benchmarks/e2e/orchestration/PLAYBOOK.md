# Benchmark v2 — LIVE EXECUTION PLAYBOOK (first validation pair)

Run these commands **verbatim** from the main session on a machine that has
`kubectl`, `eksctl`, `aws`, `docker` (buildx), `envsubst` (gettext), `python3`
with `boto3`/`pyyaml`, and the capture + poller Python deps installed
(`pip install -r benchmarks/e2e/capture/requirements.txt -r benchmarks/e2e/poller/requirements.txt boto3 pyyaml`).

Assumes: EKS cluster `kafka-bench` in `us-east-2` already exists with node group
`bench-ng` at desired 0 (`provision.sh` already run); Strimzi operator + gp3
StorageClass installed (`scale-up.sh` re-ensures them idempotently anyway);
target Cloud service already bootstrapped with `sql/` files 1-8 (perf.* schema,
`kafka_benchmark` user, capture grants). AWS admin creds are assumed present in
the environment (this playbook runs **no** `aws sts` assume-role dance — it uses
your ambient admin identity, unlike the CI OIDC role).

**Repo/branch:** `clickhouse-kafka-connect` on branch `benchmark-v2`. **NEVER push.**

**What this playbook replicates:** exactly what `.github/workflows/benchmark-nightly.yml`
would do, done manually — build both arm images + producer image, resolve the arm
order once, run `run_pair.sh`, verify, tear down. The workflow builds to GHCR; this
playbook builds to **ECR in us-east-2** so the EKS nodes can pull without registry
auth on the node.

---

## READ-FIRST: four findings that shape this playbook

1. **ROW_LIMIT is UNSUPPORTED end-to-end (S5).** `producer.py` accepts a
   `--limit` / `ROW_LIMIT` env, but `orchestration/render_producer_job.py` only
   patches `image`, `serviceAccountName`, and `PARQUET_SOURCE` into the Job — it
   never sets `ROW_LIMIT`, and `run_pair.sh phase_preload` never passes one. So
   the producer publishes the **entire** `PARQUET_SOURCE`. To get a ~10M-row
   validation pair you must point `PARQUET_SOURCE` at a **10-file subset** of the
   ClickBench partitioned parquet (each file is exactly 1,000,000 rows — verified
   below). See S0 `PARQUET_SOURCE` and S5.

2. **The public ClickBench bucket is in `eu-central-1`, not `us-east-1`.**
   Verified: `x-amz-bucket-region: eu-central-1` on
   `clickhouse-public-datasets`. The producer `job.yaml` hardcodes
   `AWS_REGION=us-east-2`; a direct cross-region read still works (S3 is global
   over HTTPS) but pays cross-region egress **eu-central-1 → us-east-2** every
   run. The grounded recommendation is to **stage a 10-file copy into
   `s3://shimons/clickbench-kafka-bench/hits-10m/` (us-east-2)** first (one-time),
   then point `PARQUET_SOURCE` there.

3. **`SOURCE_UNIQUE_EXPECTED` MUST be set or the pair dies as a FALSE integrity
   mismatch.** SQL 20 binds `{unique_expected}`; `capture/run_metrics_sql.py
   resolve_expected()` takes it from the `SOURCE_UNIQUE_EXPECTED` env constant —
   and **nothing sets it**: `run_pair.sh` exports only `SOURCE_ROWS_EXPECTED`
   (line 323), and neither the workflow nor `config.env` sets the unique
   constant. Grounded failure mode (resolve_expected, lines 75-86): with
   `SOURCE_UNIQUE_EXPECTED` unset and no `INPUT_PARQUET_GLOB`, it prints
   `WARNING: no INPUT_PARQUET_GLOB and SOURCE_ROWS_EXPECTED / SOURCE_UNIQUE_EXPECTED
   are not both set — integrity source ground truth is 0/0 and the check will FAIL.`
   and returns **`0.0, 0.0` for BOTH** (discarding even the valid rows constant).
   SQL 20 then *succeeds* — no error, no rollback — landing `rows_expected=0`,
   `unique_expected=0`, `duplicate_rows=+10000000`, `integrity_ok=0`. The runs row
   inserts, the export runs, and then `check_integrity.py` (the LAST gated step)
   `die`s with `TIER 1 INTEGRITY MISMATCH … run FAILS` on the **first arm's t1**,
   firing the cleanup trap so **the second arm never runs** — a full drain wasted
   on a false mismatch. Fix: pin `SOURCE_UNIQUE_EXPECTED=10000000` in S0 (the
   value is computed once from the staged subset — see the S5 staging gate).
   Why not the `INPUT_PARQUET_GLOB` fallback: the re-derivation query is
   `s3({glob}, NOSIGN, 'Parquet')` — **unauthenticated**, so it cannot read the
   private staged `s3://shimons/...` prefix at all; pointed at the public bucket
   it would `uniqExact`-scan the source **on every capture invocation**, on the
   memory-capped metrics service. The pinned constant costs one offline
   computation, ever.

4. **DWH export writes to bucket-ROOT prefixes, NOT `clickbench-kafka-bench/perf-export/`.**
   `export_metrics_to_dwh.py` writes `s3://$DWH_BUCKET/<table>/<run_id>.parquet`
   for `<table>` in `runs`, `metrics`, `ch_inserts`. There is **no**
   `clickbench-kafka-bench/perf-export/` prefix anywhere in the code. If you set
   `DWH_BUCKET=shimons`, exports land at `s3://shimons/runs/…`,
   `s3://shimons/metrics/…`, `s3://shimons/ch_inserts/…`. The S6/S7 `aws s3 ls`
   commands target those exact prefixes. (If you want the exports under a prefix,
   the export script does not support one — that would need a code change, out of
   scope here.)

---

## S0 — ENVIRONMENT CHECKLIST (one export block)

Every variable the whole sequence needs. Fill the placeholders, then `source` it
in the shell you will run every subsequent stage from.

```bash
# ---------------------------------------------------------------------------
# S0 env — Benchmark v2 first validation pair. Fill <...> placeholders.
# AWS admin identity is assumed already in the environment (aws sts
# get-caller-identity must succeed). Source this block once.
# ---------------------------------------------------------------------------
set -a   # export everything defined below

# --- AWS / cluster identity (match infra/env.sh + cluster.yaml) ---
AWS_REGION=us-east-2
COMPUTE_REGION=us-east-2            # contract runtime key (directive c); also run_pair.sh default
CLUSTER_NAME=kafka-bench
NODEGROUP_NAME=bench-ng
K8S_NAMESPACE=kafka-bench
SCALE_UP_NODES=2
AWS_ACCOUNT_ID=796575137974        # env.sh default; used to build the ECR registry host

# --- ECR registry host (S1 creates the two repos under it) ---
ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# --- Image refs (populated in S2 after build; slotted by arm order in S5) ---
# DIGEST-PINNED BY DEFAULT (stale-tag class fix): HEAD_IMAGE / PINNED_IMAGE /
# PRODUCER_IMAGE MUST be DIGEST references (repo@sha256:...), NOT tags. A mutable
# tag can be served STALE by node/registry caches (that made the first pair run
# the wrong image TWICE). S2 builds+pushes and CAPTURES each digest; the values
# below are placeholders overwritten in S2. run_pair.sh re-validates: it rejects
# a bare tag (unless KAFKA_ALLOW_TAG=1 / --allow-tag, local hacking only).
HEAD_IMAGE="<repo>@sha256:<...>"     # captured in S2 from build-arm.sh IMAGE_DIGEST=
PINNED_IMAGE="<repo>@sha256:<...>"   # captured in S2 (PINNED_REF = v1.3.10)
PRODUCER_IMAGE="<repo>@sha256:<...>" # captured in S2 (producer image digest)

# --- Arm order (S5 computes ARM_ORDER_SPEC; ARM0/ARM1 slotted from it) ---
# Leave ARM_ORDER_SPEC UNSET here — S5 sets it explicitly so image slots match.

# --- Producer S3 read (IRSA SA name; the IAM role/annotation is a prereq TODO) ---
PRODUCER_SA=hits-producer

# --- Parquet source (ROW_LIMIT unsupported — see READ-FIRST #1; S5 for the 10M subset) ---
# RECOMMENDED (validation size, us-east-2 staged): after the one-time stage in S5.
PARQUET_SOURCE="s3://shimons/clickbench-kafka-bench/hits-10m/"
# ALTERNATIVE (full ~100M, cross-region eu-central-1 read; NOT validation size):
#   PARQUET_SOURCE="s3://clickhouse-public-datasets/hits_compatible/hits.parquet"

# --- Integrity ground truth (READ-FIRST #3 — MANDATORY or the pair false-fails) ---
# SOURCE_UNIQUE_EXPECTED = uniqExact(WatchID) of the STAGED SUBSET. Computed once
# via clickhouse-local over exactly hits_0..hits_9 (S5 staging gate):
#   count()=10,000,000  uniqExact(WatchID)=10,000,000  (WatchID happens to be
#   fully unique within this 10-file subset; uniqExact is exact, not approximate).
# THIS CONSTANT IS TIED TO THE SUBSET: change the staged files (more/fewer/other
# hits_N, or the full dataset) and this value is WRONG — recompute it (S5 command)
# before running. (SOURCE_ROWS_EXPECTED is NOT set here: run_pair.sh derives it
# from the producer's committed offsets, line 323.)
SOURCE_UNIQUE_EXPECTED=10000000

# --- ClickHouse: drain TARGET (the dedicated us-east-2 staging service) ---
TARGET_CH_HOST="<service>.us-east-2.aws.clickhouse-staging.com"
TARGET_CH_USER=kafka_benchmark
TARGET_CH_PASSWORD="<target-password>"

# --- ClickHouse: METRICS landing (perf.* lives here) ---
# SPLIT-BRAIN NOTE: for this first pair the metrics landing IS the same service
# as the target (perf.* was bootstrapped on it by sql/ files 1-8). So set
# METRICS_CH_* = TARGET_CH_*. run_pair.sh keeps them as distinct env names on
# purpose (finalize_and_insert_metrics remaps the poller inserter onto
# METRICS_CH_*); if you ever split the two services, change ONLY these three.
METRICS_CH_HOST="${TARGET_CH_HOST}"
METRICS_CH_USER="${TARGET_CH_USER}"
METRICS_CH_PASSWORD="${TARGET_CH_PASSWORD}"

# --- DWH export ---
# DWH_BUCKET is a bare bucket NAME (no s3:// prefix, no path). shimons is us-east-1;
# cross-region export from a us-east-2 CH service is fine (HTTPS PutObject).
# Exports land at s3://shimons/{runs,metrics,ch_inserts}/<run_id>.parquet (READ-FIRST #4).
DWH_BUCKET=shimons
DWH_BUCKET_REGION=us-east-1                 # avoids the HEAD region-probe round trip
DWH_ROLE_ARN="<write-only-DWH-role-arn>"    # sts:AssumeRole-able by your admin identity;
                                            # needs s3:PutObject on the bucket. If you
                                            # instead want to write with your OWN admin
                                            # creds, see S6 note on DWH_ROLE_ARN.

# --- run_pair.sh knobs that have safe defaults but are worth pinning ---
CFG_MAX_POLL_RECORDS=100000                 # sink batch size (plan §6 first-run value)
CFG_TASKS_MAX=3
POLL_INTERVAL=10
POLL_TIMEOUT=3600                           # per-drain lag-0 timeout (s)
PRODUCER_TIMEOUT=21600                      # producer Job wait (s); 10M finishes far sooner
set +a
```

**Complete list of env vars `run_pair.sh` reads (derived from the script — missing
one aborts mid-run):**

| Var | Required? | Where read | Effect if missing |
|---|---|---|---|
| `ARM_ORDER_SPEC` | recommended | `resolve_arm_order` | falls back to UTC-day parity (fine, but S5 sets it) |
| `ARM0_IMAGE` / `ARM1_IMAGE` | **yes** | `main` (`:?ARM0_IMAGE required`) | hard `die` |
| `PRODUCER_IMAGE` | **yes** | `phase_preload`, `phase_poller_host` (`:?`) | hard `die` |
| `PARQUET_SOURCE` | **yes** | `phase_preload` (`:?`) | hard `die` |
| `PRODUCER_SA` | no (default `hits-producer`) | `phase_preload` | uses default |
| `TARGET_CH_HOST/USER/PASSWORD` | **yes** | `apply_secret_and_metrics` (`:?`), capture scripts | hard `die` / capture fail |
| `METRICS_CH_HOST/USER/PASSWORD` | **yes** | `finalize_and_insert_metrics` (`:?`), capture/export | hard `die` |
| `COMPUTE_REGION` | defaulted to `us-east-2` | `main`, `build_runtime_json` (mandatory) | default applied |
| `DWH_ROLE_ARN` / `DWH_BUCKET` | **yes for export** | `export_metrics_to_dwh.py` | export WARN (metrics still land) |
| `SOURCE_UNIQUE_EXPECTED` | **yes (tier-1 integrity)** | `run_metrics_sql.py resolve_expected()` via SQL 20 | resolve_expected returns 0/0 for BOTH constants → SQL 20 lands `integrity_ok=0` → `check_integrity.py` FALSE-fails the first arm's t1 and kills the pair (READ-FIRST #3) |
| `SOURCE_ROWS_EXPECTED` | auto | exported by `run_pair.sh` line 323 from the producer's committed-offset count | do NOT set manually |
| `K8S_NAMESPACE` | no (default `kafka-bench`) | top of script | default |
| `SCALE_UP_NODES` | no (default 2) | `phase_scale_up`, `phase_pair_cost` | default |
| `CFG_*` (`MAX_POLL_RECORDS`, `TASKS_MAX`, `PARTITION_SCHEME`, …) | no (plan §6 defaults) | `main` | defaults |
| `PRODUCER_TIMEOUT` | no (default 21600) | `phase_preload` | default |
| `POLL_INTERVAL` / `POLL_TIMEOUT` | no (10 / 3600) | poller | defaults |
| `TASKS_RUNNING_TIMEOUT` | no (600) | `wait_tasks_running` | default |
| Sourced from `config.env` at runtime | — | `TARGET_REGION=us-east-2`, `ENVIRONMENT_CLASS=staging`, `CONNECTOR=kafka-connect`, `QUERY_LOG_USER=kafka_benchmark`, `CH_DATABASE=clickbench` | already in repo; do NOT set inline |
| `GIT_SHA` / `CONNECTOR_VERSION` / `KAFKA_CONNECT_VERSION` / `STRIMZI_VERSION` / `PLUGIN_SHA256` | auto | `deploy_connect` reads them from the image's `/opt/benchmark/provenance.json` | per-arm from image |

**VERIFICATION GATE (S0):**
```bash
aws sts get-caller-identity --query Arn --output text        # must print your admin ARN
kubectl config current-context                               # must resolve; if not, S3 does update-kubeconfig
echo "$TARGET_CH_HOST / $METRICS_CH_HOST"; [ "$TARGET_CH_HOST" = "$METRICS_CH_HOST" ] && echo "split-brain: metrics==target (expected for first pair)"
```
**ABORT/ROLLBACK (S0):** nothing created yet — just re-source with corrected values.

---

## S1 — REGISTRY (ECR, us-east-2)

**PRE-CHECK:**
```bash
aws sts get-caller-identity --query Account --output text     # expect 796575137974
```

**COMMANDS (verbatim):**
```bash
# Two repos: one for the arm images, one for the producer. Tag Project=clickbench-benchmark.
aws ecr create-repository --repository-name connect-bench  --region "$AWS_REGION" \
  --tags Key=Project,Value=clickbench-benchmark || echo "connect-bench exists (ok)"
aws ecr create-repository --repository-name producer-bench --region "$AWS_REGION" \
  --tags Key=Project,Value=clickbench-benchmark || echo "producer-bench exists (ok)"

# Docker login to ECR (token valid ~12h).
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$ECR_REGISTRY"
```

**VERIFICATION GATE (S1):**
```bash
aws ecr describe-repositories --region "$AWS_REGION" \
  --repository-names connect-bench producer-bench \
  --query 'repositories[].repositoryUri' --output text
# expect:  <acct>.dkr.ecr.us-east-2.amazonaws.com/connect-bench   ...producer-bench
docker login "$ECR_REGISTRY" >/dev/null 2>&1 && echo "docker login OK"
```

**EKS node pull permission:** `cluster.yaml` uses an eksctl **managed** node group
(`managedNodeGroups: - name: bench-ng`). eksctl attaches
`AmazonEC2ContainerRegistryReadOnly` to the managed node role **by default**, so
the nodes can pull from ECR in-account with no extra wiring. To confirm on the
live cluster (and, if somehow absent, the fix):
```bash
# Confirm the policy is attached to the node role:
NODE_ROLE=$(aws eks describe-nodegroup --cluster-name "$CLUSTER_NAME" \
  --nodegroup-name "$NODEGROUP_NAME" --region "$AWS_REGION" \
  --query 'nodegroup.nodeRole' --output text | awk -F/ '{print $NF}')
aws iam list-attached-role-policies --role-name "$NODE_ROLE" \
  --query "AttachedPolicies[?PolicyName=='AmazonEC2ContainerRegistryReadOnly']" --output text
# EXPECTED: a non-empty line naming the policy. If EMPTY, attach it:
#   aws iam attach-role-policy --role-name "$NODE_ROLE" \
#     --policy-arn arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly
```

**ABORT/ROLLBACK (S1):**
```bash
aws ecr delete-repository --repository-name connect-bench  --region "$AWS_REGION" --force
aws ecr delete-repository --repository-name producer-bench --region "$AWS_REGION" --force
```

---

## S2 — IMAGES (build both arms + producer, push to ECR)

**PRE-CHECK:**
```bash
docker buildx version && docker login "$ECR_REGISTRY" >/dev/null 2>&1 && echo ok
# build-arm.sh head builds the plugin via ./gradlew createConfluentArchive — needs a JDK + gradle wrapper.
```

> `build-arm.sh` interface (grounded): flags are `--arm head|pinned`,
> `--image-tag <full ref>`, `--push`, `--digest-out <file>`, `--no-docker`. It
> does **not** take an `IMAGE_BASE`. Env overrides: `STRIMZI_IMAGE`,
> `STRIMZI_VERSION`, `KAFKA_VERSION`, `AVRO_CONVERTER_VERSION/SHA256`. Its docker
> **build context is the docker/ directory itself** (`"${SCRIPT_DIR}"` =
> `benchmarks/e2e/docker/`), not `benchmarks/e2e/`. The producer image, by
> contrast, builds with context `benchmarks/e2e/` (its Dockerfile pulls in
> `../schema/`). **`--push` builds, pushes, AND resolves the registry digest**,
> emitting `IMAGE_DIGEST=<repo>@sha256:...` on stdout and (with `--digest-out`)
> writing it to a file. DIGEST-PINNING IS THE DEFAULT: capture the digest and
> export `*_IMAGE` with it — the tag is only a build handle.

**COMMANDS (verbatim):**
```bash
cd /path/to/clickhouse-kafka-connect     # repo root; branch benchmark-v2

# --- HEAD arm: builds the plugin from the current working tree, pushes, and
#     resolves the DIGEST. The tag pins the commit under test (build handle);
#     the digest is what we deploy. ---
HEAD_SHORT_SHA=$(git rev-parse --short=12 HEAD)
HEAD_TAG="${ECR_REGISTRY}/connect-bench:head-${HEAD_SHORT_SHA}"
./benchmarks/e2e/docker/build-arm.sh --arm head --image-tag "$HEAD_TAG" \
  --push --digest-out /tmp/head.digest
HEAD_IMAGE="$(cat /tmp/head.digest)"        # repo@sha256:...  (the deployed ref)

# --- PINNED arm: bakes the released v1.3.10 plugin zip (PINNED_REF), sha-verified. ---
PINNED_TAG="${ECR_REGISTRY}/connect-bench:pinned-v1.3.10"
./benchmarks/e2e/docker/build-arm.sh --arm pinned --image-tag "$PINNED_TAG" \
  --push --digest-out /tmp/pinned.digest
PINNED_IMAGE="$(cat /tmp/pinned.digest)"    # repo@sha256:...

# --- Producer image (context = benchmarks/e2e/; Dockerfile references ../schema/).
#     No build-arm.sh here, so push then resolve the digest by hand. ---
PRODUCER_TAG="${ECR_REGISTRY}/producer-bench:latest"
docker build -f benchmarks/e2e/producer/Dockerfile -t "$PRODUCER_TAG" benchmarks/e2e
docker push "$PRODUCER_TAG"
# Resolve the pushed digest (same mechanism build-arm.sh uses: RepoDigests):
PRODUCER_IMAGE="$(docker inspect --format='{{index .RepoDigests 0}}' "$PRODUCER_TAG")"

# Re-export the resolved DIGEST refs into the env (overwrites the S0 placeholders).
export HEAD_IMAGE PINNED_IMAGE PRODUCER_IMAGE
echo "HEAD_IMAGE=$HEAD_IMAGE"; echo "PINNED_IMAGE=$PINNED_IMAGE"; echo "PRODUCER_IMAGE=$PRODUCER_IMAGE"
```

**VERIFICATION GATE (S2)** — every `*_IMAGE` MUST be a digest ref, and the digest
must exist in ECR:
```bash
# 1. Shape: all three are repo@sha256:... (never a bare tag).
for v in "$HEAD_IMAGE" "$PINNED_IMAGE" "$PRODUCER_IMAGE"; do
  case "$v" in *@sha256:*) echo "OK digest: $v" ;; *) echo "NOT A DIGEST: $v" ; false ;; esac
done
# 2. The digest exists in ECR (S2 gate — pinned by digest, verified server-side):
aws ecr describe-images --repository-name connect-bench --region "$AWS_REGION" \
  --image-ids imageDigest="${HEAD_IMAGE##*@}"   --query 'imageDetails[0].imageDigest' --output text
aws ecr describe-images --repository-name connect-bench --region "$AWS_REGION" \
  --image-ids imageDigest="${PINNED_IMAGE##*@}" --query 'imageDetails[0].imageDigest' --output text
aws ecr describe-images --repository-name producer-bench --region "$AWS_REGION" \
  --image-ids imageDigest="${PRODUCER_IMAGE##*@}" --query 'imageDetails[0].imageDigest' --output text
# (each prints back the same sha256:... — proves the digest is present)
# Provenance sanity (the arm image must carry /opt/benchmark/provenance.json):
docker run --rm --entrypoint cat "$HEAD_IMAGE" /opt/benchmark/provenance.json | python3 -m json.tool
# expect git_sha == $HEAD_SHORT_SHA (12-char prefix), connector_version from VERSION.
```

**ABORT/ROLLBACK (S2):**
```bash
aws ecr batch-delete-image --repository-name connect-bench --region "$AWS_REGION" \
  --image-ids imageDigest="${HEAD_IMAGE##*@}" imageDigest="${PINNED_IMAGE##*@}"
aws ecr batch-delete-image --repository-name producer-bench --region "$AWS_REGION" \
  --image-ids imageDigest="${PRODUCER_IMAGE##*@}"
```

---

## S3 — SCALE UP + PLATFORM VERIFY

**PRE-CHECK:**
```bash
aws eks update-kubeconfig --name "$CLUSTER_NAME" --region "$AWS_REGION"
kubectl get nodes            # expect "No resources found" (node group at 0)
```

**COMMANDS (verbatim):**
```bash
# scale-up.sh: node group 0 -> 2, wait nodes Ready, ensure operator+storageclass,
# apply kafka.yaml + schema-registry.yaml, wait Kafka CR Ready + Registry rollout.
benchmarks/e2e/infra/scale-up.sh 2
```

**VERIFICATION GATE (S3)** — these mirror the script's own internal waits:
```bash
kubectl get nodes --no-headers | grep -cw Ready          # expect 2
kubectl -n "$K8S_NAMESPACE" get kafka bench \
  -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}'; echo   # expect True
kubectl -n "$K8S_NAMESPACE" rollout status deploy/schema-registry --timeout=10s   # "successfully rolled out"
kubectl -n "$K8S_NAMESPACE" rollout status deploy/strimzi-cluster-operator --timeout=10s
# In-cluster endpoints scale-up.sh prints:
#   bootstrap: bench-kafka-bootstrap.kafka-bench.svc:9092
#   registry : http://schema-registry.kafka-bench.svc:8081
```

**Who applies `connect-metrics-configmap.yaml`?** `run_pair.sh` does — inside
`apply_secret_and_metrics()` (`kubectl apply -f ${TEMPLATES_DIR}/connect-metrics-configmap.yaml`),
called once per arm from `phase_arm`. **Do NOT apply it manually in S3.** It is
applied together with the per-run CH-creds Secret in S5.

**ABORT/ROLLBACK (S3):**
```bash
benchmarks/e2e/infra/scale-down.sh          # node group -> 0, cascades broker PVC/EBS
```

---

## S4 — CREDS INTO THE CLUSTER

**This stage collapses into S5.** `run_pair.sh` creates the CH-creds Secret
itself: `apply_secret_and_metrics()` runs
```
kubectl create secret generic bench-ch-creds \
  --from-literal=hostname=$TARGET_CH_HOST \
  --from-literal=username=$TARGET_CH_USER \
  --from-literal=password=$TARGET_CH_PASSWORD  (dry-run|apply)
```
per arm, keyed `hostname`/`username`/`password` — exactly the keys the
`kafkaconnect.yaml.tmpl` `externalConfiguration` maps to `CH_HOSTNAME`/
`CH_USERNAME`/`CH_PASSWORD`. **You create no Secret by hand.** Just ensure
`TARGET_CH_HOST/USER/PASSWORD` are in the env (S0). Nothing to run here.

**VERIFICATION GATE (S4):** none independent — verified live in S5 when the
connector reaches RUNNING (a wrong secret surfaces as an auth failure in the
connector task state).

---

## S5 — THE PAIR

### ROW_LIMIT grounding (the S5 decision)

**ROW_LIMIT is NOT plumbed through the orchestrator** (READ-FIRST #1). The
producer publishes the full `PARQUET_SOURCE`. To run a **10,000,000-row**
validation pair you point `PARQUET_SOURCE` at a **10-file subset** of the
partitioned ClickBench parquet. Verified against the public bucket
(`--no-sign-request`, bucket region `eu-central-1`):

- `s3://clickhouse-public-datasets/hits_compatible/athena_partitioned/hits_0.parquet` …
  `hits_98.parquet` are each **exactly 1,000,000 rows**; `hits_99.parquet` is
  997,497. So `hits_0..hits_9` = **exactly 10,000,000 rows** → `rows_expected`
  will be `10000000` (Tier 1 integrity target).
- The single canonical file `.../hits_compatible/hits.parquet` is the full
  ~99,997,497-row dataset (14.8 GB) — that is the FULL-size pair, not validation.

`pyarrow.dataset(source, format="parquet")` (producer.py line 323) accepts a
directory/prefix (reads **all** files under it) or a single file. There is no
per-file selection flag, so the validation subset must be a **prefix that
contains exactly the 10 files**. Stage them into us-east-2 (also fixes the
eu-central-1 cross-region egress, READ-FIRST #2):

**ONE-TIME STAGE (10M subset → us-east-2):**
```bash
for i in $(seq 0 9); do
  aws s3 cp \
    "s3://clickhouse-public-datasets/hits_compatible/athena_partitioned/hits_${i}.parquet" \
    "s3://shimons/clickbench-kafka-bench/hits-10m/hits_${i}.parquet" \
    --source-region eu-central-1 --region us-east-1 --no-sign-request 2>/dev/null \
  || aws s3 cp \
    "s3://clickhouse-public-datasets/hits_compatible/athena_partitioned/hits_${i}.parquet" \
    "/tmp/hits_${i}.parquet" --source-region eu-central-1 --no-sign-request \
  && aws s3 cp "/tmp/hits_${i}.parquet" \
    "s3://shimons/clickbench-kafka-bench/hits-10m/hits_${i}.parquet"
done
# Gate: 10 objects, ~1.2 GB total
aws s3 ls s3://shimons/clickbench-kafka-bench/hits-10m/ | grep -c hits_    # expect 10
```

**ONE-TIME INTEGRITY CONSTANT (SOURCE_UNIQUE_EXPECTED — READ-FIRST #3):** compute
`uniqExact(WatchID)` of the staged subset with `clickhouse-local` on the operator
machine, reading the staged files with your env AWS creds (the `s3()` 4-cred-arg
form, same shape `export_metrics_to_dwh.py` uses):

```bash
clickhouse local --query "
SELECT count() AS rows, uniqExact(WatchID) AS unique_watchid
FROM s3('https://shimons.s3.us-east-1.amazonaws.com/clickbench-kafka-bench/hits-10m/hits_{0..9}.parquet',
        '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY', '$AWS_SESSION_TOKEN', 'Parquet')"
# EXPECTED (staged files are byte-identical copies of public hits_0..hits_9):
#   10000000	10000000
```
> If your creds are a long-lived key pair with no session token, drop the
> `'$AWS_SESSION_TOKEN'` argument (3-arg form). Query shape + expected output
> were **verified live** against the identical public files (NOSIGN, no
> mutation): `clickhouse local --query "SELECT count(), uniqExact(WatchID) FROM
> s3('https://clickhouse-public-datasets.s3.eu-central-1.amazonaws.com/hits_compatible/athena_partitioned/hits_{0..9}.parquet', NOSIGN, 'Parquet')"`
> → `10000000	10000000` (clickhouse-local 25.12.1.649; the `hits_{0..9}.parquet`
> glob cannot over-match `hits_10..99` because of the `.parquet` suffix).
> Set the S0 `SOURCE_UNIQUE_EXPECTED` to the printed `unique_watchid`. If it is
> not `10000000`, the staged subset is NOT hits_0..hits_9 — stop and re-stage.
> Do NOT use the `INPUT_PARQUET_GLOB` fallback instead: its re-derivation is
> `s3(glob, NOSIGN, …)` (cannot read the private staged bucket) and would rescan
> the source per capture run on the memory-capped target (READ-FIRST #3).

Then `PARQUET_SOURCE="s3://shimons/clickbench-kafka-bench/hits-10m/"` (already the
S0 default). **Producer IRSA prereq:** the `hits-producer` ServiceAccount in
`kafka-bench` must be annotated with an IAM role that can
`s3:GetObject`/`ListBucket` on that prefix (README step 0.4 — NOT automated;
`render_producer_job.py` wires only the SA *name*). Confirm before S5:
```bash
kubectl -n "$K8S_NAMESPACE" get sa "$PRODUCER_SA" \
  -o jsonpath='{.metadata.annotations.eks\.amazonaws\.com/role-arn}'; echo
# EXPECTED: an arn:aws:iam::...:role/... . If EMPTY, create the IRSA role and annotate:
#   eksctl create iamserviceaccount --cluster kafka-bench --region us-east-2 \
#     --namespace kafka-bench --name hits-producer \
#     --attach-policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess \
#     --approve --override-existing-serviceaccounts
```

### Arm order (compute ARM_ORDER_SPEC exactly as CI does)

**PRE-CHECK:**
```bash
DOY=$(date -u +%j); echo "UTC day-of-year=$DOY parity=$((10#$DOY % 2)) (0=even=head first, 1=odd=pinned first)"
```

**COMMANDS (verbatim):**
```bash
# Resolve arm order ONCE and slot the images (this is what benchmark-nightly.yml does).
DOY=$(date -u +%j)
if [ $((10#$DOY % 2)) -eq 0 ]; then
  export ARM_ORDER_SPEC="head pinned"; export ARM0_IMAGE="$HEAD_IMAGE";   export ARM1_IMAGE="$PINNED_IMAGE"
else
  export ARM_ORDER_SPEC="pinned head"; export ARM0_IMAGE="$PINNED_IMAGE"; export ARM1_IMAGE="$HEAD_IMAGE"
fi
echo "ARM_ORDER_SPEC='$ARM_ORDER_SPEC'  ARM0_IMAGE=$ARM0_IMAGE  ARM1_IMAGE=$ARM1_IMAGE"

# Dry-run the phase plan first (executes nothing; needs no creds):
benchmarks/e2e/orchestration/run_pair.sh --plan
```
> The `--plan` output must show `arm order: <first> then <second>` matching your
> parity and 4 run rows. If `ARM_ORDER_SPEC` is set, the script uses it verbatim
> and never re-derives parity (review F10) — this is why we set it explicitly.

**RUN THE PAIR (verbatim):**
```bash
benchmarks/e2e/orchestration/run_pair.sh
```

### Mid-run observation points (per phase)

Run these in a **second terminal** (also `source` the S0 block there, and
`aws eks update-kubeconfig`). The orchestrator logs each phase with a
`[run_pair]` prefix; watch the pod/CR state alongside:

| Phase (log line) | Watch |
|---|---|
| PHASE 1 scale up | `kubectl get nodes` (2 Ready), already up from S3 |
| PHASE 2 pre-load | `kubectl -n $K8S_NAMESPACE get kafkatopic hits` (Ready, 3 parts); `kubectl -n $K8S_NAMESPACE logs -f job/hits-producer` — last stdout line is the JSON summary with `"rows_expected":10000000` |
| PHASE 2b poller host | `kubectl -n $K8S_NAMESPACE get pod bench-poller` (Running) |
| PHASE 3 per arm — Connect up | `kubectl -n $K8S_NAMESPACE get kafkaconnect bench-connect -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}'` |
| PHASE 3 — connector RUNNING | `kubectl -n $K8S_NAMESPACE get kafkaconnector bench-clickhouse-sink -o jsonpath='{.status.connectorStatus.connector.state} tasks={.status.connectorStatus.tasks[*].state}'` — expect `RUNNING tasks=RUNNING RUNNING RUNNING` |
| PHASE 3 — drain (poller) | poller runs in-cluster; the runner blocks on `kubectl exec bench-poller`. Progress in `benchmarks/e2e/orchestration/artifacts/samples-<run_id>.jsonl` (flushed per tick) |
| PHASE 3c pair cost | `[run_pair] PHASE 3c: pair cost` — emits `run_cost_usd` on the first-arm t1 row |
| PHASE 4/5 teardown | `kubectl -n $K8S_NAMESPACE get kafkatopic,kafkaconnect,kafkaconnector` empties; node group → 0 |

### Abort mid-run

`run_pair.sh` installs `trap cleanup_trap EXIT INT TERM` (line 780). A **single
Ctrl-C** (SIGINT) in the run terminal fires `cleanup_trap`, which — verified in
the script — deletes the KafkaConnector CR, the KafkaConnect CR, the
`bench-ch-creds` Secret, best-effort deletes all four `ch-sink-{head,pinned}-t{0,1}`
consumer groups, deletes the `bench-poller` pod, deletes the KafkaTopic, and runs
`scale-down.sh`. `fail_run` also rolls back partial perf rows for the current
`RUN_ID`. Give it time — the trap's `phase_scale_down` waits on PVC deletion (up
to 300s) before dropping nodes.

**POST-ABORT VERIFICATION:**
```bash
kubectl -n "$K8S_NAMESPACE" get kafkatopic,kafkaconnect,kafkaconnector,secret bench-ch-creds,pod bench-poller
# expect: no kafkatopic/hits, no CRs, no bench-ch-creds, no bench-poller
eksctl get nodegroup --cluster "$CLUSTER_NAME" --region "$AWS_REGION" -o json \
  | python3 -c 'import sys,json;print("desired=",json.load(sys.stdin)[0].get("DesiredCapacity"))'   # desired= 0
# Consumer groups clean:
kubectl -n "$K8S_NAMESPACE" exec bench-combined-0 -- \
  bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list 2>/dev/null \
  | grep -c '^ch-sink-' || true    # expect 0 (or command errors if broker already gone — also fine)
```

**ABORT/ROLLBACK (S5):** the trap above is the primary path. If a hard kill left
residue, run the S3 rollback (`scale-down.sh`) plus:
```bash
kubectl -n "$K8S_NAMESPACE" delete kafkaconnector bench-clickhouse-sink --ignore-not-found
kubectl -n "$K8S_NAMESPACE" delete kafkaconnect bench-connect --ignore-not-found
kubectl -n "$K8S_NAMESPACE" delete secret bench-ch-creds --ignore-not-found
kubectl -n "$K8S_NAMESPACE" delete pod bench-poller --ignore-not-found
kubectl -n "$K8S_NAMESPACE" delete kafkatopic hits --ignore-not-found
```

---

## S6 — ACCEPTANCE VERIFICATION

All queries run against the **METRICS** service (`perf.*` landing). Use the
ClickHouse HTTP interface on 8443 (the connector's port) with the metrics creds.
`PAIR_ID` is the `<UTC-ts>-<shortsha>` printed by `run_pair.sh` (line
`pair_id=...`) and is the common prefix of all 4 run_ids.

Helper (curl form against HTTPS 8443):
```bash
chq() {  # chq "<SQL>"
  curl -sS "https://${METRICS_CH_HOST}:8443/?default_format=TabSeparatedWithNames" \
    --user "${METRICS_CH_USER}:${METRICS_CH_PASSWORD}" --data-binary "$1"
}
PAIR_ID="<paste from run_pair.sh log>"
FIRST_ARM="${ARM_ORDER_SPEC%% *}"     # 'head' or 'pinned' — the first-run arm
```

**1. Four runs rows, one pair_id, mandatory scope keys + runtime keys present:**
```bash
chq "SELECT run_id, runtime['arm'] arm, runtime['tier'] tier,
            runtime['pair_id'] pair_id, runtime['target_region'] tr,
            runtime['environment_class'] ec, runtime['compute_region'] cr,
            runtime['batch_size'] bs, runtime['write_parallelism'] wp,
            runtime['partition_scheme'] ps, mapContains(runtime,'warm_up') has_warmup,
            mapContains(runtime,'outcome') has_outcome
     FROM perf.runs WHERE runtime['pair_id']='${PAIR_ID}' ORDER BY run_id"
```
Expected sketch (4 rows), runtime keys that MUST be present and non-empty:
`arm, tier, pair_id, target_region(=us-east-2), environment_class(=staging),
compute_region(=us-east-2), batch_size(=100000), write_parallelism(=3),
partition_scheme(=toYear(EventDate)), async_insert(=0), dataset(=hits)`; and
`has_warmup` MUST be `0` (warm_up absent by design), and on a CLEAN pair
`has_outcome` MUST be `0` on all 4 rows — the `outcome` key is present ONLY on
failed-class runs (value `failed`; contract §1.3 amendment). A row with
`runtime['outcome']='failed'` means that run's ingest failed but was captured
anyway — its tier ratio is non-computable; exclude it by outcome, not absence.
```
run_id                              arm     tier  pair_id            tr        ec       cr        bs      wp  ps               has_warmup
<PAIR>-<arm0>-t0                    <arm0>  0     <PAIR>             us-east-2 staging  us-east-2 100000  3   toYear(EventDate) 0
<PAIR>-<arm0>-t1                    <arm0>  1     ...
<PAIR>-<arm1>-t0                    <arm1>  0     ...
<PAIR>-<arm1>-t1                    <arm1>  1     ...
```

**2. Poller + capture metrics joined per run_id (both sources landed for each run):**
```bash
chq "SELECT run_id,
            countIf(metric_name IN ('drain_seconds','drain_rate_stability','connect_jvm_heap_peak','lag_reached_zero')) poller_side,
            countIf(metric_name IN ('ch_avg_rows_per_insert','parts_per_insert','ch_insert_cpu_seconds_per_Mrows','settle_seconds')) capture_side
     FROM perf.metrics WHERE run_id LIKE '${PAIR_ID}%'
     GROUP BY run_id ORDER BY run_id"
```
Expected: 4 rows, each with `poller_side>0` AND `capture_side>0` (both the
in-cluster poller finalize and the CH-side capture SQL landed under the same run_id).

**3. Tier-1 integrity rows present (contract §3) — tier 1 rows only:**
```bash
chq "SELECT run_id, metric_name, value FROM perf.metrics
     WHERE run_id LIKE '${PAIR_ID}%-t1'
       AND metric_name IN ('integrity_ok','rows_delivered','rows_expected',
                           'unique_delivered','unique_expected','duplicate_rows')
     ORDER BY run_id, metric_name"
```
Expected: for each `-t1` run, `rows_expected=10000000`, `rows_delivered=10000000`,
`unique_expected=10000000`, `unique_delivered=10000000`, `duplicate_rows=0`,
`integrity_ok=1`. If `rows_expected=0` AND `unique_expected=0`,
`SOURCE_UNIQUE_EXPECTED` was unset (READ-FIRST #3 — false mismatch, see the
failure table).

**4. `ch_insert_cpu_share_tier0` present on TIER-0 rows (new requirement) — and
NOT on tier-1 (it is tier-0-only, run_pair.sh line 563-565):**
```bash
chq "SELECT run_id, value FROM perf.metrics
     WHERE metric_name='ch_insert_cpu_share_tier0' AND run_id LIKE '${PAIR_ID}%'
     ORDER BY run_id"
```
Expected: exactly 2 rows, the two `-t0` run_ids, value a percent (unit `percent`).
If value camps near 100 the Null target is parse-bound (diagnostic, not a failure).

**5. `run_cost_usd` on exactly ONE row (first-run arm's t1):**
```bash
chq "SELECT run_id, value FROM perf.metrics
     WHERE metric_name='run_cost_usd' AND run_id LIKE '${PAIR_ID}%'"
```
Expected: exactly 1 row = `${PAIR_ID}-${FIRST_ARM}-t1`, a small USD float.

**6. First functional milestone (plan §6): `ch_avg_rows_per_insert >= 50000` on Tier 1:**
```bash
chq "SELECT run_id, value FROM perf.metrics
     WHERE metric_name='ch_avg_rows_per_insert' AND run_id LIKE '${PAIR_ID}%-t1'"
```
Expected: both `-t1` values `>= 50000`. Below → task 32 tunes poll size (not a code defect).

**7. S3 export objects landed (READ-FIRST #4 — bucket-root prefixes, keyed by run_id):**
```bash
for r in "${PAIR_ID}-${FIRST_ARM}-t1"; do
  for t in runs metrics ch_inserts; do
    aws s3 ls "s3://${DWH_BUCKET}/${t}/${r}.parquet"
  done
done
# Broader (all 4 runs) — one runs/metrics object per run_id exported:
aws s3 ls "s3://${DWH_BUCKET}/runs/"    | grep "${PAIR_ID}"
aws s3 ls "s3://${DWH_BUCKET}/metrics/" | grep "${PAIR_ID}"
```
Expected: a `.parquet` object per exported run under each of `runs/`, `metrics/`,
`ch_inserts/`. (`ch_inserts` may be empty for a run with no captured raw inserts —
the file is still written with 0 rows.)

> **DWH_ROLE_ARN note:** `export_metrics_to_dwh.py` always `sts:AssumeRole`s
> `DWH_ROLE_ARN` then hands those creds to CH's `s3()` function. If you do not
> have a dedicated write-only DWH role, create/point one that trusts your admin
> identity and can `s3:PutObject` on `shimons`. Export failure is a **WARN** in
> `run_pair.sh` (metrics still persist), so acceptance criteria 1-6 can pass even
> if 7 needs a role fix + a manual re-run of the export
> (`cd benchmarks/e2e/capture && RUN_ID=<run> python3 export_metrics_to_dwh.py`).

**ABORT/ROLLBACK (S6):** read-only stage. To purge a bad pair's perf rows:
`cd benchmarks/e2e/capture && for r in <the 4 run_ids>; do RUN_ID=$r python3 rollback_run_metrics.py; done`.

---

## S7 — TEARDOWN / CLOSE

`run_pair.sh` already scales the node group to 0 and deletes the topic on a
clean finish (PHASE 4/5) and on abort (trap). S7 is the explicit gate + the
persist/do-not list.

**COMMANDS (verbatim) — idempotent belt-and-suspenders:**
```bash
benchmarks/e2e/infra/scale-down.sh
```

**VERIFICATION GATE (S7):**
```bash
# 1. Node group desired 0:
eksctl get nodegroup --cluster "$CLUSTER_NAME" --region "$AWS_REGION" -o json \
  | python3 -c 'import sys,json;print("desired=",json.load(sys.stdin)[0].get("DesiredCapacity"))'   # desired= 0
# 2. No running benchmark EC2 instances:
aws ec2 describe-instances --region "$AWS_REGION" \
  --filters "Name=tag:Project,Values=clickbench-benchmark" "Name=instance-state-name,Values=running,pending" \
  --query 'Reservations[].Instances[].InstanceId' --output text     # expect EMPTY
# 3. No lingering benchmark EBS volumes (the 70Gi broker gp3 must be gone):
aws ec2 describe-volumes --region "$AWS_REGION" \
  --filters "Name=tag:Project,Values=clickbench-benchmark" "Name=status,Values=available,in-use" \
  --query 'Volumes[].{id:VolumeId,gb:Size,state:State}' --output text  # expect EMPTY (or only non-benchmark)
# 4. Namespace clear:
kubectl -n "$K8S_NAMESPACE" get kafka,kafkatopic,kafkaconnect,kafkaconnector,pvc
```
If (3) shows a stray 70Gi `available` volume, `scale-down.sh`'s PVC wait timed
out — delete it manually:
`aws ec2 delete-volume --region $AWS_REGION --volume-id <vol-...>`.

**What INTENTIONALLY persists (do not delete):**
- **EKS control plane** — ~$0.10/hr (~$73/mo). Deliberate: keeps run spin-up to
  2-5 min. Only `teardown.sh` removes it (do NOT run it here).
- **ECR images** (build-handle tags `connect-bench:head-<sha>`, `:pinned-v1.3.10`,
  `producer-bench:latest` — and the immutable digests they resolve to, which are
  the refs the pair actually deployed). Storage cost estimate: the two arm images are Strimzi
  Connect base (~500-700 MB compressed each) + producer (~200-400 MB) ≈ **~1.5-2 GB**
  total → ECR storage $0.10/GB-mo ≈ **~$0.15-0.20/mo**. Negligible; keep for the
  next pair.
- **S3 exports** under `s3://shimons/{runs,metrics,ch_inserts}/` — the acceptance
  evidence. Keep.
- Staged 10M parquet under `s3://shimons/clickbench-kafka-bench/hits-10m/` (~1.2 GB,
  ~$0.03/mo) — keep for repeat validation runs.

**DO-NOT list:**
- **Do NOT enable the nightly cron.** `benchmark-nightly.yml` has a
  `schedule: cron: '15 4 * * *'` — leave the workflow disabled / do not merge to a
  branch that would activate it. This is a one-off manual validation pair.
- **Do NOT run `teardown.sh`** (it deletes the control plane; a re-provision is a
  full ~15 min cluster create).
- **Do NOT push** the `benchmark-v2` branch or any local commits.

---

## FAILURE DECISION TABLE

Signatures are the scripts' actual `die`/`warn`/exit messages. "Clean re-run" =
recreate topic + fresh Job (never let K8s retry the producer — `backoffLimit 0`).

| Phase | Failure signature (actual message / symptom) | Retryable in place? | Action |
|---|---|---|---|
| S1 ECR | `RepositoryAlreadyExistsException` | yes | ignore (the `|| echo exists` handles it) |
| S1 login | `no basic auth credentials` on push | yes | re-run `aws ecr get-login-password | docker login` (token ~12h) |
| S2 head build | `plugin zip not found under .../build/confluent/` | yes | ensure JDK+gradle; re-run `build-arm.sh --arm head` |
| S2 pinned | `pinned artifact sha256 mismatch: release=… downloaded=…` | no | STOP-AND-REPORT: PINNED_REF release digest changed — do not benchmark a drifted pinned jar |
| S2 avro | `Avro converter sha256 mismatch` | no | STOP-AND-REPORT: Hub archive changed; bump pin deliberately (not in a run) |
| S5 image validation | `ARM0_IMAGE='...:tag' is a MUTABLE TAG and could not be resolved to a digest` (run_pair.sh, before any phase) | fix env | you exported a `*_IMAGE` as a bare tag. Set it to the DIGEST (S2 `IMAGE_DIGEST=`), or `KAFKA_ALLOW_TAG=1`/`--allow-tag` for LOCAL hacking only. Deploying a tag is the stale-tag class the pin prevents |
| S3 scale-up | `timed out waiting for N Ready node(s)` | yes | check EC2 capacity / node role; re-run `scale-up.sh` (idempotent) |
| S3 scale-up | `Kafka CR bench did not become Ready` (wait timeout) | maybe | `kubectl -n kafka-bench describe kafka bench`; if PVC/EBS CSI stuck, scale-down + up |
| S5 preload | `broker reports partition count 'X' for hits, expected 3` | needs clean re-run | delete topic, re-run pair (one-task-per-partition invariant broken) |
| S5 preload | `producer Job did not complete` / exit 2 `COUNT MISMATCH` / exit 3 `produced nothing` | needs clean re-run | check `kubectl logs job/hits-producer`; exit 3 ⇒ wrong PARQUET_SOURCE / IRSA denied; recreate topic + fresh Job |
| S5 preload | `could not parse rows_expected from producer summary` | needs clean re-run | producer crashed before summary; inspect logs |
| S5 poller host | `poller pod lacks confluent-kafka/requests and pip install failed` | maybe | node egress to PyPI blocked; producer image should already have them — rebuild producer image |
| S5 connect | `KafkaConnect did not become Ready` (600s) | maybe | `kubectl describe kafkaconnect bench-connect`; image pull error ⇒ check ECR node policy (S1); then clean re-run |
| S5 connector | `connector/tasks did not all reach RUNNING in time` | needs clean re-run | usually CH auth (wrong `bench-ch-creds`) or ClassNotFound (converter) — check task trace; `fail_run` already rolled back |
| S5 poller | `poller TIMED OUT — lag never reached 0 (ingest did not complete)` then `INGEST FAILED … outcome=failed` | continues (failed-class) | contract §1.3 outcome amendment: the run is FULLY captured + exported with `runtime['outcome']='failed'` (+ guards incl. `drain_incomplete`, and `integrity_unverified` on t1); the PAIR CONTINUES and `run_pair.sh` exits non-zero at the very end. If lag never moves at all, STOP-AND-REPORT (sink stalled) |
| S5 poller | `poller sample failed hard` / `no samples were retrieved for <run_id>` then `INGEST FAILED … outcome=failed` | continues (failed-class) | failed-class capture: CH-side capture still runs over the window; row lands with `outcome='failed'` (client-side metrics absent when no samples); pair continues; exits non-zero at end |
| S5 finalize | `poller finalize/insert failed` then `INGEST FAILED … (finalize_failed)` | continues (failed-class) | a run that cannot be fully measured never lands as success: row lands `outcome='failed'` (CH-side capture present, client-side absent); pair continues; exits non-zero at end |
| S5 truncate | `tier1 truncate failed` (fail_run) | needs clean re-run | target unreachable on 9440/`remoteSecure`; rolled back |
| S5 capture | `capture <NN> failed -> rollback` then `capture aborted` (strict mode). On a FAILED-CLASS run the message is `capture <NN> failed on a failed-class run — continuing (partial evidence beats none)` and NO rollback fires | strict: needs clean re-run; failed-class: continues | a capture SQL erred (grants/query_log filter). Strict path: rows rolled back; fix grants then re-run pair. Failed-class path: partial evidence lands under `outcome='failed'` |
| S5 capture 20 | `capture 20 (integrity) computation failed -> FLAG integrity_unverified` | continues | run FLAGGED, not failed; row lands; investigate SOURCE_UNIQUE/count constants after |
| S5 capture 20 | stderr `WARNING: no INPUT_PARQUET_GLOB and SOURCE_ROWS_EXPECTED / SOURCE_UNIQUE_EXPECTED are not both set — integrity source ground truth is 0/0 and the check will FAIL.` then later `TIER 1 INTEGRITY MISMATCH` with `rows_expected=0`/`unique_expected=0` in perf.metrics | needs clean pair re-run | NOT a data-loss mismatch — `SOURCE_UNIQUE_EXPECTED` was unset (READ-FIRST #3). SQL 20 succeeded with 0/0 ground truth, so no rollback fired and the false verdict is exported. Purge the pair's rows (S6 rollback loop), set `SOURCE_UNIQUE_EXPECTED=10000000` in the env, re-run the pair |
| S5 integrity | `TIER 1 INTEGRITY MISMATCH … run FAILS` | run failed by design | evidence already exported; STOP-AND-REPORT (real data loss/dupes) — do not retry blindly |
| S5 run record | `insert_run_record failed -> rollback` | needs clean re-run | metrics landing down / GIT_SHA empty (provenance.json unreadable); rolled back. On a failed-class run this rolls back that run only and the PAIR CONTINUES (`failed-class run … could not be recorded`) |
| S5 export | `DWH export failed (metrics persisted; export can be retried)` (WARN) | retry export only | fix `DWH_ROLE_ARN`; re-run `RUN_ID=<run> python3 export_metrics_to_dwh.py` per run (no full re-run) |
| S5 pair cost | `run_cost_usd emit failed (retryable; run rows remain valid)` (WARN) | retry emit only | re-run `emit_run_cost.py --run-id <first-arm-t1> --pair-start <PAIR_RUN_START> --nodes 2 ...` |
| S7 scale-down | `node group did NOT reach desired 0 (desired='X')` (die, exit non-zero) | must fix | STOP-AND-REPORT cost leak; inspect EC2/ASG; re-run scale-down until desired=0 |
| S7 scale-down | `TIMEOUT: N PVC(s) still present` (WARN) | proceeds | 70Gi EBS may linger; delete the stray volume manually (S7 gate 3) |

---

## SELF-VERIFICATION (grounding evidence)

**`bash -n` on assembled snippets** (S0 export block, S1, S5 arm-order block,
S5 stage loop, S6 helper) — all parse (`bash -n` clean). Interactive commands
containing placeholders were checked for shell-syntax validity, not executed.

**AWS `--no-sign-request` probes actually run against the public bucket:**
- `s3://clickhouse-public-datasets/clickbench/` → **empty** (the path in some
  docs); real data is under `hits_compatible/`.
- `s3://clickhouse-public-datasets/hits_compatible/` → `hits.parquet` (14.8 GB),
  `hits.csv.gz`, `hits.tsv.gz`, `hits.json.gz`, and `athena/`,
  `athena_partitioned/`, `native/`, `bench_test/` prefixes.
- `athena_partitioned/` → **100** files `hits_0.parquet` … `hits_99.parquet`.
- Bucket region header: `x-amz-bucket-region: eu-central-1` (NOT us-east-1).
- pyarrow row counts (anonymous S3, region eu-central-1): `hits_0/1/2/50 =
  1,000,000` each, `hits_99 = 997,497`, `hits.parquet = ~99,997,497`. ⇒ 10 files =
  exactly 10,000,000 rows.
- clickhouse-local 25.12.1.649 live queries (read-only, NOSIGN, no mutation):
  `SELECT count(), uniqExact(WatchID) FROM s3('…athena_partitioned/hits_0.parquet', NOSIGN, 'Parquet')`
  → `1000000 1000000`; same over `hits_{0..9}.parquet` → `10000000 10000000`.
  Grounds `SOURCE_UNIQUE_EXPECTED=10000000` for the exact staged subset and the
  S5 computation command's query shape.

**Grounding greps run against the scripts:**
- `run_pair.sh` — env reads: `ARM0_IMAGE`/`ARM1_IMAGE`/`PRODUCER_IMAGE`/
  `PARQUET_SOURCE` (`:?` required), `TARGET_CH_*`/`METRICS_CH_*`, `ARM_ORDER_SPEC`
  (`resolve_arm_order`), `COMPUTE_REGION` default us-east-2, `CFG_*` defaults,
  `SCALE_UP_NODES`, `PRODUCER_TIMEOUT`. Capture loop `for f in 11 12 13 14 15 16
  17 18 19 20 22 23` with `20` skipped on tier 0 and `23` skipped on tier ≠ 0
  (lines 559-565) — confirms `ch_insert_cpu_share_tier0` lands on tier-0 rows
  ONLY. Trap `trap cleanup_trap EXIT INT TERM` (line 780); `cleanup_trap` deletes
  CRs+secret+groups+poller+topic and scales down.
- `render_producer_job.py` — args `--job --image --service-account
  --parquet-source --out`; patches only image/SA/PARQUET_SOURCE. **No ROW_LIMIT.**
- `producer.py` — `--limit`/`ROW_LIMIT` exist but the orchestrator never sets
  them; `ds.dataset(source, format="parquet")` accepts dir or file.
- `build-arm.sh` — flags `--arm`/`--image-tag`/`--push`/`--digest-out`/
  `--no-docker`; docker context `"${SCRIPT_DIR}"` (docker/ dir). `PINNED_REF` =
  `v1.3.10`. `--push` resolves the registry digest (docker inspect
  `.RepoDigests`) and emits `IMAGE_DIGEST=<repo>@sha256:...` — deploy by DIGEST,
  never the tag (stale-tag class fix). run_pair.sh re-validates and rejects a
  bare tag unless `KAFKA_ALLOW_TAG=1`/`--allow-tag`.
- `producer/README.md` + `producer/Dockerfile` — build context `benchmarks/e2e/`.
- `export_metrics_to_dwh.py` — `TABLES=["runs","metrics","ch_inserts"]`, key
  `f"{table}/{tag}.parquet"`, `tag = run_id`; requires `DWH_BUCKET`+`DWH_ROLE_ARN`;
  **no `perf-export/` / `clickbench-kafka-bench/` prefix.**
- `apply_secret_and_metrics()` (run_pair.sh) — creates `bench-ch-creds`
  (keys `hostname/username/password`) AND applies
  `templates/connect-metrics-configmap.yaml` (answers "who applies the ConfigMap":
  run_pair.sh, per arm — not manual).
- `insert_run_record.py` — reads `TARGET_CH_*` for `version()`, `METRICS_CH_*`
  for the INSERT; mandatory `TARGET_REGION`/`ENVIRONMENT_CLASS` from config.env.
- `run_metrics_sql.py resolve_expected()` (lines 48-102) — constants path
  requires BOTH `SOURCE_ROWS_EXPECTED` and `SOURCE_UNIQUE_EXPECTED`; with no
  `INPUT_PARQUET_GLOB` and either constant missing it returns `0.0, 0.0` for
  both (SQL 20 succeeds with 0/0 → false `integrity_ok=0`); the glob fallback
  query is `s3({glob:String}, NOSIGN, 'Parquet')` (unauthenticated). `run_pair.sh`
  line 323 exports `SOURCE_ROWS_EXPECTED` only — nothing in run_pair.sh /
  benchmark-nightly.yml / config.env sets `SOURCE_UNIQUE_EXPECTED` (grep-verified).
- `kafkaconnector.json.tmpl` — connector uses `port 8443 ssl true` (HTTPS);
  `run_metrics_sql.py`/`23_*.sql` use `remoteSecure(host:9440,...)` (native TLS)
  — two different but both-correct target ports.
- `cluster.yaml` — `managedNodeGroups: bench-ng`, `iam.withAddonPolicies.ebs:true`
  (eksctl attaches `AmazonEC2ContainerRegistryReadOnly` to managed node roles by
  default → ECR pull works).
- `delete_consumer_group.sh` / partition assert — broker pod hardcoded
  `bench-combined-0` (Strimzi node-pool naming; confirm on live cluster).

**Known live-first-time risks inherited from the components** (not blockers, but
watch): Strimzi 0.46 pod labels + `bench-connect-connect-api` DNS,
`bench-combined-0` broker pod name, the JMX `-rate` rule output name, and the
producer IRSA role (S5 prereq). The cadvisor CPU source for
`connect_cpu_seconds_per_Mrows` / `kafka_worker_cpu_share_t0` is now **wired**
(no longer a gap): `bench-poller-sa` (`infra/poller-rbac.yaml`, `nodes/proxy get`,
applied by `scale-up.sh`) lets the poller pod scrape the kubelet cadvisor
endpoint through the API-server node proxy — the tier-0 CPU gate is **sighted**.
First-live-run watch: confirm the `nodes/proxy` grant resolves and the proxy URL
returns cadvisor text (a 403 or empty body degrades the gate to UNAVAILABLE, not
a run failure).
