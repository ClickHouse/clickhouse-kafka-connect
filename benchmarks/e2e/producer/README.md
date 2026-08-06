# Benchmark v2 producer — parquet → Avro → topic

Pre-loads the Kafka topic with the ClickBench `hits` dataset before a measured
drain (plan [`docs/benchmark-v2-plan.md`](../../../docs/benchmark-v2-plan.md)
§5 step 2, decision 6). It reads the `hits` parquet, maps every one of the 105
columns to the Avro types in [`../schema/hits.avsc`](../schema/hits.avsc),
publishes each row to the topic using the **Confluent Schema Registry wire
format** (magic byte + 4-byte schema id) so the sink's Confluent
`AvroConverter` can decode it, and then reports an authoritative
`rows_expected`.

Production finishes entirely before any measured drain begins — producer speed
only costs wall-clock, never accuracy (plan decision 6).

## Language / design choice

**Python** (`confluent-kafka` + `pyarrow`), containerised. Rationale:

- `confluent-kafka` bundles librdkafka, whose idempotent producer
  (`enable.idempotence=true`) and Confluent `AvroSerializer` produce **exactly**
  the wire format the sink's `AvroConverter` expects — the deciding criterion.
- `pyarrow` streams the parquet in bounded row batches, so a ~100M-row file
  never materialises in memory.
- The whole thing is verifiable locally end-to-end (see below) with a throwaway
  KRaft Kafka + Schema Registry and a synthetic parquet — no 70 GB download and
  no JVM build coupling to the connector's gradle build.

Deps are pinned in [`requirements.txt`](requirements.txt).

## Parallel sharded preload (why it is fast, and safe)

The single-producer preload took ~20 min for the 10M-row dataset. To cut that to
~5-6 min the producer runs as a Kubernetes **Indexed Job** of `SHARD_COUNT` pods
(default 3, `completions == parallelism == SHARD_COUNT`). Each pod:

1. reads its `JOB_COMPLETION_INDEX` (K8s-injected, `0..SHARD_COUNT-1`) and
   `SHARD_COUNT`;
2. lists the parquet files under `PARQUET_SOURCE` and **stable-sorts** them
   lexicographically (every pod sees the identical ordering);
3. produces **only** the files at `i % SHARD_COUNT == index` (stride-N).

The file partition is therefore **disjoint and complete** — each file is
produced by exactly one pod, no file twice. `SHARD_COUNT=1` degenerates to the
original single-producer behaviour. If `files < SHARD_COUNT` the high-index pods
get an **empty shard**: they produce nothing and exit `0` with a summary
`{"empty_shard": true, "rows_sent": 0}` (an empty shard is not a failure). The
canonical single `hits.parquet` gives one non-empty shard and N-1 empty shards,
so **partitioned sources are required for real parallelism** (the staged
`us-east-2` copy should be multi-file).

Every pod is otherwise identical — own idempotent producer, same
schema/serializer, same **bounded memory** (each pod streams only its shard, so
the `1×1` pyarrow readahead + 128 MiB rdkafka-queue caps from the OOM fix are
unchanged and now apply per-pod). The preload is **unmeasured**, so this
parallelism never touches the instrument.

**Produce-rate / broker expectation.** N pods produce ~N× the aggregate rate
against one RF1 topic. RF1 (no replication traffic) absorbs a 3-4× produce burst
on the bench broker trivially — no topic or broker change is needed. Backpressure
is per-pod (`BufferError` → poll-and-retry the same row), so a pod never outruns
the broker; a slow broker simply flattens each pod's throughput without breaking
correctness.

## The `rows_expected` contract (why offsets, not send counts)

`rows_expected` is the **authoritative** count of the pre-loaded topic. Under
sharding it is derived by the **orchestrator** (`run_pair.sh phase_preload`)
from the **broker's committed offsets** *after the whole Indexed Job completes*:

```
rows_expected = Σ_partitions (end_offset − beginning_offset)
```

read via `kafka-get-offsets.sh` inside the broker pod (`--time -1` latest,
`--time -2` earliest) — **never** from producer-side send counts. This is
**producer-count-agnostic**: it is the property that keeps preload *speed*
irrelevant to *accuracy* (the whole point of the offsets contract).

Under sharding the **per-pod global-offsets check is removed**: each pod sends
`1/N` of the data, but the topic end offsets reflect the global total, so a
per-pod compare against them is nonsense. Each pod instead reports **its own
shard's** `rows_sent` (delivery-callback count) in its summary, with shard
fields (`shard_index`, `shard_count`, `files_in_shard`). The per-pod sanity is
**shard-local**: a non-empty shard that produced 0 rows fails (exit `3`).

After the Job completes, the orchestrator does a best-effort **cross-check**: it
sums the N pods' `rows_sent` (parsing each *completion-index* pod's last JSON
line — the interleaved whole-Job log cannot be used) and:

- Σ `rows_sent` **== broker offsets** → OK;
- a clean sum that **disagrees** with broker offsets → **FAIL** the preload
  (duplicate or lost records — the pre-load is invalid);
- a log **parse problem** → WARN only (broker offsets remain authoritative).

`rows_expected` is the integrity target for the Tier 1 drain (`count()` /
`uniqExact(WatchID)`).

## Idempotence rationale

`enable.idempotence=true` (⇒ `acks=all`) is set on the producer. Without it,
producer retries write **duplicate records into the topic**, and the sink would
be blamed for duplicates it never created. Idempotence guarantees each record
lands exactly once in the topic, so the committed end offsets are a clean count
of the dataset — the precondition for the whole backlog-drain measurement.

## Type mapping (105 columns)

Driven by `hits.avsc` (single source of truth; see
[`../schema/README.md`](../schema/README.md)):

| CH type | Avro | Producer handling |
|---|---|---|
| `Int64` ×6 | `long` | pass-through |
| `Int32` ×19 | `int` | pass-through |
| `Int16` ×48 | `int` + `connect.type=int16` | int, **asserted within signed 16-bit range** (a wider parquet value fails here, not at 200k rows/s in the sink's `(Short)` cast) |
| `String` ×28 | `string` | **columnar bytes→utf8 decode** — the real hits parquet stores strings as BINARY, so pyarrow yields `bytes` (fastavro's `write_utf8` requires `str`). Decoded per batch at the Arrow level: `pc.cast` fast path (validates UTF-8), python `errors='replace'` fallback for the invalid UTF-8 real hits contains. `replace` is deterministic, so `rows_expected`/`uniqExact(WatchID)` semantics are untouched. Native string/large_string columns pass through. |
| `DateTime` ×3 (`EventTime`, `ClientEventTime`, `LocalEventTime`) | bare `long` | **epoch seconds** — a parquet `timestamp` is converted to int seconds; an already-int value passes through. NOT `timestamp-micros`. |
| `Date` ×1 (`EventDate`) | `int` + `logicalType=date` | `datetime.date` — the `AvroSerializer` encodes it as days-since-epoch; an already-int (days) value is accepted too |

All 105 fields are **non-nullable**: a `None` in any column fails the run loudly
with the column name (columnar `null_count` guard, O(1) per column) — never a
silent coercion. A null means the parquet source is not the expected dataset.

Records are keyed `null` → librdkafka sticky/round-robin partitioning across the
run; the per-partition split is incidental (the offset **sum** is the contract).

Mapping regression test (BINARY columns + invalid UTF-8 + null guard, fixture
generated at test time — no real hits data checked in):

```bash
./venv/bin/python test_mapping.py
```

## Run it locally (any Kafka + registry)

```bash
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt

./venv/bin/python producer.py \
  --bootstrap localhost:9092 \
  --registry-url http://localhost:8081 \
  --topic hits --partitions 3 --create-topic \
  --parquet-source /path/to/hits.parquet
```

All flags also read from env (`BOOTSTRAP`, `REGISTRY_URL`, `TOPIC`,
`PARTITIONS`, `PARQUET_SOURCE`, `CREATE_TOPIC`, `SCHEMA_PATH`,
`PARQUET_BATCH_ROWS`, `ROW_LIMIT`). Sharding is env-only: `SHARD_COUNT` (default
`1`) and `JOB_COMPLETION_INDEX` (default `0`, K8s-injected in-cluster). To
exercise 3-way sharding locally, run three processes against one topic:

```bash
for i in 0 1 2; do
  JOB_COMPLETION_INDEX=$i SHARD_COUNT=3 ./venv/bin/python producer.py \
    --bootstrap localhost:9092 --registry-url http://localhost:8081 \
    --topic hits --partitions 3 --parquet-source /path/to/hits_dir/ &
done; wait
```

`--create-topic` is for standalone/local runs only; in the benchmark the
orchestrator creates the fresh topic per run (decision 6) and the Job leaves
`CREATE_TOPIC=false`.

The last stdout line of **each pod** is a machine-readable JSON summary scoped to
that pod's shard:

```json
{"topic":"hits","shard_index":0,"shard_count":3,"files_in_shard":4,
 "rows_sent":100,"duration_seconds":1.0,"rate_rows_per_sec":99.1,
 "idempotence":true,"acks":"all","parquet_source":"...","empty_shard":false}
```

An empty shard emits `"empty_shard":true,"rows_sent":0` and exits `0`. The
authoritative global `rows_expected` is **not** in the pod summary — the
orchestrator derives it from broker offsets after the Job completes (see the
`rows_expected` contract above).

Per-pod exit codes: `0` OK (incl. an empty shard) · `3` a non-empty shard that
produced zero rows · `1` any other failure (send/delivery error, bad input).

## Build the image

Build context is **`benchmarks/e2e/`** (one level up) because the Avro schema
lives in `../schema/`:

```bash
docker build -f benchmarks/e2e/producer/Dockerfile \
             -t kc-benchmark-producer:latest \
             benchmarks/e2e
```

**Deploy by DIGEST, not the tag (stale-tag class fix).** The orchestrator
(`run_pair.sh`) validates `PRODUCER_IMAGE` and **rejects a mutable tag by
default** — a reused tag (`:latest`) can be served stale by node/registry
caches, which once made a benchmark pair run the wrong image twice. After
pushing, resolve and export the digest:

```bash
docker push <registry>/producer-bench:latest
PRODUCER_IMAGE="$(docker inspect --format='{{index .RepoDigests 0}}' <registry>/producer-bench:latest)"
export PRODUCER_IMAGE       # repo@sha256:...  (KAFKA_ALLOW_TAG=1 bypasses, local only)
```

## How the Job is invoked per run

[`job.yaml`](job.yaml) is a `batch/v1` **Indexed** Job in namespace
`kafka-bench`:

- **`completionMode: Indexed`, `completions == parallelism == SHARD_COUNT`** —
  `SHARD_COUNT` pods run in parallel, each with a distinct `JOB_COMPLETION_INDEX`
  producing its disjoint stride-N slice. `render_producer_job.py` patches all
  three from a single `--shard-count` (default 3) and refuses to render a
  non-indexed job (which would give every pod the whole dataset).
- **`backoffLimit: 0` + `restartPolicy: Never`** — **any** pod failure is
  terminal (`Job Failed`). A retried, half-complete pre-load would corrupt the
  offset-derived count (a re-run re-produces a shard on top of the first,
  inflating end offsets; idempotence dedups *records within one pod*, not *whole
  re-runs*). The orchestrator handles retry by recreating the topic and creating
  a fresh Job — never by letting K8s restart pods.
- Env-driven config; `image:` is a placeholder replaced by CI with the ECR ref.
- Resources sized for an m6i.large node; streaming parquet keeps memory flat
  per-pod (each pod streams only its shard).

Per nightly run (plan §5): scale up → **producer Job runs to completion,
`rows_expected` recorded** → per-arm drains → capture → teardown.

## Parquet staging (TBD)

`PARQUET_SOURCE` defaults to the placeholder
`s3://TBD-us-east-2-staging/clickbench/hits/` (overseer directive 3). The
staging decision is **pending**; two options:

1. **Stage a copy in `us-east-2`** (same region as the EKS cluster and the
   dedicated Cloud target). Avoids repeated cross-region egress on every nightly
   run — the recommended option; a one-time copy amortised over every run.
2. **Read the public ClickBench bucket in `us-east-1` directly**
   (`s3://clickhouse-public-datasets/hits_compatible/...`). Zero staging setup,
   but each nightly run pays cross-region (`us-east-1` → `us-east-2`) egress —
   a known recurring cost issue, which is why option 1 is preferred.

The connector's CI / orchestrator sets `PARQUET_SOURCE` (and `AWS_REGION`)
explicitly once the staging location is chosen; until then the default is an
obviously-invalid placeholder so a mis-wired run fails loudly rather than
reading the wrong data.

## Memory boundedness & OOM diagnosis

The producer streams the parquet in bounded batches, but "bounded" only holds if
**every** buffer between the parquet read side and the Kafka send side is capped.
The 2026-07-07 and 2026-07-12 (pair-3) OOMKills proved two of them were not.

### The OOMKill signature (read this first when a run dies)

An **abrupt stop right after a normal progress line, with NO Python traceback and
NO summary JSON**, is an OOMKill (the kernel `SIGKILL`s the process; Python never
runs an except/finally, so there is nothing to log). In Kubernetes the pod shows
`reason: OOMKilled`, container `exitCode: 137`. Pair-3 died exactly this way at
`2,545,486 / 10,000,000` rows.

To make this diagnosable **from the Job log alone**:

- Every progress line now carries `peak_rss=<N> MiB` (process peak RSS via
  `getrusage`). If those numbers climb toward the container limit and then the
  log stops mid-run → OOMKill.
- A best-effort **FINAL breadcrumb** line is emitted on `atexit` and on `SIGTERM`
  (`[producer] FINAL breadcrumb (...): rows_produced_so_far=... peak_rss=... MiB`).
  Note an OOMKill is `SIGKILL`, which **cannot** be trapped — so a *missing*
  breadcrumb after a climbing-RSS progress line is itself the OOMKill tell. The
  breadcrumb catches the softer cases (graceful eviction / `activeDeadlineSeconds`
  → `SIGTERM`).

### The three growth paths (all now explicitly bounded)

1. **pyarrow scanner readahead — the dominant path.** `ds.to_batches()` prefetches
   in *background threads*: defaults `batch_readahead=16`, `fragment_readahead=4`
   (up to 64 decoded batches held ahead of the consumer). When the send side
   stalls — pair-3's librdkafka logged `Failed to acquire idempotence PID ...:
   Coordinator load in progress: retrying` right as the read side started pumping —
   the row loop blocks but the scanner keeps decoding whole row-groups (the staged
   hits files are ~1M rows/row-group) into RAM. Measured on a 105-col hits-shaped
   parquet, a 2 s stall after the first batch: **RSS 5,240 MiB at the defaults vs
   2,069 MiB at `1×1` — a 3,170 MiB (60%) swing.** Now pinned to
   `batch_readahead=1, fragment_readahead=1`.
2. **librdkafka C-side queue.** `queue.buffering.max.messages/kbytes` was set 10×
   the librdkafka default (1,000,000 msgs / 1 GiB). Under a stall the queue fills
   to the cap and stays there — a ~1 GiB reservation stacked on (1). Now
   `100,000 msgs / 128 MiB` (~librdkafka default order of magnitude). `produce()`
   raises `BufferError` at the cap; the loop treats that as backpressure (poll,
   **retry the same row — the parquet iterator does not advance**), so a smaller
   queue costs throughput under a stall, never correctness.
3. **Per-batch `to_pylist()`.** Materialises one batch of 105-col dicts at once
   (bounded by `PARQUET_BATCH_ROWS`); the Arrow batch and the dict list are now
   `del`-ed promptly so they don't straddle the next batch's allocation.

### Post-fix peak vs limits

Local full-pipeline run (1.5M rows, real Kafka, fast broker): **peak_rss ≈ 4.4 GiB**
high-water (`getrusage` maxrss). The stall-driven readahead spike — the thing that
actually OOMed pair-3 — drops from ~5.2 GiB to ~2.1 GiB. With that path bounded, a
full run's peak is dominated by one ~1M-row row-group decode plus the 128 MiB queue.
That **~4.4 GiB peak is ABOVE the `4Gi` request but well under the `7Gi` limit** —
which is exactly the intended shape. The `memory` **request is scheduling-only**: it
reserves capacity for the scheduler and does NOT cap usage, so a peak above it is
expected and harmless as long as it stays under the **limit**, which is the value the
kernel OOMKills against. The `7Gi` limit (bumped from 6Gi as a stopgap on 2026-07-12)
is now **documented headroom** over that ~4.4 GiB peak, not a workaround — it can be
revisited down to ~6Gi once a full stall-under-load run confirms the flattened peak
in-cluster.

## Deferred

`produce_ts` / the rate-controlled latency profile (plan Appendix A) is **not**
implemented here — this producer runs at full speed for the backlog-drain model.
