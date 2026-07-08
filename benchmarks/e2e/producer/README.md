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

## The `rows_expected` contract (why offsets, not send counts)

`rows_expected` is derived from the topic's **committed end offsets** after the
producer flushes:

```
rows_expected = Σ_partitions (high_watermark − low_watermark)
```

read via the consumer API — **never** from producer-side send counts. The
producer also emits `rows_sent` (delivery-callback confirmations). If the two
disagree the Job **fails** (exit 2): idempotence should make them equal, so a
mismatch means the pre-load is invalid and must not feed a benchmark run. The
orchestrator (task 31) parses `rows_expected` from the JSON summary and uses it
as the integrity target for the Tier 1 drain (`count()` / `uniqExact(WatchID)`).

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
`PARQUET_BATCH_ROWS`, `ROW_LIMIT`). `--create-topic` is for standalone/local
runs only; in the benchmark the orchestrator creates the fresh topic per run
(decision 6) and the Job leaves `CREATE_TOPIC=false`.

The last stdout line is always the machine-readable JSON summary:

```json
{"topic":"hits","partitions":3,"rows_sent":300,"rows_expected":300,
 "per_partition":{"0":{"low":0,"high":7,"count":7}, ...},
 "duration_seconds":1.0,"rate_rows_per_sec":299.1,
 "idempotence":true,"acks":"all","parquet_source":"...","match":true}
```

Exit codes: `0` OK · `2` count mismatch (invalid pre-load) · `3` produced
nothing · `1` any other failure.

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

[`job.yaml`](job.yaml) is a `batch/v1` Job in namespace `kafka-bench`:

- **`backoffLimit: 0` + `restartPolicy: Never`** — a retried, half-complete
  pre-load would corrupt `rows_expected` (a second invocation re-produces the
  whole dataset on top of the first, doubling end offsets; idempotence dedups
  *records within one run*, not *whole re-runs*). The orchestrator handles retry
  by recreating the topic and creating a fresh Job — never by letting K8s
  restart this one.
- Env-driven config; `image:` is a placeholder replaced by CI with the ECR ref.
- Resources sized for an m6i.large node; streaming parquet keeps memory flat.

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

## Deferred

`produce_ts` / the rate-controlled latency profile (plan Appendix A) is **not**
implemented here — this producer runs at full speed for the backlog-drain model.
