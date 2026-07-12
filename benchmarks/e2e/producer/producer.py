#!/usr/bin/env python3
"""Benchmark v2 producer — ClickBench hits parquet -> Avro -> Kafka topic.

Reads the ClickBench ``hits`` parquet (canonical single file or partitioned
variants), maps every one of the 105 columns to the Avro types declared in
``benchmarks/e2e/schema/hits.avsc``, and publishes each row to a Kafka topic
using the Confluent Schema Registry wire format (magic byte + 4-byte schema id)
so the sink's Confluent ``AvroConverter`` can decode it.

Backlog-drain precondition (plan §5 step 2, decision 6): this Job runs to
completion BEFORE any measured drain. Its single job is to leave the topic
pre-loaded with EXACTLY the dataset, and to report an authoritative
``rows_expected`` derived from committed topic END OFFSETS — never from
producer-side send counts (overseer directive 2).

Design decisions baked in (non-negotiable, see README):
  * enable.idempotence=true (=> acks=all): producer retries never write
    duplicate records into the topic. Without it the sink would be blamed for
    producer-side dupes (overseer directive 1).
  * rows_expected = sum over partitions of (end_offset - beginning_offset),
    read via the consumer API after flush. rows_sent (delivery-callback count)
    is emitted alongside; a mismatch FAILS the Job (the pre-load is invalid).
  * PARQUET_SOURCE is a parameter (overseer directive 3), defaulting to a
    us-east-2 staging placeholder (see README for the staging TBD note).

Latency profile / produce_ts is DEFERRED (plan Appendix A) — not implemented.

Output: a single machine-readable JSON object on stdout (the last stdout line
is always the summary; all human logging goes to stderr) for the orchestrator
(task 31) to parse. Non-zero exit on any failure or count mismatch.
"""

import argparse
import atexit
import datetime
import json
import os
import resource
import signal
import sys
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# --- constants ---------------------------------------------------------------

# Placeholder default for the parquet source (overseer directive 3). The real
# staging location in us-east-2 is a pending decision (cross-region egress from
# the us-east-1 public ClickBench bucket is a known cost issue). See README.
DEFAULT_PARQUET_SOURCE = "s3://TBD-us-east-2-staging/clickbench/hits/"

# CH DateTime is UInt32 seconds; these 3 columns are bare epoch-SECONDS longs.
DATETIME_COLS = ("EventTime", "ClientEventTime", "LocalEventTime")
# CH Date is UInt16 days-since-epoch; Avro logical "date".
DATE_COL = "EventDate"
_EPOCH = datetime.date(1970, 1, 1)

# The 48 CH Int16 columns (Avro int + connect.type=int16). Kept as an explicit
# guard set so an out-of-Short-range parquet value is caught here rather than at
# 200k rows/s in the sink's (Short) cast. Derived from hits.avsc at load time,
# but we also assert Short range on these while mapping.
_SHORT_MIN, _SHORT_MAX = -(2 ** 15), 2 ** 15 - 1


def log(msg):
    print(f"[producer] {msg}", file=sys.stderr, flush=True)


def die(msg, code=1):
    print(f"[producer:error] {msg}", file=sys.stderr, flush=True)
    sys.exit(code)


# --- memory diagnosability ---------------------------------------------------
# The 2026-07-12 pair-3 producer died ABRUPTLY at 2,545,486/10,000,000 rows with
# NO traceback — the classic OOMKill (exit 137) signature: a normal progress
# line, then silence. To make a future OOM diagnosable from the Job log ALONE,
# every progress line now carries an RSS number, and we emit a best-effort final
# line on SIGTERM/atexit. If you see a progress+RSS line climbing toward the
# container memory limit and then an abrupt stop with no summary JSON and no
# traceback, that is an OOMKill — raise the limit OR (preferably) find the new
# growth path; the buffers below are all explicitly bounded.

def rss_mb():
    """Resident set size in MiB. On Linux (the container) ru_maxrss is KiB."""
    ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes; Linux (where this runs) reports KiB.
    return (ru / (1024 * 1024)) if sys.platform == "darwin" else (ru / 1024)


_last_progress = {"read": 0}


def _final_breadcrumb(reason):
    # Best-effort: a single line so an OOMKill/SIGTERM leaves a diagnosable
    # trailer in the Job log instead of silence. Must not raise.
    try:
        print(f"[producer] FINAL breadcrumb ({reason}): "
              f"rows_produced_so_far={_last_progress['read']} "
              f"peak_rss={rss_mb():.0f} MiB", file=sys.stderr, flush=True)
    except Exception:
        pass


def _install_breadcrumbs():
    atexit.register(_final_breadcrumb, "atexit")

    def _on_sigterm(signum, frame):
        # OOMKilled pods normally get SIGKILL (no handler runs), but a graceful
        # eviction / deadline sends SIGTERM first — capture that case.
        _final_breadcrumb(f"signal {signum}")
        sys.exit(143)

    try:
        signal.signal(signal.SIGTERM, _on_sigterm)
    except (ValueError, OSError):
        pass  # not in main thread / unsupported platform


# --- schema handling ---------------------------------------------------------

def load_avro_schema(path):
    with open(path, "r") as f:
        schema_str = f.read()
    schema = json.loads(schema_str)
    fields = [f["name"] for f in schema["fields"]]
    if len(fields) != 105:
        die(f"hits.avsc must have 105 fields, found {len(fields)}")
    # Classify each field by its target CH width so mapping is data-driven and
    # stays in sync with the schema file (single source of truth).
    int16_fields = set()
    string_fields = set()
    for f in schema["fields"]:
        t = f["type"]
        if isinstance(t, dict) and t.get("connect.type") == "int16":
            int16_fields.add(f["name"])
        elif t == "string":
            string_fields.add(f["name"])
    return schema_str, fields, int16_fields, string_fields


# --- batch preparation (columnar) ---------------------------------------------

def prepare_batch(batch, string_fields):
    """Columnar per-batch normalisation, BEFORE the row loop.

    1. bytes -> str: the REAL ClickBench hits parquet stores its string columns
       as BINARY, so pyarrow yields ``bytes`` and fastavro's ``write_utf8``
       raises ``TypeError: must be string``. Decode at the Arrow level, not with
       per-field isinstance checks in the hot row loop:
         - fast path: ``pc.cast(col, string())`` — validates UTF-8, zero-copyish;
         - fallback (only if the batch contains invalid UTF-8, which real hits
           data does): a columnar python decode with ``errors='replace'``.
       ``errors='replace'`` is deterministic (same bytes -> same U+FFFD output
       every run), so rows_expected / uniqExact(WatchID) semantics are
       untouched (WatchID is numeric).
    2. Null guard: every hits.avsc field is NON-nullable. A None must fail
       LOUDLY with the column name — never silently coerce (e.g. to ""). The
       Arrow ``null_count`` is O(1) metadata per column.
    """
    cols = list(batch.columns)
    names = batch.schema.names
    changed = False
    for i, name in enumerate(names):
        col = cols[i]
        if col.null_count > 0:
            die(f"column {name!r} contains {col.null_count} NULL value(s) in a "
                f"batch; hits.avsc fields are non-nullable — refusing to "
                f"silently coerce. The parquet source is not the expected "
                f"hits dataset.")
        if name in string_fields and (
                pa.types.is_binary(col.type)
                or pa.types.is_large_binary(col.type)
                or pa.types.is_fixed_size_binary(col.type)):
            try:
                # Fast path: Arrow-level cast validates UTF-8.
                col = pc.cast(col, pa.string())
            except pa.lib.ArrowInvalid:
                # Invalid UTF-8 in this batch/column: deterministic replace.
                col = pa.array(
                    (b.decode("utf-8", "replace") for b in col.to_pylist()),
                    type=pa.string())
            cols[i] = col
            changed = True
    if changed:
        batch = pa.RecordBatch.from_arrays(cols, names=names)
    return batch


# --- row mapping -------------------------------------------------------------

def make_row_mapper(field_order, int16_fields):
    """Return f(dict_from_parquet) -> dict conforming to hits.avsc.

    * DateTime cols -> bare epoch seconds (int). Accepts either an already-int
      epoch-seconds value or a datetime/Timestamp (converted to epoch seconds).
    * EventDate -> datetime.date (confluent AvroSerializer encodes the "date"
      logicalType as days-since-epoch int). Accepts an int (already days) too.
    * Int16 cols -> int, asserted within signed 16-bit range.
    * Everything else passes through (int/long/string) as-is.
    """
    datetime_set = set(DATETIME_COLS)

    def to_epoch_seconds(v):
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int,)):
            return v
        if isinstance(v, datetime.datetime):
            # Treat naive datetimes as UTC (ClickBench parquet stores UTC).
            if v.tzinfo is None:
                v = v.replace(tzinfo=datetime.timezone.utc)
            return int(v.timestamp())
        if isinstance(v, datetime.date):
            return int(datetime.datetime(v.year, v.month, v.day,
                                         tzinfo=datetime.timezone.utc).timestamp())
        # numpy / pyarrow scalars -> int
        return int(v)

    def to_date(v):
        if isinstance(v, datetime.datetime):
            return v.date()
        if isinstance(v, datetime.date):
            return v
        if isinstance(v, (int,)):
            return _EPOCH + datetime.timedelta(days=int(v))
        return _EPOCH + datetime.timedelta(days=int(v))

    def mapper(row):
        out = {}
        for name in field_order:
            v = row[name]
            if name in datetime_set:
                out[name] = to_epoch_seconds(v)
            elif name == DATE_COL:
                out[name] = to_date(v)
            elif name in int16_fields:
                iv = int(v)
                if iv < _SHORT_MIN or iv > _SHORT_MAX:
                    raise ValueError(
                        f"column {name}={iv} out of Int16 range "
                        f"[{_SHORT_MIN},{_SHORT_MAX}] (would fail the sink's "
                        f"(Short) cast)")
                out[name] = iv
            elif isinstance(v, datetime.datetime):
                # Any stray timestamp on a non-datetime column: normalise to int
                # seconds so Avro long encoding never sees a datetime.
                out[name] = to_epoch_seconds(v)
            else:
                out[name] = v
        return out

    return mapper


# --- offset accounting -------------------------------------------------------

def committed_offsets(bootstrap, topic, timeout=30.0):
    """Return (rows_expected, per_partition) from committed topic offsets.

    rows_expected = sum over partitions of (high_watermark - low_watermark),
    read via the consumer API. This is the authoritative pre-load count
    (overseer directive 2), independent of producer send counts.
    """
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"__producer_offset_probe_{int(time.time())}",
        "enable.auto.commit": False,
    })
    try:
        md = consumer.list_topics(topic, timeout=timeout)
        if topic not in md.topics or md.topics[topic].error is not None:
            die(f"topic {topic!r} not found when reading end offsets")
        parts = sorted(md.topics[topic].partitions.keys())
        per_partition = {}
        total = 0
        for p in parts:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, p), timeout=timeout, cached=False)
            count = high - low
            per_partition[p] = {"low": low, "high": high, "count": count}
            total += count
        return total, per_partition
    finally:
        consumer.close()


# --- schema registration -----------------------------------------------------

def ensure_subject(sr_client, subject, schema_str):
    """Register hits.avsc under <topic>-value if the subject is absent.

    The AvroSerializer auto-registers on first serialize, but doing it up front
    gives a clear failure if the registry is unreachable and lets us log the id.
    """
    from confluent_kafka.schema_registry import Schema
    try:
        existing = sr_client.get_latest_version(subject)
        log(f"schema registry: subject {subject!r} already present "
            f"(schema id {existing.schema_id}, version {existing.version})")
        return existing.schema_id
    except Exception:
        schema = Schema(schema_str, schema_type="AVRO")
        schema_id = sr_client.register_schema(subject, schema)
        log(f"schema registry: registered {subject!r} -> schema id {schema_id}")
        return schema_id


# --- main --------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="ClickBench hits parquet -> Avro -> Kafka")
    p.add_argument("--bootstrap", default=os.environ.get(
        "BOOTSTRAP", "bench-kafka-bootstrap.kafka-bench.svc:9092"))
    p.add_argument("--registry-url", default=os.environ.get(
        "REGISTRY_URL", "http://schema-registry.kafka-bench.svc:8081"))
    p.add_argument("--topic", default=os.environ.get("TOPIC", "hits"))
    p.add_argument("--partitions", type=int,
                   default=int(os.environ.get("PARTITIONS", "3")))
    p.add_argument("--parquet-source", default=os.environ.get(
        "PARQUET_SOURCE", DEFAULT_PARQUET_SOURCE))
    p.add_argument("--schema", default=os.environ.get(
        "SCHEMA_PATH",
        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     "..", "schema", "hits.avsc")))
    p.add_argument("--create-topic", action="store_true",
                   default=os.environ.get("CREATE_TOPIC", "false").lower() == "true",
                   help="Create the topic if absent (RF=1). The orchestrator "
                        "normally creates the topic; this is for local runs.")
    p.add_argument("--batch-size", type=int,
                   default=int(os.environ.get("PARQUET_BATCH_ROWS", "65536")),
                   help="Parquet read batch size (rows) — memory bound, not the "
                        "Kafka batch.")
    p.add_argument("--limit", type=int,
                   default=int(os.environ.get("ROW_LIMIT", "0")),
                   help="Stop after N rows (0 = all). For smoke tests only.")
    return p.parse_args()


def maybe_create_topic(bootstrap, topic, partitions):
    from confluent_kafka.admin import NewTopic
    admin = AdminClient({"bootstrap.servers": bootstrap})
    md = admin.list_topics(timeout=30)
    if topic in md.topics and md.topics[topic].error is None \
            and len(md.topics[topic].partitions) > 0:
        existing = len(md.topics[topic].partitions)
        if existing != partitions:
            die(f"topic {topic!r} exists with {existing} partitions, "
                f"expected {partitions}")
        log(f"topic {topic!r} already exists ({existing} partitions)")
        return
    fs = admin.create_topics([NewTopic(topic, num_partitions=partitions,
                                       replication_factor=1)])
    for t, f in fs.items():
        try:
            f.result()
            log(f"created topic {t!r} ({partitions} partitions, RF=1)")
        except Exception as e:
            die(f"failed to create topic {t!r}: {e}")


def main():
    _install_breadcrumbs()
    args = parse_args()
    schema_path = os.path.abspath(args.schema)
    if not os.path.exists(schema_path):
        die(f"schema not found: {schema_path}")

    log(f"bootstrap={args.bootstrap} registry={args.registry_url} "
        f"topic={args.topic} partitions={args.partitions}")
    log(f"parquet_source={args.parquet_source}")
    if args.parquet_source.startswith("s3://TBD"):
        log("WARNING: PARQUET_SOURCE is the staging placeholder — set it "
            "explicitly (see README, parquet staging TBD note).")

    schema_str, field_order, int16_fields, string_fields = \
        load_avro_schema(schema_path)
    log(f"loaded hits.avsc: 105 fields, {len(int16_fields)} Int16 columns, "
        f"{len(string_fields)} String columns")

    if args.create_topic:
        maybe_create_topic(args.bootstrap, args.topic, args.partitions)

    # Schema Registry + Avro serializer (Confluent wire format).
    sr_client = SchemaRegistryClient({"url": args.registry_url})
    subject = f"{args.topic}-value"
    ensure_subject(sr_client, subject, schema_str)
    avro_serializer = AvroSerializer(
        sr_client, schema_str,
        conf={"auto.register.schemas": True})
    ser_ctx = SerializationContext(args.topic, MessageField.VALUE)

    # Idempotent producer (overseer directive 1). acks=all is implied by
    # enable.idempotence but set explicitly for provenance.
    #
    # MEMORY BOUND (2026-07-12 OOM root cause, path (b)): librdkafka holds every
    # un-acked produced message in a C-side queue until the broker acks it. When
    # the broker is SLOWER than the parquet reader — exactly the pair-3 case,
    # where librdkafka logged "Failed to acquire idempotence PID ...: Coordinator
    # load in progress: retrying" right as the read side started pumping — this
    # queue fills to its cap and stays there. The cap is therefore a hard memory
    # reservation, not a theoretical ceiling. The previous values (1,000,000 msgs
    # / 1 GiB) were 10x the librdkafka default and reserved ~1 GiB of C heap that,
    # stacked on the pyarrow readahead spike, tipped a 6Gi container over.
    #
    # We set BOTH caps explicitly and back to the librdkafka defaults' order of
    # magnitude. queue-full raises BufferError in produce(), which the row loop
    # handles as backpressure (poll without advancing the iterator) — so a
    # smaller queue costs throughput under a stall, never correctness. Whichever
    # cap is hit first bounds the queue; keep the two consistent.
    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 50,
        # Kafka producer batch (messages coalesced per broker request). Bounded
        # by queue.buffering.max.messages below; kept modest so a full batch is
        # a fraction of the queue, not the whole thing.
        "batch.num.messages": 100000,
        # HARD memory bound on the C-side queue of un-acked messages. 100k msgs
        # is the librdkafka default; at ~1 KiB/serialised-row that is ~100 MiB.
        "queue.buffering.max.messages": 100000,
        # ...OR 128 MiB, whichever fills first (was 1 GiB). This is the dominant
        # C-heap reservation under a send-side stall — keep it small.
        "queue.buffering.max.kbytes": 131072,
        "message.max.bytes": 10485760,
    })

    mapper = make_row_mapper(field_order, int16_fields)

    sent = {"ok": 0, "err": 0}
    first_err = [None]

    def on_delivery(err, msg):
        if err is not None:
            sent["err"] += 1
            if first_err[0] is None:
                first_err[0] = str(err)
        else:
            sent["ok"] += 1

    dataset = ds.dataset(args.parquet_source, format="parquet")
    # Validate the parquet schema covers every Avro field before we stream.
    parquet_cols = set(dataset.schema.names)
    missing = [f for f in field_order if f not in parquet_cols]
    if missing:
        die(f"parquet source is missing {len(missing)} required columns: "
            f"{missing[:10]}{'...' if len(missing) > 10 else ''}")

    t0 = time.time()
    read = 0
    limit = args.limit
    stop = False
    # MEMORY BOUND (2026-07-12 OOM root cause, path (c) — the DOMINANT one).
    # pyarrow's dataset scanner prefetches batches in BACKGROUND threads. The
    # defaults are batch_readahead=16 and fragment_readahead=4, i.e. up to 64
    # decoded RecordBatches can be held in memory ahead of the consumer. When the
    # send side stalls (broker slow / idempotence PID coordinator-load), the row
    # loop below blocks in the BufferError poll, but the scanner threads keep
    # racing ahead, decoding whole row-groups (the staged hits files are ~1M rows
    # per row-group) into RAM. Measured locally on a 105-col hits-shaped parquet:
    # a 2s stall after the first batch pushed RSS to ~5.2 GiB at the defaults vs
    # ~2.1 GiB at 1x1 — a ~3.1 GiB (60%) swing that, stacked on the rdkafka queue
    # and the per-batch to_pylist, is what OOMKilled pair-3. We bound the scanner
    # to a single batch/fragment ahead: readahead now COSTS throughput only when
    # the broker can't keep up (exactly when we must not also blow up memory).
    for batch in dataset.to_batches(columns=field_order,
                                    batch_size=args.batch_size,
                                    batch_readahead=1,
                                    fragment_readahead=1):
        # Columnar bytes->utf8 decode (real hits parquet: BINARY strings) +
        # non-null guard, before the row loop.
        batch = prepare_batch(batch, string_fields)
        # to_pylist materialises this ONE batch (batch_size rows) of 105-col
        # dicts. Release the Arrow batch reference before the row loop so it is
        # not pinned alongside the python dicts, and release the dict list right
        # after — keeps at most one batch's worth of rows live at a time.
        rows = batch.to_pylist()
        del batch
        for row in rows:
            mapped = mapper(row)
            payload = avro_serializer(mapped, ser_ctx)
            # Retry on local queue-full backpressure (BufferError) — this is not
            # a send failure, just flow control. Idempotence keeps retries safe.
            # CRITICAL for memory: on BufferError we poll (drain acks) and RETRY
            # THE SAME row — we do NOT advance the iterator. So pyarrow batches
            # and python dicts are consumed no faster than the broker drains,
            # rather than piling up while librdkafka's bounded queue is full.
            while True:
                try:
                    producer.produce(args.topic, value=payload,
                                     on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.5)
            read += 1
            if limit and read >= limit:
                stop = True
                break
        # Drop this batch's rows promptly (do not let them straddle the next
        # batch's to_pylist allocation).
        del rows
        producer.poll(0)
        if read - _last_progress["read"] >= 500000 or stop:
            _last_progress["read"] = read
            # RSS on every progress line so a future OOM is diagnosable from the
            # Job log alone (see _final_breadcrumb).
            log(f"produced {read} rows "
                f"({read / max(time.time() - t0, 1e-9):.0f} rows/s) "
                f"peak_rss={rss_mb():.0f} MiB")
        if stop:
            break

    log(f"finished reading {read} rows; flushing...")
    remaining = producer.flush(300)
    if remaining > 0:
        die(f"flush timed out with {remaining} messages still in queue")
    duration = time.time() - t0

    if first_err[0] is not None:
        die(f"delivery failures: {sent['err']} messages; first error: "
            f"{first_err[0]}")

    rows_sent = sent["ok"]
    log(f"delivery-callback confirmed rows_sent={rows_sent} in {duration:.1f}s")

    # Authoritative count from committed END OFFSETS (overseer directive 2).
    rows_expected, per_partition = committed_offsets(args.bootstrap, args.topic)
    log(f"committed offsets: rows_expected={rows_expected} "
        f"across {len(per_partition)} partitions")

    rate = rows_expected / duration if duration > 0 else 0.0
    summary = {
        "topic": args.topic,
        "partitions": len(per_partition),
        "rows_sent": rows_sent,
        "rows_expected": rows_expected,
        "per_partition": {str(k): v for k, v in per_partition.items()},
        "duration_seconds": round(duration, 3),
        "rate_rows_per_sec": round(rate, 1),
        "idempotence": True,
        "acks": "all",
        "parquet_source": args.parquet_source,
        "match": rows_sent == rows_expected,
    }
    # The summary is ALWAYS the last stdout line, machine-readable JSON.
    print(json.dumps(summary), flush=True)

    if rows_sent != rows_expected:
        die(f"COUNT MISMATCH: rows_sent={rows_sent} != "
            f"rows_expected(offsets)={rows_expected}. Idempotence should make "
            f"these equal; a mismatch means the pre-load is INVALID.", code=2)

    if rows_expected == 0:
        die("rows_expected == 0: produced nothing (empty/wrong parquet source?)",
            code=3)

    log("OK: rows_sent == rows_expected; pre-load is valid.")
    sys.exit(0)


if __name__ == "__main__":
    main()
