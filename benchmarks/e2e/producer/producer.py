#!/usr/bin/env python3
"""Benchmark v2 producer — ClickBench hits parquet -> Avro -> Kafka topic.

Reads the ClickBench ``hits`` parquet (canonical single file or partitioned
variants), maps every one of the 105 columns to the Avro types declared in
``benchmarks/e2e/schema/hits.avsc``, and publishes each row to a Kafka topic
using the Confluent Schema Registry wire format (magic byte + 4-byte schema id)
so the sink's Confluent ``AvroConverter`` can decode it.

Backlog-drain precondition (plan §5 step 2, decision 6): this Job runs to
completion BEFORE any measured drain. Its single job is to leave the topic
pre-loaded with EXACTLY the dataset. The AUTHORITATIVE ``rows_expected`` is
derived by the ORCHESTRATOR from committed topic END OFFSETS after the Job
completes — never from producer-side send counts (overseer directive 2).

SHARDED PRE-LOAD (parallel producer, cut the ~20min preload to ~5-6min): the
producer runs as a K8s INDEXED Job of ``SHARD_COUNT`` pods. Each pod reads its
completion index (``JOB_COMPLETION_INDEX``, K8s-injected) and ``SHARD_COUNT``,
lists the parquet files under ``PARQUET_SOURCE``, stable-sorts them
lexicographically, and produces ONLY the files at ``i % SHARD_COUNT == index``
(stride-N). N=1 degenerates to the original single-producer behaviour. Every
pod is otherwise identical: its own idempotent producer (own PID), same
schema/serializer, the same bounded memory (each pod streams only its shard).
The preload is UNMEASURED, so this parallelism does not touch the instrument.

Count contract under sharding (the crux):
  * enable.idempotence=true (=> acks=all): producer retries never write
    duplicate records into the topic. Without it the sink would be blamed for
    producer-side dupes (overseer directive 1). Still per-pod.
  * Each pod counts ITS OWN shard: ``rows_sent`` is the delivery-callback count
    for this pod's files only, and the summary JSON carries shard fields
    (shard_index, shard_count, files_in_shard, rows_sent). The per-pod GLOBAL
    end-offsets check is REMOVED — under sharding the topic's end offsets show
    the SUM of all N pods, so a per-pod compare against them is nonsense. The
    only per-pod sanity is shard-local: rows_sent > 0 when files_in_shard > 0.
  * The GLOBAL rows_expected (= Σ end-offset − beginning-offset over partitions)
    is computed by the ORCHESTRATOR (run_pair.sh phase_preload) AFTER the Job
    completes, via the broker — producer-count-agnostic, the property that keeps
    preload speed irrelevant to the measurement. A best-effort cross-check sums
    the N pods' rows_sent and WARNs on mismatch.
  * PARQUET_SOURCE is a parameter (overseer directive 3), defaulting to a
    us-east-2 staging placeholder (see README for the staging TBD note).

Latency profile / produce_ts is DEFERRED (plan Appendix A) — not implemented.

Output: a single machine-readable JSON object on stdout (the last stdout line
is always the summary; all human logging goes to stderr) PER POD for the
orchestrator (task 31) to parse from that pod's completion-index log. Per-pod
exit codes: 0 success (incl. an empty shard), 1 any send/delivery failure, 3
a non-empty shard that produced zero rows.
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


# Throttle for the periodic progress LOG line (updated every 500k rows).
_last_progress = {"read": 0}
# LIVE row counter, updated every row by the hot loop. The breadcrumb must read
# THIS, not _last_progress: on an OOMKill the last thing we want is a trailer
# that under-reports by up to ~500k rows (the log-throttle window). A single
# dict write per row is negligible next to the Avro serialise + produce.
_live_progress = {"read": 0}


def _final_breadcrumb(reason):
    # Best-effort: a single line so an OOMKill/SIGTERM leaves a diagnosable
    # trailer in the Job log instead of silence. Must not raise. Reads the LIVE
    # counter so the reported row count is exact at the moment of death.
    try:
        print(f"[producer] FINAL breadcrumb ({reason}): "
              f"rows_produced_so_far={_live_progress['read']} "
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


# --- sharding ----------------------------------------------------------------

def read_shard_env():
    """Return (shard_index, shard_count) from the environment.

    ``JOB_COMPLETION_INDEX`` is injected by K8s into every pod of an Indexed
    Job (0..completions-1). ``SHARD_COUNT`` is set from the Job's completions/
    parallelism (default 3). Standalone/local runs default index 0 / count 1
    (=> the original single-producer whole-dataset behaviour).
    """
    idx = int(os.environ.get("JOB_COMPLETION_INDEX", "0"))
    cnt = int(os.environ.get("SHARD_COUNT", "1"))
    if cnt < 1:
        die(f"SHARD_COUNT={cnt} is invalid (must be >= 1)")
    if idx < 0 or idx >= cnt:
        die(f"JOB_COMPLETION_INDEX={idx} out of range for SHARD_COUNT={cnt}")
    return idx, cnt


def list_dataset_files(dataset):
    """Stable, lexicographically-sorted list of the dataset's parquet files.

    ``ds.dataset(...).files`` returns the fully-qualified file paths (local or
    ``s3://``). We sort them so the stride assignment is DETERMINISTIC and
    IDENTICAL across all N pods — every pod must see the same ordering or two
    pods could produce the same file (or a file could be skipped). A single
    parquet file (the canonical ClickBench ``hits.parquet``) yields a one-item
    list, so a single-file source with N>1 gives exactly one non-empty shard
    and N-1 empty shards (each empty shard exits 0, rows_sent=0).
    """
    return sorted(dataset.files)


def select_shard_files(files, shard_index, shard_count):
    """Stride-N selection: files[i] where i % shard_count == shard_index.

    ``files`` MUST already be stable-sorted (see list_dataset_files) so the
    partition of the file set across shards is disjoint and complete: every
    file lands in exactly one shard. N=1 selects every file (degenerate =
    today's behaviour). When len(files) < shard_count the high-index shards get
    an empty selection (a legitimate empty shard).
    """
    return [f for i, f in enumerate(files) if i % shard_count == shard_index]


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
#
# The authoritative global rows_expected (= Σ end-offset − beginning-offset over
# partitions) is computed by the ORCHESTRATOR (run_pair.sh phase_preload) from
# the broker AFTER the whole Indexed Job completes, not per-pod. Under sharding
# a per-pod consumer probe of the topic end offsets would return the running
# GLOBAL total (all N pods), which is meaningless as a per-pod check — so the
# producer no longer reads offsets at all. See run_pair.sh phase_preload /
# broker_topic_row_count for the (kafka-get-offsets.sh) derivation.


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

    shard_index, shard_count = read_shard_env()
    log(f"shard {shard_index}/{shard_count} "
        f"(JOB_COMPLETION_INDEX={os.environ.get('JOB_COMPLETION_INDEX','<unset>')} "
        f"SHARD_COUNT={os.environ.get('SHARD_COUNT','<unset>')})")
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
        # Kafka producer batch (max messages coalesced per broker request).
        # Set EQUAL to queue.buffering.max.messages below: this is a ceiling, not
        # a reservation, so a single broker request may pack up to the whole
        # queue when it is full — fine at acks=all/idempotence (linger.ms=50
        # still bounds latency). It is the queue cap, not this, that bounds the
        # C-side memory reservation.
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

    # Sharded selection (parallel preload). Every pod lists + stable-sorts the
    # SAME file set and takes its stride-N slice, so the shards are disjoint and
    # complete. N=1 selects everything (original behaviour).
    all_files = list_dataset_files(dataset)
    shard_files = select_shard_files(all_files, shard_index, shard_count)
    log(f"shard {shard_index}/{shard_count}: {len(shard_files)} of "
        f"{len(all_files)} file(s) assigned to this pod")
    for f in shard_files:
        log(f"  shard file: {f}")
    if not shard_files:
        # A legitimate empty shard (files < SHARD_COUNT). Produce nothing, emit a
        # summary saying so, and exit 0 — an empty shard is NOT a failure. The
        # orchestrator's global end-offsets count is unaffected (other pods carry
        # the whole dataset between them).
        log(f"shard {shard_index}/{shard_count} has NO files — nothing to "
            f"produce; exiting 0 (empty shard)")
        summary = {
            "topic": args.topic,
            "shard_index": shard_index,
            "shard_count": shard_count,
            "files_in_shard": 0,
            "rows_sent": 0,
            "duration_seconds": 0.0,
            "rate_rows_per_sec": 0.0,
            "idempotence": True,
            "acks": "all",
            "parquet_source": args.parquet_source,
            "empty_shard": True,
        }
        print(json.dumps(summary), flush=True)
        log("OK: empty shard, rows_sent=0.")
        sys.exit(0)
    # Restrict the scan to THIS pod's files. A dataset built from an explicit
    # file list reads only those files (the readahead bounds below are unchanged
    # and now apply per-shard).
    shard_dataset = ds.dataset(shard_files, format="parquet")

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
    for batch in shard_dataset.to_batches(columns=field_order,
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
            # Publish the live count every row so _final_breadcrumb is exact on
            # an abrupt OOMKill/SIGTERM (cheap dict write; see _live_progress).
            _live_progress["read"] = read
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

    # SHARDED count contract: this pod reports ITS OWN shard's rows_sent only.
    # The per-pod GLOBAL end-offsets check is REMOVED — under sharding the topic
    # end offsets reflect the SUM of all N pods, so comparing THIS pod's send
    # count to them is meaningless. The authoritative global rows_expected is
    # computed by the orchestrator from the broker AFTER the whole Job completes
    # (run_pair.sh phase_preload). The only per-pod sanity is shard-LOCAL:
    # a non-empty shard that produced zero rows is a failure (exit 3).
    rate = rows_sent / duration if duration > 0 else 0.0
    summary = {
        "topic": args.topic,
        "shard_index": shard_index,
        "shard_count": shard_count,
        "files_in_shard": len(shard_files),
        "rows_sent": rows_sent,
        "duration_seconds": round(duration, 3),
        "rate_rows_per_sec": round(rate, 1),
        "idempotence": True,
        "acks": "all",
        "parquet_source": args.parquet_source,
        "empty_shard": False,
    }
    # The summary is ALWAYS the last stdout line, machine-readable JSON.
    print(json.dumps(summary), flush=True)

    # Shard-local sanity: a non-empty shard must have produced something. The
    # empty-shard case (files_in_shard == 0) already returned above with exit 0.
    if rows_sent == 0:
        die(f"shard {shard_index}/{shard_count} had {len(shard_files)} file(s) "
            f"but produced 0 rows (empty/corrupt shard files?)", code=3)

    log(f"OK: shard {shard_index}/{shard_count} produced rows_sent={rows_sent} "
        f"from {len(shard_files)} file(s).")
    sys.exit(0)


if __name__ == "__main__":
    main()
