#!/usr/bin/env python3
"""Producer mapping test — REAL-parquet shape, not the idealised one.

Regression guard for the live-run failure: the real ClickBench hits parquet
stores its 28 string columns as BINARY, so pyarrow yields ``bytes`` (and real
hits data contains invalid UTF-8), which crashed fastavro's ``write_utf8``
("TypeError: must be string on field Title"). The original verification used a
synthetic parquet with native string columns and never exercised that path.

This test generates (at test time — no real hits data checked in) a small
parquet whose string columns are BINARY-typed and contain invalid UTF-8 plus
edge values, then runs it through the ACTUAL producer path:

    pyarrow read -> prepare_batch (columnar bytes->utf8) -> row mapper
        -> fastavro schemaless_writer against hits.avsc

``schemaless_writer`` is the same encoder confluent's ``AvroSerializer`` uses
internally, so this exercises the exact ``write_utf8`` requirement without
needing a live Schema Registry.

Also asserts:
  * invalid UTF-8 decodes deterministically (errors='replace', U+FFFD);
  * a None in a (non-nullable) column FAILS LOUDLY with the column name;
  * Avro round-trip (write + read) preserves values.

Run:  python3 test_mapping.py   (exit 0 = pass)

Follow-up (out of scope here): the #27 sink-side mapping test
(src/test/.../benchmark/HitsAvroMappingTest) covers AvroConverter -> sink; the
producer-side BINARY/bytes concern lives in THIS test.
"""

import datetime
import io
import json
import os
import sys
import tempfile

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import fastavro

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import producer  # noqa: E402  (the module under test)

HERE = os.path.dirname(os.path.abspath(__file__))
SCHEMA_PATH = os.path.join(HERE, "..", "schema", "hits.avsc")

# Invalid UTF-8 sequences that appear in real hits data (lone continuation
# bytes, truncated multibyte, 0xFF). errors='replace' must map these to U+FFFD
# deterministically.
INVALID_UTF8 = [
    b"\xff\xfebroken",
    b"caf\xe9",              # latin-1 e-acute, invalid as UTF-8
    b"\xed\xa0\x80",         # UTF-8-encoded surrogate half
    b"ok-prefix\x80suffix",  # lone continuation byte
]
N = 200

DATETIME = set(producer.DATETIME_COLS)
DATE = producer.DATE_COL


def build_binary_parquet(path, avsc, with_null_in=None):
    """Small hits-shaped parquet with BINARY string columns (like real hits)."""
    fields, arrays = [], []
    for f in avsc["fields"]:
        name, t = f["name"], f["type"]
        if name in DATETIME:
            pat = pa.timestamp("us", tz="UTC")
            base = datetime.datetime(2013, 7, 15, tzinfo=datetime.timezone.utc)
            vals = [base + datetime.timedelta(seconds=i) for i in range(N)]
            vals[0] = datetime.datetime(1970, 1, 1,
                                        tzinfo=datetime.timezone.utc)
        elif name == DATE:
            pat = pa.date32()
            vals = [datetime.date(2013, 7, 15) + datetime.timedelta(days=i)
                    for i in range(N)]
            vals[0] = datetime.date(1970, 1, 1)
        elif isinstance(t, dict) and t.get("connect.type") == "int16":
            pat = pa.int16()
            vals = [-32768, 32767, 0] + [i % 100 for i in range(N - 3)]
        elif t == "long":
            pat = pa.int64()
            vals = [-(2 ** 63), 2 ** 63 - 1, 0] + [i for i in range(N - 3)]
        elif t == "int":
            pat = pa.int32()
            vals = [-(2 ** 31), 2 ** 31 - 1, 0] + [i for i in range(N - 3)]
        elif t == "string":
            # THE POINT: BINARY-typed column, bytes values, invalid UTF-8 mixed
            # with valid edge values (empty, unicode, long).
            pat = pa.binary()
            samples = INVALID_UTF8 + [b"", "unicode-é中\U0001f600".encode(),
                                      b"x" * 500]
            vals = [samples[i % len(samples)] for i in range(N)]
        else:
            raise AssertionError(f"unhandled avro type for {name}: {t}")
        if with_null_in == name:
            vals[7] = None
        arrays.append(pa.array(vals, type=pat))
        fields.append(pa.field(name, pat, nullable=(with_null_in == name)))
    pq.write_table(pa.table(arrays, schema=pa.schema(fields)), path)


def main():
    schema_str, field_order, int16_fields, string_fields = \
        producer.load_avro_schema(SCHEMA_PATH)
    avsc = json.loads(schema_str)
    parsed = fastavro.parse_schema(avsc)
    assert len(string_fields) == 28, f"expected 28 string fields, got {len(string_fields)}"
    mapper = producer.make_row_mapper(field_order, int16_fields)

    with tempfile.TemporaryDirectory() as td:
        # --- happy path: BINARY strings + invalid UTF-8 ----------------------
        p = os.path.join(td, "hits_binary.parquet")
        build_binary_parquet(p, avsc)
        dataset = ds.dataset(p, format="parquet")
        # confirm the fixture really is BINARY-typed (the failure precondition)
        assert pa.types.is_binary(dataset.schema.field("Title").type), \
            "fixture must use BINARY string columns"

        encoded = decoded_rows = 0
        first_rec = None
        for batch in dataset.to_batches(columns=field_order, batch_size=64):
            batch = producer.prepare_batch(batch, string_fields)
            # after prepare_batch every avro-string column must be arrow string
            for name in string_fields:
                assert pa.types.is_string(batch.schema.field(name).type), name
            for row in batch.to_pylist():
                rec = mapper(row)
                buf = io.BytesIO()
                fastavro.schemaless_writer(buf, parsed, rec)  # the write_utf8 path
                encoded += 1
                buf.seek(0)
                back = fastavro.schemaless_reader(buf, parsed)
                if first_rec is None:
                    first_rec = back
                decoded_rows += 1
        assert encoded == decoded_rows == N, (encoded, decoded_rows)
        assert len(first_rec) == 105
        # determinism of errors='replace': invalid byte -> U+FFFD
        assert first_rec["Title"] == INVALID_UTF8[0].decode("utf-8", "replace")
        assert "�" in first_rec["Title"]
        assert isinstance(first_rec["EventTime"], int) and first_rec["EventTime"] == 0
        assert first_rec["EventDate"] == datetime.date(1970, 1, 1)
        assert first_rec["JavaEnable"] == -32768
        print(f"PASS happy path: {N} BINARY-string rows (invalid UTF-8 included) "
              f"mapped + Avro round-tripped; replace-decode deterministic")

        # --- None in a string column must FAIL LOUDLY with the column name ---
        p2 = os.path.join(td, "hits_null.parquet")
        build_binary_parquet(p2, avsc, with_null_in="Referer")
        dataset2 = ds.dataset(p2, format="parquet")
        failed = False
        try:
            for batch in dataset2.to_batches(columns=field_order, batch_size=64):
                producer.prepare_batch(batch, string_fields)
        except SystemExit:
            failed = True  # producer.die() -> sys.exit
        assert failed, "None in a non-nullable column must fail the run"
        print("PASS null guard: None in 'Referer' failed loudly (SystemExit), "
              "not coerced")

        # --- None in a numeric column too (guard covers all 105) -------------
        p3 = os.path.join(td, "hits_null_num.parquet")
        build_binary_parquet(p3, avsc, with_null_in="RegionID")
        failed = False
        try:
            for batch in ds.dataset(p3, format="parquet").to_batches(
                    columns=field_order, batch_size=64):
                producer.prepare_batch(batch, string_fields)
        except SystemExit:
            failed = True
        assert failed, "None in a numeric column must fail the run"
        print("PASS null guard: None in 'RegionID' failed loudly")

    # --- N3: _final_breadcrumb reports the LIVE row count, not the stale
    #        500k-throttled _last_progress. Simulate the state right before an
    #        OOMKill: the live counter is well past the last throttled log line.
    producer._last_progress["read"] = 500000   # last periodic LOG flush
    producer._live_progress["read"] = 512345    # true rows produced at death
    cap = io.StringIO()
    _orig_stderr = sys.stderr
    try:
        sys.stderr = cap
        producer._final_breadcrumb("test")
    finally:
        sys.stderr = _orig_stderr
    out = cap.getvalue()
    assert "rows_produced_so_far=512345" in out, out
    assert "rows_produced_so_far=500000" not in out, out
    print("PASS breadcrumb: FINAL trailer reports the live row count (512345), "
          "not the 500k-throttled _last_progress")

    print("ALL MAPPING TESTS PASSED")


if __name__ == "__main__":
    main()
