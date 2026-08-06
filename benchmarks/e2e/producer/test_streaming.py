#!/usr/bin/env python3
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Continuous streaming mode for the producer (chaos test T4 / IC-7).

The chaos monkey run needs a LONG-LIVED producer that streams the staged prefix
at a bounded rate until the harness fences it (SIGTERM), then reports the exact
count of distinct WatchIDs it acked (feeds SOURCE_UNIQUE_EXPECTED, IC-7). The
invariant that makes that number trustworthy is SINGLE-PASS / NEVER-WRAPS:
exhausting the dataset before the fence is a loud producer-side error (the
harness sizes the dataset >= rate x max run duration), never a silent restart
from row 0 (which would double-count and inflate uniqueness).

These tests pin, with NO broker and NO network:
  * token-bucket rate math over a synthetic clock (pure);
  * the paced single-pass driver: fences cleanly, and raises on exhaustion
    (the no-wrap invariant);
  * the WatchID ledger: exact distinct count under duplicate WatchIDs;
  * argparse: ``--stream`` REQUIRES ``--rate-limit``; the default (one-shot)
    path is unchanged;
  * end-to-end SIGTERM -> flush -> final summary JSON carrying ``unique_sent``,
    driven as a real subprocess against a tiny generated parquet with the
    confluent_kafka Producer/serializer STUBBED (no broker);
  * end-to-end no-wrap: a subprocess that exhausts the dataset before any fence
    exits non-zero with a loud message.
"""

import json
import os
import signal
import subprocess
import sys
import tempfile
import textwrap
import time

import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import producer  # noqa: E402  (module under test)
from test_mapping import build_binary_parquet  # noqa: E402  (DRY fixture builder)

HERE = os.path.dirname(os.path.abspath(__file__))
SCHEMA_PATH = os.path.join(HERE, "..", "schema", "hits.avsc")


class FakeClock:
    """Deterministic monotonic clock; sleep() advances virtual time."""

    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def sleep(self, seconds):
        if seconds > 0:
            self.t += seconds


# --- token-bucket rate math (pure) -------------------------------------------

def test_token_bucket_burst_refill_and_cap():
    clk = FakeClock()
    b = producer.TokenBucket(rate=10, clock=clk)  # capacity defaults to rate
    # Starts full: capacity (=10) immediate acquisitions succeed, 11th fails.
    for _ in range(10):
        assert b.try_acquire() is True
    assert b.try_acquire() is False
    # Exactly one token accrues after 1/rate seconds.
    assert b.time_until(1) == pytest.approx(0.1, abs=1e-9)
    clk.sleep(0.1)
    assert b.try_acquire() is True
    assert b.try_acquire() is False
    # Refill is capped at capacity: a long idle does NOT bank unbounded tokens.
    clk.sleep(1000.0)
    got = 0
    while b.try_acquire():
        got += 1
    assert got == 10, "token bucket must cap accrual at capacity"


def test_token_bucket_amortized_rate():
    clk = FakeClock()
    rate = 100
    b = producer.TokenBucket(rate=rate, clock=clk)
    n = 0
    # Consume 1000 tokens, sleeping the bucket's advised wait each time it is dry.
    while n < 1000:
        if b.try_acquire():
            n += 1
        else:
            clk.sleep(b.time_until(1))
    # 1000 tokens, capacity (=100) of them free at t0 => ~9.0s of paced supply.
    assert clk.t == pytest.approx((1000 - rate) / rate, rel=0.02)


# --- paced single-pass driver ------------------------------------------------

def test_drive_stream_paces_to_rate():
    clk = FakeClock()
    rate = 50
    b = producer.TokenBucket(rate=rate, clock=clk)
    fence = producer.FenceSignal()
    produced = []
    rows = [{"WatchID": i} for i in range(500)]
    with pytest.raises(producer.StreamExhausted):
        producer.drive_stream(iter(rows), b, fence,
                              produce=produced.append,
                              poll=lambda t=0: None,
                              sleep=clk.sleep)
    assert produced == rows, "single pass must emit every row exactly once, in order"
    # capacity free at start, the rest paced at `rate`.
    assert clk.t == pytest.approx((len(rows) - rate) / rate, rel=0.02)


def test_drive_stream_stops_on_fence_midstream():
    clk = FakeClock()
    b = producer.TokenBucket(rate=1_000_000, clock=clk)  # never the bottleneck
    fence = producer.FenceSignal()
    produced = []

    def produce(row):
        produced.append(row)
        if len(produced) == 5:
            fence.request()  # the SIGTERM equivalent lands after row 5

    rows = [{"WatchID": i} for i in range(100)]
    result = producer.drive_stream(iter(rows), b, fence,
                                   produce=produce,
                                   poll=lambda t=0: None,
                                   sleep=clk.sleep)
    assert result == "fenced"
    assert len(produced) == 5, "fence must stop the read loop, not drain the dataset"


def test_drive_stream_exhaustion_is_loud_no_wrap():
    """The load-bearing invariant: dataset exhausted before a fence => raise.

    A silent wrap-around would re-send rows and double-count WatchIDs, poisoning
    SOURCE_UNIQUE_EXPECTED. So the driver must RAISE, never restart from row 0.
    """
    clk = FakeClock()
    b = producer.TokenBucket(rate=1_000_000, clock=clk)
    fence = producer.FenceSignal()  # never fenced
    produced = []
    rows = [{"WatchID": i} for i in range(7)]
    with pytest.raises(producer.StreamExhausted):
        producer.drive_stream(iter(rows), b, fence,
                              produce=produced.append,
                              poll=lambda t=0: None,
                              sleep=clk.sleep)
    assert produced == rows  # emitted the whole prefix exactly once, then raised


# --- WatchID ledger (pure) ---------------------------------------------------

def test_watchid_ledger_distinct_with_duplicates():
    led = producer.WatchIDLedger()
    for wid in [10, 20, 20, 30, 30, 30, 10, 40]:
        led.record_ack(wid)
    assert led.acked == 8, "acked counts every acked row"
    assert led.unique_count == 4, "unique_count is distinct WatchIDs (10,20,30,40)"
    # Big-int WatchIDs (Int64) must not be truncated/collapsed.
    led2 = producer.WatchIDLedger()
    led2.record_ack(2 ** 63 - 1)
    led2.record_ack(-(2 ** 63))
    led2.record_ack(2 ** 63 - 1)
    assert led2.acked == 3
    assert led2.unique_count == 2


# --- argparse ----------------------------------------------------------------

def _clear_stream_env(monkeypatch):
    for k in ("STREAM", "RATE_LIMIT"):
        monkeypatch.delenv(k, raising=False)


def test_argparse_default_is_one_shot(monkeypatch):
    _clear_stream_env(monkeypatch)
    monkeypatch.setattr(sys, "argv", ["producer.py"])
    args = producer.parse_args()
    assert args.stream is False, "default path must remain one-shot"
    assert args.rate_limit == 0
    # A one-shot invocation validates with no error (no fence semantics).
    producer.validate_stream_args(args.stream, args.rate_limit)


def test_argparse_stream_requires_rate_limit(monkeypatch):
    _clear_stream_env(monkeypatch)
    monkeypatch.setattr(sys, "argv", ["producer.py", "--stream"])
    args = producer.parse_args()
    assert args.stream is True
    with pytest.raises(SystemExit):
        producer.validate_stream_args(args.stream, args.rate_limit)


def test_argparse_stream_with_rate_limit_ok(monkeypatch):
    _clear_stream_env(monkeypatch)
    monkeypatch.setattr(sys, "argv",
                        ["producer.py", "--stream", "--rate-limit", "5000"])
    args = producer.parse_args()
    assert args.stream is True and args.rate_limit == 5000
    producer.validate_stream_args(args.stream, args.rate_limit)  # no raise


# --- end-to-end subprocess (stubbed Producer, no broker) ---------------------

_RUNNER = textwrap.dedent(
    """
    import sys, types, os
    sys.path.insert(0, {producer_dir!r})
    import producer

    class _Msg:
        def value(self):
            return b""

    class _FakeProducer:
        def __init__(self, conf):
            pass
        def produce(self, topic, value=None, on_delivery=None):
            # ack synchronously: the delivery callback threads the WatchID into
            # the ledger, exactly as librdkafka would on a real ack.
            if on_delivery is not None:
                on_delivery(None, _Msg())
        def poll(self, timeout=0):
            return 0
        def flush(self, timeout=0):
            return 0

    producer.Producer = _FakeProducer
    producer.SchemaRegistryClient = lambda conf: object()
    producer.ensure_subject = lambda *a, **k: 1
    producer.AvroSerializer = lambda *a, **k: (lambda val, ctx: b"x")
    producer.SerializationContext = lambda *a, **k: None
    producer.MessageField = types.SimpleNamespace(VALUE="value")
    producer.main()
    """
)


def _write_fixture(tmpdir):
    schema_str, field_order, _i16, _sf = producer.load_avro_schema(SCHEMA_PATH)
    avsc = json.loads(schema_str)
    path = os.path.join(tmpdir, "hits_small.parquet")
    build_binary_parquet(path, avsc)
    watchids = pq.read_table(path, columns=["WatchID"]).column(
        "WatchID").to_pylist()
    return path, watchids


def _runner_cmd(tmpdir, fixture, rate):
    runner = os.path.join(tmpdir, "runner.py")
    with open(runner, "w") as f:
        f.write(_RUNNER.format(producer_dir=HERE))
    return [sys.executable, runner,
            "--stream", "--rate-limit", str(rate),
            "--topic", "hits",
            "--bootstrap", "localhost:0",
            "--registry-url", "http://localhost:0",
            "--parquet-source", fixture]


def _last_json_line(stdout):
    lines = [ln for ln in stdout.splitlines() if ln.strip()]
    assert lines, f"no stdout produced; got:\n{stdout}"
    return json.loads(lines[-1])


def test_subprocess_sigterm_flushes_then_summarizes_unique_sent():
    with tempfile.TemporaryDirectory() as td:
        fixture, watchids = _write_fixture(td)
        # Low rate so the ~200-row fixture is NOT exhausted before we fence.
        proc = subprocess.Popen(_runner_cmd(td, fixture, rate=20),
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True)
        time.sleep(1.5)  # let streaming get well underway (still far from done)
        proc.send_signal(signal.SIGTERM)  # THE FENCE
        out, err = proc.communicate(timeout=30)
        assert proc.returncode == 0, f"fenced run must exit 0\nstderr:\n{err}"
        summary = _last_json_line(out)
        assert summary["stream"] is True
        assert summary["fenced"] is True
        assert "unique_sent" in summary, "summary must carry unique_sent (IC-7)"
        rows_sent = summary["rows_sent"]
        assert 0 < rows_sent < len(watchids), \
            "fenced early: some but not all rows"
        # EXACT: unique_sent == distinct WatchIDs over the produced prefix (the
        # dataset is streamed in deterministic order, acked synchronously).
        assert summary["unique_sent"] == len(set(watchids[:rows_sent]))
        assert "exhaust" not in err.lower(), "fence path must not report exhaustion"


def test_subprocess_exhaustion_before_fence_is_loud_error():
    with tempfile.TemporaryDirectory() as td:
        fixture, watchids = _write_fixture(td)
        # High rate: the whole fixture drains in well under a second; with NO
        # SIGTERM the single-pass driver must exhaust and exit LOUDLY.
        proc = subprocess.run(_runner_cmd(td, fixture, rate=1_000_000),
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              text=True, timeout=30)
        assert proc.returncode != 0, "exhaustion before a fence must fail the run"
        assert "exhaust" in proc.stderr.lower() or "wrap" in proc.stderr.lower(), \
            f"error must name the no-wrap violation; stderr:\n{proc.stderr}"
