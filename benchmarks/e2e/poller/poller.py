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
"""Benchmark v2 metrics poller CLI (task 29).

Subcommands:
  sample    Run the sampling loop until the consumer group's lag reaches 0 or a
            timeout. Writes raw samples as JSONL. Exit 0 = drained, 2 = timeout.
  finalize  Read a JSONL sample file, compute the per-run scalar set + guards,
            print them as JSON. With --insert, land them into perf.metrics.

The poller samples the CLIENT side only (offsets / Connect REST / JMX / pod
stats); the CH-server-side metrics come from the reused Spark capture SQL. See
README.md for the metric-by-metric source table and the task-31 prerequisites.
"""
import argparse
import json
import os
import sys

import sampler
import finalizer


POLL_INTERVAL_DEFAULT = float(os.environ.get("POLL_INTERVAL", "10"))


def _log(msg):
    print(f"[poller] {msg}", file=sys.stderr)


def cmd_sample(args) -> int:
    cfg = {
        "bootstrap": args.bootstrap,
        "group": args.group,
        "topic": args.topic,
        "connect_url": args.connect_url,
        "connector": args.connector,
        "jmx_url": args.jmx_url,
        "cadvisor_url": args.cadvisor_url,
        "pod_name": args.pod_name,
        "pod_container": args.pod_container,
    }
    offsets_source, connect_source, jmx_source, pod_source = sampler.build_sources(cfg)
    result = sampler.run_sampler(
        out_path=args.out,
        offsets_source=offsets_source,
        connect_source=connect_source,
        jmx_source=jmx_source,
        pod_source=pod_source,
        poll_interval=args.poll_interval,
        timeout=args.timeout,
        log=_log,
    )
    _log(f"sampling done: {result}")
    print(json.dumps(result))
    # Exit codes: 0 drained, 2 timeout (task brief).
    return 0 if result["drained"] else 2


def cmd_finalize(args) -> int:
    samples = sampler.load_samples(args.samples)
    _log(f"loaded {len(samples)} samples from {args.samples}")
    result = finalizer.finalize(samples, tier=args.tier,
                                rows_expected=args.rows_expected)

    if args.insert:
        import ch_insert
        cfg = ch_insert.CHConfig()
        _log(f"inserting into perf.metrics via {cfg!r}")
        ins = ch_insert.insert_metrics(
            run_id=args.run_id, tier=args.tier,
            scalars=result["scalars"], cfg=cfg)
        result["insert_result"] = ins
        _log(f"insert result: {ins}")

    # scalars JSON to stdout (guards + insert result included).
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("sample", help="sample until lag 0 or timeout")
    s.add_argument("--group", required=True, help="consumer group to watch")
    s.add_argument("--topic", required=True, help="topic being drained")
    s.add_argument("--connector", required=True, help="Connect connector name")
    s.add_argument("--out", required=True, help="output JSONL path")
    s.add_argument("--bootstrap",
                   default=os.environ.get(
                       "KAFKA_BOOTSTRAP",
                       "bench-kafka-bootstrap.kafka-bench.svc:9092"),
                   help="Kafka bootstrap servers")
    s.add_argument("--connect-url",
                   default=os.environ.get("CONNECT_URL", "http://localhost:8083"),
                   dest="connect_url", help="Connect REST base URL")
    s.add_argument("--jmx-url",
                   default=os.environ.get("JMX_METRICS_URL", ""),
                   dest="jmx_url",
                   help="prometheus /metrics URL of the Connect jmxPrometheusExporter "
                        "(see README task-31 prerequisite); empty -> JMX unavailable")
    s.add_argument("--cadvisor-url",
                   default=os.environ.get("POD_CADVISOR_URL", ""),
                   dest="cadvisor_url",
                   help="kubelet cadvisor /metrics/cadvisor URL for container CPU; "
                        "empty -> pod CPU unavailable")
    s.add_argument("--pod-name", default=os.environ.get("CONNECT_POD_NAME", ""),
                   dest="pod_name", help="Connect pod name (cadvisor label filter)")
    s.add_argument("--pod-container",
                   default=os.environ.get("CONNECT_POD_CONTAINER", ""),
                   dest="pod_container",
                   help="Connect container name (cadvisor label filter)")
    s.add_argument("--poll-interval", type=float, default=POLL_INTERVAL_DEFAULT,
                   dest="poll_interval", help="seconds between samples (default 10)")
    s.add_argument("--timeout", type=float,
                   default=float(os.environ.get("POLL_TIMEOUT", "3600")),
                   help="max seconds before giving up (exit 2)")
    s.set_defaults(func=cmd_sample)

    f = sub.add_parser("finalize", help="compute per-run scalars from samples")
    f.add_argument("--samples", required=True, help="input JSONL path")
    f.add_argument("--run-id", required=True, dest="run_id",
                   help="perf.runs run_id for THIS (arm, tier) row (contract §1.2)")
    f.add_argument("--tier", required=True, type=int, choices=[0, 1],
                   help="benchmark tier (selects the headline metric name)")
    f.add_argument("--rows-expected", required=True, type=float,
                   dest="rows_expected", help="produced row count (rows_expected)")
    f.add_argument("--insert", action="store_true",
                   help="land the scalars into perf.metrics (creds from env)")
    f.set_defaults(func=cmd_finalize)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
