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
"""Render the KafkaConnector CR from a template for one (arm,tier) or chaos run.

Pair mode (default, unchanged): substitutes ${CH_TABLE} (per-tier target:
hits_null Tier 0 | hits Tier 1) and sets the per-(arm,tier) consumer group.

Chaos mode (--exactly-once, task T7 / spec §3.2): renders the chaos-connector
template, substituting the render-time ${EXACTLY_ONCE} and ${IPWB} placeholders,
and first runs validate_chaos_config() to reject config combinations the sink
would reject or silently ignore at runtime (ClickHouseSinkTask.java:54-62, :150).

The ${env:CH_*} placeholders are left UNTOUCHED in either mode — they are
resolved by the Connect worker from the externalConfiguration Secret at runtime,
so credentials never appear in the rendered file or any log.

CFG_MAX_POLL_RECORDS (env, review F2): when set, overrides the template's
consumer.override.max.poll.records. run_pair.sh exports the SAME env var into
the runtime echo (build_runtime_json), so the deployed connector and the
recorded provenance agree BY CONSTRUCTION — previously the env var was
echo-only, so setting it wrote one value into perf.runs while the connector
silently ran the template's (false provenance). Unset => template value.

The template carries `//`-prefixed documentation keys; those are stripped from
the emitted CR (Connect would ignore them, but a clean CR is easier to review).

Output is a KafkaConnector CR (YAML/JSON both accepted by kubectl apply).
"""
import argparse
import json
import os
import sys


def strip_doc_keys(obj):
    """Recursively drop keys that start with '//' (template documentation)."""
    if isinstance(obj, dict):
        return {k: strip_doc_keys(v) for k, v in obj.items()
                if not (isinstance(k, str) and k.startswith("//"))}
    if isinstance(obj, list):
        return [strip_doc_keys(v) for v in obj]
    return obj


def validate_chaos_config(*, exactly_once, ipwb, buffer_count=0, watch_cell=False):
    """Reject chaos config combinations the sink would reject or silently ignore.

    Pure — raises ValueError with an actionable message on any invalid combo,
    returns None when valid. Enforces spec §3.2 (mirrors the runtime guards):

    * bufferCount>0 + exactlyOnce throws at task start
      (ClickHouseSinkTask.java:54-62): buffering changes batch boundaries and
      breaks block dedup + the offset state machine.
    * ignorePartitionsWhenBatching=true + exactlyOnce is SILENTLY IGNORED
      (ClickHouseSinkTask.java:150) — a run would otherwise test a config its
      runtime keys do not reflect; reject it loudly.
    * ignorePartitionsWhenBatching=true is a WATCH-only cell (§3.2): allowed in
      at-least-once ONLY behind an explicit watch-cell flag (it exercises the
      token-null / no-block-dedup path, where duplicates are flagged not failed).
    """
    if buffer_count > 0 and exactly_once:
        raise ValueError(
            "invalid chaos config: bufferCount>0 is incompatible with "
            "exactlyOnce (ClickHouseSinkTask.java:54-62 throws at task start). "
            "Pin bufferCount=0 for exactly-once chaos runs.")
    if ipwb and exactly_once:
        raise ValueError(
            "invalid chaos config: ignorePartitionsWhenBatching=true is silently "
            "ignored under exactlyOnce (ClickHouseSinkTask.java:150) — the run "
            "would test a config its runtime keys do not reflect. Set "
            "ignorePartitionsWhenBatching=false for exactly-once runs.")
    if ipwb and not exactly_once and not watch_cell:
        raise ValueError(
            "invalid chaos config: ignorePartitionsWhenBatching=true is a "
            "WATCH-cell-only config (§3.2). Pass --watch-cell to characterise "
            "the token-null path in at-least-once, or set it false.")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--template", required=True)
    ap.add_argument("--ch-table", help="pair mode: hits_null (T0) | hits (T1)")
    ap.add_argument("--group", help="consumer group id override")
    ap.add_argument("--out", required=True, help="output path ('-' for stdout)")
    # Chaos mode (task T7): --exactly-once selects the chaos render path.
    ap.add_argument("--exactly-once", choices=["0", "1"],
                    help="chaos mode: delivery column (1=exactly-once, 0=at-least-once)")
    ap.add_argument("--ipwb", choices=["true", "false"], default="false",
                    help="chaos mode: ignorePartitionsWhenBatching (default false)")
    ap.add_argument("--watch-cell", action="store_true",
                    help="chaos mode: permit ipwb=true in at-least-once (WATCH cell)")
    args = ap.parse_args()

    with open(args.template) as f:
        doc = json.load(f)

    doc = strip_doc_keys(doc)
    cfg = doc["spec"]["config"]

    chaos_mode = args.exactly_once is not None
    if chaos_mode:
        exactly_once = args.exactly_once == "1"
        ipwb = args.ipwb == "true"
        buffer_count = int(cfg.get("bufferCount", "0"))
        try:
            validate_chaos_config(exactly_once=exactly_once, ipwb=ipwb,
                                  buffer_count=buffer_count,
                                  watch_cell=args.watch_cell)
        except ValueError as e:
            print(str(e), file=sys.stderr)
            return 1
        # Render-time placeholders (distinct from worker-resolved ${env:...}).
        cfg["exactlyOnce"] = cfg["exactlyOnce"].replace(
            "${EXACTLY_ONCE}", "true" if exactly_once else "false")
        cfg["ignorePartitionsWhenBatching"] = cfg["ignorePartitionsWhenBatching"].replace(
            "${IPWB}", "true" if ipwb else "false")
    else:
        # Pair mode: ${CH_TABLE} substitution in topic2TableMap.
        if not args.ch_table:
            print("--ch-table is required in pair mode", file=sys.stderr)
            return 1
        cfg["topic2TableMap"] = cfg["topic2TableMap"].replace("${CH_TABLE}", args.ch_table)

    # Consumer group id: override explicitly so a fresh group means a fresh
    # offset-0 drain even if the connector name is reused. Kafka Connect's sink
    # default is "connect-<connector-name>". Pair mode requires it; chaos mode
    # passes ch-sink-chaos-eo<0|1>.
    if args.group:
        cfg["consumer.override.group.id"] = args.group
    elif not chaos_mode:
        print("--group is required in pair mode", file=sys.stderr)
        return 1

    # Poll-size knob (review F2): the SAME env var run_pair.sh echoes into the
    # runtime map drives the deployed value, so provenance cannot lie. Validate
    # it is a positive integer — a typo here must fail the render, not deploy.
    max_poll = os.environ.get("CFG_MAX_POLL_RECORDS")
    if max_poll:
        if not max_poll.isdigit() or int(max_poll) <= 0:
            print(f"CFG_MAX_POLL_RECORDS must be a positive integer, got {max_poll!r}",
                  file=sys.stderr)
            return 1
        cfg["consumer.override.max.poll.records"] = max_poll

    out = json.dumps(doc, indent=2, sort_keys=False)
    if args.out == "-":
        sys.stdout.write(out + "\n")
    else:
        with open(args.out, "w") as f:
            f.write(out + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
