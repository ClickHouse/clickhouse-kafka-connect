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
"""Read back this run's integrity verdict and fail the job ONLY on a mismatch.

Ported from spark-clickhouse-connector benchmarks/scripts/check_integrity.py,
then crash-class hardened after the pair-4 incident (see EXIT CODES below).

20_insert_integrity.sql records rows_delivered / rows_expected / unique_delivered
/ unique_expected / duplicate_rows / integrity_ok into perf.metrics as part of
the (gated, atomic) capture step. This step runs AFTER capture + run-record + DWH
export so the evidence is already persisted and exported before we decide the
run's fate. This checker is a REDUNDANT CONFIRMATION LAYER: the capture-computed
integrity_ok (20_insert_integrity.sql) is the AUTHORITATIVE verdict; it is
already on the row. This script merely reads it back.

EXIT CODES (pair-4 fix — distinct, so run_pair.sh can tell a real mismatch from
an infra hiccup that could not verify anything):
  0  ran, verdict OK (integrity_ok == 1).
  1  RAN and MISMATCHED (integrity_ok != 1, or the metrics are missing).
     THE ONLY EXIT CODE THAT MAY FAIL A RUN.
  3  CHECK_ERROR: any infra/connection/query exception — the checker could not
     verify. NOT a data verdict. The pair-4 crash (a transient clickhouse_connect
     OperationalError read-timeout during the connection-handshake SELECT) was
     THIS case, but the old code exited 1 and turned a perfect run false-red.

The client acquisition AND the query are wrapped: on any exception we retry with
backoff (RETRY_BACKOFFS) before conceding exit 3. Stderr states plainly which
case occurred.

Required env: METRICS_CH_HOST, METRICS_CH_USER, METRICS_CH_PASSWORD, RUN_ID
Optional env: CH_INTEGRITY_ATTEMPTS (default 3), CH_INTEGRITY_BACKOFFS
              (default "5,15,30" seconds between attempts).
"""
import dataclasses
import json
import os
import sys
import time

import ch_common
import integrity_math

EXIT_OK = 0
EXIT_MISMATCH = 1
EXIT_CHECK_ERROR = 3

# 3 attempts, sleeping 5s then 15s then 30s between them (the last backoff is
# unused when attempts==len(backoffs)+1, but kept for headroom). The historic
# lesson from the B-cluster hang: socket timeout + retry.
DEFAULT_ATTEMPTS = 3
DEFAULT_BACKOFFS = [5, 15, 30]


def _attempts() -> int:
    raw = os.environ.get("CH_INTEGRITY_ATTEMPTS", "")
    try:
        n = int(raw)
        return n if n >= 1 else DEFAULT_ATTEMPTS
    except (TypeError, ValueError):
        return DEFAULT_ATTEMPTS


def _backoffs() -> list:
    raw = os.environ.get("CH_INTEGRITY_BACKOFFS", "")
    if not raw:
        return DEFAULT_BACKOFFS
    try:
        return [float(x) for x in raw.split(",") if x.strip() != ""] or DEFAULT_BACKOFFS
    except ValueError:
        return DEFAULT_BACKOFFS


def _fetch_metrics(run_id: str) -> dict:
    """Acquire the client and read the integrity metrics. Raises on any infra
    error (connection/query) — the caller handles retries."""
    client = ch_common.get_client("METRICS_CH_HOST", "METRICS_CH_USER", "METRICS_CH_PASSWORD")
    rows = client.query(
        "SELECT metric_name, value FROM perf.metrics "
        "WHERE run_id = {run_id:String} "
        "AND metric_name IN ('integrity_ok', 'rows_delivered', 'rows_expected', "
        "'unique_delivered', 'unique_expected', 'duplicate_rows')",
        parameters={"run_id": run_id},
    ).result_rows
    return {name: value for name, value in rows}


def main() -> None:
    run_id = ch_common.require("RUN_ID")

    attempts = _attempts()
    backoffs = _backoffs()
    m = None
    last_exc = None
    for i in range(attempts):
        try:
            m = _fetch_metrics(run_id)
            break
        except Exception as exc:  # infra/connection/query — could not verify
            last_exc = exc
            remaining = attempts - i - 1
            if remaining > 0:
                sleep_s = backoffs[i] if i < len(backoffs) else backoffs[-1]
                print(
                    f"CHECK_ERROR (attempt {i + 1}/{attempts}) reading integrity "
                    f"for {run_id}: {type(exc).__name__}: {exc} — retrying in "
                    f"{sleep_s}s ({remaining} attempt(s) left)",
                    file=sys.stderr,
                )
                time.sleep(sleep_s)
            else:
                print(
                    f"CHECK_ERROR: could not verify integrity for {run_id} after "
                    f"{attempts} attempt(s): {type(last_exc).__name__}: {last_exc}. "
                    f"This is an INFRA/CONNECTION failure, NOT a data mismatch — "
                    f"the run's fate is UNCHANGED (capture-computed integrity_ok "
                    f"remains authoritative). Exiting {EXIT_CHECK_ERROR} "
                    f"(CHECK_ERROR).",
                    file=sys.stderr,
                )
                sys.exit(EXIT_CHECK_ERROR)

    if "integrity_ok" not in m:
        print(
            f"MISMATCH: no integrity metrics found for run {run_id} "
            f"(the checker RAN and read back an empty verdict — capture SQL 20 "
            f"did not land its row). Exiting {EXIT_MISMATCH}.",
            file=sys.stderr,
        )
        sys.exit(EXIT_MISMATCH)

    delivered = m.get("rows_delivered")
    expected = m.get("rows_expected")
    uniq_delivered = m.get("unique_delivered")
    uniq_expected = m.get("unique_expected")
    dups = m.get("duplicate_rows")
    print(f"integrity for {run_id}: rows delivered={delivered} expected={expected} "
          f"| unique delivered={uniq_delivered} expected={uniq_expected} "
          f"| duplicate_rows={dups} integrity_ok={m['integrity_ok']}")

    if m["integrity_ok"] != 1.0:
        print(
            f"MISMATCH: integrity check FAILED for {run_id} "
            f"(rows delivered={delivered}/expected={expected}, "
            f"unique delivered={uniq_delivered}/expected={uniq_expected}, "
            f"duplicate_rows={dups}). The checker RAN and read a real mismatch. "
            f"Exiting {EXIT_MISMATCH}.",
            file=sys.stderr,
        )
        sys.exit(EXIT_MISMATCH)

    print("integrity OK")
    sys.exit(EXIT_OK)


# --------------------------------------------------------------------------- #
# --direct mode (chaos test #771, IC-6): read the self-hosted chaos TARGET
# directly and classify via integrity_math.chaos_verdict. The perf.metrics
# read-back mode (main() above) is UNTOUCHED. Exit codes preserved:
#   0 PASS | 1 MISMATCH | 3 CHECK_ERROR or UNVERIFIED (reason distinguishes).
#
# Required env: TARGET_CH_HOST, TARGET_CH_USER, TARGET_CH_PASSWORD, CH_DATABASE,
#   CH_TABLE, ROWS_EXPECTED, SOURCE_UNIQUE_EXPECTED. Optional: DLQ_DEPTH
#   (default 0), FAULT_OBSERVED (0|1, default 0), plus the shared
#   CH_INTEGRITY_ATTEMPTS / CH_INTEGRITY_BACKOFFS retry knobs.
# --------------------------------------------------------------------------- #
DIRECT_COUNT_QUERY = (
    "SELECT count(), uniqExact(WatchID) FROM {db}.{table} "
    "SETTINGS select_sequential_consistency=1"
)


def _read_target_direct():
    """One direct read of the chaos target: (rows_delivered, unique_delivered).

    Uses select_sequential_consistency=1 (spec §5 replica-read caveat). Raises on
    any infra/connection/query error — the caller applies the retry envelope.

    The chaos target is the in-cluster self-hosted CH (IC-2): plaintext, port 8123,
    no TLS. TARGET_CH_PORT / TARGET_CH_SECURE are threaded so this read reaches it
    rather than the pair's Cloud 8443/TLS default baked into ch_common.get_client.
    Defaults (8443 + secure) match get_client so an unset env is harmless."""
    try:
        port = int(os.environ.get("TARGET_CH_PORT", "") or 8443)
    except ValueError:
        port = 8443
    secure = os.environ.get("TARGET_CH_SECURE", "true").strip().lower() in (
        "1", "true", "yes")
    client = ch_common.get_client(
        "TARGET_CH_HOST", "TARGET_CH_USER", "TARGET_CH_PASSWORD",
        port=port, secure=secure)
    db = ch_common.require("CH_DATABASE")
    table = ch_common.require("CH_TABLE")
    rows = client.query(DIRECT_COUNT_QUERY.format(db=db, table=table)).result_rows
    if not rows:
        raise RuntimeError("target count query returned no rows")
    return float(rows[0][0]), float(rows[0][1])


def _read_with_retry(label: str):
    """Run _read_target_direct through the shared retry/backoff envelope. On
    persistent failure, concede EXIT_CHECK_ERROR (never a data verdict)."""
    attempts = _attempts()
    backoffs = _backoffs()
    last_exc = None
    for i in range(attempts):
        try:
            return _read_target_direct()
        except Exception as exc:  # infra/connection/query — could not verify
            last_exc = exc
            remaining = attempts - i - 1
            if remaining > 0:
                sleep_s = backoffs[i] if i < len(backoffs) else backoffs[-1]
                print(
                    f"CHECK_ERROR (attempt {i + 1}/{attempts}) {label}: "
                    f"{type(exc).__name__}: {exc} — retrying in {sleep_s}s "
                    f"({remaining} attempt(s) left)",
                    file=sys.stderr,
                )
                time.sleep(sleep_s)
            else:
                print(
                    f"CHECK_ERROR: could not {label} after {attempts} "
                    f"attempt(s): {type(last_exc).__name__}: {last_exc}. This is "
                    f"an INFRA/CONNECTION failure (or a replica not yet caught "
                    f"up), NOT a data mismatch. Exiting {EXIT_CHECK_ERROR} "
                    f"(CHECK_ERROR).",
                    file=sys.stderr,
                )
                sys.exit(EXIT_CHECK_ERROR)


def main_direct() -> None:
    rows_expected = float(ch_common.require("ROWS_EXPECTED"))
    unique_expected = float(ch_common.require("SOURCE_UNIQUE_EXPECTED"))
    dlq_depth = float(os.environ.get("DLQ_DEPTH", "0") or "0")
    fault_observed = os.environ.get("FAULT_OBSERVED", "0").strip() == "1"

    rows_delivered, unique_delivered = _read_with_retry("read target integrity")

    # Replica-consistency guard (spec §5): a count() below expectation right
    # after a node kill/rejoin can be a lagging replica, not real loss. Re-read;
    # if the count is not stable across reads it is a TRANSIENT undercount =>
    # CHECK_ERROR, never MISMATCH.
    if rows_delivered < rows_expected:
        confirm_delivered, confirm_unique = _read_with_retry(
            "re-read target on suspected undercount")
        if confirm_delivered != rows_delivered:
            print(
                f"CHECK_ERROR: target count is not stable across reads "
                f"({rows_delivered} then {confirm_delivered}) — a lagging-replica "
                f"transient undercount, NOT loss. Exiting {EXIT_CHECK_ERROR} "
                f"(CHECK_ERROR).",
                file=sys.stderr,
            )
            sys.exit(EXIT_CHECK_ERROR)
        unique_delivered = confirm_unique

    verdict = integrity_math.chaos_verdict(
        rows_delivered=rows_delivered,
        rows_expected=rows_expected,
        unique_delivered=unique_delivered,
        unique_expected=unique_expected,
        dlq_depth=dlq_depth,
        fault_observed=fault_observed,
    )

    # The IC-6 JSON on stdout, consumed by the artifact writer (T6/T11).
    print(json.dumps(dataclasses.asdict(verdict)))

    if verdict.verdict == integrity_math.MISMATCH:
        print(f"MISMATCH (direct): {verdict.reason}. Exiting {EXIT_MISMATCH}.",
              file=sys.stderr)
        sys.exit(EXIT_MISMATCH)
    if verdict.verdict == integrity_math.PASS:
        print("integrity OK (direct)", file=sys.stderr)
        sys.exit(EXIT_OK)
    # UNVERIFIED_FAULT_NOT_OBSERVED — clean but unattributable, never PASS.
    print(f"UNVERIFIED (direct): {verdict.reason}. Exiting {EXIT_CHECK_ERROR}.",
          file=sys.stderr)
    sys.exit(EXIT_CHECK_ERROR)


if __name__ == "__main__":
    if "--direct" in sys.argv[1:]:
        main_direct()
    else:
        main()
