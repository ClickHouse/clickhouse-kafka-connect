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
# Ported verbatim from spark-clickhouse-connector benchmarks/scripts/ch_common.py,
# then hardened for the staging-service stall class (see get_client note).
import os
import sys

import clickhouse_connect

# Staging-service stall class (pair-4 incident + the historic B-cluster hang
# lesson: "socket timeout + retry"). This shared metrics/target service
# periodically stalls the connection-handshake SELECT (e.g. version()) or an
# in-flight query for tens of seconds. clickhouse_connect's stock timeouts are
# too tight (connect ~10s, read ~10s under the sync HTTP client) and surface as
# a transient OperationalError that once CRASHED the post-export integrity
# verdict and turned a perfect run false-red.
#
# These are CAPTURE-side defaults: every ch_common.get_client caller (capture
# SQL, run-record, export, integrity, truncate, settle) is a persistence /
# verification step where a longer wait + retry is strictly better than a
# fast-fail. The in-cluster poller has its OWN client (poller/ch_insert.py over
# raw requests) and is intentionally NOT affected by this change.
#
# Parameterized via env so an operator can tighten/loosen without a code change.
DEFAULT_CONNECT_TIMEOUT = 60   # was clickhouse_connect default (~10s)
DEFAULT_READ_TIMEOUT = 60      # send/receive; was clickhouse_connect default (~10s)


def require(name: str) -> str:
    if name not in os.environ:
        print(f"ERROR: required env var {name} is not set", file=sys.stderr)
        sys.exit(1)
    return os.environ[name]


def _int_env(name: str, default: int) -> int:
    """Read an int env var, falling back to default on unset/empty/garbage."""
    raw = os.environ.get(name, "")
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def get_client(host_var: str, user_var: str, password_var: str,
               *, port: int = 8443, secure: bool = True):
    # connect_timeout: TCP connect + handshake budget (covers the stalled
    # SELECT version() the driver issues on connect). send_receive_timeout:
    # per-request socket read/write budget. Both raised to ~60s for the
    # staging-service stall class; overridable via CH_CONNECT_TIMEOUT /
    # CH_READ_TIMEOUT.
    #
    # port/secure DEFAULT to the pair's ClickHouse Cloud endpoint (8443 + TLS) so
    # every existing caller is unchanged. The chaos oracle (check_integrity.py
    # --direct) overrides them to reach the in-cluster self-hosted target
    # (plaintext 8123, no TLS — IC-2); it must NOT silently use the Cloud default.
    connect_timeout = _int_env("CH_CONNECT_TIMEOUT", DEFAULT_CONNECT_TIMEOUT)
    read_timeout = _int_env("CH_READ_TIMEOUT", DEFAULT_READ_TIMEOUT)
    return clickhouse_connect.get_client(
        host=require(host_var),
        port=port,
        username=require(user_var),
        password=require(password_var),
        secure=secure,
        connect_timeout=connect_timeout,
        send_receive_timeout=read_timeout,
    )
