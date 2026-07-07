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
"""perf.metrics inserter — atomic, rollback-on-partial-failure (contract §2).

Credentials come STRICTLY from env (TARGET_CH_HOST / TARGET_CH_USER /
TARGET_CH_PASSWORD) and are NEVER logged and NEVER written to a file. Inserts
go over the ClickHouse HTTP(S) interface with the credentials in request headers
(not in the URL, so they never appear in access logs or exception strings).

Rollback discipline (mirrors the Spark pipeline): a single batch INSERT is
atomic on the ClickHouse side; but to guarantee no orphaned rows if the process
is interrupted between building and confirming the insert, the inserter first
DELETEs any pre-existing rows for (run_id, <these metric names>) — so a retry is
idempotent — then inserts. On any insert failure it issues a delete-by-run_id
(scoped to the metric names it was inserting) to roll back a partial landing.

The poller does NOT write perf.runs rows (contract §1.2 / task brief); it only
lands perf.metrics keyed by a run_id the orchestrator already created.
"""
import os
import sys
from typing import Dict, List, Optional, Tuple

import metric_names as mn


class CHConfig:
    """Connection config sourced only from env. __repr__ never leaks the pw."""

    def __init__(self):
        self.host = _require("TARGET_CH_HOST")
        self.user = os.environ.get("TARGET_CH_USER", "default")
        self._password = _require("TARGET_CH_PASSWORD")
        self.port = int(os.environ.get("TARGET_CH_PORT", "8443"))
        self.secure = os.environ.get("TARGET_CH_SECURE", "1") != "0"
        self.database = os.environ.get("PERF_CH_DATABASE", "perf")

    @property
    def base_url(self) -> str:
        scheme = "https" if self.secure else "http"
        return f"{scheme}://{self.host}:{self.port}/"

    def headers(self) -> Dict[str, str]:
        # Credentials in headers, never in the URL/query string.
        return {
            "X-ClickHouse-User": self.user,
            "X-ClickHouse-Key": self._password,
        }

    def __repr__(self):
        return (f"CHConfig(host={self.host!r}, user={self.user!r}, "
                f"port={self.port}, secure={self.secure}, "
                f"database={self.database!r}, password=<redacted>)")


def _require(name: str) -> str:
    if name not in os.environ or not os.environ[name]:
        print(f"ERROR: required env var {name} is not set", file=sys.stderr)
        sys.exit(1)
    return os.environ[name]


def _ch_escape(s: str) -> str:
    """Escape a string for a single-quoted ClickHouse SQL literal."""
    return s.replace("\\", "\\\\").replace("'", "\\'")


def build_rows(
    run_id: str, tier: int, scalars: Dict[str, Optional[float]]
) -> List[Tuple[str, str, str, float]]:
    """(run_id, metric_name, unit, value) rows for perf.metrics.

    * None values are SKIPPED — a metric whose source was unavailable is not a
      zero; it simply does not produce a row.
    * Every emitted name is validated against the tier's allowed set
      (contract §1.2): tagging a tier-0 name onto a tier-1 run_id (or vice
      versa) raises rather than lands a mixed series.
    """
    allowed = mn.METRICS_BY_TIER[tier]
    rows = []
    for name, value in scalars.items():
        if value is None:
            continue
        if name not in allowed:
            raise AssertionError(
                f"metric {name!r} is not allowed for tier {tier} run_id "
                f"{run_id!r} (contract §1.2); refusing to land a mixed series")
        unit = mn.unit_for(name)
        rows.append((run_id, name, unit, float(value)))
    return rows


def _rows_to_values_sql(rows: List[Tuple[str, str, str, float]]) -> str:
    parts = []
    for run_id, name, unit, value in rows:
        parts.append(
            f"('{_ch_escape(run_id)}', '{_ch_escape(name)}', "
            f"'{_ch_escape(unit)}', {value!r})")
    return ", ".join(parts)


def _exec(requests_mod, cfg: CHConfig, sql: str, timeout: float = 60.0):
    """POST a statement to the CH HTTP interface. Raises on non-2xx.

    The SQL body may contain the run_id/metric strings but NEVER the password
    (which lives only in headers). Exceptions raised by requests include the URL
    but not the headers, so the password does not leak into tracebacks."""
    r = requests_mod.post(
        cfg.base_url,
        params={"database": cfg.database},
        headers=cfg.headers(),
        data=sql.encode("utf-8"),
        timeout=timeout,
    )
    if r.status_code >= 300:
        # r.text is the CH error message; it echoes the SQL but not credentials.
        raise RuntimeError(
            f"ClickHouse HTTP {r.status_code}: {r.text[:2000]}")
    return r


def _delete_by_run_id(requests_mod, cfg: CHConfig, run_id: str,
                      names: List[str]) -> None:
    """DELETE the given metric names for this run_id (idempotency + rollback).

    Scoped to the names we manage so a concurrent CH-side capture writing OTHER
    metric names for the same run_id is never touched. Uses lightweight DELETE
    (ALTER TABLE ... DELETE) which is fine for the tiny per-run row count."""
    if not names:
        return
    name_list = ", ".join(f"'{_ch_escape(n)}'" for n in sorted(set(names)))
    sql = (f"ALTER TABLE {cfg.database}.metrics DELETE "
           f"WHERE run_id = '{_ch_escape(run_id)}' "
           f"AND metric_name IN ({name_list}) SETTINGS mutations_sync = 1")
    _exec(requests_mod, cfg, sql)


def insert_metrics(
    run_id: str,
    tier: int,
    scalars: Dict[str, Optional[float]],
    cfg: Optional[CHConfig] = None,
    requests_mod=None,
) -> Dict[str, object]:
    """Land the scalars into perf.metrics atomically with rollback.

    Steps:
      1. Build rows (skips None, validates tier ownership).
      2. Pre-delete any existing rows for (run_id, these names) -> idempotent retry.
      3. Single batch INSERT.
      4. On INSERT failure: delete-by-run_id (scoped to these names) to roll
         back any partial landing, then re-raise.

    Returns {"inserted": <n>, "names": [...]}. Never logs credentials.
    """
    if requests_mod is None:
        import requests as requests_mod  # noqa: F811
    if cfg is None:
        cfg = CHConfig()

    rows = build_rows(run_id, tier, scalars)
    names = [r[1] for r in rows]
    if not rows:
        return {"inserted": 0, "names": []}

    # 2. idempotent pre-clean (also the rollback target).
    _delete_by_run_id(requests_mod, cfg, run_id, names)

    # 3. batch insert.
    insert_sql = (
        f"INSERT INTO {cfg.database}.metrics "
        f"(run_id, metric_name, unit, value) VALUES "
        + _rows_to_values_sql(rows))
    try:
        _exec(requests_mod, cfg, insert_sql)
    except Exception:
        # 4. rollback: remove any partial landing for the names we own.
        try:
            _delete_by_run_id(requests_mod, cfg, run_id, names)
        except Exception as rollback_err:
            print(f"WARNING: rollback delete failed after insert error: "
                  f"{rollback_err}", file=sys.stderr)
        raise

    return {"inserted": len(rows), "names": names}
