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
"""perf.metrics inserter tests: row building, tier ownership, rollback discipline,
credential hygiene — all against a fake requests module (no live ClickHouse)."""
import pytest

import ch_insert
import metric_names as mn


class FakeResponse:
    def __init__(self, status_code=200, text="OK"):
        self.status_code = status_code
        self.text = text


class FakeRequests:
    """Records POSTs; can be told to fail INSERTs to exercise rollback."""

    def __init__(self, fail_on_insert=False):
        self.calls = []
        self.fail_on_insert = fail_on_insert

    def post(self, url, params=None, headers=None, data=None, timeout=None):
        body = data.decode("utf-8") if isinstance(data, (bytes, bytearray)) else data
        self.calls.append({"url": url, "params": params, "headers": headers,
                           "body": body})
        if self.fail_on_insert and body.strip().upper().startswith("INSERT"):
            return FakeResponse(500, "boom: insert failed")
        return FakeResponse(200, "")


def cfg():
    c = ch_insert.CHConfig.__new__(ch_insert.CHConfig)
    c.host = "target.example.com"
    c.user = "bench"
    c._password = "s3cret-should-never-appear"
    c.port = 8443
    c.secure = True
    c.database = "perf"
    return c


def scalars_tier1():
    return {
        mn.DRAIN_ROWS_PER_SEC: 30.0,
        mn.DRAIN_SECONDS: 30.0,
        mn.PARTITION_SKEW: None,       # None -> skipped
        mn.REBALANCE_COUNT: 0.0,
        mn.LAG_REACHED_ZERO: 1.0,
    }


def test_build_rows_skips_none():
    rows = ch_insert.build_rows("rid", 1, scalars_tier1())
    names = {r[1] for r in rows}
    assert mn.PARTITION_SKEW not in names  # None dropped
    assert mn.DRAIN_ROWS_PER_SEC in names
    # units come from the pinned table
    for run_id, name, unit, value in rows:
        assert run_id == "rid"
        assert unit == mn.unit_for(name)


def test_build_rows_rejects_wrong_tier_headline():
    # tier-0 headline onto a tier-1 run -> PROHIBITED (contract §1.2)
    bad = {mn.NULL_DRAIN_ROWS_PER_SEC: 10.0}
    with pytest.raises(AssertionError):
        ch_insert.build_rows("rid", 1, bad)


def test_insert_happy_path_predeletes_then_inserts():
    fr = FakeRequests()
    res = ch_insert.insert_metrics("rid", 1, scalars_tier1(), cfg=cfg(),
                                   requests_mod=fr)
    bodies = [c["body"].strip().upper() for c in fr.calls]
    # first a DELETE (idempotent pre-clean), then the INSERT
    assert bodies[0].startswith("ALTER TABLE") and "DELETE" in bodies[0]
    assert bodies[1].startswith("INSERT")
    assert res["inserted"] == len([v for v in scalars_tier1().values() if v is not None])


def test_insert_rollback_on_failure():
    fr = FakeRequests(fail_on_insert=True)
    with pytest.raises(RuntimeError):
        ch_insert.insert_metrics("rid", 1, scalars_tier1(), cfg=cfg(),
                                 requests_mod=fr)
    bodies = [c["body"].strip().upper() for c in fr.calls]
    # sequence: pre-delete, insert (fails), rollback-delete
    assert bodies[0].startswith("ALTER TABLE")   # pre-clean
    assert bodies[1].startswith("INSERT")         # the failing insert
    assert bodies[2].startswith("ALTER TABLE")    # rollback delete
    assert "DELETE" in bodies[2]


def test_credentials_only_in_headers_never_in_url_or_body():
    fr = FakeRequests()
    ch_insert.insert_metrics("rid", 1, scalars_tier1(), cfg=cfg(), requests_mod=fr)
    for c in fr.calls:
        assert "s3cret-should-never-appear" not in c["url"]
        assert "s3cret-should-never-appear" not in (c["body"] or "")
        # password IS in headers (that's the only place it may appear)
        assert c["headers"]["X-ClickHouse-Key"] == "s3cret-should-never-appear"


def test_config_repr_redacts_password():
    assert "s3cret-should-never-appear" not in repr(cfg())
    assert "redacted" in repr(cfg())


def test_empty_scalars_no_calls():
    fr = FakeRequests()
    res = ch_insert.insert_metrics("rid", 1, {mn.PARTITION_SKEW: None},
                                   cfg=cfg(), requests_mod=fr)
    assert res["inserted"] == 0
    assert fr.calls == []


def test_sql_string_escaping():
    # a run_id with a quote must not break the SQL literal
    fr = FakeRequests()
    ch_insert.insert_metrics("ri'd", 1, {mn.DRAIN_SECONDS: 1.0},
                             cfg=cfg(), requests_mod=fr)
    insert_body = [c["body"] for c in fr.calls if c["body"].strip().upper().startswith("INSERT")][0]
    assert "ri\\'d" in insert_body
