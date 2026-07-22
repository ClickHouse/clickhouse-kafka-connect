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
"""ch_common.get_client timeout hardening (pair-4 / B-cluster-hang lesson).

The staging metrics/target service periodically stalls the handshake SELECT and
in-flight queries for tens of seconds. clickhouse_connect's stock ~10s timeouts
surface as transient OperationalErrors. get_client now passes connect_timeout /
send_receive_timeout raised to ~60s, overridable via CH_CONNECT_TIMEOUT /
CH_READ_TIMEOUT. These tests stub clickhouse_connect.get_client to capture the
kwargs (no network).

Run: python3 -m pytest benchmarks/e2e/capture/tests/test_ch_common_timeouts.py -v
"""
import importlib
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _load_ch_common(monkeypatch):
    import clickhouse_connect

    captured = {}

    def _fake_get_client(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(clickhouse_connect, "get_client", _fake_get_client)

    import ch_common
    importlib.reload(ch_common)
    return ch_common, captured


@pytest.fixture(autouse=True)
def _creds(monkeypatch):
    monkeypatch.setenv("METRICS_CH_HOST", "h")
    monkeypatch.setenv("METRICS_CH_USER", "u")
    monkeypatch.setenv("METRICS_CH_PASSWORD", "p")


def test_default_timeouts_are_60s(monkeypatch):
    monkeypatch.delenv("CH_CONNECT_TIMEOUT", raising=False)
    monkeypatch.delenv("CH_READ_TIMEOUT", raising=False)
    ch_common, captured = _load_ch_common(monkeypatch)
    ch_common.get_client("METRICS_CH_HOST", "METRICS_CH_USER", "METRICS_CH_PASSWORD")
    assert captured["connect_timeout"] == 60
    assert captured["send_receive_timeout"] == 60
    # existing kwargs preserved
    assert captured["secure"] is True
    assert captured["port"] == 8443


def test_env_overrides_honored(monkeypatch):
    monkeypatch.setenv("CH_CONNECT_TIMEOUT", "120")
    monkeypatch.setenv("CH_READ_TIMEOUT", "90")
    ch_common, captured = _load_ch_common(monkeypatch)
    ch_common.get_client("METRICS_CH_HOST", "METRICS_CH_USER", "METRICS_CH_PASSWORD")
    assert captured["connect_timeout"] == 120
    assert captured["send_receive_timeout"] == 90


@pytest.mark.parametrize("bad", ["", "abc", "12.5"])
def test_garbage_env_falls_back_to_default(monkeypatch, bad):
    monkeypatch.setenv("CH_CONNECT_TIMEOUT", bad)
    monkeypatch.setenv("CH_READ_TIMEOUT", bad)
    ch_common, captured = _load_ch_common(monkeypatch)
    ch_common.get_client("METRICS_CH_HOST", "METRICS_CH_USER", "METRICS_CH_PASSWORD")
    assert captured["connect_timeout"] == 60
    assert captured["send_receive_timeout"] == 60


def test_port_and_secure_default_to_cloud(monkeypatch):
    """Every existing (pair) caller passes no port/secure and MUST keep the Cloud
    endpoint (8443 + TLS) — the #771 change is additive."""
    ch_common, captured = _load_ch_common(monkeypatch)
    ch_common.get_client("METRICS_CH_HOST", "METRICS_CH_USER", "METRICS_CH_PASSWORD")
    assert captured["port"] == 8443
    assert captured["secure"] is True


def test_port_and_secure_overridable_for_self_hosted(monkeypatch):
    """The chaos oracle reaches the in-cluster self-hosted CH (plaintext 8123, no
    TLS — IC-2) by overriding port/secure; get_client must honor them."""
    ch_common, captured = _load_ch_common(monkeypatch)
    ch_common.get_client("METRICS_CH_HOST", "METRICS_CH_USER", "METRICS_CH_PASSWORD",
                         port=8123, secure=False)
    assert captured["port"] == 8123
    assert captured["secure"] is False
    # the timeout hardening still applies on the self-hosted path
    assert captured["connect_timeout"] == 60


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
