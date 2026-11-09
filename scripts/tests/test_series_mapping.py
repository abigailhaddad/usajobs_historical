"""_fetch_series_mapping: retry, then fall back to the saved snapshot, and raise
only when there is no snapshot. It must never return an empty mapping (that
publishes web data with bare series codes and no names).
"""
import json
import os
import sys

import pytest
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import prep_web_data as pwd  # noqa: E402


class _Resp:
    def __init__(self, payload=None, status=200):
        self._payload, self.status = payload, status

    def raise_for_status(self):
        if self.status >= 400:
            raise requests.HTTPError(f"{self.status}")

    def json(self):
        return self._payload


GOOD = {"CodeList": [{"ValidValue": [
    {"Code": "0343", "Value": "MANAGEMENT AND PROGRAM ANALYSIS", "IsDisabled": "No"},
    {"Code": "9999", "Value": "OLD SERIES", "IsDisabled": "Yes"},
]}]}


def test_good_response(monkeypatch):
    monkeypatch.setattr(pwd.requests, "get", lambda *a, **k: _Resp(GOOD))
    m = pwd._fetch_series_mapping()
    assert m["0343"] == "Management And Program Analysis" and m["343"] == m["0343"]
    assert "9999" not in m


def test_transient_failure_is_retried(monkeypatch):
    calls = []

    def fake_get(*a, **k):
        calls.append(1)
        return _Resp(status=503) if len(calls) < 3 else _Resp(GOOD)
    monkeypatch.setattr(pwd.requests, "get", fake_get)
    monkeypatch.setattr(pwd.time, "sleep", lambda s: None)
    assert pwd._fetch_series_mapping()["0343"]
    assert len(calls) == 3


@pytest.fixture
def no_fallback(monkeypatch, tmp_path):
    monkeypatch.setattr(pwd, "FALLBACK_SERIES_PATH", str(tmp_path / "missing.json"))


def _write_fallback(monkeypatch, tmp_path, payload):
    p = tmp_path / "fallback.json"
    p.write_text(json.dumps(payload))
    monkeypatch.setattr(pwd, "FALLBACK_SERIES_PATH", str(p))


def _down(monkeypatch, exc=None):
    def fake_get(*a, **k):
        if exc:
            raise exc
        return _Resp(status=503)
    monkeypatch.setattr(pwd.requests, "get", fake_get)
    monkeypatch.setattr(pwd.time, "sleep", lambda s: None)


def test_connection_error_with_previous_mapping_uses_it_and_warns(monkeypatch, tmp_path, capsys):
    _write_fallback(monkeypatch, tmp_path, {"0343": "MANAGEMENT AND PROGRAM ANALYSIS"})
    _down(monkeypatch, requests.ConnectionError("boom"))
    m = pwd._fetch_series_mapping()
    assert m == {"0343": "Management And Program Analysis"}
    out = capsys.readouterr().out
    assert "::warning::" in out and "snapshot" in out
    assert pwd.SERIES_MAPPING_STATUS["source"] == "fallback_snapshot"
    assert "boom" in pwd.SERIES_MAPPING_STATUS["warning"]


def test_connection_error_without_previous_mapping_raises(monkeypatch, no_fallback):
    _down(monkeypatch, requests.ConnectionError("boom"))
    with pytest.raises(RuntimeError, match="no previous mapping"):
        pwd._fetch_series_mapping()


def test_empty_fallback_file_counts_as_no_fallback(monkeypatch, tmp_path):
    _write_fallback(monkeypatch, tmp_path, {})
    _down(monkeypatch)
    with pytest.raises(RuntimeError):
        pwd._fetch_series_mapping()


def test_live_success_resets_status(monkeypatch):
    monkeypatch.setattr(pwd.requests, "get", lambda *a, **k: _Resp(GOOD))
    pwd.SERIES_MAPPING_STATUS.update(source="fallback_snapshot", warning="x")
    pwd._fetch_series_mapping()
    assert pwd.SERIES_MAPPING_STATUS == {"source": "live", "warning": None}


def test_checked_in_snapshot_is_usable():
    m = pwd._load_fallback_series_mapping()
    assert len(m) > 500 and m["0343"]


def test_persistent_failure_raises(monkeypatch, no_fallback):
    monkeypatch.setattr(pwd.requests, "get", lambda *a, **k: _Resp(status=503))
    monkeypatch.setattr(pwd.time, "sleep", lambda s: None)
    with pytest.raises(RuntimeError, match="series"):
        pwd._fetch_series_mapping()


def test_empty_codelist_raises(monkeypatch, no_fallback):
    monkeypatch.setattr(pwd.requests, "get", lambda *a, **k: _Resp({"CodeList": []}))
    monkeypatch.setattr(pwd.time, "sleep", lambda s: None)
    with pytest.raises(RuntimeError):
        pwd._fetch_series_mapping()
