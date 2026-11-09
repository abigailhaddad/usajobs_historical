"""_fetch_series_mapping must fail loudly rather than return an empty mapping.

An empty mapping publishes web data with bare series codes and no names.
"""
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


def test_persistent_failure_raises(monkeypatch):
    monkeypatch.setattr(pwd.requests, "get", lambda *a, **k: _Resp(status=503))
    monkeypatch.setattr(pwd.time, "sleep", lambda s: None)
    with pytest.raises(RuntimeError, match="series"):
        pwd._fetch_series_mapping()


def test_empty_codelist_raises(monkeypatch):
    monkeypatch.setattr(pwd.requests, "get", lambda *a, **k: _Resp({"CodeList": []}))
    monkeypatch.setattr(pwd.time, "sleep", lambda s: None)
    with pytest.raises(RuntimeError):
        pwd._fetch_series_mapping()
