"""Tests for sync_to_r2.py — the --snapshot / --only-changed baseline.

The dangerous failure here is skipping a file that DID change: R2 would keep
serving stale data and nothing would error. So the cases that matter most are
the ones asserting a file is *not* skipped.
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from sync_to_r2 import BASELINE_NAME, _fingerprint, load_baseline, write_baseline


def _write(path, content=b"x"):
    with open(path, "wb") as f:
        f.write(content)
    return path


def _touch_content(path, content):
    """Rewrite a file so both its size and mtime move, as a real rewrite does."""
    st_before = os.stat(path).st_mtime_ns
    with open(path, "wb") as f:
        f.write(content)
    # Guarantee a distinct mtime even on a coarse-grained filesystem.
    os.utime(path, ns=(st_before + 1_000_000, st_before + 1_000_000))


def test_unchanged_file_matches_baseline(tmp_path):
    d = str(tmp_path)
    p = _write(os.path.join(d, "historical_jobs_2020.parquet"), b"unchanged")

    write_baseline(d)
    baseline = load_baseline(d)

    assert baseline["historical_jobs_2020.parquet"] == _fingerprint(p)


def test_rewritten_file_does_not_match(tmp_path):
    d = str(tmp_path)
    p = _write(os.path.join(d, "current_jobs_2026.parquet"), b"before")

    write_baseline(d)
    baseline = load_baseline(d)
    _touch_content(p, b"after-and-longer")

    assert baseline["current_jobs_2026.parquet"] != _fingerprint(p)


def test_same_size_but_new_mtime_does_not_match(tmp_path):
    """A rewrite that happens to preserve byte count must still be uploaded."""
    d = str(tmp_path)
    p = _write(os.path.join(d, "current_jobs_2025.parquet"), b"aaaa")

    write_baseline(d)
    baseline = load_baseline(d)
    _touch_content(p, b"bbbb")  # identical length, different content

    assert os.path.getsize(p) == baseline["current_jobs_2025.parquet"]["size"]
    assert baseline["current_jobs_2025.parquet"] != _fingerprint(p)


def test_new_file_is_absent_from_baseline(tmp_path):
    """January's brand-new year file has no baseline entry, so it uploads."""
    d = str(tmp_path)
    _write(os.path.join(d, "historical_jobs_2026.parquet"))
    write_baseline(d)
    baseline = load_baseline(d)

    new = _write(os.path.join(d, "historical_jobs_2027.parquet"))
    assert baseline.get("historical_jobs_2027.parquet") is None
    assert baseline.get("historical_jobs_2027.parquet") != _fingerprint(new)


def test_missing_baseline_returns_none(tmp_path, capsys):
    """No baseline must mean 'upload everything', never 'skip everything'."""
    assert load_baseline(str(tmp_path)) is None
    assert "uploading every file" in capsys.readouterr().err


def test_corrupt_baseline_returns_none(tmp_path, capsys):
    d = str(tmp_path)
    with open(os.path.join(d, BASELINE_NAME), "w") as f:
        f.write("{not json")

    assert load_baseline(d) is None
    assert "uploading every file" in capsys.readouterr().err


def test_non_object_baseline_returns_none(tmp_path):
    d = str(tmp_path)
    with open(os.path.join(d, BASELINE_NAME), "w") as f:
        json.dump(["not", "a", "dict"], f)

    assert load_baseline(d) is None


def test_baseline_only_covers_parquet(tmp_path):
    """The baseline file itself must never end up in its own manifest."""
    d = str(tmp_path)
    _write(os.path.join(d, "historical_jobs_2019.parquet"))
    _write(os.path.join(d, "notes.txt"))

    write_baseline(d)
    baseline = load_baseline(d)

    assert set(baseline) == {"historical_jobs_2019.parquet"}
    assert BASELINE_NAME not in baseline
