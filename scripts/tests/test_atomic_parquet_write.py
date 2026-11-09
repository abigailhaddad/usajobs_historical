"""A failed write must leave the existing parquet byte-identical and no temp file."""
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from parquet_utils import write_parquet_atomic  # noqa: E402


def test_replaces_file_and_leaves_no_temp(tmp_path):
    path = str(tmp_path / "x.parquet")
    write_parquet_atomic(pd.DataFrame({"a": [1]}), path)
    write_parquet_atomic(pd.DataFrame({"a": [1, 2]}), path, compression="zstd")
    assert list(pd.read_parquet(path)["a"]) == [1, 2]
    assert os.listdir(tmp_path) == ["x.parquet"]


def test_failed_write_leaves_original_untouched(tmp_path, monkeypatch):
    path = str(tmp_path / "x.parquet")
    write_parquet_atomic(pd.DataFrame({"a": [1]}), path)
    before = open(path, "rb").read()

    def boom(self, tmp, *a, **k):
        open(tmp, "wb").write(b"half a file")
        raise OSError("disk full")
    monkeypatch.setattr(pd.DataFrame, "to_parquet", boom)
    with pytest.raises(OSError):
        write_parquet_atomic(pd.DataFrame({"a": [9]}), path)
    assert open(path, "rb").read() == before
    assert os.listdir(tmp_path) == ["x.parquet"]


def test_writers_use_the_helper():
    here = os.path.join(os.path.dirname(__file__), '..')
    for name in ("collect_data.py", "repoll_status.py"):
        src = open(os.path.join(here, name)).read()
        assert ".to_parquet(" not in src, f"{name} writes a data file directly again"
        assert "write_parquet_atomic" in src
