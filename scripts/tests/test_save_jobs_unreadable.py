"""save_jobs_to_parquet must never replace an existing file with only the new rows."""
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import collect_current_data as ccd  # noqa: E402


def test_unreadable_existing_file_is_left_alone(tmp_path):
    path = str(tmp_path / "current_jobs_2026.parquet")
    garbage = b"PAR1 not really a parquet file"
    with open(path, "wb") as f:
        f.write(garbage)
    with pytest.raises(Exception):
        ccd.save_jobs_to_parquet([{"usajobsControlNumber": "1", "positionTitle": "x"}], path)
    with open(path, "rb") as f:
        assert f.read() == garbage


def test_normal_merge_still_adds_rows(tmp_path):
    path = str(tmp_path / "current_jobs_2026.parquet")
    ccd.save_jobs_to_parquet([{"usajobsControlNumber": "1", "positionTitle": "a"}], path)
    ccd.save_jobs_to_parquet([{"usajobsControlNumber": "2", "positionTitle": "b"}], path)
    df = pd.read_parquet(path)
    assert sorted(df["usajobs_control_number"]) == ["1", "2"]


def test_bad_year_does_not_stop_later_years(tmp_path):
    bad = tmp_path / "current_jobs_2025.parquet"
    garbage = b"PAR1 not really a parquet file"
    bad.write_bytes(garbage)
    jobs = {
        2025: [{"usajobsControlNumber": "1", "positionTitle": "x"}],
        2026: [{"usajobsControlNumber": "2", "positionTitle": "y"}],
    }
    total, failed = ccd.process_years(jobs, str(tmp_path))
    assert failed == [2025]
    assert total == 1
    assert bad.read_bytes() == garbage
    assert list(pd.read_parquet(tmp_path / "current_jobs_2026.parquet")["usajobs_control_number"]) == ["2"]
