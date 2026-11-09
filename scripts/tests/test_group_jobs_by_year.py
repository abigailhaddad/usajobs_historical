"""collect_data.group_jobs_by_year must skip (and count) undated jobs, not file them under 2024."""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import collect_data as cd  # noqa: E402


def job(cn, date):
    return {"usajobsControlNumber": cn, "positionOpenDate": date}


@pytest.mark.parametrize("bad", [None, "", "   ", "not a date", "xxxx-01-01", 20250101, "1850-01-01"])
def test_missing_or_unparseable_date_is_skipped_and_counted(bad):
    skipped = []
    out = cd.group_jobs_by_year([job("1", bad), job("2", "2025-03-04T00:00:00.000")], skipped)
    assert out == {2025: [job("2", "2025-03-04T00:00:00.000")]}
    assert skipped == ["1"]


def test_missing_key_entirely_is_skipped():
    skipped = []
    assert cd.group_jobs_by_year([{"usajobsControlNumber": "9"}], skipped) == {}
    assert skipped == ["9"]


def test_valid_formats_still_group_by_year():
    out = cd.group_jobs_by_year([job("1", "2024-12-31"), job("2", "2025-01-01T00:00:00Z"),
                                 job("3", "2025-06-30T12:00:00.123")])
    assert sorted(out) == [2024, 2025] and len(out[2025]) == 2


def test_skipped_argument_is_optional():
    assert cd.group_jobs_by_year([job("1", None)]) == {}
