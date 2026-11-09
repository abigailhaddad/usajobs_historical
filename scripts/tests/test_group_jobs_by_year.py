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


def test_undated_limit_is_max_of_floor_and_one_percent():
    assert cd.undated_limit(0) == 5
    assert cd.undated_limit(100) == 5
    assert cd.undated_limit(500) == 5
    assert cd.undated_limit(10_000) == 100


def _run_fetch(monkeypatch, tmp_path, n_dated, n_undated):
    jobs = ([{"usajobsControlNumber": f"d{i}", "positionOpenDate": "2025-03-04T00:00:00"}
             for i in range(n_dated)] +
            [{"usajobsControlNumber": f"u{i}", "positionOpenDate": None}
             for i in range(n_undated)])
    monkeypatch.setattr(cd, "fetch_jobs_for_date", lambda d, s=None: (jobs, True, True))
    monkeypatch.setattr(cd.time, "sleep", lambda s: None)
    return cd.fetch_jobs("2025-03-04", "2025-03-04", data_dir=str(tmp_path))


def test_few_undated_in_a_big_fetch_is_tolerated_and_logged(monkeypatch, tmp_path, capsys):
    # 5 undated of 1005 fetched: at the floor of 5, so not over the limit
    _run_fetch(monkeypatch, tmp_path, 1000, 5)
    out = capsys.readouterr().out
    assert "u0" in out and "fetch window" in out


def test_more_undated_than_rate_threshold_fails_after_saving_dated(monkeypatch, tmp_path):
    # 6 undated of 106 fetched: limit is max(5, 1.06) = 5, 6 > 5
    with pytest.raises(RuntimeError, match="fetch window"):
        _run_fetch(monkeypatch, tmp_path, 100, 6)
    assert (tmp_path / "historical_jobs_2025.parquet").exists()


def test_rate_threshold_scales_up_with_big_fetch(monkeypatch, tmp_path):
    # 12 undated of 1512 fetched: 1% is 15.12, so 12 is tolerated (a fixed 5 would fail)
    _run_fetch(monkeypatch, tmp_path, 1500, 12)


def test_undated_over_rate_in_big_fetch_fails(monkeypatch, tmp_path):
    # 16 undated of 1516 fetched: 1% is 15.16, 16 > 15.16
    with pytest.raises(RuntimeError):
        _run_fetch(monkeypatch, tmp_path, 1500, 16)
