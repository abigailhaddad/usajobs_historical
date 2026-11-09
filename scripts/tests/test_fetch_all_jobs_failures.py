"""A series whose page fails after retries must be reported, not returned as complete."""
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import collect_current_data as ccd  # noqa: E402


def _page(ids, total):
    return {"SearchResult": {"SearchResultCountAll": total,
                             "SearchResultItems": [{"id": i} for i in ids]}}


def _patch_flatten(monkeypatch):
    monkeypatch.setattr(ccd, "flatten_current_job",
                        lambda item, a, h: {"usajobsControlNumber": item["id"],
                                            "positionOpenDate": "2026-05-01T00:00:00"})
    monkeypatch.setattr(ccd.time, "sleep", lambda s: None)


def test_failed_page_is_recorded_and_fetched_rows_kept(monkeypatch):
    _patch_flatten(monkeypatch)
    pages = {1: _page(["1", "2"], 4), 2: None}
    monkeypatch.setattr(ccd, "fetch_jobs_page", lambda p, h, page: pages[page])
    failures = []
    raw, flat = ccd.fetch_all_jobs({"JobCategoryCode": "0343"}, {}, {}, {}, failures=failures)
    assert [j["usajobsControlNumber"] for j in flat] == ["1", "2"]
    assert failures == [{"series": "0343", "page": 2, "fetched": 2, "total": 4}]


def test_complete_series_records_no_failure(monkeypatch):
    _patch_flatten(monkeypatch)
    monkeypatch.setattr(ccd, "fetch_jobs_page", lambda p, h, page: _page(["1", "2"], 2))
    failures = []
    _, flat = ccd.fetch_all_jobs({"JobCategoryCode": "0343"}, {}, {}, {}, failures=failures)
    assert len(flat) == 2 and failures == []


def test_main_saves_partial_rows_and_reports_failed_series(monkeypatch, tmp_path):
    _patch_flatten(monkeypatch)
    monkeypatch.setattr(ccd, "get_api_headers", lambda: {})
    monkeypatch.setattr(ccd, "fetch_position_offering_types", lambda: {})
    monkeypatch.setattr(ccd, "fetch_hiring_paths", lambda: {})
    monkeypatch.setattr(ccd, "fetch_occupational_series",
                        lambda: [{"code": "0343", "name": "A"}, {"code": "0301", "name": "B"}])
    pages = {"0301": {1: _page(["1", "2"], 4), 2: None},
             "0343": {1: _page(["3"], 1)}}
    monkeypatch.setattr(ccd, "fetch_jobs_page",
                        lambda p, h, page: pages[p["JobCategoryCode"]].get(page))
    monkeypatch.setattr(sys, "argv", ["x", "--data-dir", str(tmp_path)])
    ccd.main()
    assert [f["series"] for f in ccd.FAILED_SERIES] == ["0301"]
    df = pd.read_parquet(tmp_path / "current_jobs_2026.parquet")
    assert sorted(df["usajobs_control_number"]) == ["1", "2", "3"]


def test_report_failed_series_writes_marker(tmp_path):
    ccd.FAILED_SERIES.clear()
    assert ccd.report_failed_series(str(tmp_path)) is None
    ccd.FAILED_SERIES.append({"series": "0301", "page": 2, "fetched": 2, "total": 4})
    path = ccd.report_failed_series(str(tmp_path))
    ccd.FAILED_SERIES.clear()
    text = open(path).read()
    assert "0301" in text and "page 2" in text and "of 4 reported" in text
