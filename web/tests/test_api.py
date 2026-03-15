"""Tests for USAJobs web API endpoints."""
import http.server
import io
import json
import os
import sys

import pytest

# Ensure the web package is importable
WEB_DIR = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, WEB_DIR)

from api import jobs as jobs_mod
from api import aggregate as agg_mod


# ---------------------------------------------------------------------------
# Helper: invoke a handler's do_GET and capture the JSON response
# ---------------------------------------------------------------------------

def _invoke_handler(handler_class, path):
    """Create a handler instance and call do_GET, returning (status_code, body_dict)."""
    instance = object.__new__(handler_class)
    instance.path = path
    instance.requestline = f"GET {path} HTTP/1.1"
    instance.request_version = "HTTP/1.1"
    instance.command = "GET"
    instance.headers = {}

    buf = io.BytesIO()
    instance.wfile = buf

    captured = {"status": None, "headers": []}

    def fake_send_response(code, message=None):
        captured["status"] = code

    def fake_send_header(key, value):
        captured["headers"].append((key, value))

    def fake_end_headers():
        pass

    instance.send_response = fake_send_response
    instance.send_header = fake_send_header
    instance.end_headers = fake_end_headers
    instance.log_message = lambda *a: None
    instance.log_request = lambda *a: None
    instance.responses = http.server.BaseHTTPRequestHandler.responses

    instance.do_GET()

    body = json.loads(buf.getvalue().decode("utf-8"))
    return captured["status"], body


# ===================================================================
# 1. Multi-sort in jobs.py
# ===================================================================

class TestJobsSort:
    """Tests for sort behaviour in the /api/jobs endpoint."""

    def test_default_sort_is_opendate_desc(self):
        """When no order params, results should be sorted by openDate DESC."""
        status, body = _invoke_handler(jobs_mod.handler, "/api/jobs?draw=1&start=0&length=10")
        assert status == 200
        data = body["data"]
        assert len(data) > 0
        # openDate is column index 6 in the COLUMNS list
        open_dates = [row[6] for row in data]
        assert open_dates == sorted(open_dates, reverse=True), "Default sort should be openDate DESC"

    def test_single_sort_asc(self):
        """Single sort on positionTitle ASC (column 0)."""
        status, body = _invoke_handler(
            jobs_mod.handler,
            "/api/jobs?draw=1&start=0&length=10&order[0][column]=0&order[0][dir]=asc",
        )
        assert status == 200
        titles = [row[0] for row in body["data"]]
        assert titles == sorted(titles), "Should be sorted by positionTitle ASC"

    def test_single_sort_desc(self):
        """Single sort on positionTitle DESC."""
        status, body = _invoke_handler(
            jobs_mod.handler,
            "/api/jobs?draw=1&start=0&length=10&order[0][column]=0&order[0][dir]=desc",
        )
        assert status == 200
        titles = [row[0] for row in body["data"]]
        assert titles == sorted(titles, reverse=True), "Should be sorted by positionTitle DESC"

    def test_multi_sort_two_columns(self):
        """Multi-sort: primary by hiringDepartmentName ASC, secondary by openDate DESC."""
        status, body = _invoke_handler(
            jobs_mod.handler,
            "/api/jobs?draw=1&start=0&length=25"
            "&order[0][column]=1&order[0][dir]=asc"
            "&order[1][column]=6&order[1][dir]=desc",
        )
        assert status == 200
        data = body["data"]
        assert len(data) > 1
        # Verify primary sort: department names should be non-decreasing
        depts = [row[1] for row in data]
        assert depts == sorted(depts), "Primary sort by department ASC should hold"

    def test_multi_sort_three_columns(self):
        """Three sort columns should all be respected."""
        path = (
            "/api/jobs?draw=1&start=0&length=25"
            "&order[0][column]=1&order[0][dir]=asc"
            "&order[1][column]=2&order[1][dir]=asc"
            "&order[2][column]=6&order[2][dir]=desc"
        )
        status, body = _invoke_handler(jobs_mod.handler, path)
        assert status == 200
        assert len(body["data"]) > 0

    def test_fourth_sort_column_ignored(self):
        """Only 3 sort columns allowed; order[3] should be ignored."""
        path_3 = (
            "/api/jobs?draw=1&start=0&length=25"
            "&order[0][column]=0&order[0][dir]=asc"
            "&order[1][column]=1&order[1][dir]=asc"
            "&order[2][column]=6&order[2][dir]=desc"
        )
        path_4 = (
            path_3 + "&order[3][column]=2&order[3][dir]=asc"
        )
        _, body3 = _invoke_handler(jobs_mod.handler, path_3)
        _, body4 = _invoke_handler(jobs_mod.handler, path_4)
        # The 4th sort column shouldn't change results
        assert body3["data"] == body4["data"], "4th sort column should be ignored"

    def test_invalid_column_index_falls_back_to_default(self):
        """An out-of-range column index should be skipped; if all are invalid, use default sort."""
        status, body = _invoke_handler(
            jobs_mod.handler,
            "/api/jobs?draw=1&start=0&length=10&order[0][column]=999&order[0][dir]=asc",
        )
        assert status == 200
        # Should fall back to default openDate DESC
        open_dates = [row[6] for row in body["data"]]
        assert open_dates == sorted(open_dates, reverse=True)


# ===================================================================
# 2. Month gap filling in aggregate.py
# ===================================================================

class TestAggregateMonthGapFill:
    """Tests for continuous month gap-filling in /api/aggregate?group_by=month."""

    def test_month_returns_continuous_months(self):
        """group_by=month should return a continuous sequence with no gaps."""
        status, body = _invoke_handler(agg_mod.handler, "/api/aggregate?group_by=month")
        assert status == 200
        labels = body["labels"]
        assert len(labels) > 2

        # Walk through labels and verify each month increments by exactly 1
        for i in range(1, len(labels)):
            prev_y, prev_m = int(labels[i - 1][:4]), int(labels[i - 1][5:7])
            cur_y, cur_m = int(labels[i][:4]), int(labels[i][5:7])
            expected_m = prev_m + 1
            expected_y = prev_y
            if expected_m > 12:
                expected_m = 1
                expected_y += 1
            assert (cur_y, cur_m) == (expected_y, expected_m), (
                f"Gap detected between {labels[i-1]} and {labels[i]}"
            )

    def test_missing_months_have_zero_count(self):
        """Filled-in months should have count=0 and avg_salary=0."""
        status, body = _invoke_handler(agg_mod.handler, "/api/aggregate?group_by=month")
        assert status == 200
        counts = body["datasets"]["count"]
        avg_sals = body["datasets"]["avg_salary"]
        # Every entry should be a number (int/float), including zeros
        for c in counts:
            assert isinstance(c, (int, float))
        for s in avg_sals:
            assert isinstance(s, (int, float))

    def test_month_labels_match_dataset_lengths(self):
        """labels and dataset arrays must be the same length."""
        status, body = _invoke_handler(agg_mod.handler, "/api/aggregate?group_by=month")
        assert status == 200
        n = len(body["labels"])
        assert len(body["datasets"]["count"]) == n
        assert len(body["datasets"]["avg_salary"]) == n

    def test_all_months_between_min_max_present(self):
        """Every month between the first and last label should appear."""
        status, body = _invoke_handler(agg_mod.handler, "/api/aggregate?group_by=month")
        assert status == 200
        labels = body["labels"]
        if not labels:
            pytest.skip("No data")
        min_label, max_label = labels[0], labels[-1]
        # Build expected set
        expected = []
        y, m = int(min_label[:4]), int(min_label[5:7])
        end_y, end_m = int(max_label[:4]), int(max_label[5:7])
        while (y, m) <= (end_y, end_m):
            expected.append(f"{y:04d}-{m:02d}")
            m += 1
            if m > 12:
                m = 1
                y += 1
        assert labels == expected


# ===================================================================
# 3. Filter parsing (_parse_filters)
# ===================================================================

class TestJobsParseFilters:
    """Tests for _parse_filters in jobs.py."""

    def test_multiselect_pipe_separated(self):
        params = {"filter_appointmentType": ["Term|Permanent"]}
        clauses, values = jobs_mod._parse_filters(params)
        assert len(clauses) == 1
        assert "IN" in clauses[0]
        assert values == ["term", "permanent"]

    def test_range_filter_min(self):
        params = {"filter_minimumSalary_min": ["50000"]}
        clauses, values = jobs_mod._parse_filters(params)
        assert len(clauses) == 1
        assert ">=" in clauses[0]
        assert values == [50000.0]

    def test_range_filter_max(self):
        params = {"filter_maximumSalary_max": ["120000"]}
        clauses, values = jobs_mod._parse_filters(params)
        assert len(clauses) == 1
        assert "<=" in clauses[0]
        assert values == [120000.0]

    def test_text_like_filter(self):
        params = {"filter_positionTitle": ["engineer"]}
        clauses, values = jobs_mod._parse_filters(params)
        assert len(clauses) == 1
        assert "LIKE" in clauses[0]
        assert values == ["%engineer%"]

    def test_invalid_column_rejected(self):
        params = {"filter_nonExistentColumn": ["value"]}
        clauses, values = jobs_mod._parse_filters(params)
        assert clauses == []
        assert values == []

    def test_invalid_range_column_rejected(self):
        params = {"filter_fakeColumn_min": ["100"]}
        clauses, values = jobs_mod._parse_filters(params)
        assert clauses == []
        assert values == []

    def test_empty_value_skipped(self):
        params = {"filter_positionTitle": [""]}
        clauses, values = jobs_mod._parse_filters(params)
        assert clauses == []

    def test_non_filter_keys_ignored(self):
        params = {"draw": ["1"], "start": ["0"], "length": ["10"]}
        clauses, values = jobs_mod._parse_filters(params)
        assert clauses == []


class TestAggregateParseFilters:
    """Tests for _parse_filters in aggregate.py."""

    def test_multiselect_pipe_separated(self):
        params = {"filter_hiringAgencyName": ["Army|Navy"]}
        clauses, values = agg_mod._parse_filters(params)
        assert len(clauses) == 1
        assert "IN" in clauses[0]
        assert values == ["army", "navy"]

    def test_range_filter(self):
        params = {"filter_minimumSalary_min": ["30000"], "filter_minimumSalary_max": ["80000"]}
        clauses, values = agg_mod._parse_filters(params)
        assert len(clauses) == 2
        assert any(">=" in c for c in clauses)
        assert any("<=" in c for c in clauses)

    def test_text_like_filter(self):
        params = {"filter_positionTitle": ["analyst"]}
        clauses, values = agg_mod._parse_filters(params)
        assert len(clauses) == 1
        assert "LIKE" in clauses[0]
        assert values == ["%analyst%"]

    def test_invalid_column_rejected(self):
        params = {"filter_madeUpField": ["x"]}
        clauses, values = agg_mod._parse_filters(params)
        assert clauses == []
        assert values == []
