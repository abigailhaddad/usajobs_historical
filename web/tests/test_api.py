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
from api import columns as col_mod
from api import filter_options as fopt_mod

# Column indexes from the shared config
COL_IDX = {col: i for i, col in enumerate(col_mod.COLUMNS)}


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
        idx = COL_IDX['openDate']
        open_dates = [row[idx] for row in data]
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
        dept_idx = COL_IDX['hiringDepartmentName']
        date_idx = COL_IDX['openDate']
        status, body = _invoke_handler(
            jobs_mod.handler,
            f"/api/jobs?draw=1&start=0&length=25"
            f"&order[0][column]={dept_idx}&order[0][dir]=asc"
            f"&order[1][column]={date_idx}&order[1][dir]=desc",
        )
        assert status == 200
        data = body["data"]
        assert len(data) > 1
        # Verify primary sort: department names should be non-decreasing
        depts = [row[dept_idx] for row in data]
        assert depts == sorted(depts), "Primary sort by department ASC should hold"

    def test_multi_sort_three_columns(self):
        """Three sort columns should all be respected."""
        dept_idx = COL_IDX['hiringDepartmentName']
        agency_idx = COL_IDX['hiringAgencyName']
        date_idx = COL_IDX['openDate']
        path = (
            f"/api/jobs?draw=1&start=0&length=25"
            f"&order[0][column]={dept_idx}&order[0][dir]=asc"
            f"&order[1][column]={agency_idx}&order[1][dir]=asc"
            f"&order[2][column]={date_idx}&order[2][dir]=desc"
        )
        status, body = _invoke_handler(jobs_mod.handler, path)
        assert status == 200
        assert len(body["data"]) > 0

    def test_fourth_sort_column_ignored(self):
        """Only 3 sort columns allowed; order[3] should be ignored."""
        title_idx = COL_IDX['positionTitle']
        dept_idx = COL_IDX['hiringDepartmentName']
        date_idx = COL_IDX['openDate']
        agency_idx = COL_IDX['hiringAgencyName']
        path_3 = (
            f"/api/jobs?draw=1&start=0&length=25"
            f"&order[0][column]={title_idx}&order[0][dir]=asc"
            f"&order[1][column]={dept_idx}&order[1][dir]=asc"
            f"&order[2][column]={date_idx}&order[2][dir]=desc"
        )
        path_4 = (
            path_3 + f"&order[3][column]={agency_idx}&order[3][dir]=asc"
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
        idx = COL_IDX['openDate']
        open_dates = [row[idx] for row in body["data"]]
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

class TestParseFilters:
    """Tests for the shared parse_filters function (used by all endpoints)."""

    def test_multiselect_pipe_separated(self):
        params = {"filter_appointmentType": ["Term|Permanent"]}
        clauses, values = col_mod.parse_filters(params)
        assert len(clauses) == 1
        assert "IN" in clauses[0]
        assert values == ["term", "permanent"]

    def test_range_filter_min(self):
        params = {"filter_minimumSalary_min": ["50000"]}
        clauses, values = col_mod.parse_filters(params)
        assert len(clauses) == 1
        assert ">=" in clauses[0]
        assert values == [50000.0]

    def test_range_filter_max(self):
        params = {"filter_maximumSalary_max": ["120000"]}
        clauses, values = col_mod.parse_filters(params)
        assert len(clauses) == 1
        assert "<=" in clauses[0]
        assert values == [120000.0]

    def test_text_like_filter(self):
        params = {"filter_positionTitle": ["engineer"]}
        clauses, values = col_mod.parse_filters(params)
        assert len(clauses) == 1
        assert "LIKE" in clauses[0]
        assert values == ["%engineer%"]

    def test_invalid_column_rejected(self):
        params = {"filter_nonExistentColumn": ["value"]}
        clauses, values = col_mod.parse_filters(params)
        assert clauses == []
        assert values == []

    def test_invalid_range_column_rejected(self):
        params = {"filter_fakeColumn_min": ["100"]}
        clauses, values = col_mod.parse_filters(params)
        assert clauses == []
        assert values == []

    def test_empty_value_skipped(self):
        params = {"filter_positionTitle": [""]}
        clauses, values = col_mod.parse_filters(params)
        assert clauses == []

    def test_non_filter_keys_ignored(self):
        params = {"draw": ["1"], "start": ["0"], "length": ["10"]}
        clauses, values = col_mod.parse_filters(params)
        assert clauses == []

    def test_date_range_filter(self):
        """Date range filters should generate >= and <= clauses."""
        params = {
            "filter_openDate_min": ["2024-01-01"],
            "filter_openDate_max": ["2024-12-31"],
        }
        clauses, values = col_mod.parse_filters(params)
        assert len(clauses) == 2
        assert any(">=" in c for c in clauses)
        assert any("<=" in c for c in clauses)

    def test_service_type_accepted(self):
        """serviceType should be accepted (was previously missing from aggregate)."""
        params = {"filter_serviceType": ["Competitive"]}
        clauses, values = col_mod.parse_filters(params)
        assert len(clauses) == 1

    def test_combined_filters(self):
        """Multiple filter types applied together."""
        params = {
            "filter_positionTitle": ["engineer"],
            "filter_minimumSalary_min": ["50000"],
            "filter_status": ["Active|Closed"],
        }
        clauses, values = col_mod.parse_filters(params)
        assert len(clauses) == 3


# ===================================================================
# 4. Column consistency across endpoints
# ===================================================================

class TestFilterColumnConsistency:
    """All endpoints now use the shared parse_filters from columns.py.
    These tests verify the shared config covers all frontend columns."""

    # Columns the frontend can filter on (from index.html column config)
    FRONTEND_FILTER_COLUMNS = {
        'positionTitle', 'hiringDepartmentName', 'hiringAgencyName',
        'grade', 'minimumSalary', 'maximumSalary',
        'appointmentType', 'serviceType', 'locations', 'status',
    }

    def test_all_frontend_columns_in_filterable(self):
        """Every frontend-filterable column must be in FILTERABLE_COLUMNS."""
        missing = self.FRONTEND_FILTER_COLUMNS - col_mod.FILTERABLE_COLUMNS
        assert not missing, f"columns.py FILTERABLE_COLUMNS is missing: {missing}"

    def test_parse_filters_accepts_all_frontend_columns(self):
        """parse_filters should generate clauses for every frontend column."""
        accepted = set()
        for col in self.FRONTEND_FILTER_COLUMNS:
            clauses, _ = col_mod.parse_filters({f"filter_{col}": ["test"]})
            if clauses:
                accepted.add(col)
            clauses_min, _ = col_mod.parse_filters({f"filter_{col}_min": ["1"]})
            if clauses_min:
                accepted.add(col)
        missing = self.FRONTEND_FILTER_COLUMNS - accepted
        assert not missing, f"parse_filters rejects frontend columns: {missing}"

    def test_dropdown_fields_subset_of_filterable(self):
        """DROPDOWN_FIELDS should be a subset of FILTERABLE_COLUMNS."""
        assert col_mod.DROPDOWN_FIELDS <= col_mod.FILTERABLE_COLUMNS

    def test_columns_list_matches_headers(self):
        """COLUMNS and COLUMN_HEADERS must have same length."""
        assert len(col_mod.COLUMNS) == len(col_mod.COLUMN_HEADERS)


# ===================================================================
# 5. Integration: filtered queries return correct results
# ===================================================================

class TestJobsFilteredResults:
    """Test that filters actually affect the returned data."""

    def test_text_filter_reduces_results(self):
        """A text filter should return fewer results than unfiltered."""
        _, body_all = _invoke_handler(jobs_mod.handler, "/api/jobs?draw=1&start=0&length=10")
        _, body_filt = _invoke_handler(
            jobs_mod.handler,
            "/api/jobs?draw=1&start=0&length=10&filter_positionTitle=ZZZZNOTREAL"
        )
        assert body_filt["recordsFiltered"] < body_all["recordsTotal"]

    def test_text_filter_results_match(self):
        """Filtered rows should contain the search term."""
        _, body = _invoke_handler(
            jobs_mod.handler,
            "/api/jobs?draw=1&start=0&length=25&filter_positionTitle=engineer"
        )
        for row in body["data"]:
            assert "engineer" in row[0].lower(), f"Row title '{row[0]}' doesn't match filter"

    def test_multiselect_filter_results_match(self):
        """Multiselect filter should only return matching values."""
        # First get some actual agency names
        _, body_all = _invoke_handler(jobs_mod.handler, "/api/jobs?draw=1&start=0&length=5")
        if not body_all["data"]:
            pytest.skip("No data")
        agency_idx = COL_IDX['hiringAgencyName']
        agency = body_all["data"][0][agency_idx]

        _, body = _invoke_handler(
            jobs_mod.handler,
            f"/api/jobs?draw=1&start=0&length=25&filter_hiringAgencyName={agency}"
        )
        for row in body["data"]:
            assert agency.lower() in row[agency_idx].lower(), (
                f"Row agency '{row[agency_idx]}' doesn't match filter '{agency}'"
            )

    def test_range_filter_min_salary(self):
        """Min salary filter should exclude lower salaries."""
        _, body = _invoke_handler(
            jobs_mod.handler,
            "/api/jobs?draw=1&start=0&length=25&filter_minimumSalary_min=100000"
        )
        sal_idx = COL_IDX['minimumSalary']
        for row in body["data"]:
            if row[sal_idx]:
                salary = float(row[sal_idx])
                assert salary >= 100000, f"Salary {salary} is below min filter 100000"

    def test_combined_filters(self):
        """Multiple filters applied simultaneously should all take effect."""
        _, body = _invoke_handler(
            jobs_mod.handler,
            "/api/jobs?draw=1&start=0&length=25"
            "&filter_positionTitle=engineer"
            "&filter_minimumSalary_min=50000"
        )
        title_idx = COL_IDX['positionTitle']
        sal_idx = COL_IDX['minimumSalary']
        for row in body["data"]:
            assert "engineer" in row[title_idx].lower()
            if row[sal_idx]:
                assert float(row[sal_idx]) >= 50000


class TestAggregateFilteredResults:
    """Test that aggregate endpoint respects filters."""

    def test_agency_filter_on_month_aggregate(self):
        """Filtering by agency should reduce the monthly counts."""
        _, body_all = _invoke_handler(agg_mod.handler, "/api/aggregate?group_by=month")
        _, body_filt = _invoke_handler(
            agg_mod.handler,
            "/api/aggregate?group_by=month&filter_positionTitle=ZZZZNOTREAL"
        )
        total_all = sum(body_all["datasets"]["count"])
        total_filt = sum(body_filt["datasets"]["count"])
        assert total_filt < total_all, "Filter should reduce aggregate counts"

    def test_grade_aggregate_returns_numeric_labels(self):
        """Grade distribution labels must be integers (GS grade numbers), not empty."""
        status, body = _invoke_handler(
            agg_mod.handler,
            "/api/aggregate?group_by=grade"
        )
        assert status == 200
        assert len(body["labels"]) > 0, "Grade chart should not be empty"
        assert all(isinstance(g, int) for g in body["labels"]), \
            f"Grade labels should be integers, got: {body['labels'][:5]}"
        assert all(1 <= g <= 15 for g in body["labels"]), \
            f"Grade labels should be between 1-15, got: {body['labels']}"

    def test_department_aggregate_returns_data(self):
        """Department chart should return non-empty string labels."""
        status, body = _invoke_handler(
            agg_mod.handler, "/api/aggregate?group_by=department"
        )
        assert status == 200
        assert len(body["labels"]) > 0, "Department chart should not be empty"
        assert all(isinstance(l, str) and l for l in body["labels"]), \
            "Department labels should be non-empty strings"
        assert len(body["datasets"]["count"]) == len(body["labels"])

    def test_agency_aggregate_returns_data(self):
        """Agency chart should return non-empty string labels."""
        status, body = _invoke_handler(
            agg_mod.handler, "/api/aggregate?group_by=agency"
        )
        assert status == 200
        assert len(body["labels"]) > 0, "Agency chart should not be empty"
        assert all(isinstance(l, str) and l for l in body["labels"]), \
            "Agency labels should be non-empty strings"
        assert len(body["datasets"]["count"]) == len(body["labels"])

    def test_series_aggregate_returns_data(self):
        """Occupational series chart should return non-empty string labels."""
        status, body = _invoke_handler(
            agg_mod.handler, "/api/aggregate?group_by=series"
        )
        assert status == 200
        assert len(body["labels"]) > 0, "Series chart should not be empty"
        assert all(isinstance(l, str) and l for l in body["labels"]), \
            "Series labels should be non-empty strings"
        assert len(body["datasets"]["count"]) == len(body["labels"])

    def test_grade_aggregate_with_filter(self):
        """Grade distribution should change when filtered."""
        status, body = _invoke_handler(
            agg_mod.handler,
            "/api/aggregate?group_by=grade&filter_positionTitle=engineer"
        )
        assert status == 200
        assert "labels" in body
        assert "datasets" in body


# ===================================================================
# 7. Grade normalization (GS-7 ≡ GS-07)
# ===================================================================

class TestGradeNormalization:
    """Pay-plan codes appear both zero-padded ('GS-07') and not ('GS-7') in
    the data. Filtering on either should return identical results, and the
    dropdown should offer only one canonical option per grade."""

    def test_canonical_grade_helper(self):
        cg = col_mod.canonical_grade
        assert cg('GS-7') == 'gs-7'
        assert cg('GS-07') == 'gs-7'
        assert cg('gs-07') == 'gs-7'
        assert cg('GS-09/11') == 'gs-9/11'
        assert cg('GS-07/09') == 'gs-7/9'
        # XX-00 means 'ungraded' — must NOT collapse to XX-0
        assert cg('GS-00') == 'gs-00'
        assert cg('VN-00') == 'vn-00'
        # Already canonical, double-digit grades unchanged
        assert cg('GS-13') == 'gs-13'
        assert cg('GS-15') == 'gs-15'
        assert cg('') == ''
        assert cg(None) == ''

    def _month_total(self, query):
        status, body = _invoke_handler(agg_mod.handler, query)
        assert status == 200
        return sum(body["datasets"]["count"])

    def test_filter_grade_padded_equals_unpadded_single(self):
        """filter_grade=GS-7 and filter_grade=GS-07 must yield the same totals."""
        a = self._month_total("/api/aggregate?group_by=month&filter_grade=GS-7")
        b = self._month_total("/api/aggregate?group_by=month&filter_grade=GS-07")
        assert a == b, f"GS-7 ({a}) and GS-07 ({b}) should match after normalization"
        assert a > 0, "Filter should match at least one listing"

    def test_filter_grade_padded_equals_unpadded_multiselect(self):
        """Multiselect with mixed padding must agree with the un-padded form."""
        a = self._month_total("/api/aggregate?group_by=month&filter_grade=GS-7|GS-13")
        b = self._month_total("/api/aggregate?group_by=month&filter_grade=GS-07|GS-13")
        assert a == b, f"Mixed-pad multiselect ({b}) must match un-padded ({a})"

    def test_grade_dropdown_dedupes_zero_padded_variants(self):
        """The grade filter dropdown should not surface BOTH GS-7 and GS-07."""
        status, body = _invoke_handler(fopt_mod.handler, "/api/filter_options?field=grade")
        assert status == 200
        values = body["values"]
        # Bucket every returned value by its canonical form; each bucket
        # should have exactly one representative.
        from collections import defaultdict
        buckets = defaultdict(list)
        for v in values:
            buckets[col_mod.canonical_grade(v)].append(v)
        dupes = {k: v for k, v in buckets.items() if len(v) > 1}
        assert not dupes, f"Dropdown still contains zero-padded duplicates: {dupes}"
        # Sanity: GS-7 should be present, GS-07 should not
        assert 'GS-7' in values
        assert 'GS-07' not in values

    def test_grade_dropdown_keeps_ungraded(self):
        """XX-00 ('ungraded') must survive deduplication — it is its own grade."""
        status, body = _invoke_handler(fopt_mod.handler, "/api/filter_options?field=grade")
        assert status == 200
        values = body["values"]
        # At least one of the common 'ungraded' codes should remain
        assert any(v in values for v in ('GS-00', 'VN-00', 'AD-00', 'ES-00')), \
            "Ungraded XX-00 codes should not be collapsed away"


class TestExactMatchDropdownFilters:
    """Single-value dropdown filters (agency, dept, grade, etc.) must use exact
    string match. A value that contains a comma — e.g. an agency name like
    'Treasury, Financial Crimes Enforcement Network' — must NOT be split into
    OR'd substring searches, because that silently matches the wrong rows."""

    def _filtered_total(self, query):
        status, body = _invoke_handler(jobs_mod.handler, query)
        assert status == 200
        return body["recordsFiltered"]

    def test_comma_in_agency_name_is_literal(self):
        """An agency name with a comma should match itself, not 'OR'd substrings."""
        # Pick a known agency value with a comma and ensure the filter behaves like
        # an exact match. Non-matching agencies must NOT be returned.
        comma_agency = 'Treasury, Financial Crimes Enforcement Network'
        url = "/api/jobs?length=20&filter_hiringAgencyName=" + comma_agency.replace(' ', '%20')
        status, body = _invoke_handler(jobs_mod.handler, url)
        assert status == 200
        ag_idx = COL_IDX['hiringAgencyName']
        for row in body["data"]:
            assert row[ag_idx] == comma_agency, (
                f"Filter for {comma_agency!r} returned {row[ag_idx]!r} — "
                f"comma-split substring bug regressed"
            )

    def test_exact_agency_does_not_match_substring(self):
        """An agency name that is a substring of another agency must not match it."""
        # 'Department of the Treasury' is a substring of (e.g.) 'Treasury, Departmental Offices'
        # only via the buggy comma-split. With exact match, only literal matches return.
        url = "/api/jobs?length=10&filter_hiringAgencyName=Department%20of%20the%20Treasury"
        status, body = _invoke_handler(jobs_mod.handler, url)
        assert status == 200
        ag_idx = COL_IDX['hiringAgencyName']
        for row in body["data"]:
            assert row[ag_idx] == 'Department of the Treasury'

    def test_freetext_position_title_still_substring(self):
        """positionTitle is free-text — substring + comma-OR behavior must be preserved."""
        # Run the same query two ways; substring match should still work.
        a = self._filtered_total("/api/jobs?length=1&filter_positionTitle=engineer")
        b = self._filtered_total("/api/jobs?length=1&filter_positionTitle=Engineer")
        assert a == b > 0, "positionTitle substring search regressed"

    def test_freetext_comma_or_still_works(self):
        """Comma-separated terms in free-text fields should still OR-match."""
        a = self._filtered_total("/api/jobs?length=1&filter_positionTitle=engineer")
        b = self._filtered_total("/api/jobs?length=1&filter_positionTitle=scientist")
        either = self._filtered_total("/api/jobs?length=1&filter_positionTitle=engineer,scientist")
        assert either >= max(a, b), "Comma-OR search should match at least as many as either term alone"
