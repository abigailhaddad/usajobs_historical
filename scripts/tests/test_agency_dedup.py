"""
Tests for the historical+current parquet combining logic in parquet_utils.py.

The core issue: current_jobs parquets sometimes store hiringAgencyName at the
department level (e.g. "Department of Justice") when OrganizationName is null in
the current API response.  The same job in a historical parquet has the correct
bureau-level name (e.g. "Executive Office for U.S. Attorneys...").

Naive UNION ALL + GROUP BY hiringAgencyName double-counts those jobs.
combine_and_fix / build_deduped_query must handle this correctly.
"""
import json
import os
import sys
import tempfile

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from parquet_utils import combine_and_fix, fix_agency_names, build_deduped_query

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DEPT = "Department of Justice"
BUREAU = "Executive Office for U.S. Attorneys and the Office of the U.S. Attorneys"
BUREAU2 = "Civil Rights Division"


def _hist_row(control_number, agency_name, dept_name=DEPT, title="Attorney"):
    return {
        "usajobsControlNumber": str(control_number),
        "hiringAgencyName": agency_name,
        "hiringDepartmentName": dept_name,
        "positionTitle": title,
        "positionOpenDate": "2025-06-01",
    }


def _curr_row(control_number, agency_name, dept_name=DEPT, title="Attorney", org_name=None):
    mod = {"DepartmentName": dept_name, "OrganizationName": org_name, "PositionTitle": title}
    return {
        "usajobsControlNumber": str(control_number),
        "hiringAgencyName": agency_name,
        "hiringDepartmentName": dept_name,
        "positionTitle": title,
        "positionOpenDate": "2025-06-01",
        "MatchedObjectDescriptor": json.dumps(mod),
    }


def _df(*rows):
    return pd.DataFrame(list(rows))


# ---------------------------------------------------------------------------
# fix_agency_names
# ---------------------------------------------------------------------------

class TestFixAgencyNames:
    def test_fixes_dept_level_when_org_name_available(self):
        """hiringAgencyName == hiringDepartmentName → replaced with OrganizationName."""
        df = _df(_curr_row(1, DEPT, DEPT, org_name=BUREAU))
        result = fix_agency_names(df)
        assert result.loc[0, "hiringAgencyName"] == BUREAU

    def test_leaves_specific_name_untouched(self):
        """Records that already have a specific agency name are not modified."""
        df = _df(_curr_row(1, BUREAU, DEPT, org_name=BUREAU))
        result = fix_agency_names(df)
        assert result.loc[0, "hiringAgencyName"] == BUREAU

    def test_leaves_dept_level_when_no_mod(self):
        """When MatchedObjectDescriptor column is absent, no fix is possible."""
        df = _df(_hist_row(1, DEPT))  # no MatchedObjectDescriptor column
        result = fix_agency_names(df)
        assert result.loc[0, "hiringAgencyName"] == DEPT

    def test_leaves_dept_level_when_org_name_null(self):
        """OrganizationName is null in API response — keep department-level name."""
        df = _df(_curr_row(1, DEPT, DEPT, org_name=None))
        result = fix_agency_names(df)
        assert result.loc[0, "hiringAgencyName"] == DEPT

    def test_does_not_mutate_input(self):
        df = _df(_curr_row(1, DEPT, DEPT, org_name=BUREAU))
        original_value = df.loc[0, "hiringAgencyName"]
        _ = fix_agency_names(df)
        assert df.loc[0, "hiringAgencyName"] == original_value


# ---------------------------------------------------------------------------
# combine_and_fix — deduplication
# ---------------------------------------------------------------------------

class TestCombineAndFixDedup:
    def test_specific_agency_name_wins_over_dept_level(self):
        """
        Same control number: historical has specific bureau name, current has
        department-level fallback.  Result must use the specific name.
        """
        hist = _df(_hist_row(100, BUREAU))
        curr = _df(_curr_row(100, DEPT, org_name=BUREAU))
        result = combine_and_fix([hist], [curr])
        assert len(result) == 1
        assert result.loc[0, "hiringAgencyName"] == BUREAU

    def test_no_double_counting(self):
        """
        A job in both historical and current must appear exactly once in output.
        """
        hist = _df(_hist_row(100, BUREAU), _hist_row(101, BUREAU2))
        curr = _df(_curr_row(100, DEPT, org_name=BUREAU), _curr_row(102, DEPT, org_name=None))
        result = combine_and_fix([hist], [curr])
        assert len(result) == 3  # 100, 101, 102 — not 4
        assert result["usajobsControlNumber"].nunique() == 3

    def test_current_only_job_with_fix(self):
        """
        Job only in current with dept-level name but OrganizationName available
        → retroactive fix restores bureau name.
        """
        hist = _df(_hist_row(999, "Some Other Agency", dept_name="Some Dept"))
        curr = _df(_curr_row(200, DEPT, org_name=BUREAU))
        result = combine_and_fix([hist], [curr])
        row = result[result["usajobsControlNumber"] == "200"].iloc[0]
        assert row["hiringAgencyName"] == BUREAU

    def test_current_only_job_without_fix(self):
        """
        Job only in current with dept-level name and null OrganizationName
        → department-level name is kept (acceptable, no better data available).
        """
        hist = _df(_hist_row(999, "Other"))
        curr = _df(_curr_row(200, DEPT, org_name=None))
        result = combine_and_fix([hist], [curr])
        row = result[result["usajobsControlNumber"] == "200"].iloc[0]
        assert row["hiringAgencyName"] == DEPT

    def test_historical_beats_current_as_tiebreak(self):
        """
        When both have specific (non-dept) agency names, historical record wins.
        """
        hist = _df(_hist_row(100, "Bureau A (from historical)"))
        curr = _df(_curr_row(100, "Bureau A (from current)", org_name="Bureau A (from current)"))
        result = combine_and_fix([hist], [curr])
        assert result.loc[0, "hiringAgencyName"] == "Bureau A (from historical)"

    def test_multiple_same_control_number_in_current(self):
        """
        If the same control number appears twice in current (different series),
        it should still appear only once in output.
        """
        hist = _df()
        curr = _df(
            _curr_row(100, DEPT, org_name=BUREAU),
            _curr_row(100, DEPT, org_name=BUREAU),
        )
        result = combine_and_fix([hist], [curr])
        assert len(result) == 1

    def test_empty_current(self):
        hist = _df(_hist_row(1, BUREAU), _hist_row(2, BUREAU2))
        result = combine_and_fix([hist], [])
        assert len(result) == 2

    def test_empty_historical(self):
        curr = _df(_curr_row(1, DEPT, org_name=BUREAU))
        result = combine_and_fix([], [curr])
        assert len(result) == 1
        assert result.loc[0, "hiringAgencyName"] == BUREAU


# ---------------------------------------------------------------------------
# build_deduped_query — DuckDB SQL
# ---------------------------------------------------------------------------

class TestBuildDedupedQuery:
    def test_query_deduplicates_and_prefers_specific_agency(self, tmp_path):
        """DuckDB query returns one row per control number with specific agency name."""
        duckdb = pytest.importorskip("duckdb")

        hist_path = tmp_path / "hist.parquet"
        curr_path = tmp_path / "curr.parquet"

        _df(_hist_row(100, BUREAU)).to_parquet(hist_path, index=False)
        _df(_curr_row(100, DEPT, org_name=BUREAU)).to_parquet(curr_path, index=False)

        sql = build_deduped_query(
            hist_urls=[f"'{hist_path}'"],
            curr_urls=[f"'{curr_path}'"],
            select="usajobsControlNumber, hiringAgencyName",
        )
        result = duckdb.connect().execute(sql).df()
        assert len(result) == 1
        assert result.loc[0, "hiringAgencyName"] == BUREAU

    def test_query_no_double_count(self, tmp_path):
        duckdb = pytest.importorskip("duckdb")

        hist_path = tmp_path / "hist.parquet"
        curr_path = tmp_path / "curr.parquet"

        _df(
            _hist_row(1, BUREAU),
            _hist_row(2, BUREAU2),
        ).to_parquet(hist_path, index=False)

        _df(
            _curr_row(1, DEPT, org_name=BUREAU),
            _curr_row(3, DEPT, org_name=None),
        ).to_parquet(curr_path, index=False)

        sql = build_deduped_query(
            hist_urls=[f"'{hist_path}'"],
            curr_urls=[f"'{curr_path}'"],
            select="usajobsControlNumber, hiringAgencyName",
        )
        result = duckdb.connect().execute(sql).df()
        assert len(result) == 3
        assert set(result["usajobsControlNumber"].astype(str)) == {"1", "2", "3"}

    def test_where_clause_applied(self, tmp_path):
        duckdb = pytest.importorskip("duckdb")

        hist_path = tmp_path / "hist.parquet"
        curr_path = tmp_path / "curr.parquet"

        _df(
            _hist_row(1, BUREAU, DEPT),
            _hist_row(2, "Internal Revenue Service", "Department of the Treasury"),
        ).to_parquet(hist_path, index=False)
        _df(_curr_row(3, DEPT, org_name=None)).to_parquet(curr_path, index=False)

        sql = build_deduped_query(
            hist_urls=[f"'{hist_path}'"],
            curr_urls=[f"'{curr_path}'"],
            select="usajobsControlNumber",
            where=f"hiringDepartmentName = '{DEPT}'",
        )
        result = duckdb.connect().execute(sql).df()
        assert set(result["usajobsControlNumber"].astype(str)) == {"1", "3"}
