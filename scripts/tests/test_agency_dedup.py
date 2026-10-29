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


def _hist_row(control_number, agency_name, dept_name=DEPT, title="Attorney", **extra):
    row = {
        "usajobsControlNumber": str(control_number),
        "hiringAgencyName": agency_name,
        "hiringDepartmentName": dept_name,
        "positionTitle": title,
        "positionOpenDate": "2025-06-01",
    }
    row.update(extra)
    return row


def _curr_row(control_number, agency_name, dept_name=DEPT, title="Attorney", org_name=None, **extra):
    mod = {"DepartmentName": dept_name, "OrganizationName": org_name, "PositionTitle": title}
    row = {
        "usajobsControlNumber": str(control_number),
        "hiringAgencyName": agency_name,
        "hiringDepartmentName": dept_name,
        "positionTitle": title,
        "positionOpenDate": "2025-06-01",
        "MatchedObjectDescriptor": json.dumps(mod),
    }
    row.update(extra)
    return row


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


# ---------------------------------------------------------------------------
# build_deduped_query — coalesce_fields
# ---------------------------------------------------------------------------

class TestCoalesceCurrentOnlyFields:
    """Regression: applicationCap (and friends) only exist on current-API rows.
    When a control number has both a historical and a current row with equally
    specific agency names, historical wins the tiebreak -- which silently
    dropped these fields even though a current row for the same control
    number had them. coalesce_fields fixes that.
    """

    def test_field_survives_when_historical_row_wins_dedup(self, tmp_path):
        duckdb = pytest.importorskip("duckdb")
        hist_path = tmp_path / "hist.parquet"
        curr_path = tmp_path / "curr.parquet"

        # Both sides have an equally-specific agency name -> historical wins
        # the tiebreak (src_priority). Without coalescing, applicationCap
        # would vanish even though the current row has it.
        _df(_hist_row(1, BUREAU)).to_parquet(hist_path, index=False)
        _df(_curr_row(1, BUREAU, org_name=BUREAU, applicationCap=50.0)).to_parquet(curr_path, index=False)

        sql = build_deduped_query(
            hist_urls=[f"'{hist_path}'"],
            curr_urls=[f"'{curr_path}'"],
            select="usajobsControlNumber, applicationCap",
            coalesce_fields=["applicationCap"],
        )
        result = duckdb.connect().execute(sql).df()
        assert len(result) == 1
        assert result.loc[0, "applicationCap"] == 50.0

    def test_field_is_dropped_without_the_flag(self, tmp_path):
        """Same setup, but without coalesce_fields -- documents
        the bug this option exists to fix, so it doesn't silently reappear."""
        duckdb = pytest.importorskip("duckdb")
        hist_path = tmp_path / "hist.parquet"
        curr_path = tmp_path / "curr.parquet"

        _df(_hist_row(1, BUREAU)).to_parquet(hist_path, index=False)
        _df(_curr_row(1, BUREAU, org_name=BUREAU, applicationCap=50.0)).to_parquet(curr_path, index=False)

        sql = build_deduped_query(
            hist_urls=[f"'{hist_path}'"],
            curr_urls=[f"'{curr_path}'"],
            select="usajobsControlNumber, applicationCap",
        )
        result = duckdb.connect().execute(sql).df()
        assert len(result) == 1
        assert pd.isna(result.loc[0, "applicationCap"])

    def test_multiple_fields_coalesced_together(self, tmp_path):
        duckdb = pytest.importorskip("duckdb")
        hist_path = tmp_path / "hist.parquet"
        curr_path = tmp_path / "curr.parquet"

        _df(_hist_row(1, BUREAU)).to_parquet(hist_path, index=False)
        _df(_curr_row(1, BUREAU, org_name=BUREAU, applicationCap=25.0,
                       remoteIndicator="Y")).to_parquet(curr_path, index=False)

        sql = build_deduped_query(
            hist_urls=[f"'{hist_path}'"],
            curr_urls=[f"'{curr_path}'"],
            select="usajobsControlNumber, applicationCap, remoteIndicator",
            coalesce_fields=["applicationCap", "remoteIndicator"],
        )
        result = duckdb.connect().execute(sql).df()
        assert result.loc[0, "applicationCap"] == 25.0
        assert result.loc[0, "remoteIndicator"] == "Y"

    def test_prefers_historical_when_both_sides_populated(self, tmp_path):
        """Regression: coalescing must NOT be MAX-by-alphabetical-value. Unlike
        applicationCap (only ever non-null on one side), a field like
        HiringPaths is populated on both sides most years and should still
        prefer historical when both have a value -- matching the row dedup's
        own preference -- falling back to current only when historical is null.
        "Zeta..." sorts after "Alpha..." alphabetically, so a MAX()-based
        coalesce would (wrongly) return the current value here."""
        duckdb = pytest.importorskip("duckdb")
        hist_path = tmp_path / "hist.parquet"
        curr_path = tmp_path / "curr.parquet"

        _df(_hist_row(1, BUREAU, HiringPaths="Alpha: historical value")).to_parquet(hist_path, index=False)
        _df(_curr_row(1, BUREAU, org_name=BUREAU,
                       HiringPaths="Zeta: current value")).to_parquet(curr_path, index=False)

        sql = build_deduped_query(
            hist_urls=[f"'{hist_path}'"],
            curr_urls=[f"'{curr_path}'"],
            select="usajobsControlNumber, HiringPaths",
            coalesce_fields=["HiringPaths"],
        )
        result = duckdb.connect().execute(sql).df()
        assert result.loc[0, "HiringPaths"] == "Alpha: historical value"

    def test_falls_back_to_current_when_historical_is_null(self, tmp_path):
        """The other half of the same fix: when historical's value IS null,
        fall back to current rather than leaving it null."""
        duckdb = pytest.importorskip("duckdb")
        hist_path = tmp_path / "hist.parquet"
        curr_path = tmp_path / "curr.parquet"

        _df(_hist_row(1, BUREAU)).to_parquet(hist_path, index=False)  # no HiringPaths column value
        _df(_curr_row(1, BUREAU, org_name=BUREAU,
                       HiringPaths="current value")).to_parquet(curr_path, index=False)

        sql = build_deduped_query(
            hist_urls=[f"'{hist_path}'"],
            curr_urls=[f"'{curr_path}'"],
            select="usajobsControlNumber, HiringPaths",
            coalesce_fields=["HiringPaths"],
        )
        result = duckdb.connect().execute(sql).df()
        assert result.loc[0, "HiringPaths"] == "current value"

    def test_default_query_unchanged_without_the_flag(self, tmp_path):
        """No coalesce_fields -> byte-identical to the pre-existing
        query shape (no merged CTE), so every caller that doesn't ask for this
        gets exactly what it always got."""
        sql = build_deduped_query(
            hist_urls=["'a.parquet'"],
            curr_urls=["'b.parquet'"],
            select="usajobsControlNumber",
        )
        assert "merged" not in sql
        assert "FROM combined" in sql


# ---------------------------------------------------------------------------
# build_deduped_query — the two HiringPaths spellings
# ---------------------------------------------------------------------------

class TestHiringPathsSpellings:
    """Regression: historical files from 2025 on send hiring paths as
    "hiringPaths", and some carry both that and "HiringPaths". duckdb reads the
    second as "hiringpaths_1", so HiringPaths alone was null for all of 2026.
    """

    def _run(self, tmp_path, hist_rows, select="usajobsControlNumber, HiringPaths"):
        duckdb = pytest.importorskip("duckdb")
        hist_path = tmp_path / "hist.parquet"
        curr_path = tmp_path / "curr.parquet"
        _df(*hist_rows).to_parquet(hist_path, index=False)
        _df(_curr_row(99, BUREAU, org_name=BUREAU)).to_parquet(curr_path, index=False)
        sql = build_deduped_query(
            hist_urls=[f"'{hist_path}'"],
            curr_urls=[f"'{curr_path}'"],
            select=select,
        )
        return duckdb.connect().execute(sql).df().set_index("usajobsControlNumber")

    def test_both_spellings_in_one_file(self, tmp_path):
        result = self._run(tmp_path, [
            _hist_row(1, BUREAU, HiringPaths="old spelling", hiringPaths=None),
            _hist_row(2, BUREAU, HiringPaths=None, hiringPaths="new spelling"),
        ])
        assert result.loc["1", "HiringPaths"] == "old spelling"
        assert result.loc["2", "HiringPaths"] == "new spelling"

    def test_only_new_spelling(self, tmp_path):
        result = self._run(tmp_path, [_hist_row(1, BUREAU, hiringPaths="new spelling")])
        assert result.loc["1", "HiringPaths"] == "new spelling"

    def test_no_hiring_paths_column(self, tmp_path):
        result = self._run(tmp_path, [_hist_row(1, BUREAU)])
        assert pd.isna(result.loc["1", "HiringPaths"])

    def test_extra_spellings_not_in_output(self, tmp_path):
        result = self._run(tmp_path, [
            _hist_row(1, BUREAU, HiringPaths=None, hiringPaths="new spelling"),
        ], select="*")
        hp = [c for c in result.columns if c.lower().startswith("hiringpaths")]
        assert hp == ["HiringPaths"]
        assert "_no_hiring_paths" not in result.columns
