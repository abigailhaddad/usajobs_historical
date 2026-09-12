"""Tests for the deduped counts behind the README's headline and coverage table.

The numbers used to be sums of `pq.read_metadata(path).num_rows` over both the
historical and the current parquet for each year. A posting that is in both APIs
has a row in each, so every published figure was high by that overlap -- which is
the same double-count the README already warns about for hiringAgencyName.
"""
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from generate_docs_data import analyze_data_coverage, data_files, deduped_counts


def write(path, rows):
    """rows: list of (control_number, open_date, close_date)."""
    pq.write_table(pa.table({
        "usajobsControlNumber": pa.array([r[0] for r in rows], pa.string()),
        "positionOpenDate": pa.array([r[1] for r in rows], pa.string()),
        "positionCloseDate": pa.array([r[2] for r in rows], pa.string()),
    }), path)


class TestDedupedCounts:
    def test_a_posting_in_both_apis_is_counted_once(self, tmp_path):
        write(tmp_path / "historical_jobs_2025.parquet",
              [("1", "2025-03-01", "2025-04-01"),
               ("2", "2025-03-02", "2025-04-02")])
        write(tmp_path / "current_jobs_2025.parquet",
              [("2", "2025-03-02", "2025-04-02")])   # the overlap
        counts, total = deduped_counts(str(tmp_path))
        assert counts[2025]["total"] == 2
        assert counts[2025]["opened"] == 2
        assert counts[2025]["closed"] == 2
        assert total == 2

    def test_the_overall_total_dedupes_across_years_too(self, tmp_path):
        # A posting open at a year boundary sits in two years' current files.
        write(tmp_path / "current_jobs_2025.parquet",
              [("1", "2025-12-20", "2026-01-10")])
        write(tmp_path / "current_jobs_2026.parquet",
              [("1", "2025-12-20", "2026-01-10")])
        counts, total = deduped_counts(str(tmp_path))
        assert total == 1
        assert counts[2025]["opened"] == 1
        assert counts[2026]["opened"] == 0
        assert counts[2026]["closed"] == 1

    def test_dates_are_attributed_to_their_own_year_not_the_file_s(self, tmp_path):
        write(tmp_path / "historical_jobs_2026.parquet",
              [("1", "2025-11-01", "2026-02-01")])
        counts, _ = deduped_counts(str(tmp_path))
        assert counts[2026]["opened"] == 0
        assert counts[2026]["closed"] == 1
        assert counts[2026]["total"] == 1

    def test_a_missing_date_counts_for_neither(self, tmp_path):
        write(tmp_path / "historical_jobs_2025.parquet",
              [("1", "2025-01-01", None), ("2", None, None)])
        counts, _ = deduped_counts(str(tmp_path))
        assert counts[2025] == {"total": 2, "opened": 1, "closed": 0}

    def test_no_files_is_not_a_crash(self, tmp_path):
        assert deduped_counts(str(tmp_path)) == ({}, 0)

    def test_backup_and_other_suffixes_are_skipped(self, tmp_path):
        write(tmp_path / "historical_jobs_2025.parquet", [("1", "2025-01-01", "")])
        write(tmp_path / "historical_jobs_backup.parquet", [("9", "2025-01-01", "")])
        assert sorted(data_files(str(tmp_path))) == [2025]


class TestCoverageLabels:
    """Which year is the partial one has to come from the data.

    The ladder this replaced named 2025 as "current through" and gave every
    later year "Closing dates only", so on 2026-01-01 it began describing the
    year being collected as closing-dates-only.
    """
    def _labels(self, counts, monkeypatch, latest):
        import generate_docs_data as g
        monkeypatch.setattr(g, "get_latest_date", lambda: latest)
        return {row["year"]: row["coverage"]
                for row in analyze_data_coverage(counts)}

    def test_the_collected_year_is_the_current_one(self, monkeypatch):
        counts = {y: {"total": 1, "opened": 1, "closed": 1}
                  for y in (2016, 2025, 2026)}
        labels = self._labels(counts, monkeypatch, "2026-09-12")
        assert labels[2016] == "Very limited"
        assert labels[2025] == "✅ Complete year"
        assert labels[2026] == "Current through September 12, 2026"

    def test_a_year_past_the_collection_is_closing_dates_only(self, monkeypatch):
        counts = {y: {"total": 1, "opened": 1, "closed": 1} for y in (2026, 2027)}
        labels = self._labels(counts, monkeypatch, "2026-09-12")
        assert labels[2027] == "Closing dates only"


def test_a_file_without_a_control_number_is_an_error(tmp_path):
    # count(DISTINCT) drops NULLs, so this would undercount silently.
    pq.write_table(pa.table({
        "positionOpenDate": pa.array(["2025-01-01"], pa.string()),
    }), tmp_path / "current_jobs_2025.parquet")
    with pytest.raises(ValueError, match="usajobsControlNumber"):
        deduped_counts(str(tmp_path))
