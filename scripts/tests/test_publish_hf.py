"""Tests for publish_to_huggingface.py — the guard against shrinking a month.

Month files are rewritten wholesale. Publishing one built from an incomplete
local join deletes announcements the dataset already holds, and nothing on the
dataset side can recover them. That is the normal state while
backfill_scraped_pages.py is still running, which is exactly when a daily
publish would otherwise fire.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from publish_to_huggingface import (TEXT_FIELDS, metadata_fields,
                                    partition_safe_months,
                                    prune_published_text, resolve_list_columns)


class TestPartitionSafeMonths:
    def test_a_month_gaining_rows_is_safe(self):
        safe, refused = partition_safe_months(
            todo=["2026-09"],
            months={"2026-09": {"1", "2", "3"}},
            have={"1", "2"},
            month_of={"1": "2026-09", "2": "2026-09", "3": "2026-09"})
        assert safe == ["2026-09"]
        assert refused == []

    def test_a_month_that_would_lose_rows_is_refused(self):
        safe, refused = partition_safe_months(
            todo=["2026-09"],
            months={"2026-09": {"1", "9"}},          # local join lost "2"
            have={"1", "2"},
            month_of={"1": "2026-09", "2": "2026-09", "9": "2026-09"})
        assert safe == []
        assert refused == [("2026-09", 1, 2)]

    def test_one_bad_month_does_not_block_a_good_one(self):
        safe, refused = partition_safe_months(
            todo=["2026-08", "2026-09"],
            months={"2026-08": set(), "2026-09": {"3"}},
            have={"1", "3"},
            month_of={"1": "2026-08", "3": "2026-09"})
        assert safe == ["2026-09"]
        assert [m for m, *_ in refused] == ["2026-08"]

    def test_published_rows_from_another_month_are_not_this_month_s_problem(self):
        safe, refused = partition_safe_months(
            todo=["2026-09"],
            months={"2026-09": {"3"}},
            have={"1", "3"},
            month_of={"1": "2026-01", "3": "2026-09"})
        assert safe == ["2026-09"]

    def test_a_published_row_of_unknown_month_is_ignored(self):
        # A control number the local metadata has never heard of -- a posting
        # from another year, say -- cannot be attributed to this month, so it
        # must not veto the rebuild.
        safe, refused = partition_safe_months(
            todo=["2026-09"],
            months={"2026-09": {"3"}},
            have={"unknown", "3"},
            month_of={"3": "2026-09"})
        assert safe == ["2026-09"]

    def test_empty_dataset_publishes_everything(self):
        safe, refused = partition_safe_months(
            todo=["2026-01", "2026-02"],
            months={"2026-01": {"1"}, "2026-02": {"2"}},
            have=set(),
            month_of={"1": "2026-01", "2": "2026-02"})
        assert safe == ["2026-01", "2026-02"]
        assert refused == []


class TestGuardUsesTheExistingFile:
    """Regression for the 2026-09-04 refusal.

    The guard read the manifest cross-referenced against current metadata to
    decide what a month already holds. Which file a posting actually lives in
    was fixed by its open date at publish time, and the daily collection
    refreshes the last 14 days, so open dates shift and the two disagree. That
    refused 2026-09 over 4 postings sitting safely in another month's file, and
    blocked the daily publish.
    """

    def test_the_prior_file_beats_the_manifest(self):
        # "4" is in the manifest and maps to this month by today's metadata,
        # but is not in the month file, so it is not this month's to lose.
        safe, refused = partition_safe_months(
            todo=["2026-09"],
            months={"2026-09": {"1", "2", "3"}},
            have={"1", "2", "3", "4"},
            month_of={c: "2026-09" for c in "1234"},
            priors={"2026-09": {"1", "2", "3"}})
        assert safe == ["2026-09"]
        assert refused == []

    def test_a_genuine_shrink_is_still_refused(self):
        # "3" IS in the month file and would not survive the rebuild.
        safe, refused = partition_safe_months(
            todo=["2026-09"],
            months={"2026-09": {"1", "2"}},
            have={"1", "2", "3"},
            month_of={c: "2026-09" for c in "123"},
            priors={"2026-09": {"1", "2", "3"}})
        assert safe == []
        assert refused == [("2026-09", 1, 3)]

    def test_no_prior_file_falls_back_to_the_manifest(self):
        # Download failed or the month was never published: the manifest is
        # all there is, so keep the conservative check.
        safe, refused = partition_safe_months(
            todo=["2026-09"],
            months={"2026-09": {"1"}},
            have={"1", "2"},
            month_of={c: "2026-09" for c in "12"},
            priors={})
        assert safe == []
        assert [m for m, *_ in refused] == ["2026-09"]

    def test_an_empty_prior_is_not_the_same_as_a_missing_one(self):
        # A month file that exists and is empty cannot lose anything.
        safe, refused = partition_safe_months(
            todo=["2026-09"],
            months={"2026-09": {"1"}},
            have={"1", "2"},
            month_of={c: "2026-09" for c in "12"},
            priors={"2026-09": set()})
        assert safe == ["2026-09"]


class TestResolveListColumns:
    """Regression for the 2019 crash.

    Each list-valued field has been spelled three ways in the mirror, and
    which one holds the data varies by year and over time. Hardcoding the
    2026 shape crashed on 2019 with 'does not have a column named
    hiringpaths_1'.
    """

    def _schema(self, tmp_path, columns):
        import pyarrow as pa
        import pyarrow.parquet as pq
        path = tmp_path / "mirror.parquet"
        pq.write_table(pa.table({n: pa.array([None], type=t)
                                 for n, t in columns.items()}), path)
        return str(path)

    def test_the_suffixed_varchar_wins_when_present(self, tmp_path):
        import pyarrow as pa
        path = self._schema(tmp_path, {"hiringpaths_1": pa.string(),
                                       "hiringpaths": pa.string(),
                                       "HiringPaths": pa.string()})
        assert resolve_list_columns(path)["hiringPaths"] == "h.hiringpaths_1"

    def test_falls_back_when_the_suffixed_one_is_absent(self, tmp_path):
        # 2019, 2021, 2022, 2023 have only the capitalised column.
        import pyarrow as pa
        path = self._schema(tmp_path, {"HiringPaths": pa.string()})
        assert resolve_list_columns(path)["hiringPaths"] == "h.HiringPaths"

    def test_the_current_lowercase_spelling(self, tmp_path):
        import pyarrow as pa
        path = self._schema(tmp_path, {"hiringpaths": pa.string()})
        assert resolve_list_columns(path)["hiringPaths"] == "h.hiringpaths"

    def test_a_null_typed_column_does_not_win(self, tmp_path):
        # Present but entirely empty, which is what the capitalised ones are
        # in the recent mirrors. It must lose to a real string column.
        import pyarrow as pa
        path = self._schema(tmp_path, {"HiringPaths": pa.null(),
                                       "hiringpaths": pa.string()})
        assert resolve_list_columns(path)["hiringPaths"] == "h.hiringpaths"

    def test_large_string_counts(self, tmp_path):
        # pyarrow distinguishes string from large_string; the mirror uses both.
        import pyarrow as pa
        path = self._schema(tmp_path, {"hiringpaths": pa.large_string()})
        assert resolve_list_columns(path)["hiringPaths"] == "h.hiringpaths"

    def test_no_candidate_at_all_yields_null_not_a_crash(self, tmp_path):
        import pyarrow as pa
        path = self._schema(tmp_path, {"positionTitle": pa.string()})
        assert resolve_list_columns(path)["hiringPaths"] == "CAST(NULL AS VARCHAR)"


class TestPublishedSchema:
    def _fields(self, tmp_path):
        import pyarrow as pa
        import pyarrow.parquet as pq
        path = tmp_path / "m.parquet"
        pq.write_table(pa.table({"hiringpaths_1": pa.array([None], pa.string())}),
                       path)
        return metadata_fields(str(path))

    def test_the_job_title_is_selected(self, tmp_path):
        # The field list this replaced omitted positionTitle, so the published
        # dataset had no job title in it at all.
        fields = self._fields(tmp_path)
        assert "h.positionTitle," in fields
        assert "h.hiringSubelementName," in fields

    def test_bookkeeping_columns_are_not_published(self, tmp_path):
        fields = self._fields(tmp_path)
        for column in ("inserted_at", "last_seen", "usajobs_control_number",
                       "backfilled"):
            assert f"h.{column}" not in fields

    def test_who_may_apply_is_cast_for_a_stable_column_type(self, tmp_path):
        # VARCHAR through 2024, empty INTEGER from 2025. Without the cast the
        # month files disagree and the dataset cannot be read as a whole.
        assert "CAST(h.whoMayApply AS VARCHAR)" in self._fields(tmp_path)

    def test_every_announcement_section_is_published(self):
        for section in ("jobSummary", "majorDuties", "qualificationSummary",
                        "education", "requiredDocuments", "howToApply", "text"):
            assert section in TEXT_FIELDS
        assert len(TEXT_FIELDS) == 12


class TestPrunePublishedText:
    """Announcement text is 97.8% of scraped_jobs_{year}.parquet — 5.42 KB of a
    5.54 KB row, so a full year is 920 MB against 20 MB for everything else.
    Once a posting is on HuggingFace the dataset is the store for its text, and
    what stays local is the structured shadow the API comparison reads.
    """

    def _parquet(self, tmp_path, rows):
        import pandas as pd
        path = tmp_path / "scraped_jobs_2026.parquet"
        pd.DataFrame(rows).to_parquet(path, index=False, compression="zstd")
        return str(path)

    def _rows(self):
        return [
            {"usajobs_control_number": "1", "positionTitle": "Analyst",
             "minimumSalary": 50000.0, "text": "page one",
             "qualificationSummary": "quals one", "majorDuties": "duties one"},
            {"usajobs_control_number": "2", "positionTitle": "Engineer",
             "minimumSalary": 90000.0, "text": "page two",
             "qualificationSummary": "quals two", "majorDuties": "duties two"},
        ]

    def test_text_is_dropped_only_on_published_rows(self, tmp_path):
        import pandas as pd
        path = self._parquet(tmp_path, self._rows())
        assert prune_published_text(path, {"1"}) == 1

        out = pd.read_parquet(path).set_index("usajobs_control_number")
        assert pd.isna(out.loc["1", "text"])
        assert pd.isna(out.loc["1", "qualificationSummary"])
        assert out.loc["2", "text"] == "page two"

    def test_structured_columns_survive(self, tmp_path):
        import pandas as pd
        path = self._parquet(tmp_path, self._rows())
        prune_published_text(path, {"1", "2"})
        out = pd.read_parquet(path).set_index("usajobs_control_number")
        assert out.loc["1", "positionTitle"] == "Analyst"
        assert out.loc["2", "minimumSalary"] == 90000.0

    def test_no_rows_are_lost(self, tmp_path):
        import pandas as pd
        path = self._parquet(tmp_path, self._rows())
        prune_published_text(path, {"1", "2"})
        assert len(pd.read_parquet(path)) == 2

    def test_the_file_actually_shrinks(self, tmp_path):
        path = self._parquet(tmp_path, self._rows())
        before = os.path.getsize(path)
        prune_published_text(path, {"1", "2"})
        assert os.path.getsize(path) < before

    def test_publishing_nothing_changes_nothing(self, tmp_path):
        path = self._parquet(tmp_path, self._rows())
        assert prune_published_text(path, set()) == 0

    def test_a_missing_file_is_not_an_error(self, tmp_path):
        assert prune_published_text(str(tmp_path / "absent.parquet"), {"1"}) == 0

    def test_a_file_with_no_text_columns_is_left_alone(self, tmp_path):
        import pandas as pd
        path = self._parquet(tmp_path, [
            {"usajobs_control_number": "1", "positionTitle": "Analyst"}])
        assert prune_published_text(path, {"1"}) == 0
        assert pd.read_parquet(path).loc[0, "positionTitle"] == "Analyst"
