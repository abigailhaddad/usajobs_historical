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
from publish_to_huggingface import METADATA_FIELDS, TEXT_FIELDS, partition_safe_months


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


class TestPublishedSchema:
    def test_the_job_title_is_selected(self):
        # The field list this replaced omitted positionTitle, so the published
        # dataset had no job title in it at all.
        assert "h.positionTitle," in METADATA_FIELDS
        assert "h.hiringSubelementName," in METADATA_FIELDS

    def test_bookkeeping_columns_are_not_published(self):
        for column in ("inserted_at", "last_seen", "usajobs_control_number"):
            assert f"h.{column}" not in METADATA_FIELDS

    def test_the_empty_integer_columns_are_not_published(self):
        # HiringPaths / JobCategories / PositionLocations are empty integer
        # columns in the mirror, superseded by the *_1 varchars.
        assert "h.HiringPaths" not in METADATA_FIELDS
        assert "h.hiringpaths_1" in METADATA_FIELDS

    def test_every_announcement_section_is_published(self):
        for section in ("jobSummary", "majorDuties", "qualificationSummary",
                        "education", "requiredDocuments", "howToApply", "text"):
            assert section in TEXT_FIELDS
        assert len(TEXT_FIELDS) == 12
