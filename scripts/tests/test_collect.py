"""Tests for collect_current_data.py — grade field extraction."""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from collect_current_data import flatten_current_job


def _make_job_item(low_grade="7", high_grade="9", pay_plan="GS"):
    """Build a minimal USAJobs API job item with grade fields."""
    return {
        "MatchedObjectDescriptor": {
            "PositionTitle": "Test Analyst",
            "PositionURI": "https://www.usajobs.gov:443/job/123456789",
            "DepartmentName": "Test Dept",
            "OrganizationCodes": "TEST",
            "PositionID": "TEST-001",
            "PositionStartDate": "2026-01-01",
            "PositionEndDate": "2026-12-31",
            "JobGrade": [{"Code": pay_plan}],
            "PositionRemuneration": [{"MinimumRange": "50000", "MaximumRange": "80000"}],
            "JobCategory": [{"Code": "0343"}],
            "UserArea": {
                "Details": {
                    "LowGrade": low_grade,
                    "HighGrade": high_grade,
                    "ServiceType": "01",
                }
            },
        }
    }


class TestFlattenGradeFields:
    """Verify grade numbers come from UserArea.Details, not JobGrade."""

    def test_grade_numbers_extracted_from_user_area(self):
        """minimumGrade/maximumGrade should be numeric grade levels, not pay plan code."""
        job = _make_job_item(low_grade="7", high_grade="9", pay_plan="GS")
        flat = flatten_current_job(job, {}, {})

        assert flat["minimumGrade"] == "7", \
            f"minimumGrade should be '7' (from LowGrade), got '{flat['minimumGrade']}'"
        assert flat["maximumGrade"] == "9", \
            f"maximumGrade should be '9' (from HighGrade), got '{flat['maximumGrade']}'"

    def test_pay_scale_extracted_from_job_grade(self):
        """payScale should be the pay plan code from JobGrade."""
        job = _make_job_item(pay_plan="GS")
        flat = flatten_current_job(job, {}, {})

        assert flat["payScale"] == "GS", \
            f"payScale should be 'GS' (from JobGrade[0].Code), got '{flat.get('payScale')}'"

    def test_single_grade_level(self):
        """When LowGrade == HighGrade, both should be the same number."""
        job = _make_job_item(low_grade="12", high_grade="12", pay_plan="GS")
        flat = flatten_current_job(job, {}, {})

        assert flat["minimumGrade"] == "12"
        assert flat["maximumGrade"] == "12"

    def test_non_gs_pay_plan(self):
        """Other pay plans (WG, NH, etc.) should also extract correctly."""
        job = _make_job_item(low_grade="10", high_grade="10", pay_plan="WG")
        flat = flatten_current_job(job, {}, {})

        assert flat["payScale"] == "WG"
        assert flat["minimumGrade"] == "10"

    def test_missing_grade_details(self):
        """If UserArea.Details has no grade info, fields should be None."""
        job = _make_job_item()
        job["MatchedObjectDescriptor"]["UserArea"]["Details"].pop("LowGrade")
        job["MatchedObjectDescriptor"]["UserArea"]["Details"].pop("HighGrade")
        flat = flatten_current_job(job, {}, {})

        assert flat["minimumGrade"] is None
        assert flat["maximumGrade"] is None


class TestUnifiedSchemaNullPromotion:
    """Regression for a run killed on 2026-09-08.

    A column the old file never held a value for is typed `null`. Nothing
    casts to `null`, so once real values arrived the streaming merge failed
    with "Unsupported cast from large_string to null" and fell back to the
    in-memory path, which reads the whole parquet and hit the memory ceiling.
    """

    def _schemas(self, existing, new):
        import pyarrow as pa
        from collect_current_data import _unified_schema
        return _unified_schema(pa.schema(existing), pa.schema(new))

    def test_a_concrete_type_beats_a_null_one(self):
        import pyarrow as pa
        s = self._schemas([("text", pa.null())], [("text", pa.large_string())])
        assert s.field("text").type == pa.large_string()

    def test_an_existing_concrete_type_is_kept(self):
        import pyarrow as pa
        s = self._schemas([("text", pa.string())], [("text", pa.null())])
        assert s.field("text").type == pa.string()

    def test_column_order_is_preserved(self):
        import pyarrow as pa
        s = self._schemas(
            [("a", pa.string()), ("b", pa.null())],
            [("b", pa.string()), ("c", pa.int64())])
        assert s.names == ["a", "b", "c"]

    def test_conform_can_write_a_real_column_into_a_null_field(self):
        import pyarrow as pa
        from collect_current_data import _conform
        table = pa.table({"text": pa.array(["a", "b"], pa.large_string())})
        out = _conform(table, pa.schema([("text", pa.null())]))
        assert out.num_rows == 2
        assert out.schema.field("text").type == pa.null()
