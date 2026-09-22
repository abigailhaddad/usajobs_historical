"""Tests for collect_current_data.py — grade field extraction."""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from collect_current_data import flatten_current_job


def _make_job_item(low_grade="7", high_grade="9", pay_plan="GS",
                    organization_codes="TEST/TEST1",
                    announcement_closing_type=None, announcement_closing_type_option=None,
                    extra_details=None, extra_top_level=None):
    """Build a minimal USAJobs API job item with grade fields."""
    details = {
        "LowGrade": low_grade,
        "HighGrade": high_grade,
        "ServiceType": "01",
        "OrganizationCodes": organization_codes,
    }
    if announcement_closing_type is not None:
        details["AnnouncementClosingType"] = announcement_closing_type
    if announcement_closing_type_option is not None:
        details["AnnouncementClosingTypeOption"] = announcement_closing_type_option
    if extra_details:
        details.update(extra_details)

    top_level = {
        "PositionTitle": "Test Analyst",
        "PositionURI": "https://www.usajobs.gov:443/job/123456789",
        "DepartmentName": "Test Dept",
        "PositionID": "TEST-001",
        "PositionStartDate": "2026-01-01",
        "PositionEndDate": "2026-12-31",
        "JobGrade": [{"Code": pay_plan}],
        "PositionRemuneration": [{"MinimumRange": "50000", "MaximumRange": "80000"}],
        "JobCategory": [{"Code": "0343"}],
        "UserArea": {
            "Details": details,
        },
    }
    if extra_top_level:
        top_level.update(extra_top_level)

    return {
        "MatchedObjectDescriptor": top_level
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


class TestFlattenOrganizationAndClosingType:
    """Regression for two fields silently dropped: OrganizationCodes lives under
    UserArea.Details (not top-level `job`), and AnnouncementClosingTypeOption is
    the applicant-cap number for "Applicant Cut-Off" (code 03) postings.
    """

    def test_agency_and_department_code_split_from_organization_codes(self):
        job = _make_job_item(organization_codes="VA/VATA")
        flat = flatten_current_job(job, {}, {})

        assert flat["hiringDepartmentCode"] == "VA"
        assert flat["hiringAgencyCode"] == "VATA"

    def test_missing_organization_codes_is_none(self):
        job = _make_job_item(organization_codes=None)
        flat = flatten_current_job(job, {}, {})

        assert flat["hiringDepartmentCode"] is None
        assert flat["hiringAgencyCode"] is None

    def test_applicant_cutoff_cap_extracted(self):
        job = _make_job_item(announcement_closing_type="03", announcement_closing_type_option="50")
        flat = flatten_current_job(job, {}, {})

        assert flat["announcementClosingTypeCode"] == "03"
        assert flat["announcementClosingTypeDescription"] == "Applicant Cut-Off"
        assert flat["applicationCap"] == 50

    def test_closing_date_type_has_no_cap(self):
        job = _make_job_item(announcement_closing_type="01")
        flat = flatten_current_job(job, {}, {})

        assert flat["announcementClosingTypeCode"] == "01"
        assert flat["announcementClosingTypeDescription"] == "Closing Date"
        assert flat["applicationCap"] is None

    def test_closing_date_type_option_echo_is_not_mistaken_for_a_cap(self):
        """Regression: on real API responses, "01" (Closing Date) postings have
        AnnouncementClosingTypeOption == "01" too -- just an echo of the type
        code, not a cap. int("01") == 1 would silently look like a real cap."""
        job = _make_job_item(announcement_closing_type="01", announcement_closing_type_option="01")
        flat = flatten_current_job(job, {}, {})

        assert flat["applicationCap"] is None


class TestFlattenAdditionalFields:
    """Regression for the broader field audit: TravelCode has the same wrong-dict
    bug as OrganizationCodes, and several real fields (location, promotion
    potential, who-may-apply, remote/financial-disclosure/union/sensitivity
    flags) were never extracted at all despite having real values in the raw
    API response.
    """

    def test_travel_requirement_read_from_user_area_not_top_level(self):
        """Regression: job.get("TravelCode") was always None -- TravelCode lives
        under UserArea.Details, same mistake as the OrganizationCodes bug."""
        job = _make_job_item(extra_details={"TravelCode": "1"})
        flat = flatten_current_job(job, {}, {})

        assert flat["travelRequirement"] == "1"

    def test_boolean_flags_convert_to_yn(self):
        job = _make_job_item(extra_details={
            "RemoteIndicator": True,
            "FinancialDisclosure": False,
            "BargainingUnitStatus": True,
        })
        flat = flatten_current_job(job, {}, {})

        assert flat["remoteIndicator"] == "Y"
        assert flat["financialDisclosureRequired"] == "N"
        assert flat["representedByUnion"] == "Y"

    def test_missing_boolean_flags_are_none_not_n(self):
        """A missing flag should stay None/unknown, not silently look like "N"."""
        job = _make_job_item()
        flat = flatten_current_job(job, {}, {})

        assert flat["remoteIndicator"] is None
        assert flat["financialDisclosureRequired"] is None
        assert flat["representedByUnion"] is None

    def test_position_sensitivity_and_promotion_potential(self):
        job = _make_job_item(extra_details={
            "PositionSensitivitiy": "Non-sensitive (NS)/Low Risk",  # API's own spelling
            "PromotionPotential": "12",
        })
        flat = flatten_current_job(job, {}, {})

        assert flat["positionSensitivity"] == "Non-sensitive (NS)/Low Risk"
        assert flat["promotionPotential"] == "12"

    def test_who_may_apply_prefers_name_over_code(self):
        job = _make_job_item(extra_details={"WhoMayApply": {"Name": "All U.S. Citizens", "Code": "15317"}})
        flat = flatten_current_job(job, {}, {})

        assert flat["whoMayApply"] == "All U.S. Citizens"

    def test_who_may_apply_falls_back_to_code(self):
        job = _make_job_item(extra_details={"WhoMayApply": {"Name": "", "Code": "15317"}})
        flat = flatten_current_job(job, {}, {})

        assert flat["whoMayApply"] == "15317"

    def test_position_location_extracted_from_top_level(self):
        """PositionLocation lives at top level (job), not UserArea.Details."""
        job = _make_job_item(extra_top_level={
            "PositionLocationDisplay": "Fayetteville, Arkansas",
            "PositionLocation": [{
                "CityName": "Fayetteville, Arkansas",
                "CountrySubDivisionCode": "Arkansas",
                "CountryCode": "United States",
                "Latitude": 36.0632,
                "Longitude": -94.1579,
            }],
        })
        flat = flatten_current_job(job, {}, {})

        assert flat["positionLocationDisplay"] == "Fayetteville, Arkansas"
        assert flat["positionLocationCount"] == 1
        assert "Fayetteville" in flat["PositionLocations"]

    def test_no_position_location_is_none(self):
        job = _make_job_item()
        flat = flatten_current_job(job, {}, {})

        assert flat["positionLocationCount"] is None
        assert flat["PositionLocations"] is None


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
