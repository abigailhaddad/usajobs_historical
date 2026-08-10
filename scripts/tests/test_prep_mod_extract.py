"""Tests for prep_web_data._extract_mod_fields.

This function replaced five separate json.loads passes over
MatchedObjectDescriptor. Its contract is that it produces exactly what those
passes produced, including their quirks — notably that a blob which is present
but unparseable still counts as present, because the grade rewrite keyed off
the raw column being non-empty and blanked the grade fields in that case.
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from prep_web_data import _MOD_DERIVED, _extract_mod_fields


def fields(val):
    """Extract and label, so assertions read by name rather than tuple index."""
    return dict(zip(_MOD_DERIVED, _extract_mod_fields(val)))


@pytest.mark.parametrize("missing", [None, "", "   ", float("nan")])
def test_absent_blob_yields_all_none(missing):
    f = fields(missing)
    assert f["_had_mod"] is False
    assert all(f[k] is None for k in _MOD_DERIVED if k != "_had_mod")


@pytest.mark.parametrize("broken", ["{not json", "[1,2", "notjsonatall"])
def test_unparseable_blob_counts_as_present(broken):
    """Present-but-broken must set _had_mod so the grade rewrite still fires."""
    f = fields(broken)
    assert f["_had_mod"] is True
    assert f["_mod_low"] is None and f["_mod_high"] is None and f["_mod_payplan"] is None


def test_empty_object_is_present_with_no_values():
    f = fields("{}")
    assert f["_had_mod"] is True
    assert f["_mod_org"] is None


def test_org_name():
    assert fields(json.dumps({"OrganizationName": "Internal Revenue Service"}))["_mod_org"] \
        == "Internal Revenue Service"
    assert fields(json.dumps({"PositionTitle": "x"}))["_mod_org"] is None


@pytest.mark.parametrize("code,expected", [
    ("01", "Competitive"), ("02", "Excepted"), ("03", "Senior Executive"),
    ("99", "99"),  # unknown codes pass through rather than becoming None
])
def test_service_type(code, expected):
    blob = json.dumps({"UserArea": {"Details": {"ServiceType": code}}})
    assert fields(blob)["_mod_service"] == expected


def test_service_type_absent():
    assert fields(json.dumps({"UserArea": {"Details": {}}}))["_mod_service"] is None


def test_grade_fields():
    blob = json.dumps({"UserArea": {"Details": {"LowGrade": "07", "HighGrade": "09"}},
                       "JobGrade": [{"Code": "GS"}]})
    f = fields(blob)
    assert (f["_mod_low"], f["_mod_high"], f["_mod_payplan"]) == ("07", "09", "GS")


def test_grade_without_jobgrade_has_no_pay_plan():
    blob = json.dumps({"UserArea": {"Details": {"LowGrade": "13", "HighGrade": "13"}}})
    f = fields(blob)
    assert (f["_mod_low"], f["_mod_high"], f["_mod_payplan"]) == ("13", "13", None)


def test_series_codes():
    blob = json.dumps({"JobCategory": [{"Code": "2210"}, {"Code": "0343"}]})
    assert fields(blob)["_mod_series"] == ["2210", "0343"]


@pytest.mark.parametrize("blob", ['{"JobCategory": []}', '{}'])
def test_series_empty_is_none_not_empty_list(blob):
    """None matters: the caller's mask is .isna(), which [] would not satisfy."""
    assert fields(blob)["_mod_series"] is None


def test_locations_from_position_location():
    blob = json.dumps({"PositionLocation": [
        {"CityName": "Boston", "StateName": "Massachusetts"},
        {"CityName": "Reno", "StateName": "Nevada"}]})
    assert fields(blob)["_mod_locations"] == "Boston, Massachusetts; Reno, Nevada"


def test_locations_falls_back_to_display():
    blob = json.dumps({"PositionLocationDisplay": "Multiple Locations"})
    assert fields(blob)["_mod_locations"] == "Multiple Locations"


def test_locations_empty_list_falls_back_to_display():
    blob = json.dumps({"PositionLocation": [], "PositionLocationDisplay": "Anywhere"})
    assert fields(blob)["_mod_locations"] == "Anywhere"


def test_locations_absent():
    assert fields(json.dumps({"PositionTitle": "x"}))["_mod_locations"] is None


def test_dict_input_is_accepted_like_a_json_string():
    """Parquet may hand back a dict rather than a string; both must work."""
    obj = {"OrganizationName": "Census Bureau",
           "UserArea": {"Details": {"ServiceType": "02"}}}
    from_dict = fields(obj)
    from_str = fields(json.dumps(obj))
    assert from_dict == from_str
    assert from_dict["_mod_org"] == "Census Bureau"


def test_all_fields_together():
    blob = json.dumps({
        "OrganizationName": "Census Bureau",
        "UserArea": {"Details": {"ServiceType": "02", "LowGrade": "11", "HighGrade": "12"}},
        "JobGrade": [{"Code": "ZP"}],
        "JobCategory": [{"Code": "1560"}],
        "PositionLocation": [{"CityName": "Suitland", "StateName": "Maryland"}],
    })
    assert fields(blob) == {
        "_had_mod": True,
        "_mod_locations": "Suitland, Maryland",
        "_mod_series": ["1560"],
        "_mod_org": "Census Bureau",
        "_mod_service": "Excepted",
        "_mod_low": "11",
        "_mod_high": "12",
        "_mod_payplan": "ZP",
    }
