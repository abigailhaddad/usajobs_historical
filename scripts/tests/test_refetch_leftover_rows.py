"""Tests for refetch_leftover_rows.py -- finding the rows the text repair
left, and swapping in only their section fields."""
import os
import sys

import pyarrow as pa
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from refetch_leftover_rows import (leftover_kinds, replace_rows,  # noqa: E402
                                   verify, SECTION_FIELDS)


def test_education_fragment_inside_qualifications_is_a_leftover():
    row = {"qualificationSummary": "Have a degree. Education A transcript. "
                                   "Desired: more.",
           "education": "A transcript."}
    assert leftover_kinds(row) == ["education_in_qualificationSummary"]


def test_qualifications_inside_conditions_is_a_leftover():
    row = {"conditionsOfEmployment": "Citizens. Qualifications Have a degree.",
           "qualificationSummary": "Have a degree."}
    assert leftover_kinds(row) == ["quals_in_conditionsOfEmployment"]


def test_repaired_row_is_not_a_leftover():
    row = {"qualificationSummary": "Have a degree.",
           "education": "A transcript.", "additionalInformation": "Other."}
    assert leftover_kinds(row) == []


def table():
    return pa.table({
        "usajobsControlNumber": ["1", "2", "1"],
        "positionTitle": ["A", "B", "A"],
        "qualificationSummary": ["old", "keep", "old"],
        "education": ["old edu", "keep edu", "old edu"],
    })


def test_only_matching_rows_and_section_fields_change():
    fresh = {"1": {f: None for f in SECTION_FIELDS}}
    fresh["1"]["qualificationSummary"] = "new"
    before = table()
    after, n = replace_rows(before, fresh)
    verify(before, after)
    assert n == 2
    assert after.column("qualificationSummary").to_pylist() == [
        "new", "keep", "new"]
    # A field the fresh page does not have comes out empty, as the page says.
    assert after.column("education").to_pylist() == [None, "keep edu", None]
    assert after.column("positionTitle").equals(before.column("positionTitle"))


def test_verify_refuses_a_change_outside_the_sections():
    before = table()
    after = before.set_column(1, "positionTitle", pa.array(["X", "B", "A"]))
    with pytest.raises(SystemExit):
        verify(before, after)
