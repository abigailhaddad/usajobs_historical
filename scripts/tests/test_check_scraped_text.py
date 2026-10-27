"""Tests for check_scraped_text.py -- the daily check on stored section text."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from check_scraped_text import (agency_entities, api_check,  # noqa: E402
                                pattern_check)

QUALS = "Applicants must have one year of specialized experience at GS-11."


def row(cn="1", **sections):
    r = {"usajobs_control_number": cn, "qualificationSummary": QUALS,
         "education": "A transcript is required.",
         "additionalInformation": "Relocation is not paid."}
    r.update(sections)
    return r


def api(cn="1", quals=QUALS, other="Relocation is not paid."):
    return {cn: {"QualificationSummary": quals, "UserArea": {"Details": {
        "Education": "A transcript is required.",
        "OtherInformation": other}}}}


def test_clean_row_has_no_marks():
    hits = pattern_check([row()])
    assert all(not v for v in hits.values())


def test_button_label_and_run_on_are_caught():
    hits = pattern_check([
        row("1", howToApply="Apply. Expand Hide next steps We call."),
        row("2", qualificationSummary=QUALS + " Education A transcript is "
                                              "required.")])
    assert hits["button_label"] == ["1"]
    assert hits["run_on"] == ["2"]


def test_agency_text_is_not_a_mark():
    # Both are common in agencies' own text: a template blank before a
    # period, and an email address.
    hits = pattern_check([row(howToApply="Apply by . Email Jo.Doe@va.gov")])
    assert all(not v for v in hits.values())


def test_matching_api_text_passes():
    result, spacing = api_check([row()], api())
    assert result["qualificationSummary"] == [1, []]
    assert spacing == []


def test_run_on_disagrees_with_the_api():
    long = QUALS + " Education A transcript is required. Additional " \
                   "information Relocation is not paid."
    result, _ = api_check([row(qualificationSummary=long)], api())
    assert [cn for cn, _ in result["qualificationSummary"][1]] == ["1"]


def test_site_text_added_to_a_section_is_allowed_but_a_cut_is_not():
    padded = row(additionalInformation="Relocation is not paid. Candidates "
                 "should be committed to improving the efficiency of the "
                 "Federal government.")
    result, _ = api_check([padded], api())
    assert result["additionalInformation"] == [1, []]
    cut = row(additionalInformation="Relocation")
    result, _ = api_check([cut], api())
    assert len(result["additionalInformation"][1]) == 1


def test_space_before_punctuation_only_counts_when_the_api_lacks_it():
    spaced = row(qualificationSummary=QUALS.replace("GS-11.", "GS-11 ."))
    _, spacing = api_check([spaced], api())
    assert spacing == ["1"]
    _, spacing = api_check([spaced], api(quals=QUALS.replace("GS-11.",
                                                             "GS-11 .")))
    assert spacing == []


def test_entity_in_the_api_text_is_the_agencys_own():
    # 890036200, 26 October 2026: the agency's HTML has &amp;#8217;, so the
    # page shows "&#8217;" and the scrape stores it as shown.
    duty = "Improve personnel&#8217;s expertise."
    rows = [row("1", majorDuties=duty), row("2", majorDuties=duty),
            row("3", majorDuties=duty)]
    hits = pattern_check(rows)
    assert hits["html_entity"] == ["1", "2", "3"]
    a = api("1")
    a["1"]["UserArea"]["Details"]["MajorDuties"] = [duty]
    a.update(api("2"))  # the API decoded it: the parse is suspect
    a["2"]["UserArea"]["Details"]["MajorDuties"] = [
        "Improve personnel’s expertise."]
    # "3" is not in the API at all: still counted.
    assert agency_entities(rows, hits["html_entity"], a) == ["1"]


def test_every_entity_must_be_in_the_api_text():
    rows = [row("1", majorDuties="A&#8217;s duty &amp; more.")]
    a = api("1")
    a["1"]["UserArea"]["Details"]["MajorDuties"] = ["A&#8217;s duty & more."]
    assert agency_entities(rows, ["1"], a) == []


def test_rows_the_api_does_not_have_are_skipped():
    result, spacing = api_check([row("2")], api("1"))
    assert all(n == 0 for n, _ in result.values()) and spacing == []
