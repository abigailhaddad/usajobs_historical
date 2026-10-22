"""Tests for repair_section_leaks.py -- undoing what the parser stored before
PR #668, from the stored text alone."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from repair_section_leaks import QUESTIONNAIRE, repair_row

AI = ("This is a Temporary Promotion. Expand Hide additional information "
      "Candidates should be committed to upholding the rule of law.")


def stored(**overrides):
    """A row the way the old parser stored it."""
    row = {
        "jobSummary": "The Chief serves the medical center. "
                      "Learn more about this agency",
        "majorDuties": "Duties include oversight.",
        "conditionsOfEmployment": "You must be a U.S. Citizen.",
        "qualificationSummary": "Must have one year of experience. Education "
                                "A transcript is required. Additional "
                                "information " + AI,
        "education": "A transcript is required.",
        "additionalInformation": AI,
        "requirements": "Conditions of employment ... " + AI,
        "howToApply": "Apply online. Expand Hide how to apply Agency contact "
                      "Jane Doe Expand Hide next steps We will notify you.",
    }
    row.update(overrides)
    return row


def test_run_on_is_cut_when_it_ends_exactly_with_the_wrapped_sections():
    fixed, fixes = repair_row(stored())
    assert fixed["qualificationSummary"] == "Must have one year of experience."
    assert "run_on_qualificationSummary" in fixes


def test_only_the_part_that_matches_exactly_is_cut():
    # The stored education differs from what trails qualifications, so that
    # part stays (and leftovers() counts it); the exact trailing match goes.
    row = stored(education="Something else.")
    fixed, _ = repair_row(row)
    assert fixed["qualificationSummary"] == (
        "Must have one year of experience. Education A transcript is required.")


def test_run_on_is_left_alone_when_nothing_matches():
    row = stored(education="Something else.", additionalInformation="Other.")
    fixed, fixes = repair_row(row)
    assert "run_on_qualificationSummary" not in fixes
    assert fixed["qualificationSummary"].endswith("rule of law.")


def test_only_additional_information_trailing():
    row = stored(education=None,
                 qualificationSummary="Must have experience. Additional "
                                      "information " + AI)
    fixed, _ = repair_row(row)
    assert fixed["qualificationSummary"] == "Must have experience."


def test_section_that_is_only_the_run_on_becomes_empty():
    # An announcement whose Qualifications heading has no text of its own.
    row = stored(education=None,
                 qualificationSummary="Additional information " + AI)
    fixed, _ = repair_row(row)
    assert fixed["qualificationSummary"] is None


def test_agency_written_ending_survives_once_the_button_is_gone():
    # 495604800: the agency ended its summary with the same words.
    row = stored(jobSummary="Meet deadlines. Learn more about this agency "
                            "Learn more about this agency")
    fixed, _ = repair_row(row)
    assert fixed["jobSummary"] == "Meet deadlines. Learn more about this agency"


def test_conditions_run_on_through_nested_qualifications():
    # 429653100: broken agency HTML nested the later headings in a <p>.
    row = stored(
        education=None,
        qualificationSummary="Have a degree.",
        conditionsOfEmployment="Citizens. Submit transcripts. Qualifications "
                               "Have a degree. Additional information " + AI)
    fixed, fixes = repair_row(row)
    assert fixed["conditionsOfEmployment"] == "Citizens. Submit transcripts."
    assert fixed["qualificationSummary"] == "Have a degree."


def test_button_labels_come_out_of_every_section_that_had_them():
    fixed, _ = repair_row(stored())
    assert fixed["additionalInformation"] == (
        "This is a Temporary Promotion. Candidates should be committed to "
        "upholding the rule of law.")
    assert fixed["howToApply"] == ("Apply online. Agency contact Jane Doe We "
                                   "will notify you.")
    assert "Expand Hide" not in fixed["requirements"]


def test_agency_link_only_comes_off_the_end_of_job_summary():
    row = stored(majorDuties="Reports to the Board. Learn more about this "
                             "agency APPLICATION LIMIT: 100.")
    fixed, _ = repair_row(row)
    assert fixed["jobSummary"] == "The Chief serves the medical center."
    assert fixed["majorDuties"] == row["majorDuties"]


def test_rows_from_the_fixed_parser_keep_an_agency_written_ending():
    row = {"jobSummary": "Meet deadlines. Learn more about this agency",
           "howToApply": "Apply online.", "additionalInformation": None,
           "education": None, "qualificationSummary": "Have a degree."}
    fixed, fixes = repair_row(row)
    assert fixes == {} and fixed is row


def test_questionnaire_block_is_removed():
    row = stored(qualificationSummary="Must have experience. " + QUESTIONNAIRE
                 + " Additional information " + AI, education=None)
    fixed, _ = repair_row(row)
    assert fixed["qualificationSummary"] == "Must have experience."


def test_clean_row_is_returned_unchanged():
    row = {"jobSummary": "Plain.", "qualificationSummary": "Have a degree.",
           "additionalInformation": None, "education": None}
    fixed, fixes = repair_row(row)
    assert fixes == {} and fixed is row
