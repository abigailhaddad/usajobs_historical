"""Tests for usajobs_scrape.py — announcement-page parsing.

The fixtures are two real pages captured 2026-09-02, gzipped: one open
announcement (CIA Honors Attorney) and one closed, canceled one (Air National
Guard IT Specialist). Between them they cover the branches that differ by
state — the status badge, and open/close dates, which closed pages state
explicitly and open ones leave for client-side JS.

If usajobs.gov changes its markup these fail, which is the point: the daily
collection would otherwise write rows of nulls and only the API comparison
would notice, a day later.
"""
import gzip
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from usajobs_scrape import (SECTION_FIELDS, normalize_search_job,
                            parse_job_page, parse_locations, parse_sections)

FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures')


def load(name):
    with gzip.open(os.path.join(FIXTURES, name), 'rt', encoding='utf-8') as f:
        return f.read()


@pytest.fixture(scope="module")
def open_job():
    return parse_job_page(load('job_open_721962100.html.gz'))


@pytest.fixture(scope="module")
def closed_job():
    return parse_job_page(load('job_closed_806611100.html.gz'))


@pytest.fixture(scope="module")
def job():
    return parse_job_page(load('job_open_889459300.html.gz'))


class TestOpenAnnouncement:
    def test_identity(self, open_job):
        assert open_job['usajobsControlNumber'] == 721962100
        assert open_job['usajobs_control_number'] == '721962100'
        assert open_job['announcementNumber'] == '23-11939789-7219/SEHD'

    def test_title_comes_from_the_header_not_ld_json(self, open_job):
        # ld+json carries a shortened title that drops the parenthetical
        # specialty; the API returns the full one, so we read the <h1>.
        assert open_job['positionTitle'] == 'Honors Attorney'

    def test_status_badge(self, open_job):
        assert open_job['positionOpeningStatus'] == 'Accepting applications'

    def test_dates_fall_back_to_ld_json(self, open_job):
        # An open page leaves #open-dates for client-side JS to fill, so these
        # can only come from the ld+json block.
        assert open_job['positionOpenDate'] == '2026-06-01'
        assert open_job['positionCloseDate'] == '2026-09-30'

    def test_salary_and_grade(self, open_job):
        assert open_job['minimumSalary'] == 93994.0
        assert open_job['maximumSalary'] == 121785.0
        assert open_job['salaryType'] == 'Per Year'
        assert open_job['payScale'] == 'GS'
        assert open_job['minimumGrade'] == '11'
        assert open_job['maximumGrade'] == '13'

    def test_yes_no_fields_become_api_style_flags(self, open_job):
        assert open_job['supervisoryStatus'] == 'N'
        assert open_job['teleworkEligible'] == 'N'
        assert open_job['remoteJob'] == 'N'
        # 'Yes—You may qualify for reimbursement of relocation expenses...'
        assert open_job['relocationExpensesReimbursed'] == 'Y'
        assert open_job['drugTestRequired'] == 'Y'

    def test_service_type_is_normalized(self, open_job):
        # Page says 'This job is in the Excepted Service'.
        assert open_job['serviceType'] == 'Excepted'

    def test_position_sensitivity_value_is_a_help_link(self, open_job):
        # This value is itself a help.usajobs.gov link, so the rule that
        # strips help blurbs must key on the .text-xs class, not on the link.
        assert open_job['positionSensitivity'] == 'Special-Sensitive (SS)/High Risk'

    def test_pay_scale_dd_nests_another_dl(self, open_job):
        # 'Pay scale & grade' wraps the promotion-potential <dl>; both must
        # come out clean.
        assert open_job['promotionPotential'] == 'None'

    def test_non_numeric_vacancy_count(self, open_job):
        assert open_job['totalOpenings'] == 'Many'

    def test_series_and_hiring_paths(self, open_job):
        assert json.loads(open_job['JobCategories']) == [{'series': '0905'}]
        paths = [p['hiringPath'] for p in json.loads(open_job['HiringPaths'])]
        assert 'The public' in paths
        assert len(paths) == 3

    def test_text_is_captured(self, open_job):
        assert len(open_job['text']) > 5000
        assert 'Honors Attorney' in open_job['text']
        assert '<' not in open_job['text']


class TestClosedAnnouncement:
    def test_closed_pages_are_still_fully_parseable(self, closed_job):
        assert closed_job['usajobsControlNumber'] == 806611100
        assert closed_job['positionTitle'] == 'ITSPEC (CUSTOMER SUPPORT)'
        assert len(closed_job['text']) > 5000

    def test_status_badge_reports_the_outcome(self, closed_job):
        assert closed_job['positionOpeningStatus'] == 'Job canceled'

    def test_explicit_dates_are_converted_to_iso(self, closed_job):
        # Closed pages print 'Open date: 08/26/2024' / 'Closed date: ...'.
        assert closed_job['positionOpenDate'] == '2024-08-26'
        assert closed_job['positionCloseDate'] == '2025-08-25'

    def test_appointment_type_drops_the_agency_elaboration(self, closed_job):
        # Page: 'Temporary - Indefinite'. The API reports the canonical value.
        assert closed_job['appointmentType'] == 'Temporary'
        assert closed_job['appointmentTypeDetail'] == 'Temporary - Indefinite'

    def test_numeric_vacancy_count(self, closed_job):
        assert closed_job['totalOpenings'] == '1'

    def test_absent_field_stays_absent(self, closed_job):
        # This announcement has no sensitivity designation. A missing field
        # must not be coerced to 'N' or ''.
        assert closed_job['positionSensitivity'] is None


class TestLongTextSections:
    """The announcement body, split by the page's own headings.

    The API is a poor source for this: MatchedObjectDescriptor drops content
    the page shows, and the ld+json block carries a truncated 'qualifications'
    (1,503 chars against the page's 1,845 on the open fixture).
    """

    def test_every_section_is_captured(self, open_job):
        assert open_job['jobSummary'].startswith('Attorneys at the CIA provide')
        assert open_job['majorDuties'].startswith("CIA's Office of General Counsel")
        assert 'Juris Doctor (JD) degree' in open_job['education']
        assert open_job['howYouWillBeEvaluated'].startswith(
            'You will be evaluated for this job')
        assert 'cia.gov/careers' in open_job['requiredDocuments']
        assert open_job['howToApply'].startswith('This post is for viewing')

    def test_summary_heading_is_a_direct_child(self, open_job):
        # Summary and How-you-will-be-evaluated put the <h2> straight in the
        # section; Requirements wraps it in a flex row with a Help link.
        # Stripping the heading's parent would empty the first two.
        assert len(open_job['jobSummary']) > 100
        assert 'Summary' not in open_job['jobSummary'][:20]

    def test_requirements_splits_on_its_h3_headings(self, open_job):
        assert open_job['conditionsOfEmployment'].startswith(
            'You must be physically in the United States')
        assert open_job['qualificationSummary'].startswith('Minimum Qualifications:')
        # Education and Additional information sit inside their own wrapper
        # <div>, so a recursive=False search for <h3> misses both.
        assert open_job['education']
        assert open_job['additionalInformation']

    def test_qualifications_matches_the_ld_json_copy(self, open_job):
        # The page's ld+json carries the same qualifications. This test used
        # to assert the parse was longer (1,845 characters against 1,503) and
        # call ld+json truncated; the extra length was Education and
        # Additional information running on into qualificationSummary.
        import re
        from bs4 import BeautifulSoup
        from usajobs_scrape import _ld_json
        soup = BeautifulSoup(load('job_open_721962100.html.gz'), 'html.parser')
        ld = BeautifulSoup(_ld_json(soup)['qualifications'], 'html.parser')
        words = lambda s: re.sub(r'\W+', ' ', s).strip().lower()
        assert words(open_job['qualificationSummary']) == words(ld.get_text(' '))

    def test_benefits_boilerplate_is_split_out_of_requirements(self, open_job):
        # The Benefits accordion is ~470 characters of identical text on every
        # announcement, and it lives inside the Requirements section.
        assert open_job['benefits'].startswith('A career with the U.S. government')
        assert 'A career with the U.S. government' not in open_job['requirements']

    def test_help_links_are_not_part_of_the_text(self, open_job):
        for field in ('jobSummary', 'majorDuties', 'requirements'):
            assert 'help.usajobs.gov' not in open_job[field]

    def test_absent_section_is_absent_not_empty(self, closed_job):
        # This announcement has no Education heading.
        assert 'education' not in closed_job

    def test_closed_announcements_keep_their_full_text(self, closed_job):
        assert len(closed_job['qualificationSummary']) > 7000
        assert len(closed_job['majorDuties']) > 4000

    def test_no_sections_on_an_unrelated_page(self):
        from bs4 import BeautifulSoup
        assert parse_sections(BeautifulSoup('<html><body>hi</body></html>',
                                            'html.parser')) == {}


class TestPageControlsAndLinks:
    """What the page draws around the announcement stays out of the text, and
    what the agency wrote stays in. Fixture: a VA announcement captured
    2026-10-22, after usajobs.gov moved its help pages to usajobshelp.opm.gov.
    """

    def test_qualifications_stop_at_the_next_heading(self, job):
        # Education and Additional information are each wrapped in a <div>,
        # and the walk from 'Qualifications' used to run on through both.
        q = job['qualificationSummary']
        assert job['education'] not in q
        assert job['additionalInformation'][:200] not in q
        assert q.endswith('general-schedule-qualification-standards/.')
        assert job['education'].startswith('A transcript must be submitted')
        assert job['additionalInformation'].startswith(
            'This is a Temporary Promotion')

    def test_buttons_are_not_text(self, job):
        # `text` is the whole page as-is, controls and all.
        for field in [f for f in SECTION_FIELDS if f != 'text']:
            for label in ('Expand', 'Hide additional information',
                          'Hide how to apply', 'Learn more about this agency',
                          'Opens in a new window'):
                assert label not in job.get(field, ''), (field, label)

    def test_help_buttons_go_but_agency_help_links_stay(self, job):
        # The agency's own sentence links to the help site; only the heading
        # buttons labeled "Help" are the page's.
        assert ('Information regarding your application status can be found '
                'in the USAJobs Help Center.') in job['howToApply']
        for field in SECTION_FIELDS:
            assert not job.get(field, '').startswith('Help '), field

    def test_benefits_links_stay_as_text(self, open_job, closed_job, job):
        # Dropping the "Learn more about federal benefits" link left
        # "very rewarding. . Review our benefits" on ~90% of postings.
        for page in (open_job, closed_job, job):
            assert ('very rewarding. Learn more about federal benefits.'
                    in page['benefits'])
            assert '. .' not in page['benefits']

    def test_no_space_before_punctuation_after_a_link(self, job):
        # get_text(" ") put a space at every tag: "The Fair Chance Act ."
        assert 'The Fair Chance Act.' in job['additionalInformation']
        assert 'Fair Chance Act .' not in job['requirements']

    def _requirements(self, body):
        from bs4 import BeautifulSoup
        return parse_sections(BeautifulSoup(
            f'<div id="joa-requirements">{body}</div>', 'html.parser'))

    def test_questionnaire_block_is_the_sites(self):
        # 867720800: a button-styled link and a fixed note, right after the
        # qualifications.
        out = self._requirements(
            '<h3>Qualifications</h3><p>Must show experience.</p>'
            '<div class="pt-4"><a class="button button-primary" href="x">'
            'Preview job questionnaire</a><p class="italic text-sm">Make sure '
            'your resume includes detailed information.</p></div>'
            '<div><h3>Additional information</h3><p>More.</p></div>')
        assert out['qualificationSummary'] == 'Must show experience.'
        assert 'questionnaire' not in out['requirements']

    def test_text_before_a_nested_heading_stays_with_its_section(self):
        # 429653100: broken agency HTML leaves the next headings inside a <p>
        # that opens with the last sentence of Conditions of employment.
        out = self._requirements(
            '<h3>Conditions of employment</h3><ul><li>Citizens.</li></ul>'
            '<p>Submit transcripts.<h3>Qualifications</h3>Have a degree.'
            '<div><h3>Additional information</h3>Worldwide.</div></p>')
        assert out['conditionsOfEmployment'] == 'Citizens. Submit transcripts.'
        assert out['qualificationSummary'] == 'Have a degree.'
        assert out['additionalInformation'] == 'Worldwide.'

    def test_agency_headings_inside_qualifications_are_content(self):
        # 634551300: the agency's own <h3>s, "Education" among them, inside
        # the qualifications text. Only the site's headings end a section.
        out = self._requirements(
            '<h3>Qualifications</h3><p>Series 0601<h3>Basic Requirements</h3>'
            'A degree.<h3>Education</h3>Major study.</p><p>Desired: skills.</p>'
            '<div><h3>Education</h3><p>Accredited schools only.</p></div>'
            '<div><h3>Additional information</h3><p>More.</p></div>')
        assert out['qualificationSummary'] == (
            'Series 0601 Basic Requirements A degree. Education Major study. '
            'Desired: skills.')
        assert out['education'] == 'Accredited schools only.'

    def test_agency_education_heading_without_a_site_section_is_content(self):
        # 854673900: no Education section on the page, and the agency's own
        # bare <h3>Education</h3> inside the qualifications. Taking it for the
        # section left qualificationSummary empty.
        out = self._requirements(
            '<h3>Qualifications</h3><p><h3>Education</h3>Major study -- art.'
            '<h3>Experience</h3>One year.</p>'
            '<div><h3>Additional information</h3><p>More.</p></div>')
        assert out['qualificationSummary'] == (
            'Education Major study -- art. Experience One year.')
        assert 'education' not in out
        assert out['additionalInformation'] == 'More.'

    def test_agency_headings_inside_education_are_content(self):
        # 635389100: the Education wrapper holds the agency's own "Education"
        # and "Experience" headings after the site's.
        out = self._requirements(
            '<h3>Qualifications</h3><p>See below.</p>'
            '<div><h3>Education</h3><p>Intro.</p><h3>Education</h3><p>A '
            'degree.</p><h3>Experience</h3><p>One year.</p></div>'
            '<div><h3>Additional information</h3><p>More.</p></div>')
        assert out['qualificationSummary'] == 'See below.'
        assert out['education'] == ('Intro. Education A degree. Experience '
                                    'One year.')

    def test_blocks_are_still_separated(self, job):
        # Joining inline tags as written must not glue list items or
        # paragraphs together.
        assert 'You must be a U.S. Citizen to apply for this job Selective' in (
            job['conditionsOfEmployment'])


class TestLocations:
    """Duty stations, emitted in the historical API's shape.

    prep_web_data.py already reads a PositionLocations column of
    {positionLocationCity, positionLocationState} dicts, so matching that shape
    means the web build needs no changes to consume scraped locations.
    """

    def _soup(self, items):
        from bs4 import BeautifulSoup
        html = "".join(
            f'<div class="location-item" data-location-text="{key}">'
            f'<div class="font-bold">{label}</div></div>'
            for key, label in items)
        return BeautifulSoup(f"<html><body>{html}</body></html>", "html.parser")

    def test_fixture_locations(self, open_job, closed_job):
        assert json.loads(open_job['PositionLocations']) == [
            {'positionLocationCity': 'Washington',
             'positionLocationState': 'District of Columbia'}]
        assert json.loads(closed_job['PositionLocations']) == [
            {'positionLocationCity': 'Eielson AFB',
             'positionLocationState': 'Alaska'}]

    def test_state_abbreviation_is_expanded(self):
        locs = parse_locations(self._soup([("anchorage, ak", "Anchorage, AK")]))
        # The APIs write "Anchorage, Alaska"; the page writes "Anchorage, AK".
        assert locs == [{'positionLocationCity': 'Anchorage',
                         'positionLocationState': 'Alaska'}]

    def test_overseas_region_is_left_alone(self):
        locs = parse_locations(
            self._soup([("sigonella sicily, italy", "Sigonella Sicily, Italy")]))
        assert locs == [{'positionLocationCity': 'Sigonella Sicily',
                         'positionLocationState': 'Italy'}]

    def test_city_containing_a_comma_keeps_only_the_last_part_as_region(self):
        locs = parse_locations(
            self._soup([("x", "Winston-Salem, NC"), ("y", "Ft. Belvoir, VA")]))
        assert [l['positionLocationCity'] for l in locs] == [
            'Winston-Salem', 'Ft. Belvoir']

    def test_page_renders_each_station_twice(self):
        # One block for mobile, one for desktop. Every real page does this.
        locs = parse_locations(self._soup([
            ("denver, co (co1234) 1 main st", "Denver, CO"),
            ("denver, co (co1234) 1 main st", "Denver, CO"),
        ]))
        assert len(locs) == 1

    def test_two_offices_in_one_city_are_kept_apart(self):
        # Deduping on the city/state line instead of data-location-text
        # collapsed a real 408-station IRS posting to 361.
        locs = parse_locations(self._soup([
            ("denver, co (co1234) 1 main st", "Denver, CO"),
            ("denver, co (co5678) 9 elm ave", "Denver, CO"),
        ]))
        assert len(locs) == 2
        assert {l['positionLocationCity'] for l in locs} == {'Denver'}

    def test_collapsed_label_with_no_comma_is_kept_whole(self):
        # e.g. "Location Negotiable After Selection", or State's
        # "Department of State Posts - Overseas and Domestic".
        locs = parse_locations(self._soup([
            ("location negotiable after selection",
             "Location Negotiable After Selection")]))
        assert locs == [{'positionLocationCity':
                         'Location Negotiable After Selection'}]

    def test_page_count_does_not_overwrite_the_search_count(self, open_job):
        # Discovery stores the search endpoint's count, which matched the API
        # on all 348 postings checked. The page's own count goes in its own
        # field so a shortfall stays visible.
        assert open_job['scrapedLocationCount'] == 1
        assert 'positionLocationCount' not in open_job

    def test_no_location_block(self):
        from bs4 import BeautifulSoup
        assert parse_locations(
            BeautifulSoup('<html><body>hi</body></html>', 'html.parser')) == []


class TestNormalizeSearchJob:
    """The discovery half: one ExecuteSearch result -> a row."""

    def _job(self, **overrides):
        job = {
            "Title": "Data Engineer",
            "Agency": "Internal Revenue Service",
            "Department": "Department of the Treasury",
            "DocumentID": "865188800",
            "PositionID": "25-12345678-ABC",
            "PositionStartDate": "2026-08-01T00:00:00.0000",
            "PositionEndDate": "2026-08-31T23:59:59.9970",
            "LowGrade": "12",
            "HighGrade": "13",
            "JobGrade": "GS",
            "MinimumRange": "99200",
            "WorkSchedule": "Full-time",
            "WorkType": "Permanent",
            "AnnouncementClosingType": "01",
            "PositionLocationCount": "2",
            "JobCategoryCode": [{"Name": "IT Management", "Code": "2210"}],
            "HiringPath": [{"Code": "public", "SearchDisplay": "Open to the public"}],
        }
        job.update(overrides)
        return job

    def test_field_names_match_the_api_collection(self):
        row = normalize_search_job(self._job())
        assert row['usajobsControlNumber'] == 865188800
        assert row['usajobs_control_number'] == '865188800'
        assert row['positionTitle'] == 'Data Engineer'
        assert row['announcementNumber'] == '25-12345678-ABC'
        assert row['hiringAgencyName'] == 'Internal Revenue Service'
        assert row['hiringDepartmentName'] == 'Department of the Treasury'
        assert row['payScale'] == 'GS'
        assert row['minimumGrade'] == '12'
        assert row['minimumSalary'] == 99200.0

    def test_nested_lists_are_json_encoded_like_the_api_collection(self):
        row = normalize_search_job(self._job())
        assert json.loads(row['JobCategories']) == [{'series': '2210'}]
        assert json.loads(row['HiringPaths']) == [
            {'hiringPath': 'Open to the public'}]

    def test_missing_values_do_not_raise(self):
        row = normalize_search_job({"DocumentID": "123400"})
        assert row['usajobsControlNumber'] == 123400
        assert row['positionTitle'] is None
        assert row['JobCategories'] is None
        assert row['minimumSalary'] is None

    def test_unparseable_control_number(self):
        row = normalize_search_job({"DocumentID": "not-a-number"})
        assert row['usajobsControlNumber'] is None
        assert row['usajobs_control_number'] is None
