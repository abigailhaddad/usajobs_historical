#!/usr/bin/env python3
"""
Scrape usajobs.gov directly, with no API key.

Two sources, both keyless:

  Discovery -- POST https://www.usajobs.gov/Search/ExecuteSearch
      The JSON endpoint behind the search page. No auth, no cookie, no CSRF
      token. Returns the same shape the search API does (~25 fields per job)
      plus a facet block. It has the same 10,000-result ceiling as the API, so
      we slice by occupational series exactly as collect_current_data.py does.

  Detail -- GET https://www.usajobs.gov/job/{control_number}
      Server-rendered HTML. Every overview field is a <dt>/<dd> pair, so the
      parse is a label lookup rather than positional guesswork. Closed
      announcements stay up indefinitely, which is why the announcement-text
      dataset scrapes these pages rather than reading the API.

robots.txt disallows /Content/, /Scripts/, /foresee/ and /Service References/
only; /job/ and /Search/ are not restricted.

Field names here deliberately match the ones collect_current_data.py writes
(positionTitle, usajobsControlNumber, minimumGrade, ...) so the two collections
can be diffed column by column. compare_scrape_to_api.py does that diff.
"""

import copy
import html as html_mod
import json
import random
import re
import time
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

SEARCH_URL = "https://www.usajobs.gov/Search/ExecuteSearch"
JOB_URL = "https://www.usajobs.gov/job/{}"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

# 500 is the endpoint's own maximum, same as the API's ResultsPerPage.
PAGE_SIZE = 500

# The endpoint stops paging at 10,000 results regardless of the reported total,
# so any single slice that reaches this is truncated and needs sub-slicing.
MAX_RESULTS = 10000

_WS = re.compile(r"\s+")


def new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/json",
    })
    return s


# ---------------------------------------------------------------- discovery

def execute_search(session: requests.Session, params: Dict,
                   retries: int = 4, timeout: int = 45) -> Optional[Dict]:
    """One ExecuteSearch POST. Returns the decoded body, or None if it never
    came back. Bounded retry with backoff, matching fetch_jobs_page()."""
    for attempt in range(1, retries + 1):
        try:
            r = session.post(SEARCH_URL, data=json.dumps(params), timeout=timeout)
            r.raise_for_status()
            return r.json()
        except (requests.exceptions.RequestException, ValueError) as e:
            if attempt == retries:
                print(f"   ExecuteSearch failed after {retries} attempts "
                      f"({params}): {e}")
                return None
            time.sleep(attempt * 3)
    return None


def open_inventory(session: requests.Session) -> Tuple[Dict[str, int], Optional[int]]:
    """One unfiltered call -> (series code -> job count, true open total).

    The facet block is computed over the *whole* result set, not the capped
    10,000 the `Total` field reports, so this is a better ground truth for the
    inventory size than anything the API exposes. Facets that can only apply
    once per job ('ss', supervisory status) sum to the real total; 'j'
    (series) and 'd' (department) over-count because a job can carry several.
    """
    body = execute_search(session, {"Page": 1, "ResultsPerPage": 1})
    if not body:
        return {}, None

    facets = body.get("f") or {}
    series = {code: int(n) for code, n in (facets.get("j") or {}).items()}

    # 'ss' is supervisory-yes/no: every job lands in exactly one bucket.
    supervisory = facets.get("ss") or {}
    total = sum(int(n) for n in supervisory.values()) if supervisory else None

    return series, total


def search_slice(session: requests.Session, params: Dict,
                 pause: float = 0.4) -> Tuple[List[Dict], Optional[int], bool]:
    """Page through one search slice.

    Returns (jobs, reported_total, truncated). `truncated` is True when the
    slice ran into the 10,000 ceiling, which means these results are a subset
    and the slice needs breaking down further.
    """
    jobs: List[Dict] = []
    reported_total = None
    page = 1

    while True:
        body = execute_search(session, {**params, "Page": page,
                                        "ResultsPerPage": PAGE_SIZE})
        if not body:
            break

        if reported_total is None:
            try:
                reported_total = int(body.get("Total") or 0)
            except (TypeError, ValueError):
                reported_total = None

        batch = body.get("Jobs") or []
        if not batch:
            break
        jobs.extend(batch)

        pager = body.get("Pager") or {}
        if not pager.get("HasNextPage") or len(jobs) >= MAX_RESULTS:
            break

        page += 1
        time.sleep(pause)

    return jobs, reported_total, len(jobs) >= MAX_RESULTS


# ------------------------------------------------------- search JSON -> row

_SERVICE_FROM_PAGE = {
    "competitive service": "Competitive",
    "excepted service": "Excepted",
    "senior executive service": "Senior Executive",
}


def _int_or_none(value) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_search_job(job: Dict) -> Dict:
    """One ExecuteSearch result -> a row using collect_current_data.py's names.

    Everything here comes from the search JSON alone. The detail-page fields
    (service type, clearance, telework, openings, ...) are layered on top by
    parse_job_page(); this is what we know before fetching anything.
    """
    control_number = _int_or_none(job.get("DocumentID"))

    categories = [{"series": c.get("Code")}
                  for c in (job.get("JobCategoryCode") or [])
                  if isinstance(c, dict) and c.get("Code")]
    paths = [{"hiringPath": p.get("SearchDisplay") or p.get("Code")}
             for p in (job.get("HiringPath") or [])
             if isinstance(p, dict)]

    return {
        "usajobsControlNumber": control_number,
        "usajobs_control_number": str(control_number) if control_number else None,
        "announcementNumber": job.get("PositionID"),
        "positionTitle": job.get("Title"),
        "hiringAgencyName": job.get("Agency"),
        "hiringDepartmentName": job.get("Department"),
        "positionOpenDate": job.get("PositionStartDate"),
        "positionCloseDate": job.get("PositionEndDate"),
        "payScale": job.get("JobGrade"),
        "minimumGrade": job.get("LowGrade"),
        "maximumGrade": job.get("HighGrade"),
        "minimumSalary": _float_or_none(job.get("MinimumRange")),
        "workSchedule": job.get("WorkSchedule"),
        "appointmentType": job.get("WorkType"),
        "announcementClosingTypeCode": job.get("AnnouncementClosingType"),
        "JobCategories": json.dumps(categories) if categories else None,
        "HiringPaths": json.dumps(paths) if paths else None,
        "positionLocationDisplay": job.get("LocationDisplay"),
        "positionLocationCount": _int_or_none(job.get("PositionLocationCount")),
        "positionURI": job.get("PositionURI"),
        "salaryDisplay": job.get("SalaryDisplay"),
        "dateDisplay": job.get("DateDisplay"),
    }


# --------------------------------------------------------- detail page parse

def page_text(soup: BeautifulSoup) -> str:
    """Whole-page visible text, in the shape the HuggingFace announcement
    dataset stores (see abigailhaddad/joa scrape.py page_text)."""
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return _WS.sub(" ", soup.get_text(" ")).strip()


def _dt_dd_pairs(soup: BeautifulSoup) -> Dict[str, str]:
    """Every overview field on the page.

    The announcement markup is uniformly
        <dl><dt class="font-bold">Label</dt><dd>Value</dd></dl>
    with one wrinkle: 'Pay scale & grade' nests another <dl> (promotion
    potential) inside its <dd>. Nested lists are stripped before reading the
    value, and the nested pair is picked up on its own iteration.
    """
    fields: Dict[str, str] = {}
    for dl in soup.find_all("dl"):
        dt = dl.find("dt", recursive=False)
        dd = dl.find("dd", recursive=False)
        if not dt or not dd:
            continue

        label = _WS.sub(" ", dt.get_text(" ")).strip()
        value_node = copy.copy(dd)
        for nested in value_node.find_all("dl"):
            nested.decompose()
        # Every value is followed by an optional help blurb ("Pay scale and
        # grade determines the salary of the job.") set in `text-xs`. Strip by
        # that class, not by the help.usajobs.gov link it contains: for
        # 'Position sensitivity and risk' the value *is* such a link.
        for helper in value_node.select(".text-xs"):
            helper.decompose()

        value = _WS.sub(" ", value_node.get_text(" ")).strip()
        if label and label not in fields:
            fields[label] = value
    return fields


def _ld_json(soup: BeautifulSoup) -> Dict:
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(html_mod.unescape(tag.string or ""))
        except (ValueError, TypeError):
            continue
        if isinstance(data, dict) and data.get("@type") == "JobPosting":
            return data
    return {}


def _mdy_to_iso(value: Optional[str]) -> Optional[str]:
    """'08/26/2024' -> '2024-08-26'. Anything else comes back untouched."""
    if not value:
        return None
    m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", value.strip())
    return f"{m.group(3)}-{m.group(1)}-{m.group(2)}" if m else value


_SALARY = re.compile(
    r"\$([\d,]+(?:\.\d+)?)\s*-\s*\$([\d,]+(?:\.\d+)?)\s*(per\s+\w+)?", re.I)
_PAY_GRADE = re.compile(r"^([A-Z]{1,3}[A-Z0-9]{0,3})\s+(\S+)\s*-\s*(\S+)$")
_VACANCIES = re.compile(r"([\w]+)\s+vacanc(?:y|ies)\s+in\s+the\s+following", re.I)


# The announcement body, section by section. Each is a <div class="page-section"
# id="joa-..."> whose first child is the heading row (an <h2> plus a "Help"
# link), and inside Requirements the parts are delimited by <h3>. Storing them
# separately rather than as one blob is the whole point: the API's
# MatchedObjectDescriptor drops content these sections show, and 'Qualifications'
# is the field most analysis actually wants.
_SECTIONS = {
    "joa-summary": "jobSummary",
    "joa-duties": "majorDuties",
    "joa-evaluation": "howYouWillBeEvaluated",
    "joa-required-documents": "requiredDocuments",
    "joa-how-to-apply": "howToApply",
}

# <h3> headings inside Requirements. 'Qualifications' takes the API's name for
# the same content; the others have no API counterpart.
_REQUIREMENT_PARTS = {
    "conditions of employment": "conditionsOfEmployment",
    "qualifications": "qualificationSummary",
    "education": "education",
    "additional information": "additionalInformation",
}


def _clean(node) -> str:
    """Visible text of a section, minus its own heading and the help links.

    Removing the heading's parent <div> instead would work for Requirements,
    where the <h2> sits in a flex row with a "Help" link, and silently empty
    Summary and How-you-will-be-evaluated, where the <h2> is a direct child of
    the section itself.
    """
    node = copy.copy(node)
    for tag in node(["script", "style", "noscript", "svg"]):
        tag.decompose()
    for heading in node.find_all(["h1", "h2"]):
        heading.decompose()
    for link in node.select("a[href*='help.usajobs.gov']"):
        link.decompose()
    return _WS.sub(" ", node.get_text(" ")).strip()


# Everything parse_job_page produces from the announcement body. Health checks
# key off this: if usajobs.gov changes its markup, these stop being populated.
SECTION_FIELDS = [
    "jobSummary", "majorDuties", "requirements", "conditionsOfEmployment",
    "qualificationSummary", "education", "additionalInformation", "benefits",
    "howYouWillBeEvaluated", "requiredDocuments", "howToApply", "text",
]


def parse_sections(soup: BeautifulSoup) -> Dict[str, str]:
    """The long-text fields, keyed by the name the row will carry."""
    out: Dict[str, str] = {}

    for section_id, field in _SECTIONS.items():
        node = soup.find(id=section_id)
        if node:
            text = _clean(node)
            if text:
                out[field] = text

    requirements = soup.find(id="joa-requirements")
    if not requirements:
        return out

    requirements = copy.copy(requirements)

    # The Benefits accordion lives inside Requirements and is near-identical
    # boilerplate on every announcement. Pulled out so it stops padding the
    # requirements text by ~2,400 characters per posting.
    benefits = [art.extract() for art in requirements.find_all("article")]
    if benefits:
        text = " ".join(_clean(art) for art in benefits).strip()
        if text:
            out["benefits"] = text

    out["requirements"] = _clean(requirements)

    # Not recursive=False: 'Conditions of employment' and 'Qualifications' are
    # flat children of the section, but 'Education' and 'Additional
    # information' each sit inside their own wrapper <div>. Walking siblings
    # from the heading handles both, since a wrapped heading's siblings are its
    # own content.
    for h3 in requirements.find_all("h3"):
        field = _REQUIREMENT_PARTS.get(h3.get_text(" ", strip=True).lower())
        if not field:
            continue
        parts = []
        for sibling in h3.next_siblings:
            if getattr(sibling, "name", None) == "h3":
                break
            parts.append(sibling.get_text(" ") if getattr(sibling, "name", None)
                         else str(sibling))
        text = _WS.sub(" ", " ".join(parts)).strip()
        if text:
            out[field] = text

    return out


# The announcement page writes duty stations as "City, ST"; both APIs use the
# full state name ("Anchorage, Alaska"). Territories and the military postal
# codes (AA/AE/AP) are in here because federal postings use all of them.
_US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut",
    "DE": "Delaware", "DC": "District of Columbia", "FL": "Florida",
    "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois",
    "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky",
    "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana",
    "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire",
    "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania",
    "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota",
    "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming",
    "AS": "American Samoa", "FM": "Federated States of Micronesia",
    "GU": "Guam", "MH": "Marshall Islands",
    "MP": "Northern Mariana Islands", "PR": "Puerto Rico",
    "PW": "Palau", "VI": "Virgin Islands",
    "AA": "Armed Forces Americas", "AE": "Armed Forces Europe",
    "AP": "Armed Forces Pacific",
}


def parse_locations(soup: BeautifulSoup) -> List[Dict[str, str]]:
    """Duty stations, in the shape the historical API uses.

    Each station is a .location-item whose bold line is "City, ST" for a US
    posting and "City, Country" for an overseas one. The page renders every
    item twice (one block for mobile, one for desktop), so entries are deduped
    on data-location-text -- which carries the facility code and street address
    and therefore separates two offices in the same city. Deduping on the
    city/state line instead collapsed a 408-station IRS posting to 361.

    Emitting positionLocationCity/positionLocationState means
    prep_web_data.py's existing PositionLocations path handles this with no
    changes: it renders "Anchorage, Alaska", the same string the API produces.
    """
    locations: List[Dict[str, str]] = []
    seen = set()

    for item in soup.select(".location-item"):
        key = (item.get("data-location-text") or "").strip()
        bold = item.select_one(".font-bold")
        if not bold:
            continue
        label = _WS.sub(" ", bold.get_text(" ")).strip()
        if not label:
            continue

        # No data-location-text to dedupe on: fall back to the label, which at
        # worst merges two identical-looking stations.
        if (key or label) in seen:
            continue
        seen.add(key or label)

        city, _, region = label.rpartition(", ")
        if not city:
            # No comma at all -- a collapsed label such as "Department of
            # State Posts - Overseas and Domestic". Keep it whole.
            locations.append({"positionLocationCity": label})
            continue

        state = _US_STATES.get(region.upper(), region)
        locations.append({"positionLocationCity": city,
                          "positionLocationState": state})

    return locations


def parse_job_page(html: str) -> Dict:
    """A /job/{n} page -> the fields the API collection stores, plus the text.

    Works identically on open and closed announcements. Closed pages carry
    explicit 'Open date:'/'Closed date:' spans; open ones leave that block for
    client-side JS to fill, so their dates come from the ld+json instead.
    """
    soup = BeautifulSoup(html, "html.parser")
    fields = _dt_dd_pairs(soup)
    ld = _ld_json(soup)

    row: Dict = {}

    control = soup.find("dt", string=re.compile(r"^\s*Control number\s*$"))
    if control:
        row["usajobsControlNumber"] = _int_or_none(fields.get("Control number"))
        row["usajobs_control_number"] = fields.get("Control number")

    row["announcementNumber"] = fields.get("Announcement number")

    # The header <h1> is the announcement's full title. The ld+json `title` is
    # a shortened form that drops the parenthetical specialty ('IT Specialist'
    # for 'IT Specialist (APPSW) - .NET - DHA'), which is what the API returns
    # and what everything downstream matches on.
    header = soup.find(id="joa-header")
    heading = header.find("h1") if header else soup.find("h1")
    row["positionTitle"] = (_WS.sub(" ", heading.get_text(" ")).strip()
                            if heading else ld.get("title"))

    # Status badge: 'Accepting applications', 'Job canceled', 'Candidate
    # selected'. The same string the API calls positionOpeningStatus.
    badge = soup.select_one("[class*=badge]")
    if badge:
        row["positionOpeningStatus"] = _WS.sub(" ", badge.get_text(" ")).strip()

    # Dates. Closed announcements state both explicitly; open ones only have
    # ld+json, whose datePosted is m/d/Y and validThrough already ISO.
    dates = {}
    for span in soup.find_all("span", class_="font-bold"):
        label = span.get_text(strip=True)
        if label in ("Open date:", "Closed date:"):
            tail = span.next_sibling
            if tail:
                dates[label] = tail.strip()
    row["positionOpenDate"] = (_mdy_to_iso(dates.get("Open date:"))
                               or _mdy_to_iso(ld.get("datePosted")))
    row["positionCloseDate"] = (_mdy_to_iso(dates.get("Closed date:"))
                                or ld.get("validThrough"))

    # Salary: '$93,994 - $121,785 per year'
    salary = _SALARY.search(fields.get("Salary", ""))
    if salary:
        row["minimumSalary"] = _float_or_none(salary.group(1).replace(",", ""))
        row["maximumSalary"] = _float_or_none(salary.group(2).replace(",", ""))
        if salary.group(3):
            row["salaryType"] = salary.group(3).strip().title()

    # Pay scale & grade: 'GS 11 - 13', or 'GS 11' when there is only one.
    grade = (fields.get("Pay scale & grade") or "").strip()
    m = _PAY_GRADE.match(grade)
    if m:
        row["payScale"], row["minimumGrade"], row["maximumGrade"] = m.groups()
    elif grade:
        parts = grade.split()
        if len(parts) == 2:
            row["payScale"], row["minimumGrade"] = parts
            row["maximumGrade"] = parts[1]

    row["promotionPotential"] = fields.get("Promotion potential")
    row["workSchedule"] = fields.get("Work schedule")
    # 'Permanent - Career Conditional/Career Appointment requires a 1 year
    # probationary period...' is the agency's own elaboration appended to the
    # canonical type. None of the 15 canonical values contains ' - ', so the
    # first segment recovers exactly what the API reports; the full string is
    # kept alongside because it is information the API does not carry.
    appointment = fields.get("Appointment type")
    if appointment:
        row["appointmentType"] = appointment.split(" - ", 1)[0].strip()
        row["appointmentTypeDetail"] = appointment
    row["travelRequirement"] = fields.get("Travel Required")
    row["supervisoryStatus"] = _yes_no(fields.get("Supervisory status"))
    row["securityClearance"] = fields.get("Security clearance")
    row["drugTestRequired"] = _yes_no(fields.get("Drug test"))
    row["teleworkEligible"] = _yes_no(fields.get("Telework eligible"))
    row["remoteJob"] = _yes_no(fields.get("Remote job"))
    row["relocationExpensesReimbursed"] = _yes_no(
        fields.get("Relocation expenses reimbursed"))
    row["financialDisclosureRequired"] = _yes_no(
        fields.get("Financial disclosure required"))
    row["representedByUnion"] = _yes_no(fields.get("Represented by a union"))
    row["positionSensitivity"] = fields.get("Position sensitivity and risk")

    # 'This job is in the Excepted Service' -> 'Excepted'
    service = (fields.get("Federal service type") or "").lower()
    for phrase, name in _SERVICE_FROM_PAGE.items():
        if phrase in service:
            row["serviceType"] = name
            break

    # '1 vacancy in the following location:' / 'Few vacancies in the following
    # locations:'. The API's totalOpenings is a string too, and takes the same
    # non-numeric values.
    body_text = page_text(soup)
    vacancies = _VACANCIES.search(body_text)
    if vacancies:
        row["totalOpenings"] = vacancies.group(1)

    series = []
    for link in soup.select("a[href*='/search/results/?j=']"):
        code = re.search(r"[?&]j=(\d+)", link.get("href", ""))
        if code and {"series": code.group(1)} not in series:
            series.append({"series": code.group(1)})
    if series:
        row["JobCategories"] = json.dumps(series)

    paths_block = soup.find(id="joa-hiring-paths")
    if paths_block:
        paths = [{"hiringPath": _WS.sub(" ", h.get_text(" ")).strip()}
                 for h in paths_block.find_all("h3")]
        if paths:
            row["HiringPaths"] = json.dumps(paths)

    # Deliberately does NOT set positionLocationCount. Discovery already put
    # the search endpoint's count there, and for about 3% of postings the page
    # lists fewer stations than the API holds -- a job with 31 API entries
    # across 22 cities renders 12. Keeping both means the gap stays visible
    # instead of being papered over.
    locations = parse_locations(soup)
    if locations:
        row["PositionLocations"] = json.dumps(locations)
        row["scrapedLocationCount"] = len(locations)

    row.update(parse_sections(soup))
    row["text"] = body_text
    return row


def _yes_no(value: Optional[str]) -> Optional[str]:
    """Page prose -> the API's Y/N. 'Yes—You may qualify for reimbursement...'
    is a Yes; a missing field stays missing rather than becoming N."""
    if not value:
        return None
    head = value.strip().lower()
    if head.startswith("yes"):
        return "Y"
    if head.startswith("no"):
        return "N"
    return value.strip()


# ------------------------------------------------------------ page fetching

def fetch_job_page(session: requests.Session, control_number: str,
                   retries: int = 4, timeout: int = 45,
                   pause: float = 0.25) -> Tuple[Optional[str], Optional[str]]:
    """GET one announcement page -> (html, error).

    A 404 is a real answer, not a failure: the announcement is gone and
    retrying will not bring it back. It returns (None, None) so the caller
    records the miss without queueing a retry.
    """
    for attempt in range(retries):
        try:
            r = session.get(JOB_URL.format(control_number), timeout=timeout)
            if r.status_code == 404:
                return None, None
            r.raise_for_status()
            # A challenge or error page still returns 200 but is a fraction of
            # a real announcement, and would parse to a row of nulls.
            if len(r.text) < 20000:
                raise ValueError(f"short page ({len(r.text)} bytes)")
            time.sleep(pause * (0.5 + random.random()))
            return r.text, None
        except (requests.exceptions.RequestException, ValueError) as e:
            if attempt == retries - 1:
                return None, str(e)
            time.sleep(3 * (attempt + 1))
    return None, "exhausted retries"
