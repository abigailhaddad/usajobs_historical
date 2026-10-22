#!/usr/bin/env python3
"""Check the announcement text the scrape just stored.

Two checks on the rows data/scraped_jobs_*.parquet gained recently (inserted
at or after --since, by default the last 26 hours, so a day's retries and the
gap-fill step's rows are covered):

1. Marks of the page, not the agency. Things that come from the site's own
   controls and should never be in a section: the "Expand Hide ..." button
   labels, the "Preview job questionnaire" block, HTML entities left
   undecoded, mis-decoded UTF-8 ("â€™"), and one section's text running on
   inside another. These are the defects PRs #668 and #672 fixed in 3.2
   million stored rows; this is what notices the next one.

   Deliberately not checked here: a space before punctuation, or a sentence
   glued to the next ("experience.Resumes"). Agencies' own text has both --
   86 and 166 of 541 correctly parsed pages -- so they are only counted
   against the API below.

2. A second source. The Search API (data.usajobs.gov) returns the same
   sections as text for open announcements. For every stored row it also
   returns, the sections are compared word for word:

     qualificationSummary, jobSummary, majorDuties, education
         must match both ways: extra words mean a run-on, missing words a
         section cut short.
     additionalInformation, howYouWillBeEvaluated, requiredDocuments
         the page adds the site's standard text to these ("Candidates should
         be committed to improving the efficiency of the Federal
         government..."), so only the API's words must all be there.

   Measured on 30 announcements, 22 October 2026: the first four agree word
   for word on all 30 (108 of 112 also character for character; the four
   that do not are tags with no space between them in the agency's HTML,
   which the API's text puts a space at). A space before punctuation that
   the API's text does not have is counted too.

Problems past the thresholds go to logs/SCRAPE_TEXT_WARNING.txt, which the
daily workflow turns into a GitHub issue. The full table goes to
logs/scrape_vs_api_text.md either way. Never fails the run: the scrape is
checked, not depended on.

    python check_scraped_text.py --data-dir data
    python check_scraped_text.py --data-dir data --since 2026-10-22T13:00
"""

import argparse
import difflib
import glob
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

LOGS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs")
WARNING_FILE = os.path.join(LOGS, "SCRAPE_TEXT_WARNING.txt")
REPORT_FILE = os.path.join(LOGS, "scrape_vs_api_text.md")

API_URL = "https://data.usajobs.gov/api/Search"

SECTIONS = ["jobSummary", "majorDuties", "howYouWillBeEvaluated",
            "requiredDocuments", "howToApply", "benefits", "requirements",
            "conditionsOfEmployment", "qualificationSummary", "education",
            "additionalInformation"]

# Should never be in a section. Each is one rule; a row counts once per rule.
NEVER = {
    "button_label": re.compile(
        r"Expand Hide (?:additional information|how to apply|next steps)"),
    "questionnaire_block": re.compile(r"Preview job questionnaire Make sure"),
    "html_entity": re.compile(
        r"&(?:amp|nbsp|rsquo|lsquo|ldquo|rdquo|quot|apos|lt|gt|#\d+);"),
    "mojibake": re.compile(r"â€|Ã[\x80-\xbf]|Â\s"),
}

# Share of checked rows past which a rule is reported. The marks above were
# on 0 of 541 correctly parsed pages, so any is worth a look; a run-on is
# also what agencies write when they repeat their education requirement
# inside the qualifications (VA does), so that one has room.
NEVER_LIMIT = 0.0
RUN_ON_LIMIT = 0.01

# Search API field for each section, and how strictly it must agree.
STRICT = {
    "qualificationSummary": lambda j, d: j.get("QualificationSummary"),
    "jobSummary": lambda j, d: d.get("JobSummary"),
    "majorDuties": lambda j, d: " ".join(d.get("MajorDuties") or []),
    "education": lambda j, d: d.get("Education"),
}
CONTAINS = {
    "additionalInformation": lambda j, d: d.get("OtherInformation"),
    "howYouWillBeEvaluated": lambda j, d: d.get("Evaluations"),
    "requiredDocuments": lambda j, d: d.get("RequiredDocuments"),
}
# Word agreement below this is a mismatch. A run-on adds whole sections and a
# cut drops them, which moves it far more than an agency edit between the
# scrape and the API call does.
MATCH = 0.95
# Share of compared rows with a mismatch, per field, past which it is
# reported, once at least MIN_COMPARED rows were compared.
MISMATCH_LIMIT = 0.03
MIN_COMPARED = 20

_WORD = re.compile(r"\w+")
_SPACE_PUNCT = re.compile(r"\w [.,;:](?=\s|$)")


def words(text):
    return _WORD.findall((text or "").lower())


def agreement(ours, theirs):
    """Share of the two word sequences that line up."""
    return difflib.SequenceMatcher(None, ours, theirs,
                                   autojunk=False).ratio()


def coverage(ours, theirs):
    """Share of `theirs`, in order, found in `ours`."""
    if not theirs:
        return 1.0
    m = difflib.SequenceMatcher(None, ours, theirs, autojunk=False)
    return sum(b.size for b in m.get_matching_blocks()) / len(theirs)


def run_on(row):
    """Another section's text inside qualifications or conditions, as the
    repair counted it (repair_section_leaks.leftovers)."""
    q = row.get("qualificationSummary")
    coe = row.get("conditionsOfEmployment")
    if coe and q and ("Qualifications " + q[:150]) in coe:
        return True
    for f, heading in (("additionalInformation", "Additional information "),
                       ("education", "Education ")):
        v = row.get(f)
        if q and v and (heading + v[:150]) in q:
            return True
    return False


def recent_rows(data_dir, since):
    import pyarrow.parquet as pq
    rows = []
    for path in sorted(glob.glob(os.path.join(data_dir,
                                              "scraped_jobs_*.parquet"))):
        names = pq.read_schema(path).names
        cols = ["usajobs_control_number", "inserted_at"] + [
            s for s in SECTIONS if s in names]
        t = pq.read_table(path, columns=cols)
        for r in t.to_pylist():
            if r.get("inserted_at") and r["inserted_at"] >= since:
                rows.append(r)
    return rows


def pattern_check(rows):
    """{rule: [control numbers]} over the rows."""
    hits = {k: [] for k in list(NEVER) + ["run_on"]}
    for r in rows:
        for rule, rx in NEVER.items():
            if any(rx.search(r.get(f) or "") for f in SECTIONS):
                hits[rule].append(r["usajobs_control_number"])
        if run_on(r):
            hits["run_on"].append(r["usajobs_control_number"])
    return hits


def api_items(token, days, pages):
    """Search API results posted in the last `days`, by control number."""
    import requests
    headers = {"Host": "data.usajobs.gov", "Authorization-Key": token}
    out = {}
    for page in range(1, pages + 1):
        for attempt in range(4):
            try:
                r = requests.get(API_URL, headers=headers, timeout=60, params={
                    "DatePosted": days, "ResultsPerPage": 500, "Page": page})
                r.raise_for_status()
                result = r.json()["SearchResult"]
                break
            except Exception as e:  # noqa: BLE001 -- retried, then reported
                if attempt == 3:
                    print(f"Search API page {page} failed: {e}")
                    return out
                time.sleep(5 * (attempt + 1))
        items = result.get("SearchResultItems") or []
        for it in items:
            # MatchedObjectId is the control number (PositionID is the
            # agency's announcement number).
            out[str(it.get("MatchedObjectId"))] = it["MatchedObjectDescriptor"]
        if len(items) < 500:
            break
    return out


def api_check(rows, api):
    """{field: (compared, [(control number, score)])} and the rows with a
    space before punctuation the API text does not have."""
    result = {f: [0, []] for f in list(STRICT) + list(CONTAINS)}
    spacing = []
    for r in rows:
        j = api.get(str(r["usajobs_control_number"]))
        if not j:
            continue
        d = (j.get("UserArea") or {}).get("Details") or {}
        extra = 0
        for f, get in list(STRICT.items()) + list(CONTAINS.items()):
            theirs_text = get(j, d)
            if not theirs_text:
                # The page fills an empty field with the site's own words
                # ("This job does not have an education qualification
                # requirement."); nothing to compare.
                continue
            ours, theirs = words(r.get(f)), words(theirs_text)
            score = (agreement(ours, theirs) if f in STRICT
                     else coverage(ours, theirs))
            result[f][0] += 1
            if score < MATCH:
                result[f][1].append((r["usajobs_control_number"], score))
            extra += (len(_SPACE_PUNCT.findall(r.get(f) or ""))
                      - len(_SPACE_PUNCT.findall(" ".join(theirs_text.split()))))
        if extra > 0:
            spacing.append(r["usajobs_control_number"])
    return result, spacing


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data-dir", default="data")
    p.add_argument("--since", help="ISO time; default 26 hours ago (UTC)")
    p.add_argument("--api-days", type=int, default=3,
                   help="Search API DatePosted window")
    # 500 a page; the API stops at 10,000 results. Three days of postings
    # came to more than 3,000 on 22 October 2026.
    p.add_argument("--api-pages", type=int, default=20)
    args = p.parse_args()

    since = args.since or (datetime.now(timezone.utc) - timedelta(hours=26)
                           ).strftime("%Y-%m-%dT%H:%M")
    rows = recent_rows(args.data_dir, since)
    print(f"{len(rows):,} scraped rows inserted since {since}")
    lines = [f"# Scraped text check, {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC",
             "", f"{len(rows):,} rows inserted since {since}.", ""]
    problems = []
    if not rows:
        print("Nothing to check")
        return 0

    hits = pattern_check(rows)
    lines += ["## Marks of the page", "", "| rule | rows | examples |",
              "|---|---|---|"]
    for rule, cns in hits.items():
        share = len(cns) / len(rows)
        lines.append(f"| {rule} | {len(cns):,} ({share:.1%}) | "
                     f"{' '.join(map(str, cns[:5]))} |")
        limit = RUN_ON_LIMIT if rule == "run_on" else NEVER_LIMIT
        if share > limit:
            problems.append(f"{rule}: {len(cns):,} of {len(rows):,} rows "
                            f"({share:.1%}), e.g. "
                            + " ".join(map(str, cns[:5])))

    token = os.environ.get("USAJOBS_API_TOKEN")
    lines += ["", "## Against the Search API", ""]
    if not token:
        lines.append("Skipped: no USAJOBS_API_TOKEN.")
    else:
        api = api_items(token, args.api_days, args.api_pages)
        both = sum(1 for r in rows if str(r["usajobs_control_number"]) in api)
        print(f"Search API: {len(api):,} announcements, {both:,} also "
              f"scraped in the window")
        lines.append(f"{len(api):,} announcements from the API "
                     f"(DatePosted={args.api_days}); {both:,} of them among "
                     f"the rows checked.")
        lines += ["", "| field | compared | below "
                  f"{MATCH:.0%} | examples (score) |", "|---|---|---|---|"]
        result, spacing = api_check(rows, api)
        for f, (n, bad) in result.items():
            ex = " ".join(f"{cn} ({s:.2f})" for cn, s in
                          sorted(bad, key=lambda x: x[1])[:5])
            lines.append(f"| {f} | {n:,} | {len(bad):,} | {ex} |")
            if n >= MIN_COMPARED and len(bad) / n > MISMATCH_LIMIT:
                problems.append(f"{f} disagrees with the Search API on "
                                f"{len(bad):,} of {n:,} rows, e.g. {ex}")
        lines.append(f"| space before punctuation, not in the API's text | "
                     f"{both:,} | {len(spacing):,} | "
                     f"{' '.join(map(str, spacing[:5]))} |")
        if both >= MIN_COMPARED and len(spacing) / both > MISMATCH_LIMIT:
            problems.append(f"space before punctuation the API's text does "
                            f"not have: {len(spacing):,} of {both:,} rows, "
                            f"e.g. {' '.join(map(str, spacing[:5]))}")

    os.makedirs(LOGS, exist_ok=True)
    with open(REPORT_FILE, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    print("\n".join(lines))
    if problems:
        with open(WARNING_FILE, "a") as fh:
            for line in problems:
                fh.write(line + "\n")
        print("\nProblems:\n  " + "\n  ".join(problems))
    return 0


if __name__ == "__main__":
    sys.exit(main())
