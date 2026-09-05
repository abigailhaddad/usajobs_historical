#!/usr/bin/env python3
"""
Collect current USAJobs postings by scraping usajobs.gov, with no API key.

This is a shadow of collect_current_data.py, not a replacement. It writes
data/scraped_jobs_{year}.parquet using the same field names the API collection
writes to current_jobs_{year}.parquet, so compare_scrape_to_api.py can diff the
two column by column and tell us whether the HTML is a faithful substitute.
Nothing downstream reads the scraped files yet.

Two phases:

  1. Discovery. One unfiltered ExecuteSearch call returns a facet block giving
     the true open-inventory size and its breakdown by occupational series.
     We then page each series, which is the same 10,000-cap workaround the API
     collection uses. This alone yields ~20 fields per posting.

  2. Detail. For control numbers we have not seen before, fetch
     /job/{n} and parse the announcement page for the fields the search JSON
     does not carry (service type, clearance, telework, openings, promotion
     potential, ...) plus the full page text, stored in the same shape the
     HuggingFace announcement dataset uses.

Postings already in the parquet keep their stored detail fields and only have
their search-derived fields refreshed, so a daily run fetches roughly the
number of announcements posted that day rather than the whole inventory.

    python collect_scraped_data.py --data-dir data/
    python collect_scraped_data.py --data-dir data/ --test      # 5 series
    python collect_scraped_data.py --data-dir data/ --no-details
"""

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Dict, List

from tqdm import tqdm

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cap_alert import check_cap
from collect_current_data import (get_year_from_date, group_jobs_by_year,
                                  load_existing_jobs, save_jobs_to_parquet)
from usajobs_scrape import (MAX_RESULTS, SECTION_FIELDS, diagnose_structure,
                            fetch_job_page, new_session, normalize_search_job,
                            open_inventory, parse_job_page, search_slice)

# Under this fraction of the inventory the facets promised, something in
# discovery is silently dropping postings and the run should be looked at.
COVERAGE_FLOOR = 0.97

WARNING_FILE = os.path.join(os.path.dirname(__file__), "..", "logs",
                            "SCRAPE_COVERAGE_WARNING.txt")

# Fill rates for the announcement sections, measured on the pages parsed this
# run. It has to be measured here rather than by scanning the stored parquet:
# publishing prunes the text of every posting it pushes, so a scan reports the
# unpublished fraction and reads it as markup drift. That is exactly what
# happened on 2026-09-04, when every field was flagged at 47.2% while live
# pages were parsing at 12/12.
HEALTH_FILE = os.path.join(os.path.dirname(__file__), "..", "logs",
                           "scrape_field_health.json")

# When a section stops parsing, the fill rate alone only says which field
# broke. These keep a page it broke on, so the alarm carries its own evidence
# and diagnosing it does not mean opening a browser.
SAMPLE_DIR = os.path.join(os.path.dirname(__file__), "..", "logs",
                          "scrape_samples")
MAX_SAMPLES = 2

# education and benefits are legitimately absent on plenty of announcements,
# so a page missing only those is not evidence of anything.
_ALWAYS_EXPECTED = [f for f in SECTION_FIELDS
                    if f not in ("education", "benefits")]

# 'education' is genuinely absent from about one announcement in six.
MIN_FILL = {"education": 0.50}
MIN_FILL_DEFAULT = 0.90

_local = threading.local()


def parse_args():
    p = argparse.ArgumentParser(
        description="Collect current USAJobs postings by scraping usajobs.gov")
    p.add_argument("--data-dir", default="data",
                   help="Directory for parquet files (defaults to data/)")
    p.add_argument("--workers", type=int, default=8,
                   help="Concurrent detail-page fetches")
    p.add_argument("--test", action="store_true",
                   help="Only walk the first 5 occupational series")
    p.add_argument("--series", help="Only these occupational series "
                                    "(comma-separated), for spot checks")
    p.add_argument("--no-details", action="store_true",
                   help="Discovery only — skip announcement pages entirely")
    p.add_argument("--max-details", type=int, default=0,
                   help="Cap detail fetches this run (0 = no cap)")
    return p.parse_args()


def thread_session():
    """One requests.Session per worker thread. Sharing one across a pool
    mostly works, but its connection pool and cookie jar are not documented as
    thread-safe and this costs nothing."""
    if not hasattr(_local, "session"):
        _local.session = new_session()
    return _local.session


def warn(message: str) -> None:
    os.makedirs(os.path.dirname(WARNING_FILE), exist_ok=True)
    with open(WARNING_FILE, "a") as f:
        f.write(f"{datetime.now().isoformat()} | {message}\n")
    print(f"WARNING: {message}")


def discover(session, test: bool, only: str = "") -> tuple[List[Dict], int]:
    """Every open posting, from the search endpoint. Returns (rows, expected)."""
    series, expected = open_inventory(session)
    if not series:
        print("Discovery returned no series — the search endpoint is not "
              "answering. Nothing collected.")
        return [], 0

    print(f"Open inventory: {expected:,} announcements across "
          f"{len(series)} occupational series")

    codes = sorted(series, key=lambda c: -series[c])
    if only:
        wanted = {c.strip() for c in only.split(",") if c.strip()}
        codes = [c for c in codes if c in wanted]
        print(f"Restricted to {len(codes)} series: {', '.join(codes)}")
    if test:
        codes = codes[:5]
        print(f"Test mode: only the {len(codes)} largest series")

    rows: List[Dict] = []
    seen = set()
    truncated_slices = []

    for code in tqdm(codes, desc="Series", unit="series"):
        jobs, reported, truncated = search_slice(
            session, {"JobCategoryCode": [code]})

        if truncated:
            truncated_slices.append(code)
        check_cap(reported, f"scrape discovery [series {code}] reported total")

        for job in jobs:
            cn = job.get("DocumentID")
            if cn and cn not in seen:
                seen.add(cn)
                rows.append(normalize_search_job(job))

    if truncated_slices:
        warn(f"{len(truncated_slices)} series hit the {MAX_RESULTS:,}-result "
             f"ceiling and were truncated: {', '.join(truncated_slices)}. "
             f"These need sub-slicing (by department) to collect fully.")

    print(f"Discovered {len(rows):,} unique postings "
          f"(expected ~{expected:,} from facets)")

    # The facet total counts every open posting; discovery walks only postings
    # that carry at least one occupational series, so a small shortfall is
    # normal. A large one means pagination broke.
    if expected and not test and not only and len(rows) < expected * COVERAGE_FLOOR:
        warn(f"Scrape discovery found {len(rows):,} postings but the search "
             f"facets report {expected:,} open ({len(rows)/expected:.1%}). "
             f"Pagination may have stopped early.")

    return rows, expected


def add_details(rows: List[Dict], known: set, workers: int,
                max_details: int) -> set:
    """Fetch and parse announcement pages for postings we have not stored yet.

    Mutates `rows` in place. Returns the control numbers that did NOT come back
    with a parsed page, so the caller can leave them unstored. That matters:
    storing a row without its detail fields would mark the posting as known and
    it would never be retried, whereas leaving it out means the next run
    rediscovers it and tries again.
    """
    todo = [r for r in rows
            if r["usajobs_control_number"]
            and r["usajobs_control_number"] not in known]
    skipped = set()
    if max_details and len(todo) > max_details:
        skipped = {r["usajobs_control_number"] for r in todo[max_details:]}
        todo = todo[:max_details]

    if not todo:
        print("No new postings need an announcement page")
        return skipped

    print(f"Fetching {len(todo):,} announcement pages with {workers} workers")
    fetched = 0
    failures = set()
    gone = set()
    samples = []
    lock = threading.Lock()
    started = time.time()

    def work(row: Dict) -> None:
        nonlocal fetched
        cn = row["usajobs_control_number"]
        html, error = fetch_job_page(thread_session(), cn)
        if html is None:
            with lock:
                # A 404 (error is None) is a real answer, not a failure: the
                # page is gone and retrying will not bring it back. Store the
                # search-derived row so we stop asking for it.
                (gone if error is None else failures).add(cn)
            return
        try:
            detail = parse_job_page(html)
        except Exception as e:  # a markup change should not kill the run
            with lock:
                failures.add(cn)
            print(f"   parse failed for {cn}: {e}")
            return

        # The page is the richer source, so it wins wherever it has a value.
        # Control-number fields stay as discovery set them.
        for key, value in detail.items():
            if key in ("usajobsControlNumber", "usajobs_control_number"):
                continue
            if value not in (None, ""):
                row[key] = value
        with lock:
            fetched += 1
            if (len(samples) < MAX_SAMPLES
                    and any(not detail.get(f) for f in _ALWAYS_EXPECTED)):
                samples.append((cn, html))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(tqdm(pool.map(work, todo), total=len(todo),
                  desc="Announcement pages", unit="page"))

    elapsed = (time.time() - started) / 60
    print(f"Fetched {fetched:,} pages in {elapsed:.1f} min, "
          f"{len(failures)} failed, {len(gone)} already removed (404)")
    if failures:
        print("   Failed pages stay unstored and are retried on the next run")

    parsed = [r for r in todo
              if r["usajobs_control_number"] not in failures | gone]
    record_field_health(parsed, samples)
    return skipped | failures


def record_field_health(parsed: List[Dict], samples=None) -> None:
    """Fill rates for the announcement sections on the pages parsed this run.

    This is the only check on whether the HTML parse still works — the API has
    no counterpart for these fields — so it measures the freshly parsed rows,
    which is the one population guaranteed not to have been pruned yet.
    """
    if not parsed:
        return

    stats = {}
    for field in SECTION_FIELDS:
        filled = sum(1 for r in parsed if r.get(field))
        stats[field] = {"parsed": len(parsed), "filled": filled,
                        "rate": filled / len(parsed)}

    os.makedirs(os.path.dirname(HEALTH_FILE), exist_ok=True)
    with open(HEALTH_FILE, "w") as f:
        json.dump({"measured_at": datetime.now().isoformat(),
                   "pages_parsed": len(parsed), "fields": stats}, f, indent=2)

    below = []
    print(f"Announcement section fill over {len(parsed):,} pages parsed:")
    for field, st in stats.items():
        floor = MIN_FILL.get(field, MIN_FILL_DEFAULT)
        flag = "" if st["rate"] >= floor else "   BELOW FLOOR"
        print(f"   {st['filled']:6,}/{st['parsed']:,}  {st['rate']:6.1%}  "
              f"{field}{flag}")
        if st["rate"] < floor:
            below.append(field)
            warn(f"Announcement section '{field}' parsed out of only "
                 f"{st['rate']:.1%} of {len(parsed):,} pages fetched this run; "
                 f"floor is {floor:.0%}. The page markup has probably changed.")

    if below:
        report_structure(samples or [], below)


def report_structure(samples, below) -> None:
    """Say what the page looks like now, and keep a copy of one.

    A fill rate says which field broke. This says why: which section ids the
    page still has, which headings sit inside Requirements, and which of those
    the parser cannot map -- that last list is the answer whenever a heading
    gets renamed. Without it the alarm means opening a browser.
    """
    if not samples:
        warn(f"{len(below)} section(s) below floor but no sample page was "
             f"kept, because every page parsed cleanly. The shortfall is in "
             f"which pages were fetched, not in the markup.")
        return

    os.makedirs(SAMPLE_DIR, exist_ok=True)
    for cn, html in samples:
        with open(os.path.join(SAMPLE_DIR, f"{cn}.html"), "w",
                  encoding="utf-8") as f:
            f.write(html)

    cn, html = samples[0]
    try:
        structure = diagnose_structure(html)
    except Exception as e:
        warn(f"Could not diagnose the sample page {cn}: {e}")
        return

    warn(f"Structure of {cn}, a page where a section did not parse: "
         + json.dumps(structure, separators=(",", ":")))
    print(f"\nStructure of {cn} (full page saved under {SAMPLE_DIR}):")
    for key, value in structure.items():
        print(f"   {key}: {value}")


def main() -> None:
    args = parse_args()
    os.makedirs(args.data_dir, exist_ok=True)

    print("Collecting current USAJobs postings by scraping usajobs.gov")
    print("=" * 60)

    session = new_session()
    rows, expected = discover(session, args.test, args.series)
    if not rows:
        warn("Scrape discovery returned 0 postings.")
        return

    # Only postings we have never stored need their announcement page; the
    # detail fields do not change once an announcement is posted, and the ones
    # that do (status, close date) come from discovery on every run.
    known = set()
    for year in {get_year_from_date(r.get("positionOpenDate")) for r in rows}:
        if year:
            known |= load_existing_jobs(
                os.path.join(args.data_dir, f"scraped_jobs_{year}.parquet"))
    print(f"Already stored: {len(known):,} postings")

    undetailed = set()
    if not args.no_details:
        undetailed = add_details(rows, known, args.workers, args.max_details)

    # Insert-only, matching collect_current_data.py: a posting already in the
    # parquet is left alone rather than overwritten. Rewriting it would replace
    # a full row with a discovery-only one, since we only fetch announcement
    # pages for control numbers we have not stored. Changes to a stored
    # posting's status and close date are the repoll job's business, not this
    # collection's.
    fresh = [r for r in rows
             if r["usajobs_control_number"] not in known
             and r["usajobs_control_number"] not in undetailed]
    print(f"\n{len(fresh):,} new postings to store "
          f"({len(rows) - len(fresh) - len(undetailed):,} already stored, "
          f"{len(undetailed):,} deferred to the next run)")
    if not fresh:
        print("Nothing new to write")
        return

    by_year = group_jobs_by_year(fresh)
    if not by_year:
        warn("No scraped postings had a usable open date.")
        return

    for year in sorted(by_year):
        path = os.path.join(args.data_dir, f"scraped_jobs_{year}.parquet")
        print(f"\n{year}: {len(by_year[year]):,} postings -> {path}")
        save_jobs_to_parquet(by_year[year], path)

    print(f"\nDone: {len(fresh):,} new postings across {len(by_year)} year(s)")


if __name__ == "__main__":
    main()
