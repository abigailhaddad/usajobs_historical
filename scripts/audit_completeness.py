#!/usr/bin/env python3
"""Is the published dataset missing any announcements the mirror knows about?

Compares every posting in the historical mirror against the dataset's manifest.
The manifest is a couple of MB and the mirrors are ~40 MB a year, so this
answers the question without downloading the dataset itself.

    python audit_completeness.py                    # 2018-2025
    python audit_completeness.py --years 2022 2023
    python audit_completeness.py --probe            # also fetch what is missing

Exit status is 0 when every gap is a known-unreachable announcement and 1 when
something new is missing, so it can gate a workflow.

Why the known-unreachable list exists: usajobs.gov serves 503 forever for 21
announcements and 404 for one more. An audit that does not know this reports 22
missing, and the obvious response -- rebuild those nineteen month files -- moves
about 5 GB to add nothing, because a month file is rewritten whole and there is
no text to put in it. See unreachable_announcements.csv.
"""

import argparse
import csv
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

MIRROR = "https://pub-317c58882ec04f329b63842c1eb65b0c.r2.dev/data"
HERE = os.path.dirname(os.path.abspath(__file__))
UNREACHABLE = os.path.join(HERE, "unreachable_announcements.csv")


def known_unreachable():
    """Control numbers usajobs.gov refuses to serve, as {cn: status}."""
    out = {}
    if not os.path.exists(UNREACHABLE):
        return out
    with open(UNREACHABLE) as fh:
        rows = csv.DictReader(line for line in fh if not line.startswith("#"))
        for r in rows:
            out[r["usajobs_control_number"]] = r["status"]
    return out


def mirror_path(year, cache_dir):
    """The year's mirror, downloaded if this machine does not have it."""
    import urllib.request
    os.makedirs(cache_dir, exist_ok=True)
    path = os.path.join(cache_dir, f"historical_jobs_{year}.parquet")
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        print(f"  downloading the {year} mirror", flush=True)
        urllib.request.urlretrieve(f"{MIRROR}/historical_jobs_{year}.parquet", path)
    return path


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    # 2013-2026, not the backfill's 2018-2025. Defaulting to the range the
    # backfill happens to run makes the audit self-confirming: on 2026-09-11 it
    # reported the dataset complete while 4,048 postings from 2013-2016 had
    # never been fetched at all, because nothing had ever asked about them.
    p.add_argument("--years", nargs="+", type=int,
                   default=list(range(2013, 2027)))
    p.add_argument("--cache-dir", default="data/mirror-cache",
                   help="where year mirrors are kept between runs")
    p.add_argument("--repo", default=os.environ.get(
        "HF_DATASET_REPO", "abigailhaddad/usajobs-scraping"))
    p.add_argument("--probe", action="store_true",
                   help="fetch each missing announcement to see whether it is "
                        "genuinely unreachable rather than merely unscraped")
    args = p.parse_args()

    import duckdb
    from publish_to_huggingface import published_control_numbers
    from huggingface_hub import get_token, list_repo_files

    token = os.environ.get("HF_TOKEN") or get_token()
    have = published_control_numbers(args.repo, token)
    print(f"manifest: {len(have):,} announcements")

    files = {os.path.basename(f)[:-len('.parquet')].replace("_", "-")
             for f in list_repo_files(args.repo, repo_type="dataset", token=token)
             if f.startswith("data/") and f.endswith(".parquet")}
    month_files = sorted(m for m in files if len(m) == 7)
    print(f"month files on the dataset: {len(month_files)} "
          f"({month_files[0]} .. {month_files[-1]})")

    con = duckdb.connect()
    expected = defaultdict(set)
    for year in args.years:
        path = mirror_path(year, args.cache_dir)
        rows = con.execute(f"""
            SELECT usajobsControlNumber::varchar,
                   substr(positionOpenDate, 1, 7)
            FROM read_parquet('{path}')
            WHERE positionOpenDate IS NOT NULL
              AND usajobsControlNumber IS NOT NULL
        """).fetchall()
        # A mirror year holds postings whose open date lands outside it; each
        # belongs to whichever year's audit covers its own month.
        for cn, month in rows:
            if month[:4] == str(year):
                expected[month].add(cn)

    # Deduplicated: a handful of control numbers carry a different open date in
    # two mirror years and so appear under two months. Summing the per-month
    # sets counts those twice and quietly inflates both the total and the
    # published figure -- 124 of them across 2018-2025.
    every = set()
    for cns in expected.values():
        every |= cns
    total = len(every)
    unreachable = known_unreachable()
    # The current year is still being collected, so a gap there means "not
    # fetched yet", not "missing". Reported, but it does not fail the audit --
    # otherwise this can never come back clean.
    from datetime import date
    current_year = str(date.today().year)
    gaps, unexplained, in_flight, seen = [], [], [], set()
    for month in sorted(expected):
        for cn in sorted(expected[month] - have):
            if cn in seen:
                continue
            seen.add(cn)
            if cn in unreachable:
                gaps.append((month, cn))
            elif month[:4] == current_year:
                in_flight.append((month, cn))
            else:
                unexplained.append((month, cn))

    published = total - len(gaps) - len(unexplained) - len(in_flight)
    print(f"mirror postings: {total:,}")
    print(f"published:       {published:,} ({100 * published / total:.4f}%)")
    print(f"known unreachable: {len(gaps)}")
    if in_flight:
        print(f"{current_year} not yet collected: {len(in_flight)}  "
              f"(the daily pipeline's, not a backfill gap)")
    print(f"UNEXPLAINED:       {len(unexplained)}")

    missing_files = [m for m in sorted(expected) if m not in files]
    if missing_files:
        print(f"\nmonths with no file on the dataset: {missing_files}")

    if unexplained:
        print("\nannouncements missing for no known reason:")
        for month, cn in unexplained:
            print(f"  {month}  {cn}")
        if args.probe:
            from usajobs_scrape import fetch_job_page, new_session
            session = new_session()
            print("\nprobing them:")
            for month, cn in unexplained:
                html, err = fetch_job_page(session, cn)
                state = (f"OK {len(html):,} bytes" if html is not None
                         else "404" if err is None else str(err)[:60])
                print(f"  {month}  {cn}  {state}")
            print("\nAnything that answers 503 or 404 consistently belongs in "
                  "unreachable_announcements.csv. Check it against a known-good "
                  "control number from the same month first: 21 pages failing "
                  "at once looks exactly like rate limiting and is not.")

    return 1 if (unexplained or missing_files) else 0


if __name__ == "__main__":
    sys.exit(main())
