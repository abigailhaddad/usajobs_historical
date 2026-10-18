#!/usr/bin/env python3
"""Notice when usajobs.gov changes its markup and junk leaks into a section.

The fill-rate check in collect_scraped_data.py catches a section that stops
parsing. It cannot catch the opposite: a section that still parses but now
carries page chrome. That is what happened in September 2026, when the help
links moved domain and five sections started with the button label "Help" --
every section stayed filled, so nothing fired, and 9,800 published
announcements carried it before anyone looked.

Real announcement text varies from posting to posting, but how a section opens
and closes across thousands of postings is stable from one month to the next:
"You will be" opens every howYouWillBeEvaluated, "A career with" opens 89% of
benefits. So the check profiles the first word and the last three words of
each section, and compares new postings against ones published just before
them. In September 2026 "Help" went from 0% to 42% of majorDuties' first
words, and "Conditions" fell from 100% to 58% of requirements'.

The baseline rolls rather than being fixed, because the content itself shifts:
mid-2025 every requirements section started ending "United States
Constitution." Measured month against previous month over 2025-01..2026-09, a
20-point threshold fires on 2025-03, 2025-04, 2025-07, 2025-08 (that
Constitution line arriving, and a "Hide additional information" button
leaving the page), 2025-10 and 2026-09, and is quiet for the eleven months
between. Each of those is a real change in what the announcements say or how
the page is built.

    python section_shape.py --check 2026-09 --against 2026-08
"""

import argparse
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from usajobs_scrape import SECTION_FIELDS

SECTIONS = [f for f in SECTION_FIELDS if f != "text"]
# A share has to move by this much to count as drift. See the calibration above.
THRESHOLD = 0.20
# Below this many rows a share is too noisy to compare.
MIN_ROWS = 200
TOP = 10


def ends(value):
    words = value.split()
    return words[0], " ".join(words[-3:])


def profile(columns):
    """{section: {"n", "first": {word: share}, "last": {words: share}}} from
    {section: [values]}. Empty values are not counted; a section that stops
    parsing is the fill-rate check's job."""
    out = {}
    for section in SECTIONS:
        first, last, n = Counter(), Counter(), 0
        for value in columns.get(section, []):
            # Missing sections arrive as None, or NaN by way of pandas.
            if not isinstance(value, str) or not value.strip():
                continue
            a, b = ends(value)
            first[a] += 1
            last[b] += 1
            n += 1
        out[section] = {
            "n": n,
            "first": {k: c / n for k, c in first.most_common(TOP)} if n else {},
            "last": {k: c / n for k, c in last.most_common(TOP)} if n else {},
        }
    return out


def drift(current, baseline, threshold=THRESHOLD, min_rows=MIN_ROWS):
    """Messages for every opening or closing whose share moved by threshold
    or more. Shares outside a profile's top list count as zero, which is
    conservative: the top list holds everything above a few percent."""
    found = []
    for section in SECTIONS:
        cur, base = current.get(section), baseline.get(section)
        if not cur or not base or cur["n"] < min_rows:
            continue
        for end in ("first", "last"):
            for key in set(cur[end]) | set(base[end]):
                now, was = cur[end].get(key, 0.0), base[end].get(key, 0.0)
                if abs(now - was) >= threshold:
                    where = "start" if end == "first" else "end"
                    found.append(
                        f"{section}: {now:.0%} of {cur['n']:,} now {where} "
                        f"with {key!r}, against {was:.0%} in the baseline")
    return found


def merge(profiles):
    """One baseline from several month profiles, weighted by rows."""
    out = {}
    for section in SECTIONS:
        total = sum(p[section]["n"] for p in profiles)
        merged = {"n": total, "first": {}, "last": {}}
        for end in ("first", "last"):
            acc = Counter()
            for p in profiles:
                for k, share in p[section][end].items():
                    acc[k] += share * p[section]["n"]
            merged[end] = {k: v / total for k, v in acc.most_common(TOP)} \
                if total else {}
        out[section] = merged
    return out


def month_profile(repo, month, token=None):
    import pyarrow.parquet as pq
    from huggingface_hub import HfFileSystem
    from publish_to_huggingface import month_file
    fs = HfFileSystem(token=token)
    with fs.open(f"datasets/{repo}/{month_file(month)}", "rb") as fh:
        t = pq.read_table(fh, columns=SECTIONS)
    return profile({s: t.column(s).to_pylist() for s in SECTIONS})


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--check", required=True, metavar="YYYY-MM")
    p.add_argument("--against", nargs="+", required=True, metavar="YYYY-MM")
    p.add_argument("--repo", default=os.environ.get(
        "HF_DATASET_REPO", "abigailhaddad/usajobs-scraping"))
    args = p.parse_args()
    token = os.environ.get("HF_TOKEN")

    baseline = merge([month_profile(args.repo, m, token) for m in args.against])
    found = drift(month_profile(args.repo, args.check, token), baseline)
    print(f"{args.check} against {', '.join(args.against)}: "
          f"{'as expected' if not found else ''}")
    for line in found:
        print(f"   {line}")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
