#!/usr/bin/env python3
"""Re-fetch every announcement page on the dataset, a few months a night.

The parser before PR #668 put a space at every HTML tag. Agencies' own HTML
splits words across tags (<strong>Fi</strong><strong>nancial), so 12 of 92
stored rows sampled on 22 October 2026 had a word split in two ("Fi
nancial", "Ed ucation", "P lease"); 24 were missing the words of an agency's
link to the USAJOBS help site, which it deleted; 91 had a space before
punctuation after a link. None of it can be undone from the stored text, so
this fetches each page again and gives the row the current parser's section
fields. Everything else in the row stays as it is.

One month file at a time, newest first, each committed on its own on top of
the revision it was read at. A month is done when the dataset has a commit
titled with COMMIT_PREFIX for it, so progress lives on the dataset itself and
a run that dies loses at most the month it was on. A month is only started
if, at the rate measured so far, it should finish before --hours runs out.

A month is not committed if fewer than MIN_FETCHED of its pages came back
(rate limiting, challenge pages, an outage), or if too many rows would lose
their qualifications (a markup change). Pages that 404 or fail keep their
stored text and are counted.

Pages come back as they are now, so an agency's edits since the original
scrape come with them.

Rows with the same control number in R2's scraped_jobs files (--scraped) get
the same text, so a month rebuilt from those files does not bring the old
text back.

    python refetch_all_pages.py --hours 5 --scraped data/scraped_jobs_*.parquet
    python refetch_all_pages.py --months 2013-09 --dry-run
"""

import argparse
import csv
import multiprocessing
import os
import shutil
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from refetch_leftover_rows import SECTION_FIELDS, replace_rows, verify  # noqa: E402

COMMIT_PREFIX = "Re-fetch every announcement page in "

# Below this share of a month's pages fetched, something is wrong with the
# fetching, not the pages; the month waits for the next run.
MIN_FETCHED = 0.95
# Above this share of rows going from some qualifications to none, the parse
# is wrong (a markup change), not the rows.
MAX_LOST_QUALS = 0.02
# Pages a second before any month has been measured. The backfill managed
# 3.7 to 9.7 on a 4-core box; Actions runners have 4.
FIRST_GUESS_RATE = 3.0

_local = threading.local()


def _session():
    from usajobs_scrape import new_session
    if not hasattr(_local, "session"):
        _local.session = new_session()
    return _local.session


def parse(html):
    """Section fields of one page, or None if it has no requirements text."""
    from bs4 import BeautifulSoup
    from usajobs_scrape import parse_sections
    sections = parse_sections(BeautifulSoup(html, "html.parser"))
    if not any(sections.get(f) for f in ("qualificationSummary",
                                         "requirements")):
        return None
    return {f: sections.get(f) for f in SECTION_FIELDS}


def fetch_all(cns, workers, pool):
    """{cn: sections} for the pages that came back, and {cn: problem}."""
    from usajobs_scrape import fetch_job_page

    def work(cn):
        html, err = fetch_job_page(_session(), cn)
        if html is None:
            return cn, None, "404" if err is None else f"error: {err[:60]}"
        try:
            sections = (pool.submit(parse, html).result() if pool
                        else parse(html))
        except Exception as e:  # noqa: BLE001 -- one page, counted
            return cn, None, f"parse failed: {e}"[:80]
        if sections is None:
            return cn, None, "no requirements text"
        return cn, sections, None

    fresh, problems = {}, {}
    with ThreadPoolExecutor(max_workers=workers) as threads:
        for cn, sections, problem in threads.map(work, cns):
            if sections:
                fresh[cn] = sections
            else:
                problems[cn] = problem
    return fresh, problems


def done_months(api, repo):
    done = set()
    for c in api.list_repo_commits(repo, repo_type="dataset"):
        if c.title.startswith(COMMIT_PREFIX):
            done.add(c.title[len(COMMIT_PREFIX):].strip())
    return done


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--hours", type=float, default=5.0,
                   help="stop starting months after this long")
    p.add_argument("--months", nargs="*", default=[],
                   help="these months (YYYY-MM) instead of the next undone")
    p.add_argument("--scraped", nargs="*", default=[])
    p.add_argument("--workers", type=int, default=8,
                   help="pages fetched at once")
    p.add_argument("--report", default="refetch_all_pages.csv")
    p.add_argument("--repo", default=os.environ.get(
        "HF_DATASET_REPO", "abigailhaddad/usajobs-scraping"))
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    import pyarrow.parquet as pq
    from huggingface_hub import (CommitOperationAdd, HfApi, get_token,
                                 hf_hub_download)
    from publish_to_huggingface import (BUILD_DIR, DOWNLOAD_DIR, check_types,
                                        month_file)
    from repair_section_leaks import all_months

    started = time.time()
    deadline = started + args.hours * 3600
    token = os.environ.get("HF_TOKEN") or get_token()
    api = HfApi(token=token)

    every = all_months(args.repo, token)
    done = done_months(api, args.repo)
    names = {m: month_file(m) for m in every}
    todo = args.months or [m for m in sorted(every, reverse=True)
                           if names[m] not in done]
    print(f"{len(every)} month files; {len(done)} done; "
          f"{len(todo)} to go{' (as asked)' if args.months else ''}")

    cores = os.cpu_count() or 2
    pool = (ProcessPoolExecutor(max_workers=max(1, min(4, cores - 1)),
                                mp_context=multiprocessing.get_context("spawn"))
            if cores > 2 else None)
    out_dir = BUILD_DIR / "refetch_all"
    rate = FIRST_GUESS_RATE
    pages_done, seconds = 0, 0.0
    report = []

    try:
        for month in todo:
            name = names.get(month) or month_file(month)
            revision = api.dataset_info(args.repo).sha
            src = hf_hub_download(args.repo, name, repo_type="dataset",
                                  token=token, revision=revision,
                                  cache_dir=DOWNLOAD_DIR)
            before = pq.read_table(src)
            shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
            cns = sorted({str(c) for c in before.column(
                "usajobsControlNumber").to_pylist() if c})

            left = deadline - time.time()
            if len(cns) / rate > left:
                print(f"{month}: {len(cns):,} pages would take "
                      f"~{len(cns) / rate / 60:.0f} min at {rate:.1f}/s; "
                      f"{left / 60:.0f} min left. Stopping here.")
                break

            t0 = time.time()
            fresh, problems = fetch_all(cns, args.workers, pool)
            took = time.time() - t0
            pages_done += len(cns)
            seconds += took
            rate = pages_done / seconds if seconds else rate

            after, n = replace_rows(before, fresh)
            verify(before, after)
            old_q = before.column("qualificationSummary").to_pylist()
            new_q = after.column("qualificationSummary").to_pylist()
            lost = sum(1 for a, b in zip(old_q, new_q) if a and not b)
            changed = sum(1 for f in SECTION_FIELDS
                          if f in before.column_names
                          and not before.column(f).equals(after.column(f)))
            kinds = {}
            for prob in problems.values():
                k = prob.split(":")[0]
                kinds[k] = kinds.get(k, 0) + 1
            share = len(fresh) / len(cns) if cns else 1.0
            print(f"{month}: {len(cns):,} pages in {took / 60:.1f} min "
                  f"({len(cns) / took if took else 0:.1f}/s); "
                  f"{len(fresh):,} fetched ({share:.1%}), "
                  f"not fetched {kinds or 'none'}; {n:,} rows replaced; "
                  f"{lost:,} would lose their qualifications; "
                  f"{changed} section column(s) changed")
            for cn, prob in sorted(problems.items()):
                report.append({"month": month, "control_number": cn,
                               "problem": prob})

            if share < MIN_FETCHED:
                print(f"{month}: only {share:.1%} of pages came back; not "
                      f"committing, and stopping -- the fetching is the "
                      f"problem, not the month")
                return 1
            if n and lost / n > MAX_LOST_QUALS:
                print(f"{month}: {lost:,} of {n:,} rows would lose their "
                      f"qualifications; not committing, and stopping")
                return 1

            if not args.dry_run:
                dest = out_dir / name
                dest.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(after, dest, compression="zstd",
                               compression_level=12)
                check_types(dest)
                info = api.create_commit(
                    repo_id=args.repo, repo_type="dataset",
                    operations=[CommitOperationAdd(
                        path_in_repo=name, path_or_fileobj=str(dest))],
                    parent_commit=revision,
                    commit_message=COMMIT_PREFIX + name,
                    commit_description=(
                        f"{len(fresh):,} of {len(cns):,} pages fetched; "
                        f"not fetched: {kinds or 'none'}. Section fields "
                        f"re-parsed with the current parser."))
                print(f"{month}: committed {info.oid}")
                shutil.rmtree(out_dir, ignore_errors=True)

            for path in args.scraped:
                t = pq.read_table(path)
                t2, m = replace_rows(t, fresh)
                if m:
                    verify(t, t2)
                    print(f"{month}: {path}: {m:,} row(s) given the "
                          f"re-fetched text")
                    if not args.dry_run:
                        pq.write_table(t2, path + ".tmp", compression="zstd")
                        os.replace(path + ".tmp", path)
                del t, t2
            del before, after, fresh
    finally:
        if pool:
            pool.shutdown()
        with open(args.report, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=["month", "control_number",
                                               "problem"])
            w.writeheader()
            w.writerows(report)

    print(f"Run took {(time.time() - started) / 60:.0f} min; "
          f"{pages_done:,} pages at {rate:.1f}/s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
