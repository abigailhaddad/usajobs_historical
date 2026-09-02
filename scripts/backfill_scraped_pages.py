#!/usr/bin/env python3
"""
Fetch announcement pages for postings collected before the scrape existed.

collect_scraped_data.py only sees what is open right now, so the scraped
collection starts the day it was switched on. usajobs.gov serves closed
announcements indefinitely, though, so the earlier ones can be filled in — and
have to be, or the published dataset has two shapes and every consumer has to
handle both.

The posting list comes from data/historical_jobs_{year}.parquet: the historical
API needs no key and reports closed postings, so it is the complete list of
what exists.

Long-running by design — roughly 160k pages for a full year, about three hours
at the default concurrency. Run it on its own, not inside the daily workflow.

Resumable and crash-safe. Pages land in immutable shard files tagged with a
per-run id; a rerun skips every control number already in a shard or in the
main parquet. Kill it whenever. Shards fold into the main parquet at the end,
or on the next run with --compact-only.

    python backfill_scraped_pages.py --year 2026 --dry-run
    python backfill_scraped_pages.py --year 2026
    python backfill_scraped_pages.py --year 2026 --limit 500   # a taste
    python backfill_scraped_pages.py --year 2026 --compact-only
"""

import argparse
import os
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd
import pyarrow.parquet as pq
from tqdm import tqdm

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from collect_current_data import save_jobs_to_parquet
from usajobs_scrape import fetch_job_page, new_session, parse_job_page

SHARD_ROWS = 2000
_local = threading.local()


def parse_args():
    p = argparse.ArgumentParser(
        description="Backfill announcement pages for a year of postings")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--data-dir", default="data")
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--limit", type=int, default=0,
                   help="Stop after this many pages (0 = everything)")
    p.add_argument("--dry-run", action="store_true",
                   help="Report what is missing and fetch nothing")
    p.add_argument("--compact-only", action="store_true",
                   help="Fold existing shards into the main parquet and stop")
    return p.parse_args()


def thread_session():
    if not hasattr(_local, "session"):
        _local.session = new_session()
    return _local.session


def shard_dir(data_dir, year):
    return os.path.join(data_dir, f".scraped_shards_{year}")


def stored_control_numbers(data_dir, year):
    """Control numbers already scraped: in the main parquet or in any shard."""
    known = set()
    main = os.path.join(data_dir, f"scraped_jobs_{year}.parquet")
    if os.path.exists(main):
        known |= set(pd.read_parquet(main, columns=["usajobs_control_number"])
                     ["usajobs_control_number"].dropna().astype(str))

    shards = shard_dir(data_dir, year)
    if os.path.isdir(shards):
        for name in sorted(os.listdir(shards)):
            if not name.endswith(".parquet"):
                continue
            path = os.path.join(shards, name)
            try:
                known |= set(pq.read_table(path, columns=["usajobs_control_number"])
                             .column(0).to_pylist())
            except Exception as e:
                print(f"  unreadable shard {name} ({e}), ignoring")
    return known


def wanted_postings(data_dir, year):
    """Every posting the historical mirror has for the year."""
    path = os.path.join(data_dir, f"historical_jobs_{year}.parquet")
    if not os.path.exists(path):
        sys.exit(f"{path} is missing — this needs the historical mirror.")
    df = pd.read_parquet(path, columns=["usajobsControlNumber", "positionOpenDate"])
    df = df.dropna(subset=["usajobsControlNumber"]).drop_duplicates(
        "usajobsControlNumber")
    return {str(int(r.usajobsControlNumber)): str(r.positionOpenDate)[:10]
            for r in df.itertuples()}


def compact(data_dir, year):
    """Fold shards into the main parquet, then delete them.

    save_jobs_to_parquet rewrites the whole file, so this runs once at the end
    rather than per batch — at a year's scale, rewriting a growing multi-hundred
    megabyte parquet after every batch is the difference between minutes and
    hours.
    """
    shards = shard_dir(data_dir, year)
    if not os.path.isdir(shards):
        return 0
    files = sorted(f for f in os.listdir(shards) if f.endswith(".parquet"))
    if not files:
        return 0

    print(f"Folding {len(files)} shard(s) into scraped_jobs_{year}.parquet")
    frames = [pd.read_parquet(os.path.join(shards, f)) for f in files]
    rows = pd.concat(frames, ignore_index=True).drop_duplicates(
        "usajobs_control_number")

    # save_jobs_to_parquet stamps inserted_at/last_seen and merges on control
    # number, so a posting already in the file is updated rather than doubled.
    save_jobs_to_parquet(rows.to_dict("records"),
                         os.path.join(data_dir, f"scraped_jobs_{year}.parquet"))

    for f in files:
        os.remove(os.path.join(shards, f))
    os.rmdir(shards)
    return len(rows)


def main() -> int:
    args = parse_args()

    if args.compact_only:
        n = compact(args.data_dir, args.year)
        print(f"Compacted {n:,} rows" if n else "No shards to compact")
        return 0

    wanted = wanted_postings(args.data_dir, args.year)
    known = stored_control_numbers(args.data_dir, args.year)
    todo = sorted(cn for cn in wanted if cn not in known)

    print(f"{args.year}: {len(wanted):,} postings in the historical mirror, "
          f"{len(known):,} already scraped, {len(todo):,} to fetch")
    if args.limit:
        todo = todo[:args.limit]
        print(f"  limited to {len(todo):,} this run")
    if not todo:
        compact(args.data_dir, args.year)
        return 0
    if args.dry_run:
        for cn in todo[:10]:
            print(f"   {wanted[cn]}  {cn}")
        return 0

    shards = shard_dir(args.data_dir, args.year)
    os.makedirs(shards, exist_ok=True)
    run_id = uuid.uuid4().hex[:8]

    lock = threading.Lock()
    batch, part, gone, failed = [], 0, 0, 0
    started = time.time()

    def flush():
        nonlocal batch, part
        if not batch:
            return
        part += 1
        path = os.path.join(shards, f"part-{run_id}-{part:05d}.parquet")
        pd.DataFrame(batch).to_parquet(path, index=False, compression="zstd")
        batch = []

    def work(cn):
        nonlocal gone, failed
        html, error = fetch_job_page(thread_session(), cn)
        if html is None:
            with lock:
                # A 404 (error is None) is a real answer, not a failure: the
                # page is gone and retrying will not bring it back.
                if error is None:
                    gone += 1
                else:
                    failed += 1
            return

        try:
            row = parse_job_page(html)
        except Exception as e:
            with lock:
                failed += 1
            print(f"   parse failed for {cn}: {e}")
            return

        # The page carries its own control number, but trust the list we asked
        # for: a redirect would otherwise file the row under the wrong posting.
        row["usajobs_control_number"] = cn
        row["usajobsControlNumber"] = int(cn)
        row["positionOpenDate"] = row.get("positionOpenDate") or wanted[cn]
        with lock:
            batch.append(row)
            if len(batch) >= SHARD_ROWS:
                flush()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        list(tqdm(pool.map(work, todo), total=len(todo),
                  desc="Announcement pages", unit="page"))
    flush()

    elapsed = (time.time() - started) / 60
    print(f"Fetched {len(todo) - gone - failed:,} pages in {elapsed:.1f} min, "
          f"{failed} failed, {gone} already removed (404)")

    compact(args.data_dir, args.year)
    if failed:
        print("Failed pages stay unstored — rerun to retry them")
    return 0


if __name__ == "__main__":
    sys.exit(main())
