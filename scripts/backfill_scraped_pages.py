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

It is network-bound, not CPU-bound: a page costs ~32 ms to parse, so a full
year is about 86 CPU-minutes spread over those three hours, or roughly half of
one core. It still runs niced and under a CPU governor by default, so it stays
out of the way of whatever else the machine is doing. --max-cpu is a share of
ONE core, not of the machine: at the default 50 the governor barely bites,
since the fetch rate already holds it near there. Halving it roughly doubles
the wall clock.

Resumable and crash-safe. Pages land in immutable shard files tagged with a
per-run id; a rerun skips every control number already in a shard or in the
main parquet. Kill it whenever. Shards fold into the main parquet at the end,
or on the next run with --compact-only.

    python backfill_scraped_pages.py --year 2026 --dry-run
    python backfill_scraped_pages.py --year 2026
    python backfill_scraped_pages.py --year 2026 --limit 500   # a taste
    python backfill_scraped_pages.py --year 2026 --max-cpu 25  # gentler, slower
    python backfill_scraped_pages.py --year 2026 --max-cpu 0   # no governor
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
    p.add_argument("--max-cpu", type=float, default=50.0,
                   help="Hold average CPU under this percent of ONE core "
                        "(default 50; 0 disables). Lower is gentler and "
                        "proportionally slower.")
    p.add_argument("--nice", type=int, default=10,
                   help="Process niceness, 0-19 (default 10). Higher yields "
                        "more readily to whatever else is running.")
    p.add_argument("--month", help="Only this month (YYYY-MM). Useful for a "
                                   "pilot, or to resume a specific month.")
    p.add_argument("--no-publish", action="store_true",
                   help="Do not push each month to HuggingFace as it finishes. "
                        "Announcement text is 97.8%% of what this writes — 920 MB "
                        "for a year — so by default each month is published and "
                        "its text dropped locally, keeping the working set to "
                        "about one month.")
    p.add_argument("--dry-run", action="store_true",
                   help="Report what is missing and fetch nothing")
    p.add_argument("--compact-only", action="store_true",
                   help="Fold existing shards into the main parquet and stop")
    return p.parse_args()


class CpuGovernor:
    """Hold this process's average CPU use under a share of one core.

    Measures the process's own CPU time against wall time and sleeps the
    calling worker when the ratio runs ahead of target. That lowers the duty
    cycle rather than the cost per page: the same work happens, spread over
    more wall clock, which is what "use less CPU" means for a job that is
    already network-bound.

    time.process_time() counts every thread, so the target is a share of one
    core regardless of --workers.
    """

    def __init__(self, percent_of_one_core: float):
        self.target = (percent_of_one_core or 0) / 100.0
        self.lock = threading.Lock()
        self.wall0 = time.monotonic()
        self.cpu0 = time.process_time()

    def throttle(self) -> None:
        if self.target <= 0:
            return
        with self.lock:
            wall = time.monotonic() - self.wall0
            cpu = time.process_time() - self.cpu0
            if wall <= 0:
                return
            # Want cpu/wall <= target, so wall must be at least cpu/target.
            deficit = cpu / self.target - wall
        if deficit > 0:
            # Capped so a long stall cannot park a worker for minutes.
            time.sleep(min(deficit, 2.0))

    def report(self) -> str:
        wall = time.monotonic() - self.wall0
        cpu = time.process_time() - self.cpu0
        share = 100 * cpu / wall if wall else 0
        return (f"{cpu/60:.1f} CPU-minutes over {wall/60:.1f} wall-minutes "
                f"({share:.0f}% of one core)")


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


def months_awaiting_publish(data_dir, year):
    """Months whose local rows still hold announcement text.

    Publishing prunes that text, so its presence is the marker for "fetched but
    not yet on the dataset". Without this, a run killed between finishing a
    month's pages and pushing them would strand it: the next run finds nothing
    left to fetch for that month, so the fetch loop skips it and it is never
    published.
    """
    import duckdb
    hist = os.path.join(data_dir, f"historical_jobs_{year}.parquet")
    scraped = os.path.join(data_dir, f"scraped_jobs_{year}.parquet")
    if not (os.path.exists(hist) and os.path.exists(scraped)):
        return []
    rows = duckdb.connect().execute(f"""
        SELECT DISTINCT substr(h.positionOpenDate, 1, 7) AS month
        FROM read_parquet('{hist}') h
        JOIN read_parquet('{scraped}') s
          ON h.usajobsControlNumber::varchar = s.usajobs_control_number
        WHERE s.text IS NOT NULL AND h.positionOpenDate IS NOT NULL
        ORDER BY 1
    """).fetchall()
    return [r[0] for r in rows]


def publish_month(data_dir, year, month):
    """Push one month to HuggingFace, which also drops its text from disk.

    Run in a subprocess rather than imported: publish_to_huggingface holds a
    duckdb connection and reads the parquet this process has been writing, and
    a fresh process is the simplest way to be sure it sees the compacted file
    rather than anything cached.
    """
    import subprocess
    from huggingface_hub import get_token
    if not (os.environ.get("HF_TOKEN") or get_token()):
        print(f"  No HuggingFace token — keeping {month} on disk unpublished")
        return
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "publish_to_huggingface.py")
    # --refresh-all, scoped to this one month by --month. The manifest records
    # which announcements are published, not what is in them, so a month whose
    # postings were already listed would otherwise be skipped — which is
    # exactly the case here: those rows are on the dataset with text but no
    # sections, and this run is what gives them sections.
    sys.stdout.flush()
    result = subprocess.run(
        [sys.executable, "-u", script, "--year", str(year), "--month", month,
         "--refresh-all", "--data-dir", data_dir],
        check=False)
    if result.returncode != 0:
        print(f"  publish of {month} did not complete cleanly — its text "
              f"stays on disk and the next run retries it")


def main() -> int:
    args = parse_args()

    if args.compact_only:
        n = compact(args.data_dir, args.year)
        print(f"Compacted {n:,} rows" if n else "No shards to compact")
        return 0

    wanted = wanted_postings(args.data_dir, args.year)
    if args.month:
        wanted = {cn: d for cn, d in wanted.items() if d[:7] == args.month}
        print(f"Restricted to {args.month}: {len(wanted):,} postings")
    known = stored_control_numbers(args.data_dir, args.year)
    todo = sorted(cn for cn in wanted if cn not in known)

    print(f"{args.year}: {len(wanted):,} postings in the historical mirror, "
          f"{len(known):,} already scraped, {len(todo):,} to fetch")
    if args.limit:
        todo = todo[:args.limit]
        print(f"  limited to {len(todo):,} this run")
    if not todo:
        compact(args.data_dir, args.year)
        # Nothing to fetch does not mean nothing to publish: a month can be
        # fully scraped and still be on the dataset without its sections,
        # which is the state every month is in on the first pass.
        if args.month and not args.no_publish:
            publish_month(args.data_dir, args.year, args.month)
        return 0
    if args.dry_run:
        for cn in todo[:10]:
            print(f"   {wanted[cn]}  {cn}")
        return 0

    if args.nice:
        try:
            os.nice(args.nice)
        except (AttributeError, OSError) as e:
            print(f"  could not renice ({e}) — continuing at normal priority")

    governor = CpuGovernor(args.max_cpu)
    if governor.target > 0:
        print(f"  holding CPU under {args.max_cpu:.0f}% of one core, "
              f"niceness +{args.nice}")

    # Push anything a previous run fetched but did not get to publish, before
    # spending hours on new pages. Only months with no work left, so a month
    # that is about to be fetched is not published twice.
    if not args.no_publish:
        pending = [m for m in months_awaiting_publish(args.data_dir, args.year)
                   if m not in {wanted[cn][:7] for cn in todo}]
        for month in pending:
            print(f"Publishing {month}, fetched by an earlier run")
            publish_month(args.data_dir, args.year, month)

    # Work a month at a time and publish each one as it lands. Fetching the
    # whole year first would pile up 920 MB of announcement text on disk before
    # anything could be pushed; this keeps the working set to roughly one
    # month, and makes a killed run resume at month granularity.
    by_month = {}
    for cn in todo:
        by_month.setdefault(wanted[cn][:7], []).append(cn)
    print(f"  across {len(by_month)} month(s): "
          + ", ".join(f"{m} ({len(c):,})" for m, c in sorted(by_month.items())))

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
        # compact() removes the shard directory when it folds a month in, so
        # the next month has to recreate it. Doing that here rather than once
        # before the loop is what keeps a multi-month run alive.
        os.makedirs(shards, exist_ok=True)
        path = os.path.join(shards, f"part-{run_id}-{part:05d}.parquet")
        pd.DataFrame(batch).to_parquet(path, index=False, compression="zstd")
        batch = []

    def work(cn):
        nonlocal gone, failed
        governor.throttle()
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

    for month, cns in sorted(by_month.items()):
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            list(tqdm(pool.map(work, cns), total=len(cns),
                      desc=f"{month}", unit="page"))
        flush()
        compact(args.data_dir, args.year)
        sys.stdout.flush()
        if not args.no_publish:
            publish_month(args.data_dir, args.year, month)

    elapsed = (time.time() - started) / 60
    print(f"\nFetched {len(todo) - gone - failed:,} pages in {elapsed:.1f} min, "
          f"{failed} failed, {gone} already removed (404)")
    print(f"  used {governor.report()}")
    if failed:
        print("Failed pages stay unstored — rerun to retry them")
    return 0


if __name__ == "__main__":
    sys.exit(main())
