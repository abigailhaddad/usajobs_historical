#!/usr/bin/env python3
"""
Re-poll positionOpeningStatus for historical jobs that don't have a final status.

Queries the USAJobs Historical API by date range for all jobs currently marked as
'Accepting applications', 'Applications under review', or NULL status, then updates
only the positionOpeningStatus (and last_seen) field in the parquet files.

Features:
  - Starts with most recent non-final dates first
  - Writes updates to parquet every 10 dates (incremental saves)
  - Parallel API fetching with ThreadPoolExecutor
  - Flushed stdout for real-time progress visibility

Usage:
    caffeinate python scripts/repoll_status.py
    caffeinate python scripts/repoll_status.py --years 2024 2025 2026
    caffeinate python scripts/repoll_status.py --workers 4
"""

import argparse
import json
import time
import os
import sys
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

FINAL_STATUSES = {'Candidate selected', 'Job canceled', 'Job closed'}

SAVE_EVERY = 10  # Write to parquet every N dates


def log(msg):
    """Print with flush so output is visible even when piped."""
    print(msg, flush=True)


def get_page(params: Optional[Dict] = None, next_url: Optional[str] = None, retries: int = 5) -> Dict:
    base_url = "https://data.usajobs.gov"
    for attempt in range(retries):
        try:
            if next_url:
                resp = requests.get(next_url, timeout=120)
            else:
                resp = requests.get(f"{base_url}/api/historicjoa", params=params, timeout=120)

            if resp.status_code == 503:
                wait = (attempt + 1) * 5
                time.sleep(wait)
                continue

            resp.raise_for_status()

            if resp.text.strip() == "204 No Content":
                return {"data": []}

            return resp.json()

        except requests.RequestException as e:
            if attempt < retries - 1:
                wait = (attempt + 1) * 5
                time.sleep(wait)
            else:
                raise


def fetch_all_for_date(date_str: str) -> List[Dict]:
    """Fetch all jobs from the Historical API for a single date."""
    params = {"StartPositionOpenDate": date_str, "EndPositionOpenDate": date_str}
    all_jobs = []
    next_url = None

    while True:
        data = get_page(params=params, next_url=next_url)
        jobs = data.get("data", [])
        all_jobs.extend(jobs)

        next_path = data.get("paging", {}).get("next")
        if next_path and next_path.strip():
            if next_path.startswith("http"):
                next_url = next_path
            else:
                next_url = f"https://data.usajobs.gov{next_path}"
            params = None
        else:
            break

    return all_jobs


def get_nonfinal_dates(parquet_path: str) -> List[str]:
    """Get sorted list of distinct positionOpenDate values for non-final jobs."""
    df = pd.read_parquet(parquet_path)
    mask = ~df['positionOpeningStatus'].isin(FINAL_STATUSES) | df['positionOpeningStatus'].isna()
    nonfinal = df[mask]
    dates = nonfinal['positionOpenDate'].dropna().apply(lambda x: str(x)[:10]).unique()
    return sorted(dates)


def update_statuses(parquet_path: str, status_map: Dict[str, str]) -> int:
    """Update positionOpeningStatus in a parquet file from a control_number->status map.
    Returns count of changed statuses."""
    if not status_map:
        return 0

    df = pd.read_parquet(parquet_path)

    # Build lookup key column
    if 'usajobsControlNumber' in df.columns:
        keys = df['usajobsControlNumber'].astype(str)
    elif 'usajobs_control_number' in df.columns:
        keys = df['usajobs_control_number'].astype(str)
    else:
        return 0

    changed = 0
    now = datetime.now().isoformat()
    for idx, key in keys.items():
        if key in status_map:
            old = df.at[idx, 'positionOpeningStatus']
            new = status_map[key]
            if old != new:
                df.at[idx, 'positionOpeningStatus'] = new
                df.at[idx, 'last_seen'] = now
                changed += 1

    if changed > 0:
        df.to_parquet(parquet_path, index=False)

    return changed


def fetch_date_worker(date_str: str) -> tuple:
    """Worker function for parallel fetching. Returns (date_str, jobs_list)."""
    try:
        jobs = fetch_all_for_date(date_str)
        return (date_str, jobs, None)
    except Exception as e:
        return (date_str, [], str(e))


def main():
    parser = argparse.ArgumentParser(description="Re-poll status for non-final historical jobs")
    parser.add_argument("--years", nargs="*", type=int, help="Specific years to re-poll (default: all)")
    parser.add_argument("--dry-run", action="store_true", help="Just count dates, don't query API")
    parser.add_argument("--workers", type=int, default=10, help="Parallel API workers (default: 10)")
    args = parser.parse_args()

    # Find all historical parquet files
    files = {}
    for f in sorted(os.listdir(DATA_DIR)):
        if f.startswith('historical_jobs_') and f.endswith('.parquet') and 'backup' not in f:
            year = int(f.replace('historical_jobs_', '').replace('.parquet', ''))
            if args.years and year not in args.years:
                continue
            files[year] = os.path.join(DATA_DIR, f)

    # Collect all dates that need re-polling
    dates_by_year = {}  # year -> [dates]
    date_to_year = {}   # date -> year
    for year, path in sorted(files.items()):
        dates = get_nonfinal_dates(path)
        dates_by_year[year] = dates
        for d in dates:
            date_to_year[d] = year
        log(f"{year}: {len(dates)} dates with non-final jobs")

    # Sort all dates DESCENDING (most recent first)
    all_dates = sorted(date_to_year.keys(), reverse=True)
    total_dates = len(all_dates)

    log(f"\nTotal dates to query: {total_dates}")
    log(f"Workers: {args.workers}")
    log(f"Will save every {SAVE_EVERY} dates\n")

    if args.dry_run:
        return

    start_time = time.time()
    year_status_maps: Dict[int, Dict[str, str]] = {}
    dates_queried = 0
    api_jobs_seen = 0
    total_changed = 0
    dates_since_save = 0
    errors = 0

    # Process in batches for parallel fetching
    batch_size = args.workers
    i = 0

    while i < total_dates:
        batch = all_dates[i:i + batch_size]

        # Fetch batch in parallel
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(fetch_date_worker, d): d for d in batch}
            for future in as_completed(futures):
                date_str, jobs, error = future.result()

                if error:
                    log(f"  ERROR {date_str}: {error}")
                    errors += 1
                    dates_queried += 1
                    continue

                year = date_to_year[date_str]
                api_jobs_seen += len(jobs)

                if year not in year_status_maps:
                    year_status_maps[year] = {}

                for job in jobs:
                    cn = str(job.get('usajobsControlNumber', ''))
                    status = job.get('positionOpeningStatus')
                    if cn and status:
                        year_status_maps[year][cn] = status

                dates_queried += 1
                dates_since_save += 1

        # Progress update every batch
        elapsed = time.time() - start_time
        rate = dates_queried / elapsed * 60 if elapsed > 0 else 0
        remaining = (total_dates - dates_queried) / rate if rate > 0 else 0
        log(f"[{dates_queried}/{total_dates}] {batch[0]} | "
            f"{api_jobs_seen:,} jobs seen | "
            f"{rate:.0f} dates/min | ~{remaining:.0f} min left | "
            f"{total_changed:,} changed")

        # Incremental save every SAVE_EVERY dates
        if dates_since_save >= SAVE_EVERY:
            for year, path in sorted(files.items()):
                sm = year_status_maps.get(year, {})
                if sm:
                    changed = update_statuses(path, sm)
                    total_changed += changed
                    if changed:
                        log(f"  Saved {year}: {changed:,} statuses updated")
            # Clear maps after saving
            year_status_maps = {}
            dates_since_save = 0

        i += batch_size

    # Final save for any remaining updates
    if year_status_maps:
        log(f"\n--- Final save ---")
        for year, path in sorted(files.items()):
            sm = year_status_maps.get(year, {})
            if sm:
                changed = update_statuses(path, sm)
                total_changed += changed
                log(f"{year}: {changed:,} statuses updated")

    elapsed_total = time.time() - start_time
    log(f"\nDone! {dates_queried} dates in {elapsed_total/60:.1f} min")
    log(f"API jobs seen: {api_jobs_seen:,}")
    log(f"Statuses changed: {total_changed:,}")
    log(f"Errors: {errors}")


if __name__ == "__main__":
    main()
