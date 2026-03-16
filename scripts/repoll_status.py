#!/usr/bin/env python3
"""
Re-poll positionOpeningStatus for historical jobs that don't have a final status.

Queries the USAJobs Historical API by date range for all jobs currently marked as
'Accepting applications', 'Applications under review', or NULL status, then updates
statuses and inserts any new jobs not already in the parquet files.

Features:
  - Starts with most recent non-final dates first
  - Writes updates to parquet every 10 dates (incremental saves)
  - Parallel API fetching with ThreadPoolExecutor
  - Inserts newly discovered jobs (upsert behavior)
  - Skips recent dates already covered by daily collection
  - Flushed stdout for real-time progress visibility

Usage:
    caffeinate python scripts/repoll_status.py
    caffeinate python scripts/repoll_status.py --years 2024 2025 2026
    caffeinate python scripts/repoll_status.py --workers 4
    caffeinate python scripts/repoll_status.py --skip-recent-days 14
"""

import argparse
import json
import time
import os
import sys
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

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


def update_and_insert(parquet_path: str, status_map: Dict[str, str],
                      new_jobs: List[Dict]) -> tuple:
    """Update statuses for existing jobs and insert new jobs.
    Returns (changed_count, inserted_count, status_transitions).
    status_transitions is a list of (control_number, old_status, new_status)."""
    if not status_map and not new_jobs:
        return 0, 0, []

    df = pd.read_parquet(parquet_path)

    # Sanitize columns that may have mixed list/string types (fixes ArrowInvalid on save)
    for col in ['hiringpaths', 'HiringPaths', 'jobcategories', 'JobCategories',
                'positionlocations', 'PositionLocations']:
        if col in df.columns and df[col].dtype == object:
            has_lists = df[col].apply(lambda x: isinstance(x, (list, dict))).any()
            if has_lists:
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)

    # Build lookup key column
    if 'usajobsControlNumber' in df.columns:
        key_col = 'usajobsControlNumber'
    elif 'usajobs_control_number' in df.columns:
        key_col = 'usajobs_control_number'
    else:
        return 0, 0, []

    keys = df[key_col].astype(str)

    # Update statuses for existing records
    changed = 0
    transitions = []
    now = datetime.now().isoformat()
    for idx, key in keys.items():
        if key in status_map:
            old = df.at[idx, 'positionOpeningStatus']
            new = status_map[key]
            if old != new:
                df.at[idx, 'positionOpeningStatus'] = new
                df.at[idx, 'last_seen'] = now
                transitions.append((key, str(old), str(new)))
                changed += 1

    # Insert new jobs
    inserted = 0
    if new_jobs:
        new_rows = []
        for job in new_jobs:
            row = {}
            for field in ['usajobsControlNumber', 'hiringAgencyCode', 'hiringAgencyName',
                          'hiringDepartmentCode', 'hiringDepartmentName', 'agencyLevel',
                          'agencyLevelSort', 'appointmentType', 'workSchedule', 'payScale',
                          'salaryType', 'vendor', 'travelRequirement', 'teleworkEligible',
                          'serviceType', 'securityClearanceRequired', 'securityClearance',
                          'whoMayApply', 'announcementClosingTypeCode',
                          'announcementClosingTypeDescription', 'positionOpenDate',
                          'positionCloseDate', 'positionExpireDate', 'announcementNumber',
                          'hiringSubelementName', 'positionTitle', 'minimumGrade',
                          'maximumGrade', 'promotionPotential', 'minimumSalary',
                          'maximumSalary', 'supervisoryStatus', 'drugTestRequired',
                          'relocationExpensesReimbursed', 'totalOpenings',
                          'disableApplyOnline', 'positionOpeningStatus']:
                if field in job:
                    row[field] = job[field]
                elif field == 'disableApplyOnline' and 'disableAppyOnline' in job:
                    row[field] = job['disableAppyOnline']
            # Convert list/dict fields to JSON strings
            for field in ['hiringpaths', 'jobcategories', 'positionlocations',
                          'HiringPaths', 'JobCategories', 'PositionLocations']:
                if field in job and isinstance(job[field], (list, dict)):
                    row[field] = json.dumps(job[field])
                elif field in job:
                    row[field] = job[field]
            row['inserted_at'] = now
            row['last_seen'] = now
            new_rows.append(row)

        if new_rows:
            new_df = pd.DataFrame(new_rows)
            # Ensure all list/dict values are JSON strings to avoid mixed types
            for col in new_df.columns:
                if new_df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                    new_df[col] = new_df[col].apply(
                        lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
                    )
            df = pd.concat([df, new_df], ignore_index=True)
            inserted = len(new_rows)

    if changed > 0 or inserted > 0:
        # Final safety: serialize any remaining list/dict values in ALL object columns
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
        df.to_parquet(parquet_path, index=False)

    return changed, inserted, transitions


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
    parser.add_argument("--workers", type=int, default=15, help="Parallel API workers (default: 15)")
    parser.add_argument("--skip-recent-days", type=int, default=14,
                        help="Skip dates within this many days of today (default: 14, covered by daily collection)")
    args = parser.parse_args()

    # Find all historical parquet files
    files = {}
    for f in sorted(os.listdir(DATA_DIR)):
        if f.startswith('historical_jobs_') and f.endswith('.parquet') and 'backup' not in f:
            year = int(f.replace('historical_jobs_', '').replace('.parquet', ''))
            if args.years and year not in args.years:
                continue
            files[year] = os.path.join(DATA_DIR, f)

    # Load existing control numbers per year for detecting new jobs
    existing_ids: Dict[int, Set[str]] = {}
    for year, path in sorted(files.items()):
        df = pd.read_parquet(path)
        if 'usajobsControlNumber' in df.columns:
            existing_ids[year] = set(df['usajobsControlNumber'].astype(str))
        elif 'usajobs_control_number' in df.columns:
            existing_ids[year] = set(df['usajobs_control_number'].astype(str))
        else:
            existing_ids[year] = set()

    # Collect all dates that need re-polling
    cutoff_date = (datetime.now() - timedelta(days=args.skip_recent_days)).strftime('%Y-%m-%d')
    dates_by_year = {}  # year -> [dates]
    date_to_year = {}   # date -> year
    skipped = 0
    for year, path in sorted(files.items()):
        dates = get_nonfinal_dates(path)
        filtered = [d for d in dates if d <= cutoff_date]
        skipped += len(dates) - len(filtered)
        dates_by_year[year] = filtered
        for d in filtered:
            date_to_year[d] = year
        log(f"{year}: {len(filtered)} dates with non-final jobs" +
            (f" (skipped {len(dates) - len(filtered)} recent)" if len(dates) != len(filtered) else ""))

    # Sort all dates DESCENDING (most recent first)
    all_dates = sorted(date_to_year.keys(), reverse=True)
    total_dates = len(all_dates)

    log(f"\nTotal dates to query: {total_dates}" +
        (f" (skipped {skipped} within last {args.skip_recent_days} days)" if skipped else ""))
    log(f"Workers: {args.workers}")
    log(f"Will save every {SAVE_EVERY} dates\n")

    if args.dry_run:
        return

    start_time = time.time()
    year_status_maps: Dict[int, Dict[str, str]] = {}
    year_new_jobs: Dict[int, List[Dict]] = {}  # new jobs to insert per year
    dates_queried = 0
    api_jobs_seen = 0
    total_changed = 0
    total_inserted = 0
    all_transitions = []
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
                if year not in year_new_jobs:
                    year_new_jobs[year] = []

                for job in jobs:
                    cn = str(job.get('usajobsControlNumber', ''))
                    status = job.get('positionOpeningStatus')
                    if cn and status:
                        if cn in existing_ids.get(year, set()):
                            year_status_maps[year][cn] = status
                        else:
                            year_new_jobs[year].append(job)
                            existing_ids.setdefault(year, set()).add(cn)

                dates_queried += 1
                dates_since_save += 1

        # Progress update every batch
        elapsed = time.time() - start_time
        rate = dates_queried / elapsed * 60 if elapsed > 0 else 0
        remaining = (total_dates - dates_queried) / rate if rate > 0 else 0
        new_pending = sum(len(v) for v in year_new_jobs.values())
        log(f"[{dates_queried}/{total_dates}] {batch[0]} | "
            f"{api_jobs_seen:,} jobs seen | "
            f"{rate:.0f} dates/min | ~{remaining:.0f} min left | "
            f"{total_changed:,} changed, {total_inserted:,} inserted"
            + (f" (+{new_pending} pending)" if new_pending else ""))

        # Incremental save every SAVE_EVERY dates
        if dates_since_save >= SAVE_EVERY:
            for year, path in sorted(files.items()):
                sm = year_status_maps.get(year, {})
                nj = year_new_jobs.get(year, [])
                if sm or nj:
                    changed, inserted, trans = update_and_insert(path, sm, nj)
                    total_changed += changed
                    total_inserted += inserted
                    all_transitions.extend(trans)
                    parts = []
                    if changed:
                        parts.append(f"{changed:,} statuses updated")
                    if inserted:
                        parts.append(f"{inserted:,} new jobs inserted")
                    if parts:
                        log(f"  Saved {year}: {', '.join(parts)}")
            # Clear maps after saving
            year_status_maps = {}
            year_new_jobs = {}
            dates_since_save = 0

        i += batch_size

    # Final save for any remaining updates
    if year_status_maps or year_new_jobs:
        log(f"\n--- Final save ---")
        for year, path in sorted(files.items()):
            sm = year_status_maps.get(year, {})
            nj = year_new_jobs.get(year, [])
            if sm or nj:
                changed, inserted, trans = update_and_insert(path, sm, nj)
                total_changed += changed
                total_inserted += inserted
                all_transitions.extend(trans)
                parts = []
                if changed:
                    parts.append(f"{changed:,} statuses updated")
                if inserted:
                    parts.append(f"{inserted:,} new jobs inserted")
                if parts:
                    log(f"{year}: {', '.join(parts)}")

    elapsed_total = time.time() - start_time
    log(f"\nDone! {dates_queried} dates in {elapsed_total/60:.1f} min")
    log(f"API jobs seen: {api_jobs_seen:,}")
    log(f"Statuses changed: {total_changed:,}")
    log(f"New jobs inserted: {total_inserted:,}")
    log(f"Errors: {errors}")

    # Summary of status transitions
    if all_transitions:
        from collections import Counter
        transition_counts = Counter((old, new) for _, old, new in all_transitions)
        log(f"\nStatus transitions:")
        for (old, new), count in transition_counts.most_common():
            log(f"  {old} -> {new}: {count:,}")

        # Write detailed transitions to a file for review
        summary_path = os.path.join(os.path.dirname(__file__), '..', 'logs',
                                     f'status_transitions_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
        os.makedirs(os.path.dirname(summary_path), exist_ok=True)
        with open(summary_path, 'w') as f:
            f.write('control_number,old_status,new_status\n')
            for cn, old, new in all_transitions:
                f.write(f'{cn},{old},{new}\n')
        log(f"Detailed transitions written to: {summary_path}")


if __name__ == "__main__":
    main()
