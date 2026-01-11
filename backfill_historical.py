#!/usr/bin/env python3
"""
Backfill missing jobs into the historical parquet.
Fetches from API, compares to existing parquets, and adds missing jobs.
"""

import json
import pandas as pd
import requests
from datetime import datetime, timedelta
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_jobs_for_date(date_str):
    """Fetch all jobs for a specific date from historical API."""
    base_url = "https://data.usajobs.gov/api/historicjoa"
    params = {
        "StartPositionOpenDate": date_str,
        "EndPositionOpenDate": date_str
    }

    all_jobs = []
    next_url = None

    while True:
        try:
            if next_url:
                response = requests.get(next_url, timeout=60)
            else:
                response = requests.get(base_url, params=params, timeout=60)

            if response.status_code == 503:
                time.sleep(5)
                continue

            response.raise_for_status()

            if response.text.strip() == "204 No Content":
                break

            data = response.json()
            jobs = data.get("data", [])
            all_jobs.extend(jobs)

            next_path = data.get("paging", {}).get("next")
            if next_path and next_path.strip():
                if next_path.startswith('http'):
                    next_url = next_path
                else:
                    next_url = f"https://data.usajobs.gov{next_path}"
            else:
                break

        except Exception as e:
            print(f"  Error for {date_str}: {e}")
            break

    return all_jobs

def process_job(job):
    """Process a job like collect_data.py does"""
    processed_job = job.copy()

    # Convert arrays to JSON strings
    for field in ['HiringPaths', 'JobCategories', 'PositionLocations']:
        if field in processed_job and isinstance(processed_job[field], (list, dict)):
            processed_job[field] = json.dumps(processed_job[field])

    # Add consistent control number field
    processed_job['usajobs_control_number'] = str(processed_job['usajobsControlNumber'])

    # Fix typo in API response
    if 'disableAppyOnline' in processed_job:
        processed_job['disableApplyOnline'] = processed_job.pop('disableAppyOnline')

    # Add metadata
    processed_job['inserted_at'] = datetime.now().isoformat()
    processed_job['last_seen'] = datetime.now().isoformat()
    processed_job['backfilled'] = True

    return processed_job

def main():
    # Load existing parquets
    print("Loading existing parquets...")

    hist_path = 'data/historical_jobs_2025.parquet'
    curr_path = 'data/current_jobs_2025.parquet'

    # Try local first, then /tmp (from earlier download)
    if os.path.exists(hist_path):
        hist_df = pd.read_parquet(hist_path)
    elif os.path.exists('/tmp/historical_jobs_2025_remote.parquet'):
        hist_df = pd.read_parquet('/tmp/historical_jobs_2025_remote.parquet')
        hist_path = 'data/historical_jobs_2025.parquet'  # Will save here
    else:
        print("ERROR: No historical parquet found")
        return

    if os.path.exists(curr_path):
        curr_df = pd.read_parquet(curr_path)
    elif os.path.exists('/tmp/current_jobs_2025_remote.parquet'):
        curr_df = pd.read_parquet('/tmp/current_jobs_2025_remote.parquet')
    else:
        print("WARNING: No current parquet found, continuing without")
        curr_df = pd.DataFrame()

    hist_controls = set(hist_df['usajobsControlNumber'].dropna().astype(str))
    curr_controls = set(curr_df['usajobsControlNumber'].dropna().astype(str)) if len(curr_df) > 0 else set()

    print(f"Historical parquet: {len(hist_controls):,} jobs")
    print(f"Current parquet: {len(curr_controls):,} jobs")

    # Generate dates for 2025
    dates = [(datetime(2025, 1, 1) + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(351)]
    # Only up to yesterday
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    dates = [d for d in dates if d <= yesterday]

    print(f"\nFetching {len(dates)} dates from API (parallel)...")

    jobs_to_add = []
    missing_from_hist_count = 0
    missing_from_both_count = 0

    def check_and_fetch_date(date_str):
        """Fetch jobs for date and return those missing from historical"""
        jobs = fetch_jobs_for_date(date_str)
        missing_hist = []
        missing_both = 0

        for job in jobs:
            ctrl = str(job.get('usajobsControlNumber', ''))
            if ctrl and ctrl not in hist_controls:
                missing_hist.append(job)
                if ctrl not in curr_controls:
                    missing_both += 1

        return date_str, len(jobs), missing_hist, missing_both

    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(check_and_fetch_date, d): d for d in dates}

        for future in as_completed(futures):
            date_str, api_count, missing_hist, missing_both = future.result()

            if missing_hist:
                print(f"{date_str}: {api_count:>4} jobs | missing from hist: {len(missing_hist):>3} | missing from BOTH: {missing_both:>3}")
                for job in missing_hist:
                    jobs_to_add.append(process_job(job))
                missing_from_hist_count += len(missing_hist)
                missing_from_both_count += missing_both
            else:
                print(f"{date_str}: {api_count:>4} jobs | OK")

    # Summary
    print(f"\n{'='*60}")
    print(f"Missing from historical: {missing_from_hist_count}")
    print(f"Missing from BOTH: {missing_from_both_count}")
    print(f"Jobs to backfill: {len(jobs_to_add)}")

    if not jobs_to_add:
        print("\nNo jobs to backfill!")
        return

    # Add to parquet
    print(f"\nAdding {len(jobs_to_add)} jobs to historical parquet...")

    new_df = pd.DataFrame(jobs_to_add)
    combined_df = pd.concat([hist_df, new_df], ignore_index=True)

    # Verify no data loss
    if len(combined_df) < len(hist_df):
        print("ERROR: Would lose data! Aborting.")
        return

    print(f"Historical parquet: {len(hist_df):,} -> {len(combined_df):,} jobs")

    # Backup and save
    os.makedirs('data', exist_ok=True)
    if os.path.exists(hist_path):
        backup_path = hist_path.replace('.parquet', '_backup.parquet')
        os.rename(hist_path, backup_path)
        print(f"Backed up to {backup_path}")

    combined_df.to_parquet(hist_path, index=False)
    print(f"Saved to {hist_path}")

    # Save results summary
    results = {
        'run_date': datetime.now().isoformat(),
        'missing_from_hist': missing_from_hist_count,
        'missing_from_both': missing_from_both_count,
        'jobs_backfilled': len(jobs_to_add)
    }
    with open('backfill_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    print("Saved summary to backfill_results.json")

if __name__ == "__main__":
    main()
