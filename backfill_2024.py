#!/usr/bin/env python3
"""
Backfill missing jobs into the 2024 historical parquet.
Only compares against historical (no current for 2024).
Uses positionOpenDate to determine which jobs belong to 2024.
"""

import json
import pandas as pd
import requests
from datetime import datetime, timedelta
import time
import os
import subprocess
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
    year = 2024
    print(f"Backfilling historical parquet for {year}...")

    hist_path = f'data/historical_jobs_{year}.parquet'
    tmp_path = f'/tmp/historical_jobs_{year}_remote.parquet'

    # Download if needed
    if not os.path.exists(hist_path) and not os.path.exists(tmp_path):
        url = f'https://github.com/abigailhaddad/usajobs_historical/raw/main/data/historical_jobs_{year}.parquet'
        print(f"Downloading historical_jobs_{year}.parquet...")
        subprocess.run(['curl', '-L', '-o', tmp_path, url], check=True)

    # Load existing historical
    if os.path.exists(hist_path):
        hist_df = pd.read_parquet(hist_path)
    elif os.path.exists(tmp_path):
        hist_df = pd.read_parquet(tmp_path)
    else:
        print(f"ERROR: No historical parquet found for {year}")
        return

    hist_controls = set(hist_df['usajobsControlNumber'].dropna().astype(str))
    print(f"Historical parquet: {len(hist_controls):,} jobs")

    # Generate dates for 2024
    dates = [(datetime(2024, 1, 1) + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(366)]

    print(f"\nFetching {len(dates)} dates from API (parallel)...")

    jobs_to_add = []
    missing_count = 0

    def check_and_fetch_date(date_str):
        """Fetch jobs for date and return those missing from historical"""
        jobs = fetch_jobs_for_date(date_str)
        missing = []

        for job in jobs:
            ctrl = str(job.get('usajobsControlNumber', ''))
            # Check positionOpenDate is actually in 2024
            open_date = job.get('positionOpenDate', '')
            if ctrl and ctrl not in hist_controls and open_date.startswith('2024'):
                missing.append(job)

        return date_str, len(jobs), missing

    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(check_and_fetch_date, d): d for d in dates}

        for future in as_completed(futures):
            date_str, api_count, missing = future.result()

            if missing:
                print(f"{date_str}: {api_count:>4} jobs | missing: {len(missing):>3}")
                for job in missing:
                    jobs_to_add.append(process_job(job))
                missing_count += len(missing)
            else:
                print(f"{date_str}: {api_count:>4} jobs | OK")

    # Summary
    print(f"\n{'='*60}")
    print(f"Missing from historical: {missing_count}")
    print(f"Jobs to backfill: {len(jobs_to_add)}")

    if not jobs_to_add:
        print("\nNo jobs to backfill!")
        return

    # Add to parquet
    print(f"\nAdding {len(jobs_to_add)} jobs to historical parquet...")

    new_df = pd.DataFrame(jobs_to_add)
    combined_df = pd.concat([hist_df, new_df], ignore_index=True)

    if len(combined_df) < len(hist_df):
        print("ERROR: Would lose data! Aborting.")
        return

    print(f"Historical parquet: {len(hist_df):,} -> {len(combined_df):,} jobs")

    # Backup and save
    os.makedirs('data', exist_ok=True)
    if os.path.exists(hist_path):
        backup_path = hist_path.replace('.parquet', '_backup.parquet')
        if os.path.exists(backup_path):
            os.remove(backup_path)
        os.rename(hist_path, backup_path)
        print(f"Backed up to {backup_path}")

    combined_df.to_parquet(hist_path, index=False)
    print(f"Saved to {hist_path}")

    # Save results
    with open('backfill_2024_results.json', 'w') as f:
        json.dump({
            'run_date': datetime.now().isoformat(),
            'year': year,
            'missing': missing_count,
            'backfilled': len(jobs_to_add)
        }, f, indent=2)
    print("Saved summary to backfill_2024_results.json")

if __name__ == "__main__":
    main()
