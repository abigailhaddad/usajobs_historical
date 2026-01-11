#!/usr/bin/env python3
"""
Verify historical data coverage by re-fetching and comparing.
Finds jobs that exist in the API but are missing from BOTH:
  - existing historical parquet
  - existing current parquet
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import json

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

def main():
    # Load existing data
    print("Loading existing parquets...")
    hist_df = pd.read_parquet('/tmp/historical_jobs_2025_remote.parquet')
    curr_df = pd.read_parquet('/tmp/current_jobs_2025_remote.parquet')

    # Get all existing control numbers
    hist_controls = set(hist_df['usajobsControlNumber'].dropna().astype(str))

    # Current API might have different column name
    if 'usajobsControlNumber' in curr_df.columns:
        curr_controls = set(curr_df['usajobsControlNumber'].dropna().astype(str))
    elif 'usajobs_control_number' in curr_df.columns:
        curr_controls = set(curr_df['usajobs_control_number'].dropna().astype(str))
    else:
        print(f"Current columns: {curr_df.columns.tolist()}")
        curr_controls = set()

    print(f"Existing historical jobs: {len(hist_controls):,}")
    print(f"Existing current jobs: {len(curr_controls):,}")

    # Fetch fresh from API - sequential so we can print per-day stats
    print("\nFetching fresh from historical API...")
    start = datetime(2025, 1, 1)
    end = datetime(2025, 12, 17)

    # Generate all dates
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    fresh_controls = set()
    missing_from_hist = []
    missing_from_both = []
    daily_stats = []

    for date_str in dates:
        jobs = fetch_jobs_for_date(date_str)

        day_missing_hist = 0
        day_missing_both = 0

        for job in jobs:
            ctrl = str(job.get('usajobsControlNumber', ''))
            if ctrl:
                fresh_controls.add(ctrl)
                in_hist = ctrl in hist_controls
                in_curr = ctrl in curr_controls

                if not in_hist:
                    day_missing_hist += 1
                    missing_from_hist.append({
                        'control_number': ctrl,
                        'date': date_str,
                        'title': job.get('positionTitle', ''),
                        'agency': job.get('hiringAgencyName', ''),
                        'in_current': in_curr,
                        'full_job': job  # Save full job data for backfill
                    })
                    if not in_curr:
                        day_missing_both += 1
                        missing_from_both.append({
                            'control_number': ctrl,
                            'date': date_str,
                            'title': job.get('positionTitle', ''),
                            'agency': job.get('hiringAgencyName', ''),
                            'full_job': job  # Save full job data for backfill
                        })

        # Print daily stats
        if day_missing_hist > 0:
            print(f"{date_str}: {len(jobs):>4} jobs | missing from hist: {day_missing_hist:>3} | missing from BOTH: {day_missing_both:>3}")
        else:
            print(f"{date_str}: {len(jobs):>4} jobs | OK")

        daily_stats.append({
            'date': date_str,
            'api_count': len(jobs),
            'missing_hist': day_missing_hist,
            'missing_both': day_missing_both
        })

        time.sleep(0.2)  # Be nice to API

    # Summary
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    print(f"Fresh fetch found: {len(fresh_controls):,} unique jobs")
    print(f"Missing from historical parquet: {len(missing_from_hist)}")
    print(f"Missing from BOTH parquets: {len(missing_from_both)}")

    # Save results to JSON
    results = {
        'run_date': datetime.now().isoformat(),
        'fresh_count': len(fresh_controls),
        'missing_from_hist_count': len(missing_from_hist),
        'missing_from_both_count': len(missing_from_both),
        'daily_stats': daily_stats,
        'missing_from_hist': missing_from_hist,
        'missing_from_both': missing_from_both
    }

    with open('coverage_results.json', 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\nResults saved to coverage_results.json")

    if missing_from_both:
        print(f"\nJOBS MISSING FROM BOTH:")
        for job in missing_from_both[:20]:
            print(f"  {job['date']} | {job['control_number']} | {job['title'][:50]} | {job['agency']}")
        if len(missing_from_both) > 20:
            print(f"  ... and {len(missing_from_both) - 20} more")
    else:
        print("\n✅ No jobs missing from both! Current API caught everything.")

if __name__ == "__main__":
    main()
