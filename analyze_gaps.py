#!/usr/bin/env python3
"""
Analyze gaps in historical data collection.
Shows breakdown by date of:
1. Jobs missing from historical parquet (but may be in current)
2. Jobs missing from BOTH parquets (truly missing)
"""

import pandas as pd
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def main():
    # Load parquets
    print("Loading parquets...")
    hist_df = pd.read_parquet('/tmp/historical_jobs_2025_remote.parquet')
    curr_df = pd.read_parquet('/tmp/current_jobs_2025_remote.parquet')

    hist_controls = set(hist_df['usajobsControlNumber'].dropna().astype(str))
    curr_controls = set(curr_df['usajobsControlNumber'].dropna().astype(str))

    print(f"Historical parquet: {len(hist_controls):,} jobs")
    print(f"Current parquet: {len(curr_controls):,} jobs")

    def check_date(date_str):
        try:
            r = requests.get('https://data.usajobs.gov/api/historicjoa',
                params={'StartPositionOpenDate': date_str, 'EndPositionOpenDate': date_str},
                timeout=60)
            if r.text.strip() == '204 No Content':
                return {'date': date_str, 'api_count': 0, 'missing_hist': 0, 'missing_both': 0}

            data = r.json()
            jobs = data.get('data', [])

            missing_hist = 0
            missing_both = 0

            for job in jobs:
                ctrl = str(job.get('usajobsControlNumber', ''))
                if ctrl:
                    in_hist = ctrl in hist_controls
                    in_curr = ctrl in curr_controls
                    if not in_hist:
                        missing_hist += 1
                        if not in_curr:
                            missing_both += 1

            return {'date': date_str, 'api_count': len(jobs), 'missing_hist': missing_hist, 'missing_both': missing_both}
        except Exception as e:
            return {'date': date_str, 'api_count': -1, 'missing_hist': 0, 'missing_both': 0, 'error': str(e)}

    # Generate dates for 2025
    dates = [(datetime(2025, 1, 1) + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(351)]

    print(f"\nFetching {len(dates)} days from API (parallel)...")

    results = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(check_date, d): d for d in dates}
        for future in tqdm(as_completed(futures), total=len(dates)):
            results.append(future.result())

    # Sort by date
    results.sort(key=lambda x: x['date'])

    # Print results
    print("\n" + "=" * 70)
    print("DATES WITH GAPS")
    print("=" * 70)
    print(f"{'Date':<12} {'API Count':>10} {'Missing Hist':>14} {'Missing Both':>14}")
    print("-" * 70)

    for r in results:
        if r['missing_hist'] > 0 or r['missing_both'] > 0:
            print(f"{r['date']:<12} {r['api_count']:>10} {r['missing_hist']:>14} {r['missing_both']:>14}")

    # Summary
    total_missing_hist = sum(r['missing_hist'] for r in results)
    total_missing_both = sum(r['missing_both'] for r in results)
    dates_with_gaps = sum(1 for r in results if r['missing_hist'] > 0)

    print("-" * 70)
    print(f"{'TOTAL':<12} {'':<10} {total_missing_hist:>14} {total_missing_both:>14}")
    print(f"\nDates with gaps: {dates_with_gaps}")
    print(f"Missing from historical: {total_missing_hist}")
    print(f"Missing from BOTH (truly lost): {total_missing_both}")

if __name__ == "__main__":
    main()
