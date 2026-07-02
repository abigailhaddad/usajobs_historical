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

from cap_alert import check_cap

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

    # Cap tripwire: a single date returning exactly 500 (page size) or 10,000
    # may mean pagination silently stopped instead of reaching the true end.
    check_cap(len(all_jobs), f"repoll date {date_str}")

    return all_jobs


def get_nonfinal_dates(parquet_path: str) -> List[str]:
    """Get sorted list of distinct positionOpenDate values for non-final jobs."""
    df = pd.read_parquet(parquet_path)
    mask = ~df['positionOpeningStatus'].isin(FINAL_STATUSES) | df['positionOpeningStatus'].isna()
    nonfinal = df[mask]
    dates = nonfinal['positionOpenDate'].dropna().apply(lambda x: str(x)[:10]).unique()
    return sorted(dates)


def get_gap_dates(parquet_path: str, year: int, cutoff_date: str, lookback_months: int = 13) -> List[str]:
    """Find calendar dates within `year` that have NO records at all.

    Only looks within the last `lookback_months` window so we don't spam the API
    for ancient years that are genuinely sparse.  Dates with zero records are
    pipeline gaps (API outage, collection failure) that the repoll should fill.
    """
    lookback_start = (datetime.now() - timedelta(days=lookback_months * 30)).strftime('%Y-%m-%d')
    range_start = max(f"{year}-01-01", lookback_start)
    range_end   = min(f"{year}-12-31", cutoff_date)

    if range_end < range_start:
        return []

    df = pd.read_parquet(parquet_path)
    existing = set(df['positionOpenDate'].dropna().apply(lambda x: str(x)[:10]).unique())

    all_cal_dates = pd.date_range(range_start, range_end, freq='D')
    return [d.strftime('%Y-%m-%d') for d in all_cal_dates if d.strftime('%Y-%m-%d') not in existing]


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
                ann = df.at[idx, 'announcementNumber'] if 'announcementNumber' in df.columns else ''
                title = df.at[idx, 'positionTitle'] if 'positionTitle' in df.columns else ''
                agency = df.at[idx, 'hiringAgencyName'] if 'hiringAgencyName' in df.columns else ''
                odate = df.at[idx, 'positionOpenDate'] if 'positionOpenDate' in df.columns else ''
                transitions.append((key, str(old), str(new), str(ann), str(title),
                                    str(agency), str(odate)))
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
        import numpy as np
        # Nuclear fix: force known list-type columns to plain strings.
        # pyarrow reads parquet list columns into ArrowDtype. After
        # .astype(object), these become numpy ndarrays (not Python lists)
        # and pyarrow NA (not None). Must handle both.
        LIST_PRONE_COLS = ['hiringpaths', 'HiringPaths', 'jobcategories', 'JobCategories',
                           'positionlocations', 'PositionLocations']

        def _to_json_str(x):
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return None
            try:
                if pd.isna(x):
                    return None
            except (ValueError, TypeError):
                pass
            if isinstance(x, np.ndarray):
                return json.dumps(x.tolist())
            if isinstance(x, (list, dict)):
                return json.dumps(x)
            return x

        for col in LIST_PRONE_COLS:
            if col in df.columns:
                df[col] = df[col].astype(object).apply(_to_json_str)
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
    parser.add_argument("--max-minutes", type=float, default=None,
                        help="Stop cleanly after this many minutes, saving partial progress and "
                             "writing logs/REPOLL_INCOMPLETE.txt (default: no limit)")
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

        # Also include dates within the last 13 months that have NO records at all
        # so pipeline gaps (API outages, collection failures) get back-filled here.
        gaps = get_gap_dates(path, year, cutoff_date)
        combined = sorted(set(filtered) | set(gaps))

        dates_by_year[year] = combined
        for d in combined:
            date_to_year[d] = year
        gap_msg = f", {len(gaps)} gap dates to fill" if gaps else ""
        log(f"{year}: {len(filtered)} dates with non-final jobs{gap_msg}" +
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
    all_new_jobs_details = []  # list of (year, date, control_number, title, agency, status)
    inserts_by_year = {}   # year -> count
    changes_by_year = {}   # year -> count

    # Snapshot file sizes before repoll
    file_sizes_before = {}
    for year, path in files.items():
        file_sizes_before[year] = os.path.getsize(path)

    # Snapshot row counts before repoll
    row_counts_before = {year: len(ids) for year, ids in existing_ids.items()}
    dates_since_save = 0
    errors = 0

    # Process in batches for parallel fetching
    batch_size = args.workers
    i = 0
    stopped_early = False

    while i < total_dates:
        # Honor a wall-clock budget so the workflow always leaves time to
        # save, prep and sync. Partial progress is preserved (incremental
        # saves already happened) and the final save below flushes the rest.
        if args.max_minutes and (time.time() - start_time) > args.max_minutes * 60:
            stopped_early = True
            log(f"\n⏱️  Time budget of {args.max_minutes:.0f} min reached — "
                f"stopping at {dates_queried}/{total_dates} dates. "
                f"Remaining dates will be picked up on the next run.")
            break

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
                            title = job.get('positionTitle', 'Unknown')
                            agency = job.get('hiringAgencyName', 'Unknown')
                            open_date = job.get('positionOpenDate', 'Unknown')
                            log(f"  🆕 New job: {cn} — {title} ({agency}) opened {open_date} [{status}]")
                            all_new_jobs_details.append((year, open_date, cn, title, agency, status))

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
                    inserts_by_year[year] = inserts_by_year.get(year, 0) + inserted
                    changes_by_year[year] = changes_by_year.get(year, 0) + changed
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
                inserts_by_year[year] = inserts_by_year.get(year, 0) + inserted
                changes_by_year[year] = changes_by_year.get(year, 0) + changed
                parts = []
                if changed:
                    parts.append(f"{changed:,} statuses updated")
                if inserted:
                    parts.append(f"{inserted:,} new jobs inserted")
                if parts:
                    log(f"{year}: {', '.join(parts)}")

    elapsed_total = time.time() - start_time
    from collections import Counter

    log(f"\n{'='*60}")
    log(f"REPOLL SUMMARY")
    log(f"{'='*60}")
    log(f"Runtime: {elapsed_total/60:.1f} min | {dates_queried} dates | {api_jobs_seen:,} API jobs seen | {errors} errors")
    log(f"Totals: {total_changed:,} statuses changed, {total_inserted:,} new jobs inserted")

    # Per-year breakdown
    log(f"\nPer-year breakdown:")
    log(f"  {'Year':<6} {'Dates':>6} {'Before':>10} {'Inserted':>10} {'Changed':>10} {'After':>10} {'Size MB':>10}")
    log(f"  {'-'*6} {'-'*6} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")
    for year in sorted(files.keys()):
        n_dates = len(dates_by_year.get(year, []))
        before = row_counts_before.get(year, 0)
        ins = inserts_by_year.get(year, 0)
        chg = changes_by_year.get(year, 0)
        after = before + ins
        size_mb = os.path.getsize(files[year]) / 1_048_576
        log(f"  {year:<6} {n_dates:>6} {before:>10,} {ins:>10,} {chg:>10,} {after:>10,} {size_mb:>10.1f}")

    # Flag anything unusual
    warnings = []
    if total_inserted > 1000:
        big_years = [f"{y}: {c:,}" for y, c in sorted(inserts_by_year.items()) if c > 100]
        warnings.append(f"Large number of new insertions ({total_inserted:,}): {', '.join(big_years)}")
    if errors > 5:
        warnings.append(f"High error count: {errors}")
    for year, count in inserts_by_year.items():
        before = row_counts_before.get(year, 0)
        if before > 0 and count / before > 0.05:
            warnings.append(f"{year}: inserted {count:,} jobs ({100*count/before:.1f}% of existing {before:,}) — is R2 data stale?")

    if warnings:
        log(f"\n⚠️  WARNINGS:")
        for w in warnings:
            log(f"  - {w}")

    # New jobs discovered
    if all_new_jobs_details:
        log(f"\nNew jobs discovered ({len(all_new_jobs_details)}):")
        for year, open_date, cn, title, agency, status in sorted(all_new_jobs_details):
            log(f"  {year} | {open_date} | {cn} | {title} | {agency} | {status}")

    # Status transitions
    if all_transitions:
        transition_counts = Counter((t[1], t[2]) for t in all_transitions)
        log(f"\nStatus transitions:")
        for (old, new), count in transition_counts.most_common():
            log(f"  {old} -> {new}: {count:,}")

        # Highlight "reopens": a supposedly-final status going back to non-final.
        # These are the anomalies worth eyeballing, so print each with enough to
        # locate the job (control + announcement number + best-effort URL).
        reopens = [t for t in all_transitions
                   if t[1] in FINAL_STATUSES and t[2] not in FINAL_STATUSES]
        if reopens:
            log(f"\n⚠️  {len(reopens)} reopened job(s) (final -> non-final):")
            for cn, old, new, ann, title, agency, odate in reopens:
                log(f"  {cn} | ann {ann} | {old} -> {new} | {title} | {agency} | "
                    f"opened {odate} | https://www.usajobs.gov/job/{cn}")

        # Write detailed transitions to CSV (enough to locate each job)
        def _q(s):
            s = str(s)
            return '"' + s.replace('"', '""') + '"' if (',' in s or '"' in s) else s

        summary_path = os.path.join(os.path.dirname(__file__), '..', 'logs',
                                     f'status_transitions_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
        os.makedirs(os.path.dirname(summary_path), exist_ok=True)
        with open(summary_path, 'w') as f:
            f.write('control_number,old_status,new_status,announcement_number,'
                    'position_title,hiring_agency,position_open_date\n')
            for cn, old, new, ann, title, agency, odate in all_transitions:
                f.write(f'{_q(cn)},{_q(old)},{_q(new)},{_q(ann)},{_q(title)},'
                        f'{_q(agency)},{_q(odate)}\n')
        log(f"  Detailed transitions: {summary_path}")

    # Loud, non-fatal signal that this run did not cover all assigned dates.
    # The workflow turns this marker into a GitHub issue, and the remaining
    # dates get retried on the next scheduled run.
    if stopped_early or dates_queried < total_dates:
        marker_path = os.path.join(os.path.dirname(__file__), '..', 'logs',
                                   'REPOLL_INCOMPLETE.txt')
        os.makedirs(os.path.dirname(marker_path), exist_ok=True)
        reason = (f"Hit --max-minutes budget of {args.max_minutes:.0f} min"
                  if stopped_early else "Loop ended before all dates were queried")
        with open(marker_path, 'w') as f:
            f.write(f"Repoll did not finish all assigned dates.\n"
                    f"Reason: {reason}\n"
                    f"Progress: {dates_queried}/{total_dates} dates queried "
                    f"({total_dates - dates_queried} remaining)\n"
                    f"Years this run: {sorted(files.keys())}\n"
                    f"Partial progress WAS saved and synced. "
                    f"Remaining dates will be retried on the next scheduled run.\n")
        log(f"\n⚠️  Wrote {marker_path} (workflow will open an issue).")

    log(f"{'='*60}")


if __name__ == "__main__":
    main()
