#!/usr/bin/env python3
"""
USAJobs Historic Data Puller

Pulls all jobs from the USAJobs Historical API
and saves the structured data to Parquet files.

Usage:
    python collect_data.py --start-date 2023-01-01 --end-date 2023-01-15 --data-dir data
"""

import argparse
import json
import time
import requests
from typing import List, Dict, Optional
import pandas as pd
from datetime import datetime, timedelta
import os
from tqdm import tqdm
import logging
import sys

# Base URL for API requests

def setup_aggressive_logging(data_dir: str, start_date: str, end_date: str):
    """Set up aggressive logging system that violently flags data issues."""
    # Create logs directory
    log_dir = os.path.join(data_dir, "..", "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    # Create timestamp for log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"historical_{start_date}_to_{end_date}_{timestamp}.log")
    
    # Configure aggressive logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Create custom logger for data issues
    data_logger = logging.getLogger('DATA_ISSUES')
    data_handler = logging.FileHandler(os.path.join(log_dir, f"DATA_GAPS_{timestamp}.log"))
    data_handler.setFormatter(logging.Formatter('%(asctime)s - 🚨 CRITICAL DATA ISSUE 🚨 - %(message)s'))
    data_logger.addHandler(data_handler)
    data_logger.setLevel(logging.CRITICAL)
    
    return logging.getLogger(__name__), data_logger

def log_violent_data_gap_warning(data_logger, failed_dates: List[str], start_date: str, end_date: str):
    """Violently flag missing data with aggressive warnings."""
    if not failed_dates:
        return
    
    warning_msg = f"""
    ═══════════════════════════════════════════════════════════════════════════════
    🚨🚨🚨 CRITICAL DATA COLLECTION FAILURE 🚨🚨🚨
    ═══════════════════════════════════════════════════════════════════════════════
    
    DATA RANGE: {start_date} to {end_date}
    FAILED DATES: {len(failed_dates)} out of {(datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days + 1} total days
    FAILURE RATE: {len(failed_dates) / ((datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days + 1) * 100:.1f}%
    
    MISSING DATA FOR THESE DATES:
    {chr(10).join([f"    💀 {date} - NO DATA COLLECTED" for date in failed_dates])}
    
    ⚠️  WARNING: These gaps represent MISSING FEDERAL JOB DATA that may never be recoverable!
    ⚠️  WARNING: Analysis based on this dataset will be INCOMPLETE and INACCURATE!
    ⚠️  WARNING: Immediate action required - retry failed dates ASAP!
    
    RETRY COMMAND:
    python scripts/collect_data.py --start-date {start_date} --end-date {end_date} --data-dir data
    
    INDIVIDUAL RETRY COMMANDS:
    {chr(10).join([f"    python scripts/collect_data.py --start-date {date} --end-date {date} --data-dir data" for date in failed_dates[:10]])}
    {"    ... (showing first 10 failed dates only)" if len(failed_dates) > 10 else ""}
    
    ═══════════════════════════════════════════════════════════════════════════════
    """
    
    data_logger.critical(warning_msg)
    
    # Also print to console with extra emphasis
    print("\n" + "🚨" * 80)
    print("🚨" + " CRITICAL DATA COLLECTION FAILURE ".center(78) + "🚨")
    print("🚨" * 80)
    print(f"🚨 {len(failed_dates)} DATES FAILED TO COLLECT DATA!")
    print(f"🚨 FAILURE RATE: {len(failed_dates) / ((datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days + 1) * 100:.1f}%")
    print("🚨" * 80)
    print("🚨 CHECK LOGS FOR FULL DETAILS AND RETRY COMMANDS!")
    print("🚨" * 80 + "\n")

def parse_args():
    parser = argparse.ArgumentParser(description="Fetch USAJobs historical jobs")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--position-series", help="Optional: filter by position series (e.g., 2210)")
    parser.add_argument("--data-dir", help="Directory for parquet files (defaults to data/)", default="data")
    return parser.parse_args()


def get_job_data_page(params: Optional[Dict] = None, next_url: Optional[str] = None, retries: int = 7) -> Dict:
    """Fetch a page of job data from the API with retry logic."""
    base_url = "https://data.usajobs.gov"
    
    for attempt in range(retries):
        try:
            if next_url:
                # Use full URL for continuation requests
                response = requests.get(next_url)
            else:
                # Use base URL + endpoint for initial requests
                response = requests.get(f"{base_url}/api/historicjoa", params=params)

            print(f"Requesting: {response.url} -> {response.status_code}")
            
            if response.status_code == 503:
                if attempt < retries - 1:
                    wait_time = (attempt + 1) * 5  # 5, 10, 15 seconds
                    print(f"  503 Service Unavailable, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"  Failed after {retries} attempts")
            
            response.raise_for_status()
            
            # Handle "204 No Content" text response
            if response.text.strip() == "204 No Content":
                return {"results": []}
                
            return response.json()
            
        except json.JSONDecodeError as e:
            # JSONDecodeError means the API returned HTML/invalid JSON - don't retry
            print(f"  ❌ FAILED immediately: JSONDecodeError (API returned invalid JSON): {e}")
            raise e
        except requests.RequestException as e:
            if attempt < retries - 1:
                wait_time = (attempt + 1) * 5
                print(f"  Request failed ({type(e).__name__}: {e}), retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"  ❌ FAILED after {retries} attempts: {type(e).__name__}: {e}")
                raise e


def fetch_all_pages(params: Dict, description: str = "") -> List[Dict]:
    """Fetch all pages of data with pagination. Returns list of all jobs."""
    all_jobs = []
    next_url = None
    page_num = 1
    
    while True:
        try:
            data = get_job_data_page(params=params, next_url=next_url)
            page_jobs = data.get("data", [])
            all_jobs.extend(page_jobs)
            
            print(f"  📄 Page {page_num}: {len(page_jobs)} jobs (total so far: {len(all_jobs)})")
            
            next_path = data.get("paging", {}).get("next")
            if next_path and next_path.strip():
                # Handle both full URLs and relative paths
                if next_path.startswith('http'):
                    next_url = next_path
                else:
                    base_url = "https://data.usajobs.gov"
                    next_url = f"{base_url}{next_path}"
                params = None  # Only needed for the first request
                page_num += 1
            else:
                break
                
        except Exception as e:
            print(f"  ⚠️  Page {page_num} failed with error: {e}")
            print(f"  💾 Saving {len(all_jobs)} jobs collected so far and continuing...")
            break
    
    return all_jobs


def fetch_jobs_for_date(date: str, position_series: Optional[str] = None) -> tuple[List[Dict], bool]:
    """Fetch all jobs for a specific date. Returns (jobs_list, success_flag)."""
    params = {
        "StartPositionOpenDate": date,
        "EndPositionOpenDate": date
    }
    
    # Add position series filter if specified
    if position_series:
        params["PositionSeries"] = position_series

    try:
        # Try single day query first
        jobs_for_date = fetch_all_pages(params)
        # If we got some jobs, return success
        if jobs_for_date:
            return jobs_for_date, True
        else:
            # If we got 0 jobs, it might be legitimate or it might be a failure
            # Let's try the fallback to be sure
            print(f"  🤔 Got 0 jobs for {date}, trying fallback to confirm...")
            raise Exception("Got 0 jobs, trying fallback")
        
    except Exception as e:
        print(f"  ❌ API FAILURE for single day {date}: {e}")
        
        # Try two 2-day range fallbacks
        current_date = datetime.strptime(date, '%Y-%m-%d')
        previous_date = current_date - timedelta(days=1)
        next_date = current_date + timedelta(days=1)
        
        prev_date_str = previous_date.strftime('%Y-%m-%d')
        next_date_str = next_date.strftime('%Y-%m-%d')
        
        all_target_jobs = []
        
        # Fallback 1: previous day + target day
        try:
            print(f"  🔄 Fallback 1: querying {prev_date_str} to {date}")
            
            fallback1_params = {
                "StartPositionOpenDate": prev_date_str,
                "EndPositionOpenDate": date
            }
            if position_series:
                fallback1_params["PositionSeries"] = position_series
            
            range1_jobs = fetch_all_pages(fallback1_params)
            
            # Filter to only jobs that actually have positionOpenDate = our target date
            target_jobs_1 = [job for job in range1_jobs 
                            if job.get('positionOpenDate', '').startswith(date)]
            
            all_target_jobs.extend(target_jobs_1)
            print(f"  ✅ Fallback 1: found {len(target_jobs_1)} jobs for {date} (from {len(range1_jobs)} total)")
            
        except Exception as fallback1_error:
            print(f"  ❌ Fallback 1 failed: {fallback1_error}")
        
        # Fallback 2: target day + next day
        try:
            print(f"  🔄 Fallback 2: querying {date} to {next_date_str}")
            
            fallback2_params = {
                "StartPositionOpenDate": date,
                "EndPositionOpenDate": next_date_str
            }
            if position_series:
                fallback2_params["PositionSeries"] = position_series
            
            range2_jobs = fetch_all_pages(fallback2_params)
            
            # Filter to only jobs that actually have positionOpenDate = our target date
            target_jobs_2 = [job for job in range2_jobs 
                            if job.get('positionOpenDate', '').startswith(date)]
            
            # Deduplicate with jobs from fallback 1
            existing_control_nums = {job.get('usajobsControlNumber') for job in all_target_jobs}
            new_jobs_2 = [job for job in target_jobs_2 
                         if job.get('usajobsControlNumber') not in existing_control_nums]
            
            all_target_jobs.extend(new_jobs_2)
            print(f"  ✅ Fallback 2: found {len(new_jobs_2)} additional jobs for {date} (from {len(range2_jobs)} total)")
            
        except Exception as fallback2_error:
            print(f"  ❌ Fallback 2 failed: {fallback2_error}")
        
        if all_target_jobs:
            print(f"  ✅ Combined fallbacks: found {len(all_target_jobs)} jobs for {date}")
            return all_target_jobs, True
        else:
            print(f"  ❌ All fallbacks failed for {date}")
            return [], False


def load_existing_jobs(parquet_path: str) -> set:
    """Load existing job control numbers from parquet file."""
    try:
        df = pd.read_parquet(parquet_path)
        # Use usajobs_control_number if available (consistent with current script), otherwise fall back
        if 'usajobs_control_number' in df.columns:
            return set(df['usajobs_control_number'].dropna().astype(str))
        elif 'usajobsControlNumber' in df.columns:
            return set(df['usajobsControlNumber'].dropna().astype(str))
        else:
            return set()
    except (FileNotFoundError, KeyError):
        return set()


def save_jobs_to_parquet(jobs: List[Dict], parquet_path: str):
    """Save jobs to parquet file, merging with existing data.
    
    CRITICAL: This function NEVER removes jobs, only adds or updates them.
    """
    if not jobs:
        return
    
    # Convert nested fields to JSON strings and add field rationalization
    processed_jobs = []
    for job in jobs:
        processed_job = job.copy()
        
        # Convert arrays to JSON strings
        for field in ['HiringPaths', 'JobCategories', 'PositionLocations']:
            if field in processed_job and isinstance(processed_job[field], (list, dict)):
                processed_job[field] = json.dumps(processed_job[field])
        
        # Add field rationalization to match current data script format
        # Ensure usajobs_control_number is string for consistency (like current script does)
        if 'usajobsControlNumber' in processed_job:
            processed_job['usajobs_control_number'] = str(processed_job['usajobsControlNumber'])
        
        # Fix the typo that exists in API response: disableAppyOnline -> disableApplyOnline
        if 'disableAppyOnline' in processed_job:
            processed_job['disableApplyOnline'] = processed_job.pop('disableAppyOnline')
        
        # Add metadata
        processed_job['inserted_at'] = datetime.now().isoformat()
        processed_job['last_seen'] = datetime.now().isoformat()
        processed_jobs.append(processed_job)
    
    # Convert to DataFrame
    new_df = pd.DataFrame(processed_jobs)
    
    # Load existing data if file exists
    if os.path.exists(parquet_path):
        existing_df = pd.read_parquet(parquet_path)
        initial_count = len(existing_df)
        
        # Create a unified control number column for deduplication
        # Handle both old and new column names
        if 'usajobsControlNumber' in existing_df.columns and 'usajobs_control_number' not in existing_df.columns:
            existing_df['usajobs_control_number'] = existing_df['usajobsControlNumber'].astype(str)
        
        # Get control numbers from both dataframes
        existing_control_numbers = set()
        if 'usajobs_control_number' in existing_df.columns:
            existing_control_numbers.update(existing_df['usajobs_control_number'].dropna().astype(str))
        if 'usajobsControlNumber' in existing_df.columns:
            existing_control_numbers.update(existing_df['usajobsControlNumber'].dropna().astype(str))
            
        new_control_numbers = set()
        if 'usajobs_control_number' in new_df.columns:
            new_control_numbers.update(new_df['usajobs_control_number'].dropna().astype(str))
        if 'usajobsControlNumber' in new_df.columns:
            new_control_numbers.update(new_df['usajobsControlNumber'].dropna().astype(str))
        
        # Identify which jobs to update (exist in both)
        overlapping_control_numbers = existing_control_numbers.intersection(new_control_numbers)
        
        # Update last_seen for existing jobs that we're updating
        if overlapping_control_numbers and 'last_seen' not in existing_df.columns:
            existing_df['last_seen'] = existing_df.get('inserted_at', datetime.now().isoformat())
        
        # Remove duplicates from existing data (keep existing version for now)
        # We'll add the new versions below
        if overlapping_control_numbers:
            # Create mask for jobs to keep (not in overlap)
            mask = ~existing_df.apply(lambda row: 
                str(row.get('usajobs_control_number', row.get('usajobsControlNumber', ''))) in overlapping_control_numbers, 
                axis=1)
            existing_df = existing_df[mask]
        
        # Combine dataframes - this adds new jobs and updated versions of existing jobs
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        # Verify we haven't lost any jobs
        final_count = len(combined_df)
        if final_count < initial_count:
            raise ValueError(f"DATA LOSS PREVENTED: Would have lost {initial_count - final_count} jobs. "
                           f"Initial: {initial_count}, Final: {final_count}")
        
        print(f"    📊 {parquet_path}: {initial_count} → {final_count} jobs "
              f"(+{final_count - initial_count} new/updated)")
    else:
        combined_df = new_df
        print(f"    📊 Created {parquet_path} with {len(combined_df)} jobs")
    
    # Save to parquet
    combined_df.to_parquet(parquet_path, index=False)


def group_jobs_by_year(jobs: List[Dict]) -> Dict[int, List[Dict]]:
    """Group jobs by year based on position open date."""
    jobs_by_year = {}
    
    for job in jobs:
        # Extract year from position open date
        open_date = job.get('positionOpenDate', '')
        if open_date:
            try:
                year = datetime.fromisoformat(open_date.replace('Z', '+00:00')).year
            except:
                # Fallback parsing
                try:
                    year = int(open_date[:4])
                except:
                    year = 2024  # Default year if parsing fails
        else:
            year = 2024  # Default year if no date
        
        if year not in jobs_by_year:
            jobs_by_year[year] = []
        jobs_by_year[year].append(job)
    
    return jobs_by_year


def fetch_jobs(start_date: str, end_date: str, position_series: Optional[str] = None, 
               data_dir: str = "data") -> List[Dict]:
    """Fetch job data from the API for a date range, iterating day by day with weekly saves."""
    
    # Set up aggressive logging
    logger, data_logger = setup_aggressive_logging(data_dir, start_date, end_date)
    logger.info(f"🚀 Starting data collection for {start_date} to {end_date}")
    
    all_jobs = []
    seen_control_numbers = set()  # Track unique jobs
    weekly_batch = []  # Accumulate jobs for weekly saves
    failed_dates = []  # Track dates that failed to fetch
    suspicious_zero_days = []  # Track days with 0 jobs (may be legitimate but worth flagging)
    
    # Create data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)
    
    # Load existing jobs from all year files to avoid duplicates
    for year in range(2015, 2030):  # Check reasonable year range
        parquet_path = f"{data_dir}/historical_jobs_{year}.parquet"
        if os.path.exists(parquet_path):
            existing = load_existing_jobs(parquet_path)
            seen_control_numbers.update(existing)
    
    print(f"Found {len(seen_control_numbers)} existing historical jobs across all years")
    
    # Convert string dates to datetime objects
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Calculate total days for progress bar
    total_days = (end - start).days + 1
    
    # Create progress bar
    progress_bar = tqdm(total=total_days, desc="Fetching jobs", unit="day", smoothing=0.1)
    
    # Iterate through each date
    current_date = start
    days_processed = 0
    
    while current_date <= end:
        date_str = current_date.strftime("%Y-%m-%d")
        progress_bar.set_description(f"Fetching {date_str}")
        
        try:
            jobs, success = fetch_jobs_for_date(date_str, position_series)
            if not success:
                failed_dates.append(date_str)
                logger.critical(f"💀 CRITICAL FAILURE: No data collected for {date_str}")
                data_logger.critical(f"FAILED TO COLLECT DATA: {date_str} - API errors or connectivity issues")
                progress_bar.write(f"  {date_str}: ❌ API FAILED - retrying later may help")
                current_date += timedelta(days=1)
                progress_bar.update(1)
                continue
        except Exception as e:
            failed_dates.append(date_str)
            logger.critical(f"💀 CRITICAL FAILURE: Unexpected error for {date_str}: {e}")
            data_logger.critical(f"FAILED TO COLLECT DATA: {date_str} - Exception: {e}")
            progress_bar.write(f"❌ Unexpected error for {date_str}: {e}")
            current_date += timedelta(days=1)
            progress_bar.update(1)
            continue
        
        # Process jobs for this date
        daily_jobs = []
        new_jobs = 0
        updated_jobs = 0
        for job in jobs:
            usajobs_control_number = job.get("usajobsControlNumber")
            if usajobs_control_number:
                if str(usajobs_control_number) not in seen_control_numbers:
                    seen_control_numbers.add(str(usajobs_control_number))
                    new_jobs += 1
                else:
                    updated_jobs += 1
                # Always add the job - save_jobs_to_parquet will handle replacing old data
                daily_jobs.append(job)
                all_jobs.append(job)
        
        # Add daily jobs to weekly batch
        weekly_batch.extend(daily_jobs)
        
        # Distinguish between 0 jobs (legitimate) and failures
        if len(jobs) == 0:
            suspicious_zero_days.append(date_str)
            logger.warning(f"⚠️  SUSPICIOUS: Found 0 jobs for {date_str} - may be legitimate weekend/holiday or API issue")
            progress_bar.write(f"  {date_str}: Found 0 jobs (⚠️  suspicious - check if legitimate)")
        else:
            logger.info(f"✅ SUCCESS: {date_str} - {len(jobs)} jobs ({new_jobs} new, {updated_jobs} updated)")
            progress_bar.write(f"  {date_str}: Found {len(jobs)} jobs ({new_jobs} new, {updated_jobs} updated)")
        days_processed += 1
        
        # Save to Parquet weekly (every 7 days) or at the end
        is_last_day = current_date == end
        is_week_boundary = days_processed % 7 == 0
        
        if (is_week_boundary or is_last_day) and weekly_batch:
            # Group jobs by year and save to appropriate parquet files
            jobs_by_year = group_jobs_by_year(weekly_batch)
            
            for year, year_jobs in jobs_by_year.items():
                parquet_path = f"{data_dir}/historical_jobs_{year}.parquet"
                save_jobs_to_parquet(year_jobs, parquet_path)
            
            progress_bar.write(f"  💾 Saved {len(weekly_batch)} jobs to year-based parquet files (week {days_processed//7 + 1})")
            weekly_batch = []  # Reset batch
        
        # Move to next day
        current_date += timedelta(days=1)
        progress_bar.update(1)
        
        # Small delay between dates to be polite to the API
        if current_date <= end:
            time.sleep(0.5)
    
    progress_bar.close()
    
    # Save any remaining jobs in the final batch
    if weekly_batch:
        jobs_by_year = group_jobs_by_year(weekly_batch)
        
        for year, year_jobs in jobs_by_year.items():
            parquet_path = f"{data_dir}/historical_jobs_{year}.parquet"
            save_jobs_to_parquet(year_jobs, parquet_path)
        
        print(f"💾 Saved final {len(weekly_batch)} jobs to year-based parquet files")
    
    # Report results and any failures with aggressive warnings
    logger.info(f"✅ Total unique jobs found: {len(all_jobs)}")
    print(f"\n✅ Total unique jobs found: {len(all_jobs)}")
    
    # Violently flag any data issues
    if failed_dates:
        log_violent_data_gap_warning(data_logger, failed_dates, start_date, end_date)
    
    # Flag suspicious zero-job days
    if suspicious_zero_days:
        logger.warning(f"⚠️  SUSPICIOUS: {len(suspicious_zero_days)} days had 0 jobs - verify if legitimate:")
        data_logger.critical(f"SUSPICIOUS ZERO-JOB DAYS: {', '.join(suspicious_zero_days)}")
        print(f"\n⚠️  WARNING: {len(suspicious_zero_days)} days had 0 jobs (may be weekends/holidays or API issues):")
        for zero_date in suspicious_zero_days:
            print(f"  🤔 {zero_date}")
        print(f"💡 If these aren't weekends/holidays, consider re-running those dates")
    
    if not failed_dates and not suspicious_zero_days:
        logger.info("🎉 PERFECT RUN: All dates fetched successfully with data!")
        print("🎉 All dates fetched successfully!")
    
    return all_jobs


def main():
    args = parse_args()
    
    print(f"🚀 Fetching historical jobs from {args.start_date} to {args.end_date}")
    if args.position_series:
        print(f"📋 Filtering by position series: {args.position_series}")
    print(f"💾 Saving to parquet files in: {args.data_dir}/")
    
    flattened_jobs = fetch_jobs(
        args.start_date, 
        args.end_date, 
        args.position_series,
        args.data_dir
    )
    
    print(f"\n✅ Completed! Processed {len(flattened_jobs)} total job records")
    
    # Show final parquet file summary
    print(f"\n📊 Parquet files in {args.data_dir}/:")
    for year in range(2015, 2030):
        parquet_path = f"{args.data_dir}/historical_jobs_{year}.parquet"
        if os.path.exists(parquet_path):
            df = pd.read_parquet(parquet_path)
            print(f"  historical_jobs_{year}.parquet: {len(df):,} jobs")


if __name__ == "__main__":
    main()