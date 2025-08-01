#!/usr/bin/env python3
"""
Update Active Job Statuses

This script finds all jobs with "active" statuses (Accepting applications, 
Applications under review) and re-fetches data for those specific dates
to capture status changes.

Usage:
    python update_active_statuses.py --data-dir ../data
    python update_active_statuses.py --data-dir ../data --dry-run
"""

import argparse
import pandas as pd
import glob
import os
import subprocess
from datetime import datetime
from collections import defaultdict
import sys
import time
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('status_updates.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description="Update jobs with active statuses")
    parser.add_argument("--data-dir", default="../data", help="Directory containing parquet files")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be updated without running")
    parser.add_argument("--max-dates", type=int, default=None, help="Maximum number of dates to update in one run (default: all dates)")
    parser.add_argument("--year", type=int, default=2025, help="Year to process (default: 2025)")
    parser.add_argument("--all-years", action="store_true", help="Process all years, not just the specified year")
    return parser.parse_args()


def find_active_jobs(data_dir, year=None):
    """Find all jobs with active statuses and group by date
    
    Args:
        data_dir: Directory containing parquet files
        year: If set, only process this year's data
    """
    print(f"🔍 Scanning for jobs with active statuses{f' in {year}' if year else ''}...")
    
    # Statuses that indicate a job might change
    active_statuses = ['Accepting applications', 'Applications under review']
    
    # Dictionary to store dates and their job counts
    dates_to_update = defaultdict(list)
    total_active_jobs = 0
    
    # Scan historical parquet files
    if year:
        historical_files = [os.path.join(data_dir, f"historical_jobs_{year}.parquet")]
        if not os.path.exists(historical_files[0]):
            print(f"   ⚠️  No data file found for {year}")
            return dates_to_update
    else:
        historical_files = glob.glob(os.path.join(data_dir, "historical_jobs_*.parquet"))
    
    for file_path in sorted(historical_files):
        print(f"📄 Scanning {os.path.basename(file_path)}...")
        
        try:
            df = pd.read_parquet(file_path)
            
            # Find jobs with active statuses
            if 'positionOpeningStatus' in df.columns:
                active_df = df[df['positionOpeningStatus'].isin(active_statuses)].copy()
                
                if len(active_df) > 0:
                    # Extract date from positionOpenDate
                    active_df['date'] = pd.to_datetime(active_df['positionOpenDate']).dt.date
                    
                    # Group by date
                    for date, group in active_df.groupby('date'):
                        date_str = date.strftime('%Y-%m-%d')
                        job_info = []
                        
                        for _, job in group.iterrows():
                            job_info.append({
                                'control_number': job.get('usajobsControlNumber', 'Unknown') if hasattr(job, 'get') else job['usajobsControlNumber'],
                                'title': str(job.get('positionTitle', 'Unknown') if hasattr(job, 'get') else job['positionTitle'])[:50],
                                'agency': str(job.get('hiringAgencyName', 'Unknown') if hasattr(job, 'get') else job['hiringAgencyName'])[:30],
                                'status': job.get('positionOpeningStatus', 'Unknown') if hasattr(job, 'get') else job['positionOpeningStatus']
                            })
                        
                        dates_to_update[date_str].extend(job_info)
                        total_active_jobs += len(group)
                    
                    print(f"   Found {len(active_df)} active jobs in this file")
            
        except Exception as e:
            print(f"   ⚠️  Error reading {file_path}: {e}")
    
    print(f"\n✅ Found {total_active_jobs} total jobs with active statuses")
    print(f"📅 These jobs span {len(dates_to_update)} unique dates")
    
    return dates_to_update


def display_update_plan(dates_to_update, max_dates):
    """Display what will be updated"""
    print("\n📋 Update Plan:")
    print("=" * 80)
    
    # Sort dates from most recent to oldest
    sorted_dates = sorted(dates_to_update.keys(), reverse=True)
    
    # Show summary by month
    month_counts = defaultdict(int)
    for date_str in sorted_dates:
        month = date_str[:7]  # YYYY-MM
        month_counts[month] += len(dates_to_update[date_str])
    
    print("\n📊 Active jobs by month:")
    for month in sorted(month_counts.keys(), reverse=True):
        print(f"   {month}: {month_counts[month]} jobs")
    
    # Show dates to be updated
    if max_dates is None:
        dates_to_process = sorted_dates
        print(f"\n📅 Will update ALL {len(dates_to_process)} dates")
    else:
        dates_to_process = sorted_dates[:max_dates]
        print(f"\n📅 Will update {len(dates_to_process)} most recent dates (of {len(sorted_dates)} total)")
    
    if len(dates_to_process) <= 10:
        print("\nDates to update:")
        for date_str in dates_to_process:
            job_count = len(dates_to_update[date_str])
            print(f"   {date_str}: {job_count} active jobs")
    else:
        print(f"\nDate range: {dates_to_process[-1]} to {dates_to_process[0]}")
        total_jobs = sum(len(dates_to_update[d]) for d in dates_to_process)
        print(f"Total active jobs to check: {total_jobs}")
    
    return dates_to_process


def get_status_snapshot(data_dir, date_str):
    """Get a snapshot of job statuses for a specific date"""
    job_statuses = {}
    
    # Parse the date
    year = int(date_str.split('-')[0])
    
    # Read the relevant parquet file
    parquet_file = os.path.join(data_dir, f"historical_jobs_{year}.parquet")
    if os.path.exists(parquet_file):
        try:
            df = pd.read_parquet(parquet_file)
            # Filter to jobs from this specific date
            df['date'] = pd.to_datetime(df['positionOpenDate']).dt.strftime('%Y-%m-%d')
            date_jobs = df[df['date'] == date_str]
            
            # Create snapshot of control number -> status
            for _, job in date_jobs.iterrows():
                control_num = job.get('usajobsControlNumber')
                if control_num:
                    job_statuses[str(control_num)] = job.get('positionOpeningStatus', 'Unknown')
        except:
            pass
    
    return job_statuses


def compare_status_changes(before_snapshot, after_snapshot):
    """Compare two status snapshots and return changes"""
    status_changes = {
        'cancelled': 0,
        'selected': 0,
        'closed': 0,
        'other_changes': 0,
        'unchanged': 0,
        'details': []
    }
    
    for control_num, old_status in before_snapshot.items():
        new_status = after_snapshot.get(control_num, old_status)
        
        if old_status != new_status:
            if new_status == 'Job canceled':
                status_changes['cancelled'] += 1
            elif new_status == 'Candidate selected':
                status_changes['selected'] += 1
            elif new_status == 'Job closed':
                status_changes['closed'] += 1
            else:
                status_changes['other_changes'] += 1
            
            status_changes['details'].append({
                'control_num': control_num,
                'old_status': old_status,
                'new_status': new_status
            })
        else:
            status_changes['unchanged'] += 1
    
    return status_changes


def run_update_for_dates(dates_to_update, data_dir, dry_run=False):
    """Run the historical data collection for specific dates"""
    if dry_run:
        print("\n🔄 DRY RUN - Would execute:")
        for date in dates_to_update:
            cmd = f"python ../scripts/collect_data.py --start-date {date} --end-date {date} --data-dir {data_dir}"
            print(f"   {cmd}")
        return True
    
    print("\n🔄 Running updates...")
    success_count = 0
    failed_dates = []
    total_status_changes = {
        'cancelled': 0,
        'selected': 0,
        'closed': 0,
        'other_changes': 0
    }
    
    for i, date in enumerate(dates_to_update, 1):
        print(f"\n[{i}/{len(dates_to_update)}] Updating {date}...")
        
        # Take snapshot before update
        before_snapshot = get_status_snapshot(data_dir, date)
        
        cmd = [
            "python", "../scripts/collect_data.py",
            "--start-date", date,
            "--end-date", date,
            "--data-dir", data_dir
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # Take snapshot after update
            after_snapshot = get_status_snapshot(data_dir, date)
            
            # Compare snapshots
            changes = compare_status_changes(before_snapshot, after_snapshot)
            
            # Look for update counts in output
            output_lines = result.stdout.split('\n')
            for line in output_lines:
                if 'new,' in line and 'updated' in line:
                    print(f"   ✅ {line.strip()}")
                    break
            
            # Print status changes
            if changes['cancelled'] > 0 or changes['selected'] > 0 or changes['closed'] > 0:
                print(f"   📊 Status changes: {changes['cancelled']} cancelled, {changes['selected']} selected, {changes['closed']} closed")
                
                # Update totals
                total_status_changes['cancelled'] += changes['cancelled']
                total_status_changes['selected'] += changes['selected']
                total_status_changes['closed'] += changes['closed']
                total_status_changes['other_changes'] += changes['other_changes']
                
                # Log details of cancelled jobs
                if changes['cancelled'] > 0:
                    logger.info(f"CANCELLED JOBS for {date}:")
                    for detail in changes['details']:
                        if detail['new_status'] == 'Job canceled':
                            logger.info(f"  Control #{detail['control_num']}: {detail['old_status']} -> CANCELLED")
            
            success_count += 1
            
        except subprocess.CalledProcessError as e:
            print(f"   ❌ Failed: {e}")
            failed_dates.append(date)
    
    print(f"\n📊 Update Summary:")
    print(f"   ✅ Successfully updated: {success_count} dates")
    if failed_dates:
        print(f"   ❌ Failed: {len(failed_dates)} dates")
        print(f"      Failed dates: {', '.join(failed_dates[:5])}")
        if len(failed_dates) > 5:
            print(f"      ... and {len(failed_dates) - 5} more")
    
    # Show total status changes
    print(f"\n📈 Total Status Changes Detected:")
    print(f"   🚫 Jobs cancelled: {total_status_changes['cancelled']}")
    print(f"   ✅ Candidates selected: {total_status_changes['selected']}")
    print(f"   🔒 Jobs closed: {total_status_changes['closed']}")
    if total_status_changes['other_changes'] > 0:
        print(f"   📝 Other changes: {total_status_changes['other_changes']}")
    
    return len(failed_dates) == 0


def analyze_status_changes(data_dir, dates_updated):
    """Analyze what status changes occurred after the update"""
    print("\n📊 Analyzing status changes...")
    
    status_changes = []
    
    # This would need to compare before/after snapshots
    # For now, just show current status distribution
    
    all_statuses = defaultdict(int)
    for file_path in glob.glob(os.path.join(data_dir, "historical_jobs_*.parquet")):
        try:
            df = pd.read_parquet(file_path)
            if 'positionOpeningStatus' in df.columns:
                status_counts = df['positionOpeningStatus'].value_counts()
                for status, count in status_counts.items():
                    all_statuses[status] += count
        except:
            pass
    
    print("\n📈 Current status distribution:")
    for status, count in sorted(all_statuses.items(), key=lambda x: x[1], reverse=True):
        print(f"   {status}: {count:,}")


def main():
    args = parse_args()
    
    start_time = time.time()
    
    print("🚀 USAJobs Active Status Updater")
    print("=" * 80)
    logger.info(f"Starting update run at {datetime.now()}")
    
    # Ensure we're in the right directory
    if not os.path.exists('../scripts/collect_data.py'):
        print("❌ Please run this script from the job_status_tracker/ directory")
        print("   Current directory:", os.getcwd())
        sys.exit(1)
    
    # Find all active jobs
    year_to_process = None if args.all_years else args.year
    dates_to_update = find_active_jobs(args.data_dir, year=year_to_process)
    
    if not dates_to_update:
        print("\n✅ No active jobs found - nothing to update!")
        return
    
    # Display update plan
    dates_to_process = display_update_plan(dates_to_update, args.max_dates)
    
    if args.dry_run:
        print("\n🔍 DRY RUN MODE - No actual updates will be performed")
    else:
        # Confirm before proceeding
        print(f"\n⚠️  This will re-fetch data for {len(dates_to_process)} dates")
        response = input("Continue? (y/N): ")
        if response.lower() != 'y':
            print("❌ Cancelled by user")
            return
    
    # Run the updates
    success = run_update_for_dates(dates_to_process, args.data_dir, args.dry_run)
    
    if success and not args.dry_run:
        # Analyze what changed
        analyze_status_changes(args.data_dir, dates_to_process)
    
    print("\n✅ Active status update completed!")
    
    # Suggest next steps
    remaining_dates = len(dates_to_update) - len(dates_to_process)
    if remaining_dates > 0:
        print(f"\n💡 Note: {remaining_dates} older dates with active jobs were not updated")
        print(f"   Run with --max-dates {len(dates_to_update)} to update all dates")
    
    # Report timing
    elapsed_time = time.time() - start_time
    hours = int(elapsed_time // 3600)
    minutes = int((elapsed_time % 3600) // 60)
    seconds = int(elapsed_time % 60)
    
    print(f"\n⏱️  Total time: {hours}h {minutes}m {seconds}s ({elapsed_time:.1f} seconds)")
    logger.info(f"Update completed in {hours}h {minutes}m {seconds}s")
    
    if not args.dry_run and len(dates_to_process) > 0:
        avg_time_per_date = elapsed_time / len(dates_to_process)
        print(f"   Average time per date: {avg_time_per_date:.1f} seconds")
        
        if remaining_dates > 0:
            estimated_total_time = avg_time_per_date * len(dates_to_update)
            est_hours = int(estimated_total_time // 3600)
            est_minutes = int((estimated_total_time % 3600) // 60)
            print(f"   Estimated time for all {len(dates_to_update)} dates: {est_hours}h {est_minutes}m")


if __name__ == "__main__":
    main()