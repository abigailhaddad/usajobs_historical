#!/usr/bin/env python3
"""
Comprehensive update script for USAJobs data pipeline

This script:
1. Determines the last collection date from existing data
2. Collects historical jobs from last collection date to today
3. Collects current jobs 
4. Updates documentation with current data
5. Provides summary of what was updated

Usage:
    python update/update_all.py
"""

import os
import sys
import json
import pandas as pd
import glob
import subprocess
from datetime import datetime, timedelta

def get_last_collection_date():
    """Get the last date when data was collected"""
    latest_date = None
    
    all_files = glob.glob('../data/historical_jobs_*.parquet') + glob.glob('../data/current_jobs_*.parquet')
    
    for file in all_files:
        try:
            df = pd.read_parquet(file)
            if 'inserted_at' in df.columns:
                df['inserted_at'] = pd.to_datetime(df['inserted_at'], errors='coerce')
                file_latest = df['inserted_at'].max()
                
                if pd.notna(file_latest) and (latest_date is None or file_latest > latest_date):
                    latest_date = file_latest
        except Exception as e:
            print(f"Warning: Could not read {file}: {e}")
            continue
    
    return latest_date

def run_command(command, description):
    """Run a shell command and return success status and output"""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, 
                              capture_output=True, text=True)
        print(f"✅ {description} completed")
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        print(f"Error output: {e.stderr}")
        return False, e.stderr

def parse_collection_output(output):
    """Parse output from data collection scripts to extract stats"""
    import re
    
    stats = {
        'new_jobs': 0,
        'total_jobs': 0,
        'failed_dates': [],
        'errors': []
    }
    
    # Look for patterns in the output
    if 'new jobs total' in output:
        # Current API pattern: "Added 3 new jobs total"
        match = re.search(r'Added (\d+) new jobs total', output)
        if match:
            stats['new_jobs'] = int(match.group(1))
    
    if 'jobs saved' in output:
        # Historical API pattern: "123 jobs saved"
        match = re.search(r'(\d+) jobs saved', output)
        if match:
            stats['new_jobs'] = int(match.group(1))
    
    # Look for error patterns
    if 'CRITICAL DATA ISSUE' in output or 'failed' in output.lower():
        # Extract failed dates if present
        failed_matches = re.findall(r'Failed.*?(\d{4}-\d{2}-\d{2})', output)
        stats['failed_dates'].extend(failed_matches)
        
        # Extract general errors
        error_lines = [line.strip() for line in output.split('\n') 
                      if 'error' in line.lower() or 'failed' in line.lower()]
        stats['errors'].extend(error_lines[:3])  # Limit to first 3 errors
    
    return stats

def main():
    print("🚀 USAJobs Data Pipeline - Comprehensive Update")
    print("=" * 50)
    
    # Step 1: Determine last collection date
    print("📅 Checking last collection date...")
    last_date = get_last_collection_date()
    
    if last_date:
        last_date_str = last_date.strftime('%Y-%m-%d')
        print(f"   Last collection: {last_date_str}")
    else:
        print("   No previous data found - this appears to be a first run")
        last_date_str = '2025-01-01'  # Default start date
    
    # Calculate date range to collect
    today = datetime.now()
    today_str = today.strftime('%Y-%m-%d')
    
    # Start from last collection date (include overlap to ensure no gaps)
    if last_date:
        start_date = last_date.strftime('%Y-%m-%d')  # Include the last day
    else:
        start_date = last_date_str
    
    print(f"   Will collect: {start_date} to {today_str}")
    print("\\n" + "=" * 50)
    
    # Initialize collection statistics
    total_new_jobs = 0
    all_failed_dates = []
    collection_errors = []
    
    # Step 2: Collect historical data for the new date range
    if start_date <= today_str:
        historical_cmd = f"python ../scripts/collect_data.py --start-date {start_date} --end-date {today_str} --data-dir ../data"
        success, output = run_command(historical_cmd, f"Collecting historical jobs ({start_date} to {today_str})")
        
        if success:
            stats = parse_collection_output(output)
            total_new_jobs += stats['new_jobs']
            all_failed_dates.extend(stats['failed_dates'])
            collection_errors.extend(stats['errors'])
            print(f"   📊 Added {stats['new_jobs']} historical jobs")
        else:
            print("❌ Historical data collection failed. Continuing with current jobs...")
            collection_errors.append("Historical data collection failed")
    else:
        print("ℹ️  No new historical data to collect (already up to date)")
    
    # Step 3: Collect current jobs (always do this to get latest active postings)
    current_cmd = "python ../scripts/collect_current_data.py --data-dir ../data"
    success, output = run_command(current_cmd, "Collecting current jobs")
    
    if success:
        stats = parse_collection_output(output)
        total_new_jobs += stats['new_jobs']
        all_failed_dates.extend(stats['failed_dates'])
        collection_errors.extend(stats['errors'])
        print(f"   📊 Added {stats['new_jobs']} current jobs")
    else:
        print("❌ Current data collection failed. Continuing with documentation update...")
        collection_errors.append("Current data collection failed")
    
    # Step 4: Update documentation
    print("\\n📝 Updating documentation...")
    
    # Generate new documentation data
    success, _ = run_command("python generate_docs_data.py", 
                                 "Generating documentation data")
    if not success:
        print("❌ Documentation generation failed")
        return
    
    # Update README and index.html
    success, _ = run_command("python update_docs.py", 
                                 "Updating README and index.html")
    if not success:
        print("❌ Documentation update failed")
        return
    
    # Step 5: Summary
    print("\\n" + "=" * 50)
    print("📊 UPDATE SUMMARY")
    print("=" * 50)
    
    try:
        # Read updated stats
        with open('../docs_data.json', 'r') as f:
            data = json.load(f)
        
        print(f"✅ Total jobs in dataset: {data['total_jobs']:,}")
        print(f"✅ Dataset size: {data['file_size']}")
        print(f"✅ Last collection: {data['latest_job_date']}")
        print(f"✅ Documentation updated: {data['generated_at']}")
        
        # Show what files were updated
        print("\\n📁 Updated files:")
        print("   • README.md")
        print("   • index.html") 
        print("   • docs_data.json")
        
        # Show data files
        parquet_files = glob.glob('../data/*.parquet')
        print(f"   • {len(parquet_files)} parquet data files")
        
    except Exception as e:
        print(f"❌ Could not read summary data: {e}")
    
    print("\\n🎉 Update completed successfully!")
    print("\\nNext steps:")
    print("   • Review any error logs in logs/ directory")
    print("   • Check updated documentation in README.md and index.html")
    print("   • Consider committing changes with git")

if __name__ == "__main__":
    # Ensure we're in the right directory
    if not os.path.exists('../scripts/collect_data.py'):
        print("❌ Please run this script from the update/ directory")
        sys.exit(1)
    
    main()