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

def run_command(command, description, stream_output=False):
    """Run a shell command and return success status and output"""
    print(f"🔄 {description}...")
    try:
        if stream_output:
            # Stream output in real-time for commands with progress bars
            result = subprocess.run(command, shell=True, check=True)
            print(f"✅ {description} completed")
            return True, ""  # No captured output when streaming
        else:
            # Capture output for parsing
            result = subprocess.run(command, shell=True, check=True, 
                                  capture_output=True, text=True)
            print(f"✅ {description} completed")
            return True, result.stdout
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        if not stream_output and hasattr(e, 'stderr'):
            print(f"Error output: {e.stderr}")
        return False, e.stderr if hasattr(e, 'stderr') else ""

def parse_collection_output(output):
    """Parse output from data collection scripts to extract stats"""
    import re
    
    stats = {
        'new_jobs': 0,
        'total_jobs': 0,
        'failed_dates': [],
        'errors': [],
        'jobs_per_file': {}  # New field to track jobs added per file
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
    
    # Look for per-file patterns
    # Pattern 1: "Saved 123 jobs to /path/to/file.parquet"
    file_matches = re.findall(r'Saved (\d+) jobs to (.+\.parquet)', output)
    for count, filepath in file_matches:
        filename = os.path.basename(filepath)
        stats['jobs_per_file'][filename] = int(count)
    
    # Pattern 2: "historical_jobs_2025.parquet: 123 jobs" (from final summary)
    summary_matches = re.findall(r'((?:historical|current)_jobs_\d+\.parquet): ([\d,]+) jobs', output)
    for filename, count in summary_matches:
        # This is total count, not new jobs, so skip if we already have data
        if filename not in stats['jobs_per_file']:
            # Remove commas from number
            stats['jobs_per_file'][filename] = int(count.replace(',', ''))
    
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

def record_initial_file_sizes():
    """Record initial file sizes before data collection"""
    print("📏 Recording initial file sizes...")
    
    data_files = glob.glob('../data/current_jobs_*.parquet') + glob.glob('../data/historical_jobs_*.parquet')
    initial_sizes = {}
    
    for file in data_files:
        try:
            initial_size = os.path.getsize(file)
            initial_sizes[file] = initial_size
            print(f"📝 {file}: {initial_size:,} bytes")
        except Exception as e:
            print(f"⚠️  Could not read {file}: {e}")
            initial_sizes[file] = 0  # Assume new file
    
    return initial_sizes

def record_initial_job_counts():
    """Record initial job counts before data collection"""
    print("📊 Recording initial job counts...")
    
    data_files = glob.glob('../data/current_jobs_*.parquet') + glob.glob('../data/historical_jobs_*.parquet')
    initial_counts = {}
    
    for file in data_files:
        try:
            df = pd.read_parquet(file)
            count = len(df)
            initial_counts[file] = count
            print(f"📝 {os.path.basename(file)}: {count:,} jobs")
        except Exception as e:
            print(f"⚠️  Could not read {file}: {e}")
            initial_counts[file] = 0  # Assume new file or error
    
    return initial_counts

def calculate_job_additions(initial_counts):
    """Calculate how many jobs were added to each file"""
    print("📊 Calculating job additions...")
    
    data_files = glob.glob('../data/current_jobs_*.parquet') + glob.glob('../data/historical_jobs_*.parquet')
    job_additions = {}
    
    for file in data_files:
        try:
            df = pd.read_parquet(file)
            current_count = len(df)
            initial_count = initial_counts.get(file, 0)
            added = current_count - initial_count
            
            filename = os.path.basename(file)
            job_additions[filename] = added
            
            if added > 0:
                print(f"✅ {filename}: {added:,} jobs added (was {initial_count:,}, now {current_count:,})")
            else:
                print(f"ℹ️  {filename}: no new jobs (still {current_count:,})")
                
        except Exception as e:
            print(f"⚠️  Could not read {file}: {e}")
            job_additions[os.path.basename(file)] = 0
    
    return job_additions

def check_file_sizes_vs_initial(initial_sizes):
    """Check that data files are same size or bigger than initial"""
    print("🔍 Checking data file sizes vs initial...")
    
    data_files = glob.glob('../data/current_jobs_*.parquet') + glob.glob('../data/historical_jobs_*.parquet')
    size_checks = []
    files_changed = False
    
    for file in data_files:
        try:
            current_size = os.path.getsize(file)
            initial_size = initial_sizes.get(file, 0)
            
            if current_size < initial_size:
                print(f"❌ {file} SHRUNK: {initial_size:,} → {current_size:,} bytes")
                size_checks.append(False)
            elif current_size == initial_size:
                print(f"✅ {file}: {current_size:,} bytes (unchanged)")
                size_checks.append(True)
            else:
                print(f"✅ {file}: {initial_size:,} → {current_size:,} bytes (+{current_size-initial_size:,})")
                size_checks.append(True)
                files_changed = True
                
        except Exception as e:
            print(f"❌ Could not check size of {file}: {e}")
            size_checks.append(False)
    
    if not all(size_checks):
        print("⚠️  Some data files shrunk! Skipping git operations for safety.")
        return False, False
    
    return True, files_changed

def commit_and_push_changes():
    """Commit and push changes to git repository"""
    print("\n📤 Committing and pushing changes...")
    
    # Check git status first
    success, output = run_command("git status --porcelain", "Checking git status")
    if not success:
        print("❌ Could not check git status")
        return False
    
    if not output.strip():
        print("ℹ️  No changes to commit")
        return True
    
    # Add files - use relative paths from update directory
    files_to_add = [
        "../data/current_jobs_*.parquet",
        "../data/historical_jobs_*.parquet", 
        "../index.html", 
        "../README.md",
        "../docs_data.json"
    ]
    
    for file_pattern in files_to_add:
        success, _ = run_command(f"git add {file_pattern}", f"Adding {file_pattern}")
        if not success:
            print(f"⚠️  Could not add {file_pattern} (may not exist)")
    
    # Commit with timestamp
    today = datetime.now().strftime('%Y-%m-%d')
    commit_msg = f"Daily data and documentation update - {today}"
    
    success, _ = run_command(f'git commit -m "{commit_msg}"', "Committing changes")
    if not success:
        print("ℹ️  No changes to commit or commit failed")
        return False
    
    # Push to remote
    success, _ = run_command("git push", "Pushing to remote repository")
    if not success:
        print("❌ Failed to push to remote repository")
        return False
    
    print("✅ Successfully committed and pushed changes")
    return True

def main():
    print("🚀 USAJobs Data Pipeline - Comprehensive Update")
    print("=" * 50)
    
    # Step 1: Record initial file sizes and job counts
    initial_sizes = record_initial_file_sizes()
    initial_counts = record_initial_job_counts()
    
    # Step 2: Determine last collection date
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
    all_jobs_per_file = {}  # Track jobs added per file across both collections
    
    # Step 2: Collect historical data for the new date range
    if start_date <= today_str:
        historical_cmd = f"python ../scripts/collect_data.py --start-date {start_date} --end-date {today_str} --data-dir ../data"
        success, output = run_command(historical_cmd, f"Collecting historical jobs ({start_date} to {today_str})", stream_output=True)
        
        if success:
            # When streaming, we need to calculate stats differently
            print(f"   📊 Historical data collection completed")
        else:
            print("❌ Historical data collection failed. Continuing with current jobs...")
            collection_errors.append("Historical data collection failed")
    else:
        print("ℹ️  No new historical data to collect (already up to date)")
    
    # Step 3: Collect current jobs (always do this to get latest active postings)
    current_cmd = "python ../scripts/collect_current_data.py --data-dir ../data"
    success, output = run_command(current_cmd, "Collecting current jobs", stream_output=True)
    
    if success:
        # When streaming, we need to calculate stats differently
        print(f"   📊 Current data collection completed")
    else:
        print("❌ Current data collection failed. Continuing with documentation update...")
        collection_errors.append("Current data collection failed")
    
    # Step 4: Check data file integrity
    print("\\n🔍 Checking data file integrity...")
    files_ok, files_changed = check_file_sizes_vs_initial(initial_sizes)
    if not files_ok:
        print("⚠️  Data file checks failed. Skipping git operations for safety.")
        return
    
    if not files_changed:
        print("ℹ️  No data files changed. Skipping documentation update and git operations.")
        print("\\n🎉 Update completed - no changes needed!")
        return
    
    # Step 5: Calculate actual job additions
    job_additions = calculate_job_additions(initial_counts)
    
    # Step 6: Update documentation
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
    
    # Step 7: Commit and push changes
    git_success = commit_and_push_changes()
    if not git_success:
        print("⚠️  Git operations failed - changes are local only")
    
    # Step 7: Summary
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
        
        # Show jobs added per file (using actual calculated additions)
        if job_additions:
            print("\\n📊 Jobs added per file:")
            for filename in sorted(job_additions.keys()):
                count = job_additions[filename]
                if count > 0:
                    print(f"   • {filename}: {count:,} jobs added")
                else:
                    print(f"   • {filename}: 0 jobs added")
        
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
    if git_success:
        print("   • Changes have been committed and pushed to GitHub")
    else:
        print("   • Manual git commit/push may be needed")

if __name__ == "__main__":
    # Ensure we're in the right directory
    if not os.path.exists('../scripts/collect_data.py'):
        print("❌ Please run this script from the update/ directory")
        sys.exit(1)
    
    main()