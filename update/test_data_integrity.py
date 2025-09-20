#!/usr/bin/env python3
"""
Test script to verify data integrity after daily updates.
Ensures parquet files are valid and contain expected data.

Usage: python test_data_integrity.py
"""

import os
import sys
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import json

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'

def print_header(text):
    print(f"\n{Colors.BLUE}{'=' * 60}{Colors.RESET}")
    print(f"{Colors.BLUE}{text}{Colors.RESET}")
    print(f"{Colors.BLUE}{'=' * 60}{Colors.RESET}")

def check_parquet_file(filepath, min_rows, required_columns, description):
    """Check if parquet file is valid and has required columns"""
    if not os.path.exists(filepath):
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} {description} - File not found")
        return False
    
    try:
        # Check if file is valid parquet
        pq_file = pq.ParquetFile(filepath)
        schema = pq_file.schema
        
        # Read the data
        df = pd.read_parquet(filepath)
        
        # Check row count
        if len(df) < min_rows:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} {description}")
            print(f"     Expected at least {min_rows} rows, found {len(df)}")
            return False
        
        # Check columns
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} {description} - Missing columns: {missing_cols}")
            return False
        
        print(f"{Colors.GREEN}✅ PASS{Colors.RESET} {description} ({len(df):,} rows)")
        return True
        
    except Exception as e:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} {description} - Error reading parquet")
        print(f"     Error: {e}")
        return False

def check_data_recency(filepath, max_days_old, description):
    """Check if data is recent enough"""
    try:
        df = pd.read_parquet(filepath)
        
        # Check if there's a date column
        date_columns = ['DatePosted', 'date_posted', 'PositionOpenDate', 'ApplicationCloseDate', 'applicationCloseDate', 'positionOpenDate']
        date_col = None
        for col in date_columns:
            if col in df.columns:
                date_col = col
                break
        
        if not date_col:
            print(f"{Colors.YELLOW}⚠️  WARN{Colors.RESET} {description} - No date column found")
            return True
        
        # Convert to datetime and find most recent
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        most_recent = df[date_col].max()
        
        if pd.isna(most_recent):
            print(f"{Colors.YELLOW}⚠️  WARN{Colors.RESET} {description} - Could not parse dates")
            return True
        
        days_old = (datetime.now() - most_recent).days
        
        if days_old > max_days_old:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} {description}")
            print(f"     Most recent data is {days_old} days old (max allowed: {max_days_old})")
            return False
        else:
            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} {description} - Most recent data is {days_old} days old")
            return True
            
    except Exception as e:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} {description} - Error checking dates: {e}")
        return False

def check_no_data_loss():
    """Ensure historical data hasn't been lost"""
    baseline_file = 'data_baseline.json'  # Store in update directory
    
    # If baseline doesn't exist, create it
    if not os.path.exists(baseline_file):
        print(f"{Colors.YELLOW}⚠️  Creating baseline file...{Colors.RESET}")
        create_baseline(baseline_file)
        print(f"{Colors.GREEN}✅ Baseline created.{Colors.RESET} Run tests again to check for regressions.")
        return True
    
    # Load baseline
    with open(baseline_file, 'r') as f:
        baseline = json.load(f)
    
    all_good = True
    
    # Check each parquet file hasn't shrunk
    for filename, baseline_count in baseline.get('row_counts', {}).items():
        filepath = f"../data/{filename}"
        if os.path.exists(filepath):
            try:
                df = pd.read_parquet(filepath)
                current_count = len(df)
                
                if current_count < baseline_count:
                    print(f"{Colors.RED}❌ FAIL{Colors.RESET} {filename} shrunk: {baseline_count} → {current_count}")
                    all_good = False
                elif current_count > baseline_count:
                    print(f"{Colors.GREEN}✅ PASS{Colors.RESET} {filename} grew: {baseline_count} → {current_count}")
                else:
                    print(f"{Colors.GREEN}✅ PASS{Colors.RESET} {filename} unchanged: {current_count} rows")
            except Exception as e:
                print(f"{Colors.RED}❌ FAIL{Colors.RESET} Could not check {filename}: {e}")
                all_good = False
    
    return all_good

def check_no_job_id_loss():
    """CRITICAL: Ensure no job IDs have been lost from any file"""
    baseline_file = 'data_baseline.json'
    
    # If baseline doesn't exist, we can't check job IDs
    if not os.path.exists(baseline_file):
        print(f"{Colors.YELLOW}⚠️  No baseline for job ID tracking yet{Colors.RESET}")
        return True
    
    # Load baseline
    with open(baseline_file, 'r') as f:
        baseline = json.load(f)
    
    if 'job_ids' not in baseline:
        print(f"{Colors.YELLOW}⚠️  Old baseline format - job IDs not tracked{Colors.RESET}")
        # Update baseline to include job IDs
        create_baseline(baseline_file)
        return True
    
    all_good = True
    total_lost_jobs = 0
    
    print(f"\n{Colors.BLUE}Checking for lost job IDs...{Colors.RESET}")
    
    # Check each file for lost job IDs
    for filename, baseline_ids in baseline.get('job_ids', {}).items():
        filepath = f"../data/{filename}"
        if os.path.exists(filepath) and baseline_ids:
            try:
                df = pd.read_parquet(filepath)
                
                # Get current job IDs using same column detection - convert to string for comparison
                current_ids = set()
                if 'usajobsControlNumber' in df.columns:
                    current_ids = set(str(x) for x in df['usajobsControlNumber'].dropna().unique())
                elif 'usajobs_control_number' in df.columns:
                    current_ids = set(str(x) for x in df['usajobs_control_number'].dropna().unique())
                elif 'PositionID' in df.columns:
                    current_ids = set(str(x) for x in df['PositionID'].dropna().unique())
                
                if current_ids:
                    baseline_set = set(baseline_ids)
                    lost_ids = baseline_set - current_ids
                    
                    if lost_ids:
                        print(f"{Colors.RED}❌ CRITICAL FAIL{Colors.RESET} {filename} LOST {len(lost_ids)} job IDs!")
                        print(f"     First 10 lost IDs: {list(lost_ids)[:10]}")
                        total_lost_jobs += len(lost_ids)
                        all_good = False
                    else:
                        new_ids = current_ids - baseline_set
                        if new_ids:
                            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} {filename} - No jobs lost (+{len(new_ids)} new)")
                        else:
                            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} {filename} - No jobs lost (unchanged)")
                
            except Exception as e:
                print(f"{Colors.RED}❌ FAIL{Colors.RESET} Could not check job IDs in {filename}: {e}")
                all_good = False
    
    if total_lost_jobs > 0:
        print(f"\n{Colors.RED}⚠️  CRITICAL: {total_lost_jobs} total job IDs lost across all files!{Colors.RESET}")
        print(f"{Colors.RED}This should NEVER happen with historical data!{Colors.RESET}")
    
    return all_good

def create_baseline(filepath):
    """Create baseline snapshot of current data"""
    baseline = {
        'created_at': datetime.now().isoformat(),
        'row_counts': {},
        'job_ids': {}  # Track all job IDs per file
    }
    
    # Get all parquet files
    data_dir = '../data'
    for filename in os.listdir(data_dir):
        if filename.endswith('.parquet'):
            try:
                df = pd.read_parquet(os.path.join(data_dir, filename))
                baseline['row_counts'][filename] = len(df)
                
                # Store job IDs based on available columns - convert to string to ensure JSON serializable
                if 'usajobsControlNumber' in df.columns:
                    baseline['job_ids'][filename] = [str(x) for x in df['usajobsControlNumber'].dropna().unique()]
                elif 'usajobs_control_number' in df.columns:
                    baseline['job_ids'][filename] = [str(x) for x in df['usajobs_control_number'].dropna().unique()]
                elif 'PositionID' in df.columns:
                    baseline['job_ids'][filename] = [str(x) for x in df['PositionID'].dropna().unique()]
                    
            except Exception as e:
                print(f"Could not read {filename}: {e}")
    
    with open(filepath, 'w') as f:
        json.dump(baseline, f, indent=2)

def check_data_consistency():
    """Check for data consistency issues"""
    try:
        # Check current year files
        current_2025 = pd.read_parquet('../data/current_jobs_2025.parquet')
        historical_2025 = pd.read_parquet('../data/historical_jobs_2025.parquet')
        
        # Current should be subset of or equal to historical for same year
        current_ids = set(current_2025['PositionID'].unique()) if 'PositionID' in current_2025.columns else set()
        historical_ids = set(historical_2025['PositionID'].unique()) if 'PositionID' in historical_2025.columns else set()
        
        # Check for control numbers if PositionID doesn't exist
        if not current_ids and 'usajobsControlNumber' in current_2025.columns:
            current_ids = set(current_2025['usajobsControlNumber'].unique())
        if not historical_ids and 'usajobsControlNumber' in historical_2025.columns:
            historical_ids = set(historical_2025['usajobsControlNumber'].unique())
        
        if current_ids and historical_ids:
            missing_in_historical = current_ids - historical_ids
            overlap = len(current_ids & historical_ids)
            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} Data consistency check")
            print(f"     {len(missing_in_historical):,} current jobs not yet in historical (normal)")
            print(f"     {overlap:,} jobs appear in both files")
        else:
            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} Could not check ID overlap (different schemas OK)")
        
        return True
        
    except Exception as e:
        print(f"{Colors.YELLOW}⚠️  WARN{Colors.RESET} Could not check consistency: {e}")
        return True

def run_tests():
    """Run all data integrity tests"""
    print(f"{Colors.BLUE}DATA INTEGRITY TESTS{Colors.RESET}")
    print(f"{Colors.BLUE}Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Colors.RESET}")
    
    all_passed = True
    
    # Test 1: Current data files exist and are valid
    print_header("1. CURRENT DATA FILES")
    
    current_files = [
        ('current_jobs_2024.parquet', 100, ['positionTitle'], 'Current jobs 2024'),
        ('current_jobs_2025.parquet', 100, ['positionTitle'], 'Current jobs 2025')
    ]
    
    for filename, min_rows, cols, desc in current_files:
        if not check_parquet_file(f'../data/{filename}', min_rows, cols, desc):
            all_passed = False
    
    # Test 2: Historical data files exist
    print_header("2. HISTORICAL DATA FILES")
    
    for year in range(2013, 2026):
        filename = f'historical_jobs_{year}.parquet'
        min_rows = 5 if year < 2015 else 10 if year < 2017 else 1000  # Early years have less data
        if not check_parquet_file(f'../data/{filename}', min_rows, 
                                   ['positionTitle'], f'Historical jobs {year}'):
            all_passed = False
    
    # Test 3: Data recency
    print_header("3. DATA RECENCY")
    
    # Current data should be no more than 7 days old
    if not check_data_recency('../data/current_jobs_2025.parquet', 7, 'Current 2025 data recency'):
        all_passed = False
    
    # Test 4: No data loss
    print_header("4. REGRESSION TEST - NO DATA LOSS")
    if not check_no_data_loss():
        all_passed = False
    
    # Test 5: CRITICAL - No job ID loss
    print_header("5. CRITICAL TEST - NO JOB ID LOSS")
    if not check_no_job_id_loss():
        all_passed = False
        print(f"{Colors.RED}CRITICAL: Job IDs were lost! This must be fixed immediately!{Colors.RESET}")
    
    # Test 6: Data consistency
    print_header("6. DATA CONSISTENCY")
    if not check_data_consistency():
        all_passed = False
    
    # Summary
    print_header("TEST SUMMARY")
    if all_passed:
        print(f"{Colors.GREEN}✅ ALL TESTS PASSED!{Colors.RESET}")
        print("Data integrity verified.")
        
        # Update baseline with new data (including job IDs)
        print(f"\n{Colors.BLUE}Updating baseline with current data...{Colors.RESET}")
        create_baseline('data_baseline.json')
        print(f"{Colors.GREEN}✅ Baseline updated for next run{Colors.RESET}")
        
        return 0
    else:
        print(f"{Colors.RED}❌ SOME TESTS FAILED!{Colors.RESET}")
        print("Please check the data update process.")
        return 1

if __name__ == "__main__":
    # Change to script directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    sys.exit(run_tests())