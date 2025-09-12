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
        date_columns = ['DatePosted', 'date_posted', 'PositionOpenDate', 'ApplicationCloseDate']
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
    baseline_file = '../data/data_baseline.json'
    
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

def create_baseline(filepath):
    """Create baseline snapshot of current data"""
    baseline = {
        'created_at': datetime.now().isoformat(),
        'row_counts': {}
    }
    
    # Get all parquet files
    data_dir = '../data'
    for filename in os.listdir(data_dir):
        if filename.endswith('.parquet'):
            try:
                df = pd.read_parquet(os.path.join(data_dir, filename))
                baseline['row_counts'][filename] = len(df)
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
        if not current_ids and 'ControlNumber' in current_2025.columns:
            current_ids = set(current_2025['ControlNumber'].unique())
        if not historical_ids and 'ControlNumber' in historical_2025.columns:
            historical_ids = set(historical_2025['ControlNumber'].unique())
        
        if current_ids and historical_ids:
            missing_in_historical = current_ids - historical_ids
            if len(missing_in_historical) > 100:  # Allow some lag
                print(f"{Colors.YELLOW}⚠️  WARN{Colors.RESET} {len(missing_in_historical)} current jobs not in historical")
            else:
                print(f"{Colors.GREEN}✅ PASS{Colors.RESET} Current/historical data consistency OK")
        else:
            print(f"{Colors.YELLOW}⚠️  WARN{Colors.RESET} Could not check ID consistency")
        
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
        ('current_jobs_2024.parquet', 100, ['PositionTitle', 'PositionLocation'], 'Current jobs 2024'),
        ('current_jobs_2025.parquet', 100, ['PositionTitle', 'PositionLocation'], 'Current jobs 2025')
    ]
    
    for filename, min_rows, cols, desc in current_files:
        if not check_parquet_file(f'../data/{filename}', min_rows, cols, desc):
            all_passed = False
    
    # Test 2: Historical data files exist
    print_header("2. HISTORICAL DATA FILES")
    
    for year in range(2013, 2026):
        filename = f'historical_jobs_{year}.parquet'
        min_rows = 10 if year < 2017 else 1000  # Early years have less data
        if not check_parquet_file(f'../data/{filename}', min_rows, 
                                   ['PositionTitle'], f'Historical jobs {year}'):
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
    
    # Test 5: Data consistency
    print_header("5. DATA CONSISTENCY")
    if not check_data_consistency():
        all_passed = False
    
    # Summary
    print_header("TEST SUMMARY")
    if all_passed:
        print(f"{Colors.GREEN}✅ ALL TESTS PASSED!{Colors.RESET}")
        print("Data integrity verified.")
        return 0
    else:
        print(f"{Colors.RED}❌ SOME TESTS FAILED!{Colors.RESET}")
        print("Please check the data update process.")
        return 1

if __name__ == "__main__":
    # Change to script directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    sys.exit(run_tests())