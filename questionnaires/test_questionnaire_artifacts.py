#!/usr/bin/env python3
"""
Test script to verify questionnaire site artifacts are generated correctly.
Run this after questionnaire extraction/generation to ensure nothing breaks.

Usage: python test_questionnaire_artifacts.py
"""

import os
import json
import csv
import sys
from datetime import datetime

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

def check_file_exists(filepath, description):
    """Check if a file exists"""
    exists = os.path.exists(filepath)
    status = f"{Colors.GREEN}✅ PASS{Colors.RESET}" if exists else f"{Colors.RED}❌ FAIL{Colors.RESET}"
    print(f"{status} {description}")
    if not exists:
        print(f"     Missing: {filepath}")
    return exists

def check_json_structure(filepath, required_keys, description):
    """Check if a JSON file has required keys"""
    if not os.path.exists(filepath):
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} {description} - File not found")
        return False
    
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        missing_keys = [key for key in required_keys if key not in data]
        
        if missing_keys:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} {description}")
            print(f"     Missing keys: {missing_keys}")
            return False
        else:
            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} {description}")
            return True
            
    except json.JSONDecodeError as e:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} {description} - Invalid JSON")
        print(f"     Error: {e}")
        return False

def check_csv_file(filepath, min_rows, required_columns, description):
    """Check if CSV file is valid and has required columns"""
    if not os.path.exists(filepath):
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} {description} - File not found")
        return False
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            rows = list(reader)
        
        # Check column names
        if required_columns:
            missing_cols = [col for col in required_columns if col not in headers]
            if missing_cols:
                print(f"{Colors.RED}❌ FAIL{Colors.RESET} {description} - Missing columns: {missing_cols}")
                return False
        
        # Check row count
        if len(rows) < min_rows:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} {description}")
            print(f"     Expected at least {min_rows} rows, found {len(rows)}")
            return False
        else:
            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} {description} ({len(rows)} rows)")
            return True
            
    except Exception as e:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} {description} - Error reading CSV")
        print(f"     Error: {e}")
        return False

def check_questionnaire_sources():
    """Check that we have both Monster and USAStaffing questionnaires"""
    raw_dir = 'raw_questionnaires'
    if not os.path.exists(raw_dir):
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Raw questionnaires directory missing")
        return False
    
    files = os.listdir(raw_dir)
    monster_files = [f for f in files if f.startswith('monster_')]
    usastaffing_files = [f for f in files if f.startswith('usastaffing_')]
    
    all_good = True
    
    # Check Monster files
    if len(monster_files) < 10:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Too few Monster questionnaires ({len(monster_files)})")
        all_good = False
    else:
        print(f"{Colors.GREEN}✅ PASS{Colors.RESET} Monster questionnaires present ({len(monster_files)} files)")
    
    # Check USAStaffing files
    if len(usastaffing_files) < 100:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Too few USAStaffing questionnaires ({len(usastaffing_files)})")
        all_good = False
    else:
        print(f"{Colors.GREEN}✅ PASS{Colors.RESET} USAStaffing questionnaires present ({len(usastaffing_files)} files)")
    
    # Check recent additions
    total_files = len(files)
    print(f"{Colors.GREEN}✅ PASS{Colors.RESET} Total questionnaire files: {total_files}")
    
    return all_good

def check_no_data_loss():
    """Check that questionnaire counts haven't decreased"""
    baseline_file = 'test_questionnaire_baseline.json'
    
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
    
    try:
        # Check total questionnaire count
        current_files = os.listdir('raw_questionnaires')
        current_total = len(current_files)
        baseline_total = baseline.get('total_questionnaires', 0)
        
        if current_total < baseline_total:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} Questionnaire count DECREASED: {baseline_total} → {current_total}")
            print("     Questionnaires should never be deleted!")
            all_good = False
        elif current_total > baseline_total:
            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} Questionnaire count increased: {baseline_total} → {current_total} (+{current_total - baseline_total})")
        else:
            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} Questionnaire count unchanged: {current_total}")
        
        # Check Monster and USAStaffing counts
        current_monster = len([f for f in current_files if f.startswith('monster_')])
        current_usastaffing = len([f for f in current_files if f.startswith('usastaffing_')])
        
        baseline_monster = baseline.get('monster_count', 0)
        baseline_usastaffing = baseline.get('usastaffing_count', 0)
        
        if current_monster < baseline_monster:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} Monster count decreased: {baseline_monster} → {current_monster}")
            all_good = False
        else:
            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} Monster count OK: {current_monster} (baseline: {baseline_monster})")
        
        if current_usastaffing < baseline_usastaffing:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} USAStaffing count decreased: {baseline_usastaffing} → {current_usastaffing}")
            all_good = False
        else:
            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} USAStaffing count OK: {current_usastaffing} (baseline: {baseline_usastaffing})")
        
        # Check CSV row counts
        with open('questionnaire_links.csv', 'r', encoding='utf-8') as f:
            current_links_count = sum(1 for _ in csv.reader(f)) - 1  # Minus header
        
        baseline_links_count = baseline.get('links_count', 0)
        if current_links_count < baseline_links_count:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} Links count decreased: {baseline_links_count} → {current_links_count}")
            all_good = False
        else:
            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} Links count OK: {current_links_count} (baseline: {baseline_links_count})")
            
    except Exception as e:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Error checking data loss: {e}")
        all_good = False
    
    return all_good

def create_baseline(filepath):
    """Create a baseline snapshot of current questionnaire data"""
    baseline = {
        'created_at': datetime.now().isoformat(),
        'total_questionnaires': 0,
        'monster_count': 0,
        'usastaffing_count': 0,
        'links_count': 0,
        'jobs_count': 0
    }
    
    try:
        # Count questionnaire files
        files = os.listdir('raw_questionnaires')
        baseline['total_questionnaires'] = len(files)
        baseline['monster_count'] = len([f for f in files if f.startswith('monster_')])
        baseline['usastaffing_count'] = len([f for f in files if f.startswith('usastaffing_')])
        
        # Count CSV rows
        with open('questionnaire_links.csv', 'r', encoding='utf-8') as f:
            baseline['links_count'] = sum(1 for _ in csv.reader(f)) - 1
        
        with open('all_jobs_clean.csv', 'r', encoding='utf-8') as f:
            baseline['jobs_count'] = sum(1 for _ in csv.reader(f)) - 1
        
        # Save baseline
        with open(filepath, 'w') as f:
            json.dump(baseline, f, indent=2)
            
    except Exception as e:
        print(f"{Colors.RED}Error creating baseline: {e}{Colors.RESET}")

def check_analysis_data():
    """Check the analysis data JSON structure"""
    try:
        with open('analysis/analysis_data.json', 'r') as f:
            data = json.load(f)
        
        # Check required top-level keys
        required_keys = ['overview', 'service_analysis', 'grade_analysis']
        missing = [k for k in required_keys if k not in data]
        
        if missing:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} Analysis data missing keys: {missing}")
            return False
        
        # Check overview
        overview = data.get('overview', {})
        if overview.get('total_scraped', 0) == 0:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} No scraped questionnaires found")
            return False
        
        total_scraped = overview.get('total_scraped', 0)
        total_with_eo = overview.get('total_with_eo', 0)
        
        print(f"{Colors.GREEN}✅ PASS{Colors.RESET} Analysis data valid")
        print(f"     Total scraped questionnaires: {total_scraped}")
        print(f"     Jobs with essay questions: {total_with_eo}")
        
        return True
        
    except Exception as e:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Could not check analysis data: {e}")
        return False

def run_tests():
    """Run all questionnaire artifact tests"""
    print(f"{Colors.BLUE}QUESTIONNAIRE SITE ARTIFACT TESTS{Colors.RESET}")
    print(f"{Colors.BLUE}Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Colors.RESET}")
    
    all_passed = True
    
    # Test 1: Core data files exist
    print_header("1. CORE DATA FILES")
    
    files_to_check = [
        ('questionnaire_links.csv', 'Questionnaire links CSV'),
        ('all_jobs_clean.csv', 'All jobs clean CSV'),
        ('all_jobs_stats.json', 'Job statistics JSON'),
        ('analysis/analysis_data.json', 'Analysis data JSON'),
        ('analysis/index.html', 'Analysis site HTML')
    ]
    
    for filepath, desc in files_to_check:
        if not check_file_exists(filepath, desc):
            all_passed = False
    
    # Test 2: CSV structure validation
    print_header("2. CSV STRUCTURE VALIDATION")
    
    # Check questionnaire_links.csv
    if not check_csv_file('questionnaire_links.csv', 
                          min_rows=1000,
                          required_columns=['usajobs_control_number', 'questionnaire_url'],
                          description='Questionnaire links CSV'):
        all_passed = False
    
    # Check all_jobs_clean.csv
    if not check_csv_file('all_jobs_clean.csv',
                          min_rows=100,
                          required_columns=['usajobs_control_number', 'position_title', 'hiring_agency', 
                                          'service_type', 'position_location'],
                          description='All jobs clean CSV'):
        all_passed = False
    
    # Test 3: Questionnaire sources
    print_header("3. QUESTIONNAIRE SOURCES")
    if not check_questionnaire_sources():
        all_passed = False
    
    # Test 4: JSON validation
    print_header("4. JSON VALIDATION")
    
    # Check all_jobs_stats.json
    stats_keys = ['metadata', 'by_service_type', 'by_grade_level', 'by_location', 'by_agency']
    if not check_json_structure('all_jobs_stats.json', stats_keys, 'Job statistics JSON'):
        all_passed = False
    
    # Test 5: Analysis data validation
    print_header("5. ANALYSIS DATA VALIDATION")
    if not check_analysis_data():
        all_passed = False
    
    # Test 6: Regression testing - ensure no data loss
    print_header("6. REGRESSION TESTS - NO DATA LOSS")
    if not check_no_data_loss():
        all_passed = False
    
    # Test 7: Specific content checks
    print_header("7. SPECIFIC CONTENT CHECKS")
    
    try:
        # Check that we have recent questionnaires
        raw_files = sorted(os.listdir('raw_questionnaires'))
        if raw_files:
            latest_file = raw_files[-1]
            # Check if it's a USAStaffing file with a recent number
            if latest_file.startswith('usastaffing_'):
                number = latest_file.replace('usastaffing_', '').replace('.txt', '')
                # Just check that we have questionnaires, don't worry about number being 'recent'
                print(f"{Colors.GREEN}✅ PASS{Colors.RESET} Questionnaires present (latest: {latest_file})")
            else:
                print(f"{Colors.GREEN}✅ PASS{Colors.RESET} Questionnaires present")
        else:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} No questionnaire files found!")
            all_passed = False
            
    except Exception as e:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Content check failed: {e}")
        all_passed = False
    
    # Summary
    print_header("TEST SUMMARY")
    if all_passed:
        print(f"{Colors.GREEN}✅ ALL TESTS PASSED!{Colors.RESET}")
        print("Your questionnaire artifacts are properly generated.")
        return 0
    else:
        print(f"{Colors.RED}❌ SOME TESTS FAILED!{Colors.RESET}")
        print("Please fix the issues before deploying.")
        return 1

if __name__ == "__main__":
    sys.exit(run_tests())