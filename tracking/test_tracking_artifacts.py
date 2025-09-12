#!/usr/bin/env python3
"""
Test script to verify tracking site artifacts are generated correctly.
Run this after making changes to ensure you haven't broken the site.

Usage: python test_tracking_artifacts.py
"""

import os
import json
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

def check_json_array(filepath, min_length, description):
    """Check if a JSON file contains an array with minimum length"""
    if not os.path.exists(filepath):
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} {description} - File not found")
        return False
    
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} {description} - Not an array")
            return False
            
        if len(data) < min_length:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} {description}")
            print(f"     Expected at least {min_length} items, found {len(data)}")
            return False
        else:
            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} {description} ({len(data)} items)")
            return True
            
    except json.JSONDecodeError as e:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} {description} - Invalid JSON")
        print(f"     Error: {e}")
        return False

def check_department_files():
    """Check department files exist and are valid"""
    dept_dir = 'public/data/departments'
    if not os.path.exists(dept_dir):
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Department directory missing")
        return False
    
    dept_files = [f for f in os.listdir(dept_dir) if f.endswith('.json')]
    
    if len(dept_files) < 20:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Too few department files ({len(dept_files)})")
        return False
    
    print(f"{Colors.GREEN}✅ PASS{Colors.RESET} Department files ({len(dept_files)} files)")
    
    # Check a sample department file structure
    if dept_files:
        sample_file = os.path.join(dept_dir, dept_files[0])
        return check_json_array(sample_file, 1, f"  Sample dept file structure ({dept_files[0]})")
    
    return True

def check_raw_jobs_files():
    """Check raw jobs files exist and are valid"""
    raw_dir = 'public/data/raw_jobs'
    if not os.path.exists(raw_dir):
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Raw jobs directory missing")
        return False
    
    raw_files = [f for f in os.listdir(raw_dir) if f.endswith('.json')]
    
    if len(raw_files) < 20:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Too few raw jobs files ({len(raw_files)})")
        return False
    
    print(f"{Colors.GREEN}✅ PASS{Colors.RESET} Raw jobs files ({len(raw_files)} files)")
    
    # Check a sample raw jobs file structure
    if raw_files:
        sample_file = os.path.join(raw_dir, raw_files[0])
        try:
            with open(sample_file, 'r') as f:
                data = json.load(f)
            if data and isinstance(data, list) and len(data) > 0:
                # Check first job has required fields
                job = data[0]
                required_fields = ['control_number', 'year', 'position_title', 'usajobs_url']
                missing = [f for f in required_fields if f not in job]
                if missing:
                    print(f"{Colors.YELLOW}⚠️  WARN{Colors.RESET} Raw job missing fields: {missing}")
                else:
                    print(f"{Colors.GREEN}✅ PASS{Colors.RESET}   Raw job structure valid")
        except Exception as e:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET}   Raw job file invalid: {e}")
            return False
    
    return True

def check_no_data_loss():
    """Check that we haven't lost any important data"""
    baseline_file = 'test_baseline.json'
    
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
    
    # Check departments haven't disappeared
    try:
        with open('public/data/department_metadata.json', 'r') as f:
            current_depts = json.load(f)
        
        current_dept_names = {d['department'] for d in current_depts}
        baseline_dept_names = set(baseline['departments'])
        
        missing_depts = baseline_dept_names - current_dept_names
        if missing_depts:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} Missing departments: {missing_depts}")
            all_good = False
        else:
            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} All baseline departments present ({len(current_dept_names)})")
        
        # Check appointment types haven't disappeared
        with open('public/data/job_listings_summary.json', 'r') as f:
            summary_data = json.load(f)
        
        current_appt_types = {row['Appointment_Type'] for row in summary_data}
        baseline_appt_types = set(baseline['appointment_types'])
        
        missing_appt_types = baseline_appt_types - current_appt_types
        if missing_appt_types:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} Missing appointment types: {missing_appt_types}")
            all_good = False
        else:
            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} All baseline appointment types present ({len(current_appt_types)})")
        
        # Check key statistics haven't decreased AT ALL - should only increase!
        with open('public/data/job_listings_stats.json', 'r') as f:
            current_stats = json.load(f)
        
        # NO decreases allowed - this is historical data
        for year_type in ['previous_year', 'current_year']:
            baseline_total = baseline['totals'].get(year_type, 0)
            current_total = current_stats['totals'].get(year_type, 0)
            
            if baseline_total > 0 and current_total < baseline_total:
                print(f"{Colors.RED}❌ FAIL{Colors.RESET} {year_type} total DECREASED: {baseline_total} → {current_total}")
                print(f"     This should never happen! Historical data only grows!")
                all_good = False
            elif current_total > baseline_total:
                print(f"{Colors.GREEN}✅ PASS{Colors.RESET} {year_type} total increased: {baseline_total} → {current_total} (+{current_total - baseline_total})")
            else:
                print(f"{Colors.GREEN}✅ PASS{Colors.RESET} {year_type} total unchanged: {current_total}")
        
    except Exception as e:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Error checking data loss: {e}")
        all_good = False
    
    return all_good

def create_baseline(filepath):
    """Create a baseline snapshot of current data"""
    baseline = {
        'created_at': datetime.now().isoformat(),
        'departments': [],
        'appointment_types': [],
        'totals': {},
        'department_totals': {}
    }
    
    try:
        # Capture department names
        with open('public/data/department_metadata.json', 'r') as f:
            dept_meta = json.load(f)
        baseline['departments'] = [d['department'] for d in dept_meta]
        
        # Capture appointment types
        with open('public/data/job_listings_summary.json', 'r') as f:
            summary_data = json.load(f)
        baseline['appointment_types'] = list({row['Appointment_Type'] for row in summary_data})
        
        # Capture totals
        with open('public/data/job_listings_stats.json', 'r') as f:
            stats = json.load(f)
        baseline['totals'] = stats['totals']
        
        # Capture per-department totals
        dept_totals = {}
        for dept in baseline['departments']:
            dept_filename = f"dept_{dept.lower().replace(' ', '_').replace(',', '')}.json"
            dept_path = f"public/data/departments/{dept_filename}"
            if os.path.exists(dept_path):
                with open(dept_path, 'r') as f:
                    dept_data = json.load(f)
                total_2024 = sum(row.get('listings2024Value', 0) for row in dept_data)
                total_2025 = sum(row.get('listings2025Value', 0) for row in dept_data)
                dept_totals[dept] = {'2024': total_2024, '2025': total_2025}
        baseline['department_totals'] = dept_totals
        
        # Save baseline
        with open(filepath, 'w') as f:
            json.dump(baseline, f, indent=2)
            
    except Exception as e:
        print(f"{Colors.RED}Error creating baseline: {e}{Colors.RESET}")

def run_tests():
    """Run all artifact tests"""
    print(f"{Colors.BLUE}TRACKING SITE ARTIFACT TESTS{Colors.RESET}")
    print(f"{Colors.BLUE}Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Colors.RESET}")
    
    all_passed = True
    
    # Test 1: Core data files exist
    print_header("1. CORE DATA FILES")
    
    files_to_check = [
        ('public/data/job_listings_summary.json', 'Main job listings data'),
        ('public/data/job_listings_stats.json', 'Statistics file'),
        ('public/data/department_metadata.json', 'Department metadata'),
        ('public/occupation_series_from_data.json', 'Occupation series mapping'),
        ('public/index.html', 'Main HTML page'),
        ('public/jobs_app.js', 'JavaScript application')
    ]
    
    for filepath, desc in files_to_check:
        if not check_file_exists(filepath, desc):
            all_passed = False
    
    # Test 2: JSON structure validation
    print_header("2. JSON STRUCTURE VALIDATION")
    
    # Check stats file structure
    stats_keys = ['generated_at', 'date_range', 'totals', 'unique_counts']
    if not check_json_structure('public/data/job_listings_stats.json', stats_keys, 'Stats file structure'):
        all_passed = False
    
    # Check summary data is an array
    if not check_json_array('public/data/job_listings_summary.json', 100, 'Summary data array'):
        all_passed = False
    
    # Check department metadata
    if not check_json_array('public/data/department_metadata.json', 20, 'Department metadata array'):
        all_passed = False
    
    # Test 3: Department files
    print_header("3. DEPARTMENT FILES")
    if not check_department_files():
        all_passed = False
    
    # Test 4: Raw jobs files
    print_header("4. RAW JOBS FILES")
    if not check_raw_jobs_files():
        all_passed = False
    
    # Test 5: Data consistency checks
    print_header("5. DATA CONSISTENCY")
    
    try:
        # Check that department count matches between metadata and directories
        with open('public/data/department_metadata.json', 'r') as f:
            dept_meta = json.load(f)
        
        dept_files = [f for f in os.listdir('public/data/departments') if f.endswith('.json')]
        raw_files = [f for f in os.listdir('public/data/raw_jobs') if f.endswith('.json')]
        
        if len(dept_meta) == len(dept_files) == len(raw_files):
            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} File counts match ({len(dept_meta)} departments)")
        else:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} File count mismatch")
            print(f"     Metadata: {len(dept_meta)}, Dept files: {len(dept_files)}, Raw files: {len(raw_files)}")
            all_passed = False
            
    except Exception as e:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Consistency check failed: {e}")
        all_passed = False
    
    # Test 6: Regression testing - ensure no data loss
    print_header("6. REGRESSION TESTS - NO DATA LOSS")
    if not check_no_data_loss():
        all_passed = False
    
    # Test 7: Critical field name checks (what JS expects)
    print_header("7. CRITICAL FIELD NAMES (JavaScript Compatibility)")
    
    try:
        with open('public/data/job_listings_summary.json', 'r') as f:
            summary_sample = json.load(f)[0]  # Check first row
        
        # These exact field names are required by jobs_app.js
        required_fields = [
            'Department', 'Agency', 'Appointment_Type', 'Occupation_Series',
            'listings2024Value', 'listings2025Value', 'percentageValue'
        ]
        
        missing_fields = [field for field in required_fields if field not in summary_sample]
        if missing_fields:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} Missing required fields: {missing_fields}")
            print("     JavaScript expects these exact field names!")
            all_passed = False
        else:
            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} All required fields present for JavaScript")
            
    except Exception as e:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Could not check field names: {e}")
        all_passed = False
    
    # Test 8: Specific content checks
    print_header("8. SPECIFIC CONTENT CHECKS")
    
    # Check that Internships appointment type exists
    try:
        with open('public/data/job_listings_summary.json', 'r') as f:
            summary_data = json.load(f)
        
        appt_types = {row['Appointment_Type'] for row in summary_data}
        if 'Internships' in appt_types:
            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} 'Internships' appointment type exists")
        else:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} 'Internships' appointment type missing!")
            all_passed = False
            
        # Check Air Force department exists
        depts = {row['Department'] for row in summary_data}
        if 'Department of the Air Force' in depts:
            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} 'Department of the Air Force' exists")
        else:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} 'Department of the Air Force' missing!")
            all_passed = False
            
    except Exception as e:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Content check failed: {e}")
        all_passed = False
    
    # Test 9: Department filename matching
    print_header("9. DEPARTMENT FILENAME MATCHING")
    
    try:
        with open('public/data/department_metadata.json', 'r') as f:
            dept_metadata = json.load(f)
        
        mismatches = []
        for dept in dept_metadata:
            expected_filename = dept['filename']
            expected_raw_filename = dept['raw_jobs_filename']
            
            # Check if files actually exist
            if not os.path.exists(f"public/data/departments/{expected_filename}"):
                mismatches.append(f"Missing dept file: {expected_filename}")
            if not os.path.exists(f"public/data/raw_jobs/{expected_raw_filename}"):
                mismatches.append(f"Missing raw file: {expected_raw_filename}")
        
        if mismatches:
            print(f"{Colors.RED}❌ FAIL{Colors.RESET} File mismatches found:")
            for mismatch in mismatches[:5]:  # Show first 5
                print(f"     {mismatch}")
            all_passed = False
        else:
            print(f"{Colors.GREEN}✅ PASS{Colors.RESET} All department files match metadata")
            
    except Exception as e:
        print(f"{Colors.RED}❌ FAIL{Colors.RESET} Could not check filename matching: {e}")
        all_passed = False
    
    # Summary
    print_header("TEST SUMMARY")
    if all_passed:
        print(f"{Colors.GREEN}✅ ALL TESTS PASSED!{Colors.RESET}")
        print("Your tracking site artifacts are properly generated.")
        return 0
    else:
        print(f"{Colors.RED}❌ SOME TESTS FAILED!{Colors.RESET}")
        print("Please fix the issues before deploying.")
        return 1

if __name__ == "__main__":
    sys.exit(run_tests())