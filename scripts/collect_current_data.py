#!/usr/bin/env python3
"""
USAJobs Current API Data Collector

Fetches current job postings from the USAJobs Search API
using occupational series to work around the 10,000 result limit.
Gets ALL jobs by fetching each occupational series separately.

Usage:
    python collect_current_data.py --data-dir data/
    python collect_current_data.py --data-dir data/ --days-posted 7
    python collect_current_data.py --data-dir data/ --test  # Only fetch first 5 series
"""

import argparse
import json
import time
import requests
from typing import List, Dict, Optional
import pandas as pd
from datetime import datetime
import os
from tqdm import tqdm
from html import unescape
import re
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Base URL for USAJobs API
BASE_URL = "https://data.usajobs.gov/api/Search"

# We will fetch hiring path mappings dynamically from the API


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch ALL current USAJobs data by occupational series")
    parser.add_argument("--days-posted", type=int, help="Jobs posted within N days (optional)", default=None)
    parser.add_argument("--all", action="store_true", help="Fetch all current jobs (no date filter)")
    parser.add_argument("--data-dir", help="Directory for parquet files (defaults to data/)", default="data")
    parser.add_argument("--test", action="store_true", help="Test mode - only fetch first 5 series")
    return parser.parse_args()


def get_api_headers() -> Dict[str, str]:
    """Get API headers with authorization"""
    api_key = os.getenv("USAJOBS_API_TOKEN")
    
    if not api_key:
        raise ValueError("API key required. Set USAJOBS_API_TOKEN environment variable")
    
    return {
        "Host": "data.usajobs.gov",
        "Authorization-Key": api_key
    }


def clean_text(text: Optional[str]) -> Optional[str]:
    """Clean HTML tags and entities from text"""
    if not text:
        return None
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Unescape HTML entities
    text = unescape(text)
    # Clean up whitespace
    text = ' '.join(text.split())
    
    return text if text else None


def flatten_current_job(job_item: dict, appointment_type_map: dict, hiring_path_map: dict) -> dict:
    """
    Keep all fields from current API job, plus add normalized fields to match historical API.
    """
    import json
    
    # Start with the entire job item to keep everything
    flattened = job_item.copy()
    
    # Convert MatchedObjectDescriptor dict to JSON string for Parquet compatibility
    if "MatchedObjectDescriptor" in flattened and isinstance(flattened["MatchedObjectDescriptor"], dict):
        flattened["MatchedObjectDescriptor"] = json.dumps(flattened["MatchedObjectDescriptor"])
    
    job = job_item.get("MatchedObjectDescriptor", {})
    user_area = job.get("UserArea", {}).get("Details", {})
    
    # Add normalized field names that match historical API (rationalize common fields)
    # Extract numeric control number from PositionURI (e.g., "https://www.usajobs.gov:443/job/686668700" -> 686668700)
    position_uri = job.get("PositionURI", "")
    numeric_control_number = None
    if position_uri and "/job/" in position_uri:
        try:
            numeric_control_number = int(position_uri.split("/job/")[-1])
        except (ValueError, IndexError):
            pass
    
    flattened["usajobsControlNumber"] = numeric_control_number
    flattened["announcementNumber"] = job.get("PositionID")  # This is actually the announcement number 
    flattened["hiringAgencyName"] = job.get("DepartmentName")
    flattened["hiringAgencyCode"] = job.get("OrganizationCodes", "").split(".")[0] if job.get("OrganizationCodes") else None
    flattened["hiringDepartmentName"] = job.get("DepartmentName")
    flattened["hiringSubelementName"] = job.get("SubAgency")
    flattened["positionTitle"] = job.get("PositionTitle")
    flattened["serviceType"] = job.get("ServiceType")
    flattened["supervisoryStatus"] = job.get("SupervisoryStatus")
    flattened["travelRequirement"] = job.get("TravelCode")
    
    # Convert teleworkEligible boolean to string format to match historical data
    telework_eligible = user_area.get("TeleworkEligible")
    if isinstance(telework_eligible, bool):
        flattened["teleworkEligible"] = "Y" if telework_eligible else "N"
    else:
        flattened["teleworkEligible"] = telework_eligible
        
    flattened["securityClearance"] = user_area.get("SecurityClearance")
    flattened["drugTestRequired"] = user_area.get("DrugTestRequired")
    flattened["relocationExpensesReimbursed"] = user_area.get("Relocation")
    flattened["totalOpenings"] = user_area.get("TotalOpenings")
    flattened["positionOpenDate"] = job.get("PositionStartDate")
    flattened["positionCloseDate"] = job.get("PositionEndDate")
    flattened["positionExpireDate"] = job.get("PositionExpireDate")
    
    # Extract some complex fields
    grades = job.get("JobGrade", [])
    flattened["minimumGrade"] = grades[0].get("Code") if grades else None
    flattened["maximumGrade"] = grades[-1].get("Code") if len(grades) > 1 else flattened["minimumGrade"]
    
    # Convert salary fields to float to match historical data format
    remuneration = job.get("PositionRemuneration", [{}])
    min_salary_str = remuneration[0].get("MinimumRange") if remuneration else None
    max_salary_str = remuneration[0].get("MaximumRange") if remuneration else None
    
    try:
        flattened["minimumSalary"] = float(min_salary_str) if min_salary_str else None
    except (ValueError, TypeError):
        flattened["minimumSalary"] = None
        
    try:
        flattened["maximumSalary"] = float(max_salary_str) if max_salary_str else None
    except (ValueError, TypeError):
        flattened["maximumSalary"] = None
    
    # Extract JobCategories to match historical data format: [{"series": "XXXX"}]
    # Historical data only has series codes, not names
    job_categories = job.get("JobCategory", [])
    if job_categories:
        categories_list = []
        for category in job_categories:
            if isinstance(category, dict) and category.get("Code"):
                categories_list.append({
                    "series": category.get("Code", "")
                })
        flattened["JobCategories"] = json.dumps(categories_list) if categories_list else None
    else:
        flattened["JobCategories"] = None
    
    # Extract HiringPaths to match historical data format: [{"hiringPath": "Description"}]
    hiring_path_codes = user_area.get("HiringPath", [])
    if hiring_path_codes:
        hiring_paths = []
        for code in hiring_path_codes:
            # Use dynamic mapping, fall back to code if not found
            path_name = hiring_path_map.get(code, code)
            hiring_paths.append({"hiringPath": path_name})
        flattened["HiringPaths"] = json.dumps(hiring_paths) if hiring_paths else None
    else:
        flattened["HiringPaths"] = None
    
    # Extract appointment type using the provided mapping
    appointment_types = job.get("PositionOfferingType", [])
    if appointment_types and appointment_types[0]:
        # Try to map the code to a name, fall back to code if not found
        code = appointment_types[0].get("Code", "")
        flattened["appointmentType"] = appointment_type_map.get(code, code)
    else:
        flattened["appointmentType"] = ""
    
    return flattened


def load_existing_jobs(parquet_path: str) -> set:
    """Load existing control numbers from parquet file"""
    if os.path.exists(parquet_path):
        try:
            df = pd.read_parquet(parquet_path)
            return set(df['usajobsControlNumber'].dropna().astype(str))
        except Exception as e:
            print(f"⚠️ Warning: Could not read existing parquet file {parquet_path}: {e}")
            return set()
    return set()


def fetch_jobs_page(params: Dict, headers: Dict, page: int = 1) -> Optional[Dict]:
    """Fetch a single page of job results"""
    params_copy = params.copy()
    params_copy['Page'] = page
    
    try:
        response = requests.get(BASE_URL, headers=headers, params=params_copy, timeout=120)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page {page}: {e}")
        return None


def fetch_position_offering_types() -> Dict[str, str]:
    """Fetch position offering type codes and their names from the API"""
    url = "https://data.usajobs.gov/api/codelist/positionofferingtypes"
    
    try:
        print(f"🔍 Fetching position offering types from: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Extract code -> name mapping
        type_map = {}
        
        if 'CodeList' in data:
            for item in data['CodeList']:
                if 'ValidValue' in item:
                    for value in item['ValidValue']:
                        code = value.get('Code')
                        name = value.get('Value', '')
                        is_disabled = value.get('IsDisabled', 'No')
                        
                        # Only include active types
                        if code and is_disabled == 'No':
                            type_map[code] = name
        
        print(f"   Found {len(type_map)} active position offering types")
        return type_map
    except Exception as e:
        print(f"❌ Error fetching position offering types: {e}")
        raise


def fetch_hiring_paths() -> Dict[str, str]:
    """Fetch hiring path codes and their names from the API"""
    url = "https://data.usajobs.gov/api/codelist/hiringpaths"
    
    try:
        print(f"🔍 Fetching hiring paths from: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Extract code -> name mapping
        path_map = {}
        
        if 'CodeList' in data:
            for item in data['CodeList']:
                if 'ValidValue' in item:
                    for value in item['ValidValue']:
                        code = value.get('Code')
                        name = value.get('Value', '')
                        
                        if code:
                            path_map[code] = name
        
        print(f"   Found {len(path_map)} hiring paths")
        return path_map
    except Exception as e:
        print(f"❌ Error fetching hiring paths: {e}")
        raise


def fetch_occupational_series() -> List[Dict[str, str]]:
    """Fetch list of all occupational series from the API"""
    # According to docs, code list endpoints don't require authentication
    url = "https://data.usajobs.gov/api/codelist/occupationalseries"
    
    try:
        print(f"🔍 Fetching occupational series from: {url}")
        response = requests.get(url, timeout=30)
        print(f"   Status code: {response.status_code}")
        response.raise_for_status()
        data = response.json()
        
        # Extract occupational series codes
        series_list = []
        
        if 'CodeList' in data:
            code_list = data['CodeList']
            print(f"   Found {len(code_list)} items in CodeList")
            
            for item in code_list:
                if isinstance(item, dict) and 'ValidValue' in item:
                    valid_values = item['ValidValue']
                    print(f"   Found {len(valid_values)} occupational series")
                    
                    for value in valid_values:
                        if isinstance(value, dict):
                            code = value.get('Code')
                            name = value.get('Value', '')
                            is_disabled = value.get('IsDisabled', 'No')
                            
                            # Only include active series
                            if code and is_disabled == 'No':
                                series_list.append({
                                    'code': code,
                                    'name': name
                                })
        
        print(f"   Extracted {len(series_list)} active occupational series")
        return series_list
    except Exception as e:
        print(f"❌ Error fetching occupational series: {e}")
        if 'response' in locals():
            print(f"   Response text: {response.text[:500]}")
        return []


def fetch_all_jobs(params: Dict, headers: Dict, appointment_type_map: Dict[str, str], hiring_path_map: Dict[str, str], max_results: int = 10000) -> tuple[List[Dict], List[Dict]]:
    """Fetch all jobs with pagination up to max_results. Returns (raw_jobs, flattened_jobs)"""
    raw_jobs = []
    flattened_jobs = []
    page = 1
    total_count = None
    
    # Create progress bar
    progress_bar = tqdm(desc="Fetching pages", unit="page")
    
    while True:
        progress_bar.set_description(f"Fetching page {page}")
        data = fetch_jobs_page(params, headers, page)
        
        if not data:
            progress_bar.write("Failed to fetch data, stopping.")
            break
            
        search_result = data.get("SearchResult", {})
        items = search_result.get("SearchResultItems", [])
        
        if not items:
            progress_bar.write("No more results.")
            break
        
        # Process and flatten each job
        for item in items:
            raw_jobs.append(item)
            flattened = flatten_current_job(item, appointment_type_map, hiring_path_map)
            flattened_jobs.append(flattened)
        
        # Get total count on first page
        if page == 1:
            total_count = search_result.get("SearchResultCountAll", 0)
            progress_bar.write(f"Total jobs available: {total_count}")
        
        # Check if we've reached the end or hit max_results
        if total_count and len(raw_jobs) >= total_count:
            progress_bar.write(f"Reached total count ({total_count})")
            break
        
        if len(raw_jobs) >= max_results:
            progress_bar.write(f"Reached max results limit ({max_results})")
            break
            
        page += 1
        progress_bar.update(1)
        
        # Be nice to the API
        time.sleep(0.5)
    
    progress_bar.close()
    print(f"✅ Fetched {len(raw_jobs)} total jobs across {page} pages")
    
    return raw_jobs, flattened_jobs


def save_jobs_to_parquet(jobs: List[Dict], raw_jobs: List[Dict], parquet_path: str):
    """Save jobs to parquet file, NEVER removing existing jobs.
    
    This creates a complete historical record of all jobs that were ever 'current'.
    """
    if not jobs:
        return
    
    # Add metadata to each job
    for job in jobs:
        job['inserted_at'] = datetime.now().isoformat()
        job['last_seen'] = datetime.now().isoformat()
    
    # Convert to DataFrame
    new_df = pd.DataFrame(jobs)
    
    # Ensure usajobs_control_number is string for consistency
    new_df['usajobs_control_number'] = new_df['usajobsControlNumber'].astype(str)
    
    # Load existing data if file exists
    if os.path.exists(parquet_path):
        try:
            existing_df = pd.read_parquet(parquet_path)
            initial_count = len(existing_df)
            
            # Handle column name variations
            if 'usajobsControlNumber' in existing_df.columns and 'usajobs_control_number' not in existing_df.columns:
                existing_df['usajobs_control_number'] = existing_df['usajobsControlNumber'].astype(str)
            
            # Get control numbers from both dataframes
            existing_control_numbers = set(existing_df['usajobs_control_number'].dropna().astype(str))
            new_control_numbers = set(new_df['usajobs_control_number'].dropna().astype(str))
            
            # Identify overlapping jobs
            overlapping_control_numbers = existing_control_numbers.intersection(new_control_numbers)
            
            # Update last_seen for existing jobs
            if 'last_seen' not in existing_df.columns:
                existing_df['last_seen'] = existing_df.get('inserted_at', datetime.now().isoformat())
            
            # For overlapping jobs, update last_seen but keep existing data
            if overlapping_control_numbers:
                # Remove overlapping jobs from existing (we'll add updated versions)
                mask = ~existing_df['usajobs_control_number'].isin(overlapping_control_numbers)
                existing_df = existing_df[mask]
            
            # Combine dataframes
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            
            # Verify we haven't lost any jobs
            final_count = len(combined_df)
            if final_count < initial_count:
                raise ValueError(f"DATA LOSS PREVENTED: Would have lost {initial_count - final_count} jobs. "
                               f"Initial: {initial_count}, Final: {final_count}")
            
            print(f"   💾 Updated {parquet_path}: {initial_count} → {final_count} jobs "
                  f"(+{len(new_control_numbers - existing_control_numbers)} new, "
                  f"{len(overlapping_control_numbers)} updated)")
        except Exception as e:
            print(f"⚠️ Warning: Could not read existing parquet file {parquet_path}: {e}")
            combined_df = new_df
            print(f"   💾 Created {parquet_path} with {len(combined_df)} jobs")
    else:
        combined_df = new_df
        print(f"   💾 Created {parquet_path} with {len(combined_df)} jobs")
    
    # Save to parquet
    try:
        combined_df.to_parquet(parquet_path, index=False)
    except Exception as e:
        print(f"❌ Error saving to parquet: {e}")
        raise


def get_year_from_date(date_str: Optional[str]) -> Optional[int]:
    """Extract year from date string, handling various formats"""
    if not date_str:
        return None
    
    try:
        # Handle ISO format dates (with or without timezone)
        if 'T' in date_str:
            date_str = date_str.split('T')[0]  # Take date part only
        
        # Try parsing YYYY-MM-DD format
        year = int(date_str.split('-')[0])
        return year if 2000 <= year <= 2030 else None  # Sanity check
    except (ValueError, IndexError):
        return None


def group_jobs_by_year(jobs: List[Dict], raw_jobs: List[Dict]) -> Dict[int, tuple[List[Dict], List[Dict]]]:
    """Group jobs by year based on positionOpenDate"""
    jobs_by_year = {}
    
    for i, job in enumerate(jobs):
        year = get_year_from_date(job.get("positionOpenDate"))
        if year:
            if year not in jobs_by_year:
                jobs_by_year[year] = ([], [])
            jobs_by_year[year][0].append(job)
            jobs_by_year[year][1].append(raw_jobs[i] if i < len(raw_jobs) else {})
        else:
            print(f"⚠️ Skipping job {job.get('usajobsControlNumber')} - no valid positionOpenDate")
    
    return jobs_by_year


def main():
    args = parse_args()
    
    # Validate arguments
    if args.all and args.days_posted:
        print("❌ Error: Cannot use both --all and --days-posted together")
        print("   Use --all to fetch all current jobs, or --days-posted N for recent jobs")
        return
    
    # Get API headers
    try:
        headers = get_api_headers()
    except ValueError as e:
        print(f"❌ {e}")
        return
    
    print(f"🚀 Fetching ALL current jobs from USAJobs API (by occupational series)")
    print("=" * 50)
    
    # Create data directory if it doesn't exist
    os.makedirs(args.data_dir, exist_ok=True)
    
    # First, fetch position offering types for mapping codes to names
    print("📋 Fetching position offering types...")
    appointment_type_map = fetch_position_offering_types()
    print(f"✅ Loaded {len(appointment_type_map)} position offering types")
    
    # Fetch hiring paths for mapping codes to names
    print("📋 Fetching hiring paths...")
    hiring_path_map = fetch_hiring_paths()
    print(f"✅ Loaded {len(hiring_path_map)} hiring paths")
    
    # Then fetch all occupational series
    print("📋 Fetching list of occupational series...")
    series_list = fetch_occupational_series()
    
    if not series_list:
        print("❌ No occupational series fetched")
        return
    
    print(f"✅ Found {len(series_list)} occupational series")
    
    # Sort series by code for consistent ordering
    series_list.sort(key=lambda x: x['code'])
    
    if args.test:
        series_list = series_list[:5]
        print(f"🧪 Test mode: Only processing first {len(series_list)} series")
    
    # Base search parameters
    base_params = {
        "ResultsPerPage": 500,  # Max allowed by API
        "Fields": "full",
        "SortField": "DatePosted",
        "SortDirection": "desc"
    }
    
    # Add date filter if specified
    if args.days_posted and not args.all:
        base_params["DatePosted"] = args.days_posted
        print(f"📅 Filtering to jobs posted in last {args.days_posted} days")
    
    all_raw_jobs = []
    all_flattened_jobs = []
    job_ids = set()  # Track unique jobs to avoid duplicates
    series_with_jobs = 0
    
    # Fetch jobs for each occupational series
    for i, series in enumerate(tqdm(series_list, desc="Processing occupational series", unit="series"), 1):
        tqdm.write(f"\n[{i}/{len(series_list)}] 📊 Fetching jobs for {series['code']} - {series['name'][:50]}...")
        
        params = base_params.copy()
        params['JobCategoryCode'] = series['code']
        
        try:
            series_raw_jobs, series_flattened_jobs = fetch_all_jobs(params, headers, appointment_type_map, hiring_path_map)
            
            # Add only unique jobs (some jobs may have multiple series)
            new_jobs = 0
            for j, job in enumerate(series_flattened_jobs):
                # Use usajobsControlNumber as unique identifier
                control_number = job.get('usajobsControlNumber')
                if control_number and control_number not in job_ids:
                    job_ids.add(control_number)
                    all_flattened_jobs.append(job)
                    all_raw_jobs.append(series_raw_jobs[j])
                    new_jobs += 1
            
            if new_jobs > 0:
                series_with_jobs += 1
                tqdm.write(f"   ✅ Added {new_jobs} new unique jobs (total unique: {len(all_flattened_jobs)})")
            else:
                tqdm.write(f"   ⏭️  No new unique jobs")
            
            # Be nice to the API
            if i < len(series_list):
                time.sleep(0.5)
                
        except Exception as e:
            tqdm.write(f"   ❌ Error fetching jobs: {e}")
            continue
    
    if not all_flattened_jobs:
        print("\n❌ No jobs fetched")
        return
    
    # Group jobs by year based on positionOpenDate
    print("\n📊 Grouping jobs by year based on position open date...")
    jobs_by_year = group_jobs_by_year(all_flattened_jobs, all_raw_jobs)
    
    if not jobs_by_year:
        print("❌ No jobs with valid position open dates found")
        return
    
    print(f"📅 Found jobs across {len(jobs_by_year)} years:")
    for year in sorted(jobs_by_year.keys()):
        print(f"   - {year}: {len(jobs_by_year[year][0])} jobs")
    
    # Process each year
    total_new_jobs = 0
    for year in sorted(jobs_by_year.keys()):
        year_jobs, year_raw_jobs = jobs_by_year[year]
        parquet_path = f"{args.data_dir}/current_jobs_{year}.parquet"
        
        print(f"\n📅 Processing {year} jobs...")
        print(f"💾 Parquet file: {parquet_path}")
        
        # Get existing control numbers to avoid duplicates
        existing_control_numbers = load_existing_jobs(parquet_path)
        print(f"   Found {len(existing_control_numbers)} existing current jobs")
        
        # Filter for new jobs only
        new_jobs = []
        new_raw_jobs = []
        for i, job in enumerate(year_jobs):
            usajobs_control_number = str(job.get("usajobsControlNumber", ""))
            if usajobs_control_number and usajobs_control_number not in existing_control_numbers:
                new_jobs.append(job)
                new_raw_jobs.append(year_raw_jobs[i])
        
        print(f"   📊 Found {len(new_jobs)} new jobs to add")
        
        # Save to Parquet
        if new_jobs:
            save_jobs_to_parquet(new_jobs, new_raw_jobs, parquet_path)
            total_new_jobs += len(new_jobs)
        
        # Show stats for this year
        if os.path.exists(parquet_path):
            try:
                df = pd.read_parquet(parquet_path)
                total_current = len(df)
                open_jobs = len(df[df['positionOpeningStatus'] == 'Open']) if 'positionOpeningStatus' in df.columns else 0
                
                print(f"   ✅ {year} parquet file now contains:")
                print(f"      - {total_current:,} total current jobs")
                print(f"      - {open_jobs:,} open positions")
            except Exception as e:
                print(f"   ⚠️ Could not read stats from {parquet_path}: {e}")
    
    print(f"\n📊 Summary:")
    print(f"   Total series processed: {len(series_list)}")
    print(f"   Series with jobs: {series_with_jobs}")
    print(f"   Total unique jobs fetched: {len(all_flattened_jobs)}")
    print(f"   New jobs added: {total_new_jobs}")
    print(f"\n🎉 All years completed!")


if __name__ == "__main__":
    main()