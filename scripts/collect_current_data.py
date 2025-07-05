#!/usr/bin/env python3
"""
USAJobs Current API Data Collector

Fetches current job postings from the USAJobs Search API
and saves them to DuckDB with field mapping to match the
historical data schema.

Usage:
    python collect_current_data.py --duckdb jobs.duckdb
    python collect_current_data.py --duckdb jobs.duckdb --days-posted 7
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


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch current USAJobs data")
    parser.add_argument("--days-posted", type=int, help="Jobs posted within N days (optional)", default=None)
    parser.add_argument("--all", action="store_true", help="Fetch all current jobs (no date filter)")
    parser.add_argument("--data-dir", help="Directory for parquet files (defaults to data/)", default="data")
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


def flatten_current_job(job_item: dict) -> dict:
    """
    Keep all fields from current API job, plus add normalized fields to match historical API.
    """
    # Start with the entire job item to keep everything
    flattened = job_item.copy()
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


def fetch_all_jobs(params: Dict, headers: Dict) -> tuple[List[Dict], List[Dict]]:
    """Fetch all jobs with pagination. Returns (raw_jobs, flattened_jobs)"""
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
            flattened = flatten_current_job(item)
            flattened_jobs.append(flattened)
        
        # Get total count on first page
        if page == 1:
            total_count = search_result.get("SearchResultCountAll", 0)
            progress_bar.write(f"Total jobs available: {total_count}")
        
        # Check if we've reached the end
        if total_count and len(raw_jobs) >= total_count:
            progress_bar.write(f"Reached total count ({total_count})")
            break
            
        page += 1
        progress_bar.update(1)
        
        # Be nice to the API
        time.sleep(0.5)
    
    progress_bar.close()
    print(f"✅ Fetched {len(raw_jobs)} total jobs across {page} pages")
    
    return raw_jobs, flattened_jobs


def save_jobs_to_parquet(jobs: List[Dict], raw_jobs: List[Dict], parquet_path: str):
    """Save jobs to parquet file, appending to existing if it exists"""
    if not jobs:
        return
    
    # Add metadata to each job
    for job in jobs:
        job['inserted_at'] = datetime.now().isoformat()
    
    # Convert to DataFrame
    new_df = pd.DataFrame(jobs)
    
    # Ensure usajobs_control_number is string for consistency
    new_df['usajobs_control_number'] = new_df['usajobsControlNumber'].astype(str)
    
    # Load existing data if file exists
    if os.path.exists(parquet_path):
        try:
            existing_df = pd.read_parquet(parquet_path)
            # Remove any duplicate control numbers (keep new data)
            existing_df = existing_df[~existing_df['usajobs_control_number'].isin(new_df['usajobs_control_number'])]
            # Combine dataframes
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        except Exception as e:
            print(f"⚠️ Warning: Could not read existing parquet file {parquet_path}: {e}")
            combined_df = new_df
    else:
        combined_df = new_df
    
    # Save to parquet
    try:
        combined_df.to_parquet(parquet_path, index=False)
        print(f"   💾 Saved {len(new_df)} jobs to {parquet_path}")
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
    
    # Build search parameters
    params = {
        "ResultsPerPage": 500,  # Max allowed by API
        "Fields": "full",
        "SortField": "DatePosted",
        "SortDirection": "desc"
    }
    # Note: Removed "WhoMayApply": "public" to get ALL jobs, not just public ones
    
    # Add date filter if specified (unless --all is used)
    if args.days_posted and not args.all:
        params["DatePosted"] = args.days_posted
    
    print(f"🚀 Fetching current USAJobs data...")
    if args.all:
        print(f"📅 Fetching ALL current jobs (no date filter)")
    elif args.days_posted:
        print(f"📅 Filtering to jobs posted within {args.days_posted} days")
    else:
        print(f"📅 Using default API behavior (no date filter)")
    print(f"💾 Saving to year-based parquet files in: {args.data_dir}/")
    
    # Create data directory if it doesn't exist
    os.makedirs(args.data_dir, exist_ok=True)
    
    # Fetch all jobs
    raw_jobs, flattened_jobs = fetch_all_jobs(params, headers)
    
    if not flattened_jobs:
        print("❌ No jobs fetched")
        return
    
    # Group jobs by year based on positionOpenDate
    print("📊 Grouping jobs by year based on position open date...")
    jobs_by_year = group_jobs_by_year(flattened_jobs, raw_jobs)
    
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
    
    print(f"\n🎉 All years completed!")
    print(f"📊 Added {total_new_jobs:,} new jobs total across {len(jobs_by_year)} year(s)")


if __name__ == "__main__":
    main()