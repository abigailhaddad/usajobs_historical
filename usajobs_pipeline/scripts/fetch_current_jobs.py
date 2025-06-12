#!/usr/bin/env python3
"""
USAJobs Current API Data Fetcher

Fetches current job postings from the USAJobs Search API
for field rationalization and integration with scraped data.

Usage:
    python fetch_current_jobs.py --days-posted 7
    python fetch_current_jobs.py --keyword "data scientist" --max-pages 5
"""

import requests
import json
import os
import time
import duckdb
from datetime import datetime, timedelta
from dotenv import load_dotenv
import argparse
from typing import List, Dict, Optional

# Load environment variables
load_dotenv()
API_KEY = os.getenv("USAJOBS_API_TOKEN")

# Base URL and headers
BASE_URL = "https://data.usajobs.gov/api/Search"
HEADERS = {
    "Host": "data.usajobs.gov",
    "Authorization-Key": API_KEY
}

def parse_args():
    parser = argparse.ArgumentParser(description="Fetch current USAJobs data")
    parser.add_argument("--keyword", help="Search keyword", default=None)
    parser.add_argument("--days-posted", type=int, help="Jobs posted within N days", default=None)
    parser.add_argument("--max-pages", type=int, help="Maximum pages to fetch", default=None)
    parser.add_argument("--who-may-apply", help="Who may apply filter", default="public")
    parser.add_argument("--remote", action="store_true", help="Remote jobs only")
    parser.add_argument("--save-to-duckdb", action="store_true", help="Save to DuckDB database")
    return parser.parse_args()


def fetch_jobs_page(params, page=1):
    """Fetch a single page of job results"""
    params_copy = params.copy()
    params_copy['Page'] = page
    
    try:
        response = requests.get(BASE_URL, headers=HEADERS, params=params_copy)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page {page}: {e}")
        return None


def fetch_all_jobs(base_params, max_pages=None):
    """Fetch all jobs with pagination"""
    all_jobs = []
    page = 1
    total_count = None
    
    while True:
        print(f"Fetching page {page}...")
        data = fetch_jobs_page(base_params, page)
        
        if not data:
            print("Failed to fetch data, stopping.")
            break
            
        search_result = data.get("SearchResult", {})
        items = search_result.get("SearchResultItems", [])
        
        if not items:
            print("No more results.")
            break
            
        all_jobs.extend(items)
        
        # Get total count on first page
        if page == 1:
            total_count = search_result.get("SearchResultCountAll", 0)
            print(f"Total jobs available: {total_count}")
        
        # Check if we've reached the end
        if len(all_jobs) >= total_count:
            break
            
        # Check max pages limit
        if max_pages and page >= max_pages:
            print(f"Reached max pages limit ({max_pages})")
            break
            
        page += 1
        # Be nice to the API
        time.sleep(0.5)
    
    print(f"Fetched {len(all_jobs)} total jobs across {page} pages")
    return all_jobs


def flatten_current_job(job_item: dict) -> dict:
    """Flatten current API job structure for comparison with historical API."""
    job = job_item.get("MatchedObjectDescriptor", {})
    
    flat = {
        # Basic identifiers
        "controlNumber": job.get("PositionID"),
        "announcementNumber": job.get("JobAnnouncement", {}).get("AnnouncementNumber"),
        
        # Agency info
        "hiringAgencyCode": job.get("OrganizationCodes", "").split(".")[0] if job.get("OrganizationCodes") else None,
        "hiringAgencyName": job.get("DepartmentName"),
        "hiringDepartmentCode": None,  # Not available in current API
        "hiringDepartmentName": job.get("DepartmentName"),
        "hiringSubelementName": job.get("SubAgency"),
        
        # Position details
        "positionTitle": job.get("PositionTitle"),
        "minimumGrade": job.get("JobGrade", [{}])[0].get("Code") if job.get("JobGrade") else None,
        "maximumGrade": job.get("JobGrade", [{}])[-1].get("Code") if job.get("JobGrade") else None,
        "promotionPotential": None,  # Not directly available
        "appointmentType": job.get("PositionSchedule", [{}])[0].get("Name") if job.get("PositionSchedule") else None,
        "workSchedule": job.get("PositionSchedule", [{}])[0].get("Name") if job.get("PositionSchedule") else None,
        "serviceType": None,  # Not directly available
        
        # Compensation
        "payScale": job.get("PayPlan", [{}])[0].get("Code") if job.get("PayPlan") else None,
        "salaryType": None,  # Need to derive from salary info
        "minimumSalary": job.get("PositionRemuneration", [{}])[0].get("MinimumRange") if job.get("PositionRemuneration") else None,
        "maximumSalary": job.get("PositionRemuneration", [{}])[0].get("MaximumRange") if job.get("PositionRemuneration") else None,
        
        # Work details
        "supervisoryStatus": job.get("SupervisoryStatus", [{}])[0].get("Name") if job.get("SupervisoryStatus") else None,
        "travelRequirement": job.get("TravelCode", {}).get("Name"),
        "teleworkEligible": None,  # Check if available in QualificationSummary
        "securityClearanceRequired": None,  # Check if in QualificationSummary
        "securityClearance": None,
        "drugTestRequired": None,
        "relocationExpensesReimbursed": None,
        
        # Application details
        "whoMayApply": job.get("UserArea", {}).get("Details", {}).get("WhoMayApply", {}).get("Name"),
        "totalOpenings": job.get("PositionOfferingType", [{}])[0].get("Name") if job.get("PositionOfferingType") else None,
        "disableApplyOnline": None,
        
        # Dates
        "positionOpenDate": job.get("PositionStartDate"),
        "positionCloseDate": job.get("PositionEndDate"),
        "positionExpireDate": None,
        "positionOpeningStatus": "Open" if job.get("PositionEndDate") else "Closed",
        
        # Categories and locations
        "jobSeries": ", ".join([cat.get("Code", "") for cat in job.get("JobCategory", [])]),
        "locations": " | ".join([f"{loc.get('CityName', '')}, {loc.get('CountrySubDivisionCode', '')}, {loc.get('CountryCode', '')}" 
                               for loc in job.get("PositionLocation", [])]),
        
        # Current API specific fields
        "applyOnlineUrl": job.get("ApplyURI", [None])[0],
        "positionUri": job.get("PositionURI"),
        "qualificationSummary": job.get("QualificationSummary", ""),
        "majorDuties": job.get("MajorDuties", []),
        "education": job.get("Education", {}),
        "requirements": job.get("Requirements", ""),
        "evaluations": job.get("Evaluations", ""),
        "howToApply": job.get("HowToApply", ""),
        "whatToExpectNext": job.get("WhatToExpectNext", ""),
        "requiredDocuments": job.get("RequiredDocuments", ""),
        
        # Metadata
        "apiSource": "current",
        "fetchDate": datetime.now().isoformat(),
        "rawData": job
    }
    
    return flat


def init_current_jobs_duckdb(db_path: str):
    """Initialize DuckDB for current jobs data."""
    print(f"Initializing current jobs DuckDB: {db_path}")
    
    # Create directory if needed
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = duckdb.connect(db_path)
    
    # Create table with both historical and current API fields
    conn.execute("""
        CREATE TABLE IF NOT EXISTS current_jobs (
            control_number VARCHAR PRIMARY KEY,
            announcement_number VARCHAR,
            hiring_agency_code VARCHAR,
            hiring_agency_name VARCHAR,
            hiring_department_code VARCHAR,
            hiring_department_name VARCHAR,
            hiring_subelement_name VARCHAR,
            position_title VARCHAR,
            minimum_grade VARCHAR,
            maximum_grade VARCHAR,
            promotion_potential VARCHAR,
            appointment_type VARCHAR,
            work_schedule VARCHAR,
            service_type VARCHAR,
            pay_scale VARCHAR,
            salary_type VARCHAR,
            minimum_salary DECIMAL,
            maximum_salary DECIMAL,
            supervisory_status VARCHAR,
            travel_requirement VARCHAR,
            telework_eligible VARCHAR,
            security_clearance_required VARCHAR,
            security_clearance VARCHAR,
            drug_test_required VARCHAR,
            relocation_expenses_reimbursed VARCHAR,
            who_may_apply VARCHAR,
            total_openings VARCHAR,
            disable_apply_online VARCHAR,
            position_open_date DATE,
            position_close_date DATE,
            position_expire_date DATE,
            position_opening_status VARCHAR,
            job_series VARCHAR,
            locations VARCHAR,
            
            -- Current API specific fields
            apply_online_url VARCHAR,
            position_uri VARCHAR,
            qualification_summary TEXT,
            major_duties JSON,
            education JSON,
            requirements TEXT,
            evaluations TEXT,
            how_to_apply TEXT,
            what_to_expect_next TEXT,
            required_documents TEXT,
            
            -- Metadata
            api_source VARCHAR DEFAULT 'current',
            fetch_date TIMESTAMP,
            raw_data JSON,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_current_agency ON current_jobs(hiring_agency_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_current_title ON current_jobs(position_title)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_current_open_date ON current_jobs(position_open_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_current_series ON current_jobs(job_series)")
    
    return conn


def save_jobs_to_duckdb(jobs: List[Dict], db_path: str):
    """Save jobs to DuckDB database."""
    conn = init_current_jobs_duckdb(db_path)
    
    for job in jobs:
        try:
            conn.execute("""
                INSERT OR REPLACE INTO current_jobs (
                    control_number, announcement_number, hiring_agency_code, hiring_agency_name,
                    hiring_department_code, hiring_department_name, hiring_subelement_name,
                    position_title, minimum_grade, maximum_grade, promotion_potential,
                    appointment_type, work_schedule, service_type, pay_scale, salary_type,
                    minimum_salary, maximum_salary, supervisory_status, travel_requirement,
                    telework_eligible, security_clearance_required, security_clearance,
                    drug_test_required, relocation_expenses_reimbursed, who_may_apply,
                    total_openings, disable_apply_online, position_open_date, position_close_date,
                    position_expire_date, position_opening_status, job_series, locations,
                    apply_online_url, position_uri, qualification_summary, major_duties,
                    education, requirements, evaluations, how_to_apply, what_to_expect_next,
                    required_documents, fetch_date, raw_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                job.get("controlNumber"), job.get("announcementNumber"), job.get("hiringAgencyCode"),
                job.get("hiringAgencyName"), job.get("hiringDepartmentCode"), job.get("hiringDepartmentName"),
                job.get("hiringSubelementName"), job.get("positionTitle"), job.get("minimumGrade"),
                job.get("maximumGrade"), job.get("promotionPotential"), job.get("appointmentType"),
                job.get("workSchedule"), job.get("serviceType"), job.get("payScale"),
                job.get("salaryType"), job.get("minimumSalary"), job.get("maximumSalary"),
                job.get("supervisoryStatus"), job.get("travelRequirement"), job.get("teleworkEligible"),
                job.get("securityClearanceRequired"), job.get("securityClearance"), job.get("drugTestRequired"),
                job.get("relocationExpensesReimbursed"), job.get("whoMayApply"), job.get("totalOpenings"),
                job.get("disableApplyOnline"), job.get("positionOpenDate"), job.get("positionCloseDate"),
                job.get("positionExpireDate"), job.get("positionOpeningStatus"), job.get("jobSeries"),
                job.get("locations"), job.get("applyOnlineUrl"), job.get("positionUri"),
                job.get("qualificationSummary"), json.dumps(job.get("majorDuties")),
                json.dumps(job.get("education")), job.get("requirements"), job.get("evaluations"),
                job.get("howToApply"), job.get("whatToExpectNext"), job.get("requiredDocuments"),
                job.get("fetchDate"), json.dumps(job.get("rawData"))
            ])
        except Exception as e:
            print(f"Error saving job {job.get('controlNumber')}: {e}")
    
    # Show results
    count = conn.execute("SELECT COUNT(*) FROM current_jobs").fetchone()[0]
    print(f"✅ DuckDB now contains {count:,} current jobs")
    
    conn.close()


def save_jobs_data(jobs, filename="current_jobs.json"):
    """Save jobs data to JSON file"""
    data = {
        "LanguageCode": "EN",
        "SearchParameters": {
            "FetchDate": datetime.now().isoformat(),
            "TotalJobsFetched": len(jobs)
        },
        "SearchResult": {
            "SearchResultCount": len(jobs),
            "SearchResultCountAll": len(jobs),
            "SearchResultItems": jobs
        }
    }
    
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f"✅ Data saved to {filename}")


def main():
    args = parse_args()
    
    if not API_KEY:
        print("❌ USAJOBS_API_TOKEN not found in environment")
        return
    
    # Build search parameters
    params = {
        "WhoMayApply": args.who_may_apply,
        "ResultsPerPage": 500,  # Max allowed by API
        "Fields": "full",
        "SortField": "DatePosted",
        "SortDirection": "desc"
    }
    
    # Add date filter if specified
    if args.days_posted:
        params["DatePosted"] = args.days_posted
    
    # Add keyword if provided
    if args.keyword:
        params["Keyword"] = args.keyword
    
    # Add remote filter if requested
    if args.remote:
        params["RemoteIndicator"] = "true"
    
    print(f"🚀 Fetching current USAJobs data...")
    print(f"📋 Search parameters: {params}")
    
    # Fetch all jobs
    raw_jobs = fetch_all_jobs(params, max_pages=args.max_pages)
    
    if not raw_jobs:
        print("❌ No jobs fetched")
        return
    
    # Flatten jobs for comparison with historical data
    print("🔄 Flattening job data for field rationalization...")
    flattened_jobs = [flatten_current_job(job) for job in raw_jobs]
    
    # Save results
    if args.save_to_duckdb:
        db_path = "../data/current_jobs.duckdb"
        save_jobs_to_duckdb(flattened_jobs, db_path)
    
    # Always save JSON for inspection
    filename = f"../data/current_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    save_jobs_data(raw_jobs, filename)
    
    print(f"\n✅ Completed! Fetched {len(flattened_jobs)} current job postings")
    print(f"📊 Use this data to compare fields with historical API and scraping results")


if __name__ == "__main__":
    main()