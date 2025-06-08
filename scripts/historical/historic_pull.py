#!/usr/bin/env python3
"""
USAJobs Historic Data Puller

Pulls all jobs from the USAJobs Historical API
and saves the structured data to a JSON file.

Usage:
    python ../../data/duckdb/usajobs_historic_2210.py --start-date 2023-01-01 --end-date 2023-01-15 --output data.json
"""

import argparse
import json
import time
import requests
from typing import List, Dict, Optional
import duckdb
from datetime import datetime
import os
from tqdm import tqdm

API_URL = "https://data.usajobs.gov/api/historicjoa"


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch USAJobs historical jobs")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--load-to-db", action="store_true", help="Load data to PostgreSQL database after fetching")
    parser.add_argument("--position-series", help="Optional: filter by position series (e.g., 2210)")
    parser.add_argument("--duckdb", required=True, help="Path to DuckDB file (e.g., jobs.duckdb)")
    return parser.parse_args()


def get_job_data_page(params: Optional[Dict] = None, next_url: Optional[str] = None, retries: int = 3) -> Dict:
    """Fetch a page of job data from the API with retry logic."""
    for attempt in range(retries):
        try:
            if next_url:
                response = requests.get(next_url)
            else:
                response = requests.get(API_URL, params=params)

            print(f"Requesting: {response.url} -> {response.status_code}")
            
            if response.status_code == 503:
                if attempt < retries - 1:
                    wait_time = (attempt + 1) * 5  # 5, 10, 15 seconds
                    print(f"  503 Service Unavailable, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"  Failed after {retries} attempts")
            
            response.raise_for_status()
            
            # Handle "204 No Content" text response
            if response.text.strip() == "204 No Content":
                return {"results": []}
                
            return response.json()
            
        except requests.RequestException as e:
            if attempt < retries - 1:
                wait_time = (attempt + 1) * 5
                print(f"  Request failed ({e}), retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise e



def fetch_jobs_for_date(date: str, position_series: Optional[str] = None) -> List[Dict]:
    """Fetch all jobs for a specific date."""
    jobs_for_date = []
    params = {
        "StartPositionOpenDate": date,
        "EndPositionOpenDate": date
    }
    
    # Add position series filter if specified
    if position_series:
        params["PositionSeries"] = position_series

    next_url = None
    while True:
        try:
            data = get_job_data_page(params=params, next_url=next_url)
        except Exception as e:
            print(f"Error fetching job data: {e}")
            break

        jobs = data.get("data", [])
        jobs_for_date.extend(jobs)

        next_path = data.get("paging", {}).get("next")
        if next_path:
            next_url = f"https://data.usajobs.gov{next_path}"
            params = None  # Only needed for the first request
        else:
            break

    return jobs_for_date


def fetch_jobs(start_date: str, end_date: str, position_series: Optional[str] = None, 
               duckdb_path: Optional[str] = None, load_to_postgres: bool = False) -> List[Dict]:
    """Fetch job data from the API for a date range, iterating day by day with weekly saves."""
    from datetime import datetime, timedelta
    
    all_jobs = []
    seen_control_numbers = set()  # Track unique jobs
    weekly_batch = []  # Accumulate jobs for weekly saves
    
    # If using DuckDB, initialize it first
    duckdb_conn = None
    if duckdb_path:
        duckdb_conn = init_duckdb(duckdb_path)
        # Load existing control numbers to avoid duplicates
        existing = duckdb_conn.execute("SELECT control_number FROM historical_jobs").fetchall()
        seen_control_numbers = {row[0] for row in existing}
        print(f"Found {len(seen_control_numbers)} existing jobs in DuckDB")
    
    # Convert string dates to datetime objects
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Calculate total days for progress bar
    total_days = (end - start).days + 1
    
    # Create progress bar
    progress_bar = tqdm(total=total_days, desc="Fetching jobs", unit="day")
    
    # Iterate through each date
    current_date = start
    days_processed = 0
    
    while current_date <= end:
        date_str = current_date.strftime("%Y-%m-%d")
        progress_bar.set_description(f"Fetching {date_str}")
        
        try:
            jobs = fetch_jobs_for_date(date_str, position_series)
        except Exception as e:
            progress_bar.write(f"❌ Failed to fetch jobs for {date_str}: {e}")
            current_date += timedelta(days=1)
            progress_bar.update(1)
            continue
        
        # Process jobs for this date
        daily_jobs = []
        new_jobs = 0
        for job in jobs:
            control_number = job.get("usajobsControlNumber")
            if control_number and control_number not in seen_control_numbers:
                seen_control_numbers.add(control_number)
                flattened = flatten_job(job)
                daily_jobs.append(flattened)
                all_jobs.append(flattened)
                new_jobs += 1
        
        # Add daily jobs to weekly batch
        weekly_batch.extend(daily_jobs)
        
        progress_bar.write(f"  {date_str}: Found {len(jobs)} jobs ({new_jobs} new)")
        days_processed += 1
        
        # Save to DuckDB weekly (every 7 days) or at the end
        is_last_day = current_date == end
        is_week_boundary = days_processed % 7 == 0
        
        if (is_week_boundary or is_last_day) and weekly_batch and duckdb_conn:
            save_batch_to_duckdb(duckdb_conn, weekly_batch)
            progress_bar.write(f"  💾 Saved {len(weekly_batch)} jobs to DuckDB (week {days_processed//7 + 1})")
            weekly_batch = []  # Reset batch
        
        # Move to next day
        current_date += timedelta(days=1)
        progress_bar.update(1)
        
        # Small delay between dates to be polite to the API
        if current_date <= end:
            time.sleep(0.5)
    
    progress_bar.close()
    
    # Save any remaining jobs in the final batch
    if weekly_batch and duckdb_conn:
        save_batch_to_duckdb(duckdb_conn, weekly_batch)
        print(f"💾 Saved final {len(weekly_batch)} jobs to DuckDB")
    
    # Export to PostgreSQL at the end if requested
    if load_to_postgres and duckdb_conn:
        export_duckdb_to_postgres(duckdb_conn)
    
    # Close connections
    if duckdb_conn:
        duckdb_conn.close()
    
    print(f"\nTotal unique jobs found: {len(all_jobs)}")
    return all_jobs


def flatten_job(job: dict) -> dict:
    """Flatten nested job structure for easier output."""
    flat = {
        "controlNumber": job.get("usajobsControlNumber"),
        "announcementNumber": job.get("announcementNumber"),
        "hiringAgencyCode": job.get("hiringAgencyCode"),
        "hiringAgencyName": job.get("hiringAgencyName"),
        "hiringDepartmentCode": job.get("hiringDepartmentCode"),
        "hiringDepartmentName": job.get("hiringDepartmentName"),
        "hiringSubelementName": job.get("hiringSubelementName"),
        "agencyLevel": job.get("agencyLevel"),
        "agencyLevelSort": job.get("agencyLevelSort"),
        "positionTitle": job.get("positionTitle"),
        "minimumGrade": job.get("minimumGrade"),
        "maximumGrade": job.get("maximumGrade"),
        "promotionPotential": job.get("promotionPotential"),
        "appointmentType": job.get("appointmentType"),
        "workSchedule": job.get("workSchedule"),
        "serviceType": job.get("serviceType"),
        "payScale": job.get("payScale"),
        "salaryType": job.get("salaryType"),
        "minimumSalary": job.get("minimumSalary"),
        "maximumSalary": job.get("maximumSalary"),
        "supervisoryStatus": job.get("supervisoryStatus"),
        "travelRequirement": job.get("travelRequirement"),
        "teleworkEligible": job.get("teleworkEligible"),
        "securityClearanceRequired": job.get("securityClearanceRequired"),
        "securityClearance": job.get("securityClearance"),
        "drugTestRequired": job.get("drugTestRequired"),
        "relocationExpensesReimbursed": job.get("relocationExpensesReimbursed"),
        "whoMayApply": job.get("whoMayApply"),
        "totalOpenings": job.get("totalOpenings"),
        "disableApplyOnline": job.get("disableAppyOnline"),
        "positionOpenDate": job.get("positionOpenDate"),
        "positionCloseDate": job.get("positionCloseDate"),
        "positionExpireDate": job.get("positionExpireDate"),
        "positionOpeningStatus": job.get("positionOpeningStatus"),
        "announcementClosingTypeCode": job.get("announcementClosingTypeCode"),
        "announcementClosingTypeDescription": job.get("announcementClosingTypeDescription"),
        "vendor": job.get("vendor")
    }

    # Join hiring paths
    hiring_paths = [p.get("hiringPath") for p in job.get("HiringPaths", [])]
    flat["hiringPaths"] = ", ".join(hiring_paths)

    # Join job series
    job_series = [c.get("series") for c in job.get("JobCategories", [])]
    flat["jobSeries"] = ", ".join(job_series)

    # Join location strings
    locations = []
    for loc in job.get("PositionLocations", []):
        parts = [loc.get("positionLocationCity"), loc.get("positionLocationState"), loc.get("positionLocationCountry")]
        locations.append(", ".join(filter(None, parts)))
    flat["locations"] = " | ".join(locations)

    return flat


def init_duckdb(db_path: str):
    """Initialize DuckDB database and return connection."""
    print(f"Initializing DuckDB: {db_path}")
    
    # Connect to DuckDB (creates file if doesn't exist)
    conn = duckdb.connect(db_path)
    
    # Create table if it doesn't exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS historical_jobs (
            control_number BIGINT PRIMARY KEY,
            announcement_number VARCHAR,
            hiring_agency_code VARCHAR,
            hiring_agency_name VARCHAR,
            hiring_department_code VARCHAR,
            hiring_department_name VARCHAR,
            hiring_subelement_name VARCHAR,
            agency_level INTEGER,
            agency_level_sort VARCHAR,
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
            hiring_paths VARCHAR,
            total_openings VARCHAR,
            disable_apply_online VARCHAR,
            position_open_date DATE,
            position_close_date DATE,
            position_expire_date DATE,
            position_opening_status VARCHAR,
            announcement_closing_type_code VARCHAR,
            announcement_closing_type_description VARCHAR,
            vendor VARCHAR,
            job_series VARCHAR,
            locations VARCHAR,
            raw_data JSON,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create useful indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_agency ON historical_jobs(hiring_agency_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_department ON historical_jobs(hiring_department_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_open_date ON historical_jobs(position_open_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_close_date ON historical_jobs(position_close_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_series ON historical_jobs(job_series)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_title ON historical_jobs(position_title)")
    
    return conn


def save_batch_to_duckdb(conn, jobs: List[Dict]):
    """Save a batch of jobs to DuckDB."""
    for job in jobs:
        try:
            conn.execute("""
                INSERT OR REPLACE INTO historical_jobs (
                    control_number, announcement_number, hiring_agency_code, hiring_agency_name,
                    hiring_department_code, hiring_department_name, hiring_subelement_name,
                    agency_level, agency_level_sort, position_title, minimum_grade, maximum_grade,
                    promotion_potential, appointment_type, work_schedule, service_type,
                    pay_scale, salary_type, minimum_salary, maximum_salary, supervisory_status,
                    travel_requirement, telework_eligible, security_clearance_required,
                    security_clearance, drug_test_required, relocation_expenses_reimbursed,
                    who_may_apply, hiring_paths, total_openings, disable_apply_online,
                    position_open_date, position_close_date, position_expire_date,
                    position_opening_status, announcement_closing_type_code,
                    announcement_closing_type_description, vendor, job_series, locations, raw_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                job.get("controlNumber"), job.get("announcementNumber"), job.get("hiringAgencyCode"),
                job.get("hiringAgencyName"), job.get("hiringDepartmentCode"), job.get("hiringDepartmentName"),
                job.get("hiringSubelementName"), job.get("agencyLevel"), job.get("agencyLevelSort"),
                job.get("positionTitle"), job.get("minimumGrade"), job.get("maximumGrade"),
                job.get("promotionPotential"), job.get("appointmentType"), job.get("workSchedule"),
                job.get("serviceType"), job.get("payScale"), job.get("salaryType"),
                job.get("minimumSalary"), job.get("maximumSalary"), job.get("supervisoryStatus"),
                job.get("travelRequirement"), job.get("teleworkEligible"), job.get("securityClearanceRequired"),
                job.get("securityClearance"), job.get("drugTestRequired"), job.get("relocationExpensesReimbursed"),
                job.get("whoMayApply"), job.get("hiringPaths"), job.get("totalOpenings"),
                job.get("disableApplyOnline"), job.get("positionOpenDate"), job.get("positionCloseDate"),
                job.get("positionExpireDate"), job.get("positionOpeningStatus"), job.get("announcementClosingTypeCode"),
                job.get("announcementClosingTypeDescription"), job.get("vendor"), job.get("jobSeries"),
                job.get("locations"), json.dumps(job)
            ])
        except Exception as e:
            print(f"Error inserting job {job.get('controlNumber')} to DuckDB: {e}")


def save_batch_to_postgres(cur, jobs: List[Dict]):
    """Save a batch of jobs to PostgreSQL."""
    from psycopg2.extras import Json
    
    for job in jobs:
        try:
            cur.execute("""
                INSERT INTO historical_jobs (
                    control_number, announcement_number, hiring_agency_code, hiring_agency_name,
                    hiring_department_code, hiring_department_name, hiring_subelement_name,
                    agency_level, agency_level_sort, position_title, minimum_grade, maximum_grade,
                    promotion_potential, appointment_type, work_schedule, service_type,
                    pay_scale, salary_type, minimum_salary, maximum_salary, supervisory_status,
                    travel_requirement, telework_eligible, security_clearance_required,
                    security_clearance, drug_test_required, relocation_expenses_reimbursed,
                    who_may_apply, hiring_paths, total_openings, disable_apply_online,
                    position_open_date, position_close_date, position_expire_date,
                    position_opening_status, announcement_closing_type_code,
                    announcement_closing_type_description, vendor, job_series, locations, raw
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (control_number) DO UPDATE SET
                    updated_at = CURRENT_TIMESTAMP,
                    position_title = EXCLUDED.position_title,
                    position_opening_status = EXCLUDED.position_opening_status,
                    raw = EXCLUDED.raw
            """, (
                job.get("controlNumber"), job.get("announcementNumber"), job.get("hiringAgencyCode"),
                job.get("hiringAgencyName"), job.get("hiringDepartmentCode"), job.get("hiringDepartmentName"),
                job.get("hiringSubelementName"), job.get("agencyLevel"), job.get("agencyLevelSort"),
                job.get("positionTitle"), job.get("minimumGrade"), job.get("maximumGrade"),
                job.get("promotionPotential"), job.get("appointmentType"), job.get("workSchedule"),
                job.get("serviceType"), job.get("payScale"), job.get("salaryType"),
                job.get("minimumSalary"), job.get("maximumSalary"), job.get("supervisoryStatus"),
                job.get("travelRequirement"), job.get("teleworkEligible"), job.get("securityClearanceRequired"),
                job.get("securityClearance"), job.get("drugTestRequired"), job.get("relocationExpensesReimbursed"),
                job.get("whoMayApply"), job.get("hiringPaths"), job.get("totalOpenings"),
                job.get("disableApplyOnline"), job.get("positionOpenDate"), job.get("positionCloseDate"),
                job.get("positionExpireDate"), job.get("positionOpeningStatus"), job.get("announcementClosingTypeCode"),
                job.get("announcementClosingTypeDescription"), job.get("vendor"), job.get("jobSeries"),
                job.get("locations"), Json(job)
            ))
        except Exception as e:
            print(f"Error inserting job {job.get('controlNumber')} to PostgreSQL: {e}")


def export_duckdb_to_postgres(duckdb_conn):
    """Export all data from DuckDB to PostgreSQL using fast parallel export."""
    print("\n🚀 Using fast parallel PostgreSQL export...")
    
    # Get the DuckDB file path
    try:
        duckdb_file = duckdb_conn.execute("PRAGMA database_list").fetchone()[2]
    except:
        print("❌ Could not determine DuckDB file path")
        return duckdb_conn
    
    # Close the current connection temporarily
    duckdb_conn.close()
    
    # Use the fast export script
    import subprocess
    import sys
    
    try:
        result = subprocess.run([
            sys.executable, "fast_postgres_export.py", duckdb_file, "8"
        ], capture_output=True, text=True, cwd=os.path.dirname(os.path.abspath(__file__)))
        
        if result.returncode == 0:
            print(result.stdout)
        else:
            print(f"❌ Fast export failed: {result.stderr}")
            
    except Exception as e:
        print(f"❌ Failed to run fast export: {e}")
    
    # Reconnect to DuckDB for any remaining operations
    return duckdb.connect(duckdb_file)


def save_to_duckdb(jobs: List[Dict], db_path: str):
    """Save jobs to DuckDB database (legacy function for non-incremental saves)."""
    print(f"\nSaving {len(jobs)} jobs to DuckDB: {db_path}")
    conn = init_duckdb(db_path)
    save_batch_to_duckdb(conn, jobs)
    
    # Show summary
    result = conn.execute("SELECT COUNT(*) FROM historical_jobs").fetchone()
    print(f"✅ DuckDB now contains {result[0]:,} total jobs")
    
    conn.close()


def main():
    args = parse_args()
    
    print(f"🚀 Fetching jobs from {args.start_date} to {args.end_date}")
    if args.position_series:
        print(f"📋 Filtering by position series: {args.position_series}")
    print(f"💾 Saving to DuckDB: {args.duckdb}")
    if args.load_to_db:
        print(f"🐘 Will export to PostgreSQL at end")
    
    flattened_jobs = fetch_jobs(
        args.start_date, 
        args.end_date, 
        args.position_series,
        args.duckdb,
        args.load_to_db
    )
    
    print(f"\n✅ Completed! Processed {len(flattened_jobs)} total job records")


if __name__ == "__main__":
    main()
