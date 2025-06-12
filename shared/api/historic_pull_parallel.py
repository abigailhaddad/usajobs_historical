#!/usr/bin/env python3
"""
USAJobs Historic Data Puller - Parallel Version

Pulls all jobs from the USAJobs Historical API using multiple workers
and saves the structured data to DuckDB.

Usage:
    python historic_pull_parallel.py --start-date 2023-01-01 --end-date 2023-12-31
    python historic_pull_parallel.py --start-date 2023-01-01 --end-date 2023-12-31 --load-to-postgres --workers 8
"""

import argparse
import json
import time
import requests
from typing import List, Dict, Optional
import duckdb
from datetime import datetime, timedelta
import os
from tqdm import tqdm
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import threading

API_URL = "https://data.usajobs.gov/api/historicjoa"


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch USAJobs historical jobs in parallel")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--output-dir", help="Output directory for data files (default: ../../data)")
    parser.add_argument("--load-to-postgres", action="store_true", help="Load data to PostgreSQL database after fetching")
    parser.add_argument("--position-series", help="Optional: filter by position series (e.g., 2210)")
    parser.add_argument("--workers", type=int, default=16, help="Number of parallel workers (default: 16)")
    return parser.parse_args()


def get_job_data_page(params: Optional[Dict] = None, next_url: Optional[str] = None, retries: int = 3) -> Dict:
    """Fetch a page of job data from the API with retry logic."""
    for attempt in range(retries):
        try:
            if next_url:
                response = requests.get(next_url)
            else:
                response = requests.get(API_URL, params=params)
            
            if response.status_code == 503:
                if attempt < retries - 1:
                    wait_time = (attempt + 1) * 5  # 5, 10, 15 seconds
                    time.sleep(wait_time)
                    continue
                else:
                    return {"results": []}
            
            response.raise_for_status()
            
            # Handle "204 No Content" text response
            if response.text.strip() == "204 No Content":
                return {"results": []}
                
            return response.json()
            
        except requests.RequestException as e:
            if attempt < retries - 1:
                wait_time = (attempt + 1) * 5
                time.sleep(wait_time)
            else:
                return {"results": []}


def fetch_jobs_for_date(date_str: str, position_series: Optional[str] = None) -> List[Dict]:
    """Fetch all jobs for a specific date."""
    jobs_for_date = []
    params = {
        "StartPositionOpenDate": date_str,
        "EndPositionOpenDate": date_str
    }
    
    # Add position series filter if specified
    if position_series:
        params["PositionSeries"] = position_series

    next_url = None
    while True:
        try:
            data = get_job_data_page(params=params, next_url=next_url)
        except Exception:
            break

        jobs = data.get("data", [])
        jobs_for_date.extend(jobs)

        next_path = data.get("paging", {}).get("next")
        if next_path:
            next_url = f"https://data.usajobs.gov{next_path}"
            params = None  # Only needed for the first request
        else:
            break
        
        # Small delay between pages
        time.sleep(0.1)

    return jobs_for_date


def process_date_chunk(args_tuple):
    """Process a chunk of dates - designed for multiprocessing."""
    date_list, position_series, worker_id, worker_db_path = args_tuple
    
    # Each worker gets its own DuckDB file
    worker_conn = init_duckdb(worker_db_path)
    
    all_jobs = []
    for date_str in date_list:
        try:
            jobs = fetch_jobs_for_date(date_str, position_series)
            flattened_jobs = [flatten_job(job) for job in jobs]
            
            # Save to worker's own database immediately
            if flattened_jobs:
                save_batch_to_duckdb(worker_conn, flattened_jobs)
            
            all_jobs.extend(flattened_jobs)
            
            # Brief delay between dates
            time.sleep(0.2)
            
        except Exception as e:
            print(f"Worker {worker_id}: Error processing {date_str}: {e}")
            continue
    
    # Close worker connection
    worker_conn.close()
    
    return len(all_jobs), worker_db_path


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
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
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


def merge_worker_databases(worker_db_paths: List[str], final_db_path: str) -> int:
    """Merge all worker databases into final database."""
    print(f"\n🔄 Merging {len(worker_db_paths)} worker databases into {final_db_path}")
    
    # Initialize final database
    final_conn = init_duckdb(final_db_path)
    
    total_jobs = 0
    seen_control_numbers = set()
    
    # Load existing control numbers to avoid duplicates
    existing = final_conn.execute("SELECT control_number FROM historical_jobs").fetchall()
    seen_control_numbers = {row[0] for row in existing}
    
    for worker_db in worker_db_paths:
        if not os.path.exists(worker_db):
            continue
            
        try:
            # Connect to worker database
            worker_conn = duckdb.connect(worker_db)
            
            # Get all jobs from worker
            results = worker_conn.execute("SELECT * FROM historical_jobs").fetchall()
            columns = [desc[0] for desc in worker_conn.description]
            
            # Insert unique jobs into final database
            new_jobs = 0
            for row in results:
                control_number = row[0]  # First column is control_number
                if control_number not in seen_control_numbers:
                    seen_control_numbers.add(control_number)
                    # Insert row into final database
                    placeholders = ','.join(['?' for _ in row])
                    final_conn.execute(f"INSERT INTO historical_jobs VALUES ({placeholders})", row)
                    new_jobs += 1
            
            total_jobs += new_jobs
            worker_conn.close()
            
            # Clean up worker database
            os.remove(worker_db)
            print(f"  Merged {new_jobs} jobs from worker database")
            
        except Exception as e:
            print(f"  Error merging {worker_db}: {e}")
    
    final_conn.close()
    print(f"✅ Merge complete: {total_jobs} total unique jobs")
    return total_jobs


def fetch_jobs_parallel(start_date: str, end_date: str, position_series: Optional[str] = None, 
                       duckdb_path: Optional[str] = None, load_to_postgres: bool = False, workers: int = 16) -> List[Dict]:
    """Fetch job data from the API for a date range using parallel workers."""
    
    # Generate list of all dates
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    all_dates = []
    current_date = start
    while current_date <= end:
        all_dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    
    print(f"Processing {len(all_dates)} dates with {workers} workers...")
    
    # Split dates into chunks for workers
    chunk_size = max(1, len(all_dates) // workers)
    date_chunks = [all_dates[i:i + chunk_size] for i in range(0, len(all_dates), chunk_size)]
    
    # Create worker database paths
    base_path = duckdb_path.replace('.duckdb', '')
    worker_db_paths = [f"{base_path}_worker_{i+1}.duckdb" for i in range(len(date_chunks))]
    
    # Prepare worker arguments with individual database paths
    worker_args = [(chunk, position_series, i+1, worker_db_paths[i]) for i, chunk in enumerate(date_chunks)]
    
    # Process chunks in parallel
    with ProcessPoolExecutor(max_workers=workers) as executor:
        # Submit all jobs
        future_to_worker = {executor.submit(process_date_chunk, args): args[2] for args in worker_args}
        
        # Collect results with progress bar
        completed_workers = []
        with tqdm(total=len(date_chunks), desc="Processing chunks", unit="chunk") as pbar:
            for future in as_completed(future_to_worker):
                worker_id = future_to_worker[future]
                try:
                    job_count, worker_db_path = future.result()
                    completed_workers.append(worker_db_path)
                    
                    pbar.set_description(f"Worker {worker_id} complete: {job_count} jobs")
                    pbar.update(1)
                    
                except Exception as e:
                    print(f"Worker {worker_id} failed: {e}")
                    pbar.update(1)
    
    # Merge all worker databases into final database
    total_jobs = merge_worker_databases(completed_workers, duckdb_path)
    
    # Export to PostgreSQL at the end if requested
    if load_to_postgres:
        final_conn = duckdb.connect(duckdb_path)
        export_duckdb_to_postgres(final_conn)
        final_conn.close()
    
    print(f"\nTotal unique jobs collected: {total_jobs}")
    return []  # Return empty list since we're working with databases directly


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
            sys.executable, "../database/fast_postgres_export.py", duckdb_file, "8"
        ], capture_output=True, text=True, cwd=os.path.dirname(os.path.abspath(__file__)))
        
        if result.returncode == 0:
            print(result.stdout)
        else:
            print(f"❌ Fast export failed: {result.stderr}")
            
    except Exception as e:
        print(f"❌ Failed to run fast export: {e}")
    
    # Reconnect to DuckDB for any remaining operations
    return duckdb.connect(duckdb_file)


def main():
    args = parse_args()
    
    # Set output directory
    output_dir = args.output_dir if args.output_dir else "../../data"
    
    # Always create separate annual databases
    year_start = int(args.start_date[:4])
    year_end = int(args.end_date[:4])
    
    for year in range(year_start, year_end + 1):
        duckdb_path = f"{output_dir}/historical_jobs_{year}.duckdb"
        year_start_date = f"{year}-01-01"
        year_end_date = f"{year}-12-31"
        
        # Adjust dates if they fall within the requested range
        if year == year_start:
            year_start_date = args.start_date
        if year == year_end:
            year_end_date = args.end_date
    
        print(f"\n🚀 Processing year {year}: {year_start_date} to {year_end_date}")
        print(f"👥 Using {args.workers} parallel workers")
        if args.position_series:
            print(f"📋 Filtering by position series: {args.position_series}")
        print(f"💾 Saving to DuckDB: {duckdb_path}")
        if args.load_to_postgres:
            print(f"🐘 Will export to PostgreSQL at end")
        
        flattened_jobs = fetch_jobs_parallel(
            year_start_date, 
            year_end_date, 
            args.position_series,
            duckdb_path,
            args.load_to_postgres,
            args.workers
        )
        
        print(f"✅ Year {year} completed! Processed {len(flattened_jobs)} job records")
    
    print(f"\n🎉 All years completed! Data saved to separate annual databases.")


if __name__ == "__main__":
    main()