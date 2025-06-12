#!/usr/bin/env python3
"""
Fast parallel export from DuckDB to PostgreSQL using bulk operations
"""

import sys
import os
import duckdb
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def export_chunk(chunk_data, conn_str, thread_id, chunk_num, total_chunks):
    """Export a chunk of data to PostgreSQL in a separate thread."""
    try:
        # Each thread gets its own connection
        pg_conn = psycopg2.connect(conn_str)
        cur = pg_conn.cursor()
        
        # Prepare data for bulk insert
        values = []
        for job_dict in chunk_data:
            values.append((
                job_dict.get('control_number'),
                job_dict.get('announcement_number'),
                job_dict.get('hiring_agency_code'),
                job_dict.get('hiring_agency_name'),
                job_dict.get('hiring_department_code'),
                job_dict.get('hiring_department_name'),
                job_dict.get('hiring_subelement_name'),
                job_dict.get('agency_level'),
                job_dict.get('agency_level_sort'),
                job_dict.get('position_title'),
                job_dict.get('minimum_grade'),
                job_dict.get('maximum_grade'),
                job_dict.get('promotion_potential'),
                job_dict.get('appointment_type'),
                job_dict.get('work_schedule'),
                job_dict.get('service_type'),
                job_dict.get('pay_scale'),
                job_dict.get('salary_type'),
                job_dict.get('minimum_salary'),
                job_dict.get('maximum_salary'),
                job_dict.get('supervisory_status'),
                job_dict.get('travel_requirement'),
                job_dict.get('telework_eligible'),
                job_dict.get('security_clearance_required'),
                job_dict.get('security_clearance'),
                job_dict.get('drug_test_required'),
                job_dict.get('relocation_expenses_reimbursed'),
                job_dict.get('who_may_apply'),
                job_dict.get('total_openings'),
                job_dict.get('disable_apply_online'),
                job_dict.get('position_open_date'),
                job_dict.get('position_close_date'),
                job_dict.get('position_expire_date'),
                job_dict.get('position_opening_status'),
                job_dict.get('announcement_closing_type_code'),
                job_dict.get('announcement_closing_type_description'),
                job_dict.get('vendor'),
                job_dict.get('hiring_paths'),
                job_dict.get('job_series'),
                job_dict.get('locations')
            ))
        
        # Bulk insert using execute_values (much faster than individual inserts)
        insert_query = """
            INSERT INTO historical_jobs (
                control_number, announcement_number, hiring_agency_code, hiring_agency_name,
                hiring_department_code, hiring_department_name, hiring_subelement_name,
                agency_level, agency_level_sort, position_title, minimum_grade, maximum_grade,
                promotion_potential, appointment_type, work_schedule, service_type, pay_scale,
                salary_type, minimum_salary, maximum_salary, supervisory_status, travel_requirement,
                telework_eligible, security_clearance_required, security_clearance, drug_test_required,
                relocation_expenses_reimbursed, who_may_apply, total_openings, disable_apply_online,
                position_open_date, position_close_date, position_expire_date, position_opening_status,
                announcement_closing_type_code, announcement_closing_type_description, vendor,
                hiring_paths, job_series, locations
            ) VALUES %s
            ON CONFLICT (control_number) DO NOTHING
        """
        
        start_time = time.time()
        execute_values(
            cur, insert_query, values,
            template=None, page_size=1000
        )
        pg_conn.commit()
        end_time = time.time()
        
        pg_conn.close()
        
        print(f"✅ Thread {thread_id}: Chunk {chunk_num}/{total_chunks} completed ({len(chunk_data)} jobs) in {end_time-start_time:.1f}s")
        return len(chunk_data)
        
    except Exception as e:
        print(f"❌ Thread {thread_id}: Error in chunk {chunk_num}: {e}")
        return 0

def fast_export_duckdb_to_postgres(duckdb_file, max_workers=4):
    """Fast parallel export from DuckDB to PostgreSQL."""
    print(f"\n🚀 Fast exporting {duckdb_file} to PostgreSQL (using {max_workers} threads)...")
    
    load_dotenv()
    conn_str = os.getenv("DATABASE_URL")
    if not conn_str:
        print("⚠️  DATABASE_URL not found, skipping PostgreSQL export")
        return
    
    try:
        # Connect to DuckDB
        print("📖 Reading data from DuckDB...")
        duckdb_conn = duckdb.connect(duckdb_file, read_only=True)
        
        # Get all jobs from DuckDB
        jobs = duckdb_conn.execute("SELECT * FROM historical_jobs").fetchall()
        columns = [desc[0] for desc in duckdb_conn.description]
        
        if not jobs:
            print("⚠️  No jobs found in DuckDB")
            return
            
        print(f"📊 Found {len(jobs)} jobs to export")
        
        # Convert rows to dictionaries
        print("🔄 Converting data...")
        job_dicts = []
        for job in jobs:
            job_dict = dict(zip(columns, job))
            job_dicts.append(job_dict)
        
        duckdb_conn.close()
        
        # Split data into chunks for parallel processing
        chunk_size = max(1000, len(job_dicts) // (max_workers * 4))  # 4 chunks per worker
        chunks = [job_dicts[i:i + chunk_size] for i in range(0, len(job_dicts), chunk_size)]
        
        print(f"📦 Split into {len(chunks)} chunks of ~{chunk_size} jobs each")
        print(f"🔀 Starting parallel export with {max_workers} threads...")
        
        start_time = time.time()
        total_inserted = 0
        
        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all chunks
            futures = []
            for i, chunk in enumerate(chunks):
                future = executor.submit(export_chunk, chunk, conn_str, len(futures) + 1, i + 1, len(chunks))
                futures.append(future)
            
            # Collect results as they complete
            for future in as_completed(futures):
                try:
                    result = future.result()
                    total_inserted += result
                except Exception as e:
                    print(f"❌ Chunk failed: {e}")
        
        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"\n🎉 Fast export complete!")
        print(f"✅ Exported {total_inserted:,} jobs in {total_time:.1f} seconds")
        print(f"⚡ Speed: {total_inserted/total_time:.0f} jobs/second")
        
    except Exception as e:
        print(f"❌ Fast PostgreSQL export failed: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python fast_postgres_export.py <duckdb_file> [max_workers]")
        print("Example: python fast_postgres_export.py ../data/duckdb/usajobs_2024.duckdb 8")
        sys.exit(1)
    
    duckdb_file = sys.argv[1]
    max_workers = int(sys.argv[2]) if len(sys.argv) > 2 else 6
    
    if not os.path.exists(duckdb_file):
        print(f"Error: {duckdb_file} not found")
        sys.exit(1)
    
    fast_export_duckdb_to_postgres(duckdb_file, max_workers)