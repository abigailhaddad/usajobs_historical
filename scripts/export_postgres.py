#!/usr/bin/env python3
"""
Fast parallel export from Parquet files to PostgreSQL using bulk operations
"""

import sys
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import glob
import json

def export_chunk(chunk_data, conn_str, thread_id, chunk_num, total_chunks, table_name):
    """Export a chunk of data to PostgreSQL in a separate thread."""
    try:
        # Each thread gets its own connection
        pg_conn = psycopg2.connect(conn_str)
        cur = pg_conn.cursor()
        
        # Prepare data for bulk insert
        values = []
        for _, row in chunk_data.iterrows():
            values.append((
                row.get('controlNumber'),
                row.get('announcementNumber'),
                row.get('hiringAgencyCode'),
                row.get('hiringAgencyName'),
                row.get('hiringDepartmentCode'),
                row.get('hiringDepartmentName'),
                row.get('hiringSubelementName'),
                row.get('agencyLevel'),
                row.get('agencyLevelSort'),
                row.get('positionTitle'),
                row.get('minimumGrade'),
                row.get('maximumGrade'),
                row.get('promotionPotential'),
                row.get('appointmentType'),
                row.get('workSchedule'),
                row.get('serviceType'),
                row.get('payScale'),
                row.get('salaryType'),
                row.get('minimumSalary'),
                row.get('maximumSalary'),
                row.get('supervisoryStatus'),
                row.get('travelRequirement'),
                row.get('teleworkEligible'),
                row.get('securityClearanceRequired'),
                row.get('securityClearance'),
                row.get('drugTestRequired'),
                row.get('relocationExpensesReimbursed'),
                row.get('whoMayApply'),
                row.get('totalOpenings'),
                row.get('disableApplyOnline'),
                row.get('positionOpenDate'),
                row.get('positionCloseDate'),
                row.get('positionExpireDate'),
                row.get('positionOpeningStatus'),
                row.get('announcementClosingTypeCode'),
                row.get('announcementClosingTypeDescription'),
                row.get('vendor'),
                row.get('hiringPaths'),
                row.get('jobSeries'),
                row.get('locations')
            ))
        
        # Bulk insert
        insert_query = f"""
            INSERT INTO {table_name} (
                control_number, announcement_number, hiring_agency_code, hiring_agency_name,
                hiring_department_code, hiring_department_name, hiring_subelement_name,
                agency_level, agency_level_sort, position_title, minimum_grade, maximum_grade,
                promotion_potential, appointment_type, work_schedule, service_type,
                pay_scale, salary_type, minimum_salary, maximum_salary, supervisory_status,
                travel_requirement, telework_eligible, security_clearance_required,
                security_clearance, drug_test_required, relocation_expenses_reimbursed,
                who_may_apply, total_openings, disable_apply_online,
                position_open_date, position_close_date, position_expire_date,
                position_opening_status, announcement_closing_type_code,
                announcement_closing_type_description, vendor,
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

def fast_export_parquet_to_postgres(parquet_files, table_name, max_workers=4):
    """Fast parallel export from Parquet files to PostgreSQL."""
    print(f"\n🚀 Fast exporting {len(parquet_files)} Parquet files to PostgreSQL table '{table_name}' (using {max_workers} threads)...")
    
    load_dotenv()
    conn_str = os.getenv("DATABASE_URL")
    if not conn_str:
        print("❌ DATABASE_URL environment variable not set")
        return False
    
    try:
        # Load all Parquet files into a single DataFrame
        print("📊 Loading Parquet files...")
        dfs = []
        total_rows = 0
        
        for parquet_file in parquet_files:
            df = pd.read_parquet(parquet_file)
            dfs.append(df)
            total_rows += len(df)
            print(f"  📁 {os.path.basename(parquet_file)}: {len(df):,} jobs")
        
        if not dfs:
            print("❌ No data to export")
            return False
        
        # Combine all DataFrames
        combined_df = pd.concat(dfs, ignore_index=True)
        print(f"📦 Combined: {len(combined_df):,} total jobs")
        
        # Remove duplicates based on control number
        initial_count = len(combined_df)
        combined_df = combined_df.drop_duplicates(subset=['controlNumber'], keep='last')
        final_count = len(combined_df)
        
        if initial_count != final_count:
            print(f"🔄 Removed {initial_count - final_count:,} duplicates")
        
        # Split into chunks for parallel processing
        chunk_size = max(1000, len(combined_df) // (max_workers * 4))  # Aim for 4 chunks per worker
        chunks = [combined_df[i:i + chunk_size] for i in range(0, len(combined_df), chunk_size)]
        total_chunks = len(chunks)
        
        print(f"🔀 Split into {total_chunks} chunks of ~{chunk_size:,} jobs each")
        
        # Export chunks in parallel
        total_exported = 0
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all chunks
            futures = [
                executor.submit(export_chunk, chunk, conn_str, i+1, i+1, total_chunks, table_name)
                for i, chunk in enumerate(chunks)
            ]
            
            # Collect results
            for future in as_completed(futures):
                exported_count = future.result()
                total_exported += exported_count
        
        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"\n✅ Export complete!")
        print(f"📊 Exported {total_exported:,} jobs in {total_time:.1f} seconds")
        print(f"🚀 Rate: {total_exported / total_time:.0f} jobs/second")
        
        return True
        
    except Exception as e:
        print(f"❌ Export failed: {e}")
        return False

def create_table_if_not_exists(table_name):
    """Create the PostgreSQL table if it doesn't exist."""
    load_dotenv()
    conn_str = os.getenv("DATABASE_URL")
    if not conn_str:
        print("❌ DATABASE_URL environment variable not set")
        return False
    
    try:
        pg_conn = psycopg2.connect(conn_str)
        cur = pg_conn.cursor()
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
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
            total_openings VARCHAR,
            disable_apply_online VARCHAR,
            position_open_date DATE,
            position_close_date DATE,
            position_expire_date DATE,
            position_opening_status VARCHAR,
            announcement_closing_type_code VARCHAR,
            announcement_closing_type_description VARCHAR,
            vendor VARCHAR,
            hiring_paths VARCHAR,
            job_series VARCHAR,
            locations VARCHAR,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_{table_name}_agency ON {table_name}(hiring_agency_name);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_department ON {table_name}(hiring_department_name);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_open_date ON {table_name}(position_open_date);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_close_date ON {table_name}(position_close_date);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_series ON {table_name}(job_series);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_title ON {table_name}(position_title);
        """
        
        cur.execute(create_table_sql)
        pg_conn.commit()
        pg_conn.close()
        
        print(f"✅ Table '{table_name}' ready")
        return True
        
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        return False

def main():
    if len(sys.argv) < 2:
        print("Usage: python export_postgres.py [historical|current|all] [max_workers]")
        print("")
        print("Examples:")
        print("  python export_postgres.py historical      # Export historical jobs")
        print("  python export_postgres.py current         # Export current jobs")
        print("  python export_postgres.py all             # Export both")
        print("  python export_postgres.py all 8           # Export both with 8 threads")
        sys.exit(1)
    
    data_type = sys.argv[1].lower()
    max_workers = int(sys.argv[2]) if len(sys.argv) > 2 else 4
    
    if data_type not in ['historical', 'current', 'all']:
        print("❌ Data type must be 'historical', 'current', or 'all'")
        sys.exit(1)
    
    # Find Parquet files
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.join(script_dir, '..')
    
    historical_pattern = os.path.join(repo_root, 'data', 'historical_jobs_*.parquet')
    current_pattern = os.path.join(repo_root, 'data', 'current_jobs_*.parquet')
    
    historical_files = glob.glob(historical_pattern)
    current_files = glob.glob(current_pattern)
    
    success = True
    
    if data_type in ['historical', 'all'] and historical_files:
        if create_table_if_not_exists('historical_jobs'):
            success &= fast_export_parquet_to_postgres(historical_files, 'historical_jobs', max_workers)
        else:
            success = False
    
    if data_type in ['current', 'all'] and current_files:
        if create_table_if_not_exists('current_jobs'):
            success &= fast_export_parquet_to_postgres(current_files, 'current_jobs', max_workers)
        else:
            success = False
    
    if not historical_files and not current_files:
        print("❌ No Parquet files found in data/ directory")
        print("💡 Run data collection first:")
        print("   scripts/run_single.sh current-all")
        print("   scripts/run_single.sh range 2024-01-01 2024-12-31")
        sys.exit(1)
    
    if success:
        print("\n🎉 All exports completed successfully!")
    else:
        print("\n❌ Some exports failed")
        sys.exit(1)

if __name__ == "__main__":
    main()