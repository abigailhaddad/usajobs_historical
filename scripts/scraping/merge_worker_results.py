#!/usr/bin/env python3
"""
Merge worker databases back into main database
"""

import duckdb
import os
import argparse
from datetime import datetime

def merge_worker_databases(year, num_workers):
    """Merge all worker databases into the main database"""
    
    main_db_path = f"../../data/duckdb/usajobs_{year}.duckdb"
    
    print(f"Merging {num_workers} worker databases into main database...")
    print(f"Main database: {main_db_path}")
    print("-" * 60)
    
    # Connect to main database
    conn = duckdb.connect(main_db_path)
    
    # Add columns if they don't exist
    try:
        conn.execute("ALTER TABLE historical_jobs ADD COLUMN job_summary TEXT")
    except:
        pass
    try:
        conn.execute("ALTER TABLE historical_jobs ADD COLUMN job_duties TEXT")
    except:
        pass
    try:
        conn.execute("ALTER TABLE historical_jobs ADD COLUMN job_qualifications TEXT")
    except:
        pass
    try:
        conn.execute("ALTER TABLE historical_jobs ADD COLUMN job_requirements TEXT")
    except:
        pass
    try:
        conn.execute("ALTER TABLE historical_jobs ADD COLUMN scraped_at TIMESTAMP")
    except:
        pass
    try:
        conn.execute("ALTER TABLE historical_jobs ADD COLUMN scrape_status TEXT")
    except:
        pass
    
    total_updated = 0
    total_failed = 0
    
    # Process each worker database
    for worker_id in range(1, num_workers + 1):
        worker_db_path = f"../../data/duckdb/usajobs_{year}_worker_{worker_id}.duckdb"
        
        if not os.path.exists(worker_db_path):
            print(f"Worker {worker_id}: Database not found, skipping...")
            continue
        
        print(f"Worker {worker_id}: Processing {worker_db_path}")
        
        try:
            # Attach worker database
            conn.execute(f"ATTACH '{worker_db_path}' AS worker_db")
            
            # Count records
            worker_total = conn.execute("SELECT COUNT(*) FROM worker_db.scraped_jobs").fetchone()[0]
            worker_success = conn.execute("SELECT COUNT(*) FROM worker_db.scraped_jobs WHERE scrape_status = 'completed'").fetchone()[0]
            worker_failed = conn.execute("SELECT COUNT(*) FROM worker_db.scraped_jobs WHERE scrape_status = 'failed'").fetchone()[0]
            
            print(f"  Total: {worker_total}, Success: {worker_success}, Failed: {worker_failed}")
            
            # Update main database with scraped data
            conn.execute("""
                UPDATE historical_jobs h
                SET job_summary = w.job_summary,
                    job_duties = w.job_duties,
                    job_qualifications = w.job_qualifications,
                    job_requirements = w.job_requirements,
                    scraped_at = w.scraped_at,
                    scrape_status = w.scrape_status
                FROM worker_db.scraped_jobs w
                WHERE h.control_number = w.control_number
            """)
            
            total_updated += worker_success
            total_failed += worker_failed
            
            # Detach worker database
            conn.execute("DETACH worker_db")
            
        except Exception as e:
            print(f"  Error processing worker {worker_id}: {e}")
            try:
                conn.execute("DETACH worker_db")
            except:
                pass
    
    # Final statistics
    print("-" * 60)
    print(f"Merge completed!")
    print(f"Total successful updates: {total_updated}")
    print(f"Total failed: {total_failed}")
    print(f"Total processed: {total_updated + total_failed}")
    
    # Verify in main database
    result = conn.execute(f"""
        SELECT 
            COUNT(*) as total_jobs,
            SUM(CASE WHEN scrape_status = 'completed' THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN scrape_status = 'failed' THEN 1 ELSE 0 END) as failed,
            SUM(CASE WHEN scrape_status IS NULL THEN 1 ELSE 0 END) as pending
        FROM historical_jobs
    """).fetchone()
    
    print(f"\nMain database status for {year}:")
    print(f"  Total jobs: {result[0]}")
    print(f"  Completed: {result[1]}")
    print(f"  Failed: {result[2]}")
    print(f"  Pending: {result[3]}")
    
    conn.close()
    
    # Ask if user wants to delete worker databases
    response = input("\nDelete worker database files? (y/n): ")
    if response.lower() == 'y':
        deleted_count = 0
        for worker_id in range(1, num_workers + 1):
            worker_db_path = f"../../data/duckdb/usajobs_{year}_worker_{worker_id}.duckdb"
            if os.path.exists(worker_db_path):
                os.remove(worker_db_path)
                print(f"Deleted {worker_db_path}")
                deleted_count += 1
        if deleted_count > 0:
            print(f"Deleted {deleted_count} worker database files.")
        else:
            print("No worker database files found to delete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Merge worker databases into main database')
    parser.add_argument('year', type=int, help='Year to merge')
    parser.add_argument('num_workers', type=int, help='Number of workers that were used')
    
    args = parser.parse_args()
    
    merge_worker_databases(args.year, args.num_workers)