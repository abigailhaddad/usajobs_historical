#!/usr/bin/env python3
"""Fix the merge of temp databases"""

import duckdb
from pathlib import Path

def fix_merge():
    data_dir = Path("data")
    timestamp = "20250612_165611"
    
    # Get all temp files
    temp_files = sorted(data_dir.glob(f"temp_jobs_*_pipeline_{timestamp}.duckdb"))
    print(f"Found {len(temp_files)} temp files to merge")
    
    # Create new merged database
    final_db_path = data_dir / f"historical_jobs_pipeline_{timestamp}_fixed.duckdb"
    print(f"Creating merged database: {final_db_path.name}")
    
    conn = duckdb.connect(str(final_db_path))
    
    # Create tables
    conn.execute("""
        CREATE TABLE historical_jobs (
            control_number BIGINT PRIMARY KEY,
            announcement_number VARCHAR,
            hiring_agency_name VARCHAR,
            hiring_department_name VARCHAR,
            hiring_subelement_name VARCHAR,
            position_title VARCHAR,
            minimum_grade VARCHAR,
            maximum_grade VARCHAR,
            minimum_salary DECIMAL,
            maximum_salary DECIMAL,
            position_open_date DATE,
            position_close_date DATE,
            locations VARCHAR,
            work_schedule VARCHAR,
            travel_requirement VARCHAR,
            job_series VARCHAR,
            raw_data JSON,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.execute("""
        CREATE TABLE scraped_jobs (
            control_number VARCHAR PRIMARY KEY,
            scraped_date TIMESTAMP,
            scraped_content JSON,
            scraping_success BOOLEAN,
            error_message VARCHAR
        )
    """)
    
    # Copy scraped jobs from original
    orig_db = data_dir / f"historical_jobs_pipeline_{timestamp}.duckdb"
    if orig_db.exists():
        try:
            conn.execute(f"ATTACH '{orig_db}' AS orig_db")
            conn.execute("INSERT INTO scraped_jobs SELECT * FROM orig_db.scraped_jobs")
            scraped_count = conn.execute("SELECT COUNT(*) FROM scraped_jobs").fetchone()[0]
            print(f"Copied {scraped_count} scraped jobs from original database")
            conn.execute("DETACH orig_db")
        except Exception as e:
            print(f"Error copying scraped jobs: {e}")
    
    # Merge all temp databases
    total_jobs = 0
    failed_files = []
    
    for i, temp_file in enumerate(temp_files):
        try:
            # Attach temp database
            conn.execute(f"ATTACH '{temp_file}' AS temp_db")
            
            # Get count before insert
            before_count = conn.execute("SELECT COUNT(*) FROM historical_jobs").fetchone()[0]
            
            # Insert with ON CONFLICT DO NOTHING for deduplication
            conn.execute("""
                INSERT INTO historical_jobs 
                SELECT * FROM temp_db.historical_jobs
                ON CONFLICT (control_number) DO NOTHING
            """)
            
            # Get count after insert
            after_count = conn.execute("SELECT COUNT(*) FROM historical_jobs").fetchone()[0]
            new_jobs = after_count - before_count
            
            print(f"  [{i+1}/{len(temp_files)}] {temp_file.name}: +{new_jobs} jobs (total: {after_count})")
            
            # Detach
            conn.execute("DETACH temp_db")
            
        except Exception as e:
            print(f"  ERROR with {temp_file.name}: {e}")
            failed_files.append(temp_file)
            try:
                conn.execute("DETACH temp_db")
            except:
                pass
    
    final_count = conn.execute("SELECT COUNT(*) FROM historical_jobs").fetchone()[0]
    print(f"\n✅ Merge complete: {final_count} total jobs")
    
    if failed_files:
        print(f"⚠️ Failed to merge {len(failed_files)} files")
    
    conn.close()
    
    return final_db_path, final_count

if __name__ == "__main__":
    new_db_path, job_count = fix_merge()
    print(f"\nCreated: {new_db_path}")
    print(f"Total jobs: {job_count}")
    
    # Now we need to rename it to replace the original
    print("\nTo use this fixed database, run:")
    print(f"  mv data/historical_jobs_pipeline_20250612_165611.duckdb data/historical_jobs_pipeline_20250612_165611_backup.duckdb")
    print(f"  mv {new_db_path} data/historical_jobs_pipeline_20250612_165611.duckdb")