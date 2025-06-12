#!/usr/bin/env python3
"""
Quick test pipeline with small dataset
- Current jobs from API
- 500 recent historical jobs
- Run through full pipeline to test QMD
"""

import subprocess
import sys
import os
import duckdb
from pathlib import Path
import json
from datetime import datetime

def fetch_current_jobs():
    """Fetch current jobs"""
    print("🌐 Fetching current jobs...")
    
    current_script = Path(__file__).parent / "api" / "fetch_current_jobs.py"
    
    cmd = [
        sys.executable, str(current_script),
        "--days-posted", "7",
        "--max-pages", "5"  # Limit to just a few pages
    ]
    
    result = subprocess.run(cmd, cwd=current_script.parent)
    return result.returncode == 0

def get_subset_historical_jobs():
    """Get 500 recent jobs from the main historical database"""
    print("📊 Getting 500 recent historical jobs...")
    
    # Look for historical data in the main repo
    main_hist_db = "/Users/abigailhaddad/Documents/repos/usajobs_historic/data/historical_jobs_2024.duckdb"
    
    if not os.path.exists(main_hist_db):
        print(f"  ⚠️ Main historical DB not found at {main_hist_db}")
        print("  Trying to find any historical data...")
        
        # Try to find any historical database
        possible_paths = [
            "/Users/abigailhaddad/Documents/repos/usajobs_historic/data/",
            "/Users/abigailhaddad/Documents/repos/usajobs_historic/workflows/historical_api/data/"
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                for db_file in Path(path).glob("historical_jobs_*.duckdb"):
                    if "worker" not in db_file.name:
                        main_hist_db = str(db_file)
                        print(f"  ✅ Found: {main_hist_db}")
                        break
                if main_hist_db != "/Users/abigailhaddad/Documents/repos/usajobs_historic/data/historical_jobs_2024.duckdb":
                    break
    
    if not os.path.exists(main_hist_db):
        print("  ❌ No historical database found - skipping historical data")
        return False
    
    try:
        # Connect to main historical database
        main_conn = duckdb.connect(main_hist_db)
        
        # Get 500 most recent jobs
        recent_jobs = main_conn.execute("""
            SELECT * FROM historical_jobs 
            ORDER BY position_open_date DESC 
            LIMIT 500
        """).fetchall()
        
        columns = [desc[0] for desc in main_conn.description]
        main_conn.close()
        
        print(f"  ✅ Retrieved {len(recent_jobs)} recent jobs")
        
        # Create subset database in current_enhanced/data
        subset_db = Path(__file__).parent.parent / "data" / "historical_jobs_subset.duckdb"
        subset_conn = duckdb.connect(str(subset_db))
        
        # Create table with same schema
        main_conn = duckdb.connect(main_hist_db)
        create_table_sql = main_conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='historical_jobs'").fetchone()
        if not create_table_sql:
            # Fallback: create basic table
            subset_conn.execute("""
                CREATE TABLE historical_jobs (
                    control_number BIGINT PRIMARY KEY,
                    announcement_number VARCHAR,
                    hiring_agency_name VARCHAR,
                    hiring_department_name VARCHAR,
                    position_title VARCHAR,
                    minimum_salary DECIMAL,
                    maximum_salary DECIMAL,
                    position_open_date DATE,
                    position_close_date DATE,
                    locations VARCHAR,
                    work_schedule VARCHAR,
                    travel_requirement VARCHAR,
                    job_series VARCHAR,
                    raw_data JSON
                )
            """)
        else:
            subset_conn.execute(create_table_sql[0].replace("historical_jobs", "historical_jobs_temp"))
            subset_conn.execute("ALTER TABLE historical_jobs_temp RENAME TO historical_jobs")
        
        main_conn.close()
        
        # Insert the 500 jobs
        placeholders = ", ".join(["?" for _ in columns])
        subset_conn.execute(f"INSERT INTO historical_jobs VALUES ({placeholders})", recent_jobs)
        
        count = subset_conn.execute("SELECT COUNT(*) FROM historical_jobs").fetchone()[0]
        subset_conn.close()
        
        print(f"  ✅ Created subset database with {count} jobs")
        return True
        
    except Exception as e:
        print(f"  ❌ Error creating subset: {e}")
        return False

def run_scraping_on_subset():
    """Run scraping on the subset of historical jobs"""
    print("🕷️ Scraping subset of historical jobs...")
    
    subset_db = Path(__file__).parent.parent / "data" / "historical_jobs_subset.duckdb"
    
    if not subset_db.exists():
        print("  ⚠️ No subset database found")
        return False
    
    try:
        conn = duckdb.connect(str(subset_db))
        
        # Get control numbers
        control_numbers = conn.execute("SELECT DISTINCT control_number FROM historical_jobs LIMIT 100").fetchall()
        print(f"  📋 Found {len(control_numbers)} jobs to scrape (limiting to 100 for speed)")
        
        # Create scraped_jobs table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scraped_jobs (
                control_number VARCHAR PRIMARY KEY,
                scraped_date TIMESTAMP,
                scraped_content JSON,
                scraping_success BOOLEAN,
                error_message VARCHAR
            )
        """)
        
        # Import scraping function
        sys.path.append(str(Path(__file__).parent / "scraping"))
        from scrape_enhanced_job_posting import scrape_enhanced_job_posting
        
        success_count = 0
        for i, (control_number,) in enumerate(control_numbers[:100], 1):  # Limit to 100
            try:
                print(f"  📄 Scraping {i}/100: {control_number}")
                
                # Check if already scraped
                existing = conn.execute("SELECT control_number FROM scraped_jobs WHERE control_number = ?", [str(control_number)]).fetchone()
                if existing:
                    continue
                
                # Scrape
                result = scrape_enhanced_job_posting(str(control_number))
                
                # Save result
                conn.execute("""
                    INSERT INTO scraped_jobs (control_number, scraped_date, scraped_content, scraping_success, error_message)
                    VALUES (?, ?, ?, ?, ?)
                """, [
                    str(control_number),
                    datetime.now().isoformat(),
                    json.dumps(result) if result else None,
                    result.get('status') == 'success' if result else False,
                    result.get('error') if result and result.get('status') == 'error' else None
                ])
                
                if result and result.get('status') == 'success':
                    success_count += 1
                    
            except Exception as e:
                print(f"    ❌ Error scraping {control_number}: {e}")
        
        conn.close()
        print(f"  ✅ Scraping complete: {success_count} successes")
        return True
        
    except Exception as e:
        print(f"  ❌ Scraping error: {e}")
        return False

def run_rationalization():
    """Run field rationalization on the test dataset"""
    print("🔄 Running field rationalization...")
    
    base_dir = Path(__file__).parent.parent
    data_dir = base_dir / "data"
    
    # Find files
    current_files = list(data_dir.glob("current_jobs_*.json"))
    subset_db = data_dir / "historical_jobs_subset.duckdb"
    
    if not current_files:
        print("  ❌ No current jobs files found")
        return False
        
    if not subset_db.exists():
        print("  ❌ No subset database found")
        return False
    
    latest_current = max(current_files, key=os.path.getctime)
    
    rationalization_script = base_dir / "scripts" / "integration" / "field_rationalization.py"
    output_file = data_dir / f"unified_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.duckdb"
    
    try:
        cmd = [
            sys.executable, str(rationalization_script),
            "--historical-db", str(subset_db),
            "--current-json", str(latest_current),
            "--output", str(output_file),
            "--output-format", "duckdb"
        ]
        
        result = subprocess.run(cmd, cwd=rationalization_script.parent)
        
        if result.returncode == 0:
            print(f"  ✅ Created test unified dataset: {output_file.name}")
            return True
        else:
            print(f"  ❌ Rationalization failed")
            return False
            
    except Exception as e:
        print(f"  ❌ Rationalization error: {e}")
        return False

def main():
    print("🚀 QUICK TEST PIPELINE")
    print("=" * 30)
    
    success = True
    
    # Step 1: Get current jobs
    success = success and fetch_current_jobs()
    
    # Step 2: Get subset of historical jobs  
    success = success and get_subset_historical_jobs()
    
    # Step 3: Scrape subset
    success = success and run_scraping_on_subset()
    
    # Step 4: Rationalize
    success = success and run_rationalization()
    
    if success:
        print("\n✅ Test pipeline completed!")
        print("📊 You can now run the QMD analysis on the test dataset")
        
        # Show what was created
        data_dir = Path(__file__).parent.parent / "data"
        print("\n📁 Files created:")
        for f in data_dir.glob("*"):
            if f.is_file():
                print(f"  - {f.name}")
    else:
        print("\n❌ Test pipeline had errors")

if __name__ == "__main__":
    main()