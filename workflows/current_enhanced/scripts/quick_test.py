#!/usr/bin/env python3
"""
Quick test: 100 historical jobs + current jobs + scraping + rationalization
"""

import subprocess
import sys
import os
import duckdb
from pathlib import Path
import json
from datetime import datetime

def create_small_historical_subset():
    """Create a small subset of historical jobs for testing"""
    print("📊 Creating small historical subset...")
    
    # Find any existing historical database
    possible_paths = [
        "/Users/abigailhaddad/Documents/repos/usajobs_historic/workflows/historical_api/data/historical_jobs_2025.duckdb",
        "/Users/abigailhaddad/Documents/repos/usajobs_historic/data/historical_jobs_2024.duckdb",
        "/Users/abigailhaddad/Documents/repos/usajobs_historic/data/historical_jobs_2025.duckdb"
    ]
    
    source_db = None
    for path in possible_paths:
        if os.path.exists(path):
            source_db = path
            break
    
    if not source_db:
        print("  ❌ No historical database found")
        return False
    
    print(f"  ✅ Using source: {source_db}")
    
    try:
        # Create simple subset with just basic fields
        subset_db = Path(__file__).parent.parent / "data" / "historical_jobs_test.duckdb"
        
        source_conn = duckdb.connect(source_db)
        subset_conn = duckdb.connect(str(subset_db))
        
        # Create simple table
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
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Get 100 recent jobs from source
        jobs = source_conn.execute("""
            SELECT 
                control_number,
                announcement_number,
                hiring_agency_name,
                hiring_department_name,
                position_title,
                minimum_salary,
                maximum_salary,
                position_open_date,
                position_close_date,
                locations,
                work_schedule,
                travel_requirement,
                job_series
            FROM historical_jobs 
            WHERE control_number IS NOT NULL 
            ORDER BY position_open_date DESC 
            LIMIT 100
        """).fetchall()
        
        # Insert into subset database
        for job in jobs:
            subset_conn.execute("""
                INSERT INTO historical_jobs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, job)
        
        # Add scraped_jobs table
        subset_conn.execute("""
            CREATE TABLE scraped_jobs (
                control_number VARCHAR PRIMARY KEY,
                scraped_date TIMESTAMP,
                scraped_content JSON,
                scraping_success BOOLEAN,
                error_message VARCHAR
            )
        """)
        
        count = subset_conn.execute("SELECT COUNT(*) FROM historical_jobs").fetchone()[0]
        print(f"  ✅ Created test database with {count} jobs")
        
        source_conn.close()
        subset_conn.close()
        
        return True
        
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False

def scrape_test_jobs():
    """Scrape the test jobs"""
    print("🕷️ Scraping test jobs...")
    
    subset_db = Path(__file__).parent.parent / "data" / "historical_jobs_test.duckdb"
    
    try:
        conn = duckdb.connect(str(subset_db))
        
        # Get control numbers
        control_numbers = conn.execute("SELECT control_number FROM historical_jobs LIMIT 20").fetchall()
        print(f"  📋 Scraping {len(control_numbers)} jobs for speed...")
        
        # Import scraping function
        sys.path.append(str(Path(__file__).parent / "scraping"))
        from scrape_enhanced_job_posting import scrape_enhanced_job_posting
        
        success_count = 0
        for i, (control_number,) in enumerate(control_numbers, 1):
            try:
                print(f"    📄 {i}/{len(control_numbers)}: {control_number}")
                
                result = scrape_enhanced_job_posting(str(control_number))
                
                conn.execute("""
                    INSERT OR REPLACE INTO scraped_jobs 
                    (control_number, scraped_date, scraped_content, scraping_success, error_message)
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
                print(f"      ❌ Error: {e}")
        
        conn.close()
        print(f"  ✅ Scraped {success_count}/{len(control_numbers)} jobs successfully")
        return True
        
    except Exception as e:
        print(f"  ❌ Scraping error: {e}")
        return False

def run_test_rationalization():
    """Run rationalization on test data"""
    print("🔄 Running test rationalization...")
    
    base_dir = Path(__file__).parent.parent
    data_dir = base_dir / "data"
    
    # Find current jobs
    current_files = list(data_dir.glob("current_jobs_*.json"))
    if not current_files:
        print("  ❌ No current jobs found")
        return False
    
    latest_current = max(current_files, key=os.path.getctime)
    subset_db = data_dir / "historical_jobs_test.duckdb"
    
    if not subset_db.exists():
        print("  ❌ No test database found")
        return False
    
    # Run rationalization
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
        
        result = subprocess.run(cmd, cwd=rationalization_script.parent, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"  ✅ Created: {output_file.name}")
            
            # Show stats
            conn = duckdb.connect(str(output_file))
            count = conn.execute("SELECT COUNT(*) FROM unified_jobs").fetchone()[0]
            sources = conn.execute("SELECT data_sources, COUNT(*) FROM unified_jobs GROUP BY data_sources").fetchall()
            conn.close()
            
            print(f"  📊 {count} total records")
            print(f"  📈 Sources breakdown:")
            for source, cnt in sources:
                print(f"    - {source}: {cnt}")
                
            return output_file.name
        else:
            print(f"  ❌ Rationalization failed:")
            print(f"    stdout: {result.stdout}")
            print(f"    stderr: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False

def main():
    print("🚀 QUICK TEST PIPELINE (Historical + Current + Scraping)")
    print("=" * 60)
    
    # Step 1: Create small historical subset
    if not create_small_historical_subset():
        return
    
    # Step 2: Scrape some jobs
    if not scrape_test_jobs():
        return
    
    # Step 3: Run rationalization
    unified_file = run_test_rationalization()
    if not unified_file:
        return
    
    print(f"\n✅ TEST COMPLETE!")
    print(f"📊 Unified dataset: {unified_file}")
    print(f"📝 Update QMD line 26 to: conn = duckdb.connect('data/{unified_file}')")
    print(f"🎯 Now run: quarto render rationalization_analysis.qmd")

if __name__ == "__main__":
    main()