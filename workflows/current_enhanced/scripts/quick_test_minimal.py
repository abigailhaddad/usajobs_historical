#!/usr/bin/env python3
"""
Minimal test: Pull 50 historical jobs + current jobs + test full pipeline
"""

import requests
import duckdb
import json
import sys
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
import time

def pull_recent_historical_jobs(num_jobs=50):
    """Pull a small number of recent historical jobs"""
    print(f"📊 Pulling {num_jobs} recent historical jobs...")
    
    # Use recent dates to get some jobs
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)  # Last 30 days
    
    api_url = "https://data.usajobs.gov/api/historicjoa"
    
    jobs = []
    for days_back in range(30):
        if len(jobs) >= num_jobs:
            break
            
        date = end_date - timedelta(days=days_back)
        date_str = date.strftime('%Y-%m-%d')
        
        try:
            response = requests.get(api_url, params={
                "StartPositionOpenDate": date_str,
                "EndPositionOpenDate": date_str
            })
            
            if response.status_code == 200:
                data = response.json()
                day_jobs = data.get("data", [])
                jobs.extend(day_jobs[:10])  # Max 10 per day
                print(f"  📅 {date_str}: {len(day_jobs)} jobs")
                
                if len(jobs) >= num_jobs:
                    jobs = jobs[:num_jobs]
                    break
            
            time.sleep(0.5)  # Be respectful
            
        except Exception as e:
            print(f"  ⚠️ Error for {date_str}: {e}")
            continue
    
    print(f"  ✅ Collected {len(jobs)} historical jobs")
    return jobs

def create_test_historical_db(jobs):
    """Create test historical database"""
    print("💾 Creating test historical database...")
    
    db_path = Path(__file__).parent.parent / "data" / "historical_jobs_test_minimal.duckdb"
    conn = duckdb.connect(str(db_path))
    
    # Drop existing table if it exists
    conn.execute("DROP TABLE IF EXISTS historical_jobs")
    conn.execute("DROP TABLE IF EXISTS scraped_jobs")
    
    # Create table
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
    
    # Insert jobs
    for job in jobs:
        try:
            # Extract basic fields
            control_number = job.get("usajobsControlNumber")
            if not control_number:
                continue
                
            conn.execute("""
                INSERT OR REPLACE INTO historical_jobs (
                    control_number, announcement_number, hiring_agency_name,
                    hiring_department_name, hiring_subelement_name, position_title,
                    minimum_grade, maximum_grade, minimum_salary, maximum_salary,
                    position_open_date, position_close_date, locations,
                    work_schedule, travel_requirement, job_series, raw_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                control_number,
                job.get("announcementNumber"),
                job.get("hiringAgencyName"),
                job.get("hiringDepartmentName"), 
                job.get("hiringSubelementName"),
                job.get("positionTitle"),
                job.get("minimumGrade"),
                job.get("maximumGrade"),
                job.get("minimumSalary"),
                job.get("maximumSalary"),
                job.get("positionOpenDate"),
                job.get("positionCloseDate"),
                " | ".join([f"{loc.get('positionLocationCity', '')}, {loc.get('positionLocationState', '')}" 
                           for loc in job.get("PositionLocations", [])]),
                job.get("workSchedule"),
                job.get("travelRequirement"),
                ", ".join([c.get("series", "") for c in job.get("JobCategories", [])]),
                json.dumps(job)
            ])
        except Exception as e:
            print(f"  ⚠️ Error inserting job {control_number}: {e}")
            continue
    
    # Add scraped_jobs table
    conn.execute("""
        CREATE TABLE scraped_jobs (
            control_number VARCHAR PRIMARY KEY,
            scraped_date TIMESTAMP,
            scraped_content JSON,
            scraping_success BOOLEAN,
            error_message VARCHAR
        )
    """)
    
    count = conn.execute("SELECT COUNT(*) FROM historical_jobs").fetchone()[0]
    conn.close()
    
    print(f"  ✅ Created database with {count} jobs")
    return db_path

def scrape_test_jobs(db_path, num_to_scrape=50):
    """Scrape a few jobs for testing"""
    print(f"🕷️ Scraping {num_to_scrape} jobs...")
    
    conn = duckdb.connect(str(db_path))
    
    # Get some control numbers
    control_numbers = conn.execute(f"SELECT control_number FROM historical_jobs LIMIT {num_to_scrape}").fetchall()
    
    # Import scraping function
    sys.path.append(str(Path(__file__).parent / "scraping"))
    from scrape_enhanced_job_posting import scrape_enhanced_job_posting
    
    success_count = 0
    for i, (control_number,) in enumerate(control_numbers, 1):
        try:
            print(f"  📄 {i}/{len(control_numbers)}: {control_number}")
            
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
            print(f"    ❌ Error: {e}")
    
    conn.close()
    print(f"  ✅ Successfully scraped {success_count}/{len(control_numbers)} jobs")

def fetch_current_jobs():
    """Fetch current jobs"""
    print("🌐 Fetching current jobs...")
    
    current_script = Path(__file__).parent / "api" / "fetch_current_jobs.py"
    
    cmd = [sys.executable, str(current_script), "--days-posted", "7"]
    result = subprocess.run(cmd, cwd=current_script.parent)
    
    return result.returncode == 0

def run_rationalization(historical_db):
    """Run field rationalization"""
    print("🔄 Running field rationalization...")
    
    base_dir = Path(__file__).parent.parent
    data_dir = base_dir / "data"
    
    # Find current jobs
    current_files = list(data_dir.glob("current_jobs_*.json"))
    if not current_files:
        print("  ❌ No current jobs found")
        return False
    
    latest_current = max(current_files, key=lambda x: x.stat().st_mtime)
    
    rationalization_script = base_dir / "scripts" / "integration" / "field_rationalization.py"
    output_file = data_dir / f"unified_minimal_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.duckdb"
    
    cmd = [
        sys.executable, str(rationalization_script),
        "--historical-db", str(historical_db),
        "--current-json", str(latest_current),
        "--output", str(output_file),
        "--output-format", "duckdb"
    ]
    
    result = subprocess.run(cmd, cwd=rationalization_script.parent, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"  ✅ Created: {output_file.name}")
        return output_file
    else:
        print(f"  ❌ Failed:")
        print(f"    {result.stderr}")
        return False

def update_qmd_and_render(unified_db_path):
    """Update QMD file with new database path and render it"""
    print("📝 Updating QMD file...")
    
    base_dir = Path(__file__).parent.parent
    qmd_file = base_dir / "rationalization_analysis.qmd"
    
    if not qmd_file.exists():
        print(f"  ❌ QMD file not found at {qmd_file}")
        return False
    
    # Read QMD content
    with open(qmd_file, 'r') as f:
        content = f.read()
    
    # Find and replace the database connection line
    import re
    db_name = unified_db_path.name
    new_line = f'conn = duckdb.connect("data/{db_name}")'
    
    # Pattern to match the connection line
    pattern = r'conn\s*=\s*duckdb\.connect\([\'"].*?[\'"]\)'
    content = re.sub(pattern, new_line, content)
    
    # Write updated content
    with open(qmd_file, 'w') as f:
        f.write(content)
    
    print(f"  ✅ Updated QMD to use: {db_name}")
    
    # Render the QMD
    print("🎨 Rendering QMD to HTML...")
    cmd = ["quarto", "render", str(qmd_file)]
    result = subprocess.run(cmd, cwd=base_dir, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"  ✅ Successfully rendered HTML")
        return True
    else:
        print(f"  ❌ Render failed:")
        print(f"    {result.stderr}")
        return False

def main():
    print("🚀 MINIMAL TEST PIPELINE")
    print("=" * 40)
    
    # Step 1: Pull recent historical jobs
    historical_jobs = pull_recent_historical_jobs(50)
    if not historical_jobs:
        print("❌ No historical jobs found")
        return
    
    # Step 2: Create test database
    db_path = create_test_historical_db(historical_jobs)
    
    # Step 3: Scrape all jobs
    scrape_test_jobs(db_path, 50)
    
    # Step 4: Fetch current jobs
    if not fetch_current_jobs():
        print("❌ Current jobs fetch failed")
        return
    
    # Step 5: Run rationalization
    unified_file = run_rationalization(db_path)
    if not unified_file:
        print("❌ Rationalization failed")
        return
    
    # Step 6: Update QMD and render
    if update_qmd_and_render(unified_file):
        print(f"\n✅ MINIMAL TEST COMPLETE!")
        print(f"📊 Unified dataset: {unified_file.name}")
        print(f"📄 HTML report has been generated!")
    else:
        print(f"\n⚠️ Pipeline complete but rendering failed")
        print(f"📊 Unified dataset: {unified_file.name}")
        print(f"📝 Manual update: Update QMD line 26 to: conn = duckdb.connect('data/{unified_file.name}')")
        print(f"🎯 Then run: quarto render rationalization_analysis.qmd")

if __name__ == "__main__":
    main()