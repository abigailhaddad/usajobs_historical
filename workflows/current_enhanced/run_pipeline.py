#!/usr/bin/env python3
"""
Enhanced USAJobs Data Pipeline

A complete pipeline that:
1. Fetches current job postings from USAJobs API
2. Scrapes detailed content from job posting pages  
3. Integrates with historical job data
4. Rationalizes fields between data sources
5. Generates comprehensive analysis report

Usage:
    python run_pipeline.py [options]

Options:
    --historical-jobs N     Number of recent historical jobs to process (default: 50)
    --scrape-jobs N        Number of jobs to scrape (default: all historical jobs)
    --current-days N       Days back for current job search (default: 7)
    --output-name NAME     Custom name for output files (default: auto-generated)
    --skip-scraping        Skip the job scraping step (faster for testing)
    --render-report        Automatically render HTML report after pipeline
"""

import argparse
import subprocess
import sys
import os
import duckdb
import json
import requests
import time
from pathlib import Path
from datetime import datetime, timedelta


def setup_directories():
    """Ensure required directories exist"""
    base_dir = Path(__file__).parent
    directories = ['data', 'logs']
    
    for dir_name in directories:
        dir_path = base_dir / dir_name
        dir_path.mkdir(exist_ok=True)
    
    return base_dir


def fetch_recent_historical_jobs(num_jobs=50):
    """Fetch recent historical jobs from the USAJobs historical API"""
    print(f"📊 Fetching {num_jobs} recent historical jobs...")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
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


def create_historical_database(jobs, output_name):
    """Create historical jobs database"""
    print("💾 Creating historical jobs database...")
    
    base_dir = Path(__file__).parent
    db_path = base_dir / "data" / f"historical_jobs_{output_name}.duckdb"
    
    conn = duckdb.connect(str(db_path))
    
    # Drop existing tables
    conn.execute("DROP TABLE IF EXISTS historical_jobs")
    conn.execute("DROP TABLE IF EXISTS scraped_jobs")
    
    # Create historical jobs table
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
    
    # Create scraped jobs table
    conn.execute("""
        CREATE TABLE scraped_jobs (
            control_number VARCHAR PRIMARY KEY,
            scraped_date TIMESTAMP,
            scraped_content JSON,
            scraping_success BOOLEAN,
            error_message VARCHAR
        )
    """)
    
    # Insert historical jobs
    successful_inserts = 0
    for job in jobs:
        try:
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
            successful_inserts += 1
        except Exception as e:
            print(f"  ⚠️ Error inserting job {control_number}: {e}")
            continue
    
    conn.close()
    print(f"  ✅ Created database with {successful_inserts} jobs")
    return db_path


def fetch_current_jobs(days_posted=7):
    """Fetch current jobs using the API script"""
    print("🌐 Fetching current jobs...")
    
    current_script = Path(__file__).parent / "scripts" / "api" / "fetch_current_jobs.py"
    
    cmd = [sys.executable, str(current_script), "--days-posted", str(days_posted)]
    result = subprocess.run(cmd, cwd=current_script.parent)
    
    return result.returncode == 0


def scrape_jobs(db_path, num_to_scrape=None):
    """Scrape job postings for enhanced content"""
    print(f"🕷️ Scraping job postings...")
    
    conn = duckdb.connect(str(db_path))
    
    # Get control numbers to scrape
    if num_to_scrape:
        control_numbers = conn.execute(f"SELECT control_number FROM historical_jobs LIMIT {num_to_scrape}").fetchall()
    else:
        control_numbers = conn.execute("SELECT control_number FROM historical_jobs").fetchall()
    
    print(f"  📄 Scraping {len(control_numbers)} jobs...")
    
    # Import scraping function
    sys.path.append(str(Path(__file__).parent / "scripts" / "scraping"))
    from scrape_enhanced_job_posting import scrape_enhanced_job_posting
    
    success_count = 0
    for i, (control_number,) in enumerate(control_numbers, 1):
        try:
            print(f"  📄 {i}/{len(control_numbers)}: {control_number}")
            
            # Check if already scraped
            existing = conn.execute("SELECT control_number FROM scraped_jobs WHERE control_number = ?", [str(control_number)]).fetchone()
            if existing:
                success_count += 1
                continue
            
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
            print(f"    ❌ Error scraping {control_number}: {e}")
    
    conn.close()
    print(f"  ✅ Successfully scraped {success_count}/{len(control_numbers)} jobs")


def run_field_rationalization(historical_db, output_name):
    """Run field rationalization to create unified dataset"""
    print("🔄 Running field rationalization...")
    
    base_dir = Path(__file__).parent
    data_dir = base_dir / "data"
    
    # Find current jobs file
    current_files = list(data_dir.glob("current_jobs_*.json"))
    if not current_files:
        print("  ❌ No current jobs files found")
        return None
    
    latest_current = max(current_files, key=os.path.getctime)
    
    rationalization_script = base_dir / "scripts" / "integration" / "field_rationalization.py"
    output_file = data_dir / f"unified_{output_name}.duckdb"
    
    cmd = [
        sys.executable, str(rationalization_script),
        "--historical-db", str(historical_db),
        "--current-json", str(latest_current),
        "--output", str(output_file),
        "--output-format", "duckdb"
    ]
    
    result = subprocess.run(cmd, cwd=rationalization_script.parent, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"  ✅ Created unified dataset: {output_file.name}")
        return output_file
    else:
        print(f"  ❌ Rationalization failed:")
        print(f"    {result.stderr}")
        return None


def render_analysis_report(unified_db_path):
    """Update and render the analysis report"""
    print("📝 Generating analysis report...")
    
    base_dir = Path(__file__).parent
    qmd_file = base_dir / "rationalization_analysis.qmd"
    
    if not qmd_file.exists():
        print(f"  ❌ QMD file not found at {qmd_file}")
        return False
    
    # Update QMD to use the new database
    with open(qmd_file, 'r') as f:
        content = f.read()
    
    import re
    db_name = unified_db_path.name
    new_line = f'conn = duckdb.connect("data/{db_name}")'
    pattern = r'conn\s*=\s*duckdb\.connect\([\'"].*?[\'"]\)'
    content = re.sub(pattern, new_line, content)
    
    with open(qmd_file, 'w') as f:
        f.write(content)
    
    print(f"  ✅ Updated QMD to use: {db_name}")
    
    # Render to HTML
    print("🎨 Rendering HTML report...")
    cmd = ["quarto", "render", str(qmd_file)]
    result = subprocess.run(cmd, cwd=base_dir, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"  ✅ Report rendered successfully")
        return True
    else:
        print(f"  ❌ Render failed:")
        print(f"    {result.stderr}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Enhanced USAJobs Data Pipeline')
    parser.add_argument('--historical-jobs', type=int, default=50, 
                       help='Number of recent historical jobs to process (default: 50)')
    parser.add_argument('--scrape-jobs', type=int, 
                       help='Number of jobs to scrape (default: all historical jobs)')
    parser.add_argument('--current-days', type=int, default=7,
                       help='Days back for current job search (default: 7)')
    parser.add_argument('--output-name', 
                       help='Custom name for output files (default: auto-generated)')
    parser.add_argument('--skip-scraping', action='store_true',
                       help='Skip the job scraping step (faster for testing)')
    parser.add_argument('--render-report', action='store_true',
                       help='Automatically render HTML report after pipeline')
    
    args = parser.parse_args()
    
    # Setup
    base_dir = setup_directories()
    
    # Generate output name if not provided
    if not args.output_name:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        args.output_name = f"pipeline_{timestamp}"
    
    print("🚀 ENHANCED USAJOBS DATA PIPELINE")
    print("=" * 50)
    print(f"📊 Historical jobs: {args.historical_jobs}")
    print(f"🕷️ Scraping: {'Skipped' if args.skip_scraping else (args.scrape_jobs or 'All historical jobs')}")
    print(f"📅 Current jobs: Last {args.current_days} days")
    print(f"📁 Output name: {args.output_name}")
    print("=" * 50)
    
    try:
        # Step 1: Fetch historical jobs
        historical_jobs = fetch_recent_historical_jobs(args.historical_jobs)
        if not historical_jobs:
            print("❌ No historical jobs found")
            return 1
        
        # Step 2: Create historical database
        historical_db = create_historical_database(historical_jobs, args.output_name)
        
        # Step 3: Scrape jobs (optional)
        if not args.skip_scraping:
            scrape_jobs(historical_db, args.scrape_jobs)
        
        # Step 4: Fetch current jobs
        if not fetch_current_jobs(args.current_days):
            print("❌ Current jobs fetch failed")
            return 1
        
        # Step 5: Run field rationalization
        unified_db = run_field_rationalization(historical_db, args.output_name)
        if not unified_db:
            print("❌ Field rationalization failed")
            return 1
        
        # Step 6: Render report (optional)
        if args.render_report:
            render_analysis_report(unified_db)
        
        print(f"\n✅ PIPELINE COMPLETE!")
        print(f"📊 Unified dataset: {unified_db.name}")
        print(f"📁 Location: {unified_db}")
        
        if args.render_report:
            print(f"📄 HTML report: rationalization_analysis.html")
        else:
            print(f"💡 To generate report: python run_pipeline.py --render-report")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⚠️ Pipeline interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())