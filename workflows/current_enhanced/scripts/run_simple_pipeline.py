#!/usr/bin/env python3
"""
Simple Enhanced USAJobs Pipeline

Runs pipeline combining historical API + current API + field rationalization.
Simple resume capability - just tracks which years are done.

Usage:
    python run_simple_pipeline.py --start-date 2024-01-01
    python run_simple_pipeline.py --current-only
"""

import argparse
import json
import os
import subprocess
import sys
import time
import duckdb
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def get_processed_years(start_year=None):
    """Check which years already have historical data in the ENHANCED workflow (only from start_year forward)"""
    enhanced_data_dir = Path(__file__).parent.parent / "data"
    processed_years = []
    
    for db_file in enhanced_data_dir.glob("historical_jobs_*.duckdb"):
        # Skip worker files
        if 'worker' in db_file.name:
            continue
            
        try:
            year_str = db_file.name.replace('historical_jobs_', '').replace('.duckdb', '')
            # Skip range files like "2015_2020"
            if '_' in year_str:
                continue
                
            year = int(year_str)
            
            # Only include years from start_year forward
            if start_year and year < start_year:
                continue
                
            # Check if database has data
            conn = duckdb.connect(str(db_file))
            count = conn.execute("SELECT COUNT(*) FROM historical_jobs").fetchone()[0]
            conn.close()
            
            if count > 0:
                processed_years.append(year)
        except:
            pass
    
    return sorted(processed_years)

def run_historical_years(start_date):
    """Run historical data collection for missing years"""
    start_year = int(start_date[:4])
    current_year = datetime.now().year
    
    needed_years = list(range(start_year, current_year + 1))
    processed_years = get_processed_years(start_year)
    
    years_to_process = [y for y in needed_years if y not in processed_years]
    
    if not years_to_process:
        print(f"📊 All years {start_year}-{current_year} already processed")
        return True
    
    print(f"📈 Need to process years: {years_to_process}")
    print(f"✅ Already have years: {processed_years}")
    
    historical_script = Path(__file__).parent.parent.parent.parent / "shared" / "api" / "historic_pull_parallel.py"
    data_dir = Path(__file__).parent.parent / "data"
    
    for year in years_to_process:
        print(f"\n🚀 Processing {year}...")
        
        try:
            cmd = [
                sys.executable, str(historical_script),
                "--start-date", f"{year}-01-01",
                "--end-date", f"{year}-12-31",
                "--output-dir", str(data_dir)
            ]
            
            result = subprocess.run(cmd, cwd=historical_script.parent)
            
            if result.returncode == 0:
                print(f"✅ Completed {year}")
            else:
                print(f"❌ Failed {year}")
                return False
                
        except Exception as e:
            print(f"❌ Error processing {year}: {e}")
            return False
    
    return True

def scrape_single_job(control_number, db_path):
    """Scrape a single job posting - designed for parallel execution"""
    try:
        # Import the scraping function
        import sys
        from pathlib import Path
        base_dir = Path(__file__).parent.parent
        scraping_script = base_dir / "scripts" / "scraping" / "scrape_enhanced_job_posting.py"
        sys.path.append(str(scraping_script.parent))
        from scrape_enhanced_job_posting import scrape_enhanced_job_posting
        
        # Check if already scraped
        conn = duckdb.connect(str(db_path))
        existing = conn.execute(
            "SELECT control_number FROM scraped_jobs WHERE control_number = ?", 
            [control_number]
        ).fetchone()
        
        if existing:
            conn.close()
            return {'control_number': control_number, 'status': 'already_scraped', 'data': None, 'error': None}
        
        # Scrape the job posting
        scraped_data = scrape_enhanced_job_posting(control_number)
        
        # Save result to database
        if scraped_data:
            conn.execute("""
                INSERT INTO scraped_jobs 
                (control_number, scraped_date, scraped_content, scraping_success, error_message)
                VALUES (?, ?, ?, ?, ?)
            """, [
                control_number,
                datetime.now().isoformat(),
                json.dumps(scraped_data),
                True,
                None
            ])
            conn.close()
            return {'control_number': control_number, 'status': 'success', 'data': scraped_data, 'error': None}
        else:
            conn.execute("""
                INSERT INTO scraped_jobs 
                (control_number, scraped_date, scraped_content, scraping_success, error_message)
                VALUES (?, ?, ?, ?, ?)
            """, [
                control_number,
                datetime.now().isoformat(),
                None,
                False,
                "No data returned"
            ])
            conn.close()
            return {'control_number': control_number, 'status': 'no_data', 'data': None, 'error': "No data returned"}
            
    except Exception as e:
        try:
            conn = duckdb.connect(str(db_path))
            conn.execute("""
                INSERT INTO scraped_jobs 
                (control_number, scraped_date, scraped_content, scraping_success, error_message)
                VALUES (?, ?, ?, ?, ?)
            """, [
                control_number,
                datetime.now().isoformat(),
                None,
                False,
                str(e)
            ])
            conn.close()
        except:
            pass
        return {'control_number': control_number, 'status': 'error', 'data': None, 'error': str(e)}

def run_scraping(start_date):
    """Run parallel scraping on historical jobs to get enhanced content"""
    print(f"\n🕷️ Running enhanced scraping with 16 parallel workers...")
    
    base_dir = Path(__file__).parent.parent
    data_dir = base_dir / "data"
    
    # Get historical databases to scrape
    start_year = int(start_date[:4]) if start_date else datetime.now().year
    historical_dbs = []
    
    for db_file in data_dir.glob("historical_jobs_*.duckdb"):
        # Skip worker files and old years
        if 'worker' in db_file.name:
            continue
        try:
            year = int(db_file.name.replace('historical_jobs_', '').replace('.duckdb', ''))
            if year >= start_year:
                historical_dbs.append(db_file)
        except:
            continue
    
    if not historical_dbs:
        print("  ⚠️ No historical data found to scrape")
        return True
    
    # Process each historical database
    for db_file in historical_dbs:
        year = db_file.name.replace('historical_jobs_', '').replace('.duckdb', '')
        print(f"\n  🔍 Scraping jobs from {year}...")
        
        try:
            conn = duckdb.connect(str(db_file))
            
            # Get all control numbers from historical jobs (NO LIMIT!)
            control_numbers = conn.execute("""
                SELECT DISTINCT control_number 
                FROM historical_jobs 
                WHERE control_number IS NOT NULL
            """).fetchall()
            
            if not control_numbers:
                print(f"    ⚠️ No control numbers found in {db_file.name}")
                conn.close()
                continue
                
            print(f"    📋 Found {len(control_numbers)} jobs to scrape")
            
            # Create scraped_jobs table if it doesn't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS scraped_jobs (
                    control_number VARCHAR PRIMARY KEY,
                    scraped_date TIMESTAMP,
                    scraped_content JSON,
                    scraping_success BOOLEAN,
                    error_message VARCHAR
                )
            """)
            conn.close()
            
            # Parallel scraping with 16 workers
            success_count = 0
            error_count = 0
            already_scraped_count = 0
            no_data_count = 0
            
            print(f"    🚀 Starting parallel scraping with 16 workers...")
            
            with ThreadPoolExecutor(max_workers=16) as executor:
                # Submit all jobs
                future_to_control = {
                    executor.submit(scrape_single_job, control_number[0], db_file): control_number[0] 
                    for control_number in control_numbers
                }
                
                # Process completed jobs
                for i, future in enumerate(as_completed(future_to_control), 1):
                    try:
                        result = future.result()
                        
                        if result['status'] == 'success':
                            success_count += 1
                        elif result['status'] == 'already_scraped':
                            already_scraped_count += 1
                        elif result['status'] == 'no_data':
                            no_data_count += 1
                        else:
                            error_count += 1
                        
                        # Progress update every 100 jobs
                        if i % 100 == 0:
                            print(f"    📊 Progress: {i}/{len(control_numbers)} ({success_count} success, {error_count} errors, {already_scraped_count} skipped)")
                        
                        # Be respectful to the server - small delay between batches
                        if i % 50 == 0:
                            time.sleep(1)
                            
                    except Exception as e:
                        error_count += 1
                        print(f"    ❌ Future error: {e}")
            
            print(f"    📊 Scraping complete for {year}:")
            print(f"      ✅ Success: {success_count}")
            print(f"      ❌ Errors: {error_count}")
            print(f"      ⏭️ Already scraped: {already_scraped_count}")
            print(f"      📭 No data: {no_data_count}")
            print(f"      📊 Total processed: {len(control_numbers)}")
            
        except Exception as e:
            print(f"    ❌ Error processing {db_file.name}: {e}")
            continue
    
    print(f"  ✅ Enhanced parallel scraping completed!")
    return True

def fetch_current_jobs():
    """Fetch current jobs from API"""
    print(f"\n📡 Fetching current jobs...")
    
    current_script = Path(__file__).parent / "api" / "fetch_current_jobs.py"
    
    try:
        cmd = [
            sys.executable, str(current_script),
            "--days-posted", "30",
            "--max-pages", "10"
        ]
        
        result = subprocess.run(cmd, cwd=current_script.parent)
        
        if result.returncode == 0:
            print(f"✅ Current jobs fetched")
            return True
        else:
            print(f"❌ Current jobs fetch failed")
            return False
            
    except Exception as e:
        print(f"❌ Error fetching current jobs: {e}")
        return False

def run_rationalization(start_date=None):
    """Run field rationalization - creates ONE unified file"""
    print(f"\n🔄 Running field rationalization...")
    
    base_dir = Path(__file__).parent.parent
    data_dir = base_dir / "data"
    
    # Get latest current jobs file
    current_files = list(data_dir.glob("current_jobs_*.json"))
    if not current_files:
        print("❌ No current jobs files found")
        return False
    
    latest_current = max(current_files, key=os.path.getctime)
    print(f"📄 Using current jobs: {latest_current.name}")
    
    # Get historical databases - only from start_date forward (from ENHANCED workflow)
    start_year = int(start_date[:4]) if start_date else datetime.now().year
    enhanced_data_dir = base_dir / "data"
    historical_dbs = []
    
    for db_file in enhanced_data_dir.glob("historical_jobs_*.duckdb"):
        # Skip worker files and old years
        if 'worker' in db_file.name:
            continue
        try:
            year = int(db_file.name.replace('historical_jobs_', '').replace('.duckdb', ''))
            if year >= start_year:
                historical_dbs.append(db_file)
        except:
            continue
    
    rationalization_script = base_dir / "scripts" / "integration" / "field_rationalization.py"
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Create ONE unified DuckDB file with all relevant data
    if historical_dbs:
        # Use the most recent historical database as base
        latest_hist_db = max(historical_dbs, key=lambda x: int(x.name.replace('historical_jobs_', '').replace('.duckdb', '')))
        output_file = data_dir / f"unified_enhanced_{timestamp}.duckdb"
        
        try:
            cmd = [
                sys.executable, str(rationalization_script),
                "--historical-db", str(latest_hist_db),
                "--current-json", str(latest_current),
                "--output", str(output_file),
                "--output-format", "duckdb",
                "--limit", "100000"  # Process all available data
            ]
            
            # Add date filter if we have a start_date
            if start_date:
                cmd.extend(["--min-date", start_date])
            
            print(f"  🔄 Creating unified enhanced dataset (historical + current)...")
            result = subprocess.run(cmd, cwd=rationalization_script.parent)
            
            if result.returncode == 0:
                print(f"  ✅ Created {output_file.name}")
                
                # Clean up old unified files (both JSON and DuckDB)
                for old_file in data_dir.glob("unified_*.*"):
                    if old_file != output_file and old_file.suffix in ['.json', '.duckdb']:
                        old_file.unlink()
                print(f"  🧹 Cleaned up old unified files")
            else:
                print(f"  ❌ Failed to create unified dataset")
                
        except Exception as e:
            print(f"  ❌ Error creating unified dataset: {e}")
    else:
        # No historical data, just process current
        output_file = data_dir / f"unified_enhanced_{timestamp}.duckdb"
        
        try:
            cmd = [
                sys.executable, str(rationalization_script),
                "--current-json", str(latest_current),
                "--output", str(output_file),
                "--output-format", "duckdb",
                "--limit", "100000"
            ]
            
            if start_date:
                cmd.extend(["--min-date", start_date])
            
            print(f"  🔄 Creating unified enhanced dataset (current only)...")
            result = subprocess.run(cmd, cwd=rationalization_script.parent)
            
            if result.returncode == 0:
                print(f"  ✅ Created {output_file.name}")
            else:
                print(f"  ❌ Failed to create unified dataset")
                
        except Exception as e:
            print(f"  ❌ Error creating unified dataset: {e}")
    
    return True

def create_summary(start_date=None):
    """Show summary of what we have"""
    print(f"\n📊 PIPELINE SUMMARY")
    print("=" * 30)
    
    # Only show historical data from start_date forward
    start_year = int(start_date[:4]) if start_date else None
    processed_years = get_processed_years(start_year)
    
    if processed_years:
        year_list = ', '.join(map(str, processed_years))
        print(f"📈 Historical years (enhanced workflow): {year_list}")
        
        # Count total historical records for enhanced workflow only
        total_historical = 0
        enhanced_data_dir = Path(__file__).parent.parent / "data"
        for year in processed_years:
            db_file = enhanced_data_dir / f"historical_jobs_{year}.duckdb"
            try:
                conn = duckdb.connect(str(db_file))
                count = conn.execute("SELECT COUNT(*) FROM historical_jobs").fetchone()[0]
                total_historical += count
                conn.close()
                print(f"   {year}: {count:,} jobs")
            except:
                pass
        
        print(f"📊 Total enhanced historical records: {total_historical:,}")
        if start_date:
            print(f"   (Only showing {start_date[:4]}+ data for enhanced workflow)")
    else:
        print(f"📈 Historical years (enhanced workflow): None")
    
    # Current jobs
    data_dir = Path(__file__).parent.parent / "data"
    current_files = list(data_dir.glob("current_jobs_*.json"))
    if current_files:
        latest_current = max(current_files, key=os.path.getctime)
        try:
            with open(latest_current) as f:
                data = json.load(f)
            current_count = data.get('SearchResult', {}).get('SearchResultCount', 0)
            print(f"📡 Current jobs: {current_count:,} (from {latest_current.name})")
        except:
            print(f"📡 Current jobs: Available")
    else:
        print(f"📡 Current jobs: None")
    
    # Unified datasets (DuckDB)
    unified_files = list(data_dir.glob("unified_*.duckdb"))
    if unified_files:
        print(f"🔄 Unified datasets: {len(unified_files)} DuckDB files")
        
        # Show latest
        latest_unified = max(unified_files, key=os.path.getctime)
        try:
            conn = duckdb.connect(str(latest_unified))
            record_count = conn.execute("SELECT COUNT(*) FROM unified_jobs").fetchone()[0]
            conn.close()
            print(f"📋 Latest unified: {record_count:,} records ({latest_unified.name})")
        except:
            print(f"📋 Latest unified: {latest_unified.name}")
    else:
        print(f"🔄 Unified datasets: None")

def main():
    parser = argparse.ArgumentParser(description='Simple enhanced USAJobs pipeline')
    parser.add_argument('--start-date', help='Start date for historical data (YYYY-MM-DD)')
    parser.add_argument('--current-only', action='store_true', help='Only fetch current jobs, skip historical')
    parser.add_argument('--summary-only', action='store_true', help='Just show summary of existing data')
    
    args = parser.parse_args()
    
    if args.summary_only:
        create_summary(args.start_date)
        return 0
    
    if not args.current_only and not args.start_date:
        print("❌ Need --start-date or --current-only")
        return 1
    
    if args.start_date:
        try:
            datetime.strptime(args.start_date, '%Y-%m-%d')
        except ValueError:
            print("❌ Invalid date format. Use YYYY-MM-DD")
            return 1
    
    print("🚀 SIMPLE ENHANCED PIPELINE")
    print("=" * 35)
    
    success = True
    
    # Step 1: Historical data (if requested)
    if not args.current_only:
        success = success and run_historical_years(args.start_date)
        
        # Step 2: Enhanced scraping of historical data
        if success:
            success = success and run_scraping(args.start_date)
    
    # Step 3: Current jobs
    success = success and fetch_current_jobs()
    
    # Step 4: Rationalization (combines historical + scraped + current)
    success = success and run_rationalization(args.start_date)
    
    # Step 5: Summary
    create_summary(args.start_date)
    
    if success:
        print(f"\n✅ Pipeline completed successfully!")
    else:
        print(f"\n❌ Pipeline had errors")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())