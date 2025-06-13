#!/usr/bin/env python3
"""
Parallel USAJobs Pipeline with Parquet Storage

Uses Parquet files instead of DuckDB for lock-free parallel processing.
Each worker can write to separate files that are merged later.
"""

import argparse
import multiprocessing as mp
import json
from datetime import datetime, timedelta
from pathlib import Path
import sys
import time
import pandas as pd
from tqdm import tqdm

sys.path.append('scripts')
from parquet_storage import ParquetJobStorage
from fetch_historical_jobs import fetch_jobs_by_date_range
from fetch_current_jobs import fetch_all_jobs
from scrape_enhanced_job_posting import scrape_enhanced_job_posting
from field_rationalization import FieldRationalizer

def scrape_single_job(args):
    """
    Worker function for parallel scraping of individual jobs
    """
    control_number, = args
    
    try:
        scraped_data = scrape_enhanced_job_posting(control_number)
        
        if scraped_data.get('status') == 'success':
            content_sections = scraped_data.get('content_sections', {})
            if content_sections and len(content_sections) > 0:
                print(f"✅ {control_number} - {len(content_sections)} sections")
                return control_number, scraped_data
            else:
                print(f"⚠️ {control_number} - success but no content sections")
                return control_number, None
        else:
            error_msg = scraped_data.get('error', 'Unknown error')
            print(f"❌ {control_number} - {error_msg}")
            return control_number, None
            
    except Exception as e:
        print(f"❌ {control_number} - Exception: {e}")
        return control_number, None

def parallel_scrape_jobs(historical_jobs, storage, scrape_workers):
    """
    Parallelize scraping across individual jobs
    """
    # Check which jobs already have scraping data
    already_scraped = storage.get_control_numbers_with_scraping()
    print(f"📋 {len(already_scraped)} jobs already have scraping data")
    
    # Find jobs that need scraping
    jobs_to_scrape = []
    for job in historical_jobs:
        control_number = str(job.get('usajobsControlNumber', '') or job.get('control_number', ''))
        if control_number not in already_scraped:
            jobs_to_scrape.append((job, control_number))
    
    if not jobs_to_scrape:
        print("⏭️ All jobs already scraped")
        return historical_jobs
    
    print(f"🕷️ Scraping {len(jobs_to_scrape)} jobs with {scrape_workers} workers...")
    
    # Prepare arguments for parallel scraping
    scrape_args = [(control_number,) for job, control_number in jobs_to_scrape]
    
    # Run parallel scraping
    scraped_results = {}
    with mp.Pool(scrape_workers) as pool:
        results = pool.map(scrape_single_job, scrape_args)
        
        for control_number, scraped_data in results:
            if scraped_data:
                scraped_results[control_number] = scraped_data
    
    # Add scraped content to jobs
    scraped_count = 0
    for job in historical_jobs:
        control_number = str(job.get('usajobsControlNumber', '') or job.get('control_number', ''))
        if control_number in scraped_results:
            job['scraped_content'] = scraped_results[control_number]
            scraped_count += 1
    
    print(f"✅ Successfully scraped {scraped_count}/{len(jobs_to_scrape)} jobs")
    return historical_jobs

def fetch_and_scrape_batch(args):
    """
    Worker function for parallel processing
    Fetches historical jobs for a date range and scrapes them
    """
    start_date, end_date, batch_id, base_path, scrape_jobs = args
    
    print(f"🏃 Worker {batch_id}: Processing {start_date} to {end_date}")
    
    try:
        # Initialize storage for this worker
        storage = ParquetJobStorage(base_path)
        
        # Fetch historical jobs for this date range  
        historical_jobs = fetch_jobs_by_date_range(
            start_date=start_date,
            end_date=end_date,
            output_file=None  # We'll handle storage ourselves
        )
        
        if not historical_jobs:
            print(f"⚠️ Worker {batch_id}: No jobs found for {start_date} to {end_date}")
            return batch_id, 0, 0
        
        print(f"📊 Worker {batch_id}: Found {len(historical_jobs)} historical jobs")
        
        # Scrape jobs if requested
        scraped_count = 0
        failed_scraping = []
        if scrape_jobs:
            already_scraped = storage.get_control_numbers_with_scraping()
            print(f"📋 Worker {batch_id}: {len(already_scraped)} jobs already have scraping data")
            
            jobs_to_scrape = [job for job in historical_jobs 
                             if str(job.get('usajobsControlNumber', '') or job.get('control_number', '')) not in already_scraped]
            
            if jobs_to_scrape:
                print(f"🕷️ Worker {batch_id}: Scraping {len(jobs_to_scrape)} jobs...")
                
                with tqdm(jobs_to_scrape, desc=f"Worker {batch_id}", leave=False) as pbar:
                    for job in pbar:
                        # Historical API uses 'usajobsControlNumber' not 'control_number'
                        control_number = str(job.get('usajobsControlNumber', '') or job.get('control_number', ''))
                        pbar.set_description(f"Worker {batch_id}: {control_number}")
                        
                        scraped_data = scrape_enhanced_job_posting(control_number)
                        
                        if scraped_data.get('status') == 'success':
                            # Verify we actually got content
                            content_sections = scraped_data.get('content_sections', {})
                            if content_sections and len(content_sections) > 0:
                                job['scraped_content'] = scraped_data
                                scraped_count += 1
                                print(f"✅ Worker {batch_id}: {control_number} - {len(content_sections)} sections")
                            else:
                                failed_scraping.append(control_number)
                                print(f"⚠️ Worker {batch_id}: {control_number} - success but no content sections")
                        else:
                            failed_scraping.append(control_number)
                            error_msg = scraped_data.get('error', 'Unknown error')
                            print(f"❌ Worker {batch_id}: {control_number} - {error_msg}")
                        
                        # Small delay to be respectful
                        time.sleep(0.5)
                        
                # Report scraping results
                if failed_scraping:
                    print(f"⚠️ Worker {batch_id}: {len(failed_scraping)} jobs failed scraping: {failed_scraping}")
            else:
                print(f"⏭️ Worker {batch_id}: All jobs already scraped")
        
        # Save batch to Parquet
        storage.save_historical_jobs(historical_jobs, f"{batch_id}_{start_date}_{end_date}")
        
        print(f"✅ Worker {batch_id}: Completed - {len(historical_jobs)} jobs, {scraped_count} scraped")
        return batch_id, len(historical_jobs), scraped_count
        
    except Exception as e:
        print(f"❌ Worker {batch_id}: Error - {str(e)}")
        return batch_id, 0, 0

def create_date_batches(start_date: str, num_workers: int = 4):
    """
    Split date range into batches for parallel processing
    """
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.now()
    
    total_days = (end - start).days
    days_per_batch = max(1, total_days // num_workers)
    
    batches = []
    current_date = start
    batch_id = 0
    
    while current_date < end:
        batch_end = min(current_date + timedelta(days=days_per_batch), end)
        
        batches.append((
            current_date.strftime('%Y-%m-%d'),
            batch_end.strftime('%Y-%m-%d'),
            batch_id,
        ))
        
        current_date = batch_end + timedelta(days=1)
        batch_id += 1
    
    return batches

def run_pipeline(start_date: str, base_path: str, scrape_jobs: bool = True, scrape_workers: int = 4):
    """
    Run the USAJobs pipeline with parallel scraping
    """
    print(f"🚀 USAJOBS PIPELINE (Parallel Scraping)")
    print("=" * 60)
    print(f"📊 Start date: {start_date}")
    print(f"🕷️ Scraping: {'Yes' if scrape_jobs else 'No'}")
    print(f"🕷️ Scrape workers: {scrape_workers if scrape_jobs else 'N/A'}")
    print(f"📁 Storage: {base_path}")
    print("=" * 60)
    
    # Initialize storage
    storage = ParquetJobStorage(base_path)
    
    # Fetch current jobs first (single-threaded since it's one API call)
    print("\n📊 Fetching current API jobs...")
    # Use 500 results per page for faster fetching (default is smaller)
    current_params = {
        'ResultsPerPage': 500
    }
    current_jobs = fetch_all_jobs(current_params)
    if current_jobs:
        storage.save_current_jobs(current_jobs)
        print(f"✅ Saved {len(current_jobs)} current jobs")
    else:
        print("⚠️ No current jobs fetched")
    
    # Fetch all historical jobs sequentially (more reliable than parallel API calls)
    print(f"\n📅 Fetching historical jobs...")
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    historical_jobs = fetch_jobs_by_date_range(
        start_date=start_date,
        end_date=end_date,
        output_file=None
    )
    
    if not historical_jobs:
        print("⚠️ No historical jobs found")
        return storage
    
    print(f"📊 Found {len(historical_jobs)} historical jobs")
    
    # Parallel scraping if requested
    if scrape_jobs:
        historical_jobs = parallel_scrape_jobs(historical_jobs, storage, scrape_workers)
    
    # Save all historical jobs
    batch_id = f"0_{start_date}_{end_date}"
    storage.save_historical_jobs(historical_jobs, batch_id)
    
    return storage

def run_rationalization(storage: ParquetJobStorage):
    """
    Run field rationalization on the collected data
    """
    print(f"\n🔄 STARTING FIELD RATIONALIZATION")
    print("=" * 40)
    
    # Load data
    historical_df = storage.load_historical_jobs()
    current_df = storage.load_current_jobs()
    
    if historical_df.empty and current_df.empty:
        print("❌ No data to rationalize!")
        return
    
    print(f"📊 Historical jobs: {len(historical_df)}")
    print(f"📊 Current jobs: {len(current_df)}")
    
    # Convert DataFrames to lists of dicts for rationalization
    historical_jobs = historical_df.to_dict('records') if not historical_df.empty else []
    current_jobs = current_df.to_dict('records') if not current_df.empty else []
    
    # Parse JSON fields back to objects
    for job in historical_jobs:
        if 'scraped_sections' in job and job['scraped_sections']:
            try:
                job['scraped_content'] = {
                    'content_sections': json.loads(job['scraped_sections'])
                }
                if 'scraped_metadata' in job and job['scraped_metadata']:
                    metadata = json.loads(job['scraped_metadata'])
                    job['scraped_content'].update(metadata)
            except json.JSONDecodeError:
                pass
    
    # Flatten current jobs - extract MatchedObjectDescriptor content
    flattened_current_jobs = []
    for job in current_jobs:
        # Extract the actual job data from MatchedObjectDescriptor
        descriptor = job.get('MatchedObjectDescriptor', {})
        if descriptor:
            # Add the MatchedObjectId to the descriptor for lookup
            descriptor['MatchedObjectId'] = job.get('MatchedObjectId')
            flattened_current_jobs.append(descriptor)
    
    # Parse any JSON string fields in the flattened data
    for job in flattened_current_jobs:
        for field in ['PositionFormattedDescription', 'ApplicationDetails', 'UserArea']:
            if field in job and isinstance(job[field], str):
                try:
                    job[field] = json.loads(job[field])
                except json.JSONDecodeError:
                    pass
    
    # Run rationalization - manually implement the logic from field_rationalization.py
    rationalizer = FieldRationalizer()
    
    # Create lookup for current jobs by control number
    current_lookup = {}
    for job in flattened_current_jobs:
        control_num = str(job.get('MatchedObjectId', ''))
        if control_num:
            current_lookup[control_num] = job
    
    rationalized_records = []
    overlap_samples = []
    processed_control_numbers = set()
    duplicate_count = 0
    
    # Process historical records first
    for hist_record in historical_jobs:
        control_num = str(hist_record.get('usajobsControlNumber', ''))
        if control_num and control_num not in processed_control_numbers:
            current_record = current_lookup.get(control_num)
            
            if current_record:
                duplicate_count += 1
                
                # Save overlap sample
                hist_mapped = rationalizer._map_fields(hist_record, 'historical_to_unified')
                
                # Add scraped content if available
                if hist_record.get('scraped_content'):
                    scraped_mapped = rationalizer._map_fields(hist_record['scraped_content'], 'scraped_to_unified')
                    scraped_content = rationalizer._extract_scraped_content(hist_record['scraped_content'])
                    scraped_mapped.update(scraped_content)
                    
                    # Add scraped data where historical doesn't have it
                    for field, value in scraped_mapped.items():
                        if field not in hist_mapped or not hist_mapped[field]:
                            hist_mapped[field] = value
                
                hist_sample = {k: v for k, v in hist_mapped.items() if k in rationalizer.unified_schema}
                hist_sample['control_number'] = control_num
                hist_sample['source_type'] = 'historical'
                overlap_samples.append(hist_sample)
                
                # Save current version  
                curr_mapped = rationalizer._map_fields(current_record, 'current_to_unified')
                curr_sample = {k: v for k, v in curr_mapped.items() if k in rationalizer.unified_schema}
                curr_sample['control_number'] = control_num
                curr_sample['source_type'] = 'current'
                overlap_samples.append(curr_sample)
            
            unified_record = rationalizer.rationalize_job_record(
                historical_data=hist_record,
                current_data=current_record,
                scraped_data=hist_record.get('scraped_content')
            )
            
            rationalized_records.append(unified_record)
            processed_control_numbers.add(control_num)
    
    # Process remaining current records
    for current_record in flattened_current_jobs:
        control_num = str(current_record.get('MatchedObjectId', ''))
        if control_num and control_num not in processed_control_numbers:
            unified_record = rationalizer.rationalize_job_record(
                current_data=current_record
            )
            rationalized_records.append(unified_record)
            processed_control_numbers.add(control_num)
    
    print(f"   📊 Total unified records: {len(rationalized_records)}")
    print(f"   🔄 Overlapping jobs: {duplicate_count}")
    
    # Save results
    if rationalized_records:
        storage.save_unified_jobs(rationalized_records)
        print(f"✅ Saved {len(rationalized_records)} unified jobs")
    
    if overlap_samples:
        storage.save_overlap_samples(overlap_samples)
        print(f"✅ Saved {len(overlap_samples)} overlap samples")
    
    print("🎯 Field rationalization complete!")
    return storage

def main():
    parser = argparse.ArgumentParser(description='Run parallel USAJobs pipeline with Parquet storage')
    parser.add_argument('--start-date', default='2025-01-01',
                       help='Start date for historical jobs (YYYY-MM-DD)')
    parser.add_argument('--output-dir', default='data_parquet',
                       help='Output directory for Parquet files')
    parser.add_argument('--scrape-workers', type=int, default=4,
                       help='Number of parallel workers for scraping')
    parser.add_argument('--no-scraping', action='store_true',
                       help='Skip web scraping')
    parser.add_argument('--no-rationalization', action='store_true',
                       help='Skip field rationalization')
    parser.add_argument('--keep-existing', action='store_true',
                       help='Keep existing data (default: delete all existing data for clean run)')
    parser.add_argument('--migrate-from-duckdb', 
                       help='Migrate data from existing DuckDB file')
    
    args = parser.parse_args()
    
    # Clean up existing data unless specifically requested to keep it
    if not args.keep_existing:
        import shutil
        if Path(args.output_dir).exists():
            print(f"🗑️ Deleting existing data directory: {args.output_dir}")
            shutil.rmtree(args.output_dir)
            print("✅ Clean slate ready!")
        else:
            print("📁 No existing data found - starting fresh")
    else:
        print(f"📂 Keeping existing data in: {args.output_dir}")
    
    # Migrate from DuckDB if requested
    if args.migrate_from_duckdb:
        print(f"🔄 Migrating from DuckDB: {args.migrate_from_duckdb}")
        storage = ParquetJobStorage(args.output_dir)
        from scripts.parquet_storage import migrate_from_duckdb
        migrate_from_duckdb(args.migrate_from_duckdb, storage)
        return
    
    # Run pipeline
    storage = run_pipeline(
        start_date=args.start_date,
        base_path=args.output_dir,
        scrape_jobs=not args.no_scraping,
        scrape_workers=args.scrape_workers
    )
    
    # Run rationalization if requested
    if not args.no_rationalization:
        storage = run_rationalization(storage)
    
    # Cleanup old files
    storage.cleanup_old_files(keep_latest_n=2)
    
    print(f"\n✅ PIPELINE COMPLETE!")
    print(f"📁 Data saved to: {args.output_dir}")
    print(f"📊 Run analysis with: python analyze_parquet_results.py --data-dir {args.output_dir}")

if __name__ == "__main__":
    main()