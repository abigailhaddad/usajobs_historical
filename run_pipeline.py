#!/usr/bin/env python3
"""
USAJobs Pipeline

Fetches current and historical job data, enriches with web scraping,
and generates analysis reports. Uses Parquet files for efficient storage.
"""

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
import sys
import time
import pandas as pd
import logging
import traceback

sys.path.append('src')
from parquet_storage import ParquetJobStorage
from fetch_historical_jobs import fetch_jobs_by_date_range
from fetch_current_jobs import fetch_all_jobs
from scrape_enhanced_job_posting import scrape_enhanced_job_posting
from field_rationalization import FieldRationalizer
from simple_validation import calculate_field_overlap, generate_simple_validation_html

from generate_mismatch_analysis import generate_mismatch_html

# Set up logging
def setup_logging():
    """Set up logging to both file and console"""
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    log_filename = f'pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_path = log_dir / log_filename
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Set up file handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Set up console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Also create a specific logger for errors
    error_logger = logging.getLogger('errors')
    error_handler = logging.FileHandler(log_dir / 'errors.log')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    error_logger.addHandler(error_handler)
    
    logging.info(f"Logging initialized. Log file: {log_path}")
    return logger

def scrape_current_jobs(jobs, storage):
    """Scrape content for current jobs"""
    already_scraped = storage.get_control_numbers_with_scraping()
    print(f"📋 {len(already_scraped)} jobs already have cached HTML")
    
    scraped_count = 0
    for job in jobs:
        control_number = str(job.get('MatchedObjectId', ''))
        if control_number:
            # Always call scrape function - it will use cache if available
            scraped_data = scrape_enhanced_job_posting(control_number)
            if scraped_data.get('status') == 'success':
                content_sections = scraped_data.get('content_sections', {})
                if content_sections:
                    job['scraped_content'] = scraped_data
                    scraped_count += 1
    
    print(f"✅ Successfully scraped {scraped_count} current jobs")
    return jobs

def scrape_historical_jobs(jobs, storage):
    """Scrape content for historical jobs"""
    already_scraped = storage.get_control_numbers_with_scraping()
    print(f"📋 {len(already_scraped)} jobs already have cached HTML")
    
    scraped_count = 0
    for job in jobs:
        control_number = str(job.get('usajobsControlNumber', '') or job.get('control_number', ''))
        if control_number:
            # Always call scrape function - it will use cache if available
            scraped_data = scrape_enhanced_job_posting(control_number)
            if scraped_data.get('status') == 'success':
                content_sections = scraped_data.get('content_sections', {})
                if content_sections:
                    job['scraped_content'] = scraped_data
                    scraped_count += 1
    
    print(f"✅ Successfully scraped {scraped_count} historical jobs")
    return jobs



def run_pipeline(start_date: str, base_path: str):
    """
    Run the USAJobs pipeline
    """
    print(f"🚀 USAJOBS PIPELINE")
    print("=" * 60)
    print(f"📊 Start date: {start_date}")
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
        # Add scraping to current jobs
        print(f"\n🕷️ Scraping current jobs...")
        current_jobs = scrape_current_jobs(current_jobs, storage)
        
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
    
    # Scrape historical jobs
    print(f"\n🕷️ Scraping historical jobs...")
    historical_jobs = scrape_historical_jobs(historical_jobs, storage)
    
    # Save all historical jobs
    batch_id = f"0_{start_date}_{end_date}"
    storage.save_historical_jobs(historical_jobs, batch_id)
    
    return storage

def run_rationalization(storage: ParquetJobStorage):
    """
    Run field rationalization on the collected data
    """
    logger = logging.getLogger()
    logger.info(f"\n🔄 STARTING FIELD RATIONALIZATION")
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
    jobs_with_scraped = 0
    jobs_with_content = 0
    for job in historical_jobs:
        control_num = job.get('usajobsControlNumber', 'unknown')
        
        # Initialize scraped_content structure if it doesn't exist
        if 'scraped_content' not in job:
            job['scraped_content'] = {'content_sections': {}}
        
        if 'scraped_content' in job and job.get('scraped_content'):
            try:
                scraped_data = job['scraped_content']
                if scraped_data and scraped_data.get('content_sections'):
                    sections = scraped_data['content_sections']
                    job['scraped_content']['content_sections'] = sections
                    jobs_with_scraped += 1
                    
                    # Log what content we found
                    if sections:
                        jobs_with_content += 1
                        logger.debug(f"Job {control_num} has scraped sections: {list(sections.keys())}")
                    
                # Copy other scraped metadata if present
                for key, value in scraped_data.items():
                    if key != 'content_sections':
                        job['scraped_content'][key] = value
            except Exception as e:
                logger.error(f"Failed to process scraped content for job {control_num}: {e}")
                logging.getLogger('errors').error(f"Error processing scraped content for {control_num}: {e}")
    
    logger.info(f"Historical jobs with scraped sections: {jobs_with_scraped}/{len(historical_jobs)}")
    logger.info(f"Historical jobs with actual content: {jobs_with_content}/{len(historical_jobs)}")
    
    # Flatten current jobs - extract MatchedObjectDescriptor content
    flattened_current_jobs = []
    for job in current_jobs:
        # Extract the actual job data from MatchedObjectDescriptor
        descriptor = job.get('MatchedObjectDescriptor', {})
        if descriptor:
            # Add the MatchedObjectId to the descriptor for lookup
            descriptor['MatchedObjectId'] = job.get('MatchedObjectId')
            
            # Add scraped content if available (from current job scraping)
            if 'scraped_content' in job:
                descriptor['scraped_content'] = job['scraped_content']
                
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
    scraped_jobs_count = 0
    scraped_applied_count = 0
    for hist_record in historical_jobs:
        control_num = str(hist_record.get('usajobsControlNumber', ''))
        if control_num and control_num not in processed_control_numbers:
            current_record = current_lookup.get(control_num)
            
            # Check if this job has scraped content
            scraped_data = hist_record.get('scraped_content')
            if scraped_data and scraped_data.get('content_sections'):
                scraped_jobs_count += 1
                logger.debug(f"Job {control_num} has scraped_content with sections: {list(scraped_data.get('content_sections', {}).keys())}")
            
            if current_record:
                duplicate_count += 1
                
                # Save overlap sample
                hist_mapped = rationalizer._map_fields(hist_record, 'historical_to_unified')
                
                # Add scraped content if available
                if scraped_data and scraped_data.get('content_sections'):
                    scraped_mapped = rationalizer._map_fields(scraped_data, 'scraped_to_unified')
                    scraped_content = rationalizer._extract_scraped_content(scraped_data)
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
                scraped_data=scraped_data
            )
            
            rationalized_records.append(unified_record)
            processed_control_numbers.add(control_num)
    
    # Process remaining current records
    for current_record in flattened_current_jobs:
        control_num = str(current_record.get('MatchedObjectId', ''))
        if control_num and control_num not in processed_control_numbers:
            # Check if this current job has scraped content
            scraped_data = current_record.get('scraped_content')
            if scraped_data:
                # Parse scraped content for current jobs (same as historical)
                parsed_scraped = {'content_sections': {}}
                if 'content_sections' in scraped_data:
                    try:
                        if isinstance(scraped_data['content_sections'], str):
                            parsed_scraped['content_sections'] = json.loads(scraped_data['content_sections'])
                        else:
                            parsed_scraped['content_sections'] = scraped_data['content_sections']
                        
                        if 'extraction_stats' in scraped_data:
                            parsed_scraped.update(scraped_data)
                    except json.JSONDecodeError:
                        logger.error(f"Failed to parse scraped content for current job {control_num}")
                scraped_data = parsed_scraped if parsed_scraped['content_sections'] else None
            
            unified_record = rationalizer.rationalize_job_record(
                current_data=current_record,
                scraped_data=scraped_data
            )
            rationalized_records.append(unified_record)
            processed_control_numbers.add(control_num)
    
    print(f"   📊 Total unified records: {len(rationalized_records)}")
    print(f"   🔄 Overlapping jobs: {duplicate_count}")
    print(f"   🕷️ Jobs with scraped content: {scraped_jobs_count}")
    
    
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
    # Set up logging first
    logger = setup_logging()
    
    parser = argparse.ArgumentParser(description='Run USAJobs pipeline with enhanced scraping')
    parser.add_argument('--start-date', default='2025-01-01',
                       help='Start date for historical jobs (YYYY-MM-DD)')
    parser.add_argument('--output-dir', default='data',
                       help='Output directory for data files')
    
    args = parser.parse_args()
    
    # Run pipeline
    storage = run_pipeline(
        start_date=args.start_date,
        base_path=args.output_dir
    )
    
    # Run rationalization
    storage = run_rationalization(storage)
    
    # Run field overlap analysis
    print("\n🔍 Running field overlap analysis...")
    try:
        overlap_df = storage.load_overlap_samples()
        if not overlap_df.empty:
            overlap_results = calculate_field_overlap(overlap_df)
            if overlap_results['status'] == 'success':
                print(f"✅ Content matching analysis complete:")
                print(f"   Overlap jobs tested: {overlap_results['total_overlap_jobs']}")
                for field, stats in overlap_results['field_stats'].items():
                    if stats['both_have_content'] > 0:
                        print(f"   {field}: {stats['both_have_content']} comparisons, {stats['perfect_match_pct']:.1f}% perfect matches, {stats['avg_similarity']:.3f} avg similarity")
                    else:
                        print(f"   {field}: No overlapping content to compare")
                
                # Generate HTML report
                validation_html = generate_simple_validation_html(overlap_results)
                with open('scraping_effectiveness_report.html', 'w', encoding='utf-8') as f:
                        f.write(f"""
<!DOCTYPE html>
<html>
<head>
    <title>Scraping Effectiveness Report</title>
    <meta charset="utf-8">
</head>
<body>
    <h1>🔍 Scraping Effectiveness Analysis</h1>
    <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    {validation_html}
</body>
</html>
                        """)
                print(f"✅ Scraping effectiveness report generated: scraping_effectiveness_report.html")
            else:
                print(f"⚠️ Overlap analysis failed: {overlap_results.get('message', 'Unknown error')}")
        else:
            print("⚠️ No overlap samples available for analysis")
    except Exception as e:
        print(f"⚠️ Overlap analysis error: {e}")
    
    # Generate content mismatch analysis HTML
    print("\n🔍 Generating content mismatch analysis...")
    try:
        mismatch_html_file = generate_mismatch_html()
        print(f"✅ Content mismatch analysis generated: {mismatch_html_file}")
    except Exception as e:
        print(f"⚠️ Mismatch analysis generation failed: {e}")
        logger.error(f"Mismatch analysis generation failed: {e}")
    
    # Generate QMD analysis report
    print("\n📊 Generating analysis report...")
    try:
        import subprocess
        result = subprocess.run(['quarto', 'render', 'rationalization_analysis.qmd'], 
                              capture_output=True, text=True, cwd='.')
        if result.returncode == 0:
            print("✅ Analysis report generated: rationalization_analysis.html")
        else:
            print(f"⚠️ QMD generation failed: {result.stderr}")
    except FileNotFoundError:
        print("⚠️ Quarto not found - skipping report generation")
    except Exception as e:
        print(f"⚠️ Error generating report: {e}")
    
    # Cleanup old files and worker data
    storage.cleanup_old_files(keep_latest_n=2)
    
    # Clean up worker files if they exist
    print("\n🧹 Cleaning up worker files...")
    worker_files = list(Path(args.output_dir).glob("worker_*.parquet"))
    temp_files = list(Path(args.output_dir).glob("temp_*.parquet"))
    
    for file in worker_files + temp_files:
        try:
            file.unlink()
            print(f"   Removed: {file.name}")
        except Exception as e:
            print(f"   Warning: Could not remove {file.name}: {e}")
    
    print(f"\n✅ PIPELINE COMPLETE!")
    print(f"📁 Data saved to: {args.output_dir}")
    print(f"📊 View report: rationalization_analysis.html")
    print(f"🔍 View content mismatches: content_mismatch_analysis.html")
    print(f"📈 View scraping effectiveness: scraping_effectiveness_report.html")

if __name__ == "__main__":
    main()