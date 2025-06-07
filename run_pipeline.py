#!/usr/bin/env python3
"""
USAJobs Pipeline Runner
Orchestrates the complete workflow: fetch → generate titles → load to database
"""

import sys
import os
import json
import argparse
from datetime import datetime
import subprocess

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from fetch_usajobs import fetch_all_jobs, save_jobs_data
from generate_job_titles import process_jobs_file
from data_loading import load_jobs_to_db


def run_fetch(args):
    """Step 1: Fetch jobs from USAJobs API"""
    print("\n🔍 Step 1: Fetching jobs from USAJobs API...")
    
    # Build search parameters
    params = {
        "WhoMayApply": args.who_may_apply,
        "ResultsPerPage": 500,
        "Fields": "full",
        "SortField": "DatePosted",
        "SortDirection": "desc"
    }
    
    if args.keyword:
        params["Keyword"] = args.keyword
    
    if args.days_posted:
        from datetime import timedelta
        # Use a reasonable date range (API doesn't accept future dates)
        base_date = datetime(2024, 12, 1)  # Use December 2024 as base
        date_from = (base_date - timedelta(days=args.days_posted)).strftime("%m/%d/%Y")
        params["DatePosted"] = date_from
    
    if args.remote:
        params["RemoteIndicator"] = "true"
    
    # Fetch jobs
    jobs = fetch_all_jobs(params, max_pages=args.max_pages)
    
    if jobs:
        # Save raw data
        raw_file = os.path.join(args.data_dir, f"usajobs_raw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        save_jobs_data(jobs, raw_file)
        print(f"✅ Fetched {len(jobs)} jobs → {raw_file}")
        return raw_file
    else:
        print("❌ No jobs fetched")
        return None


def run_title_generation(raw_file, args):
    """Step 2: Generate titles using LLM"""
    print("\n🤖 Step 2: Generating titles with LLM...")
    
    output_file = raw_file.replace('_raw_', '_titles_').replace('.json', '_generated.json')
    
    # If sample limit is set, create a temporary limited file
    if args.sample_titles:
        print(f"🔧 Limiting to first {args.sample_titles} jobs for title generation")
        with open(raw_file, 'r') as f:
            data = json.load(f)
        
        # Limit the jobs
        jobs = data.get('SearchResult', {}).get('SearchResultItems', [])
        original_count = len(jobs)
        data['SearchResult']['SearchResultItems'] = jobs[:args.sample_titles]
        
        # Save to temp file
        temp_file = raw_file.replace('.json', f'_sample{args.sample_titles}.json')
        with open(temp_file, 'w') as f:
            json.dump(data, f)
        
        # Process temp file
        results = process_jobs_file(temp_file, output_file)
        
        # Clean up temp file
        os.remove(temp_file)
        
        if results:
            print(f"✅ Generated {len(results)} titles (from {original_count} jobs) → {output_file}")
        
    else:
        # Process all jobs
        results = process_jobs_file(raw_file, output_file)
        
        if results:
            print(f"✅ Generated {len(results)} titles → {output_file}")
    
    if results:
        return output_file
    else:
        print("❌ Title generation failed")
        return None


def enrich_raw_data(raw_file, titles_file, args):
    """Step 3: Enrich raw data with generated titles"""
    print("\n🔄 Step 3: Enriching raw data with generated titles...")
    
    # Load raw data
    with open(raw_file, 'r') as f:
        raw_data = json.load(f)
    
    # Load generated titles
    with open(titles_file, 'r') as f:
        titles_data = json.load(f)
    
    # Create mapping of position_id to generated_title
    title_map = {item['position_id']: item['generated_title'] 
                 for item in titles_data}
    
    # Enrich raw data
    jobs = raw_data.get('SearchResult', {}).get('SearchResultItems', [])
    enriched_count = 0
    
    for job in jobs:
        position_id = job.get('MatchedObjectDescriptor', {}).get('PositionID')
        if position_id in title_map:
            # Add generated title to the job data
            if 'MatchedObjectDescriptor' not in job:
                job['MatchedObjectDescriptor'] = {}
            job['MatchedObjectDescriptor']['GeneratedTitle'] = title_map[position_id]
            enriched_count += 1
    
    # Save enriched data
    enriched_file = raw_file.replace('_raw_', '_enriched_')
    with open(enriched_file, 'w') as f:
        json.dump(raw_data, f, indent=2)
    
    print(f"✅ Enriched {enriched_count} jobs → {enriched_file}")
    return enriched_file


def run_database_load(enriched_file, args):
    """Step 4: Load to database"""
    if not args.load_db:
        print("\n⏭️  Step 4: Skipping database load (use --load-db to enable)")
        return
    
    print("\n💾 Step 4: Loading to database...")
    
    # Create a file with only the processed jobs for database loading
    processed_jobs_file = enriched_file.replace('_enriched_', '_processed_for_db_')
    
    try:
        # Load the enriched data to see which jobs were actually processed
        with open(enriched_file, 'r') as f:
            enriched_data = json.load(f)
        
        # Filter to only jobs that have generated titles (were actually processed)
        processed_jobs = []
        for job in enriched_data.get('SearchResult', {}).get('SearchResultItems', []):
            descriptor = job.get('MatchedObjectDescriptor', {})
            if descriptor.get('GeneratedTitle'):  # Only include if it has a generated title
                processed_jobs.append(job)
        
        # Create new data structure with only processed jobs
        processed_data = {
            "LanguageCode": enriched_data.get("LanguageCode", "EN"),
            "SearchParameters": enriched_data.get("SearchParameters", {}),
            "SearchResult": {
                "SearchResultCount": len(processed_jobs),
                "SearchResultCountAll": len(processed_jobs),
                "SearchResultItems": processed_jobs
            }
        }
        
        # Save processed jobs to temporary file
        with open(processed_jobs_file, 'w') as f:
            json.dump(processed_data, f)
        
        print(f"📊 Loading {len(processed_jobs)} processed jobs to database...")
        load_jobs_to_db(processed_jobs_file)
        
        # Clean up temp file
        os.remove(processed_jobs_file)
        
        print("✅ Data loaded to database successfully")
    except Exception as e:
        print(f"❌ Database load failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="Run USAJobs data pipeline")
    
    # Pipeline options
    parser.add_argument("--skip-fetch", action="store_true", 
                       help="Skip fetching (use existing data)")
    parser.add_argument("--skip-titles", action="store_true", 
                       help="Skip title generation")
    parser.add_argument("--load-db", action="store_true", 
                       help="Load data to database")
    parser.add_argument("--data-dir", default="data", 
                       help="Directory for data files")
    
    # Fetch options
    parser.add_argument("--keyword", help="Search keyword")
    parser.add_argument("--days-posted", type=int, default=7, 
                       help="Jobs posted within N days")
    parser.add_argument("--max-pages", type=int, 
                       help="Maximum pages to fetch")
    parser.add_argument("--who-may-apply", default="public", 
                       help="Who may apply filter")
    parser.add_argument("--remote", action="store_true", 
                       help="Remote jobs only")
    
    # Use existing file
    parser.add_argument("--use-file", help="Use existing raw data file")
    
    # Title generation options
    parser.add_argument("--sample-titles", type=int, 
                       help="Only generate titles for first N jobs (for testing)")
    
    args = parser.parse_args()
    
    print("🚀 USAJobs Pipeline Starting...")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Step 1: Fetch or use existing
    if args.skip_fetch:
        if args.use_file:
            raw_file = args.use_file
            print(f"\n📂 Using existing file: {raw_file}")
        else:
            print("❌ Must provide --use-file when using --skip-fetch")
            return
    else:
        raw_file = run_fetch(args)
        if not raw_file:
            return
    
    # Step 2: Generate titles
    if not args.skip_titles:
        titles_file = run_title_generation(raw_file, args)
        if not titles_file:
            return
        
        # Step 3: Enrich raw data
        enriched_file = enrich_raw_data(raw_file, titles_file, args)
    else:
        print("\n⏭️  Skipping title generation")
        enriched_file = raw_file
    
    # Step 4: Load to database
    run_database_load(enriched_file, args)
    
    print("\n✅ Pipeline complete!")


if __name__ == "__main__":
    main()