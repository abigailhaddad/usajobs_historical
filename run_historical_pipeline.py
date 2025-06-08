#!/usr/bin/env python3
"""
USAJobs Historical Pipeline Runner
Orchestrates the workflow: fetch historical data → load to database
"""

import sys
import os
import json
import argparse
from datetime import datetime, timedelta
import subprocess

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def run_fetch(args):
    """Step 1: Fetch historical jobs from USAJobs API"""
    print("\n🔍 Step 1: Fetching historical jobs from USAJobs API...")
    
    # Build command
    cmd = [
        sys.executable,
        "historic_pull.py",
        "--start-date", args.start_date,
        "--end-date", args.end_date
    ]
    
    # Add DuckDB output (default for large pulls)
    if not args.output_file.endswith('.json'):
        # Default to DuckDB for non-JSON outputs
        cmd.extend(["--duckdb", args.output_file.replace('.json', '.duckdb')])
        cmd.append("--no-json")
    else:
        cmd.extend(["--output", args.output_file])
    
    # Add database flag if requested
    if args.load_db:
        cmd.append("--load-to-db")
    
    # Run the command
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(result.stdout)
        return True
    else:
        print(f"❌ Fetch failed: {result.stderr}")
        return False


def run_database_load(json_file, args):
    """Step 2: Load to database (if not done in fetch)"""
    if args.load_db:
        # If load_db was set, it was already done in fetch
        return
    
    if not args.load_db_separate:
        print("\n⏭️  Step 2: Skipping database load (use --load-db-separate to enable)")
        return
    
    print("\n💾 Step 2: Loading to database...")
    
    from load_historical_jobs import load_historical_jobs_to_db
    
    try:
        load_historical_jobs_to_db(json_file)
        print("✅ Data loaded to database successfully")
    except Exception as e:
        print(f"❌ Database load failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="Run USAJobs historical data pipeline")
    
    # Date range options
    parser.add_argument("--start-date", 
                       help="Start date (YYYY-MM-DD). Default: 30 days ago")
    parser.add_argument("--end-date", 
                       help="End date (YYYY-MM-DD). Default: today")
    parser.add_argument("--days-back", type=int, default=30,
                       help="Alternative: fetch last N days (default: 30)")
    
    # Output options
    parser.add_argument("--output-file", 
                       help="Output JSON file path")
    parser.add_argument("--data-dir", default="data", 
                       help="Directory for data files")
    
    # Database options
    parser.add_argument("--load-db", action="store_true", 
                       help="Load data to database immediately after fetch")
    parser.add_argument("--load-db-separate", action="store_true", 
                       help="Load existing JSON file to database")
    parser.add_argument("--use-file", 
                       help="Use existing JSON file (skip fetch)")
    
    args = parser.parse_args()
    
    # Set up dates if not provided
    if not args.end_date:
        args.end_date = datetime.now().strftime('%Y-%m-%d')
    
    if not args.start_date:
        start = datetime.now() - timedelta(days=args.days_back)
        args.start_date = start.strftime('%Y-%m-%d')
    
    # Set up output file if not provided
    if not args.output_file:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        args.output_file = os.path.join(
            args.data_dir, 
            f"historical_jobs_{args.start_date}_to_{args.end_date}_{timestamp}.duckdb"
        )
    
    # Create data directory if needed
    os.makedirs(args.data_dir, exist_ok=True)
    
    print("🚀 USAJobs Historical Pipeline Starting...")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📆 Date range: {args.start_date} to {args.end_date}")
    
    # Step 1: Fetch or use existing
    if args.use_file:
        print(f"\n📂 Using existing file: {args.use_file}")
        json_file = args.use_file
    else:
        success = run_fetch(args)
        if not success:
            return
        json_file = args.output_file
    
    # Show job count
    try:
        with open(json_file, 'r') as f:
            jobs = json.load(f)
            print(f"\n📊 Total jobs in file: {len(jobs)}")
    except Exception as e:
        print(f"⚠️  Could not read job count: {e}")
    
    # Step 2: Load to database (if separate)
    if not args.use_file:  # If we fetched new data
        run_database_load(json_file, args)
    elif args.load_db_separate:  # If using existing file and want to load
        args.load_db = False  # Make sure we don't skip
        run_database_load(json_file, args)
    
    print("\n✅ Pipeline complete!")


if __name__ == "__main__":
    main()