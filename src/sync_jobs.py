import json
import psycopg2
from psycopg2.extras import Json, RealDictCursor
from datetime import datetime, timedelta
import subprocess
import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in environment variables.")

def get_latest_job_date():
    """Get the date of the most recently posted job in the database"""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT MAX(position_start_date) as latest_date
            FROM jobs
            WHERE position_start_date IS NOT NULL
        """)
        result = cur.fetchone()
        return result[0] if result and result[0] else None
    finally:
        cur.close()
        conn.close()

def get_job_count():
    """Get total number of jobs in database"""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    try:
        cur.execute("SELECT COUNT(*) FROM jobs")
        return cur.fetchone()[0]
    finally:
        cur.close()
        conn.close()

def fetch_new_jobs(days_back=None):
    """Fetch new jobs from USAJobs API"""
    cmd = ["python3", "fetch_usajobs.py"]
    
    if days_back:
        cmd.extend(["--days-posted", str(days_back)])
    else:
        # If no days specified, fetch jobs from last 7 days by default
        cmd.extend(["--days-posted", "7"])
    
    # Use a different filename for incremental updates
    cmd.extend(["--output", "usajobs_incremental.json"])
    
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Error fetching jobs: {result.stderr}")
        return False
    
    print(result.stdout)
    return True

def load_jobs_to_database(filename="usajobs_incremental.json"):
    """Load jobs from JSON file to database"""
    # Update the data_loading.py to use the specified filename
    cmd = ["python3", "data_loading.py"]
    
    # Temporarily modify data_loading.py to use our filename
    with open("data_loading.py", "r") as f:
        original_content = f.read()
    
    modified_content = original_content.replace(
        'with open("usajobs_results.json", "r") as f:',
        f'with open("{filename}", "r") as f:'
    )
    
    with open("data_loading.py", "w") as f:
        f.write(modified_content)
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error loading jobs: {result.stderr}")
            return False
        print(result.stdout)
        return True
    finally:
        # Restore original content
        with open("data_loading.py", "w") as f:
            f.write(original_content)

def sync_jobs(mode="incremental", days_back=None):
    """Main sync function"""
    print(f"Starting job sync in {mode} mode...")
    
    # Get current state
    job_count_before = get_job_count()
    latest_date = get_latest_job_date()
    
    print(f"Current jobs in database: {job_count_before}")
    if latest_date:
        print(f"Latest job date: {latest_date}")
    
    # Determine how many days back to fetch
    if mode == "incremental" and latest_date:
        # Calculate days since last job
        days_since = (datetime.now() - latest_date).days + 1
        days_to_fetch = max(days_since, 7)  # At least 7 days
        print(f"Fetching jobs from last {days_to_fetch} days")
    else:
        days_to_fetch = days_back or 30  # Default to 30 days for full sync
        print(f"Performing full sync for last {days_to_fetch} days")
    
    # Fetch new jobs
    if not fetch_new_jobs(days_to_fetch):
        print("Failed to fetch new jobs")
        return False
    
    # Load to database
    if not load_jobs_to_database("usajobs_incremental.json"):
        print("Failed to load jobs to database")
        return False
    
    # Report results
    job_count_after = get_job_count()
    new_jobs = job_count_after - job_count_before
    
    print(f"\n✅ Sync completed successfully!")
    print(f"New jobs added: {new_jobs}")
    print(f"Total jobs in database: {job_count_after}")
    
    return True

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Sync USAJobs data to database")
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental",
                        help="Sync mode: incremental (new jobs only) or full")
    parser.add_argument("--days", type=int, help="Days back to fetch (for full mode)")
    
    args = parser.parse_args()
    
    success = sync_jobs(mode=args.mode, days_back=args.days)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()