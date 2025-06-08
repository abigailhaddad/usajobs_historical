#!/usr/bin/env python3
"""
Retry failed dates - fetch data for dates that are missing from the database
"""
import requests
import json
import time
import duckdb
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Optional

API_URL = "https://data.usajobs.gov/api/historicjoa"

def get_missing_dates(duckdb_file: str) -> List[str]:
    """Find all dates that are missing from the database."""
    conn = duckdb.connect(duckdb_file, read_only=True)
    
    # Get date range
    result = conn.execute("""
        SELECT 
            MIN(position_open_date) as earliest,
            MAX(position_open_date) as latest
        FROM historical_jobs
        WHERE position_open_date IS NOT NULL
    """).fetchone()
    
    if not result or not result[0]:
        print("❌ No dates found in database")
        return []
    
    earliest, latest = result
    
    # Get all existing dates
    existing_dates = conn.execute("""
        SELECT DISTINCT position_open_date 
        FROM historical_jobs 
        WHERE position_open_date IS NOT NULL
    """).fetchall()
    
    date_set = {str(row[0]) for row in existing_dates}
    conn.close()
    
    # Find missing dates
    missing_dates = []
    if isinstance(earliest, str):
        current_date = datetime.strptime(earliest, '%Y-%m-%d')
        end_date = datetime.strptime(latest, '%Y-%m-%d')
    else:
        current_date = datetime.combine(earliest, datetime.min.time())
        end_date = datetime.combine(latest, datetime.min.time())
    
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        if date_str not in date_set:
            missing_dates.append(date_str)
        current_date += timedelta(days=1)
    
    return missing_dates

def debug_api_response(date: str, max_retries: int = 5) -> Optional[Dict]:
    """Try various methods to get data for a problematic date."""
    params = {
        "StartPositionOpenDate": date,
        "EndPositionOpenDate": date
    }
    
    print(f"\n🔍 Attempting to fetch data for {date}...")
    
    for attempt in range(max_retries):
        try:
            response = requests.get(API_URL, params=params)
            print(f"  Attempt {attempt + 1}: Status {response.status_code}")
            
            if response.status_code == 503:
                wait_time = (attempt + 1) * 5
                print(f"  503 error, waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                continue
            
            if response.status_code != 200:
                print(f"  ❌ Bad status code: {response.status_code}")
                continue
            
            # Check for "204 No Content" text response
            if response.text.strip() == "204 No Content":
                print(f"  ℹ️  API returned '204 No Content' - no jobs posted on this date")
                return {"results": []}  # Return empty results
            
            # Try standard parsing
            try:
                data = response.json()
                print(f"  ✅ Standard JSON parsing successful")
                return data
            except json.JSONDecodeError as e:
                print(f"  ❌ JSON decode error: {e}")
                
                # Try truncating at last valid JSON character
                text = response.text
                
                # Method 1: Find last closing brace
                last_brace = text.rfind('}')
                if last_brace > 0:
                    try:
                        truncated = text[:last_brace + 1]
                        data = json.loads(truncated)
                        print(f"  ✅ Fixed by truncating at position {last_brace + 1}")
                        return data
                    except:
                        pass
                
                # Method 2: Look for null/extra characters
                if '\x00' in text or '\r' in text:
                    try:
                        cleaned = text.replace('\x00', '').replace('\r\n', '\n').replace('\r', '\n')
                        data = json.loads(cleaned)
                        print(f"  ✅ Fixed by cleaning null/carriage return characters")
                        return data
                    except:
                        pass
                
                # Method 3: Try different encodings
                try:
                    text_utf8 = response.content.decode('utf-8', errors='ignore')
                    data = json.loads(text_utf8)
                    print(f"  ✅ Fixed by re-encoding with error handling")
                    return data
                except:
                    pass
                
                # If nothing works, save raw response for debugging
                with open(f'failed_{date}_response.txt', 'w') as f:
                    f.write(response.text)
                print(f"  💾 Saved raw response to failed_{date}_response.txt for debugging")
                
        except Exception as e:
            print(f"  ❌ Request failed: {e}")
            time.sleep(2)
    
    return None

def save_jobs_to_duckdb(jobs: List[Dict], date: str, duckdb_file: str):
    """Save successfully retrieved jobs to DuckDB."""
    from historic_pull import flatten_job, init_duckdb, save_batch_to_duckdb
    
    conn = duckdb.connect(duckdb_file)
    
    # Ensure table exists
    init_duckdb(duckdb_file)
    
    # Flatten and save jobs
    flattened_jobs = []
    for job in jobs:
        flat_job = flatten_job(job)
        if flat_job:
            flattened_jobs.append(flat_job)
    
    if flattened_jobs:
        save_batch_to_duckdb(conn, flattened_jobs)
        print(f"  💾 Saved {len(flattened_jobs)} jobs for {date}")
    
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="Retry failed dates")
    parser.add_argument("duckdb_file", help="Path to DuckDB file")
    parser.add_argument("--date", help="Specific date to retry (YYYY-MM-DD)")
    parser.add_argument("--all", action="store_true", help="Retry all missing dates")
    parser.add_argument("--max-retries", type=int, default=5, help="Max retries per date")
    
    args = parser.parse_args()
    
    if args.date:
        # Retry specific date
        dates_to_retry = [args.date]
    elif args.all:
        # Find and retry all missing dates
        dates_to_retry = get_missing_dates(args.duckdb_file)
        if not dates_to_retry:
            print("✅ No missing dates found!")
            return
        print(f"📅 Found {len(dates_to_retry)} missing dates")
    else:
        # Show missing dates
        missing_dates = get_missing_dates(args.duckdb_file)
        if not missing_dates:
            print("✅ No missing dates found!")
        else:
            print(f"📅 Found {len(missing_dates)} missing dates:")
            for date in missing_dates:
                print(f"  - {date}")
            print("\nUse --date YYYY-MM-DD to retry a specific date")
            print("Use --all to retry all missing dates")
        return
    
    # Retry the dates
    success_count = 0
    for date in dates_to_retry:
        result = debug_api_response(date, args.max_retries)
        
        if result and 'results' in result:
            jobs = result['results']
            if jobs:
                save_jobs_to_duckdb(jobs, date, args.duckdb_file)
                success_count += 1
            else:
                print(f"  ⚠️  No jobs found for {date} (this might be correct)")
        else:
            print(f"  ❌ Failed to retrieve data for {date}")
    
    print(f"\n📊 Summary: Successfully retrieved {success_count}/{len(dates_to_retry)} dates")

if __name__ == "__main__":
    main()