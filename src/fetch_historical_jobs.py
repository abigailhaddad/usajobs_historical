#!/usr/bin/env python3
"""
Historical Jobs Fetching for USAJobs Pipeline

Extracted from run_pipeline.py to be used in Parquet-based pipeline
"""

import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any

def fetch_all_jobs_for_date(date_str: str) -> List[Dict[str, Any]]:
    """Fetch all jobs for a specific date using proper continuation pagination with backoff"""
    base_url = "https://data.usajobs.gov"
    endpoint = "/api/historicjoa"
    params = {
        "StartPositionOpenDate": date_str,
        "EndPositionOpenDate": date_str
    }

    all_jobs = []
    next_url = endpoint  # Start with base endpoint

    while True:
        # Retry logic with exponential backoff
        max_retries = 3
        base_delay = 1
        
        for attempt in range(max_retries):
            try:
                if next_url == endpoint:
                    response = requests.get(f"{base_url}{endpoint}", params=params)
                else:
                    response = requests.get(f"{base_url}{next_url}")

                if response.status_code == 200:
                    break  # Success!
                elif response.status_code == 503:
                    # Service unavailable - wait and retry
                    delay = base_delay * (2 ** attempt)  # 1s, 2s, 4s
                    print(f"503 error for {date_str}, retrying in {delay}s (attempt {attempt+1}/{max_retries})")
                    time.sleep(delay)
                    continue
                else:
                    raise Exception(f"Status {response.status_code}: {response.text[:200]}")
                    
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    raise Exception(f"Request failed after {max_retries} attempts: {e}")
                delay = base_delay * (2 ** attempt)
                print(f"Request error for {date_str}, retrying in {delay}s: {e}")
                time.sleep(delay)
        else:
            # All retries failed
            raise Exception(f"Failed to fetch {date_str} after {max_retries} attempts")

        data = response.json()
        page_jobs = data.get("data", [])
        all_jobs.extend(page_jobs)

        paging = data.get("paging", {})
        next_url = paging.get("next")

        if not next_url:
            break

        # Small delay between pages to be respectful
        time.sleep(0.5)

    return all_jobs

def fetch_jobs_by_date_range(start_date: str, end_date: str, output_file: str = None) -> List[Dict[str, Any]]:
    """
    Fetch historical jobs for a date range
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format  
        output_file: Optional file to save results (for compatibility)
    
    Returns:
        List of job records
    """
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    all_jobs = []
    current_date = start
    
    while current_date <= end:
        date_str = current_date.strftime('%Y-%m-%d')
        
        try:
            day_jobs = fetch_all_jobs_for_date(date_str)
            all_jobs.extend(day_jobs)
            
            if day_jobs:
                print(f"📅 {date_str}: {len(day_jobs)} jobs")
            
        except Exception as e:
            print(f"❌ Error fetching jobs for {date_str}: {e}")
        
        current_date += timedelta(days=1)
        
        # Small delay between dates
        time.sleep(0.2)
    
    return all_jobs

def fetch_recent_historical_jobs(num_jobs: int = None, start_date_str: str = '2025-01-01') -> List[Dict[str, Any]]:
    """
    Fetch recent historical jobs starting from a date
    
    Args:
        num_jobs: Maximum number of jobs to fetch (None for all)
        start_date_str: Start date in YYYY-MM-DD format
    
    Returns:
        List of job records
    """
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.now()
    
    all_jobs = []
    current_date = start_date
    
    while current_date <= end_date:
        if num_jobs and len(all_jobs) >= num_jobs:
            break
            
        date_str = current_date.strftime('%Y-%m-%d')
        
        try:
            day_jobs = fetch_all_jobs_for_date(date_str)
            all_jobs.extend(day_jobs)
            
            if day_jobs:
                print(f"📅 {date_str}: {len(day_jobs)} jobs (total: {len(all_jobs)})")
            
            if num_jobs and len(all_jobs) >= num_jobs:
                all_jobs = all_jobs[:num_jobs]
                break
                
        except Exception as e:
            print(f"❌ Error fetching jobs for {date_str}: {e}")
        
        current_date += timedelta(days=1)
        time.sleep(0.2)
    
    return all_jobs