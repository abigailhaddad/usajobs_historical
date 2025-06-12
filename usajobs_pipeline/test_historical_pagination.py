#!/usr/bin/env python3
"""
Test script for historical API pagination
Tests a single day that should have many jobs to see if we can get more than the first page
"""

import requests
import json
import time
from datetime import datetime

def test_historical_pagination(date_str="2025-01-23"):
    """Properly paginate using 'next' link from the API response"""
    print(f"🧪 Testing historical API pagination for {date_str}")

    base_url = "https://data.usajobs.gov"
    endpoint = "/api/historicjoa"
    params = {
        "StartPositionOpenDate": date_str,
        "EndPositionOpenDate": date_str
    }

    all_jobs = []
    page_count = 0
    next_url = endpoint  # Start with the base endpoint

    while True:
        if next_url == endpoint:
            full_url = f"{base_url}{endpoint}"
            response = requests.get(full_url, params=params)
        else:
            full_url = f"{base_url}{next_url}"
            response = requests.get(full_url)

        print(f"  📄 Fetching page {page_count + 1}...")
        print(f"    Request URL: {full_url}")
        print(f"    Status code: {response.status_code}")

        if response.status_code != 200:
            print(f"    ❌ Error response: {response.text[:200]}")
            break

        try:
            data = response.json()
        except json.JSONDecodeError as e:
            print(f"    ❌ JSON decode error: {e}")
            break

        jobs = data.get("data", [])
        print(f"    ✅ Jobs found: {len(jobs)}")
        all_jobs.extend(jobs)
        page_count += 1

        paging = data.get("paging", {})
        next_url = paging.get("next")

        if not next_url:
            print("    🚪 No more pages.")
            break

        time.sleep(0.2)  # Respect API rate limits

    print(f"\n✅ Results for {date_str}:")
    print(f"   Total jobs collected: {len(all_jobs)}")
    print(f"   Pages fetched: {page_count}")
    if all_jobs:
        print(f"   Sample job: {all_jobs[0].get('usajobsControlNumber')} - {all_jobs[0].get('positionTitle')}")

    return all_jobs


def test_simple_request(date_str="2025-01-23"):
    """Test a simple request without pagination for comparison"""
    print(f"\n🔍 Testing simple request for {date_str}")
    
    api_url = "https://data.usajobs.gov/api/historicjoa"
    params = {
        "StartPositionOpenDate": date_str,
        "EndPositionOpenDate": date_str
    }
    
    try:
        response = requests.get(api_url, params=params)
        print(f"  Status code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            jobs = data.get("data", [])
            paging = data.get("paging", {})
            
            print(f"  Jobs returned: {len(jobs)}")
            print(f"  Paging info: {paging}")
            
            return jobs
        else:
            print(f"  Error: {response.text[:200]}...")
            return []
            
    except Exception as e:
        print(f"  Exception: {e}")
        return []

if __name__ == "__main__":
    # Test dates that might have many jobs
    test_dates = ["2025-01-23", "2025-01-15", "2025-02-01", "2025-03-01"]
    
    for date in test_dates:
        print("\n" + "="*60)
        
        # First try simple request
        simple_jobs = test_simple_request(date)
        
        # Then try pagination
        paginated_jobs = test_historical_pagination(date)
        
        print(f"\nComparison for {date}:")
        print(f"  Simple request: {len(simple_jobs)} jobs")
        print(f"  Paginated request: {len(paginated_jobs)} jobs")
        
        if len(paginated_jobs) > len(simple_jobs):
            print(f"  🎉 Pagination found {len(paginated_jobs) - len(simple_jobs)} additional jobs!")
        elif len(simple_jobs) > 0:
            print(f"  📝 No additional jobs found via pagination")
        else:
            print(f"  ❌ No jobs found for this date")