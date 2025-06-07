import requests
import json
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
import argparse

# Load environment variables
load_dotenv()
API_KEY = os.getenv("USAJOBS_API_TOKEN")

# Base URL and headers
BASE_URL = "https://data.usajobs.gov/api/Search"
HEADERS = {
    "Host": "data.usajobs.gov",
    "Authorization-Key": API_KEY
}

def fetch_jobs_page(params, page=1):
    """Fetch a single page of job results"""
    params_copy = params.copy()
    params_copy['Page'] = page
    
    try:
        response = requests.get(BASE_URL, headers=HEADERS, params=params_copy)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page {page}: {e}")
        return None

def fetch_all_jobs(base_params, max_pages=None):
    """Fetch all jobs with pagination"""
    all_jobs = []
    page = 1
    total_count = None
    
    while True:
        print(f"Fetching page {page}...")
        data = fetch_jobs_page(base_params, page)
        
        if not data:
            print("Failed to fetch data, stopping.")
            break
            
        search_result = data.get("SearchResult", {})
        items = search_result.get("SearchResultItems", [])
        
        if not items:
            print("No more results.")
            break
            
        all_jobs.extend(items)
        
        # Get total count on first page
        if page == 1:
            total_count = search_result.get("SearchResultCountAll", 0)
            print(f"Total jobs available: {total_count}")
        
        # Check if we've reached the end
        if len(all_jobs) >= total_count:
            break
            
        # Check max pages limit
        if max_pages and page >= max_pages:
            print(f"Reached max pages limit ({max_pages})")
            break
            
        page += 1
        # Be nice to the API
        time.sleep(0.5)
    
    print(f"Fetched {len(all_jobs)} total jobs across {page} pages")
    return all_jobs

def save_jobs_data(jobs, filename="usajobs_full.json"):
    """Save jobs data to JSON file"""
    data = {
        "LanguageCode": "EN",
        "SearchParameters": {
            "FetchDate": datetime.now().isoformat(),
            "TotalJobsFetched": len(jobs)
        },
        "SearchResult": {
            "SearchResultCount": len(jobs),
            "SearchResultCountAll": len(jobs),
            "SearchResultItems": jobs
        }
    }
    
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f"✅ Data saved to {filename}")

def main():
    parser = argparse.ArgumentParser(description="Fetch USAJobs data")
    parser.add_argument("--keyword", help="Search keyword", default=None)
    parser.add_argument("--days-posted", type=int, help="Jobs posted within N days", default=7)
    parser.add_argument("--max-pages", type=int, help="Maximum pages to fetch", default=None)
    parser.add_argument("--who-may-apply", help="Who may apply filter", default="public")
    parser.add_argument("--remote", action="store_true", help="Remote jobs only")
    parser.add_argument("--output", help="Output filename", default="usajobs_full.json")
    
    args = parser.parse_args()
    
    # Build search parameters
    params = {
        "WhoMayApply": args.who_may_apply,
        "ResultsPerPage": 500,  # Max allowed by API
        "Fields": "full",
        "SortField": "DatePosted",
        "SortDirection": "desc"
    }
    
    # Add date filter if specified (jobs posted within N days)
    if args.days_posted:
        params["DatePosted"] = args.days_posted
    
    # Add keyword if provided
    if args.keyword:
        params["Keyword"] = args.keyword
    
    # Add remote filter if requested
    if args.remote:
        params["RemoteIndicator"] = "true"
    
    print(f"Search parameters: {params}")
    
    # Fetch all jobs
    jobs = fetch_all_jobs(params, max_pages=args.max_pages)
    
    # Save to file
    if jobs:
        save_jobs_data(jobs, args.output)

if __name__ == "__main__":
    main()