#!/usr/bin/env python3
"""
Quick script to test which years are available in the USAJobs historical API
"""

import requests
import json

def test_year(year):
    """Test if January 1 of a given year has data in the API"""
    url = "https://data.usajobs.gov/api/historicjoa"
    params = {
        'startpositionopendate': f'{year}-01-01',
        'endpositionopendate': f'{year}-01-01'
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            # Print raw response to debug
            print(f"🔍 {year}: Status 200, checking response...")
            try:
                data = response.json()
                # Try different possible response formats
                if 'SearchResult' in data:
                    items = data['SearchResult'].get('SearchResultItems', [])
                    job_count = len(items) if items else 0
                elif 'Jobs' in data:
                    job_count = len(data['Jobs'])
                elif isinstance(data, list):
                    job_count = len(data)
                else:
                    print(f"📋 {year}: Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                    job_count = "Unknown format"
                
                print(f"✅ {year}: {job_count} jobs found on Jan 1")
                return True
            except Exception as e:
                print(f"🔍 {year}: JSON parse error: {e}")
                print(f"📄 {year}: Raw response (first 200 chars): {response.text[:200]}")
                return False
        elif response.status_code == 400:
            print(f"❌ {year}: Bad request (likely invalid date range)")
            return False
        else:
            print(f"⚠️  {year}: HTTP {response.status_code}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"🚫 {year}: Network error - {e}")
        return False
    except json.JSONDecodeError:
        print(f"⚠️  {year}: Invalid JSON response")
        return False

if __name__ == "__main__":
    print("🔍 Testing USAJobs Historical API availability by year...")
    print("Testing January 1st of each year from 2010-2019")
    print("")
    
    available_years = []
    
    for year in range(2010, 2020):
        if test_year(year):
            available_years.append(year)
    
    print("")
    print("📊 Summary:")
    if available_years:
        print(f"✅ Available years: {', '.join(map(str, available_years))}")
        print(f"📅 Earliest available year: {min(available_years)}")
        print(f"📅 Latest tested year: {max(available_years)}")
    else:
        print("❌ No years found with data")
    
    print("")
    print("💡 To pull historical data for available years:")
    if available_years:
        year_list = ' '.join(map(str, available_years))
        print(f"   ./run_parallel.sh {year_list}")