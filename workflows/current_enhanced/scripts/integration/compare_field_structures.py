#!/usr/bin/env python3
"""
Field Structure Comparison Tool

Compares field structures between:
1. Historical USAJobs API
2. Current USAJobs Search API
3. Scraped job content

Usage:
    python compare_field_structures.py
"""

import json
import sys
import os
import duckdb
from pathlib import Path

def get_historical_fields():
    """Get field structure from historical API database."""
    hist_db_path = "../../../historical_api/data/historical_jobs_2015.duckdb"
    
    if not os.path.exists(hist_db_path):
        print(f"❌ Historical database not found: {hist_db_path}")
        return {}
    
    try:
        conn = duckdb.connect(hist_db_path)
        
        # Get column info
        columns = conn.execute("PRAGMA table_info('historical_jobs')").fetchall()
        
        # Get sample data
        sample = conn.execute("SELECT * FROM historical_jobs LIMIT 1").fetchone()
        conn.close()
        
        return {
            "columns": [col[1] for col in columns],  # column names
            "sample_data": dict(zip([col[1] for col in columns], sample if sample else []))
        }
        
    except Exception as e:
        print(f"❌ Error reading historical database: {e}")
        return {}

def get_current_fields():
    """Get field structure from current API JSON."""
    current_files = list(Path("../../data").glob("current_jobs_*.json"))
    
    if not current_files:
        print("❌ No current API JSON files found")
        return {}
    
    latest_file = max(current_files, key=os.path.getctime)
    
    try:
        with open(latest_file, 'r') as f:
            data = json.load(f)
        
        jobs = data.get("SearchResult", {}).get("SearchResultItems", [])
        if not jobs:
            return {}
        
        # Get structure from first job
        sample_job = jobs[0]
        matched_obj = sample_job.get("MatchedObjectDescriptor", {})
        
        return {
            "raw_structure": list(matched_obj.keys()),
            "sample_data": matched_obj
        }
        
    except Exception as e:
        print(f"❌ Error reading current API file: {e}")
        return {}

def get_scraped_fields():
    """Get field structure from scraping scripts."""
    scrape_script = "../scraping/scrape_job_posting.py"
    
    if not os.path.exists(scrape_script):
        print(f"❌ Scraping script not found: {scrape_script}")
        return {}
    
    try:
        with open(scrape_script, 'r') as f:
            content = f.read()
        
        # Look for field extraction patterns
        import re
        
        # Find fields being extracted
        field_patterns = [
            r'"([^"]+)"\s*:\s*job_data\.get\(',
            r"'([^']+)'\s*:\s*job_data\.get\(",
            r'extract_(\w+)\(',
            r'get_(\w+)\(',
        ]
        
        extracted_fields = set()
        for pattern in field_patterns:
            matches = re.findall(pattern, content)
            extracted_fields.update(matches)
        
        return {
            "extracted_fields": sorted(list(extracted_fields)),
            "script_path": scrape_script
        }
        
    except Exception as e:
        print(f"❌ Error reading scraping script: {e}")
        return {}

def compare_field_mappings():
    """Compare and analyze field mappings between data sources."""
    
    print("🔍 USAJobs Field Structure Comparison")
    print("=" * 50)
    
    # Get data from each source
    historical = get_historical_fields()
    current = get_current_fields()
    scraped = get_scraped_fields()
    
    print(f"\n📊 HISTORICAL API FIELDS ({len(historical.get('columns', []))} fields):")
    if historical.get('columns'):
        for field in sorted(historical['columns']):
            print(f"  • {field}")
    
    print(f"\n📊 CURRENT API FIELDS ({len(current.get('raw_structure', []))} fields):")
    if current.get('raw_structure'):
        for field in sorted(current['raw_structure']):
            print(f"  • {field}")
    
    print(f"\n📊 SCRAPED FIELDS ({len(scraped.get('extracted_fields', []))} fields):")
    if scraped.get('extracted_fields'):
        for field in sorted(scraped['extracted_fields']):
            print(f"  • {field}")
    
    # Field overlap analysis
    print(f"\n🔄 FIELD MAPPING ANALYSIS:")
    print("-" * 30)
    
    # Common core fields that should be mappable
    core_fields = [
        "position_title", "agency", "location", "salary", "grade", 
        "open_date", "close_date", "job_series", "control_number"
    ]
    
    print("Core Field Availability:")
    for core_field in core_fields:
        hist_match = any(core_field.lower() in field.lower() for field in historical.get('columns', []))
        curr_match = any(core_field.lower() in field.lower() for field in current.get('raw_structure', []))
        scrape_match = any(core_field.lower() in field.lower() for field in scraped.get('extracted_fields', []))
        
        print(f"  {core_field:15} | Hist: {'✓' if hist_match else '✗'} | Curr: {'✓' if curr_match else '✗'} | Scrape: {'✓' if scrape_match else '✗'}")
    
    # Show sample current API job structure
    if current.get('sample_data'):
        print(f"\n📝 SAMPLE CURRENT API JOB STRUCTURE:")
        print("-" * 40)
        sample = current['sample_data']
        
        key_fields = [
            'PositionID', 'PositionTitle', 'DepartmentName', 'SubAgency',
            'JobCategory', 'JobGrade', 'PositionRemuneration', 'PositionLocation',
            'PositionStartDate', 'PositionEndDate', 'QualificationSummary'
        ]
        
        for field in key_fields:
            if field in sample:
                value = sample[field]
                if isinstance(value, (list, dict)):
                    print(f"  {field:20} | {type(value).__name__}: {len(value) if hasattr(value, '__len__') else 'N/A'} items")
                else:
                    print(f"  {field:20} | {str(value)[:60]}{'...' if len(str(value)) > 60 else ''}")
    
    print(f"\n💡 RECOMMENDATIONS:")
    print("-" * 20)
    print("1. Use PositionID from current API as control_number for historical mapping")
    print("2. Map DepartmentName/SubAgency to historical hiring_agency_name")
    print("3. Extract JobCategory[0].Code for job_series mapping")
    print("4. Use PositionRemuneration for salary field rationalization")
    print("5. Combine current API + scraping for complete job profiles")

def main():
    """Main function."""
    try:
        compare_field_mappings()
    except KeyboardInterrupt:
        print("\n👋 Comparison interrupted")
    except Exception as e:
        print(f"❌ Error during comparison: {e}")

if __name__ == "__main__":
    main()