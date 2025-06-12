#!/usr/bin/env python3
"""
Field Crosswalk Analysis

Shows exact field mappings between Historical API, Current API, and Unified Schema
with real data examples.

Usage:
    python field_crosswalk_analysis.py
"""

import json
import os
import duckdb
from pathlib import Path

def get_sample_historical_record():
    """Get a sample historical record to show field structure"""
    hist_db_path = "../../../historical_api/data/historical_jobs_2015.duckdb"
    
    if not os.path.exists(hist_db_path):
        return None
    
    try:
        conn = duckdb.connect(hist_db_path)
        result = conn.execute("SELECT * FROM historical_jobs LIMIT 1").fetchone()
        columns = [desc[0] for desc in conn.description]
        conn.close()
        
        if result:
            return dict(zip(columns, result))
    except:
        pass
    return None

def get_sample_current_record():
    """Get a sample current API record"""
    current_files = list(Path("../../data").glob("current_jobs_*.json"))
    
    if not current_files:
        return None
    
    try:
        latest_file = max(current_files, key=os.path.getctime)
        with open(latest_file, 'r') as f:
            data = json.load(f)
        
        jobs = data.get("SearchResult", {}).get("SearchResultItems", [])
        if jobs:
            # Get the raw structure, not the flattened version
            return jobs[0].get("MatchedObjectDescriptor", {})
    except:
        pass
    return None

def show_field_crosswalk():
    """Show detailed field crosswalk with real examples"""
    
    print("🔍 USAJOBS FIELD CROSSWALK ANALYSIS")
    print("=" * 60)
    
    # Get sample records
    hist_sample = get_sample_historical_record()
    curr_sample = get_sample_current_record()
    
    if not hist_sample:
        print("❌ No historical sample data found")
        return
    
    if not curr_sample:
        print("❌ No current API sample data found")
        return
    
    print(f"\n📊 HISTORICAL API FIELDS ({len(hist_sample)} total):")
    print("-" * 50)
    for field, value in sorted(hist_sample.items()):
        if value is not None:
            value_str = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
            print(f"  {field:25} | {value_str}")
    
    print(f"\n📊 CURRENT API FIELDS ({len(curr_sample)} total):")
    print("-" * 50)
    for field, value in sorted(curr_sample.items()):
        if value is not None:
            if isinstance(value, (list, dict)):
                value_str = f"{type(value).__name__}({len(value) if hasattr(value, '__len__') else 'N/A'})"
            else:
                value_str = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
            print(f"  {field:25} | {value_str}")
    
    # Show detailed field mappings
    print(f"\n🔄 DETAILED FIELD CROSSWALK:")
    print("=" * 60)
    
    # Core mappings that work
    mappings = [
        {
            'category': 'IDENTIFIERS',
            'mappings': [
                ('control_number', 'PositionID', 'control_number'),
                ('announcement_number', 'n/a', 'announcement_number'),
            ]
        },
        {
            'category': 'BASIC INFO',
            'mappings': [
                ('position_title', 'PositionTitle', 'position_title'),
                ('hiring_agency_name', 'DepartmentName', 'agency_name'),
                ('hiring_department_name', 'DepartmentName', 'department_name'),
                ('hiring_subelement_name', 'SubAgency', 'sub_agency'),
            ]
        },
        {
            'category': 'CLASSIFICATION',
            'mappings': [
                ('job_series', 'JobCategory[].Code', 'job_series'),
                ('minimum_grade', 'JobGrade[].Code', 'min_grade'),
                ('maximum_grade', 'JobGrade[].Code', 'max_grade'),
                ('pay_scale', 'n/a', 'pay_scale'),
            ]
        },
        {
            'category': 'COMPENSATION',
            'mappings': [
                ('minimum_salary', 'PositionRemuneration[].MinimumRange', 'min_salary'),
                ('maximum_salary', 'PositionRemuneration[].MaximumRange', 'max_salary'),
            ]
        },
        {
            'category': 'DATES',
            'mappings': [
                ('position_open_date', 'PositionStartDate', 'open_date'),
                ('position_close_date', 'PositionEndDate', 'close_date'),
                ('n/a', 'PublicationStartDate', 'posted_date'),
            ]
        },
        {
            'category': 'LOCATION',
            'mappings': [
                ('locations', 'PositionLocation[]', 'locations'),
                ('n/a', 'PositionLocationDisplay', 'primary_location'),
            ]
        },
        {
            'category': 'WORK DETAILS',
            'mappings': [
                ('work_schedule', 'PositionSchedule[].Name', 'work_schedule'),
                ('travel_requirement', 'n/a', 'travel_requirement'),
                ('telework_eligible', 'n/a', 'telework_eligible'),
                ('security_clearance_required', 'n/a', 'security_clearance_required'),
                ('who_may_apply', 'UserArea.Details.WhoMayApply', 'who_may_apply'),
            ]
        },
        {
            'category': 'RICH CONTENT (Current API Only)',
            'mappings': [
                ('n/a', 'QualificationSummary', 'qualification_summary'),
                ('n/a', 'PositionFormattedDescription', 'major_duties'),
                ('n/a', 'ApplyURI[]', 'apply_url'),
                ('n/a', 'PositionURI', 'position_uri'),
            ]
        }
    ]
    
    for category_info in mappings:
        print(f"\n📂 {category_info['category']}:")
        print(f"{'Historical API':25} | {'Current API':30} | {'Unified Schema':20} | Status")
        print("-" * 85)
        
        for hist_field, curr_field, unified_field in category_info['mappings']:
            # Check if fields exist in sample data
            hist_exists = "✓" if hist_field != 'n/a' and hist_field in hist_sample else "✗"
            curr_exists = "✓" if curr_field != 'n/a' and any(curr_field.split('[')[0] in curr_sample for curr_field in [curr_field]) else "✗"
            
            # Determine mapping status
            if hist_exists == "✓" and curr_exists == "✓":
                status = "🔄 MAPPED"
            elif hist_exists == "✓":
                status = "📊 HIST ONLY"
            elif curr_exists == "✓":
                status = "🆕 CURR ONLY"
            else:
                status = "❌ MISSING"
            
            print(f"{hist_field:25} | {curr_field:30} | {unified_field:20} | {status}")
    
    # Show actual data examples
    print(f"\n💡 REAL DATA EXAMPLES:")
    print("=" * 40)
    
    examples = [
        ('PositionID', 'control_number'),
        ('PositionTitle', 'position_title'),
        ('DepartmentName', 'agency_name'),
        ('PositionLocationDisplay', 'location'),
        ('QualificationSummary', 'rich_content')
    ]
    
    for curr_api_field, description in examples:
        if curr_api_field in curr_sample:
            value = curr_sample[curr_api_field]
            if isinstance(value, str):
                display_value = value[:80] + "..." if len(value) > 80 else value
            else:
                display_value = str(value)
            print(f"\n{description.upper()}:")
            print(f"  Current API ({curr_api_field}): {display_value}")
    
    # Show field coverage summary
    print(f"\n📈 FIELD COVERAGE SUMMARY:")
    print("=" * 30)
    
    hist_fields = set(hist_sample.keys())
    curr_fields = set(curr_sample.keys())
    
    print(f"Historical API fields: {len(hist_fields)}")
    print(f"Current API fields: {len(curr_fields)}")
    print(f"Overlapping concepts: ~15 (position, agency, dates, location, etc.)")
    print(f"Current API unique: Rich content fields (QualificationSummary, etc.)")
    print(f"Historical API unique: Metadata fields (hiring_paths, vendor, etc.)")
    
    print(f"\n🎯 RATIONALIZATION STRATEGY:")
    print("=" * 35)
    print("1. ✅ Use Historical API for comprehensive metadata")
    print("2. ✅ Use Current API for rich content and current job details")
    print("3. ✅ Use Scraping to fill gaps and extract additional structured data")
    print("4. ✅ Apply confidence scoring based on data source reliability")
    print("5. ✅ Standardize field formats (dates, salaries, grades) during mapping")

def main():
    show_field_crosswalk()

if __name__ == "__main__":
    main()