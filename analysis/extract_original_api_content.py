#!/usr/bin/env python3
"""
Extract original API content for Historical vs Current comparison
"""

import sys
sys.path.append('src')

import pandas as pd
from pathlib import Path
import json
from difflib import SequenceMatcher

def extract_original_api_content():
    """Extract and compare original Historical vs Current API content"""
    
    # Load the main unified dataset to get overlap control numbers
    unified_df = pd.read_parquet('data/usajobs.parquet')
    print(f"📊 Loaded {len(unified_df)} unified records")
    
    # Find records that exist in both historical and current APIs
    # These should have data_sources containing both API types
    overlap_records = []
    
    for _, record in unified_df.iterrows():
        data_sources = record.get('data_sources', [])
        if isinstance(data_sources, str):
            try:
                data_sources = json.loads(data_sources)
            except:
                data_sources = []
        
        # Look for records that have both historical and current API data
        has_historical = any('historical' in str(source).lower() for source in data_sources)
        has_current = any('current' in str(source).lower() for source in data_sources)
        
        if has_historical and has_current:
            overlap_records.append(record)
    
    print(f"📊 Found {len(overlap_records)} records with both API sources")
    
    if len(overlap_records) == 0:
        print("❌ No overlap records found. Let me check data_sources field...")
        
        # Debug: check what data_sources look like
        sample_records = unified_df.head(10)
        for i, record in sample_records.iterrows():
            control_num = record.get('control_number', 'Unknown')
            data_sources = record.get('data_sources', 'None')
            source_type = record.get('source_type', 'Unknown')
            print(f"  Job {control_num} ({source_type}): {data_sources}")
        
        return
    
    # Let's try a different approach - look at the overlap_samples file
    # which should have the original comparison data
    print("\n📊 Checking overlap_samples for original API data...")
    
    overlap_df = pd.read_parquet('data/overlap_samples.parquet')
    print(f"📊 Loaded {len(overlap_df)} overlap samples")
    
    # Group by control number
    control_groups = {}
    for _, row in overlap_df.iterrows():
        control_num = row['control_number']
        source_type = row['source_type']
        if control_num not in control_groups:
            control_groups[control_num] = {}
        control_groups[control_num][source_type] = row
    
    # Find jobs with both historical and current
    comparison_pairs = []
    for control_num, sources in control_groups.items():
        if 'historical' in sources and 'current' in sources:
            comparison_pairs.append({
                'control_number': control_num,
                'historical': sources['historical'],
                'current': sources['current']
            })
    
    print(f"📊 Found {len(comparison_pairs)} job pairs for comparison")
    
    if len(comparison_pairs) == 0:
        print("❌ No comparison pairs found")
        return
    
    # Now let's look for ORIGINAL API content fields
    # Historical API fields vs Current API fields
    historical_fields = [
        'JobTitle', 'OrganizationName', 'DepartmentName', 'JobSummary',
        'MajorDuties', 'Education', 'Requirements', 'Evaluations',
        'HowToApply', 'WhatToExpectNext', 'RequiredDocuments'
    ]
    
    current_fields = [
        'PositionTitle', 'AgencyName', 'AgencySubElement', 
        'PositionFormattedDescription', 'QualificationSummary'
    ]
    
    # Check what fields are actually available in our data
    print("\n🔍 Checking available fields in historical records...")
    sample_hist = comparison_pairs[0]['historical']
    hist_available = [field for field in historical_fields if field in sample_hist.index and pd.notna(sample_hist[field])]
    print(f"Available historical fields: {hist_available}")
    
    print("\n🔍 Checking available fields in current records...")
    sample_curr = comparison_pairs[0]['current']
    curr_available = [field for field in current_fields if field in sample_curr.index and pd.notna(sample_curr[field])]
    print(f"Available current fields: {curr_available}")
    
    # Let's examine one job in detail
    print(f"\n🔍 Detailed examination of job {comparison_pairs[0]['control_number']}:")
    
    hist_record = comparison_pairs[0]['historical']
    curr_record = comparison_pairs[0]['current']
    
    print("\nHistorical record fields with content:")
    for field in hist_record.index:
        if pd.notna(hist_record[field]) and len(str(hist_record[field])) > 50:
            content = str(hist_record[field])
            print(f"  {field}: {len(content)} chars - {content[:100]}...")
    
    print("\nCurrent record fields with content:")
    for field in curr_record.index:
        if pd.notna(curr_record[field]) and len(str(curr_record[field])) > 50:
            content = str(curr_record[field])
            print(f"  {field}: {len(content)} chars - {content[:100]}...")

if __name__ == "__main__":
    extract_original_api_content()