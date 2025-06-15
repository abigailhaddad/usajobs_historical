#!/usr/bin/env python3
"""Check what data each source actually contains"""

import pandas as pd
import json

# Load overlap data
overlap_df = pd.read_parquet('data/overlap_samples.parquet')

# Analyze data availability by source
print("Data availability by source type:\n")

for source in ['historical', 'current']:
    source_df = overlap_df[overlap_df['source_type'] == source]
    print(f"\n{source.upper()} SOURCE ({len(source_df)} records):")
    
    # Check key content fields
    content_fields = ['major_duties', 'qualification_summary', 'requirements', 'education', 
                      'benefits', 'how_to_apply', 'job_summary']
    
    for field in content_fields:
        if field in source_df.columns:
            has_content = source_df[field].notna() & (source_df[field].astype(str).str.strip().str.len() > 0)
            count = has_content.sum()
            pct = (count / len(source_df)) * 100
            
            # Show sample if exists
            if count > 0:
                sample = source_df[has_content].iloc[0][field]
                sample_len = len(str(sample))
                print(f"  {field}: {count} records ({pct:.1f}%) - Sample length: {sample_len} chars")
            else:
                print(f"  {field}: {count} records ({pct:.1f}%)")
        else:
            print(f"  {field}: NOT IN COLUMNS")

# Check data_sources field to understand where data comes from
print("\n\nChecking data_sources field in unified data:")
unified_df = pd.read_parquet('data/usajobs.parquet')
print(f"Unified dataset has {len(unified_df)} records")

# Sample some records to see data_sources
sample = unified_df.sample(min(10, len(unified_df)))
for idx, row in sample.iterrows():
    control = row.get('control_number', 'Unknown')
    sources = row.get('data_sources', [])
    if isinstance(sources, str):
        try:
            sources = json.loads(sources)
        except:
            pass
    print(f"\nControl {control}: {sources}")
    
    # Check if this record has content
    has_duties = pd.notna(row.get('major_duties')) and len(str(row.get('major_duties', '')).strip()) > 0
    has_qual = pd.notna(row.get('qualification_summary')) and len(str(row.get('qualification_summary', '')).strip()) > 0
    print(f"  Has major_duties: {has_duties}")
    print(f"  Has qualification_summary: {has_qual}")