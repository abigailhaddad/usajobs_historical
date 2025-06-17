#!/usr/bin/env python3
"""Analyze how data flows through the rationalization process"""

import pandas as pd
import json

print("=== ANALYZING DATA FLOW ===\n")

# 1. Check unified dataset to see what actually has content
unified_df = pd.read_parquet('data/usajobs.parquet')
print(f"1. UNIFIED DATASET: {len(unified_df)} total records")

# Check records with different data sources
has_scraping = unified_df['data_sources'].astype(str).str.contains('scraping')
has_current = unified_df['data_sources'].astype(str).str.contains('current')
has_historical = unified_df['data_sources'].astype(str).str.contains('historical')

print(f"   - With scraping data: {has_scraping.sum()}")
print(f"   - With current API: {has_current.sum()}")
print(f"   - With historical API: {has_historical.sum()}")

# Check content availability by data source type
print("\n2. CONTENT AVAILABILITY IN UNIFIED DATASET:")

# Historical-only records
hist_only = has_historical & ~has_current
print(f"\n   Historical-only records ({hist_only.sum()}):")
hist_sample = unified_df[hist_only].head(5)
for field in ['major_duties', 'qualification_summary']:
    has_field = hist_sample[field].notna() & (hist_sample[field].astype(str).str.strip().str.len() > 0)
    print(f"     {field}: {has_field.sum()}/{len(hist_sample)} have content")

# Current-priority records (duplicates)
current_priority = unified_df['data_sources'].astype(str).str.contains('current_api_priority')
print(f"\n   Current-priority records (duplicates) ({current_priority.sum()}):")
curr_sample = unified_df[current_priority].head(5)
for field in ['major_duties', 'qualification_summary']:
    has_field = curr_sample[field].notna() & (curr_sample[field].astype(str).str.strip().str.len() > 0)
    print(f"     {field}: {has_field.sum()}/{len(curr_sample)} have content")

# 3. Check overlap samples
print("\n3. OVERLAP SAMPLES ISSUE:")
overlap_df = pd.read_parquet('data/overlap_samples.parquet')

# Get a specific example
example_control = overlap_df['control_number'].iloc[0]
example_pair = overlap_df[overlap_df['control_number'] == example_control]

print(f"\n   Example: Control number {example_control}")
for _, row in example_pair.iterrows():
    source = row['source_type']
    has_duties = pd.notna(row.get('major_duties')) and len(str(row.get('major_duties', '')).strip()) > 0
    has_qual = pd.notna(row.get('qualification_summary')) and len(str(row.get('qualification_summary', '')).strip()) > 0
    
    print(f"\n   {source.upper()} version:")
    print(f"     - Has major_duties: {has_duties}")
    print(f"     - Has qualification_summary: {has_qual}")
    
    # Check if this job should have scraped content
    unified_record = unified_df[unified_df['control_number'] == example_control]
    if not unified_record.empty:
        unified_sources = unified_record.iloc[0]['data_sources']
        print(f"     - Unified record data sources: {unified_sources}")

print("\n4. THE PROBLEM:")
print("   - Historical API doesn't provide content fields (major_duties, etc.)")
print("   - Scraped content is supposed to fill this gap")
print("   - But overlap samples are saved BEFORE scraping is merged")
print("   - So historical samples in overlap_samples have no content to compare")
print("\n   This is why mismatch analysis finds 0 mismatches - there's no content in historical samples!")