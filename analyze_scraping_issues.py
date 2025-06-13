#!/usr/bin/env python3
"""
Systematically analyze differences between scraped content and API content
"""

import pandas as pd

# Load overlap data and systematically analyze differences
overlap_df = pd.read_parquet('usajobs_pipeline/data_parquet/overlap_samples/overlap_samples_20250613_153820.parquet')

print('=== SYSTEMATIC ANALYSIS OF SCRAPING VS API ===')

content_fields = ['requirements', 'education', 'benefits', 'major_duties', 'qualification_summary']
issues = {}

for field in content_fields:
    issues[field] = {
        'scraper_too_long': [],
        'scraper_missing': [],
        'content_mismatch': []
    }

# Analyze all overlap pairs
for control_num in overlap_df['control_number'].unique():
    hist_record = overlap_df[(overlap_df['control_number'] == control_num) & (overlap_df['source_type'] == 'historical')].iloc[0]
    curr_record = overlap_df[(overlap_df['control_number'] == control_num) & (overlap_df['source_type'] == 'current')].iloc[0]
    
    for field in content_fields:
        hist_val = str(hist_record.get(field, '')) if pd.notna(hist_record.get(field)) else ''
        curr_val = str(curr_record.get(field, '')) if pd.notna(curr_record.get(field)) else ''
        
        hist_len = len(hist_val)
        curr_len = len(curr_val)
        
        # Scraper got way more content than API (likely grabbing extra)
        if hist_len > curr_len * 2 and curr_len > 0:
            issues[field]['scraper_too_long'].append((control_num, hist_len, curr_len))
        
        # API has content but scraper missed it
        elif curr_len > 100 and hist_len < 50:
            issues[field]['scraper_missing'].append((control_num, hist_len, curr_len))
        
        # Different content (check first 50 chars)
        elif hist_len > 50 and curr_len > 50:
            hist_start = hist_val[:50].lower().strip()
            curr_start = curr_val[:50].lower().strip()
            if hist_start != curr_start and curr_start not in hist_start:
                issues[field]['content_mismatch'].append((control_num, hist_start, curr_start))

print('\nISSUES FOUND:')
for field, field_issues in issues.items():
    print(f'\n{field.upper()}:')
    
    if field_issues['scraper_too_long']:
        print(f'  🔴 Scraper grabbing too much content: {len(field_issues["scraper_too_long"])} cases')
        for control, hist_len, curr_len in field_issues['scraper_too_long'][:3]:
            print(f'    {control}: scraped {hist_len} chars vs API {curr_len} chars')
    
    if field_issues['scraper_missing']:
        print(f'  🟡 Scraper missing content: {len(field_issues["scraper_missing"])} cases')
        for control, hist_len, curr_len in field_issues['scraper_missing'][:3]:
            print(f'    {control}: scraped {hist_len} chars vs API {curr_len} chars')
    
    if field_issues['content_mismatch']:
        print(f'  🟠 Content mismatch: {len(field_issues["content_mismatch"])} cases')
        for control, hist_start, curr_start in field_issues['content_mismatch'][:2]:
            print(f'    {control}:')
            print(f'      Scraped: {hist_start}...')
            print(f'      API: {curr_start}...')
    
    if not any(field_issues.values()):
        print(f'  ✅ No major issues detected')

# Print specific problematic cases for manual investigation
print('\n=== MOST PROBLEMATIC CASES FOR INVESTIGATION ===')
for field in content_fields:
    if issues[field]['scraper_too_long']:
        control, hist_len, curr_len = issues[field]['scraper_too_long'][0]
        print(f'{field}: Check job {control} - scraper got {hist_len} chars vs API {curr_len} chars')
        print(f'  URL: https://www.usajobs.gov/job/{control}')