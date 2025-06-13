#\!/usr/bin/env python3
"""
Debug field mapping issues
"""

import pandas as pd
import json

# Load samples and check specific field mapping
df = pd.read_parquet('data_parquet/overlap_samples/overlap_samples_20250613_140859.parquet')
hist_samples = df[df['source_type'] == 'historical']

print('=== FIELD MAPPING CHECK ===')
sample = hist_samples.iloc[0]

print(f'Control: {sample["control_number"]}')
req_val = sample['requirements']
edu_val = sample['education']
req_check = 'YES' if pd.notna(req_val) and req_val != '' else 'NO'
edu_check = 'YES' if pd.notna(edu_val) and edu_val != '' else 'NO'
print(f'requirements field: {req_check} - {str(req_val)[:100] if pd.notna(req_val) else "None"}...')
print(f'education field: {edu_check} - {str(edu_val)[:100] if pd.notna(edu_val) else "None"}...')

# Check scraped sections
if pd.notna(sample['scraped_sections']):
    sections = json.loads(sample['scraped_sections'])
    req_scraped = 'YES' if 'Requirements' in sections else 'NO'
    edu_scraped = 'YES' if 'Education' in sections else 'NO'
    print(f'Requirements in scraped_sections: {req_scraped}')
    if 'Requirements' in sections:
        print(f'  Content: {sections["Requirements"][:200]}...')
    print(f'Education in scraped_sections: {edu_scraped}')  
    if 'Education' in sections:
        print(f'  Content: {sections["Education"][:200]}...')

# Check current samples too
print('\n=== CURRENT SAMPLES ===')
curr_samples = df[df['source_type'] == 'current']
sample_curr = curr_samples.iloc[0]

print(f'Current Control: {sample_curr["control_number"]}')
req_val_curr = sample_curr['requirements']
edu_val_curr = sample_curr['education']
req_check_curr = 'YES' if pd.notna(req_val_curr) and req_val_curr != '' else 'NO'
edu_check_curr = 'YES' if pd.notna(edu_val_curr) and edu_val_curr != '' else 'NO'
print(f'requirements field: {req_check_curr} - {str(req_val_curr)[:100] if pd.notna(req_val_curr) else "None"}...')
print(f'education field: {edu_check_curr} - {str(edu_val_curr)[:100] if pd.notna(edu_val_curr) else "None"}...')

# Check current samples - do they have scraped data at all?
print(f'Current sample has scraped_sections: {pd.notna(sample_curr["scraped_sections"])}')
