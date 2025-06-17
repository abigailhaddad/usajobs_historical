#!/usr/bin/env python3
"""Quick script to check for content mismatches"""

import pandas as pd

# Load overlap data
overlap_df = pd.read_parquet('data/overlap_samples.parquet')

# Get pairs for comparison
unique_controls = overlap_df['control_number'].unique()
print(f'Found {len(unique_controls)} unique control numbers')

# Check a sample
sample_size = min(100, len(unique_controls))
control_nums = unique_controls[:sample_size]

mismatches = {'major_duties': 0, 'qualification_summary': 0, 'requirements': 0}
checked = 0
both_have_content = {'major_duties': 0, 'qualification_summary': 0, 'requirements': 0}

for control in control_nums:
    pair = overlap_df[overlap_df['control_number'] == control]
    if len(pair) == 2:  # Ensure we have both historical and current
        hist = pair[pair['source_type'] == 'historical'].iloc[0]
        curr = pair[pair['source_type'] == 'current'].iloc[0]
        
        for field in ['major_duties', 'qualification_summary', 'requirements']:
            hist_val = hist.get(field, '')
            curr_val = curr.get(field, '')
            
            hist_has = pd.notna(hist_val) and len(str(hist_val).strip()) > 0
            curr_has = pd.notna(curr_val) and len(str(curr_val).strip()) > 0
            
            if hist_has and curr_has:
                both_have_content[field] += 1
                if str(hist_val).strip() != str(curr_val).strip():
                    mismatches[field] += 1
                    
                    # Show first mismatch example
                    if mismatches[field] == 1:
                        print(f'\nExample mismatch for {field} (Control: {control}):')
                        print(f'  Historical ({len(str(hist_val))} chars): {str(hist_val)[:200]}...')
                        print(f'  Current ({len(str(curr_val))} chars): {str(curr_val)[:200]}...')
        
        checked += 1

print(f'\n\nChecked {checked} job pairs')
print('\nField comparison results:')
for field in ['major_duties', 'qualification_summary', 'requirements']:
    if both_have_content[field] > 0:
        mismatch_rate = (mismatches[field] / both_have_content[field]) * 100
        print(f'  {field}: {mismatches[field]} mismatches out of {both_have_content[field]} pairs with content ({mismatch_rate:.1f}% mismatch rate)')
    else:
        print(f'  {field}: No pairs where both sources have content')