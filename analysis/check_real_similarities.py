#!/usr/bin/env python3
"""
Check what the actual similarity scores are
"""

import sys
sys.path.append('src')

from parquet_storage import ParquetJobStorage
from simple_validation import calculate_field_overlap, generate_simple_validation_summary
import pandas as pd
from difflib import SequenceMatcher

def check_actual_similarities():
    """Check what the real similarity scores are"""
    
    # Load overlap data
    storage = ParquetJobStorage('data')
    overlap_df = storage.load_overlap_samples()
    
    print(f"📊 Loaded {len(overlap_df)} overlap records")
    
    # Separate records by source type
    historical_records = overlap_df[overlap_df['source_type'] == 'historical'].set_index('control_number')
    current_records = overlap_df[overlap_df['source_type'] == 'current'].set_index('control_number')
    
    # Find overlapping control numbers
    overlap_controls = set(historical_records.index) & set(current_records.index)
    print(f"📊 Found {len(overlap_controls)} overlapping control numbers")
    
    # Check a few specific examples
    sample_controls = list(overlap_controls)[:5]
    
    def similarity(a, b):
        return SequenceMatcher(None, str(a).lower().strip(), str(b).lower().strip()).ratio()
    
    for i, control_number in enumerate(sample_controls):
        print(f"\n🔍 Sample {i+1}: Job {control_number}")
        
        historical_record = historical_records.loc[control_number]
        current_record = current_records.loc[control_number]
        
        for field in ['major_duties', 'qualification_summary', 'requirements', 'education']:
            hist_content = str(historical_record.get(field, '')).strip()
            curr_content = str(current_record.get(field, '')).strip()
            
            if len(hist_content) > 20 and len(curr_content) > 20:
                sim = similarity(hist_content, curr_content)
                print(f"  {field}: {sim:.3f} similarity")
                print(f"    Historical: {len(hist_content)} chars: {hist_content[:100]}...")
                print(f"    Current: {len(curr_content)} chars: {curr_content[:100]}...")
            else:
                print(f"  {field}: No content to compare (hist: {len(hist_content)}, curr: {len(curr_content)})")
    
    # Run full validation
    print("\n📊 Running full field overlap analysis...")
    results = calculate_field_overlap(overlap_df)
    
    if results['status'] == 'success':
        print(f"\nResults for {results['total_overlap_jobs']} overlap jobs:")
        for field, stats in results['field_stats'].items():
            print(f"\n{field}:")
            print(f"  Both have content: {stats['both_have_content']} jobs ({stats['both_coverage']:.1f}%)")
            print(f"  Perfect matches (≥99%): {stats['perfect_matches']} ({stats['perfect_match_pct']:.1f}%)")
            print(f"  Good matches (≥95%): {stats['good_matches']} ({stats['good_match_pct']:.1f}%)")
            print(f"  Average similarity: {stats['avg_similarity']:.3f}")
    else:
        print(f"❌ Validation failed: {results.get('message')}")

if __name__ == "__main__":
    check_actual_similarities()