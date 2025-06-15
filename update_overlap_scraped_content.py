#!/usr/bin/env python3
"""
Update overlap data with current scraper output for better comparison
"""

import sys
sys.path.append('src')

from parquet_storage import ParquetJobStorage
from scrape_enhanced_job_posting import scrape_enhanced_job_posting
import pandas as pd
import json
from tqdm import tqdm

def update_overlap_scraped_content():
    """Update overlap data with current scraper output"""
    
    # Load overlap data
    storage = ParquetJobStorage('data')
    overlap_df = storage.load_overlap_samples()
    
    print(f"📊 Loaded {len(overlap_df)} overlap samples")
    
    # Find records that have scraped_sections (historical records)
    historical_records = overlap_df[overlap_df['source_type'] == 'historical'].copy()
    records_with_scraped = historical_records[historical_records['scraped_sections'].notna()]
    
    print(f"📊 Found {len(records_with_scraped)} historical records with scraped content")
    
    # Update scraped content for these records
    updated_count = 0
    
    for idx, record in tqdm(records_with_scraped.iterrows(), desc="Updating scraped content", total=len(records_with_scraped)):
        control_number = record['control_number']
        
        try:
            # Get fresh scraped content using current scraper
            scraped_data = scrape_enhanced_job_posting(control_number, force_refresh=False)
            
            if scraped_data.get('status') == 'success':
                new_content_sections = scraped_data.get('content_sections', {})
                
                if new_content_sections:
                    # Update the scraped_sections with new content
                    overlap_df.at[idx, 'scraped_sections'] = json.dumps(new_content_sections)
                    updated_count += 1
                    
                    # Also update individual fields if they exist
                    field_mapping = {
                        'Summary': 'job_summary',
                        'MajorDuties': 'major_duties',
                        'QualificationSummary': 'qualification_summary',
                        'Requirements': 'requirements',
                        'Education': 'education',
                        'HowToApply': 'how_to_apply',
                        'Evaluations': 'evaluations',
                        'RequiredDocuments': 'required_documents',
                        'WhatToExpectNext': 'what_to_expect_next',
                        'OtherInformation': 'other_information'
                    }
                    
                    for scraped_field, df_field in field_mapping.items():
                        if scraped_field in new_content_sections and df_field in overlap_df.columns:
                            overlap_df.at[idx, df_field] = new_content_sections[scraped_field]
                    
                    if updated_count % 100 == 0:
                        print(f"  Updated {updated_count} records...")
                        
        except Exception as e:
            print(f"  Error updating {control_number}: {e}")
    
    print(f"📊 Updated {updated_count} records with fresh scraped content")
    
    # Save updated overlap data
    if updated_count > 0:
        storage.save_overlap_samples(overlap_df.to_dict('records'))
        print(f"💾 Saved updated overlap data")
        
        # Test a specific job to verify the update
        print("\n🔍 Testing job 768730000 after update...")
        test_records = overlap_df[overlap_df['control_number'] == '768730000']
        
        for _, test_record in test_records.iterrows():
            if test_record['source_type'] == 'historical' and pd.notna(test_record['scraped_sections']):
                try:
                    scraped_sections = json.loads(test_record['scraped_sections'])
                    if 'QualificationSummary' in scraped_sections:
                        qual_content = scraped_sections['QualificationSummary']
                        print(f"  Updated scraped QualificationSummary: {qual_content[:150]}...")
                except:
                    print("  Could not parse updated scraped_sections")
    
    return updated_count

if __name__ == "__main__":
    updated_count = update_overlap_scraped_content()
    if updated_count > 0:
        print(f"\n✅ Successfully updated {updated_count} records")
        print("🔄 Now regenerate the comparison report to see the updated results")
    else:
        print("\n⚠️ No records were updated")