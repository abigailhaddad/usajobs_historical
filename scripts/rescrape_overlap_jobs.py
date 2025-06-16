#!/usr/bin/env python3
"""
Re-scrape all overlap jobs to update content with improved parser
"""

import sys
sys.path.append('src')

from parquet_storage import ParquetJobStorage
from scrape_enhanced_job_posting import scrape_enhanced_job_posting
import pandas as pd
import json
from tqdm import tqdm

def rescrape_overlap_jobs():
    """Re-scrape all overlap jobs to update content with fixed parser"""
    
    # Load overlap data
    storage = ParquetJobStorage('data')
    overlap_df = storage.load_overlap_samples()
    
    if overlap_df.empty:
        print("❌ No overlap data found")
        return
    
    print(f"📊 Loaded {len(overlap_df)} overlap records")
    
    # Get unique control numbers from overlap data
    unique_controls = overlap_df['control_number'].unique()
    print(f"📊 Found {len(unique_controls)} unique control numbers to re-scrape")
    
    # Re-scrape each job with the improved parser
    updated_jobs = []
    failed_jobs = []
    
    for control_number in tqdm(unique_controls, desc="Re-scraping jobs"):
        try:
            # Force refresh to get new parsing with fixed logic
            scraped_data = scrape_enhanced_job_posting(control_number, force_refresh=False)
            
            if scraped_data.get('status') == 'success':
                # Update each record that has this control number
                mask = overlap_df['control_number'] == control_number
                matching_records = overlap_df[mask].copy()
                
                for idx, row in matching_records.iterrows():
                    # Update scraped content fields if they exist in the scraped data
                    content_sections = scraped_data.get('content_sections', {})
                    
                    # Map scraped fields to dataframe columns
                    field_mapping = {
                        'MajorDuties': 'major_duties',
                        'QualificationSummary': 'qualification_summary', 
                        'Requirements': 'requirements',
                        'Education': 'education',
                        'Benefits': 'benefits',
                        'HowToApply': 'how_to_apply'
                    }
                    
                    updated_row = row.copy()
                    content_updated = False
                    
                    for scraped_field, df_field in field_mapping.items():
                        if scraped_field in content_sections and content_sections[scraped_field]:
                            old_content = str(updated_row.get(df_field, ''))
                            new_content = content_sections[scraped_field]
                            
                            # Only update if content is different and non-empty
                            if new_content and new_content != old_content:
                                updated_row[df_field] = new_content
                                content_updated = True
                                print(f"✅ Updated {df_field} for job {control_number} ({len(new_content)} chars)")
                    
                    # Always update scraped_sections with fresh data if we have content_sections
                    if content_sections:
                        updated_row['scraped_sections'] = json.dumps(content_sections)
                        content_updated = True
                        print(f"✅ Updated scraped_sections for job {control_number}")
                    
                    if content_updated:
                        # Update the rationalization timestamp
                        from datetime import datetime
                        updated_row['rationalization_date'] = datetime.now().isoformat()
                        
                        # Update data sources to indicate re-scraping
                        try:
                            data_sources = json.loads(updated_row.get('data_sources', '[]')) if isinstance(updated_row.get('data_sources'), str) else updated_row.get('data_sources', [])
                            if 'rescraping_v3' not in data_sources:
                                data_sources.append('rescraping_v3')
                                updated_row['data_sources'] = json.dumps(data_sources)
                        except:
                            updated_row['data_sources'] = '["rescraping_v3"]'
                    
                    updated_jobs.append(updated_row)
                    
            else:
                failed_jobs.append(control_number)
                print(f"⚠️ Failed to scrape {control_number}: {scraped_data.get('error', 'Unknown error')}")
                
        except Exception as e:
            failed_jobs.append(control_number)
            print(f"❌ Error processing {control_number}: {e}")
    
    print(f"\n📊 Re-scraping Summary:")
    print(f"   ✅ Successfully processed: {len(unique_controls) - len(failed_jobs)} jobs")
    print(f"   ❌ Failed: {len(failed_jobs)} jobs")
    
    if updated_jobs:
        # Create updated overlap dataframe
        updated_overlap_df = pd.DataFrame(updated_jobs)
        
        # Save updated overlap data - convert DataFrame to list of dicts
        storage.save_overlap_samples(updated_overlap_df.to_dict('records'))
        print(f"💾 Saved {len(updated_overlap_df)} updated overlap records")
        
        return True
    else:
        print("⚠️ No content was updated")
        return False

if __name__ == "__main__":
    success = rescrape_overlap_jobs()
    if success:
        print("\n🚀 Re-scraping complete! Ready to regenerate mismatch analysis.")
    else:
        print("\n⚠️ Re-scraping had issues. Check the logs above.")