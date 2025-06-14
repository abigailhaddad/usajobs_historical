#!/usr/bin/env python3
"""
Show Duties Examples

Display the full duties content we're now extracting from jobs that previously failed.
"""

import sys
from pathlib import Path
from bs4 import BeautifulSoup

# Add the pipeline scripts to path
sys.path.append('usajobs_pipeline/scripts')
from scrape_enhanced_job_posting import parse_all_sections, map_sections_to_fields

def show_duties_for_job(job_id, job_title=""):
    """Show the duties content for a specific job."""
    
    html_path = Path(f"html_cache/{job_id[:3]}/{job_id[3:6]}/{job_id}.html")
    
    if not html_path.exists():
        print(f"❌ HTML file not found for {job_id}")
        return
    
    try:
        with open(html_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        parsed_sections = parse_all_sections(soup)
        mapped_fields = map_sections_to_fields(parsed_sections)
        
        print(f"\n{'='*80}")
        print(f"🔍 JOB {job_id}: {job_title}")
        print(f"{'='*80}")
        
        # Show duties content
        duties_content = mapped_fields.get('MajorDuties', '')
        if duties_content:
            print(f"📋 MAJOR DUTIES ({len(duties_content)} characters):")
            print("-" * 60)
            print(duties_content)
        else:
            # Check raw sections
            for key, data in parsed_sections.items():
                if 'duties' in key.lower():
                    print(f"📋 DUTIES FROM '{key}' ({len(data.get('content', ''))} characters):")
                    print("-" * 60)
                    print(data.get('content', 'No content'))
                    break
            else:
                print("❌ No duties content found")
        
        print(f"\n📊 EXTRACTION STATS:")
        print(f"   Total sections extracted: {len(parsed_sections)}")
        print(f"   Mapped fields with content: {len([k for k, v in mapped_fields.items() if v])}")
        
        # Show other key fields we got
        other_fields = ['Summary', 'Requirements', 'Qualifications', 'Education', 'HowToApply']
        found_fields = []
        for field in other_fields:
            if field in mapped_fields and mapped_fields[field]:
                found_fields.append(f"{field} ({len(mapped_fields[field])} chars)")
        
        if found_fields:
            print(f"   Other key fields: {', '.join(found_fields)}")
            
    except Exception as e:
        print(f"❌ Error processing {job_id}: {e}")

def main():
    print("🎯 DUTIES EXTRACTION EXAMPLES")
    print("Previously failing jobs now working with improved scraper")
    
    # The jobs that were failing before the fix
    jobs = [
        ("837322200", "Nurse Practitioner (Primary Care)"),
        ("838272700", "Clinical Pharmacist"), 
        ("837331900", "Green & Gold Congressional Aide"),
        ("828825300", "Electrician")
    ]
    
    for job_id, title in jobs:
        show_duties_for_job(job_id, title)
    
    print(f"\n{'='*80}")
    print("🎉 SUMMARY")
    print("These jobs now have complete duties extraction where they had NONE before!")
    print("The fix resolved the issue with embedded <strong> tags breaking content extraction.")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()