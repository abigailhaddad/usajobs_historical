#!/usr/bin/env python3
"""
Debug why Duties section is not being parsed correctly
"""

import sys
from pathlib import Path
from bs4 import BeautifulSoup

# Add the pipeline scripts to path
sys.path.append('usajobs_pipeline/scripts')
from scrape_enhanced_job_posting import parse_all_sections, map_sections_to_fields, TARGET_SECTIONS

def debug_html_parsing(html_path):
    """Debug the parsing of a specific HTML file"""
    print(f"=== DEBUGGING: {html_path} ===")
    
    # Read the HTML file
    with open(html_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # Parse with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Check if duties section exists
    duties_div = soup.find('div', {'id': 'duties'})
    print(f"Found duties div: {duties_div is not None}")
    
    if duties_div:
        # Find the h2 header in duties section
        duties_header = duties_div.find('h2')
        if duties_header:
            print(f"Duties header text: '{duties_header.get_text(strip=True)}'")
            print(f"Duties header tag: {duties_header.name}")
        
        # Check what our parser finds
        print("\n--- PARSING RESULTS ---")
        all_sections = parse_all_sections(soup)
        
        # Show all parsed headers
        print(f"Total headers parsed: {len(all_sections)}")
        
        # Look for duty-related headers
        duty_related = []
        for header, data in all_sections.items():
            if any(word in header.lower() for word in ['dut', 'major', 'responsib']):
                duty_related.append((header, data['original_header'], len(data['content'])))
        
        print(f"Duty-related headers found: {len(duty_related)}")
        for header, original, content_len in duty_related:
            print(f"  - '{header}' (original: '{original}') - {content_len} chars")
        
        # Check mapping
        mapped = map_sections_to_fields(all_sections)
        print(f"\nMapped fields: {list(mapped.keys())}")
        print(f"MajorDuties mapped: {'MajorDuties' in mapped}")
        
        if 'MajorDuties' not in mapped:
            print(f"\nTarget variations for MajorDuties: {TARGET_SECTIONS['MajorDuties']}")
            print("Checking exact matches:")
            for variation in TARGET_SECTIONS['MajorDuties']:
                variation_lower = variation.lower().strip()
                if variation_lower in all_sections:
                    print(f"  ✓ Found exact match: '{variation_lower}'")
                else:
                    print(f"  ✗ No exact match: '{variation_lower}'")
        
        return all_sections, mapped
    else:
        print("No duties div found!")
        return {}, {}

if __name__ == "__main__":
    # Test the problematic job
    html_file = "/Users/abigailhaddad/Documents/repos/usajobs_historic/html_cache/837/322/837322200.html"
    debug_html_parsing(html_file)