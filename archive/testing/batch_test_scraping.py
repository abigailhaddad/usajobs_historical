#!/usr/bin/env python3
"""
Batch test the optimized scraper on multiple current jobs
"""

import json
import sys
import os
import time
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scrape_optimized_content import scrape_job_content


def extract_control_numbers_from_api_data(json_file, limit=12):
    """Extract control numbers from current jobs API data"""
    
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    control_numbers = []
    items = data['SearchResult']['SearchResultItems'][:limit]
    
    for item in items:
        # Extract control number from PositionID or URL
        position_id = item['MatchedObjectDescriptor']['PositionID']
        position_uri = item['MatchedObjectDescriptor']['PositionURI']
        
        # Control number is usually the last part of the URL
        control_number = position_uri.split('/')[-1]
        
        control_numbers.append({
            'control_number': control_number,
            'position_id': position_id,
            'title': item['MatchedObjectDescriptor']['PositionTitle']
        })
    
    return control_numbers


def batch_test_scraping(control_numbers):
    """Test scraping on multiple jobs and analyze results"""
    
    results = []
    success_count = 0
    total_sections = 0
    total_content_chars = 0
    section_counts = {}
    
    print("BATCH SCRAPING TEST")
    print("=" * 80)
    print(f"Testing {len(control_numbers)} jobs\n")
    
    for i, job_info in enumerate(control_numbers, 1):
        control_number = job_info['control_number']
        title = job_info['title']
        
        print(f"\n{i}. Job {control_number}: {title[:60]}...")
        print("-" * 70)
        
        # Scrape the job
        result = scrape_job_content(control_number)
        
        if result['status'] == 'success':
            success_count += 1
            sections_found = result['stats']['sectionsFound']
            content_length = result['stats']['totalContentLength']
            
            total_sections += sections_found
            total_content_chars += content_length
            
            print(f"  ✓ Success: {sections_found} sections, {content_length:,} chars")
            
            # Track which sections were found
            for section in result['content'].keys():
                section_counts[section] = section_counts.get(section, 0) + 1
            
            # Show sections found
            print("  Sections found:")
            for section, content in result['content'].items():
                print(f"    - {section}: {len(content)} chars")
        
        else:
            print(f"  ✗ Error: {result['error']}")
        
        results.append(result)
        
        # Brief delay between requests
        time.sleep(1)
    
    # Summary statistics
    print("\n\nSUMMARY STATISTICS")
    print("=" * 80)
    print(f"Total jobs tested: {len(control_numbers)}")
    print(f"Successful scrapes: {success_count} ({success_count/len(control_numbers)*100:.1f}%)")
    
    if success_count > 0:
        avg_sections = total_sections / success_count
        avg_content = total_content_chars / success_count
        
        print(f"\nAverage per successful scrape:")
        print(f"  - Sections found: {avg_sections:.1f}")
        print(f"  - Content length: {avg_content:,.0f} chars")
        
        print(f"\nSection extraction rates:")
        for section, count in sorted(section_counts.items(), key=lambda x: x[1], reverse=True):
            rate = count / success_count * 100
            print(f"  - {section}: {count}/{success_count} ({rate:.1f}%)")
    
    # Save detailed results
    output_file = f"batch_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump({
            'test_date': datetime.now().isoformat(),
            'jobs_tested': control_numbers,
            'results': results,
            'summary': {
                'total_tested': len(control_numbers),
                'successful': success_count,
                'success_rate': success_count/len(control_numbers) if control_numbers else 0,
                'avg_sections_per_job': total_sections/success_count if success_count > 0 else 0,
                'avg_content_length': total_content_chars/success_count if success_count > 0 else 0,
                'section_extraction_rates': section_counts
            }
        }, f, indent=2)
    
    print(f"\n\nDetailed results saved to: {output_file}")
    
    return results


def main():
    # Find the most recent current jobs file
    data_dir = "../../data"
    current_jobs_files = [f for f in os.listdir(data_dir) if f.startswith("current_jobs_") and f.endswith(".json")]
    
    if not current_jobs_files:
        print("No current jobs data files found!")
        return
    
    # Use the most recent file
    latest_file = sorted(current_jobs_files)[-1]
    json_file = os.path.join(data_dir, latest_file)
    
    print(f"Using data file: {latest_file}")
    
    # Extract control numbers - skip first 12 to get different jobs
    all_control_numbers = extract_control_numbers_from_api_data(json_file, limit=24)
    control_numbers = all_control_numbers[12:]  # Get jobs 13-24
    
    # Run batch test
    batch_test_scraping(control_numbers)


if __name__ == "__main__":
    main()