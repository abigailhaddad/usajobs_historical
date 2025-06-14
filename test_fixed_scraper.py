#!/usr/bin/env python3
"""
Test Fixed Scraper

Test the improved parse_all_sections function on the jobs that previously
failed MajorDuties extraction to verify the fix works.
"""

import sys
import json
from pathlib import Path
from bs4 import BeautifulSoup

# Add the pipeline scripts to path
sys.path.append('usajobs_pipeline/scripts')
from scrape_enhanced_job_posting import parse_all_sections, map_sections_to_fields

def test_specific_jobs():
    """Test the specific jobs that were failing MajorDuties extraction."""
    
    # Jobs that were identified as failing in the investigation
    failing_jobs = [
        "837322200",  # Nurse Practitioner
        "838272700",  # Clinical Pharmacist  
        "837331900",  # Congressional Aide
        "828825300"   # Job mentioned in investigation
    ]
    
    results = []
    
    for job_id in failing_jobs:
        html_path = Path(f"html_cache/{job_id[:3]}/{job_id[3:6]}/{job_id}.html")
        
        if not html_path.exists():
            print(f"❌ HTML file not found for {job_id}")
            continue
            
        try:
            print(f"🔍 Testing job {job_id}...")
            
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Test the fixed parsing
            parsed_sections = parse_all_sections(soup)
            mapped_fields = map_sections_to_fields(parsed_sections)
            
            # Check for duties content
            duties_found = False
            duties_content = ""
            duties_source = ""
            
            # Check all possible duties fields
            duties_fields = ['MajorDuties', 'Duties', 'duties', 'major duties']
            for field in duties_fields:
                if field in mapped_fields and mapped_fields[field]:
                    duties_found = True
                    duties_content = mapped_fields[field][:200] + "..." if len(mapped_fields[field]) > 200 else mapped_fields[field]
                    duties_source = field
                    break
            
            # Also check raw parsed sections
            if not duties_found:
                for key, data in parsed_sections.items():
                    if 'duties' in key.lower() and data.get('content'):
                        duties_found = True
                        duties_content = data['content'][:200] + "..." if len(data['content']) > 200 else data['content']
                        duties_source = f"raw:{key}"
                        break
            
            result = {
                'job_id': job_id,
                'duties_found': duties_found,
                'duties_source': duties_source,
                'duties_content_preview': duties_content,
                'total_sections': len(parsed_sections),
                'mapped_fields': len([k for k, v in mapped_fields.items() if v]),
                'all_sections': list(parsed_sections.keys())
            }
            
            results.append(result)
            
            status = "✅ SUCCESS" if duties_found else "❌ STILL FAILING"
            print(f"   {status} - Duties found: {duties_found}")
            if duties_found:
                print(f"   Source: {duties_source}")
                print(f"   Preview: {duties_content[:100]}...")
            
        except Exception as e:
            print(f"❌ Error testing {job_id}: {e}")
            results.append({
                'job_id': job_id,
                'error': str(e)
            })
    
    return results

def test_random_sample():
    """Test a random sample to see overall improvement."""
    import random
    
    html_cache = Path("html_cache")
    html_files = list(html_cache.rglob("*.html"))
    
    if len(html_files) < 20:
        print("❌ Not enough HTML files for random sampling")
        return []
    
    # Test 20 random files
    sample_files = random.sample(html_files, 20)
    
    results = {
        'duties_success': 0,
        'total_tested': 0,
        'errors': 0
    }
    
    print(f"\n🎲 Testing random sample of {len(sample_files)} files...")
    
    for html_file in sample_files:
        try:
            job_id = html_file.stem
            
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            soup = BeautifulSoup(html_content, 'html.parser')
            parsed_sections = parse_all_sections(soup)
            mapped_fields = map_sections_to_fields(parsed_sections)
            
            # Check for duties
            duties_found = any(
                field in mapped_fields and mapped_fields[field]
                for field in ['MajorDuties', 'Duties', 'duties']
            ) or any(
                'duties' in key.lower() and data.get('content')
                for key, data in parsed_sections.items()
            )
            
            if duties_found:
                results['duties_success'] += 1
            
            results['total_tested'] += 1
            
        except Exception as e:
            results['errors'] += 1
            print(f"❌ Error with {html_file.name}: {e}")
    
    success_rate = results['duties_success'] / results['total_tested'] * 100 if results['total_tested'] > 0 else 0
    print(f"📊 Random sample results:")
    print(f"   Duties extraction success: {results['duties_success']}/{results['total_tested']} ({success_rate:.1f}%)")
    print(f"   Errors: {results['errors']}")
    
    return results

def main():
    print("🧪 Testing Fixed Scraper")
    print("=" * 50)
    
    # Test specific failing jobs
    print("1️⃣ Testing previously failing jobs...")
    failing_results = test_specific_jobs()
    
    # Test random sample
    print("\n2️⃣ Testing random sample...")
    sample_results = test_random_sample()
    
    # Summary
    print("\n📋 SUMMARY")
    print("-" * 30)
    
    successful_fixes = sum(1 for r in failing_results if r.get('duties_found', False))
    total_tested = len([r for r in failing_results if 'error' not in r])
    
    if total_tested > 0:
        fix_rate = successful_fixes / total_tested * 100
        print(f"🎯 Previously failing jobs fixed: {successful_fixes}/{total_tested} ({fix_rate:.1f}%)")
    
    if sample_results.get('total_tested', 0) > 0:
        sample_rate = sample_results['duties_success'] / sample_results['total_tested'] * 100
        print(f"📊 Random sample success rate: {sample_rate:.1f}%")
        print(f"📈 Expected improvement from ~70% to {sample_rate:.1f}%")
    
    # Save detailed results
    with open('fixed_scraper_test_results.json', 'w') as f:
        json.dump({
            'failing_jobs_results': failing_results,
            'random_sample_results': sample_results,
            'summary': {
                'fixes_successful': successful_fixes,
                'fixes_total': total_tested,
                'sample_success_rate': sample_results.get('duties_success', 0) / sample_results.get('total_tested', 1) * 100
            }
        }, f, indent=2)
    
    print(f"\n💾 Detailed results saved to: fixed_scraper_test_results.json")

if __name__ == "__main__":
    main()