#!/usr/bin/env python3
"""
Show Side-by-Side Examples

Show actual content side-by-side for jobs where the old scraper was working,
including examples of "losses" to see what we're actually losing.
"""

import sys
import json
from pathlib import Path
from bs4 import BeautifulSoup

# Add the pipeline scripts to path
sys.path.append('usajobs_pipeline/scripts')

def old_parse_all_sections(soup):
    """Original parse_all_sections function (before the fix)"""
    sections = {}
    
    for elem in soup(['script', 'style']):
        elem.decompose()
    
    main = (soup.find('div', class_='usajobs-joa-overview') or 
            soup.find('div', class_='usajobs-joa') or
            soup.find('div', {'id': 'duties'}).parent if soup.find('div', {'id': 'duties'}) else None or
            soup.find('main') or 
            soup.body)
    
    if not main:
        return sections
    
    # OLD LOGIC: Include problematic <strong> tags
    headers = main.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'dt', 'strong'])
    headers.sort(key=lambda h: (h.sourceline or 0, h.sourcepos or 0) if hasattr(h, 'sourceline') else 0)
    
    for i, header in enumerate(headers):
        header_text = header.get_text(strip=True)
        
        if not header_text or len(header_text) > 100:
            continue
            
        if any(header.find_parent() == h for h in headers[:i]):
            continue
            
        content_parts = []
        next_header = None
        for j in range(i + 1, len(headers)):
            if headers[j].find_parent() != header.find_parent():
                next_header = headers[j]
                break
        
        current = header.next_sibling
        while current:
            if next_header and current == next_header:
                break
            if next_header and hasattr(current, 'find_all'):
                if next_header in current.find_all():
                    break
            if hasattr(current, 'name') and current.name:
                if current in headers:
                    break
                text = current.get_text(separator=' ', strip=True)
                if text:
                    content_parts.append(text)
            elif isinstance(current, str):
                text = current.strip()
                if text:
                    content_parts.append(text)
            current = current.next_sibling
        
        content = '\n\n'.join(content_parts).strip()
        if content:
            normalized_header = header_text.lower().strip()
            if normalized_header in sections:
                normalized_header = f"{normalized_header}_{header.name}"
            sections[normalized_header] = {
                'original_header': header_text,
                'content': content,
                'tag': header.name
            }
    
    return sections

def old_map_sections_to_fields(parsed_sections):
    """Original mapping function"""
    TARGET_SECTIONS = {
        'Summary': ['Summary', 'Overview'],
        'MajorDuties': ['Duties', 'Major Duties', 'Key Duties', 'Responsibilities', 'What You Will Do'],
        'QualificationSummary': ['Qualifications', 'Required Qualifications', 'Minimum Qualifications', 'Qualification Summary'],
        'Requirements': ['Conditions of Employment', 'Specialized Experience', 'Experience Requirements'],
        'Education': ['Education', 'Educational Requirements', 'Education Requirements', 'If you are relying on your education to meet qualification requirements:'],
        'HowToApply': ['How to Apply', 'Application Process', 'Application Instructions'],
        'Evaluations': ['Evaluation', 'How You Will Be Evaluated', 'Rating and Ranking'],
        'Benefits': ['Benefits', 'What We Offer', 'Compensation'],
        'RequiredDocuments': ['Required Documents', 'Documents Required'],
        'WhatToExpectNext': ['What to Expect Next', 'Next Steps'],
        'OtherInformation': ['Additional Information', 'Other Information']
    }
    
    mapped = {}
    
    for field_name, header_variations in TARGET_SECTIONS.items():
        best_match = None
        best_content = None
        
        for variation in header_variations:
            variation_lower = variation.lower().strip()
            
            if variation_lower in parsed_sections:
                best_match = variation_lower
                best_content = parsed_sections[variation_lower]['content']
                break
                
            for section_header, section_data in parsed_sections.items():
                if variation_lower in section_header or section_header in variation_lower:
                    if best_match is None or len(section_header) < len(best_match):
                        best_match = section_header
                        best_content = section_data['content']
        
        if best_content:
            mapped[field_name] = best_content
            
    return mapped

def show_side_by_side_comparison(job_ids):
    """Show detailed side-by-side content comparison"""
    
    TARGET_FIELDS = [
        'Summary', 'MajorDuties', 'QualificationSummary', 'Requirements', 
        'Education', 'HowToApply', 'Evaluations', 'Benefits', 
        'RequiredDocuments', 'WhatToExpectNext', 'OtherInformation'
    ]
    
    for job_id in job_ids:
        html_path = Path(f"html_cache/{job_id[:3]}/{job_id[3:6]}/{job_id}.html")
        
        if not html_path.exists():
            print(f"❌ HTML file not found for {job_id}")
            continue
        
        try:
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # OLD SCRAPER
            old_sections = old_parse_all_sections(soup)
            old_mapped = old_map_sections_to_fields(old_sections)
            
            # NEW SCRAPER
            from scrape_enhanced_job_posting import parse_all_sections, map_sections_to_fields
            new_sections = parse_all_sections(soup)
            new_mapped = map_sections_to_fields(new_sections)
            
            print(f"\n{'='*100}")
            print(f"🔍 JOB: {job_id}")
            print(f"{'='*100}")
            
            old_field_count = sum(1 for field in TARGET_FIELDS if old_mapped.get(field, ''))
            new_field_count = sum(1 for field in TARGET_FIELDS if new_mapped.get(field, ''))
            
            print(f"📊 OLD: {old_field_count}/11 fields | NEW: {new_field_count}/11 fields")
            
            for field in TARGET_FIELDS:
                old_content = old_mapped.get(field, '')
                new_content = new_mapped.get(field, '')
                
                old_has = bool(old_content)
                new_has = bool(new_content)
                
                # Show status
                if old_has and new_has:
                    if len(new_content) > len(old_content) * 1.5:
                        status = "📈 IMPROVED"
                    elif len(new_content) < len(old_content) * 0.5:
                        status = "📉 REDUCED"
                    else:
                        status = "✅ MAINTAINED"
                elif not old_has and new_has:
                    status = "🆕 GAINED"
                elif old_has and not new_has:
                    status = "❌ LOST"
                else:
                    status = "➖ MISSING"
                
                print(f"\n🔹 {field} {status}")
                print("-" * 80)
                
                # Show content side by side
                print("BEFORE (Old Scraper):")
                if old_content:
                    # Show first 300 chars
                    preview = old_content[:300] + "..." if len(old_content) > 300 else old_content
                    print(f"  📝 {len(old_content)} chars: {repr(preview)}")
                else:
                    print("  ❌ No content extracted")
                
                print("\nAFTER (New Scraper):")
                if new_content:
                    # Show first 300 chars
                    preview = new_content[:300] + "..." if len(new_content) > 300 else new_content
                    print(f"  📝 {len(new_content)} chars: {repr(preview)}")
                else:
                    print("  ❌ No content extracted")
                
                # For losses or significant changes, show more detail
                if (old_has and not new_has) or (old_has and new_has and len(new_content) < len(old_content) * 0.5):
                    print(f"\n⚠️  ATTENTION: {field} content changed significantly")
                    print(f"   OLD FULL CONTENT: {repr(old_content)}")
                    if new_content:
                        print(f"   NEW FULL CONTENT: {repr(new_content)}")
            
        except Exception as e:
            print(f"❌ Error processing {job_id}: {e}")

def find_example_jobs():
    """Find good example jobs to show"""
    
    # Load the comprehensive comparison results
    try:
        with open('comprehensive_comparison_results.json', 'r') as f:
            results = json.load(f)
    except FileNotFoundError:
        print("❌ Run comprehensive_before_after_test.py first")
        return []
    
    TARGET_FIELDS = [
        'Summary', 'MajorDuties', 'QualificationSummary', 'Requirements', 
        'Education', 'HowToApply', 'Evaluations', 'Benefits', 
        'RequiredDocuments', 'WhatToExpectNext', 'OtherInformation'
    ]
    
    examples = {
        'high_performing_maintained': [],
        'had_losses': [],
        'big_improvements': []
    }
    
    for result in results:
        if 'error' in result:
            continue
        
        old_field_count = sum(1 for field in TARGET_FIELDS 
                             if result['field_comparison'][field]['old_has_content'])
        
        lost_fields = sum(1 for field in TARGET_FIELDS 
                         if result['field_comparison'][field]['lost_content'])
        
        gained_fields = sum(1 for field in TARGET_FIELDS 
                           if result['field_comparison'][field]['gained_content'])
        
        # High performing jobs that maintained quality
        if old_field_count >= 8 and lost_fields == 0:
            examples['high_performing_maintained'].append(result['job_id'])
        
        # Jobs that had losses
        if lost_fields > 0:
            examples['had_losses'].append(result['job_id'])
        
        # Jobs with big improvements
        if gained_fields >= 4:
            examples['big_improvements'].append(result['job_id'])
    
    return examples

def main():
    print("👀 Side-by-Side Content Comparison")
    print("=" * 50)
    
    examples = find_example_jobs()
    
    if not examples:
        return
    
    print(f"Found examples:")
    print(f"  - High performing maintained: {len(examples['high_performing_maintained'])}")
    print(f"  - Had losses: {len(examples['had_losses'])}")  
    print(f"  - Big improvements: {len(examples['big_improvements'])}")
    
    # Show examples of each type
    example_jobs = []
    
    # 2 high performing jobs that maintained quality
    example_jobs.extend(examples['high_performing_maintained'][:2])
    
    # 2 jobs that had losses (to see what we lost)
    example_jobs.extend(examples['had_losses'][:2])
    
    # 1 job with big improvements
    example_jobs.extend(examples['big_improvements'][:1])
    
    print(f"\nShowing detailed comparison for {len(example_jobs)} example jobs...")
    
    show_side_by_side_comparison(example_jobs)

if __name__ == "__main__":
    main()