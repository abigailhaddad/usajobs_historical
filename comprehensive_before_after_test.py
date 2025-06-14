#!/usr/bin/env python3
"""
Comprehensive Before/After Test

Compare the old vs new scraper on a large sample to see:
1. What fields we were extracting before
2. How the new scraper performs on the same jobs
3. Generate an HTML report showing the comparison
"""

import sys
import json
import random
from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime
import re

# Add the pipeline scripts to path
sys.path.append('usajobs_pipeline/scripts')

# Target fields that we extract
TARGET_FIELDS = [
    'Summary', 'MajorDuties', 'QualificationSummary', 'Requirements', 
    'Education', 'HowToApply', 'Evaluations', 'Benefits', 
    'RequiredDocuments', 'WhatToExpectNext', 'OtherInformation'
]

def old_parse_all_sections(soup):
    """
    Original parse_all_sections function (before the fix)
    This includes <strong> tags as headers which caused the issue
    """
    sections = {}
    
    # Remove script, style elements first (but keep nav as it may contain content)
    for elem in soup(['script', 'style']):
        elem.decompose()
    
    # Focus on job announcement content area - check multiple possible containers
    main = (soup.find('div', class_='usajobs-joa-overview') or 
            soup.find('div', class_='usajobs-joa') or
            soup.find('div', {'id': 'duties'}).parent if soup.find('div', {'id': 'duties'}) else None or
            soup.find('main') or 
            soup.body)
    
    if not main:
        return sections
    
    # OLD LOGIC: Find all potential headers INCLUDING problematic <strong> tags
    headers = main.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'dt', 'strong'])
    
    # Sort headers by their position in the document
    headers.sort(key=lambda h: (h.sourceline or 0, h.sourcepos or 0) if hasattr(h, 'sourceline') else 0)
    
    for i, header in enumerate(headers):
        header_text = header.get_text(strip=True)
        
        # Skip very long "headers" (probably not actual headers)
        if not header_text or len(header_text) > 100:
            continue
            
        # Skip if this header is inside another header's content
        if any(header.find_parent() == h for h in headers[:i]):
            continue
            
        # Extract content until the next header at same or higher level
        content_parts = []
        
        # Get the next header at same or higher level
        next_header = None
        for j in range(i + 1, len(headers)):
            # Check if it's at same level (sibling) or higher level
            if headers[j].find_parent() != header.find_parent():
                next_header = headers[j]
                break
        
        # Collect all elements between this header and the next
        current = header.next_sibling
        while current:
            # Stop if we've reached the next header
            if next_header and current == next_header:
                break
                
            # Stop if current contains the next header
            if next_header and hasattr(current, 'find_all'):
                if next_header in current.find_all():
                    break
                
            # Extract text from elements
            if hasattr(current, 'name') and current.name:
                # Skip if this is another header
                if current in headers:
                    break
                    
                text = current.get_text(separator=' ', strip=True)
                if text:
                    content_parts.append(text)
            elif isinstance(current, str):
                # Handle text nodes
                text = current.strip()
                if text:
                    content_parts.append(text)
                    
            current = current.next_sibling
        
        # Store the section
        content = '\n\n'.join(content_parts).strip()
        if content:
            # Normalize header text for matching
            normalized_header = header_text.lower().strip()
            # Handle duplicates by appending tag type
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
    # Same TARGET_SECTIONS mapping
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
    
    # For each target field, find the best matching section
    for field_name, header_variations in TARGET_SECTIONS.items():
        best_match = None
        best_content = None
        
        # Try each variation
        for variation in header_variations:
            variation_lower = variation.lower().strip()
            
            # Look for exact matches first
            if variation_lower in parsed_sections:
                best_match = variation_lower
                best_content = parsed_sections[variation_lower]['content']
                break
                
            # Then try partial matches
            for section_header, section_data in parsed_sections.items():
                if variation_lower in section_header or section_header in variation_lower:
                    # Prefer shorter headers (more specific)
                    if best_match is None or len(section_header) < len(best_match):
                        best_match = section_header
                        best_content = section_data['content']
        
        if best_content:
            mapped[field_name] = best_content
            
    return mapped

def process_job_with_both_scrapers(html_path):
    """Process a job with both old and new scrapers"""
    try:
        job_id = html_path.stem
        
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
        
        # Compare results
        comparison = {
            'job_id': job_id,
            'old_total_sections': len(old_sections),
            'new_total_sections': len(new_sections),
            'old_mapped_fields': len([k for k, v in old_mapped.items() if v]),
            'new_mapped_fields': len([k for k, v in new_mapped.items() if v]),
            'field_comparison': {}
        }
        
        for field in TARGET_FIELDS:
            old_content = old_mapped.get(field, '')
            new_content = new_mapped.get(field, '')
            
            comparison['field_comparison'][field] = {
                'old_has_content': bool(old_content),
                'new_has_content': bool(new_content),
                'old_length': len(old_content) if old_content else 0,
                'new_length': len(new_content) if new_content else 0,
                'old_preview': old_content[:100] + '...' if len(old_content) > 100 else old_content,
                'new_preview': new_content[:100] + '...' if len(new_content) > 100 else new_content,
                'improved': len(new_content) > len(old_content),
                'gained_content': bool(new_content) and not bool(old_content),
                'lost_content': bool(old_content) and not bool(new_content)
            }
        
        return comparison
        
    except Exception as e:
        return {
            'job_id': html_path.stem,
            'error': str(e)
        }

def generate_html_report(results, output_file='before_after_comparison.html'):
    """Generate an HTML report comparing old vs new scraper"""
    
    # Calculate summary stats
    total_jobs = len([r for r in results if 'error' not in r])
    total_errors = len([r for r in results if 'error' in r])
    
    field_stats = {}
    for field in TARGET_FIELDS:
        old_success = sum(1 for r in results if 'error' not in r and r['field_comparison'][field]['old_has_content'])
        new_success = sum(1 for r in results if 'error' not in r and r['field_comparison'][field]['new_has_content'])
        gained = sum(1 for r in results if 'error' not in r and r['field_comparison'][field]['gained_content'])
        lost = sum(1 for r in results if 'error' not in r and r['field_comparison'][field]['lost_content'])
        
        field_stats[field] = {
            'old_success': old_success,
            'new_success': new_success,
            'old_percentage': old_success / total_jobs * 100 if total_jobs > 0 else 0,
            'new_percentage': new_success / total_jobs * 100 if total_jobs > 0 else 0,
            'gained': gained,
            'lost': lost
        }
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>USAJobs Scraper: Before vs After Comparison</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
            .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; }}
            h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
            h2 {{ color: #34495e; margin-top: 30px; }}
            .summary {{ background: #ecf0f1; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
            .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 30px; }}
            .field-card {{ background: white; border: 1px solid #ddd; border-radius: 5px; padding: 15px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .field-header {{ font-weight: bold; font-size: 16px; margin-bottom: 10px; color: #2c3e50; }}
            .metric {{ display: flex; justify-content: space-between; margin-bottom: 8px; }}
            .metric-label {{ color: #7f8c8d; }}
            .metric-value {{ font-weight: bold; }}
            .improvement {{ color: #27ae60; }}
            .decline {{ color: #e74c3c; }}
            .neutral {{ color: #7f8c8d; }}
            .job-details {{ margin-top: 30px; }}
            .job-card {{ background: #f8f9fa; border: 1px solid #e9ecef; margin-bottom: 20px; border-radius: 5px; }}
            .job-header {{ background: #343a40; color: white; padding: 10px 15px; font-weight: bold; }}
            .job-content {{ padding: 15px; }}
            .field-comparison {{ margin-bottom: 15px; }}
            .field-name {{ font-weight: bold; color: #495057; margin-bottom: 5px; }}
            .content-comparison {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; }}
            .old-content, .new-content {{ padding: 10px; border-radius: 3px; }}
            .old-content {{ background: #ffeaa7; border-left: 4px solid #fdcb6e; }}
            .new-content {{ background: #a8e6cf; border-left: 4px solid #00b894; }}
            .no-content {{ background: #ddd; color: #666; font-style: italic; }}
            .gained {{ background: #d4edda !important; border-left-color: #28a745 !important; }}
            .lost {{ background: #f8d7da !important; border-left-color: #dc3545 !important; }}
            .content-preview {{ font-size: 14px; line-height: 1.4; }}
            .content-stats {{ font-size: 12px; color: #666; margin-top: 5px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🔍 USAJobs Scraper: Before vs After Comparison</h1>
            
            <div class="summary">
                <h2>📊 Test Summary</h2>
                <p><strong>Total Jobs Tested:</strong> {total_jobs}</p>
                <p><strong>Errors:</strong> {total_errors}</p>
                <p><strong>Test Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p><strong>Purpose:</strong> Compare old scraper (with <code>&lt;strong&gt;</code> tag issue) vs new improved scraper</p>
            </div>
            
            <h2>📈 Field Extraction Performance</h2>
            <div class="stats-grid">
    """
    
    for field, stats in field_stats.items():
        improvement_class = "improvement" if stats['new_success'] > stats['old_success'] else "decline" if stats['new_success'] < stats['old_success'] else "neutral"
        change_text = f"+{stats['new_success'] - stats['old_success']}" if stats['new_success'] > stats['old_success'] else str(stats['new_success'] - stats['old_success']) if stats['new_success'] < stats['old_success'] else "No change"
        
        html_content += f"""
                <div class="field-card">
                    <div class="field-header">{field}</div>
                    <div class="metric">
                        <span class="metric-label">Old Success Rate:</span>
                        <span class="metric-value">{stats['old_success']}/{total_jobs} ({stats['old_percentage']:.1f}%)</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">New Success Rate:</span>
                        <span class="metric-value">{stats['new_success']}/{total_jobs} ({stats['new_percentage']:.1f}%)</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Change:</span>
                        <span class="metric-value {improvement_class}">{change_text}</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Content Gained:</span>
                        <span class="metric-value improvement">{stats['gained']} jobs</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Content Lost:</span>
                        <span class="metric-value decline">{stats['lost']} jobs</span>
                    </div>
                </div>
        """
    
    html_content += """
            </div>
            
            <h2>🔍 Job-by-Job Details</h2>
            <div class="job-details">
    """
    
    # Show details for jobs with interesting changes
    interesting_jobs = []
    for result in results:
        if 'error' in result:
            continue
            
        # Look for jobs with significant improvements or issues
        gained_fields = sum(1 for field in TARGET_FIELDS if result['field_comparison'][field]['gained_content'])
        lost_fields = sum(1 for field in TARGET_FIELDS if result['field_comparison'][field]['lost_content'])
        
        if gained_fields > 0 or lost_fields > 0:
            interesting_jobs.append((result, gained_fields, lost_fields))
    
    # Sort by most improvements first
    interesting_jobs.sort(key=lambda x: (x[1], -x[2]), reverse=True)
    
    # Show top 20 most interesting jobs
    for result, gained, lost in interesting_jobs[:20]:
        job_id = result['job_id']
        
        html_content += f"""
                <div class="job-card">
                    <div class="job-header">
                        Job {job_id} - Gained: {gained} fields, Lost: {lost} fields
                    </div>
                    <div class="job-content">
        """
        
        for field in TARGET_FIELDS:
            field_data = result['field_comparison'][field]
            if field_data['gained_content'] or field_data['lost_content'] or field_data['improved']:
                
                old_class = "no-content" if not field_data['old_has_content'] else "old-content"
                new_class = "no-content" if not field_data['new_has_content'] else "new-content"
                
                if field_data['gained_content']:
                    new_class += " gained"
                elif field_data['lost_content']:
                    old_class += " lost"
                
                html_content += f"""
                        <div class="field-comparison">
                            <div class="field-name">{field}</div>
                            <div class="content-comparison">
                                <div class="{old_class}">
                                    <strong>BEFORE:</strong>
                                    <div class="content-preview">
                                        {field_data['old_preview'] if field_data['old_has_content'] else 'No content extracted'}
                                    </div>
                                    <div class="content-stats">{field_data['old_length']} characters</div>
                                </div>
                                <div class="{new_class}">
                                    <strong>AFTER:</strong>
                                    <div class="content-preview">
                                        {field_data['new_preview'] if field_data['new_has_content'] else 'No content extracted'}
                                    </div>
                                    <div class="content-stats">{field_data['new_length']} characters</div>
                                </div>
                            </div>
                        </div>
                """
        
        html_content += """
                    </div>
                </div>
        """
    
    html_content += """
            </div>
        </div>
    </body>
    </html>
    """
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"📄 HTML report generated: {output_file}")

def main():
    print("🔬 Comprehensive Before/After Scraper Test")
    print("=" * 60)
    
    # Find all HTML files
    html_cache = Path("html_cache")
    html_files = list(html_cache.rglob("*.html"))
    
    if len(html_files) < 50:
        print("❌ Not enough HTML files for comprehensive testing")
        return
    
    # Sample a large number of files (500-1000)
    sample_size = min(500, len(html_files))
    sampled_files = random.sample(html_files, sample_size)
    
    print(f"🔍 Testing {sample_size} jobs with both old and new scrapers...")
    
    results = []
    
    for i, html_file in enumerate(sampled_files, 1):
        if i % 50 == 0:
            print(f"   Processed {i}/{sample_size} jobs...")
        
        result = process_job_with_both_scrapers(html_file)
        results.append(result)
    
    # Calculate overall stats
    successful_results = [r for r in results if 'error' not in r]
    
    print(f"\n📊 OVERALL RESULTS")
    print("-" * 40)
    print(f"✅ Successfully processed: {len(successful_results)}/{sample_size}")
    print(f"❌ Errors: {len(results) - len(successful_results)}")
    
    # Field-by-field summary
    print(f"\n📋 FIELD EXTRACTION SUMMARY")
    print("-" * 40)
    
    for field in TARGET_FIELDS:
        old_success = sum(1 for r in successful_results if r['field_comparison'][field]['old_has_content'])
        new_success = sum(1 for r in successful_results if r['field_comparison'][field]['new_has_content'])
        gained = sum(1 for r in successful_results if r['field_comparison'][field]['gained_content'])
        
        old_pct = old_success / len(successful_results) * 100 if successful_results else 0
        new_pct = new_success / len(successful_results) * 100 if successful_results else 0
        
        change_emoji = "📈" if new_success > old_success else "📉" if new_success < old_success else "➡️"
        
        print(f"{change_emoji} {field:20}: {old_success:3d}→{new_success:3d} ({old_pct:5.1f}%→{new_pct:5.1f}%) +{gained} gained")
    
    # Generate HTML report
    generate_html_report(results)
    
    # Save detailed JSON results
    with open('comprehensive_comparison_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n💾 Detailed results saved to: comprehensive_comparison_results.json")
    print(f"📄 HTML report generated: before_after_comparison.html")

if __name__ == "__main__":
    main()