#!/usr/bin/env python3
"""
Generate Side-by-Side HTML Report

Create an HTML report showing actual content side-by-side for old vs new scraper,
including examples of maintained quality and "losses".
"""

import sys
import json
from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime
import html

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

def generate_side_by_side_html(job_ids):
    """Generate HTML showing side-by-side comparison"""
    
    TARGET_FIELDS = [
        'Summary', 'MajorDuties', 'QualificationSummary', 'Requirements', 
        'Education', 'HowToApply', 'Evaluations', 'Benefits', 
        'RequiredDocuments', 'WhatToExpectNext', 'OtherInformation'
    ]
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>USAJobs Scraper: Side-by-Side Content Comparison</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
            .container {{ max-width: 1400px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; }}
            h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
            h2 {{ color: #34495e; margin-top: 30px; }}
            .job-card {{ background: #f8f9fa; border: 1px solid #e9ecef; margin-bottom: 30px; border-radius: 8px; }}
            .job-header {{ background: #343a40; color: white; padding: 15px; font-weight: bold; font-size: 18px; }}
            .job-stats {{ background: #e9ecef; padding: 10px 15px; font-weight: bold; }}
            .field-section {{ margin: 20px 0; }}
            .field-header {{ background: #6c757d; color: white; padding: 10px 15px; font-weight: bold; display: flex; justify-content: space-between; align-items: center; }}
            .status-badge {{ padding: 3px 8px; border-radius: 12px; font-size: 12px; font-weight: bold; }}
            .maintained {{ background: #28a745; }}
            .improved {{ background: #17a2b8; }}
            .gained {{ background: #007bff; }}
            .lost {{ background: #dc3545; }}
            .missing {{ background: #6c757d; }}
            .reduced {{ background: #fd7e14; }}
            .content-comparison {{ display: grid; grid-template-columns: 1fr 1fr; gap: 0; }}
            .old-content, .new-content {{ padding: 15px; min-height: 100px; }}
            .old-content {{ background: #fff3cd; border-right: 2px solid #856404; }}
            .new-content {{ background: #d1ecf1; border-left: 2px solid #0c5460; }}
            .content-header {{ font-weight: bold; margin-bottom: 10px; color: #495057; }}
            .content-text {{ background: white; padding: 10px; border-radius: 4px; border: 1px solid #dee2e6; font-size: 14px; line-height: 1.4; font-family: monospace; white-space: pre-wrap; max-height: 200px; overflow-y: auto; }}
            .content-stats {{ margin-top: 8px; font-size: 12px; color: #6c757d; }}
            .no-content {{ background: #f8f9fa; color: #6c757d; font-style: italic; text-align: center; }}
            .attention {{ background: #f8d7da; border: 2px solid #dc3545; border-radius: 4px; padding: 10px; margin-top: 10px; }}
            .attention h4 {{ color: #721c24; margin-top: 0; }}
            .summary {{ background: #e7f3ff; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🔍 USAJobs Scraper: Side-by-Side Content Comparison</h1>
            
            <div class="summary">
                <h2>📊 Report Summary</h2>
                <p><strong>Purpose:</strong> Compare actual extracted content between old scraper (with &lt;strong&gt; tag bug) and new improved scraper</p>
                <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p><strong>Examples:</strong> Shows high-performing jobs that maintained quality + jobs with "losses" to see what we actually lost</p>
            </div>
    """
    
    job_count = 0
    for job_id in job_ids:
        html_path = Path(f"html_cache/{job_id[:3]}/{job_id[3:6]}/{job_id}.html")
        
        if not html_path.exists():
            continue
        
        try:
            with open(html_path, 'r', encoding='utf-8') as f:
                html_file_content = f.read()
            
            soup = BeautifulSoup(html_file_content, 'html.parser')
            
            # OLD SCRAPER
            old_sections = old_parse_all_sections(soup)
            old_mapped = old_map_sections_to_fields(old_sections)
            
            # NEW SCRAPER
            from scrape_enhanced_job_posting import parse_all_sections, map_sections_to_fields
            new_sections = parse_all_sections(soup)
            new_mapped = map_sections_to_fields(new_sections)
            
            old_field_count = sum(1 for field in TARGET_FIELDS if old_mapped.get(field, ''))
            new_field_count = sum(1 for field in TARGET_FIELDS if new_mapped.get(field, ''))
            
            job_count += 1
            
            html_content += f"""
            <div class="job-card">
                <div class="job-header">
                    🔍 Job {job_count}: {job_id}
                </div>
                <div class="job-stats">
                    📊 OLD: {old_field_count}/11 fields extracted | NEW: {new_field_count}/11 fields extracted | Net Change: {new_field_count - old_field_count:+d}
                </div>
            """
            
            for field in TARGET_FIELDS:
                old_content = old_mapped.get(field, '')
                new_content = new_mapped.get(field, '')
                
                old_has = bool(old_content)
                new_has = bool(new_content)
                
                # Determine status
                if old_has and new_has:
                    if len(new_content) > len(old_content) * 1.5:
                        status = "IMPROVED"
                        badge_class = "improved"
                    elif len(new_content) < len(old_content) * 0.5:
                        status = "REDUCED"
                        badge_class = "reduced"
                    else:
                        status = "MAINTAINED"
                        badge_class = "maintained"
                elif not old_has and new_has:
                    status = "GAINED"
                    badge_class = "gained"
                elif old_has and not new_has:
                    status = "LOST"
                    badge_class = "lost"
                else:
                    status = "MISSING"
                    badge_class = "missing"
                
                html_content += f"""
                <div class="field-section">
                    <div class="field-header">
                        <span>{field}</span>
                        <span class="status-badge {badge_class}">{status}</span>
                    </div>
                    <div class="content-comparison">
                        <div class="old-content">
                            <div class="content-header">BEFORE (Old Scraper)</div>
                """
                
                if old_content:
                    # Escape HTML and truncate if too long
                    display_content = html.escape(old_content)
                    if len(display_content) > 500:
                        display_content = display_content[:500] + "..."
                    
                    html_content += f"""
                            <div class="content-text">{display_content}</div>
                            <div class="content-stats">📝 {len(old_content)} characters</div>
                    """
                else:
                    html_content += f"""
                            <div class="content-text no-content">No content extracted</div>
                            <div class="content-stats">📝 0 characters</div>
                    """
                
                html_content += """
                        </div>
                        <div class="new-content">
                            <div class="content-header">AFTER (New Scraper)</div>
                """
                
                if new_content:
                    # Escape HTML and truncate if too long
                    display_content = html.escape(new_content)
                    if len(display_content) > 500:
                        display_content = display_content[:500] + "..."
                    
                    html_content += f"""
                            <div class="content-text">{display_content}</div>
                            <div class="content-stats">📝 {len(new_content)} characters</div>
                    """
                else:
                    html_content += f"""
                            <div class="content-text no-content">No content extracted</div>
                            <div class="content-stats">📝 0 characters</div>
                    """
                
                html_content += """
                        </div>
                    </div>
                """
                
                # Add attention box for losses or significant reductions
                if status == "LOST" or (status == "REDUCED" and len(old_content) > 200):
                    html_content += f"""
                    <div class="attention">
                        <h4>⚠️ Content Change Analysis</h4>
                        <p><strong>What was lost:</strong> {html.escape(old_content[:200])}{'...' if len(old_content) > 200 else ''}</p>
                        <p><strong>Assessment:</strong> {'Real content loss - may need investigation' if len(old_content) > 100 else 'Likely misclassified junk content'}</p>
                    </div>
                    """
                
                html_content += """
                </div>
                """
            
            html_content += """
            </div>
            """
            
        except Exception as e:
            html_content += f"""
            <div class="job-card">
                <div class="job-header">❌ Error processing Job {job_id}</div>
                <div style="padding: 15px;">Error: {html.escape(str(e))}</div>
            </div>
            """
    
    html_content += """
        </div>
    </body>
    </html>
    """
    
    return html_content

def main():
    print("🔧 Generating Side-by-Side HTML Comparison")
    
    # Load the comprehensive comparison results to find good examples
    try:
        with open('comprehensive_comparison_results.json', 'r') as f:
            results = json.load(f)
    except FileNotFoundError:
        print("❌ Run comprehensive_before_after_test.py first")
        return
    
    TARGET_FIELDS = [
        'Summary', 'MajorDuties', 'QualificationSummary', 'Requirements', 
        'Education', 'HowToApply', 'Evaluations', 'Benefits', 
        'RequiredDocuments', 'WhatToExpectNext', 'OtherInformation'
    ]
    
    # Find good examples
    high_performing = []
    had_losses = []
    big_improvements = []
    
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
            high_performing.append(result['job_id'])
        
        # Jobs that had losses
        if lost_fields > 0:
            had_losses.append(result['job_id'])
        
        # Jobs with big improvements
        if gained_fields >= 4:
            big_improvements.append(result['job_id'])
    
    # Select examples to show
    example_jobs = []
    
    # 3 high performing jobs that maintained quality
    example_jobs.extend(high_performing[:3])
    
    # 3 jobs that had losses (to see what we lost)
    example_jobs.extend(had_losses[:3])
    
    # 2 jobs with big improvements
    example_jobs.extend(big_improvements[:2])
    
    print(f"Generating HTML for {len(example_jobs)} example jobs...")
    
    html_content = generate_side_by_side_html(example_jobs)
    
    output_file = 'side_by_side_comparison.html'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✅ Side-by-side HTML report generated: {output_file}")

if __name__ == "__main__":
    main()