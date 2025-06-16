#!/usr/bin/env python3
"""
Compare scraped content vs Current API content for the same jobs
"""

import sys
sys.path.append('src')

import pandas as pd
import json
from difflib import SequenceMatcher
from datetime import datetime

def calculate_similarity(text1, text2):
    """Calculate similarity between two text strings"""
    if not text1 or not text2:
        return 0.0
    return SequenceMatcher(None, str(text1), str(text2)).ratio()

def scraping_vs_current_api():
    """Compare scraped content vs Current API content"""
    
    # Load overlap data
    overlap_df = pd.read_parquet('data/overlap_samples.parquet')
    print(f"📊 Loaded {len(overlap_df)} overlap samples")
    
    # We want to compare:
    # - Scraped content (from web scraping)
    # - Current API content (original from Current API)
    
    # Group by control number
    control_groups = {}
    for _, row in overlap_df.iterrows():
        control_num = row['control_number']
        source_type = row['source_type']
        if control_num not in control_groups:
            control_groups[control_num] = {}
        control_groups[control_num][source_type] = row
    
    # Find jobs with both historical and current
    comparison_pairs = []
    for control_num, sources in control_groups.items():
        if 'historical' in sources and 'current' in sources:
            comparison_pairs.append({
                'control_number': control_num,
                'historical': sources['historical'],  # This has scraped content
                'current': sources['current']          # This has Current API content
            })
    
    print(f"📊 Found {len(comparison_pairs)} job pairs for comparison")
    
    # Let's examine what scraped content looks like vs API content
    print("\n🔍 Examining data structure...")
    
    sample_pair = comparison_pairs[0]
    hist_record = sample_pair['historical']  # Should have scraped content
    curr_record = sample_pair['current']     # Should have Current API content
    
    print(f"\nJob {sample_pair['control_number']}:")
    print("\nHistorical record (with scraped content):")
    for field in hist_record.index:
        if pd.notna(hist_record[field]) and len(str(hist_record[field])) > 100:
            content = str(hist_record[field])
            print(f"  {field}: {len(content)} chars")
    
    print("\nCurrent record (Current API):")
    for field in curr_record.index:
        if pd.notna(curr_record[field]) and len(str(curr_record[field])) > 100:
            content = str(curr_record[field])
            print(f"  {field}: {len(content)} chars")
    
    # Check if historical record has scraped_sections
    if 'scraped_sections' in hist_record.index and pd.notna(hist_record['scraped_sections']):
        try:
            scraped_sections = json.loads(hist_record['scraped_sections'])
            print(f"\nScraped sections available: {list(scraped_sections.keys())}")
            
            # Show example scraped content
            for section_name, section_content in list(scraped_sections.items())[:3]:
                print(f"  {section_name}: {len(section_content)} chars")
        except:
            print("  Could not parse scraped_sections")
    
    # Now let's do the real comparison: scraped content vs Current API content
    print("\n🔍 Comparing scraped content vs Current API content...")
    
    # Map scraped sections to API fields
    field_mappings = {
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
    
    # Analyze each field
    field_analysis = {}
    
    for scraped_field, api_field in field_mappings.items():
        print(f"\n🔍 Analyzing {scraped_field} (scraped) vs {api_field} (API)...")
        
        both_have_content = 0
        identical_content = 0
        similar_content = 0  # >95% but <99%
        different_content = 0  # <95%
        similarities = []
        
        examples = {
            'identical': [],
            'similar': [],
            'different': []
        }
        
        for pair in comparison_pairs:
            hist_record = pair['historical']
            curr_record = pair['current']
            
            # Get scraped content
            scraped_content = ""
            if 'scraped_sections' in hist_record.index and pd.notna(hist_record['scraped_sections']):
                try:
                    scraped_sections = json.loads(hist_record['scraped_sections'])
                    scraped_content = scraped_sections.get(scraped_field, "")
                except:
                    pass
            
            # Get Current API content
            api_content = str(curr_record.get(api_field, '')).strip()
            
            # Only compare if both have substantial content
            if len(scraped_content) > 20 and len(api_content) > 20:
                both_have_content += 1
                
                # Calculate similarity
                sim = calculate_similarity(scraped_content, api_content)
                similarities.append(sim)
                
                # Categorize the match - changed thresholds
                if sim >= 0.99:
                    identical_content += 1
                    if len(examples['identical']) < 3:
                        examples['identical'].append({
                            'control_number': pair['control_number'],
                            'similarity': sim,
                            'scraped_content': scraped_content,  # Full content, no truncation
                            'api_content': api_content  # Full content, no truncation
                        })
                elif sim >= 0.90:  # Changed from 0.95 to 0.90
                    similar_content += 1
                    if len(examples['similar']) < 3:
                        examples['similar'].append({
                            'control_number': pair['control_number'],
                            'similarity': sim,
                            'scraped_content': scraped_content,  # Full content, no truncation
                            'api_content': api_content  # Full content, no truncation
                        })
                else:  # Now <90% instead of <95%
                    different_content += 1
                    if len(examples['different']) < 3:
                        examples['different'].append({
                            'control_number': pair['control_number'],
                            'similarity': sim,
                            'scraped_content': scraped_content,  # Full content, no truncation
                            'api_content': api_content  # Full content, no truncation
                        })
        
        # Calculate statistics
        if both_have_content > 0:
            identical_pct = (identical_content / both_have_content) * 100
            similar_pct = (similar_content / both_have_content) * 100  
            different_pct = (different_content / both_have_content) * 100
            avg_similarity = sum(similarities) / len(similarities)
        else:
            identical_pct = similar_pct = different_pct = avg_similarity = 0
        
        if both_have_content > 0:
            field_analysis[scraped_field] = {
                'api_field': api_field,
                'both_have_content': both_have_content,
                'identical_content': identical_content,
                'similar_content': similar_content,
                'different_content': different_content,
                'identical_pct': identical_pct,
                'similar_pct': similar_pct,
                'different_pct': different_pct,
                'avg_similarity': avg_similarity,
                'examples': examples
            }
            
            print(f"  Both have content: {both_have_content} jobs")
            print(f"  Identical (≥99%): {identical_content} ({identical_pct:.1f}%)")
            print(f"  Similar (95-99%): {similar_content} ({similar_pct:.1f}%)")  
            print(f"  Different (<95%): {different_content} ({different_pct:.1f}%)")
            print(f"  Average similarity: {avg_similarity:.3f}")
        else:
            print(f"  No jobs with both scraped and API content for this field")
    
    # Generate HTML report
    generate_scraping_vs_api_html_report(field_analysis, len(comparison_pairs))
    
    return field_analysis

def generate_scraping_vs_api_html_report(field_analysis, total_pairs):
    """Generate HTML report comparing scraped vs API content"""
    
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Scraped Content vs Current API Comparison</title>
    <meta charset="utf-8">
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .header {{
            background: linear-gradient(135deg, #9b59b6 0%, #3498db 100%);
            color: white;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 30px;
        }}
        .summary {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 30px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        .field-section {{
            background: white;
            margin-bottom: 30px;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        .field-header {{
            background: #8e44ad;
            color: white;
            padding: 15px 20px;
            font-size: 1.2em;
            font-weight: bold;
        }}
        .field-stats {{
            padding: 20px;
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            background: #f8f9fa;
        }}
        .stat-item {{
            text-align: center;
        }}
        .stat-number {{
            font-size: 1.5em;
            font-weight: bold;
            color: #8e44ad;
        }}
        .stat-label {{
            font-size: 0.9em;
            color: #6c757d;
        }}
        .examples {{
            padding: 20px;
        }}
        .example-category {{
            margin-bottom: 25px;
        }}
        .example-title {{
            font-weight: bold;
            color: #2c3e50;
            margin-bottom: 10px;
            padding: 8px 12px;
            border-radius: 5px;
        }}
        .example-title.identical {{ background: #d5edda; }}
        .example-title.similar {{ background: #fff3cd; }}
        .example-title.different {{ background: #f8d7da; }}
        .example-item {{
            border: 1px solid #dee2e6;
            border-radius: 5px;
            margin-bottom: 15px;
            overflow: hidden;
        }}
        .example-header {{
            background: #f8f9fa;
            padding: 10px 15px;
            font-weight: bold;
            font-size: 0.9em;
        }}
        .content-comparison {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 0;
        }}
        .content-side {{
            padding: 15px;
            font-size: 0.85em;
            line-height: 1.4;
        }}
        .content-side.scraped {{
            background: #f3e5f5;
            border-right: 1px solid #dee2e6;
        }}
        .content-side.api {{
            background: #e8f4f8;
        }}
        .content-header {{
            font-weight: bold;
            margin-bottom: 8px;
            color: #495057;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🔍 Scraped Content vs Current API</h1>
        <p>Comparing web-scraped content vs Current USAJobs API content for the same jobs</p>
        <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    
    <div class="summary">
        <h2>📊 Analysis Overview</h2>
        <p><strong>Total Job Pairs Analyzed:</strong> {total_pairs:,}</p>
        <p>This analysis compares content extracted by web scraping job pages versus the content provided by the Current USAJobs API for the same job postings.</p>
        <p><strong>Purpose:</strong> Determine which source provides better/more complete content for each field type.</p>
    </div>
"""
    
    # Add section for each field
    for field, stats in field_analysis.items():
        html_content += f"""
    <div class="field-section">
        <div class="field-header">
            {field} (scraped) vs {stats['api_field']} (API) - {stats['both_have_content']} Jobs Compared
        </div>
        
        <div class="field-stats">
            <div class="stat-item">
                <div class="stat-number">{stats['identical_pct']:.1f}%</div>
                <div class="stat-label">Identical<br>(≥99% similarity)</div>
            </div>
            <div class="stat-item">
                <div class="stat-number">{stats['similar_pct']:.1f}%</div>
                <div class="stat-label">Similar<br>(90-99% similarity)</div>
            </div>
            <div class="stat-item">
                <div class="stat-number">{stats['different_pct']:.1f}%</div>
                <div class="stat-label">Different<br>(<90% similarity)</div>
            </div>
            <div class="stat-item">
                <div class="stat-number">{stats['avg_similarity']:.3f}</div>
                <div class="stat-label">Average<br>Similarity</div>
            </div>
        </div>
        
        <div class="examples">
"""
        
        # Add examples for each category
        categories = [
            ('different', 'Different Content Examples', 'different'),
            ('similar', 'Similar Content Examples', 'similar'), 
            ('identical', 'Identical Content Examples', 'identical')
        ]
        
        for cat_key, cat_title, cat_class in categories:
            examples = stats['examples'][cat_key]
            if examples:
                html_content += f"""
            <div class="example-category">
                <div class="example-title {cat_class}">{cat_title}</div>
"""
                
                for example in examples:
                    html_content += f"""
                <div class="example-item">
                    <div class="example-header">
                        Job #{example['control_number']} - Similarity: {example['similarity']:.3f}
                    </div>
                    <div class="content-comparison">
                        <div class="content-side scraped">
                            <div class="content-header">Scraped Content</div>
                            {example['scraped_content']}
                        </div>
                        <div class="content-side api">
                            <div class="content-header">Current API Content</div>
                            {example['api_content']}
                        </div>
                    </div>
                </div>
"""
                
                html_content += """
            </div>
"""
        
        html_content += """
        </div>
    </div>
"""
    
    html_content += """
    <div class="summary">
        <h2>🔍 Key Insights</h2>
        <ul>
            <li><strong>Content Source Quality:</strong> Shows which source (scraping vs API) provides better content for each field</li>
            <li><strong>Data Completeness:</strong> Identifies gaps where one source has more detailed information</li>
            <li><strong>Integration Strategy:</strong> Helps determine optimal data source prioritization</li>
        </ul>
    </div>
</body>
</html>
"""
    
    # Write HTML file
    output_file = "scraping_vs_api_comparison.html"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"\n✅ Generated scraping vs API comparison report: {output_file}")
    return output_file

if __name__ == "__main__":
    scraping_vs_current_api()