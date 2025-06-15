#!/usr/bin/env python3
"""
Generate HTML report showing side-by-side content mismatches
"""

import pandas as pd
import json
import random
import sys
from datetime import datetime
from difflib import SequenceMatcher

sys.path.append('scripts')
from parquet_storage import ParquetJobStorage

def calculate_similarity(text1, text2):
    """Calculate similarity between two text strings"""
    if not text1 or not text2:
        return 0.0
    return SequenceMatcher(None, str(text1), str(text2)).ratio()

def clean_text_for_display(text, max_length=None):
    """Clean text for HTML display"""
    if not text or pd.isna(text):
        return "❌ No content"
    
    text = str(text).strip()
    if len(text) == 0:
        return "❌ Empty content"
    
    # Remove excessive whitespace but preserve line breaks
    lines = text.split('\n')
    cleaned_lines = []
    for line in lines:
        cleaned_line = ' '.join(line.split())
        if cleaned_line:
            cleaned_lines.append(cleaned_line)
    
    text = '\n'.join(cleaned_lines)
    
    # Only truncate if max_length specified
    if max_length and len(text) > max_length:
        text = text[:max_length] + "... [TRUNCATED]"
    
    return text

def determine_data_source(row, field_name):
    """Determine if field comes from API or scraping"""
    # Fields that typically come from scraping
    scraped_fields = {'major_duties', 'qualification_summary', 'requirements', 'education', 'benefits', 'how_to_apply'}
    
    # Check if we have data_sources information
    if 'data_sources' in row.index and pd.notna(row['data_sources']):
        try:
            sources = json.loads(row['data_sources']) if isinstance(row['data_sources'], str) else row['data_sources']
            if isinstance(sources, list):
                has_scraping = any('scraping' in str(s).lower() for s in sources)
                if field_name in scraped_fields and has_scraping:
                    return "Scraped Content"
        except:
            pass
    
    # For historical records, content fields are usually scraped
    if row.get('source_type') == 'historical' and field_name in scraped_fields:
        return "Historical API + Scraping"
    elif row.get('source_type') == 'current' and field_name in scraped_fields:
        return "Current API + Scraping" 
    elif row.get('source_type') == 'historical':
        return "Historical API"
    else:
        return "Current API"

def generate_mismatch_html():
    """Generate HTML report with side-by-side content comparisons"""
    
    # Load overlap data
    storage = ParquetJobStorage('../data_parquet')
    overlap_df = storage.load_overlap_samples()
    
    if overlap_df.empty:
        print("❌ No overlap data found")
        return
    
    print(f"📊 Loaded {len(overlap_df)} overlap samples")
    
    # Group by control number to get pairs
    control_groups = {}
    for _, row in overlap_df.iterrows():
        control_num = row['control_number']
        source_type = row['source_type']
        if control_num not in control_groups:
            control_groups[control_num] = {}
        control_groups[control_num][source_type] = row
    
    # Find complete pairs and analyze content
    complete_pairs = []
    for control_num, sources in control_groups.items():
        if 'historical' in sources and 'current' in sources:
            hist_data = sources['historical']
            curr_data = sources['current']
            
            # Calculate similarities for key fields
            similarities = {}
            for field in ['major_duties', 'qualification_summary', 'requirements', 'education']:
                if field in hist_data.index and field in curr_data.index:
                    hist_val = hist_data[field] if pd.notna(hist_data[field]) else ""
                    curr_val = curr_data[field] if pd.notna(curr_data[field]) else ""
                    similarities[field] = calculate_similarity(hist_val, curr_val)
                else:
                    similarities[field] = 0.0
            
            complete_pairs.append({
                'control_number': control_num,
                'historical': hist_data,
                'current': curr_data,
                'similarities': similarities
            })
    
    print(f"📊 Found {len(complete_pairs)} complete job pairs")
    
    # Sample different types of mismatches
    samples = {
        'major_duties_mismatches': [],
        'qualification_mismatches': [],
        'requirements_mismatches': [],
        'education_mismatches': []
    }
    
    for pair in complete_pairs:
        sims = pair['similarities']
        
        # Major duties with low similarity
        if sims['major_duties'] < 0.5 and sims['major_duties'] > 0:
            samples['major_duties_mismatches'].append(pair)
        
        # Qualification summary mismatches
        if sims['qualification_summary'] < 0.9 and sims['qualification_summary'] > 0:
            samples['qualification_mismatches'].append(pair)
        
        # Requirements mismatches
        if sims['requirements'] < 0.9 and sims['requirements'] > 0:
            samples['requirements_mismatches'].append(pair)
        
        # Education mismatches
        if sims['education'] < 0.7 and sims['education'] > 0:
            samples['education_mismatches'].append(pair)
    
    # Sample 5 examples from each category
    for category in samples:
        if samples[category]:
            samples[category] = random.sample(samples[category], min(5, len(samples[category])))
    
    print(f"📊 Sampled mismatches:")
    for category, pairs in samples.items():
        print(f"   {category}: {len(pairs)} examples")
    
    # Generate HTML
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>USAJobs Content Mismatch Analysis</title>
    <meta charset="utf-8">
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
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
        .comparison-section {{
            margin-bottom: 40px;
        }}
        .section-title {{
            background: #2c3e50;
            color: white;
            padding: 15px;
            margin: 0;
            border-radius: 8px 8px 0 0;
            font-size: 1.2em;
            font-weight: bold;
        }}
        .comparison-container {{
            background: white;
            border-radius: 0 0 8px 8px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        .job-comparison {{
            border-bottom: 3px solid #ecf0f1;
            padding: 20px;
        }}
        .job-comparison:last-child {{
            border-bottom: none;
        }}
        .job-header {{
            background: #3498db;
            color: white;
            padding: 10px 15px;
            margin: -20px -20px 20px -20px;
            font-weight: bold;
        }}
        .field-comparison {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-bottom: 20px;
        }}
        .source-column {{
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            overflow: hidden;
        }}
        .source-header {{
            padding: 10px 15px;
            font-weight: bold;
            color: white;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .historical-header {{
            background: #e74c3c;
        }}
        .current-header {{
            background: #27ae60;
        }}
        .scraped-header {{
            background: #9b59b6;
        }}
        .data-source-badge {{
            background: rgba(255,255,255,0.2);
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 0.7em;
        }}
        .source-content {{
            padding: 15px;
            background: white;
            min-height: 100px;
            white-space: pre-wrap;
            font-size: 0.9em;
            line-height: 1.4;
        }}
        .similarity-badge {{
            display: inline-block;
            padding: 5px 10px;
            border-radius: 20px;
            color: white;
            font-size: 0.8em;
            font-weight: bold;
            margin-left: 10px;
        }}
        .similarity-high {{ background: #27ae60; }}
        .similarity-medium {{ background: #f39c12; }}
        .similarity-low {{ background: #e74c3c; }}
        .field-name {{
            font-weight: bold;
            color: #2c3e50;
            margin-bottom: 10px;
            font-size: 1.1em;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 20px;
        }}
        .stat-card {{
            background: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            text-align: center;
        }}
        .stat-number {{
            font-size: 2em;
            font-weight: bold;
            color: #3498db;
        }}
        .stat-label {{
            color: #7f8c8d;
            font-size: 0.9em;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🔍 USAJobs Content Mismatch Analysis</h1>
        <p>Side-by-side comparison of content differences between Historical and Current APIs</p>
        <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    
    <div class="summary">
        <h2>📊 Analysis Summary</h2>
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-number">{len(complete_pairs):,}</div>
                <div class="stat-label">Total Job Pairs</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{len(samples['major_duties_mismatches'])}</div>
                <div class="stat-label">Major Duties Mismatches</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{len(samples['qualification_mismatches'])}</div>
                <div class="stat-label">Qualification Mismatches</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{len(samples['requirements_mismatches'])}</div>
                <div class="stat-label">Requirements Mismatches</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{len(samples['education_mismatches'])}</div>
                <div class="stat-label">Education Mismatches</div>
            </div>
        </div>
        <p><strong>Key Finding:</strong> Content differences suggest the APIs provide different levels of detail and formatting for the same job postings.</p>
    </div>
    """
    
    # Add sections for each mismatch type
    section_titles = {
        'major_duties_mismatches': '📋 Major Duties Mismatches (Low Similarity)',
        'qualification_mismatches': '🎓 Qualification Summary Mismatches',
        'requirements_mismatches': '📝 Requirements Mismatches',
        'education_mismatches': '🎓 Education Mismatches'
    }
    
    for category, title in section_titles.items():
        if not samples[category]:
            continue
            
        html_content += f"""
    <div class="comparison-section">
        <h2 class="section-title">{title}</h2>
        <div class="comparison-container">
        """
        
        for i, pair in enumerate(samples[category]):
            field_name = category.replace('_mismatches', '').replace('_', ' ').title()
            if field_name == 'Major Duties':
                field_key = 'major_duties'
            elif field_name == 'Qualification':
                field_key = 'qualification_summary'
            else:
                field_key = category.replace('_mismatches', '')
            
            hist_data = pair['historical']
            curr_data = pair['current']
            similarity = pair['similarities'][field_key]
            
            # Get content for the specific field (NO TRUNCATION)
            hist_content = clean_text_for_display(hist_data.get(field_key, ""))
            curr_content = clean_text_for_display(curr_data.get(field_key, ""))
            
            # Determine data sources
            hist_source = determine_data_source(hist_data, field_key)
            curr_source = determine_data_source(curr_data, field_key)
            
            # Set header colors based on data source
            hist_header_class = "scraped-header" if "Scraping" in hist_source else "historical-header"
            curr_header_class = "scraped-header" if "Scraping" in curr_source else "current-header"
            
            # Similarity badge
            if similarity >= 0.8:
                sim_class = "similarity-high"
            elif similarity >= 0.5:
                sim_class = "similarity-medium"
            else:
                sim_class = "similarity-low"
            
            position_title = clean_text_for_display(hist_data.get('position_title', 'Unknown Position'), 100)
            agency_name = clean_text_for_display(hist_data.get('agency_name', 'Unknown Agency'), 100)
            
            html_content += f"""
            <div class="job-comparison">
                <div class="job-header">
                    Job #{pair['control_number']} - {position_title} at {agency_name}
                    <span class="similarity-badge {sim_class}">
                        {similarity:.1%} Similarity
                    </span>
                </div>
                
                <div class="field-name">{field_name} - Character Counts: Historical ({len(str(hist_content))}) vs Current ({len(str(curr_content))})</div>
                <div class="field-comparison">
                    <div class="source-column">
                        <div class="source-header {hist_header_class}">
                            <span>Historical Side</span>
                            <span class="data-source-badge">{hist_source}</span>
                        </div>
                        <div class="source-content">{hist_content}</div>
                    </div>
                    <div class="source-column">
                        <div class="source-header {curr_header_class}">
                            <span>Current Side</span>
                            <span class="data-source-badge">{curr_source}</span>
                        </div>
                        <div class="source-content">{curr_content}</div>
                    </div>
                </div>
            </div>
            """
        
        html_content += """
        </div>
    </div>
        """
    
    html_content += """
    <div class="summary">
        <h2>🔍 Analysis Insights</h2>
        <ul>
            <li><strong>API Content Variations:</strong> The Historical and Current APIs often provide different levels of detail for the same job.</li>
            <li><strong>Formatting Differences:</strong> Content may be formatted differently (HTML vs plain text, bullet points vs paragraphs).</li>
            <li><strong>Timing Factor:</strong> Job content may have been updated between API calls.</li>
            <li><strong>Data Source Reliability:</strong> Web scraping provides consistent content extraction that can bridge these gaps.</li>
        </ul>
        <p><strong>Recommendation:</strong> The scraping data provides the most complete and consistent content source. Use it as the primary source for detailed job content.</p>
    </div>
</body>
</html>
    """
    
    # Write HTML file
    output_file = "content_mismatch_analysis.html"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✅ Generated mismatch analysis: {output_file}")
    return output_file

if __name__ == "__main__":
    generate_mismatch_html()