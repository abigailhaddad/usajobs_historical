#!/usr/bin/env python3
"""
Real comparison between Historical and Current API original content
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

def real_api_comparison():
    """Compare original API content between Historical and Current sources"""
    
    # Load overlap data
    overlap_df = pd.read_parquet('data/overlap_samples.parquet')
    print(f"📊 Loaded {len(overlap_df)} overlap samples")
    
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
                'historical': sources['historical'],
                'current': sources['current']
            })
    
    print(f"📊 Found {len(comparison_pairs)} job pairs for comparison")
    
    # Fields to compare - these should show real differences
    comparison_fields = [
        'evaluations',
        'required_documents', 
        'what_to_expect_next',
        'other_information',
        'hiring_path'
    ]
    
    # Analyze differences
    field_analysis = {}
    
    for field in comparison_fields:
        print(f"\n🔍 Analyzing field: {field}")
        
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
            
            # Get content from both sources
            hist_content = str(hist_record.get(field, '')).strip()
            curr_content = str(curr_record.get(field, '')).strip()
            
            # Only compare if both have substantial content
            if len(hist_content) > 20 and len(curr_content) > 20:
                both_have_content += 1
                
                # Calculate similarity
                sim = calculate_similarity(hist_content, curr_content)
                similarities.append(sim)
                
                # Categorize the match
                if sim >= 0.99:
                    identical_content += 1
                    if len(examples['identical']) < 3:
                        examples['identical'].append({
                            'control_number': pair['control_number'],
                            'similarity': sim,
                            'hist_content': hist_content[:200],
                            'curr_content': curr_content[:200]
                        })
                elif sim >= 0.95:
                    similar_content += 1
                    if len(examples['similar']) < 3:
                        examples['similar'].append({
                            'control_number': pair['control_number'],
                            'similarity': sim,
                            'hist_content': hist_content[:200],
                            'curr_content': curr_content[:200]
                        })
                else:
                    different_content += 1
                    if len(examples['different']) < 3:
                        examples['different'].append({
                            'control_number': pair['control_number'],
                            'similarity': sim,
                            'hist_content': hist_content[:200],
                            'curr_content': curr_content[:200]
                        })
        
        # Calculate statistics
        if both_have_content > 0:
            identical_pct = (identical_content / both_have_content) * 100
            similar_pct = (similar_content / both_have_content) * 100  
            different_pct = (different_content / both_have_content) * 100
            avg_similarity = sum(similarities) / len(similarities)
        else:
            identical_pct = similar_pct = different_pct = avg_similarity = 0
        
        field_analysis[field] = {
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
    
    # Generate detailed HTML report
    generate_real_api_html_report(field_analysis, len(comparison_pairs))
    
    return field_analysis

def generate_real_api_html_report(field_analysis, total_pairs):
    """Generate HTML report showing real API differences"""
    
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Real API Comparison: Historical vs Current USAJobs APIs</title>
    <meta charset="utf-8">
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .header {{
            background: linear-gradient(135deg, #2c3e50 0%, #3498db 100%);
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
            background: #34495e;
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
            background: #ecf0f1;
        }}
        .stat-item {{
            text-align: center;
        }}
        .stat-number {{
            font-size: 1.5em;
            font-weight: bold;
            color: #2c3e50;
        }}
        .stat-label {{
            font-size: 0.9em;
            color: #7f8c8d;
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
        .content-side.historical {{
            background: #fff5f5;
            border-right: 1px solid #dee2e6;
        }}
        .content-side.current {{
            background: #f0fff4;
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
        <h1>🔍 Real API Comparison: Historical vs Current</h1>
        <p>Comparing original content from Historical API vs Current API for the same jobs</p>
        <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    
    <div class="summary">
        <h2>📊 Analysis Overview</h2>
        <p><strong>Total Job Pairs Analyzed:</strong> {total_pairs:,}</p>
        <p>This analysis compares the <em>original content</em> from the Historical API versus the Current API for the same job postings, showing real differences between the two data sources.</p>
    </div>
"""
    
    # Add section for each field
    for field, stats in field_analysis.items():
        if stats['both_have_content'] == 0:
            continue
            
        html_content += f"""
    <div class="field-section">
        <div class="field-header">
            {field.replace('_', ' ').title()} - {stats['both_have_content']} Jobs Compared
        </div>
        
        <div class="field-stats">
            <div class="stat-item">
                <div class="stat-number">{stats['identical_pct']:.1f}%</div>
                <div class="stat-label">Identical<br>(≥99% similarity)</div>
            </div>
            <div class="stat-item">
                <div class="stat-number">{stats['similar_pct']:.1f}%</div>
                <div class="stat-label">Similar<br>(95-99% similarity)</div>
            </div>
            <div class="stat-item">
                <div class="stat-number">{stats['different_pct']:.1f}%</div>
                <div class="stat-label">Different<br>(<95% similarity)</div>
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
                        <div class="content-side historical">
                            <div class="content-header">Historical API</div>
                            {example['hist_content']}...
                        </div>
                        <div class="content-side current">
                            <div class="content-header">Current API</div>
                            {example['curr_content']}...
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
            <li><strong>API Evolution:</strong> Shows how the USAJobs APIs have evolved and what content differs between sources</li>
            <li><strong>Data Quality:</strong> Identifies fields where one API provides better/more complete information</li>
            <li><strong>Integration Strategy:</strong> Helps determine which API to prioritize for each type of content</li>
        </ul>
    </div>
</body>
</html>
"""
    
    # Write HTML file
    output_file = "real_api_comparison.html"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"\n✅ Generated real API comparison report: {output_file}")
    return output_file

if __name__ == "__main__":
    real_api_comparison()