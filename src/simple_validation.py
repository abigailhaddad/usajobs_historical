#!/usr/bin/env python3
"""
Simple validation for USAJobs pipeline - just field overlap percentages
"""

import pandas as pd
from typing import Dict, Any


def calculate_field_overlap(overlap_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calculate content matching percentages between scraped and API data
    
    Returns:
        Dictionary with content matching percentages
    """
    from difflib import SequenceMatcher
    
    def similarity(a, b):
        return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()
    
    # Separate records by source type
    historical_records = overlap_df[overlap_df['source_type'] == 'historical'].set_index('control_number')
    current_records = overlap_df[overlap_df['source_type'] == 'current'].set_index('control_number')
    
    # Find overlapping control numbers
    overlap_controls = set(historical_records.index) & set(current_records.index)
    
    if not overlap_controls:
        return {'status': 'error', 'message': 'No overlapping records found'}
    
    # Fields to check
    content_fields = ['requirements', 'education', 'major_duties', 'qualification_summary', 'benefits']
    
    field_stats = {}
    
    for field in content_fields:
        api_has_content = 0
        scraped_has_content = 0
        both_have_content = 0
        perfect_matches = 0
        good_matches = 0  # >95% similarity
        similarities = []
        
        for control_number in overlap_controls:
            historical_record = historical_records.loc[control_number]
            current_record = current_records.loc[control_number]
            
            # Check API content
            api_content = str(current_record[field]).strip() if pd.notna(current_record[field]) else ''
            api_has_data = len(api_content) > 20
            
            # Check scraped content - look for the field directly in historical record
            scraped_content = ''
            if pd.notna(historical_record[field]):
                scraped_content = str(historical_record[field]).strip()
            
            scraped_has_data = len(scraped_content) > 20
            
            if api_has_data:
                api_has_content += 1
            if scraped_has_data:
                scraped_has_content += 1
            if api_has_data and scraped_has_data:
                both_have_content += 1
                
                # Calculate content similarity
                sim = similarity(api_content, scraped_content)
                similarities.append(sim)
                
                if sim >= 0.99:
                    perfect_matches += 1
                if sim >= 0.95:
                    good_matches += 1
        
        # Calculate percentages
        total_comparisons = both_have_content
        perfect_match_pct = (perfect_matches / total_comparisons * 100) if total_comparisons > 0 else 0
        good_match_pct = (good_matches / total_comparisons * 100) if total_comparisons > 0 else 0
        avg_similarity = sum(similarities) / len(similarities) if similarities else 0
        
        field_stats[field] = {
            'total_jobs': len(overlap_controls),
            'api_has_content': api_has_content,
            'scraped_has_content': scraped_has_content,
            'both_have_content': both_have_content,
            'perfect_matches': perfect_matches,
            'good_matches': good_matches,
            'perfect_match_pct': perfect_match_pct,
            'good_match_pct': good_match_pct,
            'avg_similarity': avg_similarity,
            'api_coverage': (api_has_content / len(overlap_controls)) * 100,
            'scraped_coverage': (scraped_has_content / len(overlap_controls)) * 100,
            'both_coverage': (both_have_content / len(overlap_controls)) * 100
        }
    
    return {
        'status': 'success',
        'total_overlap_jobs': len(overlap_controls),
        'field_stats': field_stats
    }


def generate_simple_validation_html(validation_results: Dict[str, Any]) -> str:
    """Generate simple HTML for field overlap"""
    
    if validation_results['status'] != 'success':
        return f"<p>Validation Error: {validation_results.get('message', 'Unknown error')}</p>"
    
    html = f"""
    <div class="simple-validation">
        <h3>Content Matching Analysis</h3>
        <p><strong>Overlap Jobs Tested:</strong> {validation_results['total_overlap_jobs']}</p>
        
        <table class="overlap-table">
            <thead>
                <tr>
                    <th>Field</th>
                    <th>Both Have Content</th>
                    <th>Perfect Matches</th>
                    <th>Good Matches (≥95%)</th>
                    <th>Avg Similarity</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for field, stats in validation_results['field_stats'].items():
        html += f"""
                <tr>
                    <td>{field.replace('_', ' ').title()}</td>
                    <td>{stats['both_coverage']:.1f}% ({stats['both_have_content']}/{stats['total_jobs']})</td>
                    <td>{stats['perfect_match_pct']:.1f}% ({stats['perfect_matches']}/{stats['both_have_content']})</td>
                    <td>{stats['good_match_pct']:.1f}% ({stats['good_matches']}/{stats['both_have_content']})</td>
                    <td>{stats['avg_similarity']:.3f}</td>
                </tr>
        """
    
    html += """
            </tbody>
        </table>
    </div>
    
    <style>
    .simple-validation {
        margin: 20px 0;
    }
    
    .overlap-table {
        width: 100%;
        border-collapse: collapse;
        margin: 10px 0;
    }
    
    .overlap-table th,
    .overlap-table td {
        border: 1px solid #ddd;
        padding: 8px;
        text-align: left;
    }
    
    .overlap-table th {
        background-color: #f2f2f2;
        font-weight: bold;
    }
    </style>
    """
    
    return html


def generate_simple_validation_summary(validation_results: Dict[str, Any]) -> str:
    """Generate simple text summary"""
    
    if validation_results['status'] != 'success':
        return f"**Validation Error:** {validation_results.get('message', 'Unknown error')}"
    
    summary = f"""
**Content Matching Summary**

- **Overlap Jobs Tested:** {validation_results['total_overlap_jobs']}

"""
    
    for field, stats in validation_results['field_stats'].items():
        if stats['both_have_content'] > 0:
            summary += f"- **{field.replace('_', ' ').title()}:** {stats['both_have_content']} comparisons, {stats['perfect_match_pct']:.1f}% perfect matches, {stats['avg_similarity']:.3f} avg similarity\n"
        else:
            summary += f"- **{field.replace('_', ' ').title()}:** No overlapping content to compare\n"
    
    return summary


if __name__ == "__main__":
    # Test with existing data
    import sys
    sys.path.append('/Users/abigailhaddad/Documents/repos/usajobs_historic/usajobs_pipeline/scripts')
    from parquet_storage import ParquetJobStorage
    
    storage = ParquetJobStorage('/Users/abigailhaddad/Documents/repos/usajobs_historic/usajobs_pipeline/data_parquet')
    overlap_df = storage.load_overlap_samples()
    
    results = calculate_field_overlap(overlap_df)
    print("Simple Validation Results:")
    print(generate_simple_validation_summary(results))