#!/usr/bin/env python3
"""
Generate JSON data for the Essay Question analysis web interface
"""
import pandas as pd
import json
import os
from pathlib import Path
import re
from datetime import datetime

# Define paths
BASE_DIR = Path('../..')
QUESTIONNAIRE_DIR = Path('..')
DATA_DIR = BASE_DIR / 'data'
RAW_QUESTIONNAIRES_DIR = QUESTIONNAIRE_DIR / 'raw_questionnaires'

def calculate_eo_stats(df, group_column, top_n=None, column_name=None):
    """Calculate EO question statistics for any grouping column"""
    stats = df.groupby(group_column).agg({
        'has_executive_order': ['sum', 'count']
    }).reset_index()
    
    # Use provided column name or default to the group column name
    display_name = column_name if column_name else group_column
    
    stats.columns = [display_name, 'Jobs with Essay Question', 'Total Jobs Analyzed']
    stats['Percentage with Essay Question'] = (stats['Jobs with Essay Question'] / stats['Total Jobs Analyzed'] * 100).round(1)
    
    if top_n:
        stats = stats.nlargest(top_n, 'Total Jobs Analyzed')
    
    return stats.sort_values('Total Jobs Analyzed', ascending=False).reset_index(drop=True)

def check_executive_order_mentions(questionnaire_dir=RAW_QUESTIONNAIRES_DIR):
    """Check which questionnaires mention the specific executive order question"""
    mentions = {}
    pattern = re.compile(r"How would you help advance the President's Executive Orders and policy priorities in this role\?", re.IGNORECASE)
    
    if not questionnaire_dir.exists():
        print(f"Warning: {questionnaire_dir} does not exist")
        return mentions
    
    txt_files = list(questionnaire_dir.glob('*.txt'))
    print(f"Found {len(txt_files):,} scraped questionnaire files")
    
    for txt_file in txt_files:
        try:
            with open(txt_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
            if pattern.search(content):
                file_id = txt_file.stem.split('_')[1]
                mentions[file_id] = 1
        except Exception as e:
            print(f"Error reading {txt_file}: {e}")
    
    return mentions

def extract_questionnaire_id(url):
    """Extract questionnaire ID from either USAStaffing or Monster URLs"""
    if 'usastaffing.gov' in url:
        match = re.search(r'/ViewQuestionnaire/(\d+)', url)
        return match.group(1) if match else None
    elif 'monstergovt.com' in url:
        match = re.search(r'jnum=(\d+)', url)
        if not match:
            match = re.search(r'J=(\d+)', url)
        return match.group(1) if match else None
    return None

def main():
    # Load questionnaire links
    links_df = pd.read_csv(QUESTIONNAIRE_DIR / 'questionnaire_links.csv')
    print(f"Loaded {len(links_df):,} questionnaire links")
    
    # Check for the specific executive order question
    eo_mentions = check_executive_order_mentions()
    print(f"\nFound {len(eo_mentions):,} questionnaires with the essay question")
    
    # Get all scraped IDs
    scraped_ids = set()
    for txt_file in RAW_QUESTIONNAIRES_DIR.glob('*.txt'):
        file_id = txt_file.stem.split('_')[1]
        scraped_ids.add(file_id)
    
    # Add questionnaire ID and executive order flags
    links_df['questionnaire_id'] = links_df['questionnaire_url'].apply(extract_questionnaire_id)
    links_df['has_executive_order'] = links_df['questionnaire_id'].isin(eo_mentions)
    
    # Filter to only scraped questionnaires
    scraped_df = links_df[links_df['questionnaire_id'].isin(scraped_ids)].copy()
    
    print(f"\nTotal scraped questionnaires: {len(scraped_df):,}")
    print(f"Records with executive order mentions: {scraped_df['has_executive_order'].sum():,}")
    
    # Calculate overall statistics
    total_scraped = len(scraped_df)
    total_with_eo = scraped_df['has_executive_order'].sum()
    percentage_with_eo = (total_with_eo / total_scraped * 100) if total_scraped > 0 else 0
    
    # Get date range
    if 'position_open_date' in scraped_df.columns:
        scraped_df['position_open_date'] = pd.to_datetime(scraped_df['position_open_date'], format='mixed')
        earliest_date = scraped_df['position_open_date'].min()
        latest_date = scraped_df['position_open_date'].max()
        data_coverage = f"Analyzing questionnaires from federal job postings from {earliest_date.strftime('%B %d, %Y')} to {latest_date.strftime('%B %d, %Y')}"
    else:
        data_coverage = "Date information not available"
    
    # Prepare the data structure
    analysis_data = {
        'overview': {
            'total_scraped': total_scraped,
            'total_with_eo': total_with_eo,
            'percentage_with_eo': round(percentage_with_eo, 1),
            'data_coverage': data_coverage
        }
    }
    
    # Service Type Analysis
    if 'service_type' in scraped_df.columns:
        service_stats = calculate_eo_stats(scraped_df, 'service_type', column_name='Service Type')
        analysis_data['service_analysis'] = service_stats.to_dict('records')
    else:
        analysis_data['service_analysis'] = []
    
    # Grade Level Analysis
    if 'grade_code' in scraped_df.columns:
        scraped_df['grade_level'] = scraped_df['grade_code'].fillna('Not Specified')
        grade_stats = calculate_eo_stats(scraped_df, 'grade_level', top_n=10, column_name='Grade Level')
        analysis_data['grade_analysis'] = grade_stats.to_dict('records')
    else:
        analysis_data['grade_analysis'] = []
    
    # Location Analysis
    if 'position_location' in scraped_df.columns:
        location_stats = calculate_eo_stats(scraped_df, 'position_location', top_n=10, column_name='Location')
        analysis_data['location_analysis'] = location_stats.to_dict('records')
    else:
        analysis_data['location_analysis'] = []
    
    # Agency Analysis
    agency_stats = calculate_eo_stats(scraped_df, 'hiring_agency', top_n=20, column_name='Agency')
    analysis_data['agency_analysis'] = agency_stats.to_dict('records')
    
    # Occupation Analysis
    scraped_df['occupation_full'] = scraped_df['occupation_series'].astype(str) + ' - ' + scraped_df['occupation_name'].fillna('Unknown')
    occupation_stats = calculate_eo_stats(scraped_df, 'occupation_full', top_n=20, column_name='Occupation Series')
    analysis_data['occupation_analysis'] = occupation_stats.to_dict('records')
    
    # Timeline Analysis
    if 'position_open_date' in scraped_df.columns:
        scraped_df['open_date'] = pd.to_datetime(scraped_df['position_open_date'], format='mixed', errors='coerce')
        jobs_with_dates = scraped_df[scraped_df['open_date'].notna()].copy()
        
        # Extract date components
        jobs_with_dates['year'] = jobs_with_dates['open_date'].dt.year
        jobs_with_dates['month'] = jobs_with_dates['open_date'].dt.month
        jobs_with_dates['month_name'] = jobs_with_dates['open_date'].dt.strftime('%B %Y')
        jobs_with_dates['week_of_month'] = jobs_with_dates['open_date'].dt.day.apply(lambda d: (d-1)//7 + 1)
        
        # Group by month and week
        weekly_stats = jobs_with_dates.groupby(['year', 'month', 'month_name', 'week_of_month']).agg({
            'questionnaire_id': 'count',
            'has_executive_order': 'sum'
        }).reset_index()
        
        weekly_stats['percentage'] = weekly_stats.apply(
            lambda row: round(row['has_executive_order'] / row['questionnaire_id'] * 100, 1) if row['questionnaire_id'] > 0 else None,
            axis=1
        )
        
        # Pivot to create heatmap format
        heatmap_data = weekly_stats.pivot_table(
            index=['year', 'month', 'month_name'],
            columns='week_of_month',
            values='percentage',
            aggfunc='first'
        ).reset_index()
        
        # Calculate monthly totals
        monthly_totals = weekly_stats.groupby(['year', 'month', 'month_name']).agg({
            'questionnaire_id': 'sum',
            'has_executive_order': 'sum'
        }).reset_index()
        monthly_totals['monthly_percentage'] = (monthly_totals['has_executive_order'] / monthly_totals['questionnaire_id'] * 100).round(1)
        
        # Format for display
        timeline_data = []
        for _, row in heatmap_data.iterrows():
            month_name = row['month_name']
            month_totals = monthly_totals[monthly_totals['month_name'] == month_name].iloc[0]
            
            timeline_entry = {
                'Month': month_name,
                'Week 1': row.get(1) if pd.notna(row.get(1)) else None,
                'Week 2': row.get(2) if pd.notna(row.get(2)) else None,
                'Week 3': row.get(3) if pd.notna(row.get(3)) else None,
                'Week 4': row.get(4) if pd.notna(row.get(4)) else None,
                'Week 5': row.get(5) if pd.notna(row.get(5)) else None,
                'Monthly Percentage': month_totals['monthly_percentage'],
                'Total Jobs Posted': int(month_totals['questionnaire_id']),
                'Jobs with Essay Question': int(month_totals['has_executive_order'])
            }
            timeline_data.append(timeline_entry)
        
        analysis_data['timeline_analysis'] = timeline_data
    else:
        analysis_data['timeline_analysis'] = []
    
    # Job Postings
    eo_jobs = scraped_df[scraped_df['has_executive_order']].copy()
    
    if len(eo_jobs) > 0:
        # Format dates
        eo_jobs['open_date'] = pd.to_datetime(eo_jobs['position_open_date'], format='mixed').dt.strftime('%m/%d/%Y')
        eo_jobs['close_date'] = pd.to_datetime(eo_jobs['position_close_date'], format='mixed').dt.strftime('%m/%d/%Y')
        
        # Create occupation display
        eo_jobs['occupation'] = eo_jobs['occupation_series'].astype(str) + ' - ' + eo_jobs['occupation_name'].fillna('Unknown')
        
        # Prepare job postings data
        job_postings = []
        for _, job in eo_jobs.iterrows():
            posting = {
                'position_title': job['position_title'][:100] if pd.notna(job['position_title']) else '',
                'occupation': job['occupation'][:50] if pd.notna(job['occupation']) else '',
                'agency': job['hiring_agency'][:50] if pd.notna(job['hiring_agency']) else '',
                'location': job.get('position_location', '')[:50] if pd.notna(job.get('position_location', '')) else '',
                'grade': job.get('grade_code', '') if pd.notna(job.get('grade_code', '')) else '',
                'service': job.get('service_type', '') if pd.notna(job.get('service_type', '')) else '',
                'open_date': job['open_date'] if pd.notna(job['open_date']) else '',
                'close_date': job['close_date'] if pd.notna(job['close_date']) else '',
                'usajobs_link': job['usajobs_control_number'] if pd.notna(job['usajobs_control_number']) else '',
                'questionnaire_link': job['questionnaire_url'] if pd.notna(job['questionnaire_url']) else ''
            }
            job_postings.append(posting)
        
        analysis_data['job_postings'] = job_postings
    else:
        analysis_data['job_postings'] = []
    
    # Convert numpy types to Python native types for JSON serialization
    def convert_to_native(obj):
        if isinstance(obj, pd.Int64Dtype) or hasattr(obj, 'item'):
            return obj.item()
        elif isinstance(obj, dict):
            return {k: convert_to_native(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_to_native(i) for i in obj]
        else:
            return obj
    
    # Write to JSON file
    with open('analysis_data.json', 'w') as f:
        json.dump(analysis_data, f, indent=2, default=str)
    
    print(f"\nAnalysis data written to analysis_data.json")
    print(f"Total data points: {sum(len(v) if isinstance(v, list) else 1 for v in analysis_data.values())}")

if __name__ == '__main__':
    main()