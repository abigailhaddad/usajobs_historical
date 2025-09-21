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
from questionnaire_utils import (
    transform_monster_url, extract_questionnaire_id, get_questionnaire_filename,
    RAW_QUESTIONNAIRES_DIR, QUESTIONNAIRE_LINKS_CSV
)

# Define paths
BASE_DIR = Path('..')
QUESTIONNAIRE_DIR = Path('.')
DATA_DIR = BASE_DIR / 'data'

def calculate_eo_stats(all_jobs_df, scraped_df, group_column, top_n=None, column_name=None):
    """Calculate EO question statistics for any grouping column"""
    # Fill NaN values with a placeholder to ensure they're included in groupby
    all_jobs_df = all_jobs_df.copy()
    scraped_df = scraped_df.copy()
    all_jobs_df[group_column] = all_jobs_df[group_column].fillna('Not Specified')
    scraped_df[group_column] = scraped_df[group_column].fillna('Not Specified')
    
    # Get total job counts from the full dataset
    total_stats = all_jobs_df.groupby(group_column).size().reset_index(name='Total Jobs')
    
    # For scraped data, count unique jobs (not questionnaire rows)
    # First deduplicate by usajobs_control_number to get one row per job
    scraped_jobs = scraped_df.drop_duplicates(subset='usajobs_control_number')
    
    # Get scraped counts and essay question counts
    scraped_stats = scraped_jobs.groupby(group_column).agg({
        'has_executive_order': 'sum',
        'usajobs_control_number': 'count'
    }).reset_index()
    scraped_stats.columns = [group_column, 'Jobs with Essay Question', 'Jobs with Questionnaires']
    
    # Merge the stats
    stats = pd.merge(total_stats, scraped_stats, on=group_column, how='left')
    stats = stats.fillna(0)
    
    # Use provided column name or default to the group column name
    display_name = column_name if column_name else group_column
    stats.rename(columns={group_column: display_name}, inplace=True)
    
    # Calculate percentage (of jobs with questionnaires that have essay question)
    stats['% with Essay Question'] = stats.apply(
        lambda row: round(row['Jobs with Essay Question'] / row['Jobs with Questionnaires'] * 100, 1) 
        if row['Jobs with Questionnaires'] > 0 else 0, axis=1
    )
    
    # Convert to int for cleaner display
    stats['Total Jobs'] = stats['Total Jobs'].astype(int)
    stats['Jobs with Questionnaires'] = stats['Jobs with Questionnaires'].astype(int)
    stats['Jobs with Essay Question'] = stats['Jobs with Essay Question'].astype(int)
    
    # Reorder columns to match desired order
    stats = stats[[display_name, 'Total Jobs', 'Jobs with Questionnaires', 
                   'Jobs with Essay Question', '% with Essay Question']]
    
    if top_n:
        stats = stats.nlargest(top_n, 'Total Jobs')
    
    return stats.sort_values('Total Jobs', ascending=False).reset_index(drop=True)

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


def main():
    # Always regenerate the clean all jobs data to get the latest
    print("Generating clean all jobs data from latest parquet files...")
    import subprocess
    subprocess.run(['python3', 'generate_all_jobs_data.py'], check=True)
    
    # Load the clean all jobs data
    all_jobs_df = pd.read_csv('all_jobs_clean.csv')
    print(f"Total jobs loaded: {len(all_jobs_df):,}")
    
    # Load questionnaire links
    links_df = pd.read_csv(QUESTIONNAIRE_DIR / 'questionnaire_links.csv')
    print(f"\nLoaded {len(links_df):,} questionnaire links")
    
    # Check for the specific executive order question
    eo_mentions = check_executive_order_mentions()
    print(f"\nFound {len(eo_mentions):,} questionnaires with the essay question")
    
    # Get all scraped IDs
    scraped_ids = set()
    for txt_file in RAW_QUESTIONNAIRES_DIR.glob('*.txt'):
        file_id = txt_file.stem.split('_')[1]
        scraped_ids.add(file_id)
    
    # Add questionnaire ID and executive order flags
    links_df['questionnaire_id'] = links_df['questionnaire_url'].apply(lambda url: extract_questionnaire_id(url)[1])
    links_df['has_executive_order'] = links_df['questionnaire_id'].isin(eo_mentions)
    
    # Filter to only scraped questionnaires
    scraped_df = links_df[links_df['questionnaire_id'].isin(scraped_ids)].copy()
    
    # Keep track of the original scraped dataframe for later use
    scraped_df_all = scraped_df.copy()
    
    # IMPORTANT: Only count jobs that exist in the current all_jobs dataset
    # This ensures consistency across all aggregations
    scraped_df_in_current = scraped_df[scraped_df['usajobs_control_number'].isin(all_jobs_df['usajobs_control_number'])].copy()
    
    # Update location, grade, and other fields from all_jobs_df to ensure consistency
    # This is important because job details might have changed between when the questionnaire was scraped
    # and the current job data
    job_info_cols = ['position_location', 'grade_code', 'occupation_series', 'occupation_name', 
                     'service_type', 'hiring_agency']
    all_jobs_info = all_jobs_df[['usajobs_control_number'] + job_info_cols].copy()
    
    # Drop the old columns from scraped_df_in_current and merge with authoritative data
    scraped_df_in_current = scraped_df_in_current.drop(columns=job_info_cols, errors='ignore')
    scraped_df_in_current = pd.merge(scraped_df_in_current, all_jobs_info, on='usajobs_control_number', how='left')
    
    # Deduplicate based on usajobs_control_number to avoid counting the same job multiple times
    # Keep the first occurrence of each control number
    original_count = len(scraped_df)
    scraped_df_in_current_dedup = scraped_df_in_current.drop_duplicates(subset='usajobs_control_number', keep='first')
    duplicate_count = len(scraped_df_in_current) - len(scraped_df_in_current_dedup)
    
    print(f"\nTotal questionnaire links scraped: {original_count:,}")
    print(f"Jobs in current dataset with questionnaires: {len(scraped_df_in_current_dedup):,}")
    print(f"Jobs with executive order mentions: {scraped_df_in_current_dedup['has_executive_order'].sum():,}")
    
    # Use the filtered dataset for all analysis
    scraped_df = scraped_df_in_current_dedup
    
    # Calculate overall statistics based on unique jobs
    total_jobs_with_questionnaires = len(scraped_df)
    total_jobs_with_eo = int(scraped_df['has_executive_order'].sum())
    percentage_with_eo = (total_jobs_with_eo / total_jobs_with_questionnaires * 100) if total_jobs_with_questionnaires > 0 else 0
    
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
            'total_jobs': len(all_jobs_df),
            'total_jobs_with_questionnaires': total_jobs_with_questionnaires,
            'total_jobs_with_eo': total_jobs_with_eo,
            'percentage_with_eo': round(percentage_with_eo, 1),
            'data_coverage': data_coverage
        }
    }
    
    # Add grade_level column to scraped dataframe (using grade_code as-is)
    if 'grade_code' in links_df.columns:
        links_df['grade_level'] = links_df['grade_code'].fillna('Not Specified')
        scraped_df['grade_level'] = scraped_df['grade_code'].fillna('Not Specified')
        scraped_df_in_current['grade_level'] = scraped_df_in_current['grade_code'].fillna('Not Specified')
    
    # Add occupation_full column to both dataframes - pad occupation series to 4 digits
    links_df['occupation_full'] = links_df['occupation_series'].astype(str).str.zfill(4) + ' - ' + links_df['occupation_name'].fillna('Unknown')
    scraped_df['occupation_full'] = scraped_df['occupation_series'].astype(str).str.zfill(4) + ' - ' + scraped_df['occupation_name'].fillna('Unknown')
    scraped_df_in_current['occupation_full'] = scraped_df_in_current['occupation_series'].astype(str).str.zfill(4) + ' - ' + scraped_df_in_current['occupation_name'].fillna('Unknown')
    
    # Service Type Analysis
    if 'service_type' in scraped_df.columns and 'service_type' in all_jobs_df.columns:
        service_stats = calculate_eo_stats(all_jobs_df, scraped_df, 'service_type', column_name='Service Type')
        analysis_data['service_analysis'] = service_stats.to_dict('records')
    else:
        analysis_data['service_analysis'] = []
    
    # Grade Level Analysis
    if 'grade_level' in scraped_df.columns and 'grade_level' in all_jobs_df.columns:
        grade_stats = calculate_eo_stats(all_jobs_df, scraped_df, 'grade_level', top_n=None, column_name='Grade Level')
        analysis_data['grade_analysis'] = grade_stats.to_dict('records')
    else:
        analysis_data['grade_analysis'] = []
    
    # Location Analysis
    if 'position_location' in scraped_df.columns and 'position_location' in all_jobs_df.columns:
        location_stats = calculate_eo_stats(all_jobs_df, scraped_df, 'position_location', top_n=None, column_name='Location')
        analysis_data['location_analysis'] = location_stats.to_dict('records')
    else:
        analysis_data['location_analysis'] = []
    
    # Agency Analysis
    if 'hiring_agency' in all_jobs_df.columns:
        agency_stats = calculate_eo_stats(all_jobs_df, scraped_df, 'hiring_agency', top_n=None, column_name='Agency')
        analysis_data['agency_analysis'] = agency_stats.to_dict('records')
    else:
        analysis_data['agency_analysis'] = []
    
    # Occupation Analysis
    if 'occupation_full' in all_jobs_df.columns:
        occupation_stats = calculate_eo_stats(all_jobs_df, scraped_df, 'occupation_full', top_n=None, column_name='Occupation Series')
        analysis_data['occupation_analysis'] = occupation_stats.to_dict('records')
    else:
        analysis_data['occupation_analysis'] = []
    
    # Timeline Analysis - use all_jobs_df to be consistent with other analyses
    if 'position_open_date' in all_jobs_df.columns:
        all_jobs_df['open_date'] = pd.to_datetime(all_jobs_df['position_open_date'], format='mixed', errors='coerce')
        jobs_with_dates = all_jobs_df[all_jobs_df['open_date'].notna()].copy()
        
        # Get the scraped and EO status for each job
        jobs_with_dates['has_questionnaire'] = jobs_with_dates['usajobs_control_number'].isin(scraped_df_in_current['usajobs_control_number'])
        jobs_with_dates['has_executive_order'] = jobs_with_dates['usajobs_control_number'].isin(
            scraped_df[scraped_df['has_executive_order']]['usajobs_control_number']
        )
        
        # Extract date components
        jobs_with_dates['year'] = jobs_with_dates['open_date'].dt.year
        jobs_with_dates['month'] = jobs_with_dates['open_date'].dt.month
        jobs_with_dates['month_name'] = jobs_with_dates['open_date'].dt.strftime('%B %Y')
        jobs_with_dates['week_of_month'] = jobs_with_dates['open_date'].dt.day.apply(lambda d: (d-1)//7 + 1)
        
        # Group by month and week - count all jobs, those with questionnaires, and those with EO question
        weekly_stats = jobs_with_dates.groupby(['year', 'month', 'month_name', 'week_of_month']).agg({
            'usajobs_control_number': 'count',  # Total jobs
            'has_questionnaire': 'sum',  # Jobs with questionnaires
            'has_executive_order': 'sum'  # Jobs with EO question
        }).reset_index()
        weekly_stats.columns = ['year', 'month', 'month_name', 'week_of_month', 'total_jobs', 'has_questionnaire', 'has_executive_order']
        
        # Calculate percentage of jobs with questionnaires that have EO question
        weekly_stats['percentage'] = weekly_stats.apply(
            lambda row: round(row['has_executive_order'] / row['has_questionnaire'] * 100, 1) if row['has_questionnaire'] > 0 else None,
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
            'total_jobs': 'sum',
            'has_questionnaire': 'sum',
            'has_executive_order': 'sum'
        }).reset_index()
        # Monthly percentage = EO jobs / questionnaire jobs (to match other analyses)
        monthly_totals['monthly_percentage'] = monthly_totals.apply(
            lambda row: round(row['has_executive_order'] / row['has_questionnaire'] * 100, 1) if row['has_questionnaire'] > 0 else 0,
            axis=1
        )
        
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
                'Total Jobs': int(month_totals['total_jobs']),
                'Jobs with Questionnaires': int(month_totals['has_questionnaire']),
                'Jobs with Essay Question': int(month_totals['has_executive_order'])
            }
            timeline_data.append(timeline_entry)
        
        analysis_data['timeline_analysis'] = timeline_data
    else:
        analysis_data['timeline_analysis'] = []
    
    # Job Postings - Include ALL jobs with questionnaire status
    # Start with all jobs
    all_jobs_for_display = all_jobs_df.copy()
    
    # Create a mapping of jobs with questionnaires
    jobs_with_questionnaires = set(links_df['usajobs_control_number'])
    jobs_with_scraped = set(scraped_df_all['usajobs_control_number'])
    jobs_with_eo = set(scraped_df[scraped_df['has_executive_order']]['usajobs_control_number'])
    
    # Add questionnaire status to all jobs
    all_jobs_for_display['has_questionnaire'] = all_jobs_for_display['usajobs_control_number'].isin(jobs_with_questionnaires)
    all_jobs_for_display['questionnaire_scraped'] = all_jobs_for_display['usajobs_control_number'].isin(jobs_with_scraped)
    all_jobs_for_display['has_eo_question'] = all_jobs_for_display['usajobs_control_number'].isin(jobs_with_eo)
    
    # Create questionnaire status column
    all_jobs_for_display['questionnaire_status'] = 'No questionnaire'
    all_jobs_for_display.loc[all_jobs_for_display['has_questionnaire'], 'questionnaire_status'] = 'Has questionnaire (not scraped)'
    all_jobs_for_display.loc[all_jobs_for_display['questionnaire_scraped'], 'questionnaire_status'] = 'Questionnaire without EO question'
    all_jobs_for_display.loc[all_jobs_for_display['has_eo_question'], 'questionnaire_status'] = 'Questionnaire with EO question'
    
    # Get questionnaire URLs for jobs that have them
    questionnaire_urls = links_df.drop_duplicates('usajobs_control_number')[['usajobs_control_number', 'questionnaire_url']]
    all_jobs_for_display = pd.merge(all_jobs_for_display, questionnaire_urls, on='usajobs_control_number', how='left')
    
    # Format dates
    all_jobs_for_display['open_date'] = pd.to_datetime(all_jobs_for_display['position_open_date'], format='mixed', errors='coerce').dt.strftime('%m/%d/%Y')
    all_jobs_for_display['close_date'] = pd.to_datetime(all_jobs_for_display['position_close_date'], format='mixed', errors='coerce').dt.strftime('%m/%d/%Y')
    
    # Create occupation display - pad occupation series to 4 digits
    all_jobs_for_display['occupation'] = all_jobs_for_display['occupation_series'].astype(str).str.zfill(4) + ' - ' + all_jobs_for_display['occupation_name'].fillna('Unknown')
    
    # Prepare job postings data for ALL jobs
    job_postings = []
    for _, job in all_jobs_for_display.iterrows():
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
            'questionnaire_status': job['questionnaire_status'],
            'questionnaire_link': transform_monster_url(job['questionnaire_url']) if pd.notna(job.get('questionnaire_url')) else ''
        }
        job_postings.append(posting)
    
    analysis_data['job_postings'] = job_postings
    
    print(f"\nJob postings by status:")
    print(all_jobs_for_display['questionnaire_status'].value_counts())
    
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
    with open('analysis/analysis_data.json', 'w') as f:
        json.dump(analysis_data, f, indent=2, default=str)
    
    print(f"\nAnalysis data written to analysis/analysis_data.json")
    print(f"Total data points: {sum(len(v) if isinstance(v, list) else 1 for v in analysis_data.values())}")

if __name__ == '__main__':
    main()