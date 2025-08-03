#!/usr/bin/env python3
"""
Extract job status data for visualization

This script extracts job data from the last month, focusing on:
1. All cancelled jobs with detailed information
2. Daily status distribution for visualization
"""

import pandas as pd
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

# Define paths
BASE_DIR = Path('..')
DATA_DIR = BASE_DIR / 'data'
OUTPUT_DIR = Path('.')

def normalize_openings(value):
    """Normalize openings value - uppercase text, remove leading zeros from numbers"""
    if not value:
        return ''
    
    value_str = str(value).strip()
    
    # Check if it's a number
    try:
        num = int(value_str)
        return str(num)  # This removes leading zeros
    except ValueError:
        # It's text, return uppercase
        return value_str.upper()

def get_tracking_date_range():
    """Get date range from January 1, 2025 to latest data available"""
    start_date = datetime(2025, 1, 1)
    # We'll determine end date from the data itself
    return start_date

def load_recent_jobs():
    """Load jobs from January 1, 2025 onwards"""
    start_date = get_tracking_date_range()
    
    # Load 2025 data
    all_jobs = []
    file_path = DATA_DIR / 'historical_jobs_2025.parquet'
    if file_path.exists():
        df = pd.read_parquet(file_path)
        all_jobs.append(df)
    
    # Combine and filter
    if all_jobs:
        combined_df = pd.concat(all_jobs, ignore_index=True)
        combined_df['positionOpenDate'] = pd.to_datetime(combined_df['positionOpenDate'])
        
        # Filter from start date onwards
        recent_jobs = combined_df[combined_df['positionOpenDate'] >= start_date].copy()
        
        # Get actual date range
        end_date = recent_jobs['positionOpenDate'].max()
        print(f"Loading jobs from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"Found {len(recent_jobs):,} jobs since June 1, 2025")
        
        return recent_jobs
    
    return pd.DataFrame()

def extract_cancelled_jobs(df):
    """Extract all cancelled jobs with detailed information"""
    cancelled_jobs = df[df['positionOpeningStatus'] == 'Job canceled'].copy()
    
    print(f"Found {len(cancelled_jobs):,} cancelled jobs")
    
    # Extract relevant fields for the table
    cancelled_data = []
    
    for _, job in cancelled_jobs.iterrows():
        control_number = job.get('usajobsControlNumber', '')
        
        # Extract location from PositionLocations
        location = ''
        location_count = 0
        if 'PositionLocations' in job and job['PositionLocations']:
            try:
                # Parse the JSON array if it's a string
                position_locations = job['PositionLocations']
                if isinstance(position_locations, str):
                    position_locations = json.loads(position_locations)
                
                if isinstance(position_locations, list) and len(position_locations) > 0:
                    location_count = len(position_locations)
                    locations_to_show = []
                    
                    # Show up to 3 locations
                    for i, loc in enumerate(position_locations[:3]):
                        city = loc.get('positionLocationCity', '')
                        state = loc.get('positionLocationState', '')
                        
                        loc_str = ''
                        if city and state:
                            # Check if state is already in city name
                            if state.lower() in city.lower():
                                loc_str = city
                            else:
                                loc_str = f"{city}, {state}"
                        elif city:
                            loc_str = city
                        elif state:
                            loc_str = state
                        
                        if loc_str:
                            locations_to_show.append(loc_str)
                    
                    # Join locations
                    if locations_to_show:
                        location = '; '.join(locations_to_show)
                        if location_count > 3:
                            location += f' (+{location_count - 3} more)'
            except:
                pass
        
        job_info = {
            'control_number': control_number,
            'usajobs_url': f'https://www.usajobs.gov/job/{control_number}' if control_number else '',
            'position_title': job.get('positionTitle', ''),
            'hiring_agency': job.get('hiringAgencyName', ''),
            'location': location,
            'open_date': pd.to_datetime(job.get('positionOpenDate')).strftime('%Y-%m-%d') if pd.notna(job.get('positionOpenDate')) else '',
            'close_date': pd.to_datetime(job.get('positionCloseDate')).strftime('%Y-%m-%d') if pd.notna(job.get('positionCloseDate')) else '',
            'pay_scale': job.get('payScale', ''),  # Add pay scale
            'min_grade': job.get('minimumGrade', ''),
            'max_grade': job.get('maximumGrade', ''),
            'min_salary': job.get('minimumSalary', 0),
            'max_salary': job.get('maximumSalary', 0),
            'total_openings': normalize_openings(job.get('totalOpenings', '')),
            'service_type': job.get('serviceType', ''),
            'work_schedule': job.get('workSchedule', ''),
            'telework_eligible': job.get('teleworkEligible', ''),
            'supervisory_status': job.get('supervisoryStatus', ''),
            'drug_test_required': job.get('drugTestRequired', ''),
            'security_clearance': job.get('securityClearance', ''),
            'days_open': (pd.to_datetime(job.get('positionCloseDate')) - pd.to_datetime(job.get('positionOpenDate'))).days if pd.notna(job.get('positionCloseDate')) and pd.notna(job.get('positionOpenDate')) else None
        }
        
        cancelled_data.append(job_info)
    
    return cancelled_data

def create_monthly_status_distribution(df):
    """Create monthly status distribution"""
    # Add month information
    df['month_start'] = df['positionOpenDate'].dt.to_period('M').dt.start_time
    
    # Get unique statuses
    all_statuses = df['positionOpeningStatus'].unique()
    
    # Create monthly breakdown
    monthly_status = []
    
    for month in sorted(df['month_start'].unique()):
        month_jobs = df[df['month_start'] == month]
        
        status_counts = month_jobs['positionOpeningStatus'].value_counts()
        
        month_data = {
            'month_start': month.strftime('%Y-%m-%d'),
            'month_label': month.strftime('%B %Y'),
            'total_jobs': len(month_jobs)
        }
        
        # Add count for each status
        for status in all_statuses:
            month_data[status] = int(status_counts.get(status, 0))
        
        monthly_status.append(month_data)
    
    return monthly_status

def aggregate_by_agency(df):
    """Aggregate status data by agency"""
    agency_stats = []
    
    for agency in df['hiringAgencyName'].unique():
        if pd.isna(agency):
            continue
            
        agency_jobs = df[df['hiringAgencyName'] == agency]
        
        status_counts = agency_jobs['positionOpeningStatus'].value_counts()
        
        agency_data = {
            'agency': agency,
            'total_jobs': len(agency_jobs),
            'cancelled': int(status_counts.get('Job canceled', 0)),
            'selected': int(status_counts.get('Candidate selected', 0)),
            'under_review': int(status_counts.get('Applications under review', 0)),
            'accepting': int(status_counts.get('Accepting applications', 0)),
            'closed': int(status_counts.get('Job closed', 0))
        }
        
        # Calculate cancellation rate
        if agency_data['total_jobs'] > 0:
            agency_data['cancellation_rate'] = round(agency_data['cancelled'] / agency_data['total_jobs'] * 100, 1)
        else:
            agency_data['cancellation_rate'] = 0
        
        agency_stats.append(agency_data)
    
    # Sort by total jobs
    agency_stats.sort(key=lambda x: x['total_jobs'], reverse=True)
    
    return agency_stats

def analyze_openings_distribution(cancelled_jobs):
    """Analyze the distribution of totalOpenings values"""
    openings_dist = {}
    
    for job in cancelled_jobs:
        opening_value = job.get('total_openings', 'Unknown')
        if opening_value:
            openings_dist[opening_value] = openings_dist.get(opening_value, 0) + 1
    
    return openings_dist

def main():
    # Load recent jobs
    df = load_recent_jobs()
    
    if df.empty:
        print("No jobs found")
        return
    
    # Extract cancelled jobs
    cancelled_jobs = extract_cancelled_jobs(df)
    
    # Create monthly status distribution
    monthly_status = create_monthly_status_distribution(df)
    
    # Aggregate by agency
    agency_stats = aggregate_by_agency(df)
    
    # Analyze openings distribution for cancelled jobs
    openings_dist = analyze_openings_distribution(cancelled_jobs)
    
    # Create summary statistics
    summary_stats = {
        'date_range': {
            'start': df['positionOpenDate'].min().strftime('%Y-%m-%d'),
            'end': datetime.now().strftime('%Y-%m-%d')  # Use today's date instead of max
        },
        'total_jobs': len(df),
        'status_breakdown': df['positionOpeningStatus'].value_counts().to_dict(),
        'cancelled_jobs_count': len(cancelled_jobs),
        'cancellation_rate': round(len(cancelled_jobs) / len(df) * 100, 1) if len(df) > 0 else 0,
        'openings_distribution': openings_dist,
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # Save all data
    output_data = {
        'summary': summary_stats,
        'cancelled_jobs': cancelled_jobs,
        'monthly_status': monthly_status,
        'agency_stats': agency_stats
    }
    
    # Write minified JSON file (smaller file size for web)
    output_path = OUTPUT_DIR / 'job_status_data.min.json'
    with open(output_path, 'w') as f:
        json.dump(output_data, f, separators=(',', ':'))
    
    print(f"\nData saved to {output_path}")
    print(f"Summary:")
    print(f"  - Total jobs: {summary_stats['total_jobs']:,}")
    print(f"  - Cancelled jobs: {summary_stats['cancelled_jobs_count']:,} ({summary_stats['cancellation_rate']}%)")
    print(f"  - Agencies tracked: {len(agency_stats)}")
    print(f"\nOpenings distribution for cancelled jobs:")
    for value, count in sorted(openings_dist.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  - '{value}': {count} jobs")

if __name__ == "__main__":
    main()