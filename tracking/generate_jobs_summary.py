#!/usr/bin/env python3
"""
Generate job listings summary comparing Feb-Aug 2024 vs 2025.
Creates CSV file in the same format as the budget obligation summary.
"""

import pandas as pd
import json
from pathlib import Path
import numpy as np

def extract_hiring_paths(hiring_paths_json):
    """Extract hiring paths from JSON field"""
    try:
        if pd.isna(hiring_paths_json):
            return 'All'
        paths = json.loads(hiring_paths_json)
        path_list = [p.get('hiringPath', '') for p in paths if 'hiringPath' in p]
        return ', '.join(path_list) if path_list else 'All'
    except:
        return 'All'

def extract_occupation_series(job_categories_json):
    """Extract occupational series from JobCategories JSON field"""
    try:
        if pd.isna(job_categories_json):
            return 'Unknown'
        categories = json.loads(job_categories_json)
        if categories and len(categories) > 0 and 'series' in categories[0]:
            return categories[0]['series']
        return 'Unknown'
    except:
        return 'Unknown'

def load_year_data(year):
    """Load and combine historical and current data for a year"""
    all_data = []
    
    # Load historical data
    try:
        df_hist = pd.read_parquet(f'../data/historical_jobs_{year}.parquet')
        all_data.append(df_hist)
    except FileNotFoundError:
        pass
    
    # Load current data
    try:
        df_curr = pd.read_parquet(f'../data/current_jobs_{year}.parquet')
        # Deduplicate if needed
        if len(all_data) > 0:
            existing_ids = set(all_data[0]['usajobsControlNumber'])
            df_curr = df_curr[~df_curr['usajobsControlNumber'].isin(existing_ids)]
        all_data.append(df_curr)
    except FileNotFoundError:
        pass
    
    if not all_data:
        return pd.DataFrame()
    
    # Combine data
    df = pd.concat(all_data, ignore_index=True)
    
    # Convert dates and filter by months (Feb-Aug)
    df['positionOpenDate'] = pd.to_datetime(df['positionOpenDate'], format='mixed')
    df['month'] = df['positionOpenDate'].dt.month
    df = df[df['month'].isin([2, 3, 4, 5, 6, 7, 8])].copy()
    
    # Extract additional fields
    df['hiring_paths'] = df['HiringPaths'].apply(extract_hiring_paths)
    df['occupation_series'] = df['JobCategories'].apply(extract_occupation_series)
    
    return df

def generate_job_listings_summary():
    """Create job listings summary comparing 2024 vs 2025"""
    
    print("Loading 2024 data...")
    df_2024 = load_year_data(2024)
    print(f"  Found {len(df_2024):,} listings for Feb-Aug 2024")
    
    print("Loading 2025 data...")
    df_2025 = load_year_data(2025)
    print(f"  Found {len(df_2025):,} listings for Feb-Aug 2025")
    
    # Create a combined view with all unique combinations
    all_combinations = set()
    
    # Add all 2024 combinations
    for _, row in df_2024.iterrows():
        key = (
            row['hiringDepartmentName'],
            row['hiringAgencyName'],
            row['appointmentType'],
            row['workSchedule'],
            row['hiring_paths']
        )
        all_combinations.add(key)
    
    # Add all 2025 combinations
    for _, row in df_2025.iterrows():
        key = (
            row['hiringDepartmentName'],
            row['hiringAgencyName'],
            row['appointmentType'],
            row['workSchedule'],
            row['hiring_paths']
        )
        all_combinations.add(key)
    
    # Count by combination for each year
    counts_2024 = df_2024.groupby([
        'hiringDepartmentName', 'hiringAgencyName', 'appointmentType', 
        'workSchedule', 'hiring_paths'
    ]).size().to_dict()
    
    counts_2025 = df_2025.groupby([
        'hiringDepartmentName', 'hiringAgencyName', 'appointmentType', 
        'workSchedule', 'hiring_paths'
    ]).size().to_dict()
    
    # Build summary rows
    summary_rows = []
    for combo in all_combinations:
        dept, agency, appt_type, schedule, paths = combo
        
        # Skip if all values are missing
        if pd.isna(dept) and pd.isna(agency):
            continue
            
        count_2024 = counts_2024.get(combo, 0)
        count_2025 = counts_2025.get(combo, 0)
        
        # Calculate percentage (2025 as % of 2024)
        if count_2024 > 0:
            percentage = (count_2025 / count_2024) * 100
        else:
            percentage = 0.0 if count_2025 == 0 else 100.0
        
        summary_rows.append({
            'Department': dept if pd.notna(dept) else 'Unknown',
            'Agency': agency if pd.notna(agency) else 'Unknown',
            'Appointment_Type': appt_type if pd.notna(appt_type) else 'All',
            'Work_Schedule': schedule if pd.notna(schedule) else 'All',
            'Hiring_Paths': paths if pd.notna(paths) else 'All',
            'Listings_2024': f"{count_2024:,}",
            'Listings_2025': f"{count_2025:,}",
            'Percentage_2025_of_2024': f"{percentage:.1f}%",
            'listings2024Value': count_2024,
            'listings2025Value': count_2025,
            'percentageValue': percentage
        })
    
    # Create DataFrame
    summary_df = pd.DataFrame(summary_rows)
    
    # Sort by 2024 listings (descending)
    summary_df = summary_df.sort_values('listings2024Value', ascending=False)
    
    # Create data directory if it doesn't exist
    Path('data').mkdir(exist_ok=True)
    
    # Save CSV (with all columns including numeric values for JavaScript)
    summary_df.to_csv('data/job_listings_summary.csv', index=False)
    print(f"\nWrote {len(summary_df):,} rows to data/job_listings_summary.csv")
    
    # Also save as JSON for reference
    summary_stats = {
        'total_2024': len(df_2024),
        'total_2025': len(df_2025),
        'unique_departments': len(summary_df['Department'].unique()),
        'unique_agencies': len(summary_df['Agency'].unique()),
        'overall_percentage': (len(df_2025) / len(df_2024) * 100) if len(df_2024) > 0 else 0
    }
    
    with open('data/job_listings_stats.json', 'w') as f:
        json.dump(summary_stats, f, indent=2)
    
    print(f"\nOverall statistics:")
    print(f"  Total 2024 listings: {summary_stats['total_2024']:,}")
    print(f"  Total 2025 listings: {summary_stats['total_2025']:,}")
    print(f"  2025 as % of 2024: {summary_stats['overall_percentage']:.1f}%")
    print(f"  Unique departments: {summary_stats['unique_departments']}")
    print(f"  Unique agencies: {summary_stats['unique_agencies']}")

if __name__ == "__main__":
    generate_job_listings_summary()