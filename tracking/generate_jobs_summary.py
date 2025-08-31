#!/usr/bin/env python3
"""
Generate job listings summary comparing Feb-Aug 2024 vs 2025.
Creates CSV file in the same format as the budget obligation summary.
"""

import pandas as pd
import json
from pathlib import Path
import numpy as np

def normalize_appointment_type(appt_type):
    """Normalize appointment type to handle case variations"""
    if pd.isna(appt_type):
        return 'All'
    # Convert to title case for consistency
    return appt_type.strip().title()

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
    all_control_numbers = set()
    all_data = []
    
    # Load historical data first
    try:
        df_hist = pd.read_parquet(f'../data/historical_jobs_{year}.parquet')
        print(f"    Historical {year}: {len(df_hist):,} jobs")
        all_control_numbers.update(df_hist['usajobsControlNumber'])
        all_data.append(df_hist)
    except FileNotFoundError:
        print(f"    No historical data for {year}")
    
    # Load current data and deduplicate
    try:
        df_curr = pd.read_parquet(f'../data/current_jobs_{year}.parquet')
        print(f"    Current {year}: {len(df_curr):,} jobs")
        
        # Only keep jobs not already in historical
        df_curr_unique = df_curr[~df_curr['usajobsControlNumber'].isin(all_control_numbers)]
        print(f"    Current {year} unique: {len(df_curr_unique):,} jobs (after deduplication)")
        
        if len(df_curr_unique) > 0:
            all_data.append(df_curr_unique)
    except FileNotFoundError:
        print(f"    No current data for {year}")
    
    if not all_data:
        return pd.DataFrame()
    
    # Combine data and verify no duplicates
    df = pd.concat(all_data, ignore_index=True)
    
    # Double-check for duplicates
    dup_count = df['usajobsControlNumber'].duplicated().sum()
    if dup_count > 0:
        print(f"    WARNING: Found {dup_count} duplicate control numbers after merge!")
        df = df.drop_duplicates(subset=['usajobsControlNumber'], keep='first')
    
    # Convert dates and filter by months (Feb-Aug)
    df['positionOpenDate'] = pd.to_datetime(df['positionOpenDate'], format='mixed')
    df['month'] = df['positionOpenDate'].dt.month
    df = df[df['month'].isin([2, 3, 4, 5, 6, 7, 8])].copy()
    
    # Extract additional fields
    df['occupation_series'] = df['JobCategories'].apply(extract_occupation_series)
    # Normalize appointment type for case-insensitive matching
    df['appointmentType'] = df['appointmentType'].apply(normalize_appointment_type)
    
    return df

def generate_job_listings_summary():
    """Create job listings summary comparing 2024 vs 2025"""
    
    print("Loading 2024 data...")
    df_2024 = load_year_data(2024)
    print(f"  Found {len(df_2024):,} listings for Feb-Aug 2024")
    
    print("Loading 2025 data...")
    df_2025 = load_year_data(2025)
    print(f"  Found {len(df_2025):,} listings for Feb-Aug 2025")
    
    # Count by combination for each year (dropna=False to keep all records)
    counts_2024 = df_2024.groupby([
        'hiringDepartmentName', 'hiringAgencyName',
        'appointmentType', 'occupation_series'
    ], dropna=False).size().to_dict()
    
    counts_2025 = df_2025.groupby([
        'hiringDepartmentName', 'hiringAgencyName',
        'appointmentType', 'occupation_series'
    ], dropna=False).size().to_dict()
    
    # Get all unique combinations from both years
    all_combinations = set(counts_2024.keys()) | set(counts_2025.keys())
    
    # Build summary rows
    summary_rows = []
    for combo in all_combinations:
        dept, agency, appt_type, occupation = combo
        
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
            'Occupation_Series': occupation if pd.notna(occupation) else 'Unknown',
            'Listings_2024': str(count_2024),  # No commas in CSV
            'Listings_2025': str(count_2025),  # No commas in CSV
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
    
    # Save as JSON for easier JavaScript consumption
    summary_df.to_json('data/job_listings_summary.json', orient='records', indent=2)
    print(f"\nWrote {len(summary_df):,} rows to data/job_listings_summary.json")
    
    # Verify totals match
    total_2024_csv = summary_df['listings2024Value'].sum()
    total_2025_csv = summary_df['listings2025Value'].sum()
    print(f"\nCSV totals:")
    print(f"  2024: {total_2024_csv:,} (expected {len(df_2024):,})")
    print(f"  2025: {total_2025_csv:,} (expected {len(df_2025):,})")
    
    if total_2024_csv != len(df_2024) or total_2025_csv != len(df_2025):
        print("\nWARNING: Totals don't match! Investigating...")
        # Check what's being lost
        missing_2024 = len(df_2024) - total_2024_csv
        missing_2025 = len(df_2025) - total_2025_csv
        print(f"  Missing 2024: {missing_2024:,}")
        print(f"  Missing 2025: {missing_2025:,}")
    
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