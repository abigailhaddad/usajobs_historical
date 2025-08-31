#!/usr/bin/env python3
"""
Generate job listings summary comparing same period in previous year vs current year.
Dynamically adjusts date range based on current date.
Outputs to public/data/ folder for Netlify deployment.
"""

import pandas as pd
import json
from pathlib import Path
import numpy as np
from datetime import datetime, timedelta

def normalize_appointment_type(appt_type):
    """Normalize appointment type to handle case variations"""
    if pd.isna(appt_type):
        return 'All'
    # Convert to title case for consistency
    return appt_type.strip().title()

def extract_occupation_series(job_categories_json):
    """Extract occupational series from JobCategories JSON field"""
    if pd.isna(job_categories_json) or not job_categories_json:
        return 'Unknown'
    
    try:
        # Parse JSON string if it's a string
        if isinstance(job_categories_json, str):
            categories = json.loads(job_categories_json)
        else:
            categories = job_categories_json
            
        # Extract series codes
        if isinstance(categories, list) and len(categories) > 0:
            # Take the first series if multiple
            if 'series' in categories[0]:
                return categories[0]['series']
    except:
        pass
    
    return 'Unknown'

def load_year_data(year, start_date, end_date):
    """Load data for a specific year and date range"""
    all_data = []
    
    # Define data directory relative to script location
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'data'
    
    # Load historical data
    historical_file = data_dir / f'historical_jobs_{year}.parquet'
    if historical_file.exists():
        print(f"    Historical {year}: ", end='')
        df_hist = pd.read_parquet(historical_file)
        print(f"{len(df_hist):,} jobs")
        all_data.append(df_hist)
    
    # Load current data
    current_file = data_dir / f'current_jobs_{year}.parquet'
    if current_file.exists():
        print(f"    Current {year}: ", end='')
        df_curr = pd.read_parquet(current_file)
        print(f"{len(df_curr):,} jobs")
        
        if all_data:
            # Deduplicate against historical data
            existing_ids = set(all_data[0]['usajobsControlNumber'])
            df_curr_unique = df_curr[~df_curr['usajobsControlNumber'].isin(existing_ids)]
            print(f"    Current {year} unique: {len(df_curr_unique):,} jobs (after deduplication)")
            if len(df_curr_unique) > 0:
                all_data.append(df_curr_unique)
        else:
            all_data.append(df_curr)
    
    if all_data:
        # Combine all data
        df_combined = pd.concat(all_data, ignore_index=True)
        
        # Convert dates - handle ISO format with timezone
        df_combined['positionOpenDate'] = pd.to_datetime(df_combined['positionOpenDate'], 
                                                         format='ISO8601', 
                                                         errors='coerce')
        
        # Filter to date range
        mask = (df_combined['positionOpenDate'] >= start_date) & \
               (df_combined['positionOpenDate'] <= end_date)
        df_filtered = df_combined[mask].copy()
        
        # Extract occupation series
        df_filtered['occupation_series'] = df_filtered['JobCategories'].apply(extract_occupation_series)
        
        # Normalize appointment type
        df_filtered['appointmentType'] = df_filtered['appointmentType'].apply(normalize_appointment_type)
        
        return df_filtered
    else:
        return pd.DataFrame()

def generate_summary():
    """Generate summary comparing same period last year vs this year"""
    
    # Calculate dynamic date ranges
    today = datetime.now()
    current_year = today.year
    previous_year = current_year - 1
    
    # Use February 1 as start date
    start_month = 2
    start_day = 1
    
    # Stop updating after December 31 - just use the last valid comparison
    if today.year > 2025:
        # After 2025, freeze the comparison at Feb-Dec 2024 vs Feb-Dec 2025
        print("Note: Data comparison frozen at 2024 vs 2025 after end of 2025")
        current_year = 2025
        previous_year = 2024
        end_date_current = datetime(2025, 12, 31)
        end_date_previous = datetime(2024, 12, 31)
    elif today.month >= start_month:
        # Normal operation during 2025
        end_date_current = today - timedelta(days=1)  # Yesterday to ensure complete data
        end_date_previous = datetime(previous_year, end_date_current.month, end_date_current.day)
    else:
        # If it's January, compare full previous years
        end_date_current = datetime(current_year - 1, 12, 31)
        end_date_previous = datetime(previous_year - 1, 12, 31)
        current_year = current_year - 1
        previous_year = previous_year - 1
    
    start_date_current = datetime(current_year, start_month, start_day)
    start_date_previous = datetime(previous_year, start_month, start_day)
    
    print(f"Comparing {previous_year} vs {current_year} data")
    print(f"Previous year: {start_date_previous.strftime('%B %d, %Y')} - {end_date_previous.strftime('%B %d, %Y')}")
    print(f"Current year: {start_date_current.strftime('%B %d, %Y')} - {end_date_current.strftime('%B %d, %Y')}")
    print()
    
    # Load data for both years
    print(f"Loading {previous_year} data...")
    df_previous = load_year_data(previous_year, start_date_previous, end_date_previous)
    print(f"  Found {len(df_previous):,} listings for selected period in {previous_year}")
    
    print(f"Loading {current_year} data...")
    df_current = load_year_data(current_year, start_date_current, end_date_current)
    print(f"  Found {len(df_current):,} listings for selected period in {current_year}")
    
    # Count by combination for each year (dropna=False to keep all records)
    counts_previous = df_previous.groupby([
        'hiringDepartmentName', 'hiringAgencyName',
        'appointmentType', 'occupation_series'
    ], dropna=False).size().to_dict()
    
    counts_current = df_current.groupby([
        'hiringDepartmentName', 'hiringAgencyName',
        'appointmentType', 'occupation_series'
    ], dropna=False).size().to_dict()
    
    # Get all unique combinations from both years
    all_combinations = set(counts_previous.keys()) | set(counts_current.keys())
    
    # Build summary rows
    summary_rows = []
    for combo in all_combinations:
        dept, agency, appt_type, occupation = combo
        
        # Skip if all values are missing
        if pd.isna(dept) and pd.isna(agency):
            continue
            
        count_previous = counts_previous.get(combo, 0)
        count_current = counts_current.get(combo, 0)
        
        # Calculate percentage (current as % of previous)
        if count_previous > 0:
            percentage = (count_current / count_previous) * 100
        else:
            percentage = 0.0 if count_current == 0 else 100.0
        
        summary_rows.append({
            'Department': dept if pd.notna(dept) else 'Unknown',
            'Agency': agency if pd.notna(agency) else 'Unknown',
            'Appointment_Type': appt_type if pd.notna(appt_type) else 'All',
            'Occupation_Series': occupation if pd.notna(occupation) else 'Unknown',
            f'Listings_{previous_year}': str(count_previous),
            f'Listings_{current_year}': str(count_current),
            f'Percentage_{current_year}_of_{previous_year}': f"{percentage:.1f}%",
            'listingsPreviousValue': count_previous,
            'listingsCurrentValue': count_current,
            'percentageValue': percentage
        })
    
    # Create DataFrame
    summary_df = pd.DataFrame(summary_rows)
    
    # Sort by previous year count descending
    summary_df = summary_df.sort_values(f'listingsPreviousValue', ascending=False)
    
    # Define output directory - public/data
    script_dir = Path(__file__).parent
    output_dir = script_dir / 'public' / 'data'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save as JSON (primary format for the app)
    json_file = output_dir / 'job_listings_summary.json'
    
    # Convert to records format for JSON
    json_data = summary_df.to_dict('records')
    
    # Update the JSON structure to use generic keys that the JS expects
    for record in json_data:
        # Rename year-specific fields to generic ones
        record['Listings_2024'] = record.pop(f'Listings_{previous_year}')
        record['Listings_2025'] = record.pop(f'Listings_{current_year}')
        record['Percentage_2025_of_2024'] = record.pop(f'Percentage_{current_year}_of_{previous_year}')
        record['listings2024Value'] = record.pop('listingsPreviousValue')
        record['listings2025Value'] = record.pop('listingsCurrentValue')
    
    with open(json_file, 'w') as f:
        json.dump(json_data, f, indent=2)
    
    print(f"\nWrote {len(summary_df):,} rows to {json_file}")
    
    # Also save as CSV for reference
    csv_file = output_dir / 'job_listings_summary.csv'
    
    # Select columns for CSV in specific order
    csv_columns = [
        'Department', 'Agency', 'Appointment_Type', 'Occupation_Series',
        f'Listings_{previous_year}', f'Listings_{current_year}', 
        f'Percentage_{current_year}_of_{previous_year}'
    ]
    summary_df[csv_columns].to_csv(csv_file, index=False)
    
    # Calculate totals for verification
    total_previous = summary_df['listingsPreviousValue'].sum()
    total_current = summary_df['listingsCurrentValue'].sum()
    
    print(f"\nCSV totals:")
    print(f"  {previous_year}: {total_previous:,} (expected {len(df_previous):,})")
    print(f"  {current_year}: {total_current:,} (expected {len(df_current):,})")
    
    # Save summary statistics
    overall_percentage = (total_current / total_previous * 100) if total_previous > 0 else 0
    
    stats = {
        'generated_at': datetime.now().isoformat(),
        'date_range': {
            'previous_year': {
                'year': previous_year,
                'start': start_date_previous.strftime('%Y-%m-%d'),
                'end': end_date_previous.strftime('%Y-%m-%d'),
                'display': f"{start_date_previous.strftime('%B %d')} - {end_date_previous.strftime('%B %d, %Y')}"
            },
            'current_year': {
                'year': current_year,
                'start': start_date_current.strftime('%Y-%m-%d'),
                'end': end_date_current.strftime('%Y-%m-%d'),
                'display': f"{start_date_current.strftime('%B %d')} - {end_date_current.strftime('%B %d, %Y')}"
            }
        },
        'totals': {
            'previous_year': int(total_previous),
            'current_year': int(total_current),
            'percentage': round(overall_percentage, 1)
        },
        'unique_counts': {
            'departments': int(summary_df['Department'].nunique()),
            'agencies': int(summary_df['Agency'].nunique())
        }
    }
    
    stats_file = output_dir / 'job_listings_stats.json'
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    
    print(f"\nOverall statistics:")
    print(f"  Total {previous_year} listings: {total_previous:,}")
    print(f"  Total {current_year} listings: {total_current:,}")
    print(f"  {current_year} as % of {previous_year}: {overall_percentage:.1f}%")
    print(f"  Unique departments: {stats['unique_counts']['departments']}")
    print(f"  Unique agencies: {stats['unique_counts']['agencies']}")

if __name__ == '__main__':
    generate_summary()