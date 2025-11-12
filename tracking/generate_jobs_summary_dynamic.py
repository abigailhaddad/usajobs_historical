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

# Initialize occupation series mapping (will be loaded after API fetch)
occupation_series_map = {}

# Initialize appointment types mapping (will be loaded after API fetch) 
appointment_types_map = {}

# Load agency to department mapping
agency_to_dept_map = {}
try:
    with open('agency_to_department_mapping.json', 'r') as f:
        agency_to_dept_map = json.load(f)
except:
    print("Warning: Could not load agency to department mapping")

# Load agency code to name mapping
agency_code_to_name_map = {}
try:
    with open('agency_code_to_name_mapping.json', 'r') as f:
        agency_code_to_name_map = json.load(f)
except:
    print("Warning: Could not load agency code to name mapping")

def fill_missing_department(row):
    """Fill missing department using agency mapping"""
    if pd.isna(row['hiringDepartmentName']) and pd.notna(row['hiringAgencyName']):
        agency = row['hiringAgencyName']
        if agency in agency_to_dept_map:
            return agency_to_dept_map[agency]['department']
    return row['hiringDepartmentName']

def fill_missing_agency(row):
    """Fill missing agency using agency code or department"""
    if pd.isna(row['hiringAgencyName']):
        # First try to use agency code
        agency_code = row.get('hiringAgencyCode')
        if agency_code and agency_code in agency_code_to_name_map:
            return agency_code_to_name_map[agency_code]
        # Otherwise use department if available
        elif pd.notna(row['hiringDepartmentName']):
            return row['hiringDepartmentName']
    return row['hiringAgencyName']

def normalize_appointment_type(appt_type):
    """Normalize appointment type using API mapping and handle variations"""
    global appointment_types_map
    
    if pd.isna(appt_type):
        return 'All'
    
    # Convert to string and strip whitespace
    appt_str = str(appt_type).strip()
    
    # Check if it's a numeric code that maps to an API value
    if appt_str in appointment_types_map:
        return appointment_types_map[appt_str]
    
    # Handle common variations and clean up text
    appt_clean = appt_str.lower()
    if 'telework' in appt_clean and 'eligible' in appt_clean:
        return 'Telework Eligible'
    elif 'temporary promotion' in appt_clean:
        return 'Temporary Promotion'
    elif 'recent graduate' in appt_clean:
        return 'Recent Graduates'
    elif 'presidential management' in appt_clean:
        return 'Presidential Management Fellows'
    elif 'ictap' in appt_clean:
        return 'ICTAP Only'
    elif 'agency employees' in appt_clean:
        return 'Agency Employees Only'
    
    # Convert to title case for consistency
    return appt_str.title()

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

def extract_all_occupation_series(job_categories_json):
    """Extract all occupational series from JobCategories JSON field"""
    if pd.isna(job_categories_json) or not job_categories_json:
        return []
    
    try:
        # Parse JSON string if it's a string
        if isinstance(job_categories_json, str):
            categories = json.loads(job_categories_json)
        else:
            categories = job_categories_json
            
        # Extract all series codes
        if isinstance(categories, list):
            return [cat.get('series', '') for cat in categories if cat.get('series')]
    except:
        pass
    
    return []

def extract_occupation_series_with_names(job_categories_json):
    """Extract occupation series with names for display"""
    if pd.isna(job_categories_json) or not job_categories_json:
        return []
    
    try:
        # Parse JSON string if it's a string
        if isinstance(job_categories_json, str):
            categories = json.loads(job_categories_json)
        else:
            categories = job_categories_json
            
        # Extract series with names
        result = []
        if isinstance(categories, list):
            for cat in categories:
                series = cat.get('series', '')
                if series:
                    # Look up name from mapping
                    name = occupation_series_map.get(series, occupation_series_map.get(series.lstrip('0'), ''))
                    if name:
                        # Format name properly (title case)
                        name = name.lower().title()
                        result.append(f"{series} - {name}")
                    else:
                        result.append(series)
        return result
    except:
        pass
    
    return []

def extract_hiring_paths(hiring_paths_json):
    """Extract hiring paths from HiringPaths JSON field"""
    if pd.isna(hiring_paths_json) or not hiring_paths_json:
        return []
    
    try:
        # Parse JSON string if it's a string
        if isinstance(hiring_paths_json, str):
            paths = json.loads(hiring_paths_json)
        else:
            paths = hiring_paths_json
            
        # Extract hiring path descriptions
        if isinstance(paths, list):
            return [p.get('hiringPath', '') for p in paths if p.get('hiringPath')]
    except:
        pass
    
    return []

def load_year_data(year, start_date, end_date):
    """Load data for a specific year and date range"""
    all_data = []
    
    # Define data directory relative to script location
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / 'data'
    
    # Load current data FIRST to prefer it over historical
    current_file = data_dir / f'current_jobs_{year}.parquet'
    if current_file.exists():
        print(f"    Current {year}: ", end='')
        df_curr = pd.read_parquet(current_file)
        print(f"{len(df_curr):,} jobs")
        all_data.append(df_curr)
    
    # Load historical data
    historical_file = data_dir / f'historical_jobs_{year}.parquet'
    if historical_file.exists():
        print(f"    Historical {year}: ", end='')
        df_hist = pd.read_parquet(historical_file)
        print(f"{len(df_hist):,} jobs")
        
        if all_data:
            # Deduplicate against current data (prefer current over historical)
            existing_ids = set(all_data[0]['usajobsControlNumber'])
            df_hist_unique = df_hist[~df_hist['usajobsControlNumber'].isin(existing_ids)]
            print(f"    Historical {year} unique: {len(df_hist_unique):,} jobs (after deduplication)")
            if len(df_hist_unique) > 0:
                all_data.append(df_hist_unique)
        else:
            all_data.append(df_hist)
    
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
        
        # Fill missing departments using agency mapping
        df_filtered['hiringDepartmentName'] = df_filtered.apply(fill_missing_department, axis=1)
        
        # Fill missing agencies using department
        df_filtered['hiringAgencyName'] = df_filtered.apply(fill_missing_agency, axis=1)
        
        # Replace any remaining NaN departments with 'Unknown'
        df_filtered['hiringDepartmentName'] = df_filtered['hiringDepartmentName'].fillna('Unknown')
        
        # Extract occupation series
        df_filtered['occupation_series'] = df_filtered['JobCategories'].apply(extract_occupation_series)
        
        # Normalize appointment type
        df_filtered['appointmentType'] = df_filtered['appointmentType'].apply(normalize_appointment_type)
        
        return df_filtered
    else:
        return pd.DataFrame()

def load_occupation_series_mapping():
    """Load occupation series mapping from API file or fallback to data file"""
    global occupation_series_map
    
    # Define script directory to match other file loading patterns
    script_dir = Path(__file__).parent
    
    try:
        api_file = script_dir / 'occupation_series_from_api.json'
        with open(api_file, 'r') as f:
            occupation_series_map = json.load(f)
            print("Using official occupation series mapping from USAJobs API")
    except:
        try:
            fallback_file = script_dir / 'public' / 'occupation_series_from_data.json'
            with open(fallback_file, 'r') as f:
                occupation_series_map = json.load(f)
            print("Warning: Using fallback occupation series mapping from data")
        except:
            print("Error: Could not load occupation series mapping from either API or data file")

def load_appointment_types_mapping():
    """Load appointment types mapping from API file"""
    global appointment_types_map
    
    # Define script directory to match other file loading patterns
    script_dir = Path(__file__).parent
    
    try:
        api_file = script_dir / 'appointment_types_from_api.json'
        with open(api_file, 'r') as f:
            appointment_types_map = json.load(f)
            print("Using official appointment types mapping from USAJobs API")
    except:
        print("Warning: Could not load appointment types mapping from API file")

def sanitize_filename(name):
    """Convert department name to safe filename"""
    # Replace spaces and special characters
    safe_name = name.replace(' ', '_').replace('/', '_').replace('&', 'and')
    # Remove other problematic characters
    safe_name = ''.join(c for c in safe_name if c.isalnum() or c in ('_', '-'))
    return safe_name.lower()

def generate_department_raw_jobs(df_previous, df_current, department, previous_year, current_year):
    """Generate raw job listings for a specific department"""
    # Filter by department
    dept_previous = df_previous[df_previous['hiringDepartmentName'] == department].copy()
    dept_current = df_current[df_current['hiringDepartmentName'] == department].copy()
    
    # Add year column
    dept_previous['year'] = previous_year
    dept_current['year'] = current_year
    
    # Combine data
    all_jobs = pd.concat([dept_previous, dept_current], ignore_index=True)
    
    # Extract the fields we need
    raw_jobs = []
    for _, job in all_jobs.iterrows():
        job_data = {
            'control_number': job.get('usajobsControlNumber', ''),
            'year': int(job['year']),
            'position_title': job.get('positionTitle', ''),
            'department': job.get('hiringDepartmentName', ''),
            'agency': job.get('hiringAgencyName', ''),
            'subagency': job.get('hiringSubelementName', ''),
            'appointment_type': job.get('appointmentType', ''),
            'occupation_series': extract_occupation_series_with_names(job.get('JobCategories', '')),
            'hiring_paths': extract_hiring_paths(job.get('HiringPaths', '')),
            'open_date': pd.to_datetime(job.get('positionOpenDate')).strftime('%Y-%m-%d') if pd.notna(job.get('positionOpenDate')) else '',
            'close_date': pd.to_datetime(job.get('positionCloseDate')).strftime('%Y-%m-%d') if pd.notna(job.get('positionCloseDate')) else '',
            'usajobs_url': f"https://www.usajobs.gov/job/{job.get('usajobsControlNumber', '')}"
        }
        raw_jobs.append(job_data)
    
    return raw_jobs

def generate_department_summary(df_previous, df_current, department, previous_year, current_year):
    """Generate summary WITH subagency for a specific department"""
    # Filter by department
    dept_previous = df_previous[df_previous['hiringDepartmentName'] == department]
    dept_current = df_current[df_current['hiringDepartmentName'] == department]
    
    # Count by combination including subagency
    counts_previous = dept_previous.groupby([
        'hiringDepartmentName', 'hiringAgencyName', 'hiringSubelementName',
        'appointmentType', 'occupation_series'
    ], dropna=False).size().to_dict()
    
    counts_current = dept_current.groupby([
        'hiringDepartmentName', 'hiringAgencyName', 'hiringSubelementName',
        'appointmentType', 'occupation_series'
    ], dropna=False).size().to_dict()
    
    # Get all unique combinations from both years
    all_combinations = set(counts_previous.keys()) | set(counts_current.keys())
    
    # Build summary rows
    summary_rows = []
    for combo in all_combinations:
        dept, agency, subelement, appt_type, occupation = combo
        
        count_previous = counts_previous.get(combo, 0)
        count_current = counts_current.get(combo, 0)
        
        # Calculate percentage
        if count_previous > 0:
            percentage = (count_current / count_previous) * 100
        else:
            percentage = 0.0 if count_current == 0 else 100.0
        
        summary_rows.append({
            'Department': dept if pd.notna(dept) else 'Unknown',
            'Agency': agency if pd.notna(agency) else 'Unknown',
            'Subagency': subelement if pd.notna(subelement) else 'Unknown',
            'Appointment_Type': appt_type if pd.notna(appt_type) else 'All',
            'Occupation_Series': occupation if pd.notna(occupation) else 'Unknown',
            f'Listings_{previous_year}': str(count_previous),
            f'Listings_{current_year}': str(count_current),
            f'Percentage_{current_year}_of_{previous_year}': f"{percentage:.1f}%",
            f'listings{previous_year}Value': count_previous,
            f'listings{current_year}Value': count_current,
            'percentageValue': percentage
        })
    
    return pd.DataFrame(summary_rows)

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
    
    # Create directories for department and raw jobs data
    (output_dir / 'departments').mkdir(exist_ok=True)
    (output_dir / 'raw_jobs').mkdir(exist_ok=True)
    
    # Generate department summaries and raw jobs
    # Get all unique departments including NaN values
    depts_prev = set(df_previous['hiringDepartmentName'].unique())
    depts_curr = set(df_current['hiringDepartmentName'].unique())
    departments = depts_prev | depts_curr
    # Replace NaN with 'Unknown' for processing
    departments = {dept if pd.notna(dept) else 'Unknown' for dept in departments}
    
    print(f"\nGenerating {len(departments)} department files...")
    dept_metadata = []
    
    for dept in sorted(departments):
            
        dept_summary_df = generate_department_summary(df_previous, df_current, dept, previous_year, current_year)
        # Sort by the actual column name that was created
        sort_col = f'listings{previous_year}Value'
        if sort_col in dept_summary_df.columns:
            dept_summary_df = dept_summary_df.sort_values(sort_col, ascending=False)
        else:
            # Fallback to any listings*Value column
            value_cols = [col for col in dept_summary_df.columns if col.endswith('Value') and 'listings' in col]
            if value_cols:
                dept_summary_df = dept_summary_df.sort_values(value_cols[0], ascending=False)
        
        # Calculate metadata before renaming columns
        unique_subagencies = int(dept_summary_df['Subagency'].nunique()) if 'Subagency' in dept_summary_df.columns else 0
        
        # Use the actual column names that exist
        prev_col = f'listings{previous_year}Value'
        curr_col = f'listings{current_year}Value'
        total_prev = int(dept_summary_df[prev_col].sum()) if prev_col in dept_summary_df.columns else 0
        total_curr = int(dept_summary_df[curr_col].sum()) if curr_col in dept_summary_df.columns else 0
        
        # Update column names for consistency with JS expectations
        dept_summary_df = dept_summary_df.rename(columns={
            f'Listings_{previous_year}': 'Listings_2024',
            f'Listings_{current_year}': 'Listings_2025', 
            f'Percentage_{current_year}_of_{previous_year}': 'Percentage_2025_of_2024',
            f'listings{previous_year}Value': 'listings2024Value',
            f'listings{current_year}Value': 'listings2025Value'
        })
        
        # Save department file
        filename = f'dept_{sanitize_filename(dept)}.json'
        filepath = output_dir / 'departments' / filename
        dept_summary_df.to_json(filepath, orient='records', indent=2)
        
        # Generate and save raw jobs for this department
        raw_jobs = generate_department_raw_jobs(df_previous, df_current, dept, previous_year, current_year)
        raw_filename = f'jobs_{sanitize_filename(dept)}.json'
        raw_filepath = output_dir / 'raw_jobs' / raw_filename
        
        with open(raw_filepath, 'w') as f:
            json.dump(raw_jobs, f, separators=(',', ':'))  # Minified
        
        dept_metadata.append({
            'department': dept,
            'filename': filename,
            'raw_jobs_filename': raw_filename,
            'rows': len(dept_summary_df),
            'unique_subagencies': unique_subagencies,
            'total_2024': total_prev,
            'total_2025': total_curr,
            'raw_jobs_count': len(raw_jobs)
        })
        
        print(f"  {dept}: {len(dept_summary_df):,} rows, {unique_subagencies} subagencies, {len(raw_jobs):,} raw jobs")
    
    # Save department metadata
    metadata_file = output_dir / 'department_metadata.json'
    with open(metadata_file, 'w') as f:
        json.dump(dept_metadata, f, indent=2)
    
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
    import subprocess
    import sys
    
    script_dir = Path(__file__).parent
    
    # First fetch agency codes from USAJobs if we don't have them
    codes_file = script_dir / 'usajobs_agency_codes.json'
    if not codes_file.exists():
        print("Fetching agency codes from USAJobs API...")
        fetch_script = script_dir / 'fetch_agency_codes.py'
        result = subprocess.run([sys.executable, str(fetch_script)], 
                              capture_output=True, text=True)
        
        if result.returncode != 0:
            print("Warning: Could not fetch agency codes")
            print(result.stderr)
        else:
            print("Agency codes fetched successfully")
    
    # First fetch occupation series from USAJobs API
    occupation_script = script_dir / 'fetch_occupation_series.py'
    print("Fetching occupation series from USAJobs API...")
    result = subprocess.run([sys.executable, str(occupation_script)], 
                          capture_output=True, text=True)
    
    if result.returncode != 0:
        print("Warning: Could not fetch occupation series from API")
        print(result.stderr)
        print("Will use fallback data file")
    else:
        print("Occupation series fetched successfully")
    
    # Fetch appointment types from USAJobs API
    appointment_script = script_dir / 'fetch_appointment_types.py'
    print("Fetching appointment types from USAJobs API...")
    result = subprocess.run([sys.executable, str(appointment_script)], 
                          capture_output=True, text=True)
    
    if result.returncode != 0:
        print("Warning: Could not fetch appointment types from API")
        print(result.stderr)
    else:
        print("Appointment types fetched successfully")
    
    # Load the mappings after fetching from API
    load_occupation_series_mapping()
    load_appointment_types_mapping()
    
    # Copy API files to public directory for website use
    import shutil
    api_file = script_dir / 'occupation_series_from_api.json'
    public_api_file = script_dir / 'public' / 'occupation_series_from_api.json'
    if api_file.exists():
        shutil.copy2(api_file, public_api_file)
        print("Copied API occupation series file to public directory")
    
    appointment_api_file = script_dir / 'appointment_types_from_api.json'
    public_appointment_file = script_dir / 'public' / 'appointment_types_from_api.json'
    if appointment_api_file.exists():
        shutil.copy2(appointment_api_file, public_appointment_file)
        print("Copied API appointment types file to public directory")

    # Generate agency mappings
    mapping_script = script_dir / 'generate_agency_mappings.py'
    
    print("Generating agency mappings...")
    result = subprocess.run([sys.executable, str(mapping_script)], 
                          capture_output=True, text=True)
    
    if result.returncode != 0:
        print("Error generating mappings:")
        print(result.stderr)
        sys.exit(1)
    
    print("Mappings generated successfully\n")
    
    # Now generate the summary
    generate_summary()