import pandas as pd
import json
from datetime import datetime
import numpy as np

def extract_hiring_paths(hiring_paths_json):
    """Extract hiring paths from JSON field"""
    try:
        if pd.isna(hiring_paths_json):
            return []
        paths = json.loads(hiring_paths_json)
        return [p.get('hiringPath', '') for p in paths if 'hiringPath' in p]
    except:
        return []

def extract_occupation_series(job_categories_json):
    """Extract occupational series from JobCategories JSON field"""
    try:
        if pd.isna(job_categories_json):
            return None
        categories = json.loads(job_categories_json)
        if categories and len(categories) > 0 and 'series' in categories[0]:
            return categories[0]['series']
        return None
    except:
        return None

def load_and_process_jobs_data():
    """Load job data and calculate February-August comparisons for 2024 vs 2025"""
    
    # Define months to compare
    comparison_months = [2, 3, 4, 5, 6, 7, 8]  # February through August
    
    # Load 2024 data
    all_2024_data = []
    try:
        df_hist = pd.read_parquet('../data/historical_jobs_2024.parquet')
        all_2024_data.append(df_hist)
    except:
        pass
    try:
        df_curr = pd.read_parquet('../data/current_jobs_2024.parquet')
        # Deduplicate if needed
        if len(all_2024_data) > 0:
            existing_ids = set(all_2024_data[0]['usajobsControlNumber'])
            df_curr = df_curr[~df_curr['usajobsControlNumber'].isin(existing_ids)]
        all_2024_data.append(df_curr)
    except:
        pass
    
    # Load 2025 data
    all_2025_data = []
    try:
        df_hist = pd.read_parquet('../data/historical_jobs_2025.parquet')
        all_2025_data.append(df_hist)
    except:
        pass
    try:
        df_curr = pd.read_parquet('../data/current_jobs_2025.parquet')
        # Deduplicate if needed
        if len(all_2025_data) > 0:
            existing_ids = set(all_2025_data[0]['usajobsControlNumber'])
            df_curr = df_curr[~df_curr['usajobsControlNumber'].isin(existing_ids)]
        all_2025_data.append(df_curr)
    except:
        pass
    
    # Combine years
    df_2024 = pd.concat(all_2024_data, ignore_index=True) if all_2024_data else pd.DataFrame()
    df_2025 = pd.concat(all_2025_data, ignore_index=True) if all_2025_data else pd.DataFrame()
    
    # Convert dates and filter by months
    if len(df_2024) > 0:
        df_2024['positionOpenDate'] = pd.to_datetime(df_2024['positionOpenDate'], format='mixed')
        df_2024['month'] = df_2024['positionOpenDate'].dt.month
        df_2024 = df_2024[df_2024['month'].isin(comparison_months)]
        # Extract hiring paths and occupation series
        df_2024['hiring_paths'] = df_2024['HiringPaths'].apply(extract_hiring_paths)
        df_2024['occupation_series'] = df_2024['JobCategories'].apply(extract_occupation_series)
    
    if len(df_2025) > 0:
        df_2025['positionOpenDate'] = pd.to_datetime(df_2025['positionOpenDate'], format='mixed')
        df_2025['month'] = df_2025['positionOpenDate'].dt.month
        df_2025 = df_2025[df_2025['month'].isin(comparison_months)]
        # Extract hiring paths and occupation series
        df_2025['hiring_paths'] = df_2025['HiringPaths'].apply(extract_hiring_paths)
        df_2025['occupation_series'] = df_2025['JobCategories'].apply(extract_occupation_series)
    
    return df_2024, df_2025

def aggregate_by_level(df_2024, df_2025, level='department'):
    """Aggregate job counts by department or agency"""
    
    if level == 'department':
        group_col = 'hiringDepartmentName'
    else:
        group_col = 'hiringAgencyName'
    
    # Count jobs by group
    counts_2024 = df_2024[group_col].value_counts() if len(df_2024) > 0 else pd.Series()
    counts_2025 = df_2025[group_col].value_counts() if len(df_2025) > 0 else pd.Series()
    
    # Combine all entities
    all_entities = set(counts_2024.index) | set(counts_2025.index)
    
    bubble_data = []
    for entity in all_entities:
        count_2024 = counts_2024.get(entity, 0)
        count_2025 = counts_2025.get(entity, 0)
        
        # Skip if no jobs in either period
        if count_2024 == 0 and count_2025 == 0:
            continue
            
        # Calculate percentage of 2024 (not percent change)
        # 100% means 2025 has same as 2024
        # 0% means 2025 has none
        # 200% means 2025 has double 2024
        if count_2024 > 0:
            percentage = (count_2025 / count_2024) * 100
        else:
            percentage = 0  # If no 2024 jobs, can't calculate meaningful percentage
        
        bubble_data.append({
            'name': entity,
            'x': float(percentage),
            'y': int(count_2024),  # Initial count (2024)
            'value': int(count_2024),  # For bubble size
            'count_2024': int(count_2024),
            'count_2025': int(count_2025)
        })
    
    return bubble_data

def get_filter_options(df_2024, df_2025):
    """Get unique values for filter dropdowns"""
    all_df = pd.concat([df_2024, df_2025], ignore_index=True)
    
    # Get unique departments
    departments = sorted(all_df['hiringDepartmentName'].dropna().unique())
    
    # Get unique agencies
    agencies = sorted(all_df['hiringAgencyName'].dropna().unique())
    
    # Get unique appointment types
    appointment_types = sorted(all_df['appointmentType'].dropna().unique())
    
    # Get unique work schedules
    work_schedules = sorted(all_df['workSchedule'].dropna().unique())
    
    # Get unique hiring paths (need to flatten the lists)
    all_paths = set()
    for paths_list in all_df['hiring_paths']:
        if paths_list:
            all_paths.update(paths_list)
    hiring_paths = sorted(list(all_paths))
    
    return {
        'departments': departments,
        'agencies': agencies,
        'appointmentTypes': appointment_types,
        'workSchedules': work_schedules,
        'hiringPaths': hiring_paths
    }

def main():
    print("Loading job data...")
    df_2024, df_2025 = load_and_process_jobs_data()
    
    print(f"2024 February-August jobs: {len(df_2024):,}")
    print(f"2025 February-August jobs: {len(df_2025):,}")
    
    # Generate data for both aggregation levels
    dept_data = aggregate_by_level(df_2024, df_2025, 'department')
    agency_data = aggregate_by_level(df_2024, df_2025, 'agency')
    
    # Get filter options
    filter_options = get_filter_options(df_2024, df_2025)
    
    # Calculate summary statistics
    total_2024 = len(df_2024)
    total_2025 = len(df_2025)
    overall_percentage = (total_2025 / total_2024 * 100) if total_2024 > 0 else 0
    
    # Prepare output data structure matching the budget tracker format
    output_data = {
        'summaryStats': {
            'total2024': total_2024,
            'total2025': total_2025,
            'percentChange': round(overall_percentage, 1),
            'entityCount': len(dept_data)  # Default to department view
        },
        'bubbleData': {
            'department': dept_data,
            'agency': agency_data
        },
        'filterOptions': filter_options,
        'lastUpdated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # Write to JSON file
    with open('jobs_data.json', 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\nData written to jobs_data.json")
    print(f"Total departments: {len(dept_data)}")
    print(f"Total agencies: {len(agency_data)}")
    print(f"2025 as % of 2024: {overall_percentage:.1f}%")

if __name__ == "__main__":
    main()