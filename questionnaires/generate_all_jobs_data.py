#!/usr/bin/env python3
"""
Generate JSON data for all jobs from parquet files with consistent field extraction
"""
import pandas as pd
import json
import os
from pathlib import Path
from datetime import datetime

# Define paths
BASE_DIR = Path('..')
DATA_DIR = BASE_DIR / 'data'

def extract_fields_from_job(row):
    """Extract fields from MatchedObjectDescriptor JSON or direct columns"""
    fields = {
        'service_type': None,
        'position_location': None,
        'occupation_series': None,
        'occupation_name': None,
        'grade_code': None,
        'low_grade': None,
        'high_grade': None
    }
    
    # First, check if we have direct columns (historical data)
    if pd.notna(row.get('serviceType')):
        fields['service_type'] = row.get('serviceType')
    else:
        # Set a default for null service types
        fields['service_type'] = 'Not Specified'
    
    # Handle PositionLocations for historical data
    if pd.notna(row.get('PositionLocations')):
        try:
            if isinstance(row['PositionLocations'], str):
                locations = json.loads(row['PositionLocations'])
            else:
                locations = row['PositionLocations']
            
            if isinstance(locations, list) and len(locations) > 0:
                loc = locations[0]
                city = loc.get('positionLocationCity', '')
                state = loc.get('positionLocationState', '')
                if city and state:
                    if state.lower() in city.lower():
                        fields['position_location'] = city
                    else:
                        fields['position_location'] = f"{city}, {state}"
                elif city:
                    fields['position_location'] = city
                elif state:
                    fields['position_location'] = state
        except:
            pass
    
    # Handle grades for historical data
    if pd.notna(row.get('minimumGrade')):
        low_grade = str(row.get('minimumGrade'))
        high_grade = str(row.get('maximumGrade', low_grade))
        grade_prefix = row.get('payScale', '')
        
        if grade_prefix and low_grade:
            if high_grade and low_grade != high_grade:
                fields['grade_code'] = f"{grade_prefix}-{low_grade}/{high_grade}"
            else:
                fields['grade_code'] = f"{grade_prefix}-{low_grade}"
        
        fields['low_grade'] = low_grade
        fields['high_grade'] = high_grade
    
    # Handle JobCategories for historical data
    if pd.notna(row.get('JobCategories')):
        try:
            if isinstance(row['JobCategories'], str):
                categories = json.loads(row['JobCategories'])
            else:
                categories = row['JobCategories']
            
            if isinstance(categories, list) and len(categories) > 0:
                if 'series' in categories[0]:
                    fields['occupation_series'] = str(categories[0]['series']).zfill(4)
        except:
            pass
    
    # Then check MatchedObjectDescriptor for current data
    if pd.notna(row.get('MatchedObjectDescriptor')):
        try:
            mod = json.loads(row['MatchedObjectDescriptor'])
            
            # Extract service type
            if 'UserArea' in mod and 'Details' in mod['UserArea']:
                service_type_code = mod['UserArea']['Details'].get('ServiceType')
                if service_type_code:
                    service_type_map = {
                        '01': 'Competitive',
                        '02': 'Excepted', 
                        '03': 'Senior Executive'
                    }
                    fields['service_type'] = service_type_map.get(service_type_code, service_type_code)
                
                # Extract grade information
                low_grade = mod['UserArea']['Details'].get('LowGrade')
                high_grade = mod['UserArea']['Details'].get('HighGrade')
                grade_prefix = row.get('minimumGrade', '')
                
                # Combine grade prefix with grade numbers
                if grade_prefix and low_grade:
                    if high_grade and low_grade != high_grade:
                        fields['grade_code'] = f"{grade_prefix}-{low_grade}/{high_grade}"
                    else:
                        fields['grade_code'] = f"{grade_prefix}-{low_grade}"
                elif low_grade:
                    # No prefix, just use the number (some cases like "00")
                    fields['grade_code'] = low_grade
                
                fields['low_grade'] = low_grade
                fields['high_grade'] = high_grade
            
            # Extract location
            if 'PositionLocation' in mod and isinstance(mod['PositionLocation'], list) and len(mod['PositionLocation']) > 0:
                loc = mod['PositionLocation'][0]
                city = loc.get('CityName', '')
                state = loc.get('CountrySubDivisionCode', '')
                if city and state:
                    if state.lower() in city.lower():
                        fields['position_location'] = city
                    else:
                        fields['position_location'] = f"{city}, {state}"
                elif city:
                    fields['position_location'] = city
                elif state:
                    fields['position_location'] = state
                    
            # Extract occupation
            if 'JobCategory' in mod and isinstance(mod['JobCategory'], list) and len(mod['JobCategory']) > 0:
                # Ensure occupation series is padded with leading zeros to 4 digits
                occ_code = mod['JobCategory'][0].get('Code')
                if occ_code:
                    fields['occupation_series'] = str(occ_code).zfill(4)
                fields['occupation_name'] = mod['JobCategory'][0].get('Name')
        except:
            pass
    
    return fields

def main():
    # Load ALL jobs from parquet files - both current AND historical
    all_jobs_dfs = []
    # Include both current and historical jobs to catch closed positions
    parquet_files = sorted(DATA_DIR.glob('current_jobs_*.parquet')) + sorted(DATA_DIR.glob('historical_jobs_*.parquet'))
    cutoff_date = pd.to_datetime('2025-06-01')
    
    print("Loading all job data...")
    for parquet_file in parquet_files:
        df = pd.read_parquet(parquet_file)
        if 'positionOpenDate' in df.columns:
            df['positionOpenDate'] = pd.to_datetime(df['positionOpenDate'])
            df = df[df['positionOpenDate'] >= cutoff_date]
        all_jobs_dfs.append(df)
    
    all_jobs_df = pd.concat(all_jobs_dfs, ignore_index=True)
    
    # Deduplicate by usajobsControlNumber (the actual control number field)
    print(f"Total jobs before deduplication: {len(all_jobs_df):,}")
    all_jobs_df = all_jobs_df.drop_duplicates(subset='usajobsControlNumber', keep='first')
    print(f"Total jobs after deduplication: {len(all_jobs_df):,}")
    
    # Extract fields for all jobs
    print("Extracting fields from job data...")
    extracted_fields = []
    for idx, row in all_jobs_df.iterrows():
        fields = extract_fields_from_job(row)
        fields['usajobs_control_number'] = row.get('usajobsControlNumber')
        fields['position_title'] = row.get('positionTitle')
        fields['hiring_agency'] = row.get('hiringDepartmentName')
        fields['position_open_date'] = row.get('positionOpenDate')
        fields['position_close_date'] = row.get('positionCloseDate')
        extracted_fields.append(fields)
    
    # Create dataframe with extracted fields
    jobs_clean_df = pd.DataFrame(extracted_fields)
    
    # Add occupation_full column
    jobs_clean_df['occupation_full'] = jobs_clean_df['occupation_series'].astype(str) + ' - ' + jobs_clean_df['occupation_name'].fillna('Unknown')
    
    # Add grade_level column (using the full grade_code)
    jobs_clean_df['grade_level'] = jobs_clean_df['grade_code'].fillna('Not Specified')
    
    # Group by relevant fields and get counts
    print("Generating aggregate statistics...")
    
    stats = {
        'metadata': {
            'total_jobs': len(jobs_clean_df),
            'cutoff_date': cutoff_date.strftime('%Y-%m-%d'),
            'generation_date': datetime.now().isoformat()
        },
        'by_service_type': {},
        'by_grade_level': {},
        'by_location': {},
        'by_agency': {},
        'by_occupation': {}
    }
    
    # Service type counts
    if 'service_type' in jobs_clean_df.columns:
        service_counts = jobs_clean_df['service_type'].value_counts().to_dict()
        stats['by_service_type'] = {k: int(v) for k, v in service_counts.items() if pd.notna(k)}
    
    # Grade level counts
    if 'grade_level' in jobs_clean_df.columns:
        grade_counts = jobs_clean_df['grade_level'].value_counts().head(20).to_dict()
        stats['by_grade_level'] = {k: int(v) for k, v in grade_counts.items() if pd.notna(k)}
    
    # Location counts
    if 'position_location' in jobs_clean_df.columns:
        location_counts = jobs_clean_df['position_location'].value_counts().head(20).to_dict()
        stats['by_location'] = {k: int(v) for k, v in location_counts.items() if pd.notna(k)}
    
    # Agency counts
    if 'hiring_agency' in jobs_clean_df.columns:
        agency_counts = jobs_clean_df['hiring_agency'].value_counts().head(20).to_dict()
        stats['by_agency'] = {k: int(v) for k, v in agency_counts.items() if pd.notna(k)}
    
    # Occupation counts
    if 'occupation_full' in jobs_clean_df.columns:
        occupation_counts = jobs_clean_df['occupation_full'].value_counts().head(20).to_dict()
        stats['by_occupation'] = {k: int(v) for k, v in occupation_counts.items() if pd.notna(k)}
    
    # Save the clean dataframe for use by the analysis script
    jobs_clean_df.to_csv('all_jobs_clean.csv', index=False)
    print(f"Saved clean jobs data to all_jobs_clean.csv ({len(jobs_clean_df):,} records)")
    
    # Save the statistics
    with open('all_jobs_stats.json', 'w') as f:
        json.dump(stats, f, indent=2)
    
    print(f"Saved statistics to all_jobs_stats.json")

if __name__ == '__main__':
    main()