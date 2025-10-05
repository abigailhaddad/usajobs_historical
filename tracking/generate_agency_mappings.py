#!/usr/bin/env python3
"""
Generate agency to department mapping from historical data.
This should be run before generate_jobs_summary_dynamic.py
"""
import pandas as pd
import json
from pathlib import Path
from collections import defaultdict

def load_usajobs_agency_codes():
    """Load the official USAJobs agency code mappings"""
    codes_file = Path(__file__).parent / 'usajobs_agency_codes.json'
    if codes_file.exists():
        with open(codes_file, 'r') as f:
            return json.load(f)
    return {}

def generate_mappings():
    """Generate agency to department mapping from all available data"""
    # Load all data to build comprehensive mapping
    dfs = []
    data_dir = Path(__file__).parent.parent / 'data'
    
    for year in [2024, 2025]:
        for data_type in ['current', 'historical']:
            try:
                file_path = data_dir / f'{data_type}_jobs_{year}.parquet'
                if file_path.exists():
                    df = pd.read_parquet(file_path)
                    dfs.append(df)
                    print(f"Loaded {data_type}_jobs_{year}: {len(df)} rows")
            except Exception as e:
                print(f"Could not load {data_type}_jobs_{year}: {e}")

    if not dfs:
        print("ERROR: No data files found!")
        return False

    # Combine all data
    all_data = pd.concat(dfs, ignore_index=True)
    print(f"\nTotal rows: {len(all_data)}")

    # Build mapping from agency to department using rows that have both
    agency_to_dept = defaultdict(lambda: defaultdict(int))

    # Only use rows where both agency and department are not null
    valid_mappings = all_data[all_data['hiringAgencyName'].notna() & all_data['hiringDepartmentName'].notna()]
    print(f"Rows with both agency and department: {len(valid_mappings)}")

    # Count occurrences of each agency->department mapping
    for _, row in valid_mappings.iterrows():
        agency = row['hiringAgencyName']
        dept = row['hiringDepartmentName']
        agency_to_dept[agency][dept] += 1

    # For each agency, pick the most common department
    agency_mapping = {}
    for agency, dept_counts in agency_to_dept.items():
        # Sort by count and take the most common
        most_common_dept = max(dept_counts.items(), key=lambda x: x[1])[0]
        total_count = sum(dept_counts.values())
        confidence = dept_counts[most_common_dept] / total_count
        
        agency_mapping[agency] = {
            'department': most_common_dept,
            'confidence': round(confidence, 3),
            'total_mappings': total_count,
            'all_departments': dict(dept_counts)
        }

    # Save mapping
    output_file = Path(__file__).parent / 'agency_to_department_mapping.json'
    with open(output_file, 'w') as f:
        json.dump(agency_mapping, f, indent=2)

    print(f"\nSaved mapping for {len(agency_mapping)} agencies to {output_file}")
    
    # Also create agency code to name mapping
    print("\nGenerating agency code to name mapping...")
    agency_codes = load_usajobs_agency_codes()
    
    # Build code to name mapping from our data as fallback
    code_to_name_fallback = defaultdict(lambda: defaultdict(int))
    valid_code_mappings = all_data[all_data['hiringAgencyCode'].notna() & all_data['hiringAgencyName'].notna()]
    
    for _, row in valid_code_mappings.iterrows():
        code = row['hiringAgencyCode']
        name = row['hiringAgencyName']
        code_to_name_fallback[code][name] += 1
    
    # Create final code mapping
    final_code_mapping = {}
    for code, name_counts in code_to_name_fallback.items():
        # First check if we have an official name
        if code in agency_codes:
            final_code_mapping[code] = agency_codes[code]['name']
        else:
            # Use most common name from our data
            most_common_name = max(name_counts.items(), key=lambda x: x[1])[0]
            final_code_mapping[code] = most_common_name
    
    # Add any codes from USAJobs that we don't have
    for code, info in agency_codes.items():
        if code not in final_code_mapping:
            final_code_mapping[code] = info['name']
    
    # Save code mapping
    code_output_file = Path(__file__).parent / 'agency_code_to_name_mapping.json'
    with open(code_output_file, 'w') as f:
        json.dump(final_code_mapping, f, indent=2, sort_keys=True)
    
    print(f"Saved code mapping for {len(final_code_mapping)} agency codes to {code_output_file}")
    
    return True

if __name__ == '__main__':
    generate_mappings()