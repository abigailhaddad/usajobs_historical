#!/usr/bin/env python3
"""
Generate documentation data for README and index.html

This script analyzes the parquet files and generates JSON data that can be used
to automatically update the documentation with accurate field information,
data coverage statistics, and examples.

Usage:
    python update/generate_docs_data.py
"""

import pandas as pd
import pyarrow.parquet as pq
import json
import os
import glob
from datetime import datetime
import numpy as np

# The current_jobs parquet files are gigabytes of job-description text. Reading
# them whole to count rows or scan one column is what OOM-killed the daily
# update runner, so everything here reads metadata or projects columns.

def read_cols(path, cols):
    """Read only `cols` that actually exist in the file; None if none do."""
    names = pq.read_schema(path).names
    present = [c for c in cols if c in names]
    if not present:
        return None
    return pd.read_parquet(path, columns=present)

def get_field_examples(series, field_name, max_examples=4):
    """Get representative examples for a field, showing variety when possible"""
    unique_vals = series.dropna().unique()
    
    if len(unique_vals) == 0:
        return "No data", 0
    
    # Special handling for very long nested fields - show 2 examples
    if field_name in ['HiringPaths', 'PositionLocations']:
        # Show 2 examples for these complex fields
        examples_to_show = unique_vals[:2] if len(unique_vals) >= 2 else unique_vals
        example_text = ', '.join(map(str, examples_to_show))
        return f"{example_text} ({len(unique_vals)} unique combinations)", len(unique_vals)
    
    # For fields with many unique values, sample strategically
    if len(unique_vals) > 10:
        if field_name in ['usajobsControlNumber', 'usajobs_control_number']:
            # For IDs, just show a few examples
            examples = ', '.join(map(str, unique_vals[:3]))
        elif field_name in ['hiringAgencyName', 'positionTitle']:
            # For names/titles, show variety
            sampled = np.random.choice(unique_vals, min(max_examples, len(unique_vals)), replace=False)
            examples = ', '.join(map(str, sampled))
        elif field_name in ['minimumSalary', 'maximumSalary']:
            # For salaries, show range
            sorted_vals = np.sort(series.dropna())
            low, med, high = sorted_vals[0], np.median(sorted_vals), sorted_vals[-1]
            examples = f"${int(low):,}, ${int(med):,}, ${int(high):,} (range: ${int(low):,}-${int(high):,})"
        elif field_name in ['minimumGrade', 'maximumGrade']:
            # For grades, show variety
            sampled = np.random.choice(unique_vals, min(max_examples, len(unique_vals)), replace=False)
            examples = ', '.join(map(str, sampled))
        else:
            # For other fields with many values, sample
            sampled = np.random.choice(unique_vals, min(max_examples, len(unique_vals)), replace=False)
            examples = ', '.join(map(str, sampled))
    else:
        # For fields with few unique values, show several
        examples = ', '.join(map(str, unique_vals[:min(max_examples, len(unique_vals))]))
    
    # Add unique count for fields with variety
    if len(unique_vals) > max_examples:
        examples += f" ({len(unique_vals)} unique)"
        
    return examples, len(unique_vals)

def analyze_data_coverage():
    """Analyze data coverage by year"""
    coverage_data = []
    
    # Get all parquet files
    historical_files = glob.glob('../data/historical_jobs_*.parquet')
    current_files = glob.glob('../data/current_jobs_*.parquet')
    
    all_years = set()
    for f in historical_files + current_files:
        part = f.split('_')[-1].replace('.parquet', '')
        try:
            all_years.add(int(part))
        except ValueError:
            continue  # skip backup files etc.
    
    for year in sorted(all_years):
        hist_file = f'../data/historical_jobs_{year}.parquet'
        curr_file = f'../data/current_jobs_{year}.parquet'
        
        total_jobs = 0
        jobs_opened = 0
        jobs_closed = 0
        
        for path in (hist_file, curr_file):
            if not os.path.exists(path):
                continue

            total_jobs += pq.read_metadata(path).num_rows

            # Count jobs opened/closed by date parsing — only the two date
            # columns get loaded, not the whole announcement text.
            dates = read_cols(path, ['positionOpenDate', 'positionCloseDate'])
            if dates is None:
                continue
            for col, bucket in (('positionOpenDate', 'opened'), ('positionCloseDate', 'closed')):
                if col not in dates.columns:
                    continue
                parsed = pd.to_datetime(dates[col], errors='coerce')
                n = int((parsed.dt.year == year).sum())
                if bucket == 'opened':
                    jobs_opened += n
                else:
                    jobs_closed += n
        
        # Determine coverage notes
        if year <= 2016:
            coverage = "Very limited"
        elif year == 2017:
            coverage = "✅ Complete year" 
        elif year >= 2018 and year <= 2024:
            coverage = "✅ Complete year"
        elif year == 2025:
            # Use the latest actual data collection date instead of current date
            latest_collection = get_latest_date()
            if latest_collection:
                latest_dt = datetime.strptime(latest_collection, '%Y-%m-%d')
                coverage = f"Current through {latest_dt.strftime('%B %d, %Y')}"
            else:
                coverage = f"Current through {datetime.now().strftime('%B %d, %Y')}"
        else:
            coverage = "Closing dates only"
            
        coverage_data.append({
            'year': year,
            'total_jobs': total_jobs,
            'jobs_opened': jobs_opened,
            'jobs_closed': jobs_closed,
            'coverage': coverage
        })
    
    return coverage_data

def analyze_all_fields():
    """Analyze all fields from the most recent complete year (2024)

    One column at a time: peak memory is a single column, not the whole file.
    """
    path = '../data/historical_jobs_2024.parquet'
    schema = pq.read_schema(path)
    n_rows = pq.read_metadata(path).num_rows

    # A stored pandas index shows up in the arrow schema but never was a
    # column in the old full-DataFrame read — don't document it as a field.
    col_names = [c for c in schema.names if not c.startswith('__index_level_')]

    field_data = []

    for col_name in sorted(col_names):
        df = pd.read_parquet(path, columns=[col_name])
        col = col_name  # keep the rest of the loop reading as before

        # Determine data type
        dtype = str(df[col].dtype)
        if dtype == 'object':
            # Check if it looks like JSON
            sample_val = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else ""
            if isinstance(sample_val, str) and (sample_val.startswith('[') or sample_val.startswith('{')):
                field_type = "JSON Array"
            else:
                field_type = "String"
        elif 'int' in dtype:
            field_type = "Integer"
        elif 'float' in dtype:
            field_type = "Number"
        elif 'datetime' in dtype:
            field_type = "DateTime"
        else:
            field_type = "String"
        
        # Calculate completeness
        completeness = (df[col].notna().sum() / n_rows) * 100 if n_rows else 0
        
        # Get examples
        examples, unique_count = get_field_examples(df[col], col)
        
        # Determine completeness class for styling
        if completeness >= 95:
            completeness_class = "good"
        elif completeness >= 70:
            completeness_class = "warning"  
        else:
            completeness_class = "poor"
        
        field_data.append({
            'field_name': col,
            'field_type': field_type,
            'examples': examples,
            'completeness_percent': round(completeness),
            'completeness_class': completeness_class,
            'unique_count': unique_count
        })
    
    return field_data

def get_file_sizes():
    """Calculate total size of parquet files"""
    all_files = glob.glob('../data/historical_jobs_*.parquet') + glob.glob('../data/current_jobs_*.parquet')
    total_size = sum(os.path.getsize(f) for f in all_files)
    
    # Convert to readable format
    if total_size >= 1024**3:  # GB
        return f"{total_size / (1024**3):.1f}GB"
    elif total_size >= 1024**2:  # MB
        return f"{total_size / (1024**2):.0f}MB"
    else:  # KB
        return f"{total_size / 1024:.0f}KB"

def get_latest_date():
    """Get the latest date when we actually collected data (from inserted_at field)"""
    latest_date = None
    
    all_files = glob.glob('../data/historical_jobs_*.parquet') + glob.glob('../data/current_jobs_*.parquet')
    
    for file in all_files:
        df = read_cols(file, ['inserted_at'])
        if df is not None:
            file_latest = pd.to_datetime(df['inserted_at'], errors='coerce').max()

            if pd.notna(file_latest) and (latest_date is None or file_latest > latest_date):
                latest_date = file_latest
    
    return latest_date.strftime('%Y-%m-%d') if latest_date else None

def generate_docs_data():
    """Generate all documentation data"""
    print("🔍 Analyzing data files...")
    
    # Get total dataset stats
    all_files = glob.glob('../data/historical_jobs_*.parquet') + glob.glob('../data/current_jobs_*.parquet')
    total_jobs = 0
    
    for file in all_files:
        total_jobs += pq.read_metadata(file).num_rows
    
    # Generate all data
    coverage_data = analyze_data_coverage()
    field_data = analyze_all_fields()
    file_size = get_file_sizes()
    latest_date = get_latest_date()
    
    docs_data = {
        'generated_at': datetime.now().isoformat(),
        'total_jobs': total_jobs,
        'total_fields': len(field_data),
        'file_size': file_size,
        'latest_job_date': latest_date,
        'data_coverage': coverage_data,
        'all_fields': field_data
    }
    
    # Write to JSON file
    with open('../docs_data.json', 'w') as f:
        json.dump(docs_data, f, indent=2)
    
    print(f"✅ Documentation data generated:")
    print(f"   Total jobs: {total_jobs:,}")
    print(f"   Total fields: {len(field_data)}")
    print(f"   File size: {file_size}")
    print(f"   Latest job date: {latest_date}")
    print(f"   Coverage years: {len(coverage_data)}")
    print(f"   Output: ../docs_data.json")
    
    return docs_data

if __name__ == "__main__":
    docs_data = generate_docs_data()