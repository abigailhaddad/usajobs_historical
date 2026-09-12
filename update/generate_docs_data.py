#!/usr/bin/env python3
"""
Generate documentation data for README and index.html

This script analyzes the parquet files and generates JSON data that can be used
to automatically update the documentation with accurate field information,
data coverage statistics, and examples.

Usage:
    cd update && python generate_docs_data.py
"""

import pandas as pd
import pyarrow.parquet as pq
import json
import os
import re
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

def data_files(data_dir='../data'):
    """Every year-suffixed historical and current parquet, as {year: [paths]}."""
    out = {}
    for path in sorted(glob.glob(os.path.join(data_dir, 'historical_jobs_*.parquet'))
                       + glob.glob(os.path.join(data_dir, 'current_jobs_*.parquet'))):
        part = os.path.basename(path).split('_')[-1].replace('.parquet', '')
        try:
            year = int(part)
        except ValueError:
            continue  # backup files, anything else that is not a year
        out.setdefault(year, []).append(path)
    return out


def deduped_counts(data_dir='../data'):
    """Distinct announcements per year and overall, as ({year: counts}, total).

    A posting that is in both the Historical and the Current API has a row in
    each file, so adding row counts together counts it twice. Everything
    published off this number -- the README header, the coverage table,
    index.html -- used to be high by that overlap. Deduplicating on
    usajobsControlNumber is the same fix the README documents for
    hiringAgencyName.

    The overall total dedupes across years too, not just within one: a posting
    open at a year boundary appears in two years' current files.

    duckdb rather than pandas because the current_jobs files are gigabytes of
    announcement text. This projects three columns and spills to disk if it has
    to, where concatenating the same columns in pandas is what OOM-killed the
    daily runner.
    """
    import duckdb
    by_year = data_files(data_dir)
    if not by_year:
        return {}, 0

    # A file without a control number would contribute rows whose cn is NULL,
    # and count(DISTINCT) drops those -- so it would quietly undercount instead
    # of failing. Say so instead.
    for year, paths in by_year.items():
        for path in paths:
            if 'usajobsControlNumber' not in pq.read_schema(path).names:
                raise ValueError(
                    f"{path} has no usajobsControlNumber column, so its rows "
                    f"cannot be deduplicated or counted")

    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET memory_limit='{os.environ.get('DUCKDB_MEMORY_LIMIT', '2GB')}'")

    def files_sql(paths):
        listed = ", ".join(f"'{p}'" for p in paths)
        return (f"read_parquet([{listed}], union_by_name=true)")

    counts = {}
    for year, paths in sorted(by_year.items()):
        row = con.execute(f"""
            SELECT count(DISTINCT cn) AS total,
                   count(DISTINCT CASE WHEN substr(opened, 1, 4) = '{year}'
                                       THEN cn END) AS opened,
                   count(DISTINCT CASE WHEN substr(closed, 1, 4) = '{year}'
                                       THEN cn END) AS closed
            FROM (SELECT usajobsControlNumber::varchar AS cn,
                         CAST(positionOpenDate AS VARCHAR)  AS opened,
                         CAST(positionCloseDate AS VARCHAR) AS closed
                  FROM {files_sql(paths)})
        """).fetchone()
        counts[year] = {'total': row[0], 'opened': row[1], 'closed': row[2]}

    every = [p for paths in by_year.values() for p in paths]
    total = con.execute(
        f"SELECT count(DISTINCT usajobsControlNumber::varchar) "
        f"FROM {files_sql(every)}").fetchone()[0]
    con.close()
    return counts, total


def analyze_data_coverage(counts=None):
    """Analyze data coverage by year"""
    coverage_data = []

    if counts is None:
        counts, _ = deduped_counts()

    # Which year is the partial one comes from the data, not a literal. This
    # ladder used to name 2025 as "current through" and hand everything after it
    # "Closing dates only", so on 2026-01-01 it started calling the year being
    # collected closing-dates-only and the year that had just finished partial.
    latest_collection = get_latest_date()
    latest_dt = (datetime.strptime(latest_collection, '%Y-%m-%d')
                 if latest_collection else datetime.now())
    current_year = latest_dt.year

    for year in sorted(counts):
        total_jobs = counts[year]['total']
        jobs_opened = counts[year]['opened']
        jobs_closed = counts[year]['closed']

        if year <= 2016:
            coverage = "Very limited"
        elif year < current_year:
            coverage = "✅ Complete year"
        elif year == current_year:
            coverage = f"Current through {latest_dt.strftime('%B %d, %Y')}"
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

def newest_complete_year(data_dir='../data'):
    """The latest year with a historical file that is not the year in progress.

    This used to be the literal 2024, so the field table kept describing 2024's
    schema long after 2025 finished -- and the schema does move between years
    (the nested array columns changed case), so a frozen snapshot documents
    columns that are empty in the data a reader actually has.
    """
    years = []
    for path in glob.glob(os.path.join(data_dir, 'historical_jobs_*.parquet')):
        m = re.search(r'historical_jobs_(\d{4})\.parquet$', path)
        if m:
            years.append(int(m.group(1)))
    complete = [y for y in years if y < datetime.now().year]
    if not complete:
        raise FileNotFoundError(
            f"No complete-year historical_jobs_*.parquet in {data_dir} "
            f"(found years: {sorted(years)})")
    return max(complete)


def analyze_all_fields(year=None):
    """Analyze all fields from the most recent complete year.

    One column at a time: peak memory is a single column, not the whole file.
    """
    if year is None:
        year = newest_complete_year()
    path = f'../data/historical_jobs_{year}.parquet'
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
    
    # Distinct announcements. Summing num_rows across both APIs' files counted
    # every posting that appears in both of them twice.
    counts, total_jobs = deduped_counts()

    # Generate all data
    coverage_data = analyze_data_coverage(counts)
    field_year = newest_complete_year()
    field_data = analyze_all_fields(field_year)
    file_size = get_file_sizes()
    latest_date = get_latest_date()
    
    docs_data = {
        'generated_at': datetime.now().isoformat(),
        'total_jobs': total_jobs,
        'total_fields': len(field_data),
        'field_year': field_year,
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
    print(f"   Total fields: {len(field_data)} (from {field_year})")
    print(f"   File size: {file_size}")
    print(f"   Latest job date: {latest_date}")
    print(f"   Coverage years: {len(coverage_data)}")
    print(f"   Output: ../docs_data.json")
    
    return docs_data

if __name__ == "__main__":
    docs_data = generate_docs_data()