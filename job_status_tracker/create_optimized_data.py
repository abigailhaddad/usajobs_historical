#!/usr/bin/env python3
"""
Create an optimized version of the data for web deployment
"""

import json
import gzip
from pathlib import Path

# Load the full data
with open('job_status_data.json', 'r') as f:
    data = json.load(f)

print(f"Original cancelled jobs: {len(data['cancelled_jobs'])}")

# Create a lightweight version - remove fields we don't display
optimized_cancelled = []
for job in data['cancelled_jobs']:
    # Only keep fields we actually use in the UI
    optimized_job = {
        'control_number': job['control_number'],
        'usajobs_url': job['usajobs_url'],
        'position_title': job['position_title'],
        'hiring_agency': job['hiring_agency'],
        'location': job['location'],
        'open_date': job['open_date'],
        'close_date': job['close_date'],
        'min_grade': job['min_grade'],
        'max_grade': job['max_grade'],
        'min_salary': job['min_salary'],
        'max_salary': job['max_salary'],
        'total_openings': job['total_openings'],
        'service_type': job['service_type'],
        'days_open': job['days_open']
    }
    optimized_cancelled.append(optimized_job)

# Create optimized data structure
optimized_data = {
    'summary': data['summary'],
    'cancelled_jobs': optimized_cancelled,
    'monthly_status': data['monthly_status'],
    'agency_stats': data['agency_stats'][:50]  # Top 50 agencies only
}

# Save optimized version
with open('job_status_data_optimized.json', 'w') as f:
    json.dump(optimized_data, f, separators=(',', ':'))

# Also create a gzipped version
with gzip.open('job_status_data_optimized.json.gz', 'wt', encoding='utf-8') as f:
    json.dump(optimized_data, f, separators=(',', ':'))

# Check sizes
import os
original_size = os.path.getsize('job_status_data.json') / 1024 / 1024
minified_size = os.path.getsize('job_status_data.min.json') / 1024 / 1024
optimized_size = os.path.getsize('job_status_data_optimized.json') / 1024 / 1024
gzipped_size = os.path.getsize('job_status_data_optimized.json.gz') / 1024 / 1024

print(f"\nFile sizes:")
print(f"  Original: {original_size:.2f} MB")
print(f"  Minified: {minified_size:.2f} MB")
print(f"  Optimized: {optimized_size:.2f} MB")
print(f"  Gzipped: {gzipped_size:.2f} MB")
print(f"\nSize reduction: {(1 - optimized_size/original_size)*100:.1f}%")
print(f"Gzipped reduction: {(1 - gzipped_size/original_size)*100:.1f}%")