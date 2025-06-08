#!/usr/bin/env python3
import duckdb
import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

print('🦆 DuckDB count:')
import glob

# Find all DuckDB files
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.join(script_dir, '..', '..')
duckdb_pattern = os.path.join(repo_root, 'data', 'duckdb', 'usajobs_*.duckdb')
duckdb_files = glob.glob(duckdb_pattern)
total_duck_count = 0

for db_file in sorted(duckdb_files):
    duck_conn = duckdb.connect(db_file, read_only=True)
    count = duck_conn.execute('SELECT COUNT(*) FROM historical_jobs').fetchone()[0]
    print(f'  {db_file}: {count:,} jobs')
    total_duck_count += count
    duck_conn.close()

print(f'  Total: {total_duck_count:,} jobs')
duck_count = total_duck_count

print('🐘 PostgreSQL count:')
load_dotenv()
pg_conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = pg_conn.cursor()
cur.execute('SELECT COUNT(*) FROM historical_jobs;')
pg_count = cur.fetchone()[0]
print(f'  {pg_count:,} jobs')
pg_conn.close()

print(f'📊 Difference: {duck_count - pg_count:,} jobs')
if duck_count != pg_count:
    print('⚠️  MISMATCH DETECTED!')
else:
    print('✅ Counts match perfectly!')

# Check date coverage across all DuckDB files
print('\n📅 Date coverage check:')
all_dates_set = set()
earliest_date = None
latest_date = None

for db_file in sorted(duckdb_files):
    duck_conn = duckdb.connect(db_file, read_only=True)
    result = duck_conn.execute("""
        SELECT 
            MIN(position_open_date) as earliest,
            MAX(position_open_date) as latest,
            COUNT(DISTINCT position_open_date) as unique_dates
        FROM historical_jobs
        WHERE position_open_date IS NOT NULL
    """).fetchone()
    
    if result and result[0]:
        file_earliest, file_latest, file_unique = result
        if earliest_date is None or file_earliest < earliest_date:
            earliest_date = file_earliest
        if latest_date is None or file_latest > latest_date:
            latest_date = file_latest
            
        # Get all dates from this file
        dates = duck_conn.execute("""
            SELECT DISTINCT position_open_date 
            FROM historical_jobs 
            WHERE position_open_date IS NOT NULL
        """).fetchall()
        for date_row in dates:
            all_dates_set.add(str(date_row[0]))
    
    duck_conn.close()

# Create a fake result tuple for compatibility with existing code
result = (earliest_date, latest_date, len(all_dates_set))

if result:
    earliest, latest, unique_dates = result
    print(f'  Earliest job: {earliest}')
    print(f'  Latest job: {latest}')
    print(f'  Unique dates with jobs: {unique_dates}')
    
    # Calculate expected days
    if earliest and latest:
        # Check if dates are strings or date objects
        if isinstance(earliest, str):
            start_date = datetime.strptime(earliest, '%Y-%m-%d')
            end_date = datetime.strptime(latest, '%Y-%m-%d')
        else:
            start_date = datetime.combine(earliest, datetime.min.time())
            end_date = datetime.combine(latest, datetime.min.time())
        expected_days = (end_date - start_date).days + 1
        
        print(f'  Expected days in range: {expected_days}')
        print(f'  Coverage: {unique_dates}/{expected_days} days ({unique_dates/expected_days*100:.1f}%)')
        
        # Find missing dates
        print('\n🔍 Checking for missing dates...')
        date_set = all_dates_set
        missing_dates = []
        
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            if date_str not in date_set:
                missing_dates.append(date_str)
            current_date += timedelta(days=1)
        
        if missing_dates:
            print(f'  ⚠️  Found {len(missing_dates)} dates with no job postings:')
            # Show first 10 and last 10 if more than 20
            if len(missing_dates) > 20:
                for date in missing_dates[:10]:
                    print(f'    - {date}')
                print(f'    ... ({len(missing_dates) - 20} more dates) ...')
                for date in missing_dates[-10:]:
                    print(f'    - {date}')
            else:
                for date in missing_dates:
                    print(f'    - {date}')
        else:
            print('  ✅ No missing dates - every day has at least one job posting!')

# Check jobs per day distribution across all files
print('\n📊 Jobs per day distribution:')
daily_job_counts = {}

for db_file in sorted(duckdb_files):
    duck_conn = duckdb.connect(db_file, read_only=True)
    daily_data = duck_conn.execute("""
        SELECT position_open_date, COUNT(*) as job_count
        FROM historical_jobs
        WHERE position_open_date IS NOT NULL
        GROUP BY position_open_date
    """).fetchall()
    
    for date_str, count in daily_data:
        date_key = str(date_str)
        daily_job_counts[date_key] = daily_job_counts.get(date_key, 0) + count
    
    duck_conn.close()

if daily_job_counts:
    job_counts = list(daily_job_counts.values())
    days = len(job_counts)
    min_jobs = min(job_counts)
    avg_jobs = sum(job_counts) / len(job_counts)
    max_jobs = max(job_counts)
    
    print(f'  Days analyzed: {days}')
    print(f'  Min jobs/day: {min_jobs}')
    print(f'  Avg jobs/day: {avg_jobs:.1f}')
    print(f'  Max jobs/day: {max_jobs}')