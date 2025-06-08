#!/usr/bin/env python3
import duckdb
import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

print('🦆 DuckDB count:')
duck_conn = duckdb.connect('usajobs_2024.duckdb', read_only=True)
duck_count = duck_conn.execute('SELECT COUNT(*) FROM historical_jobs').fetchone()[0]
print(f'  {duck_count:,} jobs')

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

# Check date coverage
print('\n📅 Date coverage check:')
result = duck_conn.execute("""
    SELECT 
        MIN(position_open_date) as earliest,
        MAX(position_open_date) as latest,
        COUNT(DISTINCT position_open_date) as unique_dates
    FROM historical_jobs
    WHERE position_open_date IS NOT NULL
""").fetchone()

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
        all_dates = duck_conn.execute("""
            SELECT DISTINCT position_open_date 
            FROM historical_jobs 
            WHERE position_open_date IS NOT NULL 
            ORDER BY position_open_date
        """).fetchall()
        
        date_set = {str(row[0]) for row in all_dates}
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

# Check jobs per day distribution
print('\n📊 Jobs per day distribution:')
daily_stats = duck_conn.execute("""
    SELECT 
        COUNT(*) as days,
        MIN(job_count) as min_jobs,
        AVG(job_count) as avg_jobs,
        MAX(job_count) as max_jobs
    FROM (
        SELECT position_open_date, COUNT(*) as job_count
        FROM historical_jobs
        WHERE position_open_date IS NOT NULL
        GROUP BY position_open_date
    )
""").fetchone()

if daily_stats:
    days, min_jobs, avg_jobs, max_jobs = daily_stats
    print(f'  Days analyzed: {days}')
    print(f'  Min jobs/day: {min_jobs}')
    print(f'  Avg jobs/day: {avg_jobs:.1f}')
    print(f'  Max jobs/day: {max_jobs}')

duck_conn.close()