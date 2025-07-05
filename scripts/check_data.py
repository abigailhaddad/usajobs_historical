#!/usr/bin/env python3
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
import glob
from datetime import datetime, timedelta

print('📊 Parquet files count:')

# Find all Parquet files
script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.join(script_dir, '..')
historical_pattern = os.path.join(repo_root, 'data', 'historical_jobs_*.parquet')
current_pattern = os.path.join(repo_root, 'data', 'current_jobs_*.parquet')

historical_files = glob.glob(historical_pattern)
current_files = glob.glob(current_pattern)

total_historical_count = 0
total_current_count = 0

print('\n📈 Historical Jobs:')
for parquet_file in sorted(historical_files):
    try:
        df = pd.read_parquet(parquet_file)
        count = len(df)
        print(f'  {os.path.basename(parquet_file)}: {count:,} jobs')
        total_historical_count += count
    except Exception as e:
        print(f'  {os.path.basename(parquet_file)}: Error reading file ({e})')

print(f'  Historical Total: {total_historical_count:,} jobs')

print('\n📊 Current Jobs:')
for parquet_file in sorted(current_files):
    try:
        df = pd.read_parquet(parquet_file)
        count = len(df)
        print(f'  {os.path.basename(parquet_file)}: {count:,} jobs')
        total_current_count += count
    except Exception as e:
        print(f'  {os.path.basename(parquet_file)}: Error reading file ({e})')

print(f'  Current Total: {total_current_count:,} jobs')

total_parquet_count = total_historical_count + total_current_count
print(f'\n📦 Grand Total: {total_parquet_count:,} jobs')

print('\n🐘 PostgreSQL count:')
load_dotenv()

try:
    pg_conn = psycopg2.connect(
        host=os.getenv('PG_HOST'),
        database=os.getenv('PG_DATABASE'),
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PASSWORD'),
        port=os.getenv('PG_PORT', 5432)
    )
    
    cur = pg_conn.cursor()
    
    # Check historical_jobs table
    try:
        cur.execute('SELECT COUNT(*) FROM historical_jobs')
        pg_historical_count = cur.fetchone()[0]
        print(f'  historical_jobs table: {pg_historical_count:,} jobs')
    except psycopg2.Error as e:
        print(f'  historical_jobs table: Error ({e})')
        pg_historical_count = 0
    
    # Check current_jobs table if it exists
    try:
        cur.execute('SELECT COUNT(*) FROM current_jobs')
        pg_current_count = cur.fetchone()[0]
        print(f'  current_jobs table: {pg_current_count:,} jobs')
    except psycopg2.Error as e:
        print(f'  current_jobs table: Not found or error ({e})')
        pg_current_count = 0
    
    pg_total_count = pg_historical_count + pg_current_count
    print(f'  PostgreSQL Total: {pg_total_count:,} jobs')
    
    cur.close()
    pg_conn.close()
    
    print('\n📈 Comparison:')
    print(f'  Parquet files: {total_parquet_count:,} jobs')
    print(f'  PostgreSQL:    {pg_total_count:,} jobs')
    
    if total_parquet_count > pg_total_count:
        diff = total_parquet_count - pg_total_count
        print(f'  📤 {diff:,} jobs ready to upload to PostgreSQL')
    elif pg_total_count > total_parquet_count:
        diff = pg_total_count - total_parquet_count
        print(f'  ⚠️  PostgreSQL has {diff:,} more jobs than Parquet files')
    else:
        print(f'  ✅ Counts match!')

except Exception as e:
    print(f'  Error connecting to PostgreSQL: {e}')
    print(f'  Parquet files contain: {total_parquet_count:,} jobs')

print('\n💡 To query Parquet data with DuckDB:')
print("  python -c \"import duckdb; conn = duckdb.connect(':memory:'); print(conn.execute('SELECT COUNT(*) FROM read_parquet(\\\"data/historical_jobs_*.parquet\\\")').fetchone()[0])\"")
print("  python -c \"import duckdb; conn = duckdb.connect(':memory:'); print(conn.execute('SELECT COUNT(*) FROM read_parquet(\\\"data/current_jobs_*.parquet\\\")').fetchone()[0])\"")