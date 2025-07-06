"""
USAJobs Data Examples
=====================

This script demonstrates how to work with the USAJobs data using both:
1. Local files (if you've cloned the repository)
2. Direct downloads from GitHub (without cloning)

The examples show common analysis patterns including:
- Loading job posting data
- Analyzing hiring trends by agency
- Exploring salary ranges and job classifications
- Tracking posting patterns over time
- Geographic and seasonal analysis

It also demonstrates how to register multiple Parquet files as external tables in a DuckDB database and query them.
You might want to do this if you want to download multiple data files and query them together.

If you run this, it will ask you if you want to delete the files this downloads.

Dataset: 2.97M federal job postings from 2013-2025
Source: USAJobs Historical and Current APIs (field-normalized, deduplication available)
"""

import pandas as pd
import os
import shutil
import duckdb
from io import StringIO
from datetime import datetime

def ensure_directory_exists(path):
    """Create directory if it doesn't exist"""
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Created directory: {path}")

def safe_numeric_conversion(value):
    """Safely convert a value to numeric, handling various edge cases"""
    if pd.isna(value) or value == 'nan' or value == '' or value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def run_local_examples():
    """Run examples using local parquet files"""
    print("\n" + "="*80)
    print("RUNNING EXAMPLES WITH LOCAL FILES")
    print("="*80 + "\n")
    
    # Check if local files exist - start with recent years
    recent_files = ['2024', '2023', '2022']
    available_files = []
    
    for year in recent_files:
        local_file = f'data/historical_jobs_{year}.parquet'
        if os.path.exists(local_file):
            available_files.append((year, local_file))
        
    if not available_files:
        print("ERROR: No local files found in data/ directory")
        print("\nThis script expects you to have cloned the full repository.")
        print("If you haven't cloned the repo, the download examples below will work instead.")
        print("\nTo use local files:")
        print("1. Clone the repository: git clone https://github.com/yourusername/usajobs_historic.git")
        print("2. Run this script from the repository root directory")
        return None
    
    # Load the most recent available file
    year, file_path = available_files[0]
    print(f"Loading local file: {file_path}")
    df = pd.read_parquet(file_path)
    print(f"✓ Successfully loaded {len(df):,} job postings from {year}")
    print(f"  Columns: {df.shape[1]}")
    print(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
    
    # Load multiple years if available
    if len(available_files) > 1:
        print(f"\nAdditional files available:")
        for yr, fp in available_files[1:]:
            df_extra = pd.read_parquet(fp)
            print(f"  {yr}: {len(df_extra):,} jobs")
    
    return df

def run_download_examples():
    """Run examples by downloading files from GitHub"""
    print("\n" + "="*80)
    print("RUNNING EXAMPLES WITH GITHUB DOWNLOADS")
    print("="*80 + "\n")
    
    # Create download directory
    download_dir = 'download'
    ensure_directory_exists(download_dir)
    
    # Download URL - use 2024 data as it's substantial but not too large
    base_url = 'https://github.com/yourusername/usajobs_historic/raw/main/data/'
    filename = 'historical_jobs_2024.parquet'
    url = base_url + filename
    local_download_path = os.path.join(download_dir, filename)
    
    print(f"Downloading from: {url}")
    print(f"Saving to: {local_download_path}")
    print("This may take a moment (file is ~60 MB)...")
    
    try:
        df = pd.read_parquet(url)
        # Save locally for faster subsequent access in this session
        df.to_parquet(local_download_path)
        print(f"✓ Successfully downloaded and loaded {len(df):,} job postings")
        print(f"  Also saved locally to: {local_download_path}")
        return df
        
    except Exception as e:
        print(f"ERROR downloading file: {e}")
        print("\nPossible causes:")
        print("- No internet connection")
        print("- GitHub rate limiting")
        print("- File URL has changed")
        print("- Repository not yet public")
        return None

def analyze_data(df, source_type):
    """Run analysis examples on the dataframe"""
    
    # Determine which year(s) this data covers
    df['positionOpenDate'] = pd.to_datetime(df['positionOpenDate'], errors='coerce')
    years_in_data = sorted(df['positionOpenDate'].dt.year.dropna().unique())
    
    if len(years_in_data) == 1:
        year_info = f"{int(years_in_data[0])} DATA"
    elif len(years_in_data) == 2:
        year_info = f"{int(years_in_data[0])}-{int(years_in_data[1])} DATA"
    else:
        year_info = f"{int(years_in_data[0])}-{int(years_in_data[-1])} DATA ({len(years_in_data)} years)"
    
    print(f"\n{'='*80}")
    print(f"ANALYSIS EXAMPLES ({source_type}) - {year_info}")
    print(f"{'='*80}\n")
    
    # Example 1: Top hiring agencies
    print(f"1. TOP 15 HIRING AGENCIES ({year_info})")
    print("-" * 40)
    try:
        top_agencies = df['hiringAgencyName'].value_counts().head(15)
        for i, (agency, count) in enumerate(top_agencies.items(), 1):
            pct = count / len(df) * 100
            print(f"{i:2d}. {agency}: {count:,} jobs ({pct:.1f}%)")
    except Exception as e:
        print(f"Error: {e}")
    
    print(f"\n2. SALARY ANALYSIS ({year_info})")
    print("-" * 40)
    try:
        # Convert salary fields to numeric
        df['min_salary_numeric'] = df['minimumSalary'].apply(safe_numeric_conversion)
        df['max_salary_numeric'] = df['maximumSalary'].apply(safe_numeric_conversion)
        
        # Filter to records with valid salary data
        df_with_salary = df[(df['min_salary_numeric'].notna()) & (df['max_salary_numeric'].notna())]
        print(f"   Records with salary data: {len(df_with_salary):,} ({len(df_with_salary)/len(df)*100:.1f}%)")
        
        if len(df_with_salary) > 0:
            print(f"   Minimum salary range: ${df_with_salary['min_salary_numeric'].min():,.0f} - ${df_with_salary['min_salary_numeric'].max():,.0f}")
            print(f"   Maximum salary range: ${df_with_salary['max_salary_numeric'].min():,.0f} - ${df_with_salary['max_salary_numeric'].max():,.0f}")
            print(f"   Median salary range: ${df_with_salary['min_salary_numeric'].median():,.0f} - ${df_with_salary['max_salary_numeric'].median():,.0f}")
            
            # Top 5 highest-paying agencies by median max salary
            salary_by_agency = df_with_salary.groupby('hiringAgencyName')['max_salary_numeric'].agg(['median', 'count']).sort_values('median', ascending=False)
            salary_by_agency = salary_by_agency[salary_by_agency['count'] >= 10]  # At least 10 jobs
            
            print(f"\n   Top 5 Highest-Paying Agencies (median max salary, 10+ jobs):")
            for agency, stats in salary_by_agency.head(5).iterrows():
                print(f"     {agency}: ${stats['median']:,.0f} ({stats['count']:.0f} jobs)")
    except Exception as e:
        print(f"Error: {e}")
    
    print(f"\n3. JOB POSTING TIMING ANALYSIS ({year_info})")
    print("-" * 40)
    try:
        # Convert position open date
        df['positionOpenDate'] = pd.to_datetime(df['positionOpenDate'], errors='coerce')
        df['open_month'] = df['positionOpenDate'].dt.month
        df['open_year'] = df['positionOpenDate'].dt.year
        
        # Monthly patterns
        monthly_counts = df['open_month'].value_counts().sort_index()
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        print("   Monthly posting patterns:")
        for month, count in monthly_counts.items():
            if pd.notna(month):
                month_name = month_names[int(month)-1]
                pct = count / len(df) * 100
                print(f"     {month_name}: {count:,} jobs ({pct:.1f}%)")
        
        # Yearly breakdown if multiple years present
        yearly_counts = df['open_year'].value_counts().sort_index()
        if len(yearly_counts) > 1:
            print("\n   Yearly breakdown:")
            for year, count in yearly_counts.items():
                if pd.notna(year):
                    print(f"     {int(year)}: {count:,} jobs")
    except Exception as e:
        print(f"Error: {e}")
    
    print(f"\n4. POSITION TITLES AND CLASSIFICATIONS ({year_info})")
    print("-" * 40)
    try:
        # Most common position titles
        top_positions = df['positionTitle'].value_counts().head(10)
        print("   Most common position titles:")
        for i, (title, count) in enumerate(top_positions.items(), 1):
            pct = count / len(df) * 100
            print(f"   {i:2d}. {title}: {count:,} ({pct:.1f}%)")
        
        # Grade level analysis if available
        if 'minimumGrade' in df.columns:
            grade_counts = df['minimumGrade'].value_counts().head(10)
            print(f"\n   Most common minimum grade levels:")
            for grade, count in grade_counts.items():
                if pd.notna(grade):
                    pct = count / len(df) * 100
                    print(f"     {grade}: {count:,} jobs ({pct:.1f}%)")
    except Exception as e:
        print(f"Error: {e}")
    
    print(f"\n5. WORK SCHEDULE AND ARRANGEMENTS ({year_info})")
    print("-" * 40)
    try:
        # Work schedule if available
        if 'workSchedule' in df.columns:
            schedule_counts = df['workSchedule'].value_counts().head(8)
            print("   Work schedule distribution:")
            for schedule, count in schedule_counts.items():
                if pd.notna(schedule):
                    pct = count / len(df) * 100
                    print(f"     {schedule}: {count:,} jobs ({pct:.1f}%)")
        
        # Remote work indicators (if available in data)
        if 'positionLocation' in df.columns:
            # Look for remote work keywords
            remote_keywords = ['remote', 'telework', 'virtual', 'anywhere']
            location_text = df['positionLocation'].astype(str).str.lower()
            remote_jobs = location_text.str.contains('|'.join(remote_keywords), na=False).sum()
            print(f"\n   Jobs mentioning remote/telework: {remote_jobs:,} ({remote_jobs/len(df)*100:.1f}%)")
    except Exception as e:
        print(f"Error: {e}")
    
    print(f"\n6. DATA QUALITY METRICS ({year_info})")
    print("-" * 40)
    try:
        print("   Data completeness:")
        key_fields = ['hiringAgencyName', 'positionTitle', 'minimumSalary', 'maximumSalary', 'positionOpenDate']
        for field in key_fields:
            if field in df.columns:
                non_null_count = df[field].notna().sum()
                completeness = non_null_count / len(df) * 100
                print(f"     {field}: {completeness:.1f}% complete ({non_null_count:,}/{len(df):,})")
        
        # Check for unique job control numbers
        if 'usajobsControlNumber' in df.columns:
            unique_controls = df['usajobsControlNumber'].nunique()
            total_records = len(df)
            duplicate_rate = (total_records - unique_controls) / total_records * 100
            print(f"\n   Unique job announcements: {unique_controls:,}")
            print(f"   Duplicate rate: {duplicate_rate:.2f}%")
            
            if duplicate_rate > 0:
                print(f"   Note: Duplicates may occur when combining Historical + Current API data")
                print(f"   Use df.drop_duplicates('usajobsControlNumber') to deduplicate")
    except Exception as e:
        print(f"Error: {e}")

def run_duckdb_examples(filenames=None):
    """
    Download USAJobs Parquet files, load them into DuckDB, 
    and run agency hiring trend queries across multiple years.
    
    Args:
        filenames (list): List of parquet filenames to download and analyze.
                         Defaults to recent years.
    """
    if filenames is None:
        filenames = [
            "historical_jobs_2024.parquet",
            "historical_jobs_2023.parquet",
            "historical_jobs_2022.parquet"
        ]
    
    print("\n" + "="*80)
    print("RUNNING DUCKDB MULTI-YEAR ANALYSIS")
    print(f"Files to process: {len(filenames)}")
    print("="*80 + "\n")

    # ---------- 1. Make sure download folder exists ----------
    download_dir = "download"
    ensure_directory_exists(download_dir)

    # ---------- 2. Check for local files first, then download if needed ----------
    base_url = "https://github.com/yourusername/usajobs_historic/raw/main/data/"
    local_files = []
    
    for filename in filenames:
        # Check if file exists locally in data/ directory
        local_data_path = os.path.join('data', filename)
        local_download_path = os.path.join(download_dir, filename)
        
        if os.path.exists(local_data_path):
            print(f"✓ Using local file: {local_data_path}")
            local_files.append(local_data_path)
        elif os.path.exists(local_download_path):
            print(f"✓ Using cached download: {local_download_path}")
            local_files.append(local_download_path)
        else:
            print(f"Downloading: {filename}")
            try:
                df_tmp = pd.read_parquet(base_url + filename)
                df_tmp.to_parquet(local_download_path)
                print(f"✓ Downloaded → {local_download_path}")
                local_files.append(local_download_path)
            except Exception as e:
                print(f"✗ Failed to download {filename}: {e}")

    if not local_files:
        print("No files available for analysis")
        return

    # ---------- 3. Create / connect to DuckDB and load files ----------
    db_path = os.path.join(download_dir, "usajobs.duckdb")
    con = duckdb.connect(db_path)
    print(f"\n✓ Connected to DuckDB database: {db_path}")
    
    # Create a unified view from all files
    con.execute("DROP VIEW IF EXISTS all_jobs")
    
    # Build UNION ALL query for all files
    union_parts = []
    for local_path in local_files:
        union_parts.append(f"SELECT * FROM read_parquet('{local_path}')")
    
    union_query = " UNION ALL ".join(union_parts)
    view_query = f"CREATE VIEW all_jobs AS {union_query}"
    
    print(f"Creating unified view from {len(local_files)} files...")
    con.execute(view_query)
    print("✓ Created unified all_jobs view")

    # ---------- 4. Run sample queries ----------
    
    # Query 1: Agency hiring trends by year
    print("\n" + "-"*60)
    print("AGENCY HIRING TRENDS BY YEAR")
    print("-"*60)
    
    query1 = """
        SELECT
            EXTRACT(year FROM positionOpenDate::DATE) AS year,
            hiringAgencyName,
            COUNT(*) AS job_count
        FROM all_jobs
        WHERE hiringAgencyName IS NOT NULL 
        AND positionOpenDate IS NOT NULL
        GROUP BY year, hiringAgencyName
        ORDER BY year DESC, job_count DESC
    """
    
    df_trends = con.execute(query1).fetchdf()
    
    # Show top 5 agencies for each year
    for year in sorted(df_trends['year'].unique(), reverse=True):
        year_data = df_trends[df_trends['year'] == year].head(5)
        print(f"\n{int(year)} - Top 5 Hiring Agencies:")
        for _, row in year_data.iterrows():
            print(f"  {row['hiringAgencyName']}: {row['job_count']:,} jobs")
    
    # Query 2: Salary trends over time
    print("\n" + "-"*60)
    print("SALARY TRENDS ANALYSIS")
    print("-"*60)
    
    query2 = """
        SELECT
            EXTRACT(year FROM positionOpenDate::DATE) AS year,
            AVG(minimumSalary::NUMERIC) AS avg_min_salary,
            AVG(maximumSalary::NUMERIC) AS avg_max_salary,
            MEDIAN(minimumSalary::NUMERIC) AS median_min_salary,
            MEDIAN(maximumSalary::NUMERIC) AS median_max_salary,
            COUNT(*) AS jobs_with_salary
        FROM all_jobs
        WHERE minimumSalary IS NOT NULL 
        AND maximumSalary IS NOT NULL
        AND minimumSalary::NUMERIC > 0
        AND maximumSalary::NUMERIC > 0
        GROUP BY year
        ORDER BY year DESC
    """
    
    df_salaries = con.execute(query2).fetchdf()
    
    print("Year | Avg Min | Avg Max | Median Min | Median Max | Jobs")
    print("-" * 65)
    for _, row in df_salaries.iterrows():
        year = int(row['year'])
        avg_min = row['avg_min_salary']
        avg_max = row['avg_max_salary']
        med_min = row['median_min_salary']
        med_max = row['median_max_salary']
        count = row['jobs_with_salary']
        print(f"{year} | ${avg_min:>6.0f} | ${avg_max:>6.0f} | ${med_min:>8.0f} | ${med_max:>8.0f} | {count:>4.0f}")

    # Query 3: Seasonal patterns
    print("\n" + "-"*60)
    print("SEASONAL HIRING PATTERNS")
    print("-"*60)
    
    query3 = """
        SELECT
            EXTRACT(month FROM positionOpenDate::DATE) AS month,
            COUNT(*) AS job_count,
            AVG(COUNT(*)) OVER() AS avg_monthly
        FROM all_jobs
        WHERE positionOpenDate IS NOT NULL
        GROUP BY month
        ORDER BY month
    """
    
    df_seasonal = con.execute(query3).fetchdf()
    
    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    
    print("Month | Job Count | vs Average")
    print("-" * 35)
    for _, row in df_seasonal.iterrows():
        month_idx = int(row['month']) - 1
        month_name = month_names[month_idx]
        count = row['job_count']
        avg = row['avg_monthly']
        vs_avg = (count / avg - 1) * 100
        print(f"{month_name:>5} | {count:>8,} | {vs_avg:>+6.1f}%")

    # ---------- 5. Finish up ----------
    con.close()
    print("\n✓ DuckDB connection closed.")

def cleanup_download_folder():
    """Show download folder contents and ask before removal"""
    download_dir = 'download'
    if os.path.exists(download_dir):
        print(f"\nDownload folder contents:")
        print("-" * 40)
        
        # Show folder contents and sizes
        total_size = 0
        for item in os.listdir(download_dir):
            item_path = os.path.join(download_dir, item)
            if os.path.isfile(item_path):
                size = os.path.getsize(item_path)
                total_size += size
                print(f"  {item} ({size / 1024 / 1024:.1f} MB)")
            else:
                print(f"  {item} (directory)")
        
        print(f"\nTotal size: {total_size / 1024 / 1024:.1f} MB")
        
        # Ask for confirmation
        response = input(f"\nDelete the '{download_dir}' folder and all its contents? (y/N): ").strip().lower()
        if response in ['y', 'yes']:
            print(f"Removing {download_dir} folder...")
            shutil.rmtree(download_dir)
            print("✓ Cleanup complete")
        else:
            print(f"✓ Keeping {download_dir} folder")
    else:
        print(f"\nNo {download_dir} folder found to clean up")

def main():
    """Main function to run all examples"""
    print("""
╔══════════════════════════════════════════════════════════════════╗
║                   USAJobs Data - Usage Examples                 ║
║                                                                  ║
║  This script demonstrates working with 2.97M job postings       ║
║  from 2013-2025.                                                ║
║                                                                  ║
║  Data source: https://github.com/yourusername/usajobs_historic  ║
╚══════════════════════════════════════════════════════════════════╝
    """)
    
    # Run local files examples
    df_local = run_local_examples()
    if df_local is not None:
        analyze_data(df_local, "LOCAL FILES")
        
        # Run DuckDB multi-year analysis
        run_duckdb_examples()
    else:
        print("\nNo local files found. To run examples:")
        print("1. Clone the repository and ensure data/ directory contains Parquet files")
        print("2. Or run the data collection pipeline first")
    
    print("\n" + "="*80)
    print("EXAMPLES COMPLETE")
    print("="*80)
    print("\nFor more information:")
    print("- Repository: https://github.com/yourusername/usajobs_historic")
    print("- Analysis Reports: analysis_2025.html, national_parks_analysis.html")
    print("- Official USAJobs: https://www.usajobs.gov/")

def main_with_output_capture():
    """Run main function and capture output to file"""
    output_file = "examples_output.txt"
    
    # Create a custom print function that writes to both console and file
    original_print = print
    output_buffer = StringIO()
    
    def dual_print(*args, **kwargs):
        # Print to console
        original_print(*args, **kwargs)
        # Also print to buffer (but not file= kwargs)
        if 'file' not in kwargs:
            original_print(*args, **kwargs, file=output_buffer)
    
    # Temporarily replace print function
    import builtins
    builtins.print = dual_print
    
    try:
        # Run the main function
        main()
        
        # Write buffer contents to file
        with open(output_file, 'w') as f:
            f.write(output_buffer.getvalue())
        
        original_print(f"\n✓ Output saved to: {output_file}")
        
    finally:
        # Restore original function
        builtins.print = original_print
        output_buffer.close()

if __name__ == "__main__":
    main_with_output_capture()