#!/usr/bin/env python3
"""Check what happened with the pipeline run"""

import duckdb
import os
from pathlib import Path
from datetime import datetime

def check_pipeline_status():
    data_dir = Path("data")
    timestamp = "20250612_165611"
    
    print("🔍 Checking pipeline status for run:", timestamp)
    print("="*50)
    
    # Check temp files
    temp_files = list(data_dir.glob(f"temp_jobs_*_pipeline_{timestamp}.duckdb"))
    print(f"\n📁 Found {len(temp_files)} temp files")
    
    if temp_files:
        total_temp_jobs = 0
        for tf in temp_files[:5]:  # Check first 5
            try:
                conn = duckdb.connect(str(tf))
                count = conn.execute("SELECT COUNT(*) FROM historical_jobs").fetchone()[0]
                total_temp_jobs += count
                print(f"  - {tf.name}: {count} jobs")
                conn.close()
            except Exception as e:
                print(f"  - {tf.name}: ERROR - {e}")
        print(f"  ... and {len(temp_files)-5} more files")
    
    # Check final historical database
    hist_db = data_dir / f"historical_jobs_pipeline_{timestamp}.duckdb"
    if hist_db.exists():
        print(f"\n📊 Historical database: {hist_db.name}")
        try:
            conn = duckdb.connect(str(hist_db))
            hist_count = conn.execute("SELECT COUNT(*) FROM historical_jobs").fetchone()[0]
            scraped_count = conn.execute("SELECT COUNT(*) FROM scraped_jobs WHERE scraping_success = true").fetchone()[0]
            print(f"  - Historical jobs: {hist_count}")
            print(f"  - Successfully scraped: {scraped_count}")
            conn.close()
        except Exception as e:
            print(f"  - ERROR: {e}")
    else:
        print(f"\n❌ Historical database not found: {hist_db.name}")
    
    # Check current jobs
    current_files = list(data_dir.glob("current_jobs_*.json"))
    latest_current = max(current_files, key=os.path.getctime) if current_files else None
    if latest_current:
        print(f"\n📄 Latest current jobs file: {latest_current.name}")
        # Get file stats
        stat = os.stat(latest_current)
        mod_time = datetime.fromtimestamp(stat.st_mtime)
        print(f"  - Modified: {mod_time}")
        print(f"  - Size: {stat.st_size:,} bytes")
    
    # Check unified database
    unified_db = data_dir / f"unified_pipeline_{timestamp}.duckdb"
    if unified_db.exists():
        print(f"\n✅ Unified database: {unified_db.name}")
        try:
            conn = duckdb.connect(str(unified_db))
            # Check tables
            tables = conn.execute("SHOW TABLES").fetchall()
            print(f"  - Tables: {[t[0] for t in tables]}")
            
            if any(t[0] == 'unified_jobs' for t in tables):
                unified_count = conn.execute("SELECT COUNT(*) FROM unified_jobs").fetchone()[0]
                print(f"  - Unified jobs: {unified_count}")
            conn.close()
        except Exception as e:
            print(f"  - ERROR: {e}")
    else:
        print(f"\n❌ Unified database not found: {unified_db.name}")
    
    # Check HTML report
    html_file = Path("rationalization_analysis.html")
    if html_file.exists():
        stat = os.stat(html_file)
        mod_time = datetime.fromtimestamp(stat.st_mtime)
        print(f"\n📝 HTML Report:")
        print(f"  - Last modified: {mod_time}")
        print(f"  - Size: {stat.st_size:,} bytes")
        
        # Check if it's older than the pipeline run
        pipeline_time = datetime.strptime(timestamp, "%Y%m%d_%H%M%S")
        if mod_time < pipeline_time:
            print(f"  - ⚠️ WARNING: Report is older than pipeline run!")
    
    # Cleanup recommendation
    print("\n🧹 Cleanup recommendation:")
    if temp_files:
        print(f"  - {len(temp_files)} temp files can be deleted")
        print("  - Run: rm data/temp_jobs_*_pipeline_20250612_165611.duckdb")

if __name__ == "__main__":
    check_pipeline_status()