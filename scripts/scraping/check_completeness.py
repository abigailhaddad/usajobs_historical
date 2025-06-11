#!/usr/bin/env python3
"""
Check completeness of scraped data across all years
Shows detailed field coverage for each database
"""

import duckdb
import glob
import os
from datetime import datetime

def check_year_completeness(db_path):
    """Check field completeness for a specific year database"""
    try:
        conn = duckdb.connect(db_path, read_only=True)
        
        # Check if scraping columns exist
        columns = conn.execute("PRAGMA table_info(historical_jobs)").fetchall()
        column_names = [col[1] for col in columns]
        
        has_scrape_columns = all(col in column_names for col in ['job_summary', 'job_duties', 'job_qualifications', 'job_requirements', 'scrape_status'])
        
        # Get basic counts
        total = conn.execute("SELECT COUNT(*) FROM historical_jobs").fetchone()[0]
        
        result = {
            'year': os.path.basename(db_path).replace('usajobs_', '').replace('.duckdb', ''),
            'total': total,
            'has_scrape_columns': has_scrape_columns
        }
        
        if not has_scrape_columns:
            result['status'] = 'not_started'
            result['scraped'] = 0
            result['failed'] = 0
            result['pending'] = total
            conn.close()
            return result
        
        # Get detailed field completeness
        field_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_rows,
                -- Scrape status
                COUNT(CASE WHEN scrape_status = 'completed' THEN 1 END) as completed,
                COUNT(CASE WHEN scrape_status = 'failed' THEN 1 END) as failed,
                COUNT(CASE WHEN scrape_status IS NULL OR scrape_status = 'pending' THEN 1 END) as pending,
                -- Field completeness (non-empty)
                COUNT(CASE WHEN job_summary IS NOT NULL AND job_summary != '' THEN 1 END) as has_summary,
                COUNT(CASE WHEN job_duties IS NOT NULL AND job_duties != '' THEN 1 END) as has_duties,
                COUNT(CASE WHEN job_qualifications IS NOT NULL AND job_qualifications != '' THEN 1 END) as has_qualifications,
                COUNT(CASE WHEN job_requirements IS NOT NULL AND job_requirements != '' THEN 1 END) as has_requirements,
                -- Average lengths for completed jobs
                AVG(CASE WHEN scrape_status = 'completed' THEN LENGTH(job_summary) END) as avg_summary_len,
                AVG(CASE WHEN scrape_status = 'completed' THEN LENGTH(job_duties) END) as avg_duties_len,
                AVG(CASE WHEN scrape_status = 'completed' THEN LENGTH(job_qualifications) END) as avg_qual_len,
                AVG(CASE WHEN scrape_status = 'completed' THEN LENGTH(job_requirements) END) as avg_req_len,
                -- Last scraped
                MAX(scraped_at) as last_scraped
            FROM historical_jobs
        """).fetchone()
        
        result.update({
            'scraped': field_stats[1],
            'failed': field_stats[2],
            'pending': field_stats[3],
            'has_summary': field_stats[4],
            'has_duties': field_stats[5],
            'has_qualifications': field_stats[6],
            'has_requirements': field_stats[7],
            'avg_summary_len': int(field_stats[8] or 0),
            'avg_duties_len': int(field_stats[9] or 0),
            'avg_qual_len': int(field_stats[10] or 0),
            'avg_req_len': int(field_stats[11] or 0),
            'last_scraped': field_stats[12]
        })
        
        # Determine overall status
        if result['pending'] == result['total']:
            result['status'] = 'not_started'
        elif result['pending'] == 0:
            result['status'] = 'complete'
        else:
            result['status'] = 'in_progress'
        
        conn.close()
        return result
        
    except Exception as e:
        return {
            'year': os.path.basename(db_path).replace('usajobs_', '').replace('.duckdb', ''),
            'error': str(e)
        }

def main():
    print("USAJobs Scraping Completeness Report")
    print("=" * 120)
    
    # Find all DuckDB files
    db_files = sorted(glob.glob("../../data/duckdb/usajobs_*.duckdb"))
    
    if not db_files:
        print("No database files found!")
        return
    
    # Headers
    print(f"{'Year':<6} {'Status':<12} {'Total':>8} {'Scraped':>8} {'Failed':>8} {'Pending':>8} | "
          f"{'Summary':>8} {'Duties':>8} {'Quals':>8} {'Reqs':>8} | "
          f"{'Avg Lengths (Summary/Duties/Quals/Reqs)'}")
    print("-" * 120)
    
    totals = {
        'total': 0, 'scraped': 0, 'failed': 0, 'pending': 0,
        'has_summary': 0, 'has_duties': 0, 'has_qualifications': 0, 'has_requirements': 0
    }
    
    for db_path in db_files:
        stats = check_year_completeness(db_path)
        
        if 'error' in stats:
            print(f"{stats['year']:<6} Error: {stats['error']}")
            continue
        
        # Update totals
        for key in totals:
            totals[key] += stats.get(key, 0)
        
        # Format output
        if stats['has_scrape_columns']:
            field_coverage = (
                f"{stats['has_summary']:>8,} "
                f"{stats['has_duties']:>8,} "
                f"{stats['has_qualifications']:>8,} "
                f"{stats['has_requirements']:>8,}"
            )
            
            avg_lengths = (
                f"{stats['avg_summary_len']:>7,}/"
                f"{stats['avg_duties_len']:>7,}/"
                f"{stats['avg_qual_len']:>6,}/"
                f"{stats['avg_req_len']:>6,}"
            )
        else:
            field_coverage = f"{'N/A':>8} {'N/A':>8} {'N/A':>8} {'N/A':>8}"
            avg_lengths = "Not scraped yet"
        
        print(f"{stats['year']:<6} {stats['status']:<12} "
              f"{stats['total']:>8,} {stats['scraped']:>8,} "
              f"{stats['failed']:>8,} {stats['pending']:>8,} | "
              f"{field_coverage} | {avg_lengths}")
    
    # Print totals
    print("-" * 120)
    print(f"{'TOTAL':<6} {'':<12} "
          f"{totals['total']:>8,} {totals['scraped']:>8,} "
          f"{totals['failed']:>8,} {totals['pending']:>8,} | "
          f"{totals['has_summary']:>8,} {totals['has_duties']:>8,} "
          f"{totals['has_qualifications']:>8,} {totals['has_requirements']:>8,}")
    
    # Summary statistics
    if totals['total'] > 0:
        percent_complete = (totals['scraped'] / totals['total']) * 100
        print(f"\nOverall completion: {percent_complete:.1f}%")
        
        if totals['scraped'] > 0:
            print(f"\nField coverage (of scraped jobs):")
            print(f"  Summary: {totals['has_summary'] / totals['scraped'] * 100:.1f}%")
            print(f"  Duties: {totals['has_duties'] / totals['scraped'] * 100:.1f}%")
            print(f"  Qualifications: {totals['has_qualifications'] / totals['scraped'] * 100:.1f}%")
            print(f"  Requirements: {totals['has_requirements'] / totals['scraped'] * 100:.1f}%")

if __name__ == "__main__":
    main()