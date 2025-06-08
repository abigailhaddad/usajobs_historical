#!/usr/bin/env python3
"""Quick script to check database contents"""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def check_database():
    conn_str = os.getenv('DATABASE_URL')
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    
    # Basic stats
    cur.execute('SELECT COUNT(*) FROM historical_jobs;')
    total = cur.fetchone()[0]
    
    # Count by agency
    cur.execute("""
        SELECT hiring_agency_name, COUNT(*) as cnt 
        FROM historical_jobs 
        GROUP BY hiring_agency_name 
        ORDER BY cnt DESC 
        LIMIT 5
    """)
    top_agencies = cur.fetchall()
    
    # Count by status
    cur.execute("""
        SELECT position_opening_status, COUNT(*) as cnt 
        FROM historical_jobs 
        GROUP BY position_opening_status 
        ORDER BY cnt DESC
    """)
    status_counts = cur.fetchall()
    
    print(f"📊 Database Statistics:")
    print(f"   Total jobs: {total:,}")
    
    print(f"\n🏛️  Top Agencies:")
    for agency, count in top_agencies:
        print(f"   {agency}: {count:,}")
    
    print(f"\n📈 Job Status:")
    for status, count in status_counts:
        print(f"   {status or 'Unknown'}: {count:,}")
    
    # Show recent jobs
    cur.execute("""
        SELECT position_title, hiring_agency_name, position_open_date, 
               position_close_date, position_opening_status
        FROM historical_jobs 
        ORDER BY created_at DESC 
        LIMIT 5
    """)
    
    print(f"\n📝 Recent jobs added:")
    for title, agency, open_date, close_date, status in cur.fetchall():
        print(f"\n   {title}")
        print(f"   Agency: {agency}")
        print(f"   Dates: {open_date} to {close_date}")
        print(f"   Status: {status}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    check_database()