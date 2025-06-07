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
    cur.execute('SELECT COUNT(*) FROM jobs;')
    total = cur.fetchone()[0]
    
    cur.execute('SELECT COUNT(*) FROM jobs WHERE generated_title IS NOT NULL;')
    with_titles = cur.fetchone()[0]
    
    print(f"📊 Database Statistics:")
    print(f"   Total jobs: {total}")
    print(f"   With generated titles: {with_titles}")
    print(f"   Coverage: {with_titles/total*100:.1f}%" if total > 0 else "   Coverage: 0%")
    
    # Show recent jobs
    cur.execute("""
        SELECT title, generated_title, organization_name, created_at 
        FROM jobs 
        WHERE generated_title IS NOT NULL 
        ORDER BY created_at DESC 
        LIMIT 10
    """)
    
    print(f"\n📝 Recent jobs:")
    for original, generated, org, created in cur.fetchall():
        print(f"   {original} → {generated}")
        print(f"      {org} ({created.strftime('%Y-%m-%d %H:%M')})")
        print()
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    check_database()