#!/usr/bin/env python3
"""
Reset databases - drop and recreate tables
"""

import argparse
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def reset_postgresql():
    """Drop and recreate the PostgreSQL historical_jobs table."""
    conn_str = os.getenv("DATABASE_URL")
    if not conn_str:
        print("❌ DATABASE_URL not found in environment variables")
        return False
    
    try:
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        
        print("🗑️  Dropping PostgreSQL historical_jobs table...")
        cur.execute("DROP TABLE IF EXISTS historical_jobs CASCADE;")
        
        print("🔨 Recreating PostgreSQL historical_jobs table...")
        with open("sql/create_historical_jobs.sql", "r") as f:
            sql = f.read()
            cur.execute(sql)
        
        conn.commit()
        cur.close()
        conn.close()
        
        print("✅ PostgreSQL database reset complete!")
        return True
        
    except Exception as e:
        print(f"❌ PostgreSQL reset failed: {e}")
        return False

def reset_duckdb(db_path):
    """Delete DuckDB file."""
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
            print(f"✅ Deleted DuckDB file: {db_path}")
            return True
        except Exception as e:
            print(f"❌ Failed to delete DuckDB file: {e}")
            return False
    else:
        print(f"ℹ️  DuckDB file not found: {db_path}")
        return True

def main():
    parser = argparse.ArgumentParser(description="Reset databases")
    parser.add_argument("--postgresql", action="store_true", help="Reset PostgreSQL database")
    parser.add_argument("--duckdb", help="Path to DuckDB file to delete")
    parser.add_argument("--all", action="store_true", help="Reset all databases")
    
    args = parser.parse_args()
    
    if not any([args.postgresql, args.duckdb, args.all]):
        print("Please specify at least one database to reset:")
        print("  --postgresql     Reset PostgreSQL database")
        print("  --duckdb PATH    Delete DuckDB file")
        print("  --all           Reset all databases")
        return
    
    print("⚠️  WARNING: This will DELETE ALL DATA in the specified databases!")
    try:
        response = input("Are you sure? (yes/no): ")
    except EOFError:
        print("Aborted (no input provided).")
        return
    
    if response.lower() != "yes":
        print("Aborted.")
        return
    
    if args.all or args.postgresql:
        reset_postgresql()
    
    if args.duckdb:
        reset_duckdb(args.duckdb)
    elif args.all:
        # Look for common DuckDB files
        for db_file in ["jobs.duckdb", "historical_jobs.duckdb", "usajobs.duckdb"]:
            if os.path.exists(db_file):
                reset_duckdb(db_file)

if __name__ == "__main__":
    main()