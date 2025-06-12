#!/usr/bin/env python3
"""
Interactive DuckDB query tool for USAJobs historical data
"""

import duckdb
import argparse
import sys

def query_database(db_path: str, query: str = None):
    """Connect to DuckDB and run queries."""
    try:
        conn = duckdb.connect(db_path, read_only=True)
        
        if query:
            # Run single query
            result = conn.execute(query).fetchall()
            # Get column names
            columns = [desc[0] for desc in conn.description]
            
            # Print header
            print("\t".join(columns))
            print("-" * 80)
            
            # Print rows
            for row in result:
                print("\t".join(str(val) for val in row))
        else:
            # Interactive mode
            print(f"Connected to: {db_path}")
            print("Type 'help' for sample queries, 'exit' to quit\n")
            
            while True:
                try:
                    user_input = input("duckdb> ").strip()
                    
                    if user_input.lower() == 'exit':
                        break
                    elif user_input.lower() == 'help':
                        print_help()
                    elif user_input:
                        result = conn.execute(user_input).fetchall()
                        # Get column names
                        columns = [desc[0] for desc in conn.description]
                        
                        # Print header
                        print("\t".join(columns))
                        print("-" * 80)
                        
                        # Print rows
                        for row in result:
                            print("\t".join(str(val) for val in row))
                        print()
                except Exception as e:
                    print(f"Error: {e}\n")
        
        conn.close()
        
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        sys.exit(1)

def print_help():
    """Print sample queries."""
    print("\n📊 Sample Queries:")
    print("\n-- Basic statistics:")
    print("SELECT COUNT(*) as total_jobs FROM historical_jobs;")
    print("\n-- Top 10 hiring agencies:")
    print("SELECT hiring_agency_name, COUNT(*) as job_count")
    print("FROM historical_jobs")
    print("GROUP BY hiring_agency_name")
    print("ORDER BY job_count DESC")
    print("LIMIT 10;")
    print("\n-- Jobs by status:")
    print("SELECT position_opening_status, COUNT(*) as count")
    print("FROM historical_jobs")
    print("GROUP BY position_opening_status")
    print("ORDER BY count DESC;")
    print("\n-- Recent IT jobs (2210 series):")
    print("SELECT position_title, hiring_agency_name, position_open_date")
    print("FROM historical_jobs")
    print("WHERE job_series LIKE '%2210%'")
    print("ORDER BY position_open_date DESC")
    print("LIMIT 20;")
    print("\n-- Salary ranges:")
    print("SELECT")
    print("    pay_scale,")
    print("    MIN(minimum_salary) as min_sal,")
    print("    AVG(minimum_salary) as avg_min_sal,")
    print("    AVG(maximum_salary) as avg_max_sal,")
    print("    MAX(maximum_salary) as max_sal,")
    print("    COUNT(*) as count")
    print("FROM historical_jobs")
    print("WHERE minimum_salary IS NOT NULL")
    print("GROUP BY pay_scale")
    print("ORDER BY count DESC;")
    print("\n-- Export to CSV:")
    print("COPY (SELECT * FROM historical_jobs WHERE position_open_date >= '2024-01-01') TO 'jobs_2024.csv' (HEADER, DELIMITER ',');")
    print()

def main():
    parser = argparse.ArgumentParser(description="Query USAJobs DuckDB database")
    parser.add_argument("database", help="Path to DuckDB database file")
    parser.add_argument("-q", "--query", help="Run single query and exit")
    
    args = parser.parse_args()
    
    query_database(args.database, args.query)

if __name__ == "__main__":
    main()