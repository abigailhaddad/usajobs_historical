#!/usr/bin/env python3
"""
Export all product manager related jobs from DuckDB databases to CSV
"""

import duckdb
import glob
import os

def main():
    # Find all DuckDB files
    duckdb_files = sorted(glob.glob("data/duckdb/usajobs_*.duckdb"))
    
    if not duckdb_files:
        print("❌ No DuckDB files found in data/duckdb/")
        return
    
    print(f"🔍 Found {len(duckdb_files)} DuckDB files to process")
    print()
    
    # Create a temporary in-memory database to combine results
    conn = duckdb.connect(':memory:')
    
    total_found = 0
    first_db_with_data = True
    
    # Process each file
    for db_file in duckdb_files:
        try:
            print(f"  Processing {os.path.basename(db_file)}...")
            
            # Attach the database
            conn.execute(f"ATTACH '{db_file}' AS db_{os.path.basename(db_file).replace('.duckdb', '').replace('-', '_')}")
            
            # Count product manager jobs in this database
            count_query = f"""
            SELECT COUNT(*) FROM db_{os.path.basename(db_file).replace('.duckdb', '').replace('-', '_')}.historical_jobs 
            WHERE 
                LOWER(position_title) LIKE '%product manager%'
                OR LOWER(position_title) LIKE '%product management%'
                OR LOWER(position_title) LIKE '%product owner%'
                OR LOWER(position_title) LIKE '%product lead%'
                OR LOWER(position_title) LIKE '%head of product%'
                OR LOWER(position_title) LIKE '%director of product%'
                OR LOWER(position_title) LIKE '%vp of product%'
                OR LOWER(position_title) LIKE '%vice president of product%'
                OR LOWER(position_title) LIKE '%chief product officer%'
                OR (LOWER(position_title) LIKE '%product%' AND LOWER(position_title) LIKE '%strategist%')
            """
            
            count = conn.execute(count_query).fetchone()[0]
            print(f"    Found {count} product manager jobs")
            total_found += count
            
            # If this is the first database with data, create the table from it
            if count > 0 and first_db_with_data:
                first_db_with_data = False
                conn.execute(f"""
                CREATE OR REPLACE TABLE all_product_jobs AS 
                SELECT * FROM db_{os.path.basename(db_file).replace('.duckdb', '').replace('-', '_')}.historical_jobs 
                WHERE 
                    LOWER(position_title) LIKE '%product manager%'
                    OR LOWER(position_title) LIKE '%product management%'
                    OR LOWER(position_title) LIKE '%product owner%'
                    OR LOWER(position_title) LIKE '%product lead%'
                    OR LOWER(position_title) LIKE '%head of product%'
                    OR LOWER(position_title) LIKE '%director of product%'
                    OR LOWER(position_title) LIKE '%vp of product%'
                    OR LOWER(position_title) LIKE '%vice president of product%'
                    OR LOWER(position_title) LIKE '%chief product officer%'
                    OR (LOWER(position_title) LIKE '%product%' AND LOWER(position_title) LIKE '%strategist%')
                """)
            elif count > 0:
                # Insert into existing table
                conn.execute(f"""
                INSERT INTO all_product_jobs 
                SELECT * FROM db_{os.path.basename(db_file).replace('.duckdb', '').replace('-', '_')}.historical_jobs 
                WHERE 
                    LOWER(position_title) LIKE '%product manager%'
                    OR LOWER(position_title) LIKE '%product management%'
                    OR LOWER(position_title) LIKE '%product owner%'
                    OR LOWER(position_title) LIKE '%product lead%'
                    OR LOWER(position_title) LIKE '%head of product%'
                    OR LOWER(position_title) LIKE '%director of product%'
                    OR LOWER(position_title) LIKE '%vp of product%'
                    OR LOWER(position_title) LIKE '%vice president of product%'
                    OR LOWER(position_title) LIKE '%chief product officer%'
                    OR (LOWER(position_title) LIKE '%product%' AND LOWER(position_title) LIKE '%strategist%')
                """)
            
            # Detach the database
            conn.execute(f"DETACH db_{os.path.basename(db_file).replace('.duckdb', '').replace('-', '_')}")
            
        except Exception as e:
            print(f"  ❌ Error processing {db_file}: {e}")
    
    if total_found > 0:
        # Remove duplicates and get final count
        conn.execute("CREATE OR REPLACE TABLE final_product_jobs AS SELECT DISTINCT * FROM all_product_jobs")
        final_count = conn.execute("SELECT COUNT(*) FROM final_product_jobs").fetchone()[0]
        unique_control_numbers = conn.execute("SELECT COUNT(DISTINCT control_number) FROM final_product_jobs").fetchone()[0]
        
        print()
        print(f"📊 Total product manager jobs found: {final_count}")
        print(f"   Unique job postings (by control_number): {unique_control_numbers}")
        
        # Export to CSV
        output_path = "data/exports/product_manager_jobs.csv"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        conn.execute(f"""
        COPY (
            SELECT DISTINCT * FROM final_product_jobs 
            ORDER BY position_open_date DESC
        ) TO '{output_path}' (HEADER, DELIMITER ',')
        """)
        
        print(f"✅ Exported to {output_path}")
        
        # Show summary statistics
        print()
        print("📈 Summary:")
        
        date_range = conn.execute("SELECT MIN(position_open_date), MAX(position_open_date) FROM final_product_jobs").fetchone()
        print(f"  Date range: {date_range[0]} to {date_range[1]}")
        
        unique_agencies = conn.execute("SELECT COUNT(DISTINCT hiring_agency_name) FROM final_product_jobs").fetchone()[0]
        print(f"  Unique agencies: {unique_agencies}")
        
        # Try to get location info if column exists
        try:
            unique_locations = conn.execute("SELECT COUNT(DISTINCT position_location_city_name) FROM final_product_jobs").fetchone()[0]
            print(f"  Unique locations: {unique_locations}")
        except:
            # Column might not exist in older data
            pass
        
        # Top agencies
        print()
        print("🏢 Top 10 hiring agencies:")
        top_agencies = conn.execute("""
            SELECT hiring_agency_name, COUNT(*) as count 
            FROM final_product_jobs 
            GROUP BY hiring_agency_name 
            ORDER BY count DESC 
            LIMIT 10
        """).fetchall()
        
        for agency, count in top_agencies:
            print(f"  - {agency}: {count} jobs")
    else:
        print("❌ No product manager jobs found in any database")
    
    conn.close()

if __name__ == "__main__":
    main()