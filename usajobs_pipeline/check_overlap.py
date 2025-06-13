import duckdb
import json

conn = duckdb.connect('data/unified_pipeline_20250613_042206.duckdb')

# Find an overlapping job
overlap_job = conn.execute("""
    WITH historical_controls AS (
        SELECT DISTINCT control_number FROM unified_jobs WHERE data_sources LIKE '%historical%'
    ),
    current_controls AS (
        SELECT DISTINCT control_number FROM unified_jobs WHERE data_sources LIKE '%current%'
    )
    SELECT h.control_number
    FROM historical_controls h
    INNER JOIN current_controls c ON h.control_number = c.control_number
    LIMIT 1
""").fetchone()

if overlap_job:
    control_num = overlap_job[0]
    print(f"Checking overlap job: {control_num}")
    print("=" * 50)
    
    # Get records for this control number
    records = conn.execute("""
        SELECT data_sources, position_title, agency_name, job_series, 
               min_grade, max_grade, hiring_path, major_duties
        FROM unified_jobs 
        WHERE control_number = ?
        ORDER BY data_sources
    """, [control_num]).fetchall()
    
    print(f"Found {len(records)} records for this control number:")
    print()
    
    for i, record in enumerate(records, 1):
        data_sources, title, agency, series, min_grade, max_grade, hiring_path, duties = record
        sources_list = json.loads(data_sources) if data_sources else []
        
        print(f"Record {i}:")
        print(f"  Data Sources: {sources_list}")
        print(f"  Title: {title}")
        print(f"  Agency: {agency}")
        print(f"  Series: {series}")
        print(f"  Grades: {min_grade}-{max_grade}")
        print(f"  Hiring Path: {hiring_path}")
        print(f"  Duties Length: {len(duties) if duties else 0} chars")
        print()
        
    # Check if these are actually different records or the same
    if len(records) > 1:
        print("COMPARISON:")
        print(f"  Same title? {records[0][1] == records[1][1] if len(records) > 1 else 'N/A'}")
        print(f"  Same agency? {records[0][2] == records[1][2] if len(records) > 1 else 'N/A'}")
        print(f"  Same grades? {records[0][4] == records[1][4] and records[0][5] == records[1][5] if len(records) > 1 else 'N/A'}")
        print(f"  Same hiring path? {records[0][6] == records[1][6] if len(records) > 1 else 'N/A'}")

else:
    print("No overlapping jobs found!")

conn.close()