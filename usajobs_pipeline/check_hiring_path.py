import duckdb

conn = duckdb.connect('data/unified_pipeline_20250613_042206.duckdb')

result = conn.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN hiring_path IS NOT NULL AND hiring_path != '' THEN 1 END) as has_hiring_path
    FROM unified_jobs 
    WHERE data_sources LIKE '%current%'
""").fetchone()

print('Hiring path coverage in current jobs:')
print(f'  Total: {result[0]}')
print(f'  Has hiring_path: {result[1]} ({result[1]/result[0]*100:.1f}%)')

# Show some samples
samples = conn.execute("""
    SELECT control_number, hiring_path 
    FROM unified_jobs 
    WHERE data_sources LIKE '%current%' AND hiring_path IS NOT NULL 
    LIMIT 5
""").fetchall()

print('\nSample hiring paths:')
for control, path in samples:
    print(f'  {control}: {path}')

conn.close()