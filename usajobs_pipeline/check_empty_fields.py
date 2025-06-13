import duckdb

conn = duckdb.connect('data/unified_pipeline_20250613_042206.duckdb')

# Get all column names  
columns = [row[1] for row in conn.execute("PRAGMA table_info(unified_jobs)").fetchall()]

# Check coverage for each field
print("Field coverage analysis:")
print("=" * 50)

empty_fields = []
low_coverage = []

for column in columns:
    if column in ['data_sources', 'rationalization_date', 'confidence_score']:
        continue  # Skip metadata fields
    
    try:
        if column in ['min_salary', 'max_salary', 'total_openings', 'open_date', 'close_date', 'posted_date']:
            # Numeric/date fields - just check NOT NULL
            result = conn.execute(f"SELECT COUNT(*) as total, COUNT(CASE WHEN {column} IS NOT NULL THEN 1 END) as filled FROM unified_jobs").fetchone()
        else:
            # String fields - check NOT NULL and not empty
            result = conn.execute(f"SELECT COUNT(*) as total, COUNT(CASE WHEN {column} IS NOT NULL AND {column} != '' THEN 1 END) as filled FROM unified_jobs").fetchone()
        
        total, filled = result
        coverage = (filled / total * 100) if total > 0 else 0
        
        if coverage == 0:
            empty_fields.append(column)
            print(f"❌ {column}: {coverage:.1f}% ({filled}/{total})")
        elif coverage < 10:
            low_coverage.append(column)
            print(f"⚠️  {column}: {coverage:.1f}% ({filled}/{total})")
        elif coverage < 50:
            print(f"📊 {column}: {coverage:.1f}% ({filled}/{total})")
        else:
            print(f"✅ {column}: {coverage:.1f}% ({filled}/{total})")
            
    except Exception as e:
        print(f"❓ {column}: Error checking - {e}")

print("\n" + "=" * 50)
print(f"📋 Summary:")
print(f"   Empty fields (0%): {len(empty_fields)}")
for field in empty_fields:
    print(f"     - {field}")
    
print(f"   Low coverage (<10%): {len(low_coverage)}")
for field in low_coverage:
    print(f"     - {field}")

conn.close()