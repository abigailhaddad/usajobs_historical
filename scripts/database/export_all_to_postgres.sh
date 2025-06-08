#!/bin/bash
# Export all DuckDB files to PostgreSQL

echo "🚀 Exporting all DuckDB files to PostgreSQL..."
echo ""

# Check for DuckDB files
DUCKDB_FILES=(usajobs_*.duckdb)
if [ ${#DUCKDB_FILES[@]} -eq 0 ]; then
    echo "❌ No DuckDB files found!"
    exit 1
fi

echo "📊 Found ${#DUCKDB_FILES[@]} DuckDB files to export:"
for db in "${DUCKDB_FILES[@]}"; do
    echo "  - $db"
done
echo ""

# Export each file
source venv/bin/activate

for db in "${DUCKDB_FILES[@]}"; do
    echo "📤 Exporting $db..."
    python fast_postgres_export.py "$db" 8
    echo ""
done

echo "✅ All exports complete!"
echo ""
echo "🔍 Verify with:"
echo "  python check_counts.py"
