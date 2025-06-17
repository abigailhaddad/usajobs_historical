#!/bin/bash
# Export all DuckDB files to PostgreSQL

echo "🚀 Exporting all DuckDB files to PostgreSQL..."
echo ""

# Check for DuckDB files
DUCKDB_FILES=(data/usajobs_*.duckdb)
if [ ${#DUCKDB_FILES[@]} -eq 0 ] || [ ! -e "${DUCKDB_FILES[0]}" ]; then
    echo "❌ No DuckDB files found in data/ directory!"
    exit 1
fi

echo "📊 Found ${#DUCKDB_FILES[@]} DuckDB files to export:"
for db in "${DUCKDB_FILES[@]}"; do
    echo "  - $db"
done
echo ""

# Export each file
for db in "${DUCKDB_FILES[@]}"; do
    echo "📤 Exporting $db..."
    python /Users/abigailhaddad/Documents/repos/usajobs_historic/scripts/export_postgres.py "$db" 8
    echo ""
done

echo "✅ All exports complete!"
echo ""
echo "🔍 Verify with:"
echo "  python scripts/check_data.py"
