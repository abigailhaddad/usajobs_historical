#!/bin/bash
# Export all Parquet files to PostgreSQL

echo "🚀 Exporting all Parquet files to PostgreSQL..."
echo ""

# Check for Parquet files
HISTORICAL_FILES=(data/historical_jobs_*.parquet)
CURRENT_FILES=(data/current_jobs_*.parquet)

if [ ! -e "${HISTORICAL_FILES[0]}" ] && [ ! -e "${CURRENT_FILES[0]}" ]; then
    echo "❌ No Parquet files found in data/ directory!"
    echo "💡 Run data collection first:"
    echo "   scripts/run_single.sh current-all"
    echo "   scripts/run_single.sh range 2024-01-01 2024-12-31"
    exit 1
fi

TOTAL_FILES=0

if [ -e "${HISTORICAL_FILES[0]}" ]; then
    TOTAL_FILES=$((TOTAL_FILES + ${#HISTORICAL_FILES[@]}))
fi

if [ -e "${CURRENT_FILES[0]}" ]; then
    TOTAL_FILES=$((TOTAL_FILES + ${#CURRENT_FILES[@]}))
fi

echo "📊 Found $TOTAL_FILES Parquet files to export:"

if [ -e "${HISTORICAL_FILES[0]}" ]; then
    echo "  📈 Historical jobs:"
    for file in "${HISTORICAL_FILES[@]}"; do
        echo "    - $(basename $file)"
    done
fi

if [ -e "${CURRENT_FILES[0]}" ]; then
    echo "  📊 Current jobs:"
    for file in "${CURRENT_FILES[@]}"; do
        echo "    - $(basename $file)"
    done
fi

echo ""

# Export all files using the updated export script
echo "📤 Exporting all data..."
python /Users/abigailhaddad/Documents/repos/usajobs_historic/scripts/export_postgres.py all 8

echo ""
echo "✅ All exports complete!"
echo ""
echo "🔍 Verify with:"
echo "  python scripts/check_data.py"