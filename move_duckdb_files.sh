#!/bin/bash
# Move DuckDB files from scripts/pipeline to data/duckdb

echo "🔄 Moving DuckDB files to data/duckdb directory..."

# Create data/duckdb directory if it doesn't exist
mkdir -p data/duckdb

# Count files to move
DUCKDB_FILES=(usajobs_*.duckdb)
if [ ! -f "${DUCKDB_FILES[0]}" ]; then
    echo "❌ No DuckDB files found in current directory"
    exit 1
fi

echo "📁 Found ${#DUCKDB_FILES[@]} DuckDB files to move:"

# Move each file and show progress
for file in "${DUCKDB_FILES[@]}"; do
    if [ -f "$file" ]; then
        filename=$(basename "$file")
        echo "  Moving $filename..."
        
        # Check if file already exists in destination
        if [ -f "data/duckdb/$filename" ]; then
            echo "    ⚠️  File already exists in data/duckdb/, backing up as ${filename}.backup"
            mv "data/duckdb/$filename" "data/duckdb/${filename}.backup"
        fi
        
        # Move the file
        mv "$file" "data/duckdb/"
        
        # Also move any associated .wal files
        if [ -f "${file}.wal" ]; then
            echo "    Moving ${filename}.wal..."
            mv "${file}.wal" "data/duckdb/"
        fi
        
        echo "    ✅ Moved to data/duckdb/$filename"
    fi
done

echo ""
echo "📊 Final status:"
echo "  DuckDB files in data/duckdb/:"
ls -lh data/duckdb/*.duckdb 2>/dev/null | awk '{print "    " $9 ": " $5}' || echo "    No files found"

echo ""
echo "✅ Move complete!"
echo ""
echo "💡 Next steps:"
echo "  1. Export to PostgreSQL: scripts/pipeline/export_all_to_postgres.sh"
echo "  2. Check data: python scripts/database/query_duckdb.py data/duckdb/usajobs_YYYY.duckdb"