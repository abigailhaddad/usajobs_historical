#!/bin/bash
# Migration script to reorganize repository structure
# RUN THIS ONLY AFTER CURRENT PROCESSES ARE COMPLETE

echo "🚨 WARNING: This will reorganize the entire repository structure!"
echo "Make sure all parallel processes are stopped and databases are closed."
echo ""
read -p "Are you sure you want to continue? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Migration cancelled."
    exit 1
fi

echo "📁 Creating new directory structure..."

# Create new directories
mkdir -p config
mkdir -p scripts/{historical,monitoring,pipeline,database}
mkdir -p data/{duckdb,exports}
mkdir -p sql

echo "📦 Moving files to new locations..."

# Move historical scripts
mv historic_pull.py scripts/historical/
mv src/load_historical_jobs.py scripts/historical/
mv retry_failed_dates.py scripts/historical/

# Move monitoring scripts
mv monitor_parallel.sh scripts/monitoring/
mv check_counts.py scripts/monitoring/
mv check_parallel_complete.sh scripts/monitoring/

# Move pipeline scripts
mv run_historical_pipeline.sh scripts/pipeline/
mv run_parallel_years.sh scripts/pipeline/

# Move database scripts
mv reset_databases.py scripts/database/
mv fast_postgres_export.py scripts/database/
mv export_all_to_postgres.sh scripts/database/
mv query_duckdb.py scripts/database/

# Move data files
mv *.duckdb data/duckdb/ 2>/dev/null || true
mv *.duckdb.wal data/duckdb/ 2>/dev/null || true
mv product_manager_jobs.csv data/exports/ 2>/dev/null || true

# SQL files are already in sql/

# Remove empty src directory
rmdir src 2>/dev/null || true

echo "🔧 Updating file paths in scripts..."

# Update paths in monitoring scripts
sed -i '' 's|logs/|../../logs/|g' scripts/monitoring/monitor_parallel.sh
sed -i '' 's|usajobs_|../../data/duckdb/usajobs_|g' scripts/monitoring/monitor_parallel.sh

# Update paths in pipeline scripts
sed -i '' 's|historic_pull.py|../historical/historic_pull.py|g' scripts/pipeline/run_parallel_years.sh
sed -i '' 's|logs/|../../logs/|g' scripts/pipeline/run_parallel_years.sh

# Update paths in database scripts
sed -i '' 's|usajobs_|../data/duckdb/usajobs_|g' scripts/database/fast_postgres_export.py
sed -i '' 's|usajobs_|../data/duckdb/usajobs_|g' scripts/database/query_duckdb.py
sed -i '' 's|usajobs_|../data/duckdb/usajobs_|g' scripts/database/reset_databases.py

# Update paths in historical scripts
sed -i '' 's|usajobs_|../../data/duckdb/usajobs_|g' scripts/historical/historic_pull.py
sed -i '' 's|logs/|../../logs/|g' scripts/historical/historic_pull.py

echo "📝 Creating convenience scripts in root..."

# Create convenience script to run monitoring
cat > monitor.sh << 'EOF'
#!/bin/bash
cd scripts/monitoring
./monitor_parallel.sh
EOF
chmod +x monitor.sh

# Create convenience script to run parallel years
cat > run_parallel.sh << 'EOF'
#!/bin/bash
cd scripts/pipeline
./run_parallel_years.sh "$@"
EOF
chmod +x run_parallel.sh

echo "✅ Migration complete!"
echo ""
echo "📋 New structure:"
echo "  - Historical scripts: scripts/historical/"
echo "  - Monitoring scripts: scripts/monitoring/"
echo "  - Pipeline scripts: scripts/pipeline/"
echo "  - Database scripts: scripts/database/"
echo "  - Data files: data/duckdb/ and data/exports/"
echo ""
echo "🚀 Quick commands from root:"
echo "  - Monitor progress: ./monitor.sh"
echo "  - Run parallel years: ./run_parallel.sh 2015 2016 2017"
echo ""
echo "🔍 Please test the scripts before running on important data!"