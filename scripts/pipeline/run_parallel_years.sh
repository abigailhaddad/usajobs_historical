#!/bin/bash
# Run multiple years in parallel using tmux sessions
# Usage: 
#   ./run_parallel_years.sh                    # Default: 2019-2023
#   ./run_parallel_years.sh 2015 2020          # Range: 2015-2020
#   ./run_parallel_years.sh 2019 2020 2021     # Specific years

# Parse arguments
if [ $# -eq 0 ]; then
    # Default years
    START_YEAR=2019
    END_YEAR=2023
elif [ $# -eq 2 ]; then
    # Range mode: start_year end_year
    START_YEAR=$1
    END_YEAR=$2
    
    # Validate years
    if ! [[ "$START_YEAR" =~ ^[0-9]{4}$ ]] || ! [[ "$END_YEAR" =~ ^[0-9]{4}$ ]]; then
        echo "❌ Error: Years must be 4-digit numbers"
        echo "Usage: $0 [start_year end_year]"
        exit 1
    fi
    
    if [ "$START_YEAR" -gt "$END_YEAR" ]; then
        echo "❌ Error: Start year must be before or equal to end year"
        exit 1
    fi
else
    # List mode: specific years provided
    YEARS=("$@")
fi

# Generate year array if using range mode
if [ -z "${YEARS+x}" ]; then
    YEARS=()
    for ((year=START_YEAR; year<=END_YEAR; year++)); do
        YEARS+=($year)
    done
fi

echo "🚀 USAJobs Historical Parallel Pull"
echo "📅 Years to process: ${YEARS[@]}"
echo ""

# Show estimated time
NUM_YEARS=${#YEARS[@]}
EST_HOURS=$(( NUM_YEARS * 365 * 20 / 3600 ))  # ~20 seconds per day
echo "⏱️  Estimated time: ~${EST_HOURS} hours total (running in parallel)"
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found. Please run: python3 -m venv venv"
    exit 1
fi

# Create logs directory
mkdir -p logs

# Function to check tmux session status
check_session() {
    local session_name=$1
    if tmux has-session -t "$session_name" 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

# Kill any existing sessions for these years
echo "🧹 Cleaning up any existing sessions..."
for year in "${YEARS[@]}"; do
    session_name="usajobs-$year"
    if check_session "$session_name"; then
        echo "  Killing existing session: $session_name"
        tmux kill-session -t "$session_name"
    fi
done

echo ""
echo "🔄 Starting parallel pulls..."
echo ""

# Start tmux sessions for each year
for year in "${YEARS[@]}"; do
    session_name="usajobs-$year"
    start_date="$year-01-01"
    end_date="$year-12-31"
    
    echo "📅 Year $year:"
    echo "  Session: $session_name"
    echo "  Range: $start_date to $end_date"
    
    # Start the tmux session
    tmux new-session -d -s "$session_name" \
        "./run_historical_pipeline.sh range $start_date $end_date"
    
    if [ $? -eq 0 ]; then
        echo "  ✅ Started successfully"
    else
        echo "  ❌ Failed to start"
    fi
    echo ""
    
    # Small delay between starts to avoid API overload
    sleep 2
done

echo "📊 All sessions started!"
echo ""
echo "🖥️  TMUX SESSION MANAGEMENT:"
echo ""

# Display session status
echo "Active sessions:"
for year in "${YEARS[@]}"; do
    session_name="usajobs-$year"
    if check_session "$session_name"; then
        echo "  ✅ $session_name - Running"
    else
        echo "  ❌ $session_name - Not running"
    fi
done

echo ""
echo "📝 Useful commands:"
echo "  Watch all sessions:     tmux ls"
echo "  Attach to a session:    tmux attach -t usajobs-2023"
echo "  Detach from session:    Press Ctrl+B then D"
echo "  Kill a session:         tmux kill-session -t usajobs-2023"
echo "  Kill all sessions:      for y in ${YEARS[@]}; do tmux kill-session -t usajobs-\$y; done"
echo ""
echo "📊 Monitor progress:"
echo "  All logs:              tail -f ../../logs/range_pull_*.log"
echo "  Specific year:         tail -f ../../logs/range_pull_2023*.log"
echo ""
echo "💾 Check DuckDB files:"
echo "  ls -lh usajobs_*.duckdb"
echo ""

# Create monitoring script
cat > monitor_parallel.sh << 'EOF'
#!/bin/bash
# Monitor parallel job progress

echo "🔍 Monitoring parallel USAJobs pulls..."
echo ""

while true; do
    clear
    echo "📊 USAJobs Parallel Pull Status - $(date)"
    echo "================================================"
    echo ""
    
    # Check tmux sessions
    echo "📺 Active Sessions:"
    tmux ls 2>/dev/null | grep usajobs || echo "  No active sessions"
    echo ""
    
    # Check DuckDB files
    echo "💾 DuckDB Files:"
    ls -lh usajobs_*.duckdb 2>/dev/null | awk '{print "  " $9 ": " $5}' || echo "  No DuckDB files yet"
    echo ""
    
    # Check latest log entries
    echo "📝 Latest Activity:"
    for log in ../../logs/range_pull_*.log; do
        if [ -f "$log" ]; then
            year=$(basename "$log" | grep -o "20[0-9][0-9]" | head -1)
            last_line=$(tail -1 "$log" | sed 's/^/  /')
            echo "  $year: $last_line" | cut -c1-80
        fi
    done
    echo ""
    
    # Check job counts in DuckDB files
    echo "📈 Job Counts:"
    for db in usajobs_*.duckdb; do
        if [ -f "$db" ]; then
            year=$(basename "$db" .duckdb | grep -o "[0-9]*")
            count=$(echo "SELECT COUNT(*) FROM historical_jobs;" | duckdb "$db" -csv | tail -1)
            echo "  $year: $count jobs"
        fi
    done
    echo ""
    
    echo "Press Ctrl+C to exit monitoring"
    sleep 30
done
EOF

chmod +x monitor_parallel.sh

echo "🔍 To monitor progress continuously:"
echo "  ./monitor_parallel.sh"
echo ""

# Create completion check script
cat > check_parallel_complete.sh << EOF
#!/bin/bash
# Check if all parallel pulls are complete

# Get years from running tmux sessions and DuckDB files
YEARS=()

# Add years from tmux sessions
for session in \$(tmux ls 2>/dev/null | grep "usajobs-" | cut -d: -f1); do
    year=\${session#usajobs-}
    YEARS+=(\$year)
done

# Add years from DuckDB files
for db in usajobs_*.duckdb; do
    if [ -f "\$db" ]; then
        year=\$(basename "\$db" .duckdb | sed 's/usajobs_//')
        if [[ "\$year" =~ ^[0-9]{4}\$ ]] && [[ ! " \${YEARS[@]} " =~ " \$year " ]]; then
            YEARS+=(\$year)
        fi
    fi
done

# Sort years
IFS=\$'\\n' YEARS=(\$(sort -n <<<"\${YEARS[*]}"))
unset IFS

ALL_COMPLETE=true

echo "🔍 Checking parallel pull completion..."
echo ""

if [ \${#YEARS[@]} -eq 0 ]; then
    echo "❌ No USAJobs pulls found!"
    exit 1
fi

for year in "\${YEARS[@]}"; do
    session_name="usajobs-\$year"
    if tmux has-session -t "\$session_name" 2>/dev/null; then
        echo "  ⏳ \$year: Still running"
        ALL_COMPLETE=false
    else
        if [ -f "usajobs_\$year.duckdb" ]; then
            count=\$(echo "SELECT COUNT(*) FROM historical_jobs;" | duckdb "usajobs_\$year.duckdb" -csv 2>/dev/null | tail -1)
            if [ -n "\$count" ] && [ "\$count" -gt 0 ]; then
                echo "  ✅ \$year: Complete (\$count jobs)"
            else
                echo "  ⚠️  \$year: DuckDB exists but empty"
                ALL_COMPLETE=false
            fi
        else
            echo "  ❌ \$year: Not started or failed"
            ALL_COMPLETE=false
        fi
    fi
done

echo ""
if [ "\$ALL_COMPLETE" = true ]; then
    echo "✅ All pulls complete!"
    echo ""
    echo "🚀 Ready to export to PostgreSQL:"
    echo "  ./export_all_to_postgres.sh"
else
    echo "⏳ Still processing..."
fi
EOF

chmod +x check_parallel_complete.sh

# Create PostgreSQL export script
cat > export_all_to_postgres.sh << 'EOF'
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
EOF

chmod +x export_all_to_postgres.sh

echo "✅ Setup complete!"
echo ""
echo "🎯 Next steps:"
echo "  1. Monitor progress:     ./monitor_parallel.sh"
echo "  2. Check completion:     ./check_parallel_complete.sh"
echo "  3. Export to PostgreSQL: ./export_all_to_postgres.sh (after all complete)"