#!/bin/bash
# Run multiple years in parallel using tmux sessions
# Now fetches both historical and current jobs, saves to Parquet files
# Usage: 
#   ./run_parallel.sh                    # Default: 2019-2023
#   ./run_parallel.sh 2015 2020          # Range: 2015-2020
#   ./run_parallel.sh 2019 2020 2021     # Specific years
#
# NOTE: Uses caffeinate to prevent Mac sleep during long runs

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

echo "🚀 USAJobs Data Pipeline - Parallel Processing"
echo "📅 Years to process: ${YEARS[@]}"
echo "💾 Data will be saved to Parquet files in data/ directory"
echo ""

# Show estimated time
NUM_YEARS=${#YEARS[@]}
EST_HOURS=$(( NUM_YEARS * 365 * 20 / 3600 ))  # ~20 seconds per day
echo "⏱️  Estimated time: ~${EST_HOURS} hours total (running in parallel)"
echo ""

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
    session_name="usajobs-historical-$year"
    if check_session "$session_name"; then
        echo "  Killing existing session: $session_name"
        tmux kill-session -t "$session_name"
    fi
done

# Also kill the current jobs session if it exists
if check_session "usajobs-current-all"; then
    echo "  Killing existing session: usajobs-current-all"
    tmux kill-session -t "usajobs-current-all"
fi

echo ""
echo "🔄 Starting parallel pulls..."
echo ""

# Start tmux sessions for each year (historical data only)
for year in "${YEARS[@]}"; do
    session_name="usajobs-historical-$year"
    start_date="$year-01-01"
    end_date="$year-12-31"
    
    echo "📅 Historical $year:"
    echo "  Session: $session_name"
    echo "  Range: $start_date to $end_date"
    echo "  File: historical_jobs_$year.parquet"
    
    # Create log file for this session
    logfile="logs/historical_${year}_$(date +%Y%m%d_%H%M%S).log"
    echo "  Log: $logfile"
    
    # Start the tmux session for historical data with logging AND caffeinate to prevent sleep
    tmux new-session -d -s "$session_name" \
        "cd $(pwd) && source venv/bin/activate && caffeinate -i python scripts/collect_data.py --start-date $start_date --end-date $end_date --data-dir data 2>&1 | tee $logfile"
    
    if [ $? -eq 0 ]; then
        echo "  ✅ Started successfully"
    else
        echo "  ❌ Failed to start"
    fi
    echo ""
    
    # Small delay between starts to avoid API overload
    sleep 2
done

# Start one session for ALL current jobs
# Create log file for current jobs session
current_logfile="logs/current_all_$(date +%Y%m%d_%H%M%S).log"

echo "📊 Current Jobs (ALL):"
echo "  Session: usajobs-current-all"
echo "  Files: current_jobs_YYYY.parquet (distributed by year)"
echo "  Log: $current_logfile"

tmux new-session -d -s "usajobs-current-all" \
    "cd $(pwd) && source venv/bin/activate && caffeinate -i python scripts/collect_current_data.py --all --data-dir data 2>&1 | tee $current_logfile"

if [ $? -eq 0 ]; then
    echo "  ✅ Started successfully"
else
    echo "  ❌ Failed to start"
fi
echo ""

echo "📊 All sessions started!"
echo ""
echo "🖥️  TMUX SESSION MANAGEMENT:"
echo ""

# Display session status
echo "Active sessions:"
for year in "${YEARS[@]}"; do
    session_name="usajobs-historical-$year"
    if check_session "$session_name"; then
        echo "  ✅ $session_name - Running"
    else
        echo "  ❌ $session_name - Not running"
    fi
done

# Check current jobs session
if check_session "usajobs-current-all"; then
    echo "  ✅ usajobs-current-all - Running"
else
    echo "  ❌ usajobs-current-all - Not running"
fi

echo ""
echo "📝 Useful commands:"
echo "  Watch all sessions:     tmux ls"
echo "  Attach to a session:    tmux attach -t usajobs-historical-2023"
echo "  Detach from session:    Press Ctrl+B then D"
echo "  Kill a session:         tmux kill-session -t usajobs-historical-2023"
echo ""
echo "📊 Monitor progress:"
echo "  ./scripts/monitor_parallel.sh"
echo ""
echo "💾 Check data:"
echo "  ls -lh data/*.parquet"
echo ""
echo "✅ Setup complete!"