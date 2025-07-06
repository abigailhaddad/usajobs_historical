#!/bin/bash
# USAJobs Data Pipeline: Fetch Historical and Current Jobs
# Usage: 
#   ./run_single.sh [mode] [value]
#
# Examples:
#   ./run_single.sh daily          # Process jobs from last 24 hours
#   ./run_single.sh days 7         # Process jobs from last 7 days
#   ./run_single.sh month          # Process jobs from last 30 days
#   ./run_single.sh year           # Process jobs from last 365 days
#   ./run_single.sh range 2023-01-01 2023-01-31  # Specific date range
#   ./run_single.sh current        # Fetch current jobs only (last 7 days)

MODE=${1:-month}
VALUE1=$2
VALUE2=$3

# Data directory for parquet files
DATA_DIR="data"

# Function to print tmux instructions
print_tmux_instructions() {
    local session_name="$1"
    local logfile="$2"
    echo ""
    echo "🖥️  TMUX SESSION MANAGEMENT:"
    echo "  📺 To watch progress:    tmux attach -t $session_name"
    echo "  📋 To view logs:         tail -f $logfile"
    echo "  🔌 To detach:           Press Ctrl+B then D"
    echo "  ❌ To kill session:     tmux kill-session -t $session_name"
    echo "  📜 List sessions:       tmux ls"
    echo ""
}

# Function to run both historical and current jobs collection
run_both_apis() {
    local start_date="$1"
    local end_date="$2"
    local description="$3"
    
    echo "🚀 Running USAJobs Data Pipeline - $description"
    echo "📊 Processing jobs from $start_date to $end_date"
    echo "💾 Saving to parquet files in: $DATA_DIR/"
    echo ""
    
    # Run historical jobs collection
    echo "📈 Fetching historical jobs..."
    python $(dirname "$0")/collect_data.py \
      --start-date "$start_date" \
      --end-date "$end_date" \
      --data-dir "$DATA_DIR"
    
    local historical_exit_code=$?
    
    # Run current jobs collection
    echo ""
    echo "📊 Fetching current jobs..."
    # Calculate days for current API (max 90 days)
    DAYS_DIFF=$(( ( $(date -jf "%Y-%m-%d" "$end_date" "+%s") - $(date -jf "%Y-%m-%d" "$start_date" "+%s") ) / 86400 + 1 ))
    if [ $DAYS_DIFF -gt 90 ]; then
        echo "⚠️  Current API limited to 90 days, using --days-posted 90"
        python $(dirname "$0")/collect_current_data.py \
          --days-posted 90 \
          --data-dir "$DATA_DIR"
    else
        python $(dirname "$0")/collect_current_data.py \
          --days-posted $DAYS_DIFF \
          --data-dir "$DATA_DIR"
    fi
    
    local current_exit_code=$?
    
    # Report results
    echo ""
    if [ $historical_exit_code -eq 0 ] && [ $current_exit_code -eq 0 ]; then
        echo "✅ Both historical and current jobs collected successfully!"
    elif [ $historical_exit_code -eq 0 ]; then
        echo "✅ Historical jobs collected successfully"
        echo "⚠️  Current jobs collection had issues (exit code: $current_exit_code)"
    elif [ $current_exit_code -eq 0 ]; then
        echo "⚠️  Historical jobs collection had issues (exit code: $historical_exit_code)"
        echo "✅ Current jobs collected successfully"
    else
        echo "❌ Both collections had issues"
        echo "   Historical exit code: $historical_exit_code"
        echo "   Current exit code: $current_exit_code"
    fi
}

# Function to run current jobs only
run_current_only() {
    local days="$1"
    local description="$2"
    
    echo "🚀 Running USAJobs Current API - $description"
    echo "📊 Processing current jobs posted within $days days"
    echo "💾 Saving to parquet files in: $DATA_DIR/"
    echo ""
    
    python $(dirname "$0")/collect_current_data.py \
      --days-posted $days \
      --data-dir "$DATA_DIR"
}

case $MODE in
  daily)
    # Daily mode: jobs from last 24 hours
    START_DATE=$(date -v-1d +%Y-%m-%d)
    END_DATE=$(date +%Y-%m-%d)
    run_both_apis "$START_DATE" "$END_DATE" "DAILY MODE"
    ;;
    
  days)
    # Custom days mode: jobs from last N days
    DAYS=${VALUE1:-7}
    START_DATE=$(date -v-${DAYS}d +%Y-%m-%d)
    END_DATE=$(date +%Y-%m-%d)
    run_both_apis "$START_DATE" "$END_DATE" "CUSTOM DAYS MODE ($DAYS days)"
    ;;
    
  month)
    # Month mode: jobs from last 30 days (default)
    START_DATE=$(date -v-30d +%Y-%m-%d)
    END_DATE=$(date +%Y-%m-%d)
    run_both_apis "$START_DATE" "$END_DATE" "MONTH MODE"
    ;;
    
  year)
    # Year mode: jobs from last 365 days
    echo "🚀 Running USAJobs Data Pipeline - YEAR MODE"
    echo "📊 Processing jobs from last 365 days"
    echo "⚠️  WARNING: This may take several hours!"
    echo "📊 Starting in 10 seconds... Press Ctrl+C to cancel"
    for i in {10..1}; do
        echo -n "$i... "
        sleep 1
    done
    echo "Starting!"
    
    START_DATE=$(date -v-365d +%Y-%m-%d)
    END_DATE=$(date +%Y-%m-%d)
    
    mkdir -p logs
    LOGFILE="logs/year_pull_$(date +%Y%m%d_%H%M%S).log"
    echo "📝 Logging to: $LOGFILE"
    echo "🕐 Started at: $(date)" | tee -a "$LOGFILE"
    
    # If running in tmux, print session management instructions
    if [ -n "$TMUX" ]; then
        SESSION_NAME=$(tmux display-message -p '#S')
        print_tmux_instructions "$SESSION_NAME" "$LOGFILE"
    fi
    
    # Run with caffeinate to prevent sleep, logging and error handling
    echo "☕ Using caffeinate to prevent system sleep"
    if caffeinate -s bash -c "
        echo '📈 Fetching historical jobs...' | tee -a '$LOGFILE'
        python $(dirname "$0")/collect_data.py \
          --start-date '$START_DATE' \
          --end-date '$END_DATE' \
          --data-dir '$DATA_DIR' 2>&1 | tee -a '$LOGFILE'
        
        echo '' | tee -a '$LOGFILE'
        echo '📊 Fetching current jobs (last 90 days)...' | tee -a '$LOGFILE'
        python $(dirname "$0")/collect_current_data.py \
          --days-posted 90 \
          --data-dir '$DATA_DIR' 2>&1 | tee -a '$LOGFILE'
    "; then
        echo "✅ SUCCESS: Year pull completed at $(date)" | tee -a "$LOGFILE"
    else
        echo "❌ FAILED: Year pull failed at $(date)" | tee -a "$LOGFILE"
        echo "📋 Check log file: $LOGFILE"
        exit 1
    fi
    ;;
    
  range)
    # Custom range mode: specific date range
    START_DATE=${VALUE1}
    END_DATE=${VALUE2}
    
    if [ -z "$START_DATE" ] || [ -z "$END_DATE" ]; then
      echo "Error: range mode requires start and end dates"
      echo "Usage: $0 range YYYY-MM-DD YYYY-MM-DD"
      exit 1
    fi
    
    # Calculate days for time estimate
    DAYS_DIFF=$(( ( $(date -jf "%Y-%m-%d" "$END_DATE" "+%s") - $(date -jf "%Y-%m-%d" "$START_DATE" "+%s") ) / 86400 + 1 ))
    EST_HOURS=$(( DAYS_DIFF * 20 / 3600 ))  # ~20 seconds per day
    
    echo "🚀 Running USAJobs Data Pipeline - RANGE MODE"
    echo "📊 Processing jobs from $START_DATE to $END_DATE"
    echo "📊 Estimated time: ~${EST_HOURS} hours for ${DAYS_DIFF} days"
    echo "📊 Starting in 10 seconds... Press Ctrl+C to cancel"
    
    for i in {10..1}; do
        echo -n "$i... "
        sleep 1
    done
    echo "Starting!"
    
    mkdir -p logs
    LOGFILE="logs/range_pull_${START_DATE}_to_${END_DATE}_$(date +%Y%m%d_%H%M%S).log"
    echo "📝 Logging to: $LOGFILE"
    echo "🕐 Started at: $(date)" | tee -a "$LOGFILE"
    
    # If running in tmux, print session management instructions
    if [ -n "$TMUX" ]; then
        SESSION_NAME=$(tmux display-message -p '#S')
        print_tmux_instructions "$SESSION_NAME" "$LOGFILE"
    fi
    
    # Run with caffeinate to prevent sleep, logging and error handling
    echo "☕ Using caffeinate to prevent system sleep"
    if caffeinate -s bash -c "
        echo '📈 Fetching historical jobs...' | tee -a '$LOGFILE'
        python $(dirname "$0")/collect_data.py \
          --start-date '$START_DATE' \
          --end-date '$END_DATE' \
          --data-dir '$DATA_DIR' 2>&1 | tee -a '$LOGFILE'
        
        echo '' | tee -a '$LOGFILE'
        echo '📊 Fetching current jobs...' | tee -a '$LOGFILE'
        if [ $DAYS_DIFF -gt 90 ]; then
            echo 'Current API limited to 90 days, using --days-posted 90' | tee -a '$LOGFILE'
            python $(dirname "$0")/collect_current_data.py \
              --days-posted 90 \
              --data-dir '$DATA_DIR' 2>&1 | tee -a '$LOGFILE'
        else
            python $(dirname "$0")/collect_current_data.py \
              --days-posted $DAYS_DIFF \
              --data-dir '$DATA_DIR' 2>&1 | tee -a '$LOGFILE'
        fi
    "; then
        echo "✅ SUCCESS: Range pull completed at $(date)" | tee -a "$LOGFILE"
    else
        echo "❌ FAILED: Range pull failed at $(date)" | tee -a "$LOGFILE"
        echo "📋 Check log file: $LOGFILE"
        exit 1
    fi
    ;;
    
  current)
    # Current jobs only mode
    DAYS=${VALUE1:-7}
    run_current_only $DAYS "CURRENT ONLY MODE ($DAYS days)"
    ;;
    
  current-all)
    # Current jobs all mode - fetch everything
    echo "🚀 Running USAJobs Current API - ALL CURRENT JOBS"
    echo "📊 Processing ALL current jobs (no date filter)"
    echo "💾 Saving to parquet files in: $DATA_DIR/"
    echo ""
    
    python $(dirname "$0")/collect_current_data.py \
      --all \
      --data-dir "$DATA_DIR"
    ;;
    
  *)
    echo "Usage: $0 [mode] [value(s)]"
    echo ""
    echo "Modes:"
    echo "  daily              - Process jobs from last 24 hours (both APIs)"
    echo "  days N             - Process jobs from last N days (both APIs, default: 7)"
    echo "  month              - Process jobs from last 30 days (both APIs, default)"
    echo "  year               - Process jobs from last 365 days (both APIs)"
    echo "  range START END    - Process jobs in date range (both APIs, YYYY-MM-DD)"
    echo "  current [N]        - Process current jobs only (default: 7 days)"
    echo "  current-all        - Process ALL current jobs (no date filter)"
    echo ""
    echo "Examples:"
    echo "  $0 daily                          # Last 24 hours (both APIs)"
    echo "  $0 days 14                        # Last 14 days (both APIs)"
    echo "  $0 month                          # Last 30 days (both APIs)"
    echo "  $0 range 2023-01-01 2023-01-31   # January 2023 (both APIs)"
    echo "  $0 current 30                     # Current jobs from last 30 days only"
    echo "  $0 current-all                    # ALL current jobs (no date filter)"
    echo ""
    echo "💡 For long-running jobs (year/large ranges), use tmux:"
    echo "  tmux new-session -d -s usajobs-2024 '$0 range 2024-01-01 2024-12-31'"
    echo "  tmux attach -t usajobs-2024       # to watch progress"
    echo "  # Press Ctrl+B then D to detach"
    echo "  tmux kill-session -t usajobs-2024 # to stop"
    exit 1
    ;;
esac

echo ""
echo "✅ Pipeline complete!"

# Show summary of Parquet files
echo ""
echo "📊 Parquet files created:"
ls -lh $DATA_DIR/*.parquet 2>/dev/null || echo "No Parquet files found"

echo ""
echo "📝 Log files:"
ls -lh logs/*.log 2>/dev/null || echo "No log files found"

echo ""
echo "💡 To analyze your data:"
echo "  python -c \"import pandas as pd; df = pd.read_parquet('$DATA_DIR/historical_jobs_2025.parquet'); print(f'Historical: {len(df):,} jobs')\""
echo "  python -c \"import pandas as pd; df = pd.read_parquet('$DATA_DIR/current_jobs_2025.parquet'); print(f'Current: {len(df):,} jobs')\""