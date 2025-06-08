#!/bin/bash
# USAJobs Historical Pipeline: Fetch and Push to Database
# Usage: 
#   ./run_historical_pipeline.sh [mode] [value]
#
# Examples:
#   ./run_historical_pipeline.sh daily          # Process jobs from last 24 hours
#   ./run_historical_pipeline.sh days 7         # Process jobs from last 7 days
#   ./run_historical_pipeline.sh month          # Process jobs from last 30 days
#   ./run_historical_pipeline.sh year           # Process jobs from last 365 days
#   ./run_historical_pipeline.sh range 2023-01-01 2023-01-31  # Specific date range

MODE=${1:-month}
VALUE1=$2
VALUE2=$3

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

case $MODE in
  daily)
    # Daily mode: jobs from last 24 hours
    echo "🚀 Running USAJobs Historical Pipeline - DAILY MODE"
    echo "📊 Processing jobs from last 24 hours"
    
    source venv/bin/activate && python historic_pull.py \
      --start-date $(date -v-1d +%Y-%m-%d) \
      --end-date $(date +%Y-%m-%d) \
      --duckdb "daily_$(date +%Y%m%d).duckdb" \
      --no-json
    ;;
    
  days)
    # Custom days mode: jobs from last N days
    DAYS=${VALUE1:-7}
    echo "🚀 Running USAJobs Historical Pipeline - CUSTOM DAYS MODE"
    echo "📊 Processing jobs from last $DAYS days"
    
    START_DATE=$(date -v-${DAYS}d +%Y-%m-%d)
    END_DATE=$(date +%Y-%m-%d)
    
    source venv/bin/activate && python historic_pull.py \
      --start-date "$START_DATE" \
      --end-date "$END_DATE" \
      --duckdb "last_${DAYS}days_$(date +%Y%m%d).duckdb" \
      --no-json
    ;;
    
  month)
    # Month mode: jobs from last 30 days (default)
    echo "🚀 Running USAJobs Historical Pipeline - MONTH MODE"
    echo "📊 Processing jobs from last 30 days"
    
    START_DATE=$(date -v-30d +%Y-%m-%d)
    END_DATE=$(date +%Y-%m-%d)
    
    source venv/bin/activate && python historic_pull.py \
      --start-date "$START_DATE" \
      --end-date "$END_DATE" \
      --duckdb "month_$(date +%Y%m%d).duckdb" \
      --no-json
    ;;
    
  year)
    # Year mode: jobs from last 365 days
    echo "🚀 Running USAJobs Historical Pipeline - YEAR MODE"
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
    
    # Run with caffeinate to prevent sleep, logging and error handling
    echo "☕ Using caffeinate to prevent system sleep"
    if caffeinate -s bash -c "source venv/bin/activate && python historic_pull.py \
      --start-date '$START_DATE' \
      --end-date '$END_DATE' \
      --duckdb 'year_$(date +%Y%m%d).duckdb'" 2>&1 | tee -a "$LOGFILE"; then
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
    
    echo "🚀 Running USAJobs Historical Pipeline - RANGE MODE"
    echo "📊 Processing jobs from $START_DATE to $END_DATE"
    
    # Calculate days for time estimate
    DAYS_DIFF=$(( ( $(date -jf "%Y-%m-%d" "$END_DATE" "+%s") - $(date -jf "%Y-%m-%d" "$START_DATE" "+%s") ) / 86400 + 1 ))
    EST_HOURS=$(( DAYS_DIFF * 20 / 3600 ))  # ~20 seconds per day
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
    # Use existing usajobs_YEAR.duckdb if it exists, otherwise create new one
    YEAR=$(echo "$START_DATE" | cut -d'-' -f1)
    if [ -f "usajobs_${YEAR}.duckdb" ]; then
        DUCKDB_FILE="usajobs_${YEAR}.duckdb"
        echo "📁 Using existing DuckDB file: $DUCKDB_FILE" | tee -a "$LOGFILE"
    else
        DUCKDB_FILE="${START_DATE}_to_${END_DATE}.duckdb"
        echo "📁 Creating new DuckDB file: $DUCKDB_FILE" | tee -a "$LOGFILE"
    fi
    
    if caffeinate -s bash -c "source venv/bin/activate && python historic_pull.py \
      --start-date '$START_DATE' \
      --end-date '$END_DATE' \
      --duckdb '$DUCKDB_FILE'" 2>&1 | tee -a "$LOGFILE"; then
        echo "✅ SUCCESS: Range pull completed at $(date)" | tee -a "$LOGFILE"
    else
        echo "❌ FAILED: Range pull failed at $(date)" | tee -a "$LOGFILE"
        echo "📋 Check log file: $LOGFILE"
        exit 1
    fi
    ;;
    
  *)
    echo "Usage: $0 [mode] [value(s)]"
    echo ""
    echo "Modes:"
    echo "  daily              - Process jobs from last 24 hours"
    echo "  days N             - Process jobs from last N days (default: 7)"
    echo "  month              - Process jobs from last 30 days (default)"
    echo "  year               - Process jobs from last 365 days"
    echo "  range START END    - Process jobs in date range (YYYY-MM-DD)"
    echo ""
    echo "Examples:"
    echo "  $0 daily                          # Last 24 hours"
    echo "  $0 days 14                        # Last 14 days"
    echo "  $0 month                          # Last 30 days"
    echo "  $0 range 2023-01-01 2023-01-31   # January 2023"
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

# Show summary of DuckDB files
echo ""
echo "📊 DuckDB files created:"
ls -lh *.duckdb 2>/dev/null || echo "No DuckDB files found"

echo ""
echo "📝 Log files:"
ls -lh logs/*.log 2>/dev/null || echo "No log files found"

echo ""
echo "💡 To query your data:"
echo "  python query_duckdb.py [filename].duckdb"
echo "💡 To check logs:"
echo "  tail -f logs/[logfile].log"