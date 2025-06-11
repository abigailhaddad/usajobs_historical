#!/bin/bash
# Run worker-based parallel scraping (no locking issues)
# Usage: 
#   ./run_workers.sh 2018                # Single year with 16 workers (default)
#   ./run_workers.sh 2018 24             # Single year with 24 workers

YEAR=$1
WORKERS=${2:-16}  # Default to 16 workers

if [ -z "$YEAR" ]; then
    echo "Usage: $0 <year> [workers]"
    echo "Example: $0 2018 24"
    exit 1
fi

echo "🚀 USAJobs Worker-Based Parallel Scraping"
echo "📅 Year: $YEAR"
echo "👥 Workers: $WORKERS"
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

# Kill any existing sessions for this year
echo "🧹 Cleaning up any existing sessions..."
for ((i=1; i<=WORKERS; i++)); do
    session_name="scrape-${YEAR}-worker-${i}"
    if check_session "$session_name"; then
        echo "  Killing existing session: $session_name"
        tmux kill-session -t "$session_name"
    fi
done

echo ""
echo "🔄 Starting worker-based parallel scraping..."
echo "📝 Each worker will write to its own database file"
echo ""

# Start tmux sessions for each worker
for ((i=1; i<=WORKERS; i++)); do
    session_name="scrape-${YEAR}-worker-${i}"
    
    echo "👤 Worker $i:"
    echo "  Session: $session_name"
    
    # Start the tmux session with worker-based scraper
    tmux new-session -d -s "$session_name" \
        "cd /Users/abigailhaddad/Documents/repos/usajobs_historic/scripts/scraping && source ../../venv/bin/activate && caffeinate -i python scrape_year_workers.py $YEAR $i $WORKERS"
    
    if [ $? -eq 0 ]; then
        echo "  ✅ Started successfully"
    else
        echo "  ❌ Failed to start"
    fi
    echo ""
    
    # Small delay between starts
    sleep 1
done

echo "📊 All workers started!"
echo ""
echo "🖥️  TMUX SESSION MANAGEMENT:"
echo ""

# Display session status
echo "Active sessions:"
for ((i=1; i<=WORKERS; i++)); do
    session_name="scrape-${YEAR}-worker-${i}"
    if check_session "$session_name"; then
        echo "  ✅ $session_name - Running"
    else
        echo "  ❌ $session_name - Not running"
    fi
done

echo ""
echo "📝 Useful commands:"
echo "  Watch all sessions:     tmux ls"
echo "  Attach to worker 1:     tmux attach -t scrape-${YEAR}-worker-1"
echo "  Detach from session:    Press Ctrl+B then D"
echo "  Kill a worker:          tmux kill-session -t scrape-${YEAR}-worker-1"
echo "  Kill all workers:       for i in {1..$WORKERS}; do tmux kill-session -t scrape-${YEAR}-worker-\$i; done"
echo ""
echo "📊 Monitor progress:"
echo "  ./monitor_parallel_scrape.sh $YEAR"
echo "  Check completeness:     python check_completeness.py"
echo ""
echo "🔗 After completion:"
echo "  Merge results:          python merge_worker_results.py $YEAR $WORKERS"
echo ""