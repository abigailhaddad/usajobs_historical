#!/bin/bash
# Complete scraping workflow - runs workers, waits for completion, merges, and cleans up
# Usage: ./run_complete_scraping.sh 2019 8

YEAR=$1
WORKERS=${2:-16}  # Default to 16 workers

if [ -z "$YEAR" ]; then
    echo "Usage: $0 <year> [workers]"
    echo "Example: $0 2019 8"
    exit 1
fi

echo "🚀 Complete USAJobs Scraping Workflow"
echo "📅 Year: $YEAR"
echo "👥 Workers: $WORKERS"
echo ""

# Step 1: Start workers
echo "1️⃣ Starting workers..."
./run_workers.sh $YEAR $WORKERS

# Step 2: Wait for workers to complete
echo ""
echo "2️⃣ Waiting for workers to complete..."
echo "   You can monitor progress with: ./monitor_parallel_scrape.sh $YEAR"
echo ""

# Function to check if any workers are still running
check_workers_running() {
    for ((i=1; i<=WORKERS; i++)); do
        if tmux has-session -t "scrape-${YEAR}-worker-${i}" 2>/dev/null; then
            return 0  # At least one worker is still running
        fi
    done
    return 1  # No workers running
}

# Wait for workers to finish
while check_workers_running; do
    echo "⏳ Workers still running... checking again in 30 seconds"
    sleep 30
done

echo "✅ All workers completed!"
echo ""

# Step 3: Merge results
echo "3️⃣ Merging worker results..."
python merge_worker_results.py $YEAR $WORKERS

# Step 4: Clean up worker databases
echo ""
echo "4️⃣ Cleaning up worker databases..."
for ((i=1; i<=WORKERS; i++)); do
    worker_db="../../data/duckdb/usajobs_${YEAR}_worker_${i}.duckdb"
    if [ -f "$worker_db" ]; then
        rm "$worker_db"
        echo "  Deleted $worker_db"
    fi
done

# Step 5: Final status check
echo ""
echo "5️⃣ Final status check..."
python check_completeness.py $YEAR

echo ""
echo "🎉 Complete scraping workflow finished for $YEAR!"
echo "   All worker databases have been merged and cleaned up."