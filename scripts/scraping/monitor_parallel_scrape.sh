#!/bin/bash
# Monitor parallel scraping progress

YEAR=${1:-"all"}

echo "🔍 Monitoring parallel scraping for year: $YEAR"
echo ""

while true; do
    clear
    echo "📊 USAJobs Parallel Scraping Status - $(date)"
    echo "================================================"
    echo ""
    
    # Check tmux sessions
    echo "📺 Active Workers:"
    if [ "$YEAR" = "all" ]; then
        tmux ls 2>/dev/null | grep "scrape-" || echo "  No active workers"
    else
        tmux ls 2>/dev/null | grep "scrape-$YEAR-" || echo "  No active workers for $YEAR"
    fi
    echo ""
    
    # Check log files and show progress
    echo "📝 Worker Progress:"
    if [ "$YEAR" = "all" ]; then
        LOG_PATTERN="logs/scrape_*_worker_*.log"
    else
        LOG_PATTERN="logs/scrape_${YEAR}_worker_*.log"
    fi
    
    for log in $LOG_PATTERN; do
        if [ -f "$log" ]; then
            # Extract worker info from filename
            worker_info=$(basename "$log" .log | sed 's/scrape_//')
            
            # Get latest progress line
            latest_progress=$(grep -E "Progress:|completed!" "$log" | tail -1)
            if [ -n "$latest_progress" ]; then
                # Extract just the progress info, remove timestamp and worker prefix
                progress_clean=$(echo "$latest_progress" | sed 's/\[.*\] Worker [0-9]*: //')
                echo "  $worker_info: $progress_clean"
            else
                echo "  $worker_info: Starting..."
            fi
        fi
    done
    
    if ! ls $LOG_PATTERN >/dev/null 2>&1; then
        echo "  No log files found"
    fi
    echo ""
    
    # Overall completion check
    if [ "$YEAR" != "all" ]; then
        echo "📈 Overall Progress for $YEAR:"
        
        # Count completed and failed from all workers
        total_scraped=0
        total_failed=0
        active_workers=0
        completed_workers=0
        
        for log in logs/scrape_${YEAR}_worker_*.log; do
            if [ -f "$log" ]; then
                active_workers=$((active_workers + 1))
                
                # Check if worker completed
                if grep -q "completed!" "$log"; then
                    completed_workers=$((completed_workers + 1))
                fi
                
                # Get final counts from this worker
                scraped=$(grep "Successful:" "$log" | tail -1 | sed 's/.*Successful: //' | awk '{print $1}')
                failed=$(grep "Failed:" "$log" | tail -1 | sed 's/.*Failed: //' | awk '{print $1}')
                
                if [ -n "$scraped" ] && [ "$scraped" -eq "$scraped" ] 2>/dev/null; then
                    total_scraped=$((total_scraped + scraped))
                fi
                if [ -n "$failed" ] && [ "$failed" -eq "$failed" ] 2>/dev/null; then
                    total_failed=$((total_failed + failed))
                fi
            fi
        done
        
        echo "  Workers: $completed_workers/$active_workers completed"
        echo "  Jobs scraped: $total_scraped"
        echo "  Jobs failed: $total_failed"
        echo "  Total processed: $((total_scraped + total_failed))"
        echo ""
    fi
    
    echo "📝 Commands:"
    echo "  Attach to worker 1: tmux attach -t scrape-${YEAR}-worker-1"
    echo "  Check completeness: python check_completeness.py"
    echo "  Kill all workers:   for i in {1..8}; do tmux kill-session -t scrape-${YEAR}-worker-\$i 2>/dev/null; done"
    echo ""
    echo "Press Ctrl+C to exit monitoring"
    sleep 10
done