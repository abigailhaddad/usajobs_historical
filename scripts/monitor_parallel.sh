#!/bin/bash
# Monitor parallel job progress with tqdm display

while true; do
    clear
    echo "📊 USAJobs Collection Progress - $(date '+%Y-%m-%d %H:%M:%S')"
    echo "=================================================="
    echo ""
    
    # Check each year for active sessions and show progress
    active_count=0
    for year in {2013..2025}; do
        if tmux has-session -t "usajobs-historical-$year" 2>/dev/null; then
            ((active_count++))
            # Get the last tqdm progress line
            progress=$(tmux capture-pane -t "usajobs-historical-$year" -p | grep -E "Fetching.*[0-9]+%" | tail -1)
            if [ -n "$progress" ]; then
                echo "Year $year:"
                echo "  $progress"
            else
                echo "Year $year: Starting..."
            fi
            echo ""
        fi
    done
    
    # Check current jobs session
    if tmux has-session -t "usajobs-current-all" 2>/dev/null; then
        ((active_count++))
        progress=$(tmux capture-pane -t "usajobs-current-all" -p | grep -E "Fetching.*[0-9]+%" | tail -1)
        if [ -n "$progress" ]; then
            echo "Current jobs:"
            echo "  $progress"
        else
            echo "Current jobs: Starting..."
        fi
        echo ""
    fi
    
    if [ $active_count -eq 0 ]; then
        echo "No active collections running"
        echo ""
    fi
    
    echo "Press Ctrl+C to exit | Refreshing every 3 seconds..."
    sleep 3
done