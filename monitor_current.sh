#!/bin/bash
# Monitor currently running parallel jobs

echo "🔍 Monitoring current parallel USAJobs pulls..."
echo ""

while true; do
    clear
    echo "📊 USAJobs Parallel Pull Status - $(date)"
    echo "================================================"
    echo ""
    
    # Check tmux sessions
    echo "📺 Active Sessions:"
    if tmux ls 2>/dev/null | grep usajobs; then
        echo ""
        
        # Get current progress from each session
        for session in $(tmux ls 2>/dev/null | grep usajobs | cut -d: -f1); do
            year=$(echo "$session" | grep -o "[0-9]*")
            echo "📈 $year Progress:"
            
            # Get last few lines from tmux
            last_lines=$(tmux capture-pane -t "$session" -p 2>/dev/null | tail -5 | grep -E "Fetching|Found|jobs|%" | tail -1)
            if [ -n "$last_lines" ]; then
                echo "  $last_lines"
            else
                echo "  Starting..."
            fi
            
            # Show file size
            if [ -f "data/usajobs_$year.duckdb" ]; then
                size=$(ls -lh "data/usajobs_$year.duckdb" 2>/dev/null | awk '{print $5}')
                echo "  File size: $size"
            fi
            echo ""
        done
    else
        echo "  No active sessions"
        echo ""
        echo "  To start: scripts/run_parallel.sh 2015 2016"
        break
    fi
    
    echo "💡 Commands:"
    echo "  tmux attach -t usajobs-YEAR  # Watch specific year"
    echo "  tmux kill-session -t usajobs-YEAR  # Stop specific year"
    echo "  Press Ctrl+C to exit monitoring"
    
    sleep 5
done