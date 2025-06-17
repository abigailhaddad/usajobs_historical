#!/bin/bash
# Quick progress check for all parallel USAJobs pulls

echo "📊 USAJobs Parallel Pull Progress - $(date)"
echo "============================================"
echo ""

for year in 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025; do
    echo "=== $year ==="
    if tmux has-session -t "usajobs-$year" 2>/dev/null; then
        # Get last few lines to catch progress bars that might not be on the last line
        progress=$(tmux capture-pane -t usajobs-$year -p | grep -E "Fetching.*%.*\|" | tail -1)
        if [[ -n "$progress" ]]; then
            echo "$progress"
        else
            echo "Running (no progress bar visible)"
        fi
    else
        echo "Session not running"
    fi
    echo ""
done

echo "💡 To watch a specific year: tmux attach -t usajobs-2023"
echo "📜 To list all sessions: tmux ls"