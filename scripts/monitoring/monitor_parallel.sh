#!/bin/bash
# Simple monitor for parallel job progress

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
    
    # Current progress for each year with visual bars
    echo "📅 Current Progress:"
    
    # Get active years from tmux sessions
    active_years=()
    while IFS= read -r session; do
        if [[ "$session" =~ usajobs-([0-9]{4}) ]]; then
            active_years+=(${BASH_REMATCH[1]})
        fi
    done < <(tmux ls 2>/dev/null | grep "usajobs-" | cut -d: -f1)
    
    # Sort the years
    IFS=$'\n' active_years=($(sort -n <<<"${active_years[*]}"))
    unset IFS
    
    if [ ${#active_years[@]} -eq 0 ]; then
        echo "  No active sessions found"
    else
        for year in "${active_years[@]}"; do
        # Find the most recent log file for this year
        log_file=$(ls -t ../../logs/range_pull_${year}-01-01_to_${year}-12-31_*.log 2>/dev/null | head -1)
        
        if [ -f "$log_file" ]; then
            # Get the actual highest date by extracting all dates and finding the maximum
            current_date=$(grep -o "Fetching ${year}-[0-9][0-9]-[0-9][0-9]" "$log_file" | grep -o "${year}-[0-9][0-9]-[0-9][0-9]" | sort -t- -k1,1n -k2,2n -k3,3n | tail -1)
            # Now get the line with that date - be more specific to avoid long lines
            last_line=$(grep "Fetching ${current_date}:" "$log_file" | tail -1)
            
            if [ -n "$current_date" ]; then
                # Get the percentage from the same line
                percentage=$(echo "$last_line" | grep -o "[0-9]\+%" | head -1)
                
                if [ -n "$percentage" ]; then
                    # Create visual progress bar
                    pct_num=$(echo "$percentage" | grep -o "[0-9]\+")
                    bar_length=30
                    filled=$((pct_num * bar_length / 100))
                    empty=$((bar_length - filled))
                    
                    bar="["
                    for ((i=1; i<=filled; i++)); do bar+="█"; done
                    for ((i=1; i<=empty; i++)); do bar+="░"; done
                    bar+="]"
                    
                    printf "  %s: %s %s %s\n" "$year" "$bar" "$percentage" "$current_date"
                else
                    printf "  %s: [░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░] %s\n" "$year" "$current_date"
                fi
            else
                printf "  %s: [░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░] Starting...\n" "$year"
            fi
        else
            printf "  %s: [░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░] No log file found\n" "$year"
        fi
        done
    fi
    
    echo ""
    echo "Press Ctrl+C to exit monitoring"
    sleep 5
done