#!/bin/bash
# Check if all parallel pulls are complete

# Get years from running tmux sessions and DuckDB files
YEARS=()

# Add years from tmux sessions
for session in $(tmux ls 2>/dev/null | grep "usajobs-" | cut -d: -f1); do
    year=${session#usajobs-}
    YEARS+=($year)
done

# Add years from DuckDB files
for db in usajobs_*.duckdb; do
    if [ -f "$db" ]; then
        year=$(basename "$db" .duckdb | sed 's/usajobs_//')
        if [[ "$year" =~ ^[0-9]{4}$ ]] && [[ ! " ${YEARS[@]} " =~ " $year " ]]; then
            YEARS+=($year)
        fi
    fi
done

# Sort years
IFS=$'\n' YEARS=($(sort -n <<<"${YEARS[*]}"))
unset IFS

ALL_COMPLETE=true

echo "🔍 Checking parallel pull completion..."
echo ""

if [ ${#YEARS[@]} -eq 0 ]; then
    echo "❌ No USAJobs pulls found!"
    exit 1
fi

for year in "${YEARS[@]}"; do
    session_name="usajobs-$year"
    if tmux has-session -t "$session_name" 2>/dev/null; then
        echo "  ⏳ $year: Still running"
        ALL_COMPLETE=false
    else
        if [ -f "usajobs_$year.duckdb" ]; then
            count=$(echo "SELECT COUNT(*) FROM historical_jobs;" | duckdb "usajobs_$year.duckdb" -csv 2>/dev/null | tail -1)
            if [ -n "$count" ] && [ "$count" -gt 0 ]; then
                echo "  ✅ $year: Complete ($count jobs)"
            else
                echo "  ⚠️  $year: DuckDB exists but empty"
                ALL_COMPLETE=false
            fi
        else
            echo "  ❌ $year: Not started or failed"
            ALL_COMPLETE=false
        fi
    fi
done

echo ""
if [ "$ALL_COMPLETE" = true ]; then
    echo "✅ All pulls complete!"
    echo ""
    echo "🚀 Ready to export to PostgreSQL:"
    echo "  ./export_all_to_postgres.sh"
else
    echo "⏳ Still processing..."
fi
