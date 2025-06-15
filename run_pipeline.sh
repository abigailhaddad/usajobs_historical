#!/bin/bash
# Overnight USAJobs Pipeline Script with Single Worker
# Designed to be gentle on the website with single-threaded scraping
# Includes caffeinate to prevent system sleep

set -e

# Configuration
START_DATE="2025-01-01"
OUTPUT_DIR="data_parquet"
LOG_DIR="logs"
SESSION_NAME="usajobs-overnight"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Function to check if tmux session exists
session_exists() {
    tmux has-session -t "$SESSION_NAME" 2>/dev/null
}

# Function to create and run the pipeline
run_pipeline() {
    echo "🌙 Starting overnight USAJobs pipeline..."
    echo "📅 Start date: $START_DATE"
    echo "👥 Workers: 1 (gentle mode)"
    echo "☕ Caffeinate: Enabled"
    echo "📝 Session: $SESSION_NAME"
    echo "📁 Output: $OUTPUT_DIR"
    echo "🕒 Started: $(date)"
    echo "==============================================="
    
    # Create tmux session with caffeinate
    tmux new-session -d -s "$SESSION_NAME" -c "$(pwd)" \
        "caffeinate -i bash -c '
            echo \"🌙 Overnight pipeline started at \$(date)\"
            echo \"☕ Caffeinate enabled - system will not sleep\"
            echo \"===============================================\"
            
            # Change to the pipeline directory for correct imports
            cd usajobs_pipeline
            
            # Run the pipeline with IMPROVED SCRAPER (overwrites old data)
            python run_pipeline_parquet.py \
                --start-date \"$START_DATE\" \
                --output-dir \"../$OUTPUT_DIR\" \
                --scrape-workers 1 \
                2>&1 | tee \"../$LOG_DIR/overnight_improved_$TIMESTAMP.log\"
            
            echo \"\"
            echo \"✅ Pipeline completed at \$(date)\"
            echo \"📊 Check results in: $OUTPUT_DIR\"
            echo \"📝 Full log: $LOG_DIR/overnight_improved_$TIMESTAMP.log\"
            echo \"🔍 View content mismatches: content_mismatch_analysis.html\"
            echo \"\"
            echo \"🌙 Overnight pipeline finished - session will auto-close in 10 seconds\"
            sleep 10
        '"
    
    echo ""
    echo "🚀 Pipeline started in tmux session: $SESSION_NAME"
    echo ""
    echo "📋 Useful commands:"
    echo "   tmux attach -t $SESSION_NAME     # Attach to session"
    echo "   tmux detach                      # Detach (Ctrl+B, then D)"
    echo "   tmux kill-session -t $SESSION_NAME  # Stop pipeline"
    echo ""
    echo "📊 Monitor progress:"
    echo "   tail -f $LOG_DIR/overnight_$TIMESTAMP.log"
    echo ""
    echo "🔍 Check status:"
    echo "   tmux list-sessions               # List all sessions"
    echo "   ps aux | grep caffeinate         # Verify caffeinate is running"
    echo ""
}

# Function to attach to existing session
attach_session() {
    echo "📺 Attaching to existing session: $SESSION_NAME"
    tmux attach -t "$SESSION_NAME"
}

# Function to show session status
show_status() {
    if session_exists; then
        echo "✅ Session '$SESSION_NAME' is running"
        echo ""
        echo "📊 Session info:"
        tmux display-message -t "$SESSION_NAME" -p "   Created: #{session_created}"
        tmux list-windows -t "$SESSION_NAME" -F "   Window: #{window_name} (#{window_flags})"
        echo ""
        echo "📝 Recent log entries:"
        if ls "$LOG_DIR"/overnight_*.log 1> /dev/null 2>&1; then
            echo "   $(ls -t "$LOG_DIR"/overnight_*.log | head -1)"
            tail -5 "$(ls -t "$LOG_DIR"/overnight_*.log | head -1)" 2>/dev/null | sed 's/^/   /'
        else
            echo "   No log files found"
        fi
        echo ""
        echo "🔧 Commands:"
        echo "   $0 attach    # Attach to session"
        echo "   $0 stop      # Stop pipeline"
    else
        echo "❌ No session '$SESSION_NAME' found"
        echo ""
        echo "🚀 Start pipeline with: $0"
    fi
}

# Function to stop the pipeline
stop_pipeline() {
    if session_exists; then
        echo "🛑 Stopping pipeline session: $SESSION_NAME"
        tmux kill-session -t "$SESSION_NAME"
        echo "✅ Session stopped"
        
        # Kill any remaining caffeinate processes
        pkill -f "caffeinate.*usajobs" 2>/dev/null || true
        echo "☕ Caffeinate processes cleaned up"
    else
        echo "❌ No session '$SESSION_NAME' found to stop"
    fi
}

# Function to show help
show_help() {
    echo "🌙 Overnight USAJobs Pipeline Runner"
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  (none)     Start the overnight pipeline"
    echo "  attach     Attach to running session"
    echo "  status     Show current status"
    echo "  stop       Stop the pipeline"
    echo "  help       Show this help"
    echo ""
    echo "Features:"
    echo "  ☕ Caffeinate enabled (prevents system sleep)"
    echo "  👥 Single worker (gentle on the website)"
    echo "  📝 Full logging to logs/ directory"
    echo "  🌙 Designed for overnight/unattended runs"
    echo "  🔄 Keeps existing data (incremental)"
    echo ""
    echo "Pipeline settings:"
    echo "  📅 Start date: $START_DATE"
    echo "  📁 Output: $OUTPUT_DIR"
    echo "  👥 Workers: 1"
    echo "  📝 Session: $SESSION_NAME"
    echo ""
}

# Main script logic
case "${1:-start}" in
    "start"|"")
        if session_exists; then
            echo "⚠️  Session '$SESSION_NAME' already exists!"
            echo ""
            show_status
            echo ""
            echo "Options:"
            echo "  $0 attach    # Attach to existing session"
            echo "  $0 stop      # Stop and restart"
            exit 1
        else
            run_pipeline
        fi
        ;;
    "attach")
        if session_exists; then
            attach_session
        else
            echo "❌ No session '$SESSION_NAME' found"
            echo "🚀 Start pipeline with: $0"
            exit 1
        fi
        ;;
    "status")
        show_status
        ;;
    "stop")
        stop_pipeline
        ;;
    "help"|"-h"|"--help")
        show_help
        ;;
    *)
        echo "❌ Unknown command: $1"
        echo ""
        show_help
        exit 1
        ;;
esac