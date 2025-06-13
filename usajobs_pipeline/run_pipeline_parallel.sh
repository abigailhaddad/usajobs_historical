#!/bin/bash

# Enhanced USAJobs Pipeline Runner with Caffeine and Tmux
# Prevents sleep and runs in a persistent tmux session

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Enhanced USAJobs Pipeline Runner${NC}"
echo -e "${BLUE}===================================${NC}"

# Check if tmux is available
if ! command -v tmux &> /dev/null; then
    echo -e "${RED}❌ tmux is not installed. Please install it first:${NC}"
    echo "   brew install tmux"
    exit 1
fi

# Check if caffeinate is available (macOS)
if ! command -v caffeinate &> /dev/null; then
    echo -e "${YELLOW}⚠️ caffeinate not found (not on macOS?). Running without sleep prevention.${NC}"
    CAFFEINATE=""
else
    echo -e "${GREEN}☕ Using caffeinate to prevent system sleep${NC}"
    CAFFEINATE="caffeinate -i"
fi

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_SCRIPT="$SCRIPT_DIR/run_pipeline.py"

# Check if pipeline script exists
if [ ! -f "$PIPELINE_SCRIPT" ]; then
    echo -e "${RED}❌ Pipeline script not found at: $PIPELINE_SCRIPT${NC}"
    exit 1
fi

# Parse arguments or use defaults
ARGS="$@"
if [ -z "$ARGS" ]; then
    echo -e "${YELLOW}🔧 No arguments provided, using defaults${NC}"
    ARGS="--start-date 2025-01-01"
fi

# Create tmux session name
SESSION_NAME="usajobs_pipeline_$(date +%Y%m%d_%H%M%S)"

echo -e "${BLUE}📺 Creating tmux session: $SESSION_NAME${NC}"
echo -e "${BLUE}🐍 Running: python run_pipeline.py $ARGS${NC}"
echo -e "${BLUE}⏰ Started at: $(date)${NC}"
echo ""

# Create tmux session and run pipeline with caffeinate
tmux new-session -d -s "$SESSION_NAME" -c "$SCRIPT_DIR"

# Send the pipeline command to the tmux session
if [ -n "$CAFFEINATE" ]; then
    tmux send-keys -t "$SESSION_NAME" "$CAFFEINATE python $PIPELINE_SCRIPT $ARGS" Enter
else
    tmux send-keys -t "$SESSION_NAME" "python $PIPELINE_SCRIPT $ARGS" Enter
fi

echo -e "${GREEN}✅ Pipeline started in tmux session: $SESSION_NAME${NC}"
echo ""
echo -e "${YELLOW}📋 Useful commands:${NC}"
echo -e "   ${BLUE}tmux attach -t $SESSION_NAME${NC}     # Attach to session"
echo -e "   ${BLUE}tmux ls${NC}                           # List all sessions"
echo -e "   ${BLUE}tmux kill-session -t $SESSION_NAME${NC} # Kill this session"
echo ""
echo -e "${YELLOW}🔧 Inside tmux session:${NC}"
echo -e "   ${BLUE}Ctrl+B, D${NC}                        # Detach from session"
echo -e "   ${BLUE}Ctrl+B, [${NC}                        # Scroll mode (q to exit)"
echo -e "   ${BLUE}Ctrl+C${NC}                           # Stop pipeline"
echo ""

# Ask if user wants to attach immediately
read -p "🤔 Attach to session now? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${GREEN}🔗 Attaching to session...${NC}"
    tmux attach -t "$SESSION_NAME"
else
    echo -e "${BLUE}💡 Session is running in background. Use 'tmux attach -t $SESSION_NAME' to connect.${NC}"
fi