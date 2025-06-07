#!/bin/bash
# USAJobs Full Pipeline: Fetch, Analyze, and Push to Database
# Usage: ./run_full_pipeline.sh [number_of_jobs]

# Default to 100 jobs if no argument provided
NUM_JOBS=${1:-100}

# Calculate pages needed (500 jobs per page)
PAGES=$(((NUM_JOBS + 499) / 500))

echo "🚀 Running USAJobs Pipeline for $NUM_JOBS jobs ($PAGES pages)"
echo "📊 This will:"
echo "   - Fetch $NUM_JOBS jobs from USAJobs API"
echo "   - Generate plain language titles using LLM"
echo "   - Push enriched data to Neon database"
echo ""

# Activate virtual environment and run pipeline
source venv/bin/activate && python run_pipeline.py \
  --max-pages $PAGES \
  --sample-titles $NUM_JOBS \
  --load-db \
  --days-posted 0

echo ""
echo "✅ Pipeline complete!"