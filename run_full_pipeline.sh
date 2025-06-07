#!/bin/bash
# USAJobs Full Pipeline: Fetch, Analyze, and Push to Database
# Usage: 
#   ./run_full_pipeline.sh [mode] [value]
#
# Examples:
#   ./run_full_pipeline.sh sample 100     # Process 100 jobs (sample mode)
#   ./run_full_pipeline.sh daily          # Process all jobs from last 24 hours
#   ./run_full_pipeline.sh days 7         # Process all jobs from last 7 days
#   ./run_full_pipeline.sh all            # Process ALL available jobs (careful!)

MODE=${1:-sample}
VALUE=${2:-100}

case $MODE in
  sample)
    # Sample mode: specific number of jobs
    NUM_JOBS=$VALUE
    PAGES=$(((NUM_JOBS + 499) / 500))
    echo "🚀 Running USAJobs Pipeline - SAMPLE MODE"
    echo "📊 Processing $NUM_JOBS jobs"
    
    source venv/bin/activate && python run_pipeline.py \
      --max-pages $PAGES \
      --sample-titles $NUM_JOBS \
      --load-db \
      --days-posted 0
    ;;
    
  daily)
    # Daily mode: all jobs from last 24 hours
    echo "🚀 Running USAJobs Pipeline - DAILY MODE"
    echo "📊 Processing all jobs from last 24 hours"
    
    source venv/bin/activate && python run_pipeline.py \
      --days-posted 1 \
      --load-db
    ;;
    
  days)
    # Custom days mode: all jobs from last N days
    DAYS=$VALUE
    echo "🚀 Running USAJobs Pipeline - CUSTOM DAYS MODE"
    echo "📊 Processing all jobs from last $DAYS days"
    
    source venv/bin/activate && python run_pipeline.py \
      --days-posted $DAYS \
      --load-db
    ;;
    
  all)
    # All mode: process everything available (use with caution!)
    echo "🚀 Running USAJobs Pipeline - ALL JOBS MODE"
    echo "⚠️  WARNING: This will process ALL available jobs!"
    echo "📊 Press Ctrl+C to cancel..."
    sleep 3
    
    source venv/bin/activate && python run_pipeline.py \
      --days-posted 0 \
      --load-db
    ;;
    
  *)
    echo "Usage: $0 [mode] [value]"
    echo ""
    echo "Modes:"
    echo "  sample N  - Process N jobs (default: 100)"
    echo "  daily     - Process all jobs from last 24 hours"
    echo "  days N    - Process all jobs from last N days"
    echo "  all       - Process ALL available jobs"
    echo ""
    echo "Examples:"
    echo "  $0 sample 50      # Process 50 jobs"
    echo "  $0 daily          # Process today's jobs"
    echo "  $0 days 3         # Process last 3 days"
    exit 1
    ;;
esac

echo ""
echo "✅ Pipeline complete!"

# Show summary
echo ""
echo "📊 Database summary:"
source venv/bin/activate && python scripts/check_database.py