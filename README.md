# USAJobs Data Pipeline

A pipeline for fetching USAJobs data, generating improved job titles using LLM, and loading to a database.

## Quick Start

```bash
# 🎯 ONE-LINER: Complete pipeline (100 jobs)
./run_full_pipeline.sh 100

# Process different amounts
./run_full_pipeline.sh 50    # 50 jobs
./run_full_pipeline.sh 1000  # 1000 jobs

# Manual command (if you prefer)
source venv/bin/activate && python run_pipeline.py --max-pages 1 --sample-titles 50 --load-db

# Test with small sample (5 jobs, no database)
python run_pipeline.py --max-pages 1 --sample-titles 5
```

## Pipeline Steps

1. **Fetch** - Pull job listings from USAJobs API
2. **Generate Titles** - Use LLM to create better job titles based on duties/qualifications
3. **Enrich** - Add generated titles to raw data
4. **Load** - Optionally load to PostgreSQL database

## Options

- `--skip-fetch` - Skip API fetch, use existing data
- `--skip-titles` - Skip LLM title generation
- `--load-db` - Load enriched data to database
- `--keyword` - Search for specific keywords
- `--days-posted N` - Jobs posted within N days (default: 7)
- `--remote` - Remote jobs only
- `--max-pages N` - Limit API pages fetched (500 jobs per page)
- `--sample-titles N` - Only generate titles for first N jobs (for testing)

## Examples

```bash
# Fetch only, no database
python run_pipeline.py --keyword "software engineer" --days-posted 7

# Use existing data, generate titles
python run_pipeline.py --skip-fetch --use-file data/usajobs_raw_20250606_120000.json

# Full pipeline with database
python run_pipeline.py --keyword "data" --remote --load-db
```