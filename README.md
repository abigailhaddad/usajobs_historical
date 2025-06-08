# USAJobs Historical Data Pipeline

Fetches historical job listings from the USAJobs Historical API and stores them in DuckDB for local analysis and PostgreSQL for cloud storage.

## Data Coverage

**Total**: 2,947,854 jobs across 11 years (2015-2025)

| Year | Job Count | Notes |
|------|-----------|-------|
| 2015 | 140 | ⚠️ Limited data - API coverage started mid-year |
| 2016 | 3,879 | ⚠️ Limited data - partial API coverage |
| 2017 | 237,145 | Full coverage |
| 2018 | 327,905 | Full coverage |
| 2019 | 349,256 | Full coverage |
| 2020 | 326,376 | Full coverage |
| 2021 | 366,943 | Full coverage |
| 2022 | 441,487 | Full coverage |
| 2023 | 454,036 | Full coverage |
| 2024 | 367,193 | Full coverage |
| 2025 | 73,494 | Current through June 7, 2025 |

**Coverage**: 90.6% of expected days (3,452/3,811 days) from 2015-01-01 to 2025-06-07

## Setup

1. **Create virtual environment:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Create .env file:**
   ```bash
   # .env
   DATABASE_URL=your_postgresql_connection_string_here
   ```

## File Structure

```
├── scripts/
│   ├── historical/           # Data collection scripts
│   │   ├── historic_pull.py     # Main data fetching script
│   │   ├── load_historical_jobs.py
│   │   └── retry_failed_dates.py
│   ├── database/            # Database management
│   │   ├── query_duckdb.py      # Interactive DuckDB queries
│   │   ├── fast_postgres_export.py  # Parallel PostgreSQL export
│   │   ├── reset_databases.py
│   │   └── export_product_manager_jobs.py
│   ├── monitoring/          # Progress tracking
│   │   ├── check_counts.py      # Verify data integrity
│   │   └── monitor_parallel.sh  # Real-time progress monitoring
│   └── pipeline/            # Orchestration scripts
│       ├── run_historical_pipeline.sh  # Main pipeline runner
│       ├── run_parallel_years.sh
│       ├── check_parallel_complete.sh
│       └── export_all_to_postgres.sh
├── data/
│   ├── duckdb/             # Local analytical databases
│   │   └── usajobs_YEAR.duckdb
│   └── exports/            # CSV exports
├── logs/                   # Pipeline execution logs
└── sql/                    # Database schemas
```

## Run Pipeline

**Quick pulls:**
```bash
# Process jobs from last 24 hours
scripts/pipeline/run_historical_pipeline.sh daily

# Process jobs from last 7 days
scripts/pipeline/run_historical_pipeline.sh days 7

# Process jobs from last 30 days
scripts/pipeline/run_historical_pipeline.sh month
```

**Large pulls (use tmux for long-running jobs):**
```bash
# Process entire year (use tmux for unattended runs)
tmux new-session -d -s usajobs-2024 'scripts/pipeline/run_historical_pipeline.sh range 2024-01-01 2024-12-31'

# Watch progress
./monitor.sh  # or tmux attach -t usajobs-2024

# Check completion status
scripts/pipeline/check_parallel_complete.sh

# Check logs
tail -f logs/range_pull_*.log

# Kill session
tmux kill-session -t usajobs-2024
```

**Parallel processing multiple years:**
```bash
# Process multiple years in parallel
scripts/pipeline/run_parallel_years.sh 2020 2021 2022 2023

# Monitor all parallel jobs
./monitor.sh
```

**Custom date ranges:**
```bash
# Specific date range
scripts/pipeline/run_historical_pipeline.sh range 2024-06-01 2024-06-30
```

## Data Storage

- **DuckDB**: Local analytical database (`usajobs_YEAR.duckdb`) for fast querying
- **PostgreSQL**: Cloud database for final storage (exported from DuckDB)
- **Logs**: Stored in `logs/` directory

## Query Data

```bash
# Interactive DuckDB queries
python scripts/database/query_duckdb.py data/duckdb/usajobs_2024.duckdb

# Export product manager jobs to CSV
python scripts/database/export_product_manager_jobs.py

# Fast parallel export to PostgreSQL
python scripts/database/fast_postgres_export.py data/duckdb/usajobs_2024.duckdb 8

# Export all databases to PostgreSQL
scripts/pipeline/export_all_to_postgres.sh

# Check data integrity
python scripts/monitoring/check_counts.py

# Reset databases (careful!)
python scripts/database/reset_databases.py --postgresql  # Only reset PostgreSQL
python scripts/database/reset_databases.py --all         # Reset both
```

## Performance

- **Data collection**: ~12 seconds per day (handles 503 errors with retry)
- **PostgreSQL export**: 13,322+ jobs/second with parallel processing
- **Local queries**: Instant with DuckDB indexing

The pipeline will:
1. Fetch historical jobs from USAJobs Historical API
2. Store data incrementally in DuckDB with deduplication  
3. Export to PostgreSQL using fast parallel bulk inserts
4. Handle 503 errors with exponential backoff retry logic
5. Resume from existing data if interrupted