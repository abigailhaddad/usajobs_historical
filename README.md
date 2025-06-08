# USAJobs Historical Data Pipeline

Fetches historical job listings from the USAJobs Historical API and stores them in DuckDB for local analysis and PostgreSQL for cloud storage.

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

## Run Pipeline

**Quick pulls:**
```bash
# Process jobs from last 24 hours
./run_historical_pipeline.sh daily

# Process jobs from last 7 days
./run_historical_pipeline.sh days 7

# Process jobs from last 30 days
./run_historical_pipeline.sh month
```

**Large pulls (use tmux for long-running jobs):**
```bash
# Process entire year (use tmux for unattended runs)
tmux new-session -d -s usajobs-2024 './run_historical_pipeline.sh range 2024-01-01 2024-12-31'

# Watch progress
tmux attach -t usajobs-2024

# Check logs
tail -f logs/range_pull_*.log

# Kill session
tmux kill-session -t usajobs-2024
```

**Custom date ranges:**
```bash
# Specific date range
./run_historical_pipeline.sh range 2024-06-01 2024-06-30
```

## Data Storage

- **DuckDB**: Local analytical database (`usajobs_YEAR.duckdb`) for fast querying
- **PostgreSQL**: Cloud database for final storage (exported from DuckDB)
- **Logs**: Stored in `logs/` directory

## Query Data

```bash
# Interactive DuckDB queries
python query_duckdb.py usajobs_2024.duckdb

# Export specific query to CSV
python query_duckdb.py usajobs_2024.duckdb -q "COPY (SELECT * FROM historical_jobs WHERE LOWER(position_title) LIKE '%product manager%') TO 'product_managers.csv' (HEADER, DELIMITER ',')"

# Fast parallel export to PostgreSQL
python fast_postgres_export.py usajobs_2024.duckdb 8

# Reset databases (careful!)
python reset_databases.py --postgresql  # Only reset PostgreSQL
python reset_databases.py --all         # Reset both
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