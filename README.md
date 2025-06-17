# USAJobs Historical Data Pipeline

Fetches historical job listings from the USAJobs Historical API and stores them in DuckDB for local analysis and PostgreSQL for cloud storage.

## Data Coverage

**Total**: 2,957,747 jobs across 11 years (2015-2025)

| Year | Job Count | Notes |
|------|-----------|-------|
| 2015 | 140 | ⚠️ Limited data - API coverage started mid-year |
| 2016 | 3,879 | ⚠️ Limited data - partial API coverage |
| 2017 | 237,145 | Full coverage |
| 2018 | 327,905 | Full coverage |
| 2019 | 349,256 | Full coverage |
| 2020 | 327,545 | Full coverage |
| 2021 | 369,151 | Full coverage |
| 2022 | 441,604 | Full coverage |
| 2023 | 454,652 | Full coverage |
| 2024 | 367,187 | Full coverage |
| 2025 | 79,283 | Current through June 16, 2025 |

**Coverage**: 87.3% of expected days (3,506/4,018 days) from 2015-01-01 to 2025-12-31

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
   DATABASE_URL=postgresql://user:password@host/usajobs_historical
   USAJOBS_API_TOKEN=your_api_token_here  # Get from https://developer.usajobs.gov/
   ```

## File Structure

```
├── scripts/                 # All scripts in one place
│   ├── collect_data.py          # Main data collection from USAJobs API
│   ├── run_parallel.sh          # Run multiple years in parallel (recommended)
│   ├── run_single.sh            # Run single date range
│   ├── export_postgres.py       # Export single DuckDB to PostgreSQL
│   ├── export_all.sh            # Export all DuckDB files to PostgreSQL
│   ├── check_data.py            # Verify data integrity
│   ├── query_data.py            # Interactive DuckDB queries
│   ├── export_analysis.py       # Export specific job analyses
│   └── upload_schema.sh         # Upload PostgreSQL schema
├── data/                    # Data storage
│   ├── usajobs_YEAR.duckdb     # Local analytical databases
│   └── exports/                # CSV exports
├── analysis/                # Analysis and reports
│   ├── product_manager_analysis.qmd    # Quarto analysis
│   ├── product_manager_analysis.html   # Generated report
│   └── usajobs_analysis.ipynb          # Jupyter notebook
├── sql/                     # Database schemas
│   └── create_historical_jobs.sql      # PostgreSQL table definition
├── logs/                    # Auto-generated pipeline logs
├── monitor_current.sh       # Monitor running parallel jobs
└── check_parallel_complete.sh # Check if parallel jobs finished
```

## Run Pipeline

**Quick pulls:**
```bash
# Process jobs from last 24 hours
scripts/run_single.sh daily

# Process jobs from last 7 days
scripts/run_single.sh days 7

# Process jobs from last 30 days
scripts/run_single.sh month
```

**Parallel processing (recommended for bulk data):**
```bash
# Process multiple years in parallel (creates one tmux session per year)
scripts/run_parallel.sh 2019 2023      # Range: 2019-2023
scripts/run_parallel.sh 2020 2021 2022 # Specific years

# Monitor all parallel jobs
./monitor_current.sh

# Check completion status
./check_parallel_complete.sh

# Export all to PostgreSQL after completion
scripts/export_all.sh
```

**Single year processing:**
```bash
# Process entire year (use tmux for unattended runs)
tmux new-session -d -s usajobs-2024 'scripts/run_single.sh range 2024-01-01 2024-12-31'

# Watch progress
tmux attach -t usajobs-2024

# Custom date ranges
scripts/run_single.sh range 2024-06-01 2024-06-30
```

## Data Storage

- **DuckDB**: Local analytical database (`usajobs_YEAR.duckdb`) for fast querying
- **PostgreSQL**: Cloud database for final storage (exported from DuckDB)
- **Logs**: Stored in `logs/` directory

## Query Data

```bash
# Interactive DuckDB queries
python scripts/query_data.py data/usajobs_2024.duckdb

# Export product manager jobs to CSV
python scripts/export_analysis.py

# Check data integrity (compares DuckDB vs PostgreSQL)
python scripts/check_data.py

# Export all DuckDB files to PostgreSQL
scripts/export_all.sh

# Export single year to PostgreSQL
python scripts/export_postgres.py data/usajobs_2024.duckdb 8
```

## Performance

- **Data collection**: ~20 seconds per day (handles 503 errors with retry)
- **PostgreSQL export**: 10,000-15,000 jobs/second with parallel processing
- **Local queries**: Instant with DuckDB indexing

## Workflow Overview

1. **Collect Data**: Use `scripts/run_parallel.sh` to fetch historical jobs from USAJobs API
2. **Store Locally**: Data saved in DuckDB files (`usajobs_YEAR.duckdb`) with deduplication
3. **Export to Cloud**: Use `scripts/export_all.sh` for fast parallel PostgreSQL upload
4. **Verify**: Use `scripts/check_data.py` to ensure data integrity between DuckDB and PostgreSQL

The pipeline handles API errors with exponential backoff and can resume from existing data if interrupted.