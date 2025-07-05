# USAJobs Historical Data Pipeline

Fetches historical and current job listings from the USAJobs APIs and stores them in Parquet files for local analysis and PostgreSQL for cloud storage.

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

## Data Sources

The pipeline collects data from two USAJobs APIs:

1. **Historical API** (`/api/historicjoa`) - Past job announcements by date range
2. **Current API** (`/api/Search`) - Currently active job postings

Both APIs are normalized to a common schema and stored in year-based Parquet files.

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
│   ├── collect_data.py          # Historical data collection
│   ├── collect_current_data.py  # Current jobs collection
│   ├── run_parallel.sh          # Run multiple years in parallel (recommended)
│   ├── run_single.sh            # Run single date range or current jobs
│   ├── monitor_parallel.sh      # Monitor parallel job progress
│   ├── export_postgres.py       # Export Parquet files to PostgreSQL
│   ├── export_all.sh            # Export all Parquet files to PostgreSQL
│   ├── check_data.py            # Verify data integrity
│   └── upload_schema.sh         # Upload PostgreSQL schema
├── data/                    # Data storage
│   ├── historical_jobs_YEAR.parquet  # Historical jobs by year
│   ├── current_jobs_YEAR.parquet     # Current jobs by year
│   └── exports/                      # CSV exports
├── analysis/                # Analysis and reports
│   ├── product_manager_analysis.qmd    # Quarto analysis
│   ├── product_manager_analysis.html   # Generated report
│   └── usajobs_analysis.ipynb          # Jupyter notebook
├── sql/                     # Database schemas
│   └── create_historical_jobs.sql      # PostgreSQL table definition
└── logs/                    # Auto-generated pipeline logs
```

## Run Pipeline

**Quick pulls:**
```bash
# Process jobs from last 24 hours (both APIs)
scripts/run_single.sh daily

# Process jobs from last 7 days (both APIs)
scripts/run_single.sh days 7

# Process jobs from last 30 days (both APIs)
scripts/run_single.sh month

# Process current jobs only (last 7 days)
scripts/run_single.sh current 7
```

**Parallel processing (recommended for bulk data):**
```bash
# Process multiple years in parallel (creates one tmux session per year)
scripts/run_parallel.sh 2019 2023      # Range: 2019-2023
scripts/run_parallel.sh 2020 2021 2022 # Specific years

# Monitor all parallel jobs with live progress
scripts/monitor_parallel.sh

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

- **Parquet Files**: Primary storage format for efficient analytics
  - `historical_jobs_YEAR.parquet`: Historical job announcements by year
  - `current_jobs_YEAR.parquet`: Current job postings by year
- **PostgreSQL**: Cloud database for final storage (exported from Parquet)
- **Logs**: Stored in `logs/` directory

## Data Architecture

The pipeline uses a "keep everything + overlay" approach:

- **Historical API**: Keeps all 40+ original fields (these field names are our standard)
- **Current API**: Keeps all original nested fields PLUS adds overlay fields using historical API names
- **Result**: No data loss + consistent querying across both APIs

## API Field Mapping

Key fields are normalized using historical API field names for consistent querying:

### Key Normalized Fields
| Historical Field Name | Historical API Source | Current API Source | Notes |
|----------------------|----------------------|-------------------|-------|
| `usajobsControlNumber` | `usajobsControlNumber` | `PositionID` | Unique job identifier |
| `announcementNumber` | `announcementNumber` | `AnnouncementNumber` | Public announcement ID |
| `hiringAgencyName` | `hiringAgencyName` | `DepartmentName` | Agency name |
| `hiringAgencyCode` | `hiringAgencyCode` | `OrganizationCodes` (first part) | Agency code |
| `positionTitle` | `positionTitle` | `PositionTitle` | Job title |
| `minimumGrade` | `minimumGrade` | `JobGrade[0].Code` | Minimum grade level |
| `maximumGrade` | `maximumGrade` | `JobGrade[-1].Code` | Maximum grade level |
| `minimumSalary` | `minimumSalary` | `PositionRemuneration[0].MinimumRange` | Minimum salary |
| `maximumSalary` | `maximumSalary` | `PositionRemuneration[0].MaximumRange` | Maximum salary |
| `positionOpenDate` | `positionOpenDate` | `PositionStartDate` | Position open date |
| `positionCloseDate` | `positionCloseDate` | `PositionEndDate` | Position close date |

### Data Structure
- **Historical API**: ~40 fields including nested arrays (`HiringPaths`, `JobCategories`, `PositionLocations`)
- **Current API**: ~25 overlay fields + full original nested structure (`MatchedObjectDescriptor`, etc.)
- **Both APIs**: Can be queried using historical field names for consistency

### Pipeline-Added Fields
| Field | Description |
|-------|-------------|
| `inserted_at` | Timestamp when pulled from API |

## Query Data

```bash
# Check data integrity and counts
python scripts/check_data.py

# Export all Parquet files to PostgreSQL
scripts/export_all.sh

# Export single year to PostgreSQL
python scripts/export_postgres.py data/historical_jobs_2024.parquet 8
```

## Performance

- **Data collection**: ~1.5 seconds per day (handles 503 errors with 7 retries per request)
- **PostgreSQL export**: 10,000-15,000 jobs/second with parallel processing  
- **Local queries**: Fast with Parquet columnar format
- **Error handling**: Distinguishes between legitimate 0-job days and API failures

## Workflow Overview

1. **Collect Data**: Use `scripts/run_parallel.sh` to fetch historical and current jobs
2. **Store Locally**: Data saved in year-based Parquet files with deduplication
3. **Monitor Progress**: Use `scripts/monitor_parallel.sh` to watch live progress
4. **Export to Cloud**: Use `scripts/export_all.sh` for fast parallel PostgreSQL upload
5. **Verify**: Use `scripts/check_data.py` to ensure data integrity

The pipeline handles API errors with exponential backoff and can resume from existing data if interrupted.