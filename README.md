# USAJobs Data Pipeline

**Job dataset from 2013-2025 with 2.97M job announcements from Historical + Current APIs**

This repository provides USAJobs data combining both Historical and Current APIs, with deduplication and field normalization. Data is available in two ways:
1. **📁 Ready-to-use Parquet files** - Download and analyze immediately (recommended for most users)
2. **⚙️ Full data pipeline** - Replicate the collection process yourself (for advanced users)

## 🚀 Quick Start Options

### Option 1: Use Pre-Built Data Files (Recommended)
Perfect for data scientists, researchers, and analysts who want to dive straight into analysis:

```python
import pandas as pd

# Load any year's data
df_2024 = pd.read_parquet('data/historical_jobs_2024.parquet')
print(f"Loaded {len(df_2024):,} federal job postings from 2024")

# See examples.py for more analysis patterns
```

**Benefits:**
- ✅ No setup required
- ✅ 430MB of clean, structured data
- ✅ Immediate analysis capability
- ✅ Works with Python, R, or any Parquet-compatible tool

### Option 2: Replicate the Pipeline
For users who want to:
- Keep data current with latest postings
- Customize the collection process
- Understand the data pipeline
- Contribute to the project

Continue reading for full setup instructions below.

## Data Coverage

**Total**: 2,965,854 jobs across 13 years (2013-2025)

| Year | Job Count | Notes |
|------|-----------|-------|
| 2013 | 5 | ⚠️ Minimal data - API testing/early development |
| 2014 | 24 | ⚠️ Minimal data - API testing/early development |
| 2015 | 140 | ⚠️ Limited data - API coverage started mid-year |
| 2016 | 3,879 | ⚠️ Limited data - partial API coverage |
| 2017 | 237,145 | Full coverage |
| 2018 | 328,111 | Full coverage |
| 2019 | 349,256 | Full coverage |
| 2020 | 327,545 | Full coverage |
| 2021 | 369,151 | Full coverage |
| 2022 | 441,604 | Full coverage |
| 2023 | 454,652 | Full coverage |
| 2024 | 367,177 | Full coverage |
| 2025 | 87,165 | Current through July 6, 2025 |

**Coverage**: 87.3% of expected days (3,506/4,018 days) from 2015-01-01 to 2025-12-31

### Opening vs Closing Patterns
The table below shows jobs opened vs closed by year, explaining data patterns in early years:

| Year | Jobs Opened | Jobs Closed |
|------|-------------|-------------|
| 2013 | 5 | 0 |
| 2014 | 24 | 23 |
| 2015 | 140 | 134 |
| 2016 | 3,879 | 1,638 |
| 2017 | 237,145 | 228,499 |
| 2018 | 328,111 | 326,578 |
| 2019 | 349,256 | 349,002 |
| 2020 | 327,545 | 327,845 |
| 2021 | 369,151 | 364,761 |
| 2022 | 441,604 | 436,071 |
| 2023 | 454,652 | 456,836 |
| 2024 | 367,177 | 372,421 |
| 2025 | 87,165 | 101,429 |
| 2026 | 0 | 617 |

Early years show many long-duration postings (e.g., 3,879 opened in 2016 but only 1,638 closed that year).

## 🔄 Dual API Integration & Deduplication

This dataset combines data from **two USAJobs APIs** with the following processing:

### API Sources
- **Historical API** (`/api/historicjoa`): Past job announcements by date range (2013-2024)
- **Current API** (`/api/Search`): Currently active job postings (2024-2025)

**Note**: In our analysis, we've found that Current API jobs generally also appear in the Historical API data, but we collect from both APIs to ensure complete coverage.

### Data Processing
- **Field Normalization**: Current API fields mapped to historical naming conventions for consistent querying
- **Data Preservation**: All original fields from both APIs retained alongside normalized overlay fields
- **No Data Loss**: "Keep everything + overlay" approach ensures complete data accessibility
- **Deduplication Available**: Use `usajobsControlNumber` to identify records appearing in both APIs when needed

### Result
Query any year using consistent field names (e.g., `hiringAgencyName`, `positionTitle`) while retaining full access to original nested structures from both APIs. Some jobs may appear in both Historical and Current API data - deduplicate using `usajobsControlNumber` when combining datasets.

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
│   └── monitor_parallel.sh      # Monitor parallel job progress
├── data/                    # Data storage
│   ├── historical_jobs_YEAR.parquet  # Historical jobs by year
│   ├── current_jobs_YEAR.parquet     # Current jobs by year
│   └── exports/                      # CSV exports
├── analysis/                # Specialized data analyses
│   └── national_parks/             # National Parks Service analysis
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

# Uses caffeinate to prevent Mac sleep during long runs
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

## Aggressive Data Gap Detection

The pipeline includes aggressive logging that **violently flags** any missing data:

- **🚨 Critical Failures**: Failed API calls are logged with detailed retry commands
- **⚠️ Suspicious Zeros**: Days with 0 jobs are flagged (may be weekends/holidays or API issues)
- **📝 Detailed Logs**: All runs create timestamped logs in `logs/` directory

### Handling Failed Dates

When data collection fails, you'll see dramatic console warnings and get specific retry commands:

```bash
# The system will show failed dates and provide exact retry commands:
python scripts/collect_data.py --start-date 2024-01-15 --end-date 2024-01-15 --data-dir data
python scripts/collect_data.py --start-date 2024-01-20 --end-date 2024-01-20 --data-dir data

# Or retry the entire range to catch any missed dates:
python scripts/collect_data.py --start-date 2024-01-01 --end-date 2024-01-31 --data-dir data
```

**Check logs for:** 
- `logs/historical_YYYY-MM-DD_to_YYYY-MM-DD_TIMESTAMP.log` - Full run details
- `logs/DATA_GAPS_TIMESTAMP.log` - Critical data gap warnings with retry commands

## Data Storage

- **Parquet Files**: Primary storage format for efficient analytics
  - `historical_jobs_YEAR.parquet`: Historical job announcements by year
  - `current_jobs_YEAR.parquet`: Current job postings by year
- **Logs**: Stored in `logs/` directory with aggressive data gap detection

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
| `usajobsControlNumber` | `usajobsControlNumber` | Extracted from `PositionURI` | Numeric job identifier |
| `announcementNumber` | `announcementNumber` | `PositionID` | Public announcement ID |
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
# Load and analyze any year's data
python examples.py
```

## Performance

- **Data collection**: ~1.5 seconds per day (handles 503 errors with 7 retries per request)
- **Local queries**: Fast with Parquet columnar format  
- **Error handling**: Aggressive logging distinguishes between legitimate 0-job days and API failures
- **Data gaps**: Violently flagged with specific retry commands and failure rate calculations

## Analysis

See `examples.py` for comprehensive usage examples showing how to analyze the data.

### Available Analyses

The `analysis/` directory contains specialized analyses of the USAJobs data:
- **National Parks Analysis** (`analysis/national_parks/`): Detailed examination of National Park Service hiring trends from 2018-2025, including occupational changes and appointment type patterns

## Workflow Overview

1. **Collect Data**: Use `scripts/run_parallel.sh` to fetch historical and current jobs
2. **Store Locally**: Data saved in year-based Parquet files with deduplication
3. **Monitor Progress**: Use `scripts/monitor_parallel.sh` to watch live progress
4. **Analyze**: Use `examples.py` for data analysis patterns

The pipeline handles API errors with fallback strategies and can resume from existing data if interrupted.