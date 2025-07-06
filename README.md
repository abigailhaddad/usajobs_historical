# USAJobs Data Pipeline

**⚠️ This is not an official USAJobs project**

**Data collection last run: 2025-07-06**

## Resources

- [Field documentation](https://abigailhaddad.github.io/usajobs_historical/) - Guide to data fields and statistics
- [USAJobs API documentation](https://developer.usajobs.gov/) - Official U.S. government developer documentation for USAJOBS APIs

**Job dataset with 2,974,930 job announcements from Historical + Current APIs**

This repository provides USAJobs data combining both Historical and Current APIs, with deduplication and field normalization. Data is available in two ways:
1. **📁 Ready-to-use Parquet files** - Download and analyze immediately
2. **⚙️ Full data pipeline** - Replicate the collection process yourself

## 🚀 Quick Start Options

### Option 1: Use Pre-Built Data Files
Download and analyze the data immediately:

```python
import pandas as pd

# Load any year's data
df_2024 = pd.read_parquet('data/historical_jobs_2024.parquet')
print(f"Loaded {len(df_2024):,} federal job postings from 2024")

# See [examples.py](https://github.com/abigailhaddad/usajobs_historical/blob/main/examples.py) for more analysis patterns
```

This provides 437MB of data that works with Python, R, or any Parquet-compatible tool.

### Option 2: Run the Pipeline Yourself
The pipeline collects data from USAJobs APIs and saves to Parquet files. Note that the USAJobs API can be unreliable - expect some failed requests that require retries. The system logs all failures and provides specific retry commands.

## Data Coverage

Data collection last run: 2025-07-06. Early years are incomplete, mostly consisting of jobs with closing dates years after the opening dates. Note: Some job postings may have future opening dates. Note: Some job postings may have future opening dates. Note: Some job postings may have future opening dates. Note: Some job postings may have future opening dates. 


| Year | Jobs Opened | Jobs Closed |
|------|-------------|-------------|
| 2013 | 5 | 0 |
| 2014 | 24 | 19 |
| 2015 | 140 | 131 |
| 2016 | 3,879 | 1,633 |
| 2017 | 237,145 | 226,248 |
| 2018 | 328,111 | 315,729 |
| 2019 | 349,256 | 336,608 |
| 2020 | 327,545 | 315,161 |
| 2021 | 369,151 | 352,375 |
| 2022 | 441,604 | 419,295 |
| 2023 | 454,652 | 434,527 |
| 2024 | 367,744 | 352,296 |
| 2025 | 95,674 | 94,583 |

Early years show many long-duration postings (e.g., 3,879 opened in 2016 but only 1,638 closed that year). 

Some job postings may have future opening dates. 

## 🔄 Dual API Integration & Deduplication

This dataset combines data from **two USAJobs APIs** with the following processing:

### API Sources
- **Historical API** (`/api/historicjoa`): Past job announcements by date range
- **Current API** (`/api/Search`): Currently active job postings
- **API Documentation**: [https://developer.usajobs.gov/](https://developer.usajobs.gov/)

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

See the [USAJobs API Documentation](https://developer.usajobs.gov/) for complete API details.

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

3. **Create .env file (only needed for current jobs collection):**
   ```bash
   # .env
   USAJOBS_API_TOKEN=your_api_token_here  # Get from https://developer.usajobs.gov/
   ```
   
   **Note:** The API key is only required for collecting current jobs. Historical data collection does not require authentication.

## File Structure

```
├── [scripts/](https://github.com/abigailhaddad/usajobs_historical/tree/main/scripts)                 # All scripts in one place
│   ├── [collect_data.py](https://github.com/abigailhaddad/usajobs_historical/blob/main/scripts/collect_data.py)          # Historical data collection
│   ├── [collect_current_data.py](https://github.com/abigailhaddad/usajobs_historical/blob/main/scripts/collect_current_data.py)  # Current jobs collection
│   ├── [run_parallel.sh](https://github.com/abigailhaddad/usajobs_historical/blob/main/scripts/run_parallel.sh)          # Run multiple years in parallel 
│   ├── [run_single.sh](https://github.com/abigailhaddad/usajobs_historical/blob/main/scripts/run_single.sh)            # Run single date range or current jobs
│   └── [monitor_parallel.sh](https://github.com/abigailhaddad/usajobs_historical/blob/main/scripts/monitor_parallel.sh)      # Monitor parallel job progress
├── [update/](https://github.com/abigailhaddad/usajobs_historical/tree/main/update)                  # Automated update scripts
│   ├── [update_all.py](https://github.com/abigailhaddad/usajobs_historical/blob/main/update/update_all.py)            # Comprehensive update: data + docs
│   ├── [generate_docs_data.py](https://github.com/abigailhaddad/usajobs_historical/blob/main/update/generate_docs_data.py)    # Generate documentation data
│   └── [update_docs.py](https://github.com/abigailhaddad/usajobs_historical/blob/main/update/update_docs.py)           # Update README and index.html
├── data/                    # Data storage
│   ├── historical_jobs_YEAR.parquet  # Historical jobs by year
│   └── current_jobs_YEAR.parquet     # Current jobs by year
└── logs/                    # Auto-generated pipeline logs
```

## Run Pipeline

**Workflow for data updates:**

```bash
# Collect current jobs and update documentation
python update/update_all.py      # Update data + docs
```

**Historical data collection (if needed):**
- Single year: [scripts/run_single.sh](https://github.com/abigailhaddad/usajobs_historical/blob/main/scripts/run_single.sh)
- Multiple years: [scripts/run_parallel.sh](https://github.com/abigailhaddad/usajobs_historical/blob/main/scripts/run_parallel.sh)

```bash
# Single year:
scripts/run_single.sh range 2024-01-01 2024-12-31

# Multiple years:
scripts/run_parallel.sh 2020 2021 2022
```

## Monitoring Data Collection

Sometimes the USAJobs API has issues. Monitor your runs and check log files for any failed dates:

### Retrying Failed Dates

If dates fail to collect, the system provides specific retry commands:

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

- **Parquet Files**: Storage format
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


## Analysis

See [`examples.py`](https://github.com/abigailhaddad/usajobs_historical/blob/main/examples.py) for usage examples.