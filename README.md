# USAJobs Data Pipeline

**Data collection last run: 2026-03-20**

**⚠️ This is not an official USAJobs project**

## Explore the Data

**[Browse the live site](https://usajobs-historical.vercel.app)** -- Interactive DataTable with filters, charts (jobs by month, top agencies, grade distribution), multi-column sorting, and shareable filter URLs.

## Resources

- [Live data explorer](https://usajobs-historical.vercel.app) - Interactive search and visualizations
- [Field documentation](https://abigailhaddad.github.io/usajobs_historical/) - Guide to data fields and statistics
- [USAJobs API documentation](https://developer.usajobs.gov/) - Official U.S. government developer documentation for USAJOBS APIs

**~2.85M job announcements from 2018-2026 via the Historical + Current APIs**

This repository provides USAJobs data combining both Historical and Current APIs, with deduplication and field rationalization. Data is available in three ways:
1. **🌐 Live website** - [Browse and filter interactively](https://usajobs-historical.vercel.app)
2. **📁 Ready-to-use Parquet files** - Download and analyze immediately
3. **⚙️ Full data pipeline** - Replicate the collection process yourself

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

Data files are stored in Cloudflare R2 and served to the [live site](https://usajobs-historical.vercel.app). Individual year files are typically 50-80MB Parquet and work with Python, R, or any Parquet-compatible tool.

### Option 2: Run the Pipeline Yourself
The pipeline collects data from USAJobs APIs and saves to Parquet files. Note that the USAJobs API can be unreliable - expect some failed requests that require retries. The system logs all failures and provides specific retry commands.

## Data Coverage

Data collection last run: 2026-03-20. Coverage spans 2018-2026 with approximately 2.85M job postings. Early years (pre-2017) are incomplete, mostly consisting of jobs with closing dates years after the opening dates. Note: Some job postings may have future opening dates.


| Year | Jobs Opened | Jobs Closed |
|------|-------------|-------------|
| 2013 | 5 | 0 |
| 2014 | 24 | 19 |
| 2015 | 140 | 131 |
| 2016 | 3,879 | 1,633 |
| 2017 | 237,146 | 226,249 |
| 2018 | 329,356 | 316,938 |
| 2019 | 349,256 | 336,608 |
| 2020 | 328,440 | 316,052 |
| 2021 | 369,151 | 352,375 |
| 2022 | 441,604 | 419,295 |
| 2023 | 454,652 | 434,527 |
| 2024 | 367,776 | 352,305 |
| 2025 | 239,166 | 228,615 |
| 2026 | 79,099 | 78,601 |

Early years show many long-duration postings (e.g., 3,879 opened in 2016 but only 1,633 closed that year). 2017 starts with limited data in January-February, then ramps up significantly from March onward. 

Some job postings may have future opening dates. 

## 🔄 Dual API Integration & Deduplication

This dataset combines data from **two USAJobs APIs** with the following processing:

### API Sources
- **Historical API** (`/api/historicjoa`): Past job announcements by date range
- **Current API** (`/api/Search`): Currently active job postings
- **API Documentation**: [https://developer.usajobs.gov/](https://developer.usajobs.gov/)

**Note**: In our analysis, we've found that Current API jobs generally also appear in the Historical API data, but we collect from both APIs to ensure complete coverage.

**Important**: The `current_jobs_*.parquet` files contain cumulative data - they include all jobs that have appeared in the Current API since we started collecting, not just jobs that are currently active. Once a job is added to these files, it remains there even after the position closes or is removed from the Current API. This provides a historical record of all jobs that were once "current."

### Data Processing
- **Field Rationalization**: Current API fields mapped to historical naming conventions for consistent querying
- **Data Preservation**: All original fields from both APIs retained alongside rationalized overlay fields
- **No Data Loss**: "Keep everything + overlay" approach ensures complete data accessibility
- **Deduplication Available**: Use `usajobsControlNumber` to identify records appearing in both APIs when needed

### Result
Query any year using consistent field names (e.g., `hiringAgencyName`, `positionTitle`) while retaining full access to original nested structures from both APIs. Some jobs may appear in both Historical and Current API data - deduplicate using `usajobsControlNumber` when combining datasets.

## Data Sources

The pipeline collects data from two USAJobs APIs:

1. **Historical API** (`/api/historicjoa`) - Past job announcements by date range
2. **Current API** (`/api/Search`) - Currently active job postings

See the [USAJobs API Documentation](https://developer.usajobs.gov/) for complete API details.

Both APIs are rationalized to a common schema and stored in year-based Parquet files.

## Setup

1. **Data files are stored in Cloudflare R2** (not in this Git repository). The [live site](https://usajobs-historical.vercel.app) reads data directly from R2. To work with the data locally, download the Parquet files or use the pipeline to collect your own.

2. **Create virtual environment:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Create .env file (only needed for current jobs collection):**
   ```bash
   # .env
   USAJOBS_API_TOKEN=your_api_token_here  # Get from https://developer.usajobs.gov/
   ```
   
   **Note:** The API key is only required for collecting current jobs. Historical data collection does not require authentication.

## File Structure

```
├── scripts/                 
│   ├── collect_data.py          # Historical data collection
│   ├── collect_current_data.py  # Current jobs collection
│   ├── run_parallel.sh          # Run multiple years in parallel 
│   ├── run_single.sh            # Run single date range or current jobs
│   └── monitor_parallel.sh      # Monitor parallel job progress
├── update/                  # Automated update scripts
│   ├── update_all.py            # Comprehensive update: data + docs
│   ├── generate_docs_data.py    # Generate documentation data
│   └── update_docs.py           # Update README and index.html
├── questionnaires/          # Job questionnaire analysis
│   ├── extract_questionnaires.py # Scrape questionnaires from job postings
│   ├── questionnaire_links.csv   # Links extracted from job data
│   └── raw_questionnaires/       # Scraped questionnaire text files
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

- **Cloudflare R2**: Parquet files are stored in R2 and served to the [live site](https://usajobs-historical.vercel.app)
  - `historical_jobs_YEAR.parquet`: Historical job announcements by year
  - `current_jobs_YEAR.parquet`: Current job postings by year
- **Logs**: Stored in `logs/` directory with aggressive data gap detection

## Data Architecture

The pipeline uses a "keep everything + overlay" approach:

- **Historical API**: Keeps all 40+ original fields (these field names are our standard)
- **Current API**: Keeps all original nested fields PLUS adds overlay fields using historical API names
- **Result**: No data loss + consistent querying across both APIs

## Analysis

See [`examples.py`](https://github.com/abigailhaddad/usajobs_historical/blob/main/examples.py) for usage examples.

## Questionnaire Analysis

The `questionnaires/` directory monitors federal job questionnaires for new essay questions.

**Dashboard (updated daily)**: https://federalhiringessays.netlify.app/

The system:
- Daily scrapes questionnaires from USAStaffing and Monster Government
- Identifies jobs asking "How would you help advance the President's Executive Orders and policy priorities in this role?"
- Shows trends by agency, location, grade level, and time
- Updates automatically via GitHub Actions