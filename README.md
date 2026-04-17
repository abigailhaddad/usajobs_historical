# USAJobs Data Pipeline

**Data collection last run: 2026-04-17**

**This is not an official USAJobs project.**

**~2.85M job announcements from 2018-2026 via the Historical + Current APIs**

## Browse the Data

**[Live site](https://usajobs-historical.vercel.app)** -- Interactive DataTable with filters, charts (jobs by month, top agencies, grade distribution), multi-column sorting, and shareable filter URLs. Shows a curated subset of 14 columns (title, agency, grade, salary, dates, location, etc.).

## Getting the Data

| Option | What you get | How |
|--------|-------------|-----|
| **[Live site](https://usajobs-historical.vercel.app)** | 14 key columns, interactive filtering/charts | Just visit the site |
| **Web dataset** | Same 14 columns as the site, one parquet file | `python download_data.py --web-only` |
| **Full dataset** | All 40+ fields per job (nested JSON, qualifications, duty descriptions, all original API fields) | `python download_data.py` |
| **Run the pipeline yourself** | Collect your own data from the USAJobs APIs | See [Setup](#setup) below |

```bash
# Download everything (40+ fields, ~50-80MB per year file)
python download_data.py

# Just the web dataset (14 columns, single file, smaller)
python download_data.py --web-only

# Download and zip
python download_data.py --zip
```

```python
import pandas as pd

# Full dataset
df_2024 = pd.read_parquet('data/historical_jobs_2024.parquet')
print(f"Loaded {len(df_2024):,} federal job postings from 2024")
print(f"Columns: {len(df_2024.columns)}")  # 40+ fields

# Or the web dataset (smaller, deduplicated, all years in one file)
df_web = pd.read_parquet('data/jobs_5yr.parquet')
```

Files are Parquet format and work with Python, R, or any Parquet-compatible tool.

## Resources

- [Field documentation](https://abigailhaddad.github.io/usajobs_historical/) - Guide to data fields and statistics
- [USAJobs API documentation](https://developer.usajobs.gov/) - Official API docs
- [`examples.py`](https://github.com/abigailhaddad/usajobs_historical/blob/main/examples.py) - Analysis examples

## Data Coverage

Data collection last run: 2026-04-17. Coverage spans 2018-2026 with approximately 2.85M job postings. Early years (pre-2017) are incomplete, mostly consisting of jobs with closing dates years after the opening dates.

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
| 2025 | 239,171 | 228,618 |
| 2026 | 100,496 | 99,754 |

Early years show many long-duration postings (e.g., 3,879 opened in 2016 but only 1,633 closed that year). 2017 starts with limited data in January-February, then ramps up significantly from March onward. Some job postings may have future opening dates.

## Dual API Integration & Deduplication

This dataset combines data from **two USAJobs APIs**:

- **Historical API** (`/api/historicjoa`): Past job announcements by date range (no auth required)
- **Current API** (`/api/Search`): Currently active job postings (requires API key)
- **API Documentation**: [developer.usajobs.gov](https://developer.usajobs.gov/)

Current API jobs generally also appear in the Historical API data, but we collect from both to ensure complete coverage. The `current_jobs_*.parquet` files contain cumulative data -- all jobs that have ever appeared in the Current API, not just currently active ones.

### Data Processing

- **Field Rationalization**: Current API fields mapped to historical naming conventions for consistent querying
- **Data Preservation**: All original fields from both APIs retained alongside rationalized overlay fields
- **Deduplication**: Use `usajobsControlNumber` to identify records appearing in both APIs

Both APIs are rationalized to a common schema and stored in year-based Parquet files in Cloudflare R2.

## Data Storage

- **Cloudflare R2**: All parquet files are stored in R2 (not in this git repo due to size)
  - `historical_jobs_YEAR.parquet`: Historical job announcements by year (full 40+ fields)
  - `current_jobs_YEAR.parquet`: Current job postings by year (full fields)
  - `jobs_5yr.parquet`: Slim 14-column file used by the live site
- **Logs**: Stored in `logs/` directory

## Setup

1. **Data files are in Cloudflare R2** (not in this git repo). Run `python download_data.py` to download them, or run the pipeline to collect your own.

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

## File Structure

```
├── scripts/
│   ├── collect_data.py          # Historical data collection
│   ├── collect_current_data.py  # Current jobs collection
│   ├── prep_web_data.py         # Build slim 14-column parquet for website
│   ├── sync_to_r2.py            # Upload parquet files to Cloudflare R2
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
├── data/                    # Local data (gitignored, stored in R2)
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

## Questionnaire Analysis

The `questionnaires/` directory monitors federal job questionnaires for new essay questions.

**Dashboard (updated daily)**: https://federalhiringessays.netlify.app/

The system:
- Daily scrapes questionnaires from USAStaffing and Monster Government
- Identifies jobs asking "How would you help advance the President's Executive Orders and policy priorities in this role?"
- Shows trends by agency, location, grade level, and time
- Updates automatically via GitHub Actions
