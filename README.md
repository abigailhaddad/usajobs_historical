# USAJobs Data Pipeline

**Data collection last run: 2026-09-17** (updated daily from GitHub Actions)

**This is not an official USAJobs project.**

**3,233,857 job announcements from 2013-2026 via the Historical + Current APIs**

## Getting the Data

| Option | What you get | How |
|--------|-------------|-----|
| **[Live site](https://usajobs-historical.abigailhaddad.com)** | 14 key columns, interactive filtering/charts | Just visit the site |
| **Web dataset** | Same 14 columns as the site, one parquet file (~50MB) | `python download_data.py --web-only` |
| **Full dataset** | All 40+ fields per job (nested JSON, qualifications, duty descriptions, all original API fields); one file per year, ~1.4GB in total | `python download_data.py` |
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

# Or the web dataset (smaller, deduplicated, 2018-present in one file)
df_web = pd.read_parquet('data/jobs_5yr.parquet')  # historical name; covers 2018-present
```

Files are Parquet format and work with Python, R, or any Parquet-compatible tool.

The 14 columns in the web dataset are `usajobsControlNumber`, `positionTitle`,
`hiringDepartmentName`, `hiringAgencyName`, `grade`, `minimumSalary`,
`maximumSalary`, `openDate`, `closeDate`, `appointmentType`, `serviceType`,
`locations`, `status` and `occupationalSeries`.

`download_data.py` writes each file whole and skips nothing on a re-run, so if a
download dies partway through, run it again -- it overwrites rather than
resuming.

## Known limitation: combining historical and current parquets

When combining `historical_jobs_*.parquet` and `current_jobs_*.parquet`, `hiringAgencyName` can differ for the same job:

- **Historical parquets** usually have the specific bureau-level name (e.g. `"Executive Office for U.S. Attorneys and the Office of the U.S. Attorneys"`), but this is not guaranteed — both APIs return only the department-level name (e.g. `"Department of Justice"`) when the posting agency never populated the sub-agency field. This means some jobs genuinely cannot be attributed to a specific bureau, regardless of which API source you use.
- **Current parquets** sometimes have only the department-level name even when the historical API has the bureau name, because the current API's `OrganizationName` field is not always populated.

Naively unioning both and grouping by `hiringAgencyName` will **double-count** those jobs — once under the specific bureau name (from historical) and once under the department name (from current).

**Correct approach:** Deduplicate by `usajobsControlNumber`, preferring the record where `hiringAgencyName != hiringDepartmentName`, and falling back to either record when neither has a bureau name. The `scripts/parquet_utils.py` module provides ready-to-use helpers:

```python
# DuckDB (server-side, no download)
import duckdb
from scripts.parquet_utils import build_deduped_query

R2 = "https://pub-317c58882ec04f329b63842c1eb65b0c.r2.dev"
years = range(2023, 2027)
sql = build_deduped_query(
    hist_urls=[f"{R2}/historical_jobs_{y}.parquet" for y in years],
    curr_urls=[f"{R2}/current_jobs_{y}.parquet" for y in years],
    where="hiringDepartmentName = 'Department of Justice'")
df = duckdb.connect().execute(sql).df()

# Pandas (local files)
from scripts.parquet_utils import combine_and_fix
result = combine_and_fix(hist_frames=[hist_df], curr_frames=[curr_df])
```

The `jobs_5yr.parquet` web dataset already has this fix applied — it is safe to query directly without deduplication.

## Resources

- [Field documentation](https://abigailhaddad.github.io/usajobs_historical/) - Guide to data fields and statistics
- [USAJobs API documentation](https://developer.usajobs.gov/) - Official API docs
- [`examples.py`](https://github.com/abigailhaddad/usajobs_historical/blob/main/examples.py) - Analysis examples

## Data Coverage

Coverage runs 2013-2026. Early years (pre-2017) are incomplete, mostly jobs
with closing dates years after the opening dates.

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
| 2024 | 367,192 | 352,305 |
| 2025 | 168,531 | 161,045 |
| 2026 | 184,915 | 183,515 |

Counts are distinct announcements, deduplicated by `usajobsControlNumber`
across both APIs. Each row is deduplicated within its own year file, so a
posting open across a year boundary is counted in both years and the column
sums to a few hundred more than the header total.

Figures published before September 2026 were row counts added together, which
counted every posting appearing in both APIs twice — 2026 read 356,088 against
a real 180,740.

Early years show many long-duration postings (e.g., 3,879 opened in 2016 but only 1,633 closed that year). 2017 starts with limited data in January-February, then ramps up from March onward. Some postings have opening dates in the future, so a filter on
open date will pick up jobs that have not opened yet.

## Dual API Integration & Deduplication

This dataset combines data from **two USAJobs APIs**:

- **Historical API** (`/api/historicjoa`): Past job announcements by date range (no auth required)
- **Current API** (`/api/Search`): Currently active job postings (requires API key)

Current API jobs generally also appear in the Historical API data, but we collect from both anyway. The `current_jobs_*.parquet` files contain cumulative data -- all jobs that have ever appeared in the Current API, not just currently active ones.

Current API fields are mapped onto the historical naming, so the same query
works against either. Nothing is dropped -- the original fields from both APIs
stay alongside the mapped ones.

Both APIs are rationalized to a common schema and stored in year-based Parquet files in Cloudflare R2.

## Announcement text (HuggingFace)

[abigailhaddad/usajobs-scraping](https://huggingface.co/datasets/abigailhaddad/usajobs-scraping)
carries the full announcement text the API does not return: one parquet per
month, published daily. Eleven parsed sections -- `jobSummary`, `majorDuties`,
`qualificationSummary`, `education` and the rest -- plus the whole-page `text`.

It exists because the daily pipeline also scrapes usajobs.gov without an API
key, alongside the API collection. That scrape is the source of the text, and
it doubles as a check on whether the site could replace the Current API if the
key ever stopped working.

```sql
SELECT count(*)
FROM read_parquet('hf://datasets/abigailhaddad/usajobs-scraping/data/2025_*.parquet');
```

DuckDB reads the month files over HTTP, so a query costs the columns it touches
rather than the whole dataset. Go a year at a time -- globbing every file at
once gets an anonymous reader a 429 partway through.

The dataset viewer's filter and search are a different thing. HuggingFace
indexes a fixed slice of a dataset this size -- here that was 99,059 rows of
about 3.2 million -- so counts off the viewer are roughly 3% of the data, and
the page does not say so. Use DuckDB for anything you plan to quote.

`notebooks/announcement_text_queries.ipynb` works through four questions the
API cannot answer: who needs a doctorate, where the structured fields and the
prose disagree, direct hire authority, and one query that looks great and means
nothing.

How the scrape works, what it can't reach, how the text is published and
pruned, and how the 2013-2025 backfill ran: [docs/scraping.md](docs/scraping.md).

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
│   ├── collect_scraped_data.py  # Same jobs, scraped from usajobs.gov (shadow)
│   ├── usajobs_scrape.py        # Search endpoint + announcement-page parsing
│   ├── compare_scrape_to_api.py # Diff the scraped and API collections
│   ├── publish_to_huggingface.py # Push the announcement dataset to HuggingFace
│   ├── backfill_scraped_pages.py # Fetch pages for postings scraped before the scrape existed
│   ├── prep_web_data.py         # Build slim 14-column parquet for website
│   ├── sync_to_r2.py            # Upload parquet files to Cloudflare R2
│   ├── run_parallel.sh          # Run multiple years in parallel
│   ├── run_single.sh            # Run single date range or current jobs
│   └── monitor_parallel.sh      # Monitor parallel job progress
├── docs/
│   └── scraping.md              # How the scrape and the announcement dataset work
├── update/                  # Automated update scripts
│   ├── update_all.py            # Comprehensive update: data + docs
│   ├── generate_docs_data.py    # Generate documentation data
│   └── update_docs.py           # Update README and index.html
├── questionnaires/          # Job questionnaire analysis
│   ├── extract_questionnaires.py # Scrape questionnaires from job postings
│   ├── questionnaire_links.csv   # Links extracted from job data
│   └── raw_questionnaires/       # Scraped questionnaire text files
├── web/                     # The live site (static files only -- no server)
│   ├── index.html               # Job table, filters, charts
│   ├── pivot.html               # Pivot table
│   ├── poster.html              # Postings-by-department poster
│   └── shared/
│       ├── wasm-api.js          # aggregate/jobs/pivot/filterOptions/downloadCsv
│       └── filters.js           # Column + filter definitions shared by the pages
├── data/                    # Local data (gitignored, stored in R2)
│   ├── historical_jobs_YEAR.parquet  # Historical jobs by year
│   ├── current_jobs_YEAR.parquet     # Current jobs by year
│   └── scraped_jobs_YEAR.parquet     # Scraped pages; source of the announcement text
└── logs/                    # Auto-generated pipeline logs
```

### How the site works

There is no backend. `web/` is plain static HTML, and every page loads
[DuckDB-WASM](https://duckdb.org/docs/api/wasm/overview.html) in the browser and
queries `jobs_5yr.parquet` on Cloudflare R2 directly over HTTP range requests --
so a filter or a chart reads only the row groups it needs, not the whole file.
`web/shared/wasm-api.js` holds the query layer: `aggregate`, `jobs`, `pivot`,
`filterOptions` and `downloadCsv`.

That makes the parquet's physical layout part of the site's performance.
`scripts/prep_web_data.py` writes it sorted, ZSTD-compressed and in 100k-row row
groups: 129.2MB to 51.2MB, 3 row groups to 30. The job-listing query went from
7.0s to 0.18s, reading 9MB instead of 56MB.

## Run Pipeline

```bash
# Collect current jobs and update documentation
cd update && python update_all.py
```

It has to be run from `update/` -- it resolves everything through `../`, and
exits immediately if you start it anywhere else.

`update_all.py` runs the scraped collection and the comparison too. To run
either alone, no API key needed:

```bash
python scripts/collect_scraped_data.py --data-dir data/
python scripts/collect_scraped_data.py --data-dir data/ --series 2210  # spot check
python scripts/compare_scrape_to_api.py --data-dir data/
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

`collect_data.py` prints a retry command for every date that failed:

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

It scrapes USAStaffing and Monster Government daily, looking for jobs that ask
"How would you help advance the President's Executive Orders and policy
priorities in this role?" The dashboard breaks it out by agency, location,
grade and date, and updates from Actions.

## License and citation

MIT, see [LICENSE](LICENSE). The data itself is US federal government work.

If you use this in published work:

> Haddad, Abigail. *USAJobs Historical Data Pipeline*, 2026.
> https://github.com/abigailhaddad/usajobs_historical

Bugs and questions: [open an issue](https://github.com/abigailhaddad/usajobs_historical/issues).
