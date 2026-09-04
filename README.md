# USAJobs Data Pipeline

**Data collection last run: 2026-09-04**

**This is not an official USAJobs project.**

**~2.85M job announcements from 2018-2026 via the Historical + Current APIs**

## Browse the Data

**[Live site](https://usajobs-historical.abigailhaddad.com)** -- Interactive DataTable with filters, charts (jobs by month, top agencies, grade distribution), multi-column sorting, and shareable filter URLs. Shows a curated subset of 14 columns (title, agency, grade, salary, dates, location, etc.).

## Getting the Data

| Option | What you get | How |
|--------|-------------|-----|
| **[Live site](https://usajobs-historical.abigailhaddad.com)** | 14 key columns, interactive filtering/charts | Just visit the site |
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

Data collection last run: 2026-09-04. Coverage spans 2018-2026 with approximately 2.85M job postings. Early years (pre-2017) are incomplete, mostly consisting of jobs with closing dates years after the opening dates.

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
| 2025 | 256,094 | 240,107 |
| 2026 | 344,597 | 341,934 |

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

### ⚠️ Known limitation: hiringAgencyName in current parquets

When combining `historical_jobs_*.parquet` and `current_jobs_*.parquet`, be aware that `hiringAgencyName` can differ for the same job:

- **Historical parquets** usually have the specific bureau-level name (e.g. `"Executive Office for U.S. Attorneys and the Office of the U.S. Attorneys"`), but this is not guaranteed — both APIs return only the department-level name (e.g. `"Department of Justice"`) when the posting agency never populated the sub-agency field. This means some jobs genuinely cannot be attributed to a specific bureau, regardless of which API source you use.
- **Current parquets** sometimes have only the department-level name even when the historical API has the bureau name, because the current API's `OrganizationName` field is not always populated.

Naively unioning both and grouping by `hiringAgencyName` will **double-count** those jobs — once under the specific bureau name (from historical) and once under the department name (from current).

**Correct approach:** Deduplicate by `usajobsControlNumber`, preferring the record where `hiringAgencyName != hiringDepartmentName`. The `scripts/parquet_utils.py` module provides ready-to-use helpers:

```python
# DuckDB (server-side, no download)
from scripts.parquet_utils import build_deduped_query
sql = build_deduped_query(hist_urls=[...], curr_urls=[...],
                          where="hiringDepartmentName = 'Department of Justice'")
df = duckdb.connect().execute(sql).df()

# Pandas (local files)
from scripts.parquet_utils import combine_and_fix
result = combine_and_fix(hist_frames=[hist_df], curr_frames=[curr_df])
```

The `jobs_5yr.parquet` web dataset already has this fix applied — it is safe to query directly without deduplication.

## Scraping usajobs.gov instead of the API (shadow run)

Since 2026-09-02 the daily pipeline also collects the same postings without an
API key, by scraping usajobs.gov. It writes `data/scraped_jobs_*.parquet`, and
nothing downstream reads them yet. The point is to find out whether the site
could replace the Current API if the key ever stopped working.

Two keyless sources. `POST https://www.usajobs.gov/Search/ExecuteSearch` is the
JSON endpoint behind the search page — no auth, no cookie — and returns about
25 fields per posting plus a facet block. The facets are computed over the
whole result set rather than the 10,000 the `Total` field reports, so one call
gives the true open-inventory size and its breakdown by occupational series;
that's how the collection slices itself under the same 10,000 ceiling the API
has. Then `GET https://www.usajobs.gov/job/{control_number}` for postings we
haven't stored, which is server-rendered HTML where every overview field is a
`<dt>`/`<dd>` pair.

It also pulls the announcement body apart by the page's own headings, which the
API can't do at all: `jobSummary`, `majorDuties`, `conditionsOfEmployment`,
`qualificationSummary`, `education`, `additionalInformation`,
`howYouWillBeEvaluated`, `requiredDocuments`, `howToApply`, plus the whole-page
`text` the [announcement-text dataset](https://github.com/abigailhaddad/joa)
stores. `MatchedObjectDescriptor` drops content the page shows, and even the
page's own ld+json carries a truncated `qualifications` — 1,503 characters
against the page's 1,845 on the announcement the tests use. Across a 50-page
live sample every field but `education` was populated on every announcement,
and `education` is genuinely missing from about one in six.

`scripts/compare_scrape_to_api.py` diffs the two collections after each run and
writes a report to `logs/scrape_vs_api_<date>.md`, uploaded as a workflow
artifact. On the first full check — all 354 open 2210 postings, 2026-09-02 —
coverage was 354/354 and all 18 comparable fields matched exactly. The scrape
also carries `workSchedule`, `promotionPotential` and `supervisoryStatus`,
which the Current API collection doesn't populate. The text fields have no API
counterpart to check against, so the report tracks their fill rate instead: if
usajobs.gov changes its markup they go empty, and that's the signal.

Duty stations come out as a `PositionLocations` column in the historical API's
`{positionLocationCity, positionLocationState}` shape, so `prep_web_data.py`
consumes them through the path it already has. Rendered through that same
extractor, the location string matched the API exactly on 337 of 348 postings.
The other 11 are the page's own doing rather than a parse failure: a posting
with 31 API entries across 22 cities renders 12, and some collapse to a label
like "Location Negotiable After Selection". The search endpoint's own
`positionLocationCount` matched the API on all 348, so the accurate count is
kept alongside the page's list in `scrapedLocationCount` and the gap stays
measurable.

What scraping can't give you: `hiringAgencyCode`, `hiringDepartmentCode`,
`agencyLevel`, `agencyLevelSort`, `vendor`, `whoMayApply`. Those are API-only
and the announcement page never shows them. Nothing reads them off the current
parquets — `repoll_status.py` is the only consumer and it writes them into the
historical files, from the keyless `/api/historicjoa`. It also can't backfill — search
only lists open postings, so this accumulates forward from the day it started.
Closed announcement pages do stay up indefinitely, which is what the
[announcement-text dataset](https://github.com/abigailhaddad/joa) relies on.

Coverage shortfalls and field disagreements past the thresholds in
`compare_scrape_to_api.py` open a GitHub issue. Neither the scrape nor the
comparison can fail the daily run.

## The HuggingFace announcement dataset

[abigailhaddad/usajobs-scraping](https://huggingface.co/datasets/abigailhaddad/usajobs-scraping)
is one parquet per month plus a manifest, published daily by
`scripts/publish_to_huggingface.py`. It joins the two halves this pipeline
already has on disk: structured fields from `historical_jobs_{year}.parquet`,
and announcement text from `scraped_jobs_{year}.parquet`. Nothing is fetched at
publish time.

This replaced a publish path that lived in a separate repo
([joa](https://github.com/abigailhaddad/joa)) and keyed off the historical
mirror alone. Two things were wrong with it. Its field list never selected
`positionTitle`, so the published dataset had no job title in it — easy to miss,
because its controls CSV aliased `announcementNumber` as "title". And it could
not carry the eleven structured announcement sections, which come from a parse
that lives here.

The dataset went from 40 columns to 53: `positionTitle` and
`hiringSubelementName`, plus `jobSummary`, `majorDuties`, `requirements`,
`conditionsOfEmployment`, `qualificationSummary`, `education`,
`additionalInformation`, `benefits`, `howYouWillBeEvaluated`,
`requiredDocuments` and `howToApply`. The whole-page `text` column stays, so
anything built against it keeps working — columns are only ever added.

A month file is rewritten wholesale, so the publisher refuses to write a month
when the local join is missing announcements the dataset already holds, rather
than silently shrinking it. That is the expected state while the page backfill
is still running.

```bash
python scripts/publish_to_huggingface.py --dry-run
python scripts/publish_to_huggingface.py
python scripts/publish_to_huggingface.py --refresh-all   # rewrite every month
```

### What stays on disk

Announcement text is 97.8% of `scraped_jobs_{year}.parquet` — 5.42 KB of a
5.54 KB row — so a full year would be 920 MB local against 20 MB for
everything else in it. HuggingFace is the store for the text instead: after a
month publishes, the publisher blanks those rows' text columns locally, and
what stays is the structured shadow the API comparison reads plus anything not
yet pushed. Measured on real rows, that takes a 162,000-row year from 920 MB to
34 MB. `--keep-text` opts out.

### Backfilling announcement pages

The scraped collection only starts the day it was switched on, so postings from
earlier in the year have metadata but no text. usajobs.gov serves closed
announcements indefinitely, so they can be filled in — roughly 160k pages for a
full year.

It works a month at a time and publishes each one as it lands, so the working
set stays near a single month rather than piling up the year, and a killed run
resumes at month granularity. Within a month it writes immutable shards and
folds them in once at the end.

It is network-bound, not CPU-bound: a page costs ~32 ms to parse, so a full
year is about 86 CPU-minutes over those three hours. It runs niced and under a
CPU governor by default. `--max-cpu` is a share of **one** core, not of the
machine — measured, an uncapped run sits at 53% of a core, and `--max-cpu 15`
holds it to 18% at three times the wall clock.

```bash
python scripts/backfill_scraped_pages.py --year 2026 --dry-run
python scripts/backfill_scraped_pages.py --year 2026
python scripts/backfill_scraped_pages.py --year 2026 --max-cpu 20   # gentler
python scripts/backfill_scraped_pages.py --year 2026 --no-publish   # keep local
```

2026 ran locally in 175 minutes for 160k pages: zero failures, zero 404s, 68.8
CPU-minutes, and the local parquet ended at 15 MB because each month's text is
pruned once published.

### The rest of the backlog, in Actions

There are ~3.2M postings from 2017 on — about 81 hours of fetching at the rate
one runner sustains, so it does not belong in a single run. The
**Backfill Announcement Pages** workflow chunks it one month per job, ~30k
pages and ~45 minutes each. Dispatch it with a year, a list, or a range
(`2017`, `2017,2018`, `2019-2022`).

Month jobs are stateless. `--known-from-hf` takes the already-done set from the
dataset's manifest instead of a shared parquet, so nothing is pulled from R2
beforehand or written back after and jobs cannot race. `max-parallel` is
deliberately 3: it multiplies with each job's `workers`, so 3 x 6 is already 18
concurrent requests against the 8 a daily run uses.

Old pages are all still served — a sample across 2017, 2019, 2021, 2023 and
2025 came back 40/40 alive with every section parsing.

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
│   └── scraped_jobs_YEAR.parquet     # Scraped shadow collection (unused downstream)
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

**Workflow for data updates:**

```bash
# Collect current jobs and update documentation
python update/update_all.py      # Update data + docs
```

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
