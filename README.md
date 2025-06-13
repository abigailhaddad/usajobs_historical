# USAJobs Historic Data Collection

This repository contains two complementary workflows for collecting and processing USAJobs data from different sources and time periods.

## Workflows Overview

### 1. Enhanced USAJobs Pipeline (`usajobs_pipeline/`)

A comprehensive, modern pipeline that combines multiple data sources for enriched job posting analysis.

**Key Features:**
- **Multi-source integration**: Current API + Historical API + Web scraping
- **Field rationalization**: Intelligent mapping between different data structures  
- **Parallel processing**: Fast collection with multiple workers
- **Parquet storage**: Lock-free parallel processing and efficient storage
- **Validation**: 100% verified field accuracy on overlapping jobs
- **Analysis reports**: Automated HTML report generation

**Data Sources:**
- Current USAJobs API (latest postings with rich metadata)
- Historical USAJobs API (historical job posting data)
- Web scraping (enhanced content from individual job pages)

**Quick Start:**
```bash
cd usajobs_pipeline/

# Run complete pipeline
python run_pipeline_parquet.py --start-date 2025-01-01

# Fast test run (no scraping)
python run_pipeline_parquet.py --start-date 2025-01-01 --no-scraping

# Custom configuration
python run_pipeline_parquet.py \
  --start-date 2025-01-01 \
  --scrape-workers 8 \
  --output-dir custom_data
```

**Output:**
- `data_parquet/historical_jobs/` - Historical API data
- `data_parquet/current_jobs/` - Current API data  
- `data_parquet/unified_jobs/` - Rationalized unified dataset
- `rationalization_analysis.html` - Analysis report

### 2. Historical API Workflow (`workflows/historical_api/`)

A focused, efficient workflow for collecting pure historical USAJobs API data (2015-2024).

**Key Features:**
- **API-only approach**: Fast, reliable historical data collection
- **DuckDB storage**: Efficient structured data storage by year
- **Parallel processing**: Multi-worker data collection
- **PostgreSQL integration**: Optional export to PostgreSQL
- **Date range filtering**: Collect specific time periods

**Data Coverage:**
- **Total**: 2,947,854+ jobs across 11 years (2015-2025)
- **Coverage**: 90.6% of expected days from 2015-01-01 to present
- **Peak years**: 2022-2024 with 400,000+ jobs per year

**Quick Start:**
```bash
cd workflows/historical_api/

# Collect full year of data
python scripts/api/historic_pull_parallel.py \
  --start-date 2023-01-01 \
  --end-date 2023-12-31

# Collect with PostgreSQL export
python scripts/api/historic_pull_parallel.py \
  --start-date 2023-01-01 \
  --end-date 2023-12-31 \
  --load-to-postgres \
  --workers 8

# Query collected data
python scripts/database/query_duckdb.py data/historical_jobs_2023.duckdb
```

**Output:**
- `data/historical_jobs_[YEAR].duckdb` - Annual historical data files
- PostgreSQL tables (optional)

## Workflow Comparison

| Feature | Enhanced Pipeline | Historical API |
|---------|------------------|----------------|
| **Data Sources** | Current API + Historical API + Web scraping | Historical API only |
| **Storage Format** | Parquet files | DuckDB |
| **Time Range** | Current + Recent historical | 2015-2024 historical |
| **Field Enrichment** | Yes (rationalization + scraping) | No (raw API data) |
| **Analysis Reports** | Automated HTML reports | Manual querying |
| **Use Case** | Rich analysis, data validation | Fast historical collection |
| **Performance** | Moderate (due to scraping) | Fast (API-only) |

## When to Use Which Workflow

### Use Enhanced Pipeline When:
- You need enriched job content (duties, qualifications, requirements)
- You want unified data from multiple sources
- You need current + historical data integration
- You want automated analysis and validation reports
- You're doing comprehensive job market analysis

### Use Historical API Workflow When:
- You need large-scale historical data collection (2015-2024)
- You want fast, efficient API-only data collection
- You're building time-series datasets
- You need PostgreSQL integration
- You want raw, unprocessed historical job data

## Repository Structure

```
usajobs_historic/
├── usajobs_pipeline/          # Enhanced multi-source pipeline
│   ├── run_pipeline_parquet.py    # Main pipeline script
│   ├── scripts/                   # Pipeline components
│   ├── data_parquet/              # Parquet data storage
│   └── README.md                  # Detailed pipeline docs
├── workflows/historical_api/   # Historical API workflow
│   ├── scripts/api/               # API collection scripts
│   ├── scripts/database/          # Database operations
│   ├── data/                      # DuckDB storage
│   └── sql/                       # Database schemas
├── shared/                     # Shared utilities
│   └── api/                       # Common API functions
├── analysis/                   # Analysis notebooks and reports
└── archive/                    # Legacy code and documentation
```

## Dependencies

Both workflows require:
```bash
pip install -r requirements.txt
```

Key packages:
- `requests` - API calls
- `pandas` - Data processing  
- `duckdb` - Database operations
- `tqdm` - Progress bars
- `beautifulsoup4` - Web scraping (Enhanced Pipeline only)

For Enhanced Pipeline reports:
- `quarto` - Report generation ([installation guide](https://quarto.org/docs/get-started/))

## Getting Started

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd usajobs_historic
   pip install -r requirements.txt
   ```

2. **Choose your workflow:**
   - For comprehensive analysis: Use Enhanced Pipeline
   - For historical data collection: Use Historical API Workflow

3. **Check the individual README files** for detailed usage instructions:
   - `usajobs_pipeline/README.md` - Enhanced Pipeline details

## Data Sources

### USAJobs APIs
- **Current API**: `https://data.usajobs.gov/api/search` - Latest job postings
- **Historical API**: `https://data.usajobs.gov/api/historicjoa` - Historical data (2015-2024)

### Web Scraping
- Individual job posting pages for enhanced content extraction
- Used only in Enhanced Pipeline workflow

## License

See LICENSE file for details.