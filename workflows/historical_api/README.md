# Historical API Workflow

This workflow focuses exclusively on historical USAJobs API data collection and processing for years 2015-2024.

## Purpose
- Collect historical job posting data via USAJobs Historical API
- Process and store structured historical data
- Provide clean historical dataset for analysis
- Fast and efficient API-only approach

## Structure
- `data/` - Historical data storage (2015-2024)
- `scripts/api/` - Historical USAJobs API calls
- `scripts/database/` - Database operations and exports
- `scripts/pipeline/` - Workflow orchestration
- `sql/` - Database schemas and queries

## Data Sources
- **USAJobs Historical API** (`https://data.usajobs.gov/api/historicjoa`)
- Structured metadata: agency, location, salary, job series, etc.
- Date range support with incremental collection