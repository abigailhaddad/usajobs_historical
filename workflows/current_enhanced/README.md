# Enhanced USAJobs Data Pipeline

A comprehensive pipeline that fetches, processes, and analyzes USAJobs data from multiple sources with intelligent field rationalization and validation.

## Overview

This pipeline combines:
- **Current USAJobs API** - Latest job postings with rich metadata
- **Historical USAJobs API** - Historical job posting data  
- **Web Scraping** - Enhanced content from individual job pages
- **Field Rationalization** - Intelligent mapping between data sources
- **Validation** - 100% verified field accuracy on overlapping jobs

## Quick Start

```bash
# Run the complete pipeline (recommended)
python run_pipeline.py --render-report

# Fast test run (skip scraping)
python run_pipeline.py --historical-jobs 25 --skip-scraping --render-report

# Custom configuration
python run_pipeline.py \
  --historical-jobs 100 \
  --scrape-jobs 50 \
  --current-days 14 \
  --output-name "weekly_analysis" \
  --render-report
```

## Pipeline Steps

1. **📊 Historical Jobs** - Fetches recent historical job postings via API
2. **💾 Database Creation** - Creates structured historical jobs database
3. **🕷️ Web Scraping** - Scrapes job pages for enhanced content (optional)
4. **🌐 Current Jobs** - Fetches latest postings from current USAJobs API  
5. **🔄 Field Rationalization** - Maps and unifies fields across data sources
6. **📊 Analysis Report** - Generates comprehensive HTML analysis report

## Options

```
--historical-jobs N     Number of historical jobs to process (default: 50)
--scrape-jobs N        Number of jobs to scrape (default: all historical)
--current-days N       Days back for current job search (default: 7)
--output-name NAME     Custom name for output files (auto-generated if not specified)
--skip-scraping        Skip web scraping step (faster for testing)
--render-report        Generate HTML analysis report after pipeline
```

## Output Files

```
data/
├── historical_jobs_[name].duckdb    # Historical jobs database
├── current_jobs_[timestamp].json    # Current API jobs  
├── unified_[name].duckdb            # Rationalized unified dataset
└── rationalization_analysis.html    # Analysis report (if --render-report)
```

## Key Features

### ✅ **Validated Field Mapping**
- 100% accuracy verified on overlapping jobs between APIs
- Extracts nested fields (salary, job series, work schedule, etc.)
- Maps equivalent content across different data structures

### 🎯 **Comprehensive Data Extraction**
- **Core Fields**: Position title, agency, job series, salary, location
- **Rich Content**: Job duties, qualifications, requirements, education
- **Metadata**: Security clearance, telework eligibility, total openings
- **Application Details**: How to apply, required documents, evaluation process

### 📊 **Analysis & Validation**
- Side-by-side comparison of same jobs across data sources
- Field coverage analysis by data source
- Data quality metrics and confidence scores
- Interactive HTML reports with detailed breakdowns

## Architecture

```
scripts/
├── api/                    # USAJobs API integration
│   └── fetch_current_jobs.py
├── scraping/              # Web scraping for enhanced content
│   └── scrape_enhanced_job_posting.py
└── integration/           # Field mapping and rationalization
    ├── field_rationalization.py
    ├── field_crosswalk_analysis.py
    └── compare_field_structures.py

rationalization_analysis.qmd   # Analysis report template
run_pipeline.py               # Main pipeline orchestrator
```

## Dependencies

```bash
pip install duckdb requests beautifulsoup4 pandas
# Quarto (for report generation): https://quarto.org/docs/get-started/
```

## Use Cases

- **Job Market Analysis** - Compare current vs historical job trends
- **Field Research** - Validate API data quality and completeness  
- **Data Integration** - Unified dataset from multiple USAJobs sources
- **Content Analysis** - Rich text analysis of job requirements and duties