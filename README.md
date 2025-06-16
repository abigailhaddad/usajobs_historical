# USAJobs Historic Data Pipeline

A comprehensive pipeline for collecting, processing, and analyzing USAJobs data from multiple sources with intelligent field rationalization and web scraping enhancement.

## Overview

This pipeline combines three data sources to create a unified, enriched dataset of federal job postings:

1. **Current USAJobs API** - Latest job postings with rich metadata
2. **Historical USAJobs API** - Historical job data (2015-2024) 
3. **Web Scraping** - Enhanced content from individual job pages

The pipeline automatically handles field mapping between different API versions, caches scraped content for efficiency, and generates comprehensive analysis reports.

## Quick Start

```bash
# Clone and install dependencies
git clone <repository-url>
cd usajobs_historic
pip install -r requirements.txt

# Run the complete pipeline
python run_pipeline.py --start-date 2025-01-01 --output-dir data

# Fast test run (recent data only)
python run_pipeline.py --start-date 2025-06-01 --output-dir data
```

## Pipeline Features

### ✅ **Multi-Source Data Integration**
- **Current API**: Latest job postings with complete metadata
- **Historical API**: Historical job data back to 2015
- **Web Scraping**: Enhanced content for duties, qualifications, requirements, education

### ✅ **Intelligent Field Rationalization** 
- Automatic mapping between Current and Historical API field structures
- Handles field name changes, data type differences, and missing fields
- Preserves data integrity while creating unified schema

### ✅ **Smart Caching & Performance**
- **HTML Caching**: Scraped content cached locally (79,577+ cached files)
- **Parallel Processing**: Multi-threaded scraping with rate limiting
- **Efficient Processing**: Smart caching and parallel processing

### ✅ **Comprehensive Analysis & Validation**
- **Overlap Analysis**: Compares data quality between APIs
- **Mismatch Detection**: Identifies content differences requiring attention
- **Field Coverage**: Shows completeness across data sources
- **Automated Reports**: HTML reports with interactive analysis

## Output Files & Reports

After running the pipeline, you'll get:

### 📊 **Data Files**
- `data/historical_jobs.parquet` - Historical API data
- `data/current_jobs.parquet` - Current API data  
- `data/unified_jobs.parquet` - Combined & rationalized dataset
- `data/overlap_samples.parquet` - Jobs found in both APIs for validation

### 📈 **Interactive Reports** (`reports/`)

All reports are generated in the `reports/` folder for easy access:

#### 🎯 **Job Explorer Dashboard**
- **[job_explorer.html](reports/job_explorer.html)** - Interactive dashboard answering "Who is posting what jobs when?"
  - Monthly hiring trends by agency and occupation
  - Federal job posting patterns over time
  - Clean, focused visualization of hiring activity

#### 📊 **Data Analysis Reports** 
- **[rationalization_analysis.html](reports/rationalization_analysis.html)** - Complete data analysis with field coverage, source breakdown, and overlap statistics  
- **[content_mismatch_analysis.html](reports/content_mismatch_analysis.html)** - Side-by-side comparison of content differences between APIs with similarity analysis
- **[scraping_effectiveness_report.html](reports/scraping_effectiveness_report.html)** - Analysis of web scraping success rates and content quality
- **[scraping_vs_api_comparison.html](reports/scraping_vs_api_comparison.html)** - Comparison between scraped content and API data

### 🗂️ **Report Generation** (`report_generation/`)

Source files for generating reports:
- `job_explorer.qmd` - Quarto document for interactive hiring dashboard
- `rationalization_analysis.qmd` - Quarto document for data analysis report
- `render_explorer.sh` - Script to generate job explorer dashboard

### 🔬 **Analysis Scripts** (`analysis/`)

One-off analysis scripts for data quality and comparison:
- `analyze_data_flow.py` - Analyzes data flow through rationalization process
- `check_mismatches.py` - Quick checks for content mismatches
- `check_real_similarities.py` - Checks similarity scores between data sources
- `extract_original_api_content.py` - Extracts original API content for comparison
- `real_api_comparison.py` - Compares Historical vs Current API content

### 🛠️ **Utility Scripts** (`scripts/`)

Maintenance and data update utilities:
- `regenerate_analysis.py` - Regenerates analysis reports after data updates
- `rescrape_overlap_jobs.py` - Re-scrapes overlap jobs with improved parser
- `update_overlap_scraped_content.py` - Updates overlap data with current scraper

### 🗂️ **Cached Content**
- `html_cache/` - Cached HTML content from scraped job pages (organized by control number)
- `logs/` - Pipeline execution logs

## Usage Examples

### Basic Pipeline Run
```bash
# Collect data from specific date range
python run_pipeline.py --start-date 2025-01-01 --output-dir data
```

### Generate Reports
```bash
# Generate all available reports
./generate_reports.sh

# Or generate specific dashboard
cd report_generation
./render_explorer.sh
```

### Background Execution  
```bash
# Run in background with logging
./run_pipeline.sh
```

### Custom Configuration
```bash
# Custom date range and output location
python run_pipeline.py \
  --start-date 2024-12-01 \
  --end-date 2025-01-31 \
  --output-dir custom_data \
  --max-workers 4
```

### Analysis & Utilities
```bash
# Run data quality analysis
python analysis/check_real_similarities.py

# Compare Historical vs Current API content
python analysis/real_api_comparison.py

# Re-scrape overlap jobs with updated parser
python scripts/rescrape_overlap_jobs.py

# Regenerate analysis reports
python scripts/regenerate_analysis.py
```

## Data Quality & Validation

The pipeline includes extensive validation to ensure data quality:

### **Field Coverage Analysis**
- **Historical-only jobs**: Jobs only in Historical API
- **Current-only jobs**: Jobs only in Current API  
- **Overlap jobs**: Jobs in both APIs (enables validation)

### **Content Similarity Analysis**
- **Perfect matches**: ≥99% content similarity between APIs
- **Good matches**: ≥95% content similarity
- **Mismatches**: <95% similarity (flagged for review)

### **Recent Pipeline Results** (5,619 job pairs analyzed)
- **Major Duties**: 100% coverage, 98.2% perfect matches
- **Qualification Summary**: 99.8% coverage, 99.1% perfect matches  
- **Requirements**: 51.9% coverage, 92.4% perfect matches
- **Education**: 74.8% coverage, 94.8% perfect matches

## Architecture

### **Field Rationalization**
The pipeline maps between different API structures:

```python
# Historical API -> Unified Schema
'JobTitle' -> 'position_title'
'OrganizationName' -> 'agency_name'  
'PositionLocation' -> 'locations'

# Current API -> Unified Schema  
'PositionTitle' -> 'position_title'
'AgencyName' -> 'agency_name'
'PositionLocationDisplay' -> 'locations'
```

### **Content Enhancement**
Web scraping extracts additional content not available in APIs:
- **Major Duties**: Detailed job responsibilities
- **Qualifications**: Required skills and experience
- **Requirements**: Employment conditions and clearances
- **Education**: Educational requirements and substitutions

### **Data Sources Integration**
Each job record tracks its data sources:
```json
{
  "control_number": "838326700",
  "position_title": "Mission Support Specialist",
  "data_sources": ["historical_api", "current_api_priority", "scraping"],
  "rationalization_date": "2025-06-15T08:04:27"
}
```

## Requirements

- **Python 3.8+**
- **Core packages**: `requests`, `pandas`, `beautifulsoup4`, `tqdm`
- **Analysis**: `quarto` for report generation ([install guide](https://quarto.org/docs/get-started/))
- **Storage**: Local filesystem (no database required)

Install dependencies:
```bash
pip install -r requirements.txt
```

## Monitoring & Logs

The pipeline provides detailed logging and progress tracking:

```bash
# View real-time progress
tail -f logs/pipeline_[timestamp].log

# Monitor cache usage
grep "💾 Using cached HTML" logs/pipeline_[timestamp].log | wc -l

# Check processing status  
ps aux | grep run_pipeline
```

## Troubleshooting

### **Common Issues**

**Pipeline hanging during scraping:**
- Check network connectivity
- Verify HTML cache directory permissions
- Monitor memory usage for large datasets

**Field mapping errors:**
- Review field rationalization logic in `src/field_rationalization.py`
- Check for new fields in API responses

**Missing analysis reports:**
- Ensure `quarto` is installed for rationalization_analysis.html
- Verify sufficient overlap data exists for analysis

### **Performance Optimization**

**Speed up pipeline:**
- Use cached HTML when available (default behavior)
- Reduce date range for testing
- Increase `--max-workers` for faster scraping

**Reduce resource usage:**
- Limit concurrent workers
- Use SSD storage for HTML cache
- Monitor disk space (cache can grow large)

## Contributing

1. **Data validation**: Add new field mappings or validation rules
2. **Analysis enhancement**: Improve report generation or add new metrics  
3. **Performance**: Optimize scraping or caching strategies
4. **Documentation**: Update examples or troubleshooting guides

## License

See LICENSE file for details.

---

**Last Updated**: 2025-06-15  
**Pipeline Version**: Field Rationalization v2.0  
**Data Coverage**: 2015-present with multi-source integration