# Current Enhanced Workflow

This workflow combines 2025 scraped job posting content with current USAJobs API data, including field rationalization between the two data sources.

## ✅ Completed Components

### 1. Current API Integration (`scripts/api/`)
- **`fetch_current_jobs.py`** - Fetches jobs from USAJobs Search API
- **Test Results**: Successfully pulled 500 current jobs with rich field structure
- **Key Fields**: PositionTitle, DepartmentName, QualificationSummary, MajorDuties, PositionLocation, etc.

### 2. Enhanced Scraping (`scripts/scraping/`)
- **`scrape_enhanced_job_posting.py`** - Enhanced scraper that extracts structured fields
- **Extracts**: Salary info, location details, dates, agency info, structured sections
- **Field Mapping**: Creates `rationalized_fields` that align with current API structure

### 3. Field Rationalization (`scripts/integration/`)
- **`compare_field_structures.py`** - Analyzes field differences between data sources
- **`field_rationalization.py`** - Combines historical, current, and scraped data into unified records
- **Test Results**: Successfully created 20 unified records with confidence scoring

## 📊 Field Analysis Results

### Data Source Coverage:
- **Historical API**: 42 structured fields (metadata-rich)
- **Current API**: 21 fields with rich content (QualificationSummary, MajorDuties, etc.)
- **Enhanced Scraping**: Extracts structured sections and detailed job information

### Key Field Mappings:
```
Historical API     → Current API        → Unified Schema
control_number     → PositionID         → control_number
position_title     → PositionTitle      → position_title
hiring_agency_name → DepartmentName     → agency_name
job_series         → JobCategory[].Code → job_series
locations          → PositionLocation   → locations
```

## 🔄 Rationalization Strategy

1. **Historical API** provides comprehensive metadata (0.9 confidence)
2. **Current API** provides rich content fields (0.95 confidence) 
3. **Scraped Data** fills gaps and adds detailed sections (0.7 confidence)
4. **Unified Schema** creates consistent field structure across all sources

## 🚀 Usage Examples

### Fetch Current Jobs:
```bash
cd workflows/current_enhanced/scripts/api/
python fetch_current_jobs.py --days-posted 7 --save-to-duckdb --max-pages 2
```

### Enhanced Scraping:
```bash
cd workflows/current_enhanced/scripts/scraping/
python scrape_enhanced_job_posting.py "CONTROL-NUMBER" --output enhanced_job.json
```

### Field Rationalization:
```bash
cd workflows/current_enhanced/scripts/integration/
python field_rationalization.py \
  --historical-db ../../../historical_api/data/historical_jobs_2015.duckdb \
  --current-json ../../data/current_jobs_*.json \
  --output ../../data/rationalized_jobs.json \
  --limit 100
```

### Field Structure Comparison:
```bash
cd workflows/current_enhanced/scripts/integration/
python compare_field_structures.py
```

## 📁 Data Flow

```
Current USAJobs API → fetch_current_jobs.py → current_jobs.json
                                           ↓
Historical DuckDB ← → field_rationalization.py ← → Scraped Content
                                           ↓
                              Unified Job Records (JSON/DuckDB)
```

## 🎯 Next Steps

1. **Scale Up**: Run field rationalization on larger datasets
2. **Scraping Integration**: Add enhanced scraping to the pipeline
3. **Current API Enrichment**: Use current job control numbers for targeted scraping
4. **DuckDB Integration**: Save unified records to DuckDB for analytics
5. **Real-time Pipeline**: Automate daily current + historical data merging

## 📊 Current Status

- ✅ Current API integration working
- ✅ Enhanced scraping framework ready
- ✅ Field rationalization engine working
- ✅ Unified schema defined
- 🔄 Ready for production pipeline development