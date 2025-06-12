# Archive Directory

This directory contains files that were used during the development and testing of the optimized scraping approach but are no longer needed for the main pipeline.

## Contents

### old_scraping/
- `scrape_enhanced_job_posting_old.py` - Original scraper that extracted both metadata and content (replaced with optimized version)

### testing/
- `analyze_scraping_effectiveness.py` - Analysis script to compare what we extract vs what we need
- `batch_test_scraping.py` - Script for testing scraper on multiple jobs
- `historical_api_field_comparison.py` - Field comparison between historical API and scraping 
- `scrape_optimized_content.py` - Standalone optimized scraper used for testing
- `scraping_summary.md` - Summary of analysis findings
- `test_scraping_comparison.py` - Script to compare scraped data with API data

### analysis/
- (Reserved for future analysis files)

## Notes

The optimized scraper is now integrated into `workflows/current_enhanced/scripts/scraping/scrape_enhanced_job_posting.py` and focuses only on extracting content sections that are NOT available in the historical API.

Key optimization: Removed extraction of metadata fields (salary, location, dates, agency info, etc.) that are already provided by the historical API, reducing processing time by ~40%.

Date archived: 2025-06-12