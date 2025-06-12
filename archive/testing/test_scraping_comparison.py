#!/usr/bin/env python3
"""
Test script to compare scraped job data with current API data

This script:
1. Scrapes example job postings
2. Fetches the same jobs from the current API (if available)
3. Compares the data to identify field mappings and gaps
4. Does NOT assume historical API fields exist
"""

import sys
import os
import json
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scraping.scrape_enhanced_job_posting import scrape_enhanced_job_posting
from api.fetch_current_jobs import flatten_current_job

# Load environment variables
load_dotenv()
API_KEY = os.getenv("USAJOBS_API_TOKEN")

def fetch_job_from_current_api(control_number: str) -> Optional[Dict]:
    """Fetch a specific job from the current API by control number"""
    
    if not API_KEY:
        print("Warning: USAJOBS_API_TOKEN not found in environment")
        return None
    
    url = "https://data.usajobs.gov/api/Search"
    headers = {
        "Host": "data.usajobs.gov",
        "Authorization-Key": API_KEY
    }
    
    params = {
        "ControlNumber": control_number,
        "Fields": "full"
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        items = data.get("SearchResult", {}).get("SearchResultItems", [])
        
        if items:
            # Flatten the first result
            return flatten_current_job(items[0])
        else:
            print(f"Job {control_number} not found in current API")
            return None
            
    except Exception as e:
        print(f"Error fetching from API: {e}")
        return None


def compare_scraped_vs_api(scraped_data: Dict, api_data: Dict) -> Dict:
    """Compare scraped data with API data to identify field mappings"""
    
    comparison = {
        "control_number": scraped_data.get("control_number"),
        "has_api_data": api_data is not None,
        "field_mappings": {},
        "scraped_only_fields": [],
        "api_only_fields": [],
        "content_comparison": {}
    }
    
    if not api_data:
        print("No API data available for comparison")
        return comparison
    
    # Compare rationalized fields from scraping with API fields
    scraped_mapping = scraped_data.get("rationalized_fields", {})
    
    # Direct field comparisons
    field_comparisons = {
        "PositionTitle": ("positionTitle", "Title match"),
        "DepartmentName": ("hiringDepartmentName", "Department match"),
        "SubAgency": ("hiringSubelementName", "Sub-agency match"),
        "PositionLocationDisplay": ("locations", "Location match"),
        "QualificationSummary": ("qualificationSummary", "Qualifications"),
        "MajorDuties": ("majorDuties", "Duties"),
        "EducationRequirements": ("education", "Education"),
        "HowToApply": ("howToApply", "Application process")
    }
    
    for scraped_field, (api_field, description) in field_comparisons.items():
        if scraped_field in scraped_mapping and api_field in api_data:
            scraped_value = scraped_mapping[scraped_field]
            api_value = api_data[api_field]
            
            comparison["field_mappings"][description] = {
                "scraped": scraped_value[:200] if isinstance(scraped_value, str) else scraped_value,
                "api": api_value[:200] if isinstance(api_value, str) else api_value,
                "match": str(scraped_value).lower() == str(api_value).lower() if scraped_value and api_value else False
            }
    
    # Check for scraped-only fields
    for field, value in scraped_mapping.items():
        if field not in field_comparisons and value:
            comparison["scraped_only_fields"].append({
                "field": field,
                "value": value[:200] if isinstance(value, str) else value
            })
    
    # Check for API-only fields
    api_fields_to_check = ["applyOnlineUrl", "positionUri", "requirements", "evaluations", 
                          "whatToExpectNext", "requiredDocuments", "supervisoryStatus"]
    
    for field in api_fields_to_check:
        if field in api_data and api_data[field]:
            comparison["api_only_fields"].append({
                "field": field,
                "value": api_data[field][:200] if isinstance(api_data[field], str) else api_data[field]
            })
    
    # Compare extracted sections
    if "structured_sections" in scraped_data:
        sections = scraped_data["structured_sections"]
        comparison["content_comparison"]["sections_found"] = list(sections.keys())
        comparison["content_comparison"]["section_lengths"] = {
            section: len(content) for section, content in sections.items()
        }
    
    # Compare extracted details
    if "job_details" in scraped_data:
        details = scraped_data["job_details"]
        comparison["content_comparison"]["extracted_details"] = details
    
    if "salary_info" in scraped_data:
        comparison["content_comparison"]["salary_info"] = scraped_data["salary_info"]
    
    if "location_info" in scraped_data:
        comparison["content_comparison"]["location_info"] = scraped_data["location_info"]
    
    if "dates" in scraped_data:
        comparison["content_comparison"]["dates"] = scraped_data["dates"]
    
    return comparison


def analyze_scraping_effectiveness(scraped_data: Dict) -> Dict:
    """Analyze how effective the scraping was at extracting structured data"""
    
    analysis = {
        "total_fields_extracted": 0,
        "sections_extracted": 0,
        "has_salary_info": False,
        "has_location_info": False,
        "has_dates": False,
        "has_agency_info": False,
        "has_job_details": False,
        "extraction_quality": {}
    }
    
    # Count rationalized fields
    if "rationalized_fields" in scraped_data:
        analysis["total_fields_extracted"] = len(scraped_data["rationalized_fields"])
    
    # Check sections
    if "structured_sections" in scraped_data:
        sections = scraped_data["structured_sections"]
        analysis["sections_extracted"] = len(sections)
        
        # Evaluate section quality
        for section, content in sections.items():
            word_count = len(content.split()) if content else 0
            analysis["extraction_quality"][section] = {
                "word_count": word_count,
                "quality": "good" if word_count > 50 else "poor" if word_count > 0 else "empty"
            }
    
    # Check specific extractions
    if "salary_info" in scraped_data and scraped_data["salary_info"]:
        analysis["has_salary_info"] = True
    
    if "location_info" in scraped_data and scraped_data["location_info"]:
        analysis["has_location_info"] = True
        
    if "dates" in scraped_data and scraped_data["dates"]:
        analysis["has_dates"] = True
    
    if "agency_info" in scraped_data and scraped_data["agency_info"]:
        analysis["has_agency_info"] = True
    
    if "job_details" in scraped_data and scraped_data["job_details"]:
        analysis["has_job_details"] = True
    
    # Calculate overall effectiveness score
    key_extractions = [
        analysis["has_salary_info"],
        analysis["has_location_info"],
        analysis["has_dates"],
        analysis["has_agency_info"],
        analysis["has_job_details"],
        analysis["sections_extracted"] > 3
    ]
    
    analysis["effectiveness_score"] = sum(key_extractions) / len(key_extractions)
    
    return analysis


def test_job_scraping(control_numbers: List[str]):
    """Test scraping for a list of job control numbers"""
    
    results = []
    
    for control_number in control_numbers:
        print(f"\n{'='*60}")
        print(f"Testing job: {control_number}")
        print(f"{'='*60}")
        
        # Scrape the job
        print(f"\n📄 Scraping job posting...")
        scraped_data = scrape_enhanced_job_posting(control_number, save_html=True)
        
        if scraped_data.get("status") == "error":
            print(f"❌ Scraping failed: {scraped_data.get('error')}")
            results.append({
                "control_number": control_number,
                "status": "scraping_failed",
                "error": scraped_data.get("error")
            })
            continue
        
        # Analyze scraping effectiveness
        print(f"\n🔍 Analyzing scraping effectiveness...")
        scraping_analysis = analyze_scraping_effectiveness(scraped_data)
        
        print(f"  - Total fields extracted: {scraping_analysis['total_fields_extracted']}")
        print(f"  - Sections extracted: {scraping_analysis['sections_extracted']}")
        print(f"  - Effectiveness score: {scraping_analysis['effectiveness_score']:.2%}")
        
        # Fetch from API if available
        print(f"\n🌐 Fetching from current API...")
        api_data = fetch_job_from_current_api(control_number)
        
        # Compare if we have API data
        comparison = None
        if api_data:
            print(f"✅ Found in current API")
            print(f"\n🔄 Comparing scraped vs API data...")
            comparison = compare_scraped_vs_api(scraped_data, api_data)
            
            # Show key findings
            print(f"\n📊 Comparison Results:")
            print(f"  - Field mappings found: {len(comparison['field_mappings'])}")
            print(f"  - Scraped-only fields: {len(comparison['scraped_only_fields'])}")
            print(f"  - API-only fields: {len(comparison['api_only_fields'])}")
        else:
            print(f"⚠️  Not found in current API (may be expired)")
        
        # Store result
        result = {
            "control_number": control_number,
            "status": "success",
            "scraped_data": scraped_data,
            "api_data": api_data,
            "scraping_analysis": scraping_analysis,
            "comparison": comparison,
            "timestamp": datetime.now().isoformat()
        }
        
        results.append(result)
        
        # Save individual result
        output_file = f"test_results_{control_number}.json"
        with open(output_file, "w") as f:
            json.dump(result, f, indent=2)
        print(f"\n💾 Saved detailed results to {output_file}")
    
    return results


def generate_summary_report(results: List[Dict]):
    """Generate a summary report of all test results"""
    
    report = {
        "test_summary": {
            "total_jobs_tested": len(results),
            "successful_scrapes": sum(1 for r in results if r["status"] == "success"),
            "found_in_api": sum(1 for r in results if r.get("api_data") is not None),
            "average_effectiveness": 0
        },
        "field_extraction_summary": {},
        "common_issues": [],
        "recommendations": []
    }
    
    # Calculate average effectiveness
    effectiveness_scores = [r["scraping_analysis"]["effectiveness_score"] 
                          for r in results if "scraping_analysis" in r]
    if effectiveness_scores:
        report["test_summary"]["average_effectiveness"] = sum(effectiveness_scores) / len(effectiveness_scores)
    
    # Summarize field extraction
    all_extracted_fields = {}
    for result in results:
        if "scraped_data" in result and "rationalized_fields" in result["scraped_data"]:
            for field in result["scraped_data"]["rationalized_fields"]:
                all_extracted_fields[field] = all_extracted_fields.get(field, 0) + 1
    
    report["field_extraction_summary"] = {
        field: f"{count}/{len(results)} jobs" 
        for field, count in sorted(all_extracted_fields.items(), key=lambda x: x[1], reverse=True)
    }
    
    # Identify common issues
    if report["test_summary"]["average_effectiveness"] < 0.7:
        report["common_issues"].append("Low overall extraction effectiveness")
    
    # Add recommendations
    report["recommendations"] = [
        "Consider adding more specific selectors for salary information",
        "Improve date extraction patterns",
        "Add fallback methods for section extraction",
        "Consider using job-specific selectors based on agency patterns"
    ]
    
    return report


def main():
    """Main test function"""
    
    # Example job control numbers to test
    test_jobs = [
        "838222500",  # Provided example
        "837949400",  # Provided example
    ]
    
    # You can add more current job numbers here
    # To find current jobs, you could run fetch_current_jobs.py first
    
    print("🚀 Starting scraping comparison tests")
    print(f"📋 Testing {len(test_jobs)} job postings")
    
    # Run tests
    results = test_job_scraping(test_jobs)
    
    # Generate summary report
    print(f"\n\n{'='*60}")
    print("📊 SUMMARY REPORT")
    print(f"{'='*60}")
    
    report = generate_summary_report(results)
    
    print(f"\nTest Summary:")
    for key, value in report["test_summary"].items():
        print(f"  - {key}: {value}")
    
    print(f"\nTop Extracted Fields:")
    for field, count in list(report["field_extraction_summary"].items())[:10]:
        print(f"  - {field}: {count}")
    
    print(f"\nRecommendations:")
    for rec in report["recommendations"]:
        print(f"  - {rec}")
    
    # Save summary report
    with open("test_summary_report.json", "w") as f:
        json.dump(report, f, indent=2)
    print(f"\n💾 Saved summary report to test_summary_report.json")


if __name__ == "__main__":
    main()