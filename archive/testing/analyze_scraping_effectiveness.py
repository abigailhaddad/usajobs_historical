#!/usr/bin/env python3
"""
Analyze what we're currently scraping vs what we actually need
based on historical API gaps
"""

import json
import os

# What we NEED from scraping (not in historical API)
NEEDED_CONTENT_FIELDS = {
    "majorDuties": "The actual job responsibilities and daily tasks",
    "qualificationSummary": "Required qualifications and experience",
    "education": "Specific education requirements",
    "howToApply": "Application instructions and process",
    "evaluations": "How candidates will be evaluated",
    "specializedExperience": "Specific experience requirements",
    "benefits": "Compensation and benefits information",
    "additionalInfo": "Other important information",
    "requirements": "General requirements for the position",
    "requiredDocuments": "Documents needed for application",
    "whatToExpectNext": "Next steps in the hiring process"
}

# What we're WASTING TIME scraping (already in historical API)
REDUNDANT_SCRAPING = {
    "position_title": "positionTitle",
    "salary_info": "minimumSalary, maximumSalary, payScale",
    "location_info": "locations (all locations already provided)",
    "dates": "positionOpenDate, positionCloseDate",
    "agency_info": "hiringAgencyName, hiringDepartmentName",
    "job_series": "jobSeries",
    "work_schedule": "workSchedule", 
    "security_clearance": "securityClearance, securityClearanceRequired",
    "travel": "travelRequirement"
}

def analyze_test_results():
    """Analyze our test scraping results to see what we're getting"""
    
    print("SCRAPING EFFECTIVENESS ANALYSIS")
    print("=" * 70)
    
    # Load test results
    test_files = ["test_results_838222500.json", "test_results_837949400.json"]
    
    for test_file in test_files:
        if not os.path.exists(test_file):
            print(f"\nSkipping {test_file} - not found")
            continue
            
        with open(test_file, 'r') as f:
            data = json.load(f)
        
        control_number = data['control_number']
        scraped = data['scraped_data']
        
        print(f"\n\nJOB: {control_number}")
        print("-" * 70)
        
        # Check what content we successfully extracted
        print("\n1. NEEDED CONTENT (Successfully Extracted):")
        print("-" * 40)
        
        sections = scraped.get('structured_sections', {})
        rationalized = scraped.get('rationalized_fields', {})
        
        extracted_needed = []
        for needed_field, description in NEEDED_CONTENT_FIELDS.items():
            found = False
            
            # Check in structured sections
            if needed_field.lower() in [s.lower() for s in sections.keys()]:
                content = sections.get(needed_field.lower()) or sections.get(needed_field)
                if content and len(content) > 50:
                    extracted_needed.append((needed_field, len(content)))
                    found = True
            
            # Check in rationalized fields
            for key, value in rationalized.items():
                if needed_field.lower() in key.lower() and value:
                    if not found:  # Don't double count
                        extracted_needed.append((needed_field, len(str(value))))
                    found = True
        
        if extracted_needed:
            for field, length in sorted(extracted_needed):
                print(f"  ✓ {field}: {length} chars")
        else:
            print("  ✗ No needed content extracted!")
        
        # Check what redundant data we extracted
        print("\n2. REDUNDANT EXTRACTION (Already in Historical API):")
        print("-" * 40)
        
        redundant_extracted = []
        
        # Check each redundant field type
        if scraped.get('salary_info'):
            redundant_extracted.append("salary_info")
        if scraped.get('location_info'):
            redundant_extracted.append("location_info")
        if scraped.get('dates'):
            redundant_extracted.append("dates")
        if scraped.get('agency_info'):
            redundant_extracted.append("agency_info")
        if scraped.get('job_details'):
            details = scraped['job_details']
            if details.get('job_series'):
                redundant_extracted.append("job_series")
            if details.get('work_schedule'):
                redundant_extracted.append("work_schedule")
            if details.get('security_clearance'):
                redundant_extracted.append("security_clearance")
            if details.get('travel'):
                redundant_extracted.append("travel")
        
        if redundant_extracted:
            for field in redundant_extracted:
                print(f"  ⚠️  {field} -> Already available as: {REDUNDANT_SCRAPING[field]}")
        
        # Check extraction quality
        print("\n3. EXTRACTION QUALITY:")
        print("-" * 40)
        
        total_sections = len(sections)
        quality_sections = len([s for s in sections.values() if len(s) > 100])
        
        print(f"  Total sections extracted: {total_sections}")
        print(f"  High-quality sections (>100 chars): {quality_sections}")
        print(f"  Extraction rate: {quality_sections}/{len(NEEDED_CONTENT_FIELDS)} needed fields")
    
    print("\n\nRECOMMENDATIONS:")
    print("=" * 70)
    print("\n1. STOP extracting these fields (use historical API instead):")
    for field, api_field in REDUNDANT_SCRAPING.items():
        print(f"   - {field} → use {api_field}")
    
    print("\n2. FOCUS scraping on these missing content fields:")
    for field, desc in NEEDED_CONTENT_FIELDS.items():
        print(f"   - {field}: {desc}")
    
    print("\n3. IMPROVE extraction for:")
    print("   - Better section header detection")
    print("   - More robust content extraction after headers")
    print("   - Handle variations in section names")
    print("   - Extract full content, not truncated")


def create_optimized_field_mapping():
    """Create mapping of what to extract vs what to get from API"""
    
    mapping = {
        "from_historical_api": {
            "identifiers": ["controlNumber", "announcementNumber"],
            "organization": ["hiringAgencyName", "hiringDepartmentName", "hiringSubelementName"],
            "position_metadata": ["positionTitle", "minimumGrade", "maximumGrade", "workSchedule"],
            "compensation": ["minimumSalary", "maximumSalary", "payScale"],
            "requirements": ["securityClearance", "travelRequirement", "teleworkEligible"],
            "dates": ["positionOpenDate", "positionCloseDate"],
            "locations": ["locations"]
        },
        "from_scraping": {
            "content_sections": {
                "majorDuties": "Main responsibilities and tasks",
                "qualificationSummary": "Required qualifications", 
                "education": "Education requirements",
                "specializedExperience": "Specific experience needed",
                "howToApply": "Application process",
                "evaluations": "Evaluation criteria",
                "benefits": "Benefits information",
                "additionalInfo": "Other important details"
            },
            "full_text": "Complete posting text for analysis"
        }
    }
    
    print("\n\nOPTIMIZED DATA STRATEGY:")
    print("=" * 70)
    print("\nGet from Historical API:")
    for category, fields in mapping["from_historical_api"].items():
        print(f"\n  {category}:")
        for field in fields:
            print(f"    - {field}")
    
    print("\n\nGet from Scraping:")
    print("\n  Content Sections:")
    for field, desc in mapping["from_scraping"]["content_sections"].items():
        print(f"    - {field}: {desc}")
    print(f"\n  Full Text: {mapping['from_scraping']['full_text']}")
    
    return mapping


if __name__ == "__main__":
    analyze_test_results()
    create_optimized_field_mapping()