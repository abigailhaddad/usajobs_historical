#!/usr/bin/env python3
"""
Compare fields available in historical API vs what we're trying to scrape
This helps identify what fields are NOT in the historical API and need scraping
"""

# Fields available in the Historical API (from historic_pull_parallel.py)
HISTORICAL_API_FIELDS = {
    # Basic identifiers
    "controlNumber",
    "announcementNumber",
    
    # Agency/Organization info
    "hiringAgencyCode",
    "hiringAgencyName",
    "hiringDepartmentCode", 
    "hiringDepartmentName",
    "hiringSubelementName",
    "agencyLevel",
    "agencyLevelSort",
    
    # Position details
    "positionTitle",
    "minimumGrade",
    "maximumGrade",
    "promotionPotential",
    "appointmentType",
    "workSchedule",
    "serviceType",
    
    # Compensation
    "payScale",
    "salaryType",
    "minimumSalary",
    "maximumSalary",
    
    # Work conditions
    "supervisoryStatus",
    "travelRequirement",
    "teleworkEligible",
    "securityClearanceRequired",
    "securityClearance",
    "drugTestRequired",
    "relocationExpensesReimbursed",
    
    # Application info
    "whoMayApply",
    "totalOpenings",
    "disableApplyOnline",
    
    # Dates
    "positionOpenDate",
    "positionCloseDate",
    "positionExpireDate",
    "positionOpeningStatus",
    
    # Other metadata
    "announcementClosingTypeCode",
    "announcementClosingTypeDescription",
    "vendor",
    
    # Composite fields (from arrays)
    "hiringPaths",
    "jobSeries",
    "locations"
}

# Fields the current API has that historical doesn't (from fetch_current_jobs.py)
CURRENT_API_ONLY_FIELDS = {
    "applyOnlineUrl",
    "positionUri",
    "qualificationSummary",
    "majorDuties",
    "education",
    "requirements", 
    "evaluations",
    "howToApply",
    "whatToExpectNext",
    "requiredDocuments"
}

# Fields we're trying to extract via scraping (from scrape_enhanced_job_posting.py)
SCRAPED_SECTIONS = {
    "summary",
    "duties", 
    "qualifications",
    "specialized_experience",
    "education",
    "additional_info",
    "how_to_apply",
    "evaluations",
    "benefits"
}

def analyze_field_coverage():
    """Analyze which fields need scraping vs already available"""
    
    print("HISTORICAL API FIELD ANALYSIS")
    print("=" * 60)
    
    print("\n1. FIELDS ALREADY IN HISTORICAL API:")
    print("-" * 40)
    for field in sorted(HISTORICAL_API_FIELDS):
        print(f"  ✓ {field}")
    
    print(f"\nTotal: {len(HISTORICAL_API_FIELDS)} fields")
    
    print("\n\n2. FIELDS NOT IN HISTORICAL API (need scraping):")
    print("-" * 40)
    
    # Content sections - these are the main value add from scraping
    print("\nContent Sections (text descriptions):")
    for section in sorted(SCRAPED_SECTIONS):
        print(f"  ✗ {section}")
    
    print("\nOther Missing Fields:")
    for field in sorted(CURRENT_API_ONLY_FIELDS):
        if field not in ["applyOnlineUrl", "positionUri"]:  # URLs not useful for historical
            print(f"  ✗ {field}")
    
    print("\n\n3. SCRAPING VALUE PROPOSITION:")
    print("-" * 40)
    print("The historical API provides structured metadata but lacks:")
    print("  1. Job description content (duties, qualifications, etc.)")
    print("  2. Application requirements and process details")
    print("  3. Education and experience requirements text")
    print("  4. Benefits and additional information")
    print("  5. Full text for NLP/analysis")
    
    print("\n\n4. FIELDS TO FOCUS SCRAPING ON:")
    print("-" * 40)
    priority_fields = [
        "majorDuties - What the job entails",
        "qualificationSummary - Required qualifications", 
        "education - Education requirements",
        "howToApply - Application instructions",
        "evaluations - How candidates are evaluated",
        "specialized_experience - Specific experience needed",
        "benefits - Compensation and benefits details",
        "full_text - Complete posting for analysis"
    ]
    
    for field in priority_fields:
        print(f"  • {field}")
    
    print("\n\n5. FIELDS NOT WORTH SCRAPING (already in historical):")
    print("-" * 40)
    overlap_fields = [
        "positionTitle",
        "salary/grade info (minimumSalary, maximumSalary, payScale)",
        "location info (locations field has all locations)",
        "dates (positionOpenDate, positionCloseDate)",
        "agency info (hiringAgencyName, hiringDepartmentName)",
        "work details (workSchedule, travelRequirement, securityClearance)"
    ]
    
    for field in overlap_fields:
        print(f"  ✓ {field}")


if __name__ == "__main__":
    analyze_field_coverage()