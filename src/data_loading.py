import json
import psycopg2
from psycopg2.extras import Json
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def load_jobs_to_db(json_file_path):
    """Load jobs from JSON file to database"""
    
    # Load your big JSON blob
    with open(json_file_path, "r") as f:
        data = json.load(f)
    
    # Get connection string from environment
    conn_str = os.getenv("DATABASE_URL")
    if not conn_str:
        raise ValueError("DATABASE_URL not found in environment variables. Please set it in your .env file.")
    
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    # Get search metadata
    search_result = data.get("SearchResult", {})
    search_count = search_result.get("SearchResultCount")
    search_count_all = search_result.get("SearchResultCountAll")
    
    for item in search_result.get("SearchResultItems", []):
        job = item["MatchedObjectDescriptor"]
        job_id = item["MatchedObjectId"]
        
        # Basic information
        position_id = job.get("PositionID")
        title = job.get("PositionTitle")
        generated_title = job.get("GeneratedTitle")  # New field from enrichment
        position_uri = job.get("PositionURI")
        apply_uri = job.get("ApplyURI", [])
        
        # Location information
        location_display = job.get("PositionLocationDisplay")
        location_data = job.get("PositionLocation", [{}])[0] if job.get("PositionLocation") else {}
        location_city = location_data.get("CityName")
        location_state = location_data.get("CountrySubDivisionCode")
        location_country = location_data.get("CountryCode")
        location_latitude = location_data.get("Latitude")
        location_longitude = location_data.get("Longitude")
        
        # Organization information
        org = job.get("OrganizationName")
        dept = job.get("DepartmentName")
        sub_agency = job.get("SubAgency")
        
        # Job details from UserArea
        user_area = job.get("UserArea", {})
        details = user_area.get("Details", {})
        
        organization_codes = details.get("OrganizationCodes")
        job_summary = details.get("JobSummary")
        major_duties = details.get("MajorDuties", [])
        qualification_summary = job.get("QualificationSummary")
        education_requirements = details.get("Education")
        general_requirements = details.get("Requirements")
        evaluations = details.get("Evaluations")
        
        # Job classification
        job_category = job.get("JobCategory", [{}])[0] if job.get("JobCategory") else {}
        job_category_name = job_category.get("Name")
        job_category_code = job_category.get("Code")
        
        job_grade = job.get("JobGrade", [{}])[0] if job.get("JobGrade") else {}
        job_grade_code = job_grade.get("Code")
        low_grade = details.get("LowGrade")
        high_grade = details.get("HighGrade")
        promotion_potential = details.get("PromotionPotential")
        
        # Schedule and type
        position_schedule = job.get("PositionSchedule", [{}])[0] if job.get("PositionSchedule") else {}
        position_schedule_code = position_schedule.get("Code")
        position_schedule_name = position_schedule.get("Name")
        
        position_offering = job.get("PositionOfferingType", [{}])[0] if job.get("PositionOfferingType") else {}
        position_offering_type_code = position_offering.get("Code")
        position_offering_type_name = position_offering.get("Name")
        service_type = details.get("ServiceType")
        
        # Compensation
        rem = job.get("PositionRemuneration", [{}])[0] if job.get("PositionRemuneration") else {}
        salary_min = rem.get("MinimumRange")
        salary_max = rem.get("MaximumRange")
        salary_interval = rem.get("RateIntervalCode")
        
        # Dates
        position_start_date = job.get("PositionStartDate")
        position_end_date = job.get("PositionEndDate")
        publication_start_date = job.get("PublicationStartDate")
        application_close_date = job.get("ApplicationCloseDate")
        
        # Application information
        who_may_apply = details.get("WhoMayApply", {})
        who_may_apply_name = who_may_apply.get("Name")
        who_may_apply_code = who_may_apply.get("Code")
        hiring_path = details.get("HiringPath", [])
        total_openings = details.get("TotalOpenings")
        
        # Convert to integer if it's a string
        if total_openings and isinstance(total_openings, str):
            try:
                total_openings = int(total_openings)
            except:
                total_openings = None
        
        # Benefits and requirements
        benefits = details.get("Benefits")
        benefits_url = details.get("BenefitsUrl")
        required_documents = details.get("RequiredDocuments")
        key_requirements = details.get("KeyRequirements", [])
        
        # Work conditions
        travel_code = details.get("TravelCode")
        relocation = details.get("Relocation") == "True"
        telework_eligible = details.get("TeleworkEligible", False)
        remote_indicator = details.get("RemoteIndicator", False)
        security_clearance = details.get("SecurityClearance")
        drug_test_required = details.get("DrugTestRequired") == "True"
        
        # Contact information
        agency_contact_email = details.get("AgencyContactEmail")
        agency_contact_phone = details.get("AgencyContactPhone")
        agency_marketing_statement = details.get("AgencyMarketingStatement")
        
        # Instructions
        how_to_apply = details.get("HowToApply")
        what_to_expect_next = details.get("WhatToExpectNext")
        other_information = details.get("OtherInformation")
        
        # Metadata
        relevance_rank = item.get("RelevanceRank")

        cur.execute(
            """
            INSERT INTO jobs (
                id, position_id, title, generated_title, position_uri, apply_uri,
                location_display, location_city, location_state, location_country, 
                location_latitude, location_longitude,
                organization_name, department_name, sub_agency, organization_codes,
                job_category_name, job_category_code, job_grade_code, 
                low_grade, high_grade, promotion_potential,
                position_schedule_code, position_schedule_name, 
                position_offering_type_code, position_offering_type_name, service_type,
                salary_min, salary_max, salary_interval,
                position_start_date, position_end_date, 
                publication_start_date, application_close_date,
                job_summary, qualification_summary, major_duties, 
                education_requirements, general_requirements, evaluations,
                who_may_apply_name, who_may_apply_code, hiring_path, total_openings,
                benefits, benefits_url, required_documents, key_requirements,
                travel_code, relocation, telework_eligible, remote_indicator, 
                security_clearance, drug_test_required,
                agency_contact_email, agency_contact_phone, agency_marketing_statement,
                how_to_apply, what_to_expect_next, other_information,
                relevance_rank, search_result_count, search_result_count_all,
                raw
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (id) DO UPDATE SET
                updated_at = CURRENT_TIMESTAMP,
                title = EXCLUDED.title,
                generated_title = EXCLUDED.generated_title,
                job_summary = EXCLUDED.job_summary,
                application_close_date = EXCLUDED.application_close_date,
                raw = EXCLUDED.raw
            """,
            (
                job_id, position_id, title, generated_title, position_uri, apply_uri,
                location_display, location_city, location_state, location_country,
                location_latitude, location_longitude,
                org, dept, sub_agency, organization_codes,
                job_category_name, job_category_code, job_grade_code,
                low_grade, high_grade, promotion_potential,
                position_schedule_code, position_schedule_name,
                position_offering_type_code, position_offering_type_name, service_type,
                salary_min, salary_max, salary_interval,
                position_start_date, position_end_date,
                publication_start_date, application_close_date,
                job_summary, qualification_summary, major_duties,
                education_requirements, general_requirements, evaluations,
                who_may_apply_name, who_may_apply_code, hiring_path, total_openings,
                benefits, benefits_url, required_documents, key_requirements,
                travel_code, relocation, telework_eligible, remote_indicator,
                security_clearance, drug_test_required,
                agency_contact_email, agency_contact_phone, agency_marketing_statement,
                how_to_apply, what_to_expect_next, other_information,
                relevance_rank, search_count, search_count_all,
                Json(job)
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    
    print(f"Successfully loaded {len(search_result.get('SearchResultItems', []))} jobs into the database.")