import json
import psycopg2
from psycopg2.extras import Json
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def load_historical_jobs_to_db(json_file_path):
    """Load historical jobs from JSON file to database"""
    
    # Load JSON data
    with open(json_file_path, "r") as f:
        jobs = json.load(f)
    
    # Get connection string from environment
    conn_str = os.getenv("DATABASE_URL")
    if not conn_str:
        raise ValueError("DATABASE_URL not found in environment variables. Please set it in your .env file.")
    
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    
    for job in jobs:
        # Extract all fields from the flattened structure
        control_number = job.get("controlNumber")
        announcement_number = job.get("announcementNumber")
        hiring_agency_code = job.get("hiringAgencyCode")
        hiring_agency_name = job.get("hiringAgencyName")
        hiring_department_code = job.get("hiringDepartmentCode")
        hiring_department_name = job.get("hiringDepartmentName")
        hiring_subelement_name = job.get("hiringSubelementName")
        agency_level = job.get("agencyLevel")
        agency_level_sort = job.get("agencyLevelSort")
        position_title = job.get("positionTitle")
        minimum_grade = job.get("minimumGrade")
        maximum_grade = job.get("maximumGrade")
        promotion_potential = job.get("promotionPotential")
        appointment_type = job.get("appointmentType")
        work_schedule = job.get("workSchedule")
        service_type = job.get("serviceType")
        pay_scale = job.get("payScale")
        salary_type = job.get("salaryType")
        minimum_salary = job.get("minimumSalary")
        maximum_salary = job.get("maximumSalary")
        supervisory_status = job.get("supervisoryStatus")
        travel_requirement = job.get("travelRequirement")
        telework_eligible = job.get("teleworkEligible")
        security_clearance_required = job.get("securityClearanceRequired")
        security_clearance = job.get("securityClearance")
        drug_test_required = job.get("drugTestRequired")
        relocation_expenses_reimbursed = job.get("relocationExpensesReimbursed")
        who_may_apply = job.get("whoMayApply")
        total_openings = job.get("totalOpenings")
        disable_apply_online = job.get("disableApplyOnline")
        position_open_date = job.get("positionOpenDate")
        position_close_date = job.get("positionCloseDate")
        position_expire_date = job.get("positionExpireDate")
        position_opening_status = job.get("positionOpeningStatus")
        announcement_closing_type_code = job.get("announcementClosingTypeCode")
        announcement_closing_type_description = job.get("announcementClosingTypeDescription")
        vendor = job.get("vendor")
        job_series = job.get("jobSeries")
        hiring_paths = job.get("hiringPaths")
        locations = job.get("locations")
        
        cur.execute(
            """
            INSERT INTO historical_jobs (
                control_number, announcement_number, hiring_agency_code, hiring_agency_name,
                hiring_department_code, hiring_department_name, hiring_subelement_name,
                agency_level, agency_level_sort, position_title, minimum_grade, maximum_grade,
                promotion_potential, appointment_type, work_schedule, service_type,
                pay_scale, salary_type, minimum_salary, maximum_salary, supervisory_status,
                travel_requirement, telework_eligible, security_clearance_required,
                security_clearance, drug_test_required, relocation_expenses_reimbursed,
                who_may_apply, hiring_paths, total_openings, disable_apply_online,
                position_open_date, position_close_date, position_expire_date,
                position_opening_status, announcement_closing_type_code,
                announcement_closing_type_description, vendor, job_series, locations, raw
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (control_number) DO UPDATE SET
                updated_at = CURRENT_TIMESTAMP,
                position_title = EXCLUDED.position_title,
                position_opening_status = EXCLUDED.position_opening_status,
                raw = EXCLUDED.raw
            """,
            (
                control_number, announcement_number, hiring_agency_code, hiring_agency_name,
                hiring_department_code, hiring_department_name, hiring_subelement_name,
                agency_level, agency_level_sort, position_title, minimum_grade, maximum_grade,
                promotion_potential, appointment_type, work_schedule, service_type,
                pay_scale, salary_type, minimum_salary, maximum_salary, supervisory_status,
                travel_requirement, telework_eligible, security_clearance_required,
                security_clearance, drug_test_required, relocation_expenses_reimbursed,
                who_may_apply, hiring_paths, total_openings, disable_apply_online,
                position_open_date, position_close_date, position_expire_date,
                position_opening_status, announcement_closing_type_code,
                announcement_closing_type_description, vendor, job_series, locations, Json(job)
            ),
        )
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"Successfully loaded {len(jobs)} historical jobs into the database.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Load historical jobs data to database")
    parser.add_argument("json_file", help="Path to JSON file containing historical jobs data")
    args = parser.parse_args()
    
    load_historical_jobs_to_db(args.json_file)