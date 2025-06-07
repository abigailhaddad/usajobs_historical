-- Drop table if exists for clean recreation
DROP TABLE IF EXISTS jobs CASCADE;

CREATE TABLE jobs (
    -- Primary identifiers
    id TEXT PRIMARY KEY,
    position_id TEXT,
    
    -- Basic job information
    title TEXT,
    generated_title TEXT,  -- LLM-generated plain language title
    position_uri TEXT,
    apply_uri TEXT[],
    
    -- Location information
    location_display TEXT,
    location_city TEXT,
    location_state TEXT,
    location_country TEXT,
    location_latitude NUMERIC,
    location_longitude NUMERIC,
    
    -- Organization information
    organization_name TEXT,
    department_name TEXT,
    sub_agency TEXT,
    organization_codes TEXT,
    
    -- Job categories and classification
    job_category_name TEXT,
    job_category_code TEXT,
    job_grade_code TEXT,
    low_grade TEXT,
    high_grade TEXT,
    promotion_potential TEXT,
    
    -- Schedule and offering type
    position_schedule_code TEXT,
    position_schedule_name TEXT,
    position_offering_type_code TEXT,
    position_offering_type_name TEXT,
    service_type TEXT,
    
    -- Compensation
    salary_min NUMERIC,
    salary_max NUMERIC,
    salary_interval TEXT,
    
    -- Dates
    position_start_date TIMESTAMP,
    position_end_date TIMESTAMP,
    publication_start_date TIMESTAMP,
    application_close_date TIMESTAMP,
    
    -- Job details
    job_summary TEXT,
    qualification_summary TEXT,
    major_duties TEXT[],
    education_requirements TEXT,
    general_requirements TEXT,
    evaluations TEXT,
    
    -- Application information
    who_may_apply_name TEXT,
    who_may_apply_code TEXT,
    hiring_path TEXT[],
    total_openings INTEGER,
    
    -- Benefits and requirements
    benefits TEXT,
    benefits_url TEXT,
    required_documents TEXT,
    key_requirements TEXT[],
    
    -- Work conditions
    travel_code TEXT,
    relocation BOOLEAN,
    telework_eligible BOOLEAN,
    remote_indicator BOOLEAN,
    security_clearance TEXT,
    drug_test_required BOOLEAN,
    
    -- Contact information
    agency_contact_email TEXT,
    agency_contact_phone TEXT,
    agency_marketing_statement TEXT,
    
    -- Instructions
    how_to_apply TEXT,
    what_to_expect_next TEXT,
    other_information TEXT,
    
    -- Metadata
    relevance_rank INTEGER,
    search_result_count INTEGER,
    search_result_count_all INTEGER,
    
    -- Store complete JSON for any fields we might have missed
    raw JSONB,
    
    -- Timestamps for our own tracking
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for common queries
CREATE INDEX idx_jobs_organization ON jobs(organization_name);
CREATE INDEX idx_jobs_department ON jobs(department_name);
CREATE INDEX idx_jobs_location_state ON jobs(location_state);
CREATE INDEX idx_jobs_salary ON jobs(salary_min, salary_max);
CREATE INDEX idx_jobs_dates ON jobs(application_close_date);
CREATE INDEX idx_jobs_job_category ON jobs(job_category_code);
CREATE INDEX idx_jobs_telework ON jobs(telework_eligible);
CREATE INDEX idx_jobs_remote ON jobs(remote_indicator);

