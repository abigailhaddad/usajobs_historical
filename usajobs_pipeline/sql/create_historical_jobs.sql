-- Drop table if exists for clean recreation
DROP TABLE IF EXISTS historical_jobs CASCADE;

CREATE TABLE historical_jobs (
    -- Primary identifiers
    control_number BIGINT PRIMARY KEY,
    announcement_number TEXT,
    
    -- Agency and department information
    hiring_agency_code TEXT,
    hiring_agency_name TEXT,
    hiring_department_code TEXT,
    hiring_department_name TEXT,
    hiring_subelement_name TEXT,
    agency_level INTEGER,
    agency_level_sort TEXT,
    
    -- Basic job information
    position_title TEXT,
    minimum_grade TEXT,
    maximum_grade TEXT,
    promotion_potential TEXT,
    
    -- Schedule and type
    appointment_type TEXT,
    work_schedule TEXT,
    service_type TEXT,
    
    -- Compensation
    pay_scale TEXT,
    salary_type TEXT,
    minimum_salary NUMERIC,
    maximum_salary NUMERIC,
    
    -- Job classification
    supervisory_status TEXT,
    job_series TEXT,  -- Comma-separated list of series codes
    
    -- Work conditions
    travel_requirement TEXT,
    telework_eligible TEXT,
    security_clearance_required TEXT,
    security_clearance TEXT,
    drug_test_required TEXT,
    relocation_expenses_reimbursed TEXT,
    
    -- Location information (pipe-separated list)
    locations TEXT,
    
    -- Hiring information
    who_may_apply TEXT,
    hiring_paths TEXT,  -- Comma-separated list
    total_openings TEXT,
    disable_apply_online TEXT,
    
    -- Dates
    position_open_date DATE,
    position_close_date DATE,
    position_expire_date DATE,
    
    -- Position status
    position_opening_status TEXT,
    announcement_closing_type_code TEXT,
    announcement_closing_type_description TEXT,
    
    -- Vendor information
    vendor TEXT,
    
    -- Store complete JSON for any fields we might have missed
    raw JSONB,
    
    -- Timestamps for our own tracking
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for common queries
CREATE INDEX idx_historical_jobs_agency_name ON historical_jobs(hiring_agency_name);
CREATE INDEX idx_historical_jobs_department_name ON historical_jobs(hiring_department_name);
CREATE INDEX idx_historical_jobs_position_open_date ON historical_jobs(position_open_date);
CREATE INDEX idx_historical_jobs_position_close_date ON historical_jobs(position_close_date);
CREATE INDEX idx_historical_jobs_job_series ON historical_jobs(job_series);
CREATE INDEX idx_historical_jobs_telework ON historical_jobs(telework_eligible);
CREATE INDEX idx_historical_jobs_announcement ON historical_jobs(announcement_number);
CREATE INDEX idx_historical_jobs_position_title ON historical_jobs(position_title);
CREATE INDEX idx_historical_jobs_pay_scale ON historical_jobs(pay_scale);
CREATE INDEX idx_historical_jobs_grade ON historical_jobs(minimum_grade, maximum_grade);