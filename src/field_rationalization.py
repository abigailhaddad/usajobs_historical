#!/usr/bin/env python3
"""
Field Rationalization Engine

Combines and rationalizes fields from:
1. Historical USAJobs API
2. Current USAJobs Search API  
3. Scraped job content

Creates unified job records with the best data from each source.

Usage:
    python field_rationalization.py --output unified_jobs.json
"""

import json
import os
import argparse
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path

class FieldRationalizer:
    """Handles field mapping and rationalization between data sources"""
    
    def __init__(self):
        self.field_mappings = self._create_field_mappings()
        self.unified_schema = self._create_unified_schema()
    
    def _create_field_mappings(self) -> Dict[str, Dict[str, str]]:
        """Define field mappings between data sources"""
        return {
            'historical_to_unified': {
                # Historical API uses camelCase field names
                'usajobsControlNumber': 'control_number',
                'positionTitle': 'position_title',
                'hiringAgencyName': 'agency_name',
                'hiringDepartmentName': 'department_name',
                'hiringSubelementName': 'sub_agency',
                'jobSeries': 'job_series',
                'minimumGrade': 'min_grade',
                'maximumGrade': 'max_grade',
                'minimumSalary': 'min_salary',
                'maximumSalary': 'max_salary',
                'positionOpenDate': 'open_date',
                'positionCloseDate': 'close_date',
                'locations': 'locations',
                'workSchedule': 'work_schedule',
                'teleworkEligible': 'telework_eligible',
                'securityClearanceRequired': 'security_clearance_required',
                'whoMayApply': 'hiring_path',
                # Add the fields from the database columns that are missing
                'pay_scale': 'pay_scale',
                'promotion_potential': 'promotion_potential', 
                'total_openings': 'total_openings',
                'positionOpeningStatus': 'position_opening_status'
            },
            'current_to_unified': {
                # Raw current API field names (MatchedObjectDescriptor)
                'MatchedObjectId': 'control_number',
                'PositionTitle': 'position_title', 
                'DepartmentName': 'department_name',
                'OrganizationName': 'agency_name',
                'SubAgency': 'sub_agency',
                'PositionStartDate': 'open_date',
                'PositionEndDate': 'close_date',
                'ApplicationCloseDate': 'close_date',
                'PositionLocationDisplay': 'locations',
                'QualificationSummary': 'qualification_summary',
                'ApplyURI': 'apply_url',
                'PositionURI': 'position_uri',
                
                # Flattened field names (for backwards compatibility)
                'controlNumber': 'control_number',
                'positionTitle': 'position_title',
                'hiringAgencyName': 'agency_name',
                'hiringDepartmentName': 'department_name',
                'hiringSubelementName': 'sub_agency',
                'jobSeries': 'job_series',
                'minimumGrade': 'min_grade',
                'maximumGrade': 'max_grade',
                'minimumSalary': 'min_salary',
                'maximumSalary': 'max_salary',
                'positionOpenDate': 'open_date',
                'positionCloseDate': 'close_date',
                'workSchedule': 'work_schedule',
                'teleworkEligible': 'telework_eligible',
                'securityClearanceRequired': 'security_clearance_required',
                'whoMayApply': 'hiring_path',
                'qualificationSummary': 'qualification_summary',
                'majorDuties': 'major_duties',
                'requirements': 'requirements',
                'howToApply': 'how_to_apply',
                'applyOnlineUrl': 'apply_url',
                'positionOpeningStatus': 'position_opening_status'
            },
            'scraped_to_unified': {
                'control_number': 'control_number',
                'content_sections': 'scraped_sections',
                'full_text': 'full_content',
                'extraction_stats': 'extraction_metadata'
            }
        }
    
    def _create_unified_schema(self) -> Dict[str, str]:
        """Define the unified schema with field types"""
        return {
            # Identifiers
            'control_number': 'str',
            
            # Basic Info
            'position_title': 'str',
            'agency_name': 'str',
            'department_name': 'str', 
            'sub_agency': 'str',
            
            # Classification
            'job_series': 'str',
            'min_grade': 'str',
            'max_grade': 'str',
            'pay_scale': 'str',
            
            # Compensation
            'min_salary': 'float',
            'max_salary': 'float',
            'salary_text': 'str',
            
            # Dates
            'open_date': 'date',
            'close_date': 'date',
            
            # Location
            'locations': 'str',
            
            # Work Details
            'work_schedule': 'str',
            'telework_eligible': 'str',
            'security_clearance_required': 'str',
            'hiring_path': 'str',
            'position_opening_status': 'str',
            
            # Content (Current API fields)
            'qualification_summary': 'text',
            'major_duties': 'text',
            'requirements': 'text',
            'how_to_apply': 'text',
            'apply_url': 'str',
            'position_uri': 'str',
            
            # Content (Additional fields from Current API UserArea.Details)
            'evaluations': 'text',
            'benefits': 'text',
            'other_information': 'text',
            'required_documents': 'text',
            'what_to_expect_next': 'text',
            'education': 'text',
            
            # Job Posting Details (Current API specific)
            'job_summary': 'text',
            'total_openings': 'str',
            'promotion_potential': 'str', 
            'relocation_assistance': 'str',
            
            # Content (Scraped fields)
            'scraped_sections': 'json',
            'full_content': 'text',
            'extraction_metadata': 'json',
            
            # Metadata
            'data_sources': 'json',
            'rationalization_date': 'timestamp'
        }
    
    def rationalize_job_record(self, historical_data: Optional[Dict] = None, 
                              current_data: Optional[Dict] = None,
                              scraped_data: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Combine data from multiple sources into unified record
        
        DEDUPLICATION STRATEGY:
        - If control_number exists in both historical and current: PRIORITIZE CURRENT API
        - Current API data overwrites historical for all overlapping fields
        - Historical API fills in metadata gaps not available in current API
        - Scraped data supplements both sources
        """
        
        import logging
        logger = logging.getLogger()
        
        # Log what data sources we have
        control_num = (historical_data or {}).get('usajobsControlNumber') or \
                     (current_data or {}).get('MatchedObjectId') or \
                     (current_data or {}).get('controlNumber') or 'unknown'
        
        logger.debug(f"Rationalizing job {control_num} - Historical: {bool(historical_data)}, Current: {bool(current_data)}, Scraped: {bool(scraped_data)}")
        if scraped_data:
            logger.debug(f"  Scraped data keys: {list(scraped_data.keys())}")
            if 'content_sections' in scraped_data:
                logger.debug(f"  Content sections: {list(scraped_data['content_sections'].keys()) if isinstance(scraped_data['content_sections'], dict) else 'not a dict'}")
        
        unified_record = {}
        data_sources = []
        dedup_strategy = "none"
        
        # Determine deduplication strategy  
        hist_control = str(historical_data.get('usajobsControlNumber', '')) if historical_data else ''
        curr_control = str(current_data.get('MatchedObjectId', current_data.get('controlNumber', ''))) if current_data else ''
        
        if hist_control and curr_control and hist_control == curr_control:
            dedup_strategy = "current_priority"
        
        # Process based on deduplication strategy
        if dedup_strategy == "current_priority":
            # DUPLICATE FOUND: Prioritize current API, supplement with historical metadata
            print(f"  🔄 Dedup: Control {curr_control} found in both sources - prioritizing current API")
            
            # Start with current API data (highest priority)
            curr_mapped = self._map_fields(current_data, 'current_to_unified')
            unified_record.update(curr_mapped)
            data_sources.append('current_api_priority')
            
            # Add historical metadata that current API doesn't have
            hist_mapped = self._map_fields(historical_data, 'historical_to_unified')
            historical_only_fields = [
                'pay_scale', 'promotion_potential', 'total_openings'
            ]
            
            for field, value in hist_mapped.items():
                if field in historical_only_fields and field not in unified_record:
                    unified_record[field] = value
            
            data_sources.append('historical_metadata_supplement')
            
        else:
            # NO DUPLICATES: Standard processing
            
            # Process historical data first
            if historical_data:
                hist_mapped = self._map_fields(historical_data, 'historical_to_unified')
                unified_record.update(hist_mapped)
                data_sources.append('historical_api')
            
            # Process current API data (overwrites historical where current is more detailed)
            if current_data:
                curr_mapped = self._map_fields(current_data, 'current_to_unified')
                
                # Prioritize current API for content fields and core info
                current_priority_fields = [
                    'qualification_summary', 'major_duties', 'requirements', 
                    'how_to_apply', 'apply_url', 'position_title', 'agency_name',
                    'open_date', 'close_date', 'locations', 'min_salary', 'max_salary'
                ]
                
                for field, value in curr_mapped.items():
                    if field in current_priority_fields or field not in unified_record:
                        unified_record[field] = value
                    elif value and not unified_record.get(field):
                        unified_record[field] = value
                
                data_sources.append('current_api')
        
        # Process scraped data (fills gaps and adds rich content)
        if scraped_data:
            import logging
            logger = logging.getLogger()
            
            scraped_mapped = self._map_fields(scraped_data, 'scraped_to_unified')
            
            # Extract content from structured sections
            scraped_content = self._extract_scraped_content(scraped_data)
            scraped_mapped.update(scraped_content)
            
            # Log what we found in scraped content
            control_num = unified_record.get('control_number', 'unknown')
            logger.debug(f"Job {control_num} - Scraped content fields: {list(scraped_content.keys())}")
            
            # Add scraped data where no API data exists
            for field, value in scraped_mapped.items():
                if field not in unified_record or not unified_record[field]:
                    unified_record[field] = value
                    logger.debug(f"Job {control_num} - Added scraped {field}: {len(str(value))} chars")
                else:
                    # Log when we're NOT using scraped content
                    existing_len = len(str(unified_record[field]))
                    scraped_len = len(str(value))
                    logger.debug(f"Job {control_num} - Kept existing {field} ({existing_len} chars) over scraped ({scraped_len} chars)")
            
            data_sources.append('scraping')
        
        # Add metadata
        unified_record['data_sources'] = data_sources
        unified_record['rationalization_date'] = datetime.now().isoformat()
        
        # Data quality enhancements
        unified_record = self._enhance_data_quality(unified_record)
        
        # Ensure JSON fields are properly serialized before database insertion
        unified_record = self._serialize_json_fields(unified_record)
        
        return unified_record
    
    def _extract_scraped_content(self, scraped_data: Dict[str, Any]) -> Dict[str, str]:
        """Extract specific content fields from scraped content sections"""
        import logging
        logger = logging.getLogger()
        
        content_fields = {}
        
        # Get content sections from new scraper format
        sections = scraped_data.get('content_sections', {})
        if isinstance(sections, str):
            try:
                sections = json.loads(sections)
            except:
                sections = {}
        
        # Map scraped sections to unified fields (using new Current API field names)
        section_mappings = {
            'Summary': 'job_summary',
            'MajorDuties': 'major_duties',
            'QualificationSummary': 'qualification_summary', 
            'Requirements': 'requirements',
            'Education': 'education',
            'HowToApply': 'how_to_apply',
            'Evaluations': 'evaluations',
            'Benefits': 'benefits',
            'OtherInformation': 'other_information',
            'RequiredDocuments': 'required_documents',
            'WhatToExpectNext': 'what_to_expect_next'
        }
        
        for scraped_key, unified_field in section_mappings.items():
            if scraped_key in sections and sections[scraped_key]:
                content = sections[scraped_key].strip()
                if content and len(content) > 20:  # Substantial content only
                    if unified_field == 'requirements' and unified_field in content_fields:
                        # Append education to existing requirements
                        content_fields[unified_field] += f"\n\nEducation Requirements:\n{content}"
                    else:
                        content_fields[unified_field] = content
                    logger.debug(f"Extracted {unified_field} from scraped {scraped_key}: {len(content)} chars")
                else:
                    logger.debug(f"Skipped {unified_field} from scraped {scraped_key}: only {len(content)} chars")
        
        # Also try to extract from full text if sections are missing
        full_text = scraped_data.get('full_text', '')
        if full_text and not content_fields.get('major_duties'):
            # Try to extract duties from full text using patterns
            import re
            duties_match = re.search(r'(?:Major Duties|Duties|Responsibilities)[:\s]*\n(.*?)(?:\n(?:Qualifications|Requirements|Education)|$)', 
                                   full_text, re.DOTALL | re.IGNORECASE)
            if duties_match:
                duties_text = duties_match.group(1).strip()
                if len(duties_text) > 50:
                    content_fields['major_duties'] = duties_text[:2000]  # Limit length
        
        return content_fields
    
    def _map_fields(self, source_data: Dict, mapping_key: str) -> Dict[str, Any]:
        """Map fields from source to unified schema"""
        mapping = self.field_mappings[mapping_key]
        mapped_data = {}
        
        # Standard field mapping
        for source_field, unified_field in mapping.items():
            if source_field in source_data and source_data[source_field] is not None:
                value = source_data[source_field]
                
                # Type conversion based on unified schema
                schema_type = self.unified_schema.get(unified_field, 'str')
                converted_value = self._convert_type(value, schema_type)
                
                if converted_value is not None:
                    mapped_data[unified_field] = converted_value
        
        # Special handling for historical API raw_data extraction
        # Only extract fields that have 0% coverage in historical data
        if mapping_key == 'historical_to_unified' and 'raw_data' in source_data:
            try:
                import json
                raw_data = json.loads(source_data['raw_data']) if isinstance(source_data['raw_data'], str) else source_data['raw_data']
                
                # Only extract metadata fields that historical API has but current doesn't
                # DO NOT extract content fields - those come from scraping
                
                if 'payScale' in raw_data and raw_data['payScale']:
                    mapped_data['pay_scale'] = raw_data['payScale']
                
                if 'promotionPotential' in raw_data and raw_data['promotionPotential']:
                    mapped_data['promotion_potential'] = raw_data['promotionPotential']
                
                if 'totalOpenings' in raw_data and raw_data['totalOpenings']:
                    mapped_data['total_openings'] = str(raw_data['totalOpenings'])
                
                # Telework - convert Y/N to Yes/No
                if 'teleworkEligible' in raw_data:
                    mapped_data['telework_eligible'] = 'Yes' if raw_data['teleworkEligible'] == 'Y' else 'No'
                    
            except (json.JSONDecodeError, TypeError):
                pass  # Skip if raw_data parsing fails
        
        # Special handling for nested historical API fields
        if mapping_key == 'historical_to_unified':
            # Extract job series from JobCategories array
            if 'JobCategories' in source_data and source_data['JobCategories'] is not None:
                job_categories = source_data['JobCategories']
                
                # Handle numpy arrays
                if hasattr(job_categories, 'tolist'):
                    job_categories = job_categories.tolist()
                
                if isinstance(job_categories, list) and job_categories:
                    job_series = job_categories[0].get('series', '')
                    if job_series:
                        mapped_data['job_series'] = job_series
            
            # Extract locations from PositionLocations array
            if 'PositionLocations' in source_data and source_data['PositionLocations'] is not None:
                position_locations = source_data['PositionLocations']
                
                # Handle numpy arrays
                if hasattr(position_locations, 'tolist'):
                    position_locations = position_locations.tolist()
                
                if isinstance(position_locations, list) and position_locations:
                    location_names = []
                    for loc in position_locations:
                        if isinstance(loc, dict):
                            city = loc.get('positionLocationCity', '')
                            state = loc.get('positionLocationState', '')
                            if city and state and state != city:
                                location_names.append(f"{city}, {state}")
                            elif city:
                                location_names.append(city)
                    
                    if location_names:
                        # Limit to first 5 locations
                        mapped_data['locations'] = ', '.join(location_names[:5])
                        if len(location_names) > 5:
                            mapped_data['locations'] += f' (and {len(location_names) - 5} more)'
            
            # Extract hiring paths from HiringPaths array
            if 'HiringPaths' in source_data and source_data['HiringPaths'] is not None:
                hiring_paths = source_data['HiringPaths']
                
                # Handle numpy arrays
                if hasattr(hiring_paths, 'tolist'):
                    hiring_paths = hiring_paths.tolist()
                
                if isinstance(hiring_paths, list) and hiring_paths:
                    path_names = []
                    for path in hiring_paths:
                        if isinstance(path, dict) and 'hiringPath' in path:
                            path_names.append(path['hiringPath'])
                    
                    if path_names:
                        mapped_data['hiring_path'] = ', '.join(path_names)
        
        # Special handling for nested current API fields
        if mapping_key == 'current_to_unified':
            # Extract job series from JobCategory array
            if 'JobCategory' in source_data and source_data['JobCategory'] is not None:
                job_category_data = source_data['JobCategory']
                
                # Handle numpy arrays
                if hasattr(job_category_data, 'tolist'):
                    job_category_data = job_category_data.tolist()
                
                if isinstance(job_category_data, list) and job_category_data:
                    job_series = job_category_data[0].get('Code', '')
                    if job_series:
                        mapped_data['job_series'] = job_series
            
            # Extract locations from PositionLocation array
            if 'PositionLocation' in source_data and source_data['PositionLocation'] is not None:
                location_data = source_data['PositionLocation']
                
                # Handle numpy arrays
                if hasattr(location_data, 'tolist'):
                    location_data = location_data.tolist()
                
                if isinstance(location_data, list) and location_data:
                    # Extract city/state names from location array
                    location_names = []
                    for loc in location_data:
                        if isinstance(loc, dict):
                            if 'LocationName' in loc:
                                location_names.append(loc['LocationName'])
                            elif 'CityName' in loc:
                                city = loc['CityName']
                                state = loc.get('CountrySubDivisionCode', '')
                                if state and state != city:
                                    location_names.append(f"{city}, {state}")
                                else:
                                    location_names.append(city)
                    
                    if location_names:
                        # Limit to first 5 locations to avoid overly long strings
                        mapped_data['locations'] = ', '.join(location_names[:5])
                        if len(location_names) > 5:
                            mapped_data['locations'] += f' (and {len(location_names) - 5} more)'
            
            # Extract work schedule from PositionSchedule array
            if 'PositionSchedule' in source_data and source_data['PositionSchedule'] is not None:
                schedule_data = source_data['PositionSchedule']
                
                # Handle numpy arrays
                if hasattr(schedule_data, 'tolist'):
                    schedule_data = schedule_data.tolist()
                
                if isinstance(schedule_data, list) and schedule_data:
                    schedule_info = schedule_data[0]
                    if isinstance(schedule_info, dict):
                        schedule_code = schedule_info.get('Code', '')
                        schedule_name = schedule_info.get('Name', '')
                        # Map codes to meaningful values
                        schedule_map = {
                            '1': 'Full-Time',
                            '2': 'Part-Time', 
                            '3': 'Shift Work',
                            '4': 'Intermittent',
                            '5': 'Job Sharing',
                            '6': 'Multiple Schedules'
                        }
                        mapped_data['work_schedule'] = schedule_map.get(schedule_code, schedule_name or schedule_code)
            
            
            # Extract salary range from PositionRemuneration array
            if 'PositionRemuneration' in source_data and source_data['PositionRemuneration'] is not None:
                remun_data = source_data['PositionRemuneration']
                
                # Handle numpy arrays
                if hasattr(remun_data, 'tolist'):
                    remun_data = remun_data.tolist()
                
                if isinstance(remun_data, list) and remun_data:
                    remun = remun_data[0]
                    if isinstance(remun, dict):
                        if 'MinimumRange' in remun:
                            try:
                                mapped_data['min_salary'] = float(remun['MinimumRange'])
                            except (ValueError, TypeError):
                                pass
                        if 'MaximumRange' in remun:
                            try:
                                mapped_data['max_salary'] = float(remun['MaximumRange'])
                            except (ValueError, TypeError):
                                pass
                        # Also capture the salary text description
                        if 'Description' in remun:
                            mapped_data['salary_text'] = f"{remun.get('MinimumRange', '')} - {remun.get('MaximumRange', '')} {remun.get('Description', '')}"
            
            # Fallback to other salary fields
            elif 'OfferRemunerationMinimumAmount' in source_data:
                mapped_data['min_salary'] = source_data['OfferRemunerationMinimumAmount']
            elif 'RemunMin' in source_data:
                mapped_data['min_salary'] = source_data['RemunMin']
                
            if 'OfferRemunerationMaximumAmount' in source_data:
                mapped_data['max_salary'] = source_data['OfferRemunerationMaximumAmount']
            elif 'RemunMax' in source_data:
                mapped_data['max_salary'] = source_data['RemunMax']
            
            # Extract grades from JobGrade array
            if 'JobGrade' in source_data and source_data['JobGrade'] is not None:
                job_grade_data = source_data['JobGrade']
                
                # Handle numpy arrays
                if hasattr(job_grade_data, 'tolist'):
                    job_grade_data = job_grade_data.tolist()
                
                if isinstance(job_grade_data, list) and job_grade_data:
                    grade_info = job_grade_data[0]
                    if isinstance(grade_info, dict):
                        if 'Code' in grade_info:
                            mapped_data['pay_scale'] = grade_info['Code']
            
            # Try to get grade info from other fields
            if 'LowGrade' in source_data:
                mapped_data['min_grade'] = source_data['LowGrade']
            if 'HighGrade' in source_data:
                mapped_data['max_grade'] = source_data['HighGrade']
            
            # Use QualificationSummary as fallback for requirements if Requirements is empty
            if 'requirements' not in mapped_data or not mapped_data.get('requirements'):
                if 'QualificationSummary' in source_data and source_data['QualificationSummary']:
                    # Only use if it's substantial content (more than just basic qualifications)
                    qual_summary = source_data['QualificationSummary'].strip()
                    if len(qual_summary) > 100:  # Substantial content
                        mapped_data['requirements'] = qual_summary
        
        # Special handling for current API UserArea.Details extraction
        if mapping_key == 'current_to_unified' and 'UserArea' in source_data:
            details = source_data.get('UserArea', {}).get('Details', {})
            
            # Extract additional current API fields from UserArea.Details
            job_posting_fields = {
                'JobSummary': 'job_summary',
                'TotalOpenings': 'total_openings', 
                'PromotionPotential': 'promotion_potential',
                'SecurityClearance': 'security_clearance_required',
                'TeleworkEligible': 'telework_eligible',
                'Relocation': 'relocation_assistance',
                'Benefits': 'benefits',
                'OtherInformation': 'other_information',
                'Evaluations': 'evaluations',
                'HowToApply': 'how_to_apply',
                'WhatToExpectNext': 'what_to_expect_next',
                'RequiredDocuments': 'required_documents',
                'Education': 'education',
                'Requirements': 'requirements',
                'MajorDuties': 'major_duties',
                'LowGrade': 'min_grade',
                'HighGrade': 'max_grade'
            }
            
            for api_field, unified_field in job_posting_fields.items():
                if api_field in details and details[api_field] is not None:
                    value = details[api_field]
                    # Handle numpy arrays
                    if hasattr(value, 'tolist'):
                        value = value.tolist()
                    # Handle different data types
                    if isinstance(value, bool):
                        mapped_data[unified_field] = 'Yes' if value else 'No'
                    elif isinstance(value, list) and api_field == 'MajorDuties':
                        # MajorDuties is often a list, join with newlines
                        mapped_data[unified_field] = '\n\n'.join(str(item) for item in value)
                    else:
                        cleaned_value = str(value).strip()
                        if cleaned_value:  # Only store non-empty values
                            mapped_data[unified_field] = cleaned_value
            
            # Extract HiringPath from UserArea.Details (better source than WhoMayApply)
            if 'HiringPath' in details and details['HiringPath'] is not None:
                hiring_paths = details['HiringPath']
                # Handle numpy arrays and lists
                if hasattr(hiring_paths, 'tolist'):
                    hiring_paths = hiring_paths.tolist()
                if isinstance(hiring_paths, list):
                    mapped_data['hiring_path'] = ', '.join(str(p) for p in hiring_paths)
                else:
                    mapped_data['hiring_path'] = str(hiring_paths)
            
            # Extract rich content fields that match our unified schema
            userarea_mappings = {
                'MajorDuties': 'major_duties',
                'Requirements': 'requirements', 
                'HowToApply': 'how_to_apply',
                'Evaluations': 'evaluations',
                'Benefits': 'benefits',
                'RequiredDocuments': 'required_documents',
                'WhatToExpectNext': 'what_to_expect_next',
                'Education': 'education',
                'OtherInformation': 'other_information'
            }
            
            for detail_field, unified_field in userarea_mappings.items():
                if detail_field in details and details[detail_field] is not None:
                    value = details[detail_field]
                    # Handle numpy arrays
                    if hasattr(value, 'tolist'):
                        value = value.tolist()
                    if isinstance(value, list):
                        value = ' '.join(str(v) for v in value)
                    mapped_data[unified_field] = str(value)
        
        return mapped_data
    
    def _convert_type(self, value: Any, target_type: str) -> Any:
        """Convert value to target type"""
        if value is None or value == '':
            return None
        
        try:
            if target_type == 'str':
                return str(value)
            elif target_type == 'float':
                # Handle salary strings like "$50,000"
                if isinstance(value, str):
                    cleaned = value.replace('$', '').replace(',', '')
                    return float(cleaned) if cleaned.replace('.', '').isdigit() else None
                return float(value)
            elif target_type == 'int':
                return int(value)
            elif target_type == 'json':
                # Ensure JSON fields are properly serialized
                if isinstance(value, (dict, list)):
                    import json
                    return json.dumps(value)
                elif isinstance(value, str):
                    return value  # Already a JSON string
                else:
                    import json
                    return json.dumps(value)
            elif target_type == 'text':
                return value  # Keep as-is
            elif target_type == 'date':
                # Handle various date formats
                if isinstance(value, str):
                    # Try to parse common date formats
                    import re
                    from datetime import datetime
                    
                    # ISO format
                    if re.match(r'\d{4}-\d{2}-\d{2}', value):
                        return value[:10]  # Return just date part
                    
                    # MM/DD/YYYY or MM-DD-YYYY
                    if re.match(r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}', value):
                        return value
                
                return str(value)
            elif target_type == 'timestamp':
                return value
            else:
                return value
        except (ValueError, TypeError):
            return None
    
    def _enhance_data_quality(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance data quality through validation and standardization"""
        
        # Standardize job series
        if record.get('job_series'):
            # Extract 4-digit series from strings like "0101" or "GS-0101"
            import re
            series_match = re.search(r'(\d{4})', str(record['job_series']))
            if series_match:
                record['job_series'] = series_match.group(1)
        
        # Standardize grade information
        if record.get('min_grade') or record.get('max_grade'):
            for grade_field in ['min_grade', 'max_grade']:
                if record.get(grade_field):
                    # Extract numeric grade from "GS-12" or "12"
                    import re
                    grade_match = re.search(r'(\d{1,2})', str(record[grade_field]))
                    if grade_match:
                        record[grade_field] = grade_match.group(1)
        
        # Validate and clean salary data
        if record.get('min_salary') and record.get('max_salary'):
            try:
                min_sal = float(record['min_salary'])
                max_sal = float(record['max_salary'])
                
                # Swap if min > max
                if min_sal > max_sal:
                    record['min_salary'], record['max_salary'] = max_sal, min_sal
            except (ValueError, TypeError):
                pass
        
        # Standardize location format
        if record.get('locations'):
            # Clean up location strings
            location = str(record['locations']).strip()
            # Remove multiple spaces and normalize separators
            import re
            location = re.sub(r'\s+', ' ', location)
            location = re.sub(r'\s*\|\s*', ' | ', location)
            record['locations'] = location
        
        return record
    
    def _serialize_json_fields(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure all JSON fields are properly serialized as strings"""
        import json
        
        # Get fields that should be JSON from the schema
        json_fields = [field for field, field_type in self.unified_schema.items() if field_type == 'json']
        
        for field in json_fields:
            if field in record and record[field] is not None:
                value = record[field]
                # Only serialize if it's not already a JSON string
                if isinstance(value, (dict, list)):
                    record[field] = json.dumps(value)
                elif not isinstance(value, str):
                    # Convert other types to JSON as well
                    record[field] = json.dumps(value)
        
        return record

def load_historical_data(db_path: str, control_numbers: List[str] = None) -> List[Dict]:
    """Load historical data from DuckDB"""
    if not os.path.exists(db_path):
        return []
    
    try:
        conn = duckdb.connect(db_path)
        
        if control_numbers:
            placeholders = ','.join(['?' for _ in control_numbers])
            query = f"SELECT * FROM historical_jobs WHERE control_number IN ({placeholders})"
            results = conn.execute(query, control_numbers).fetchall()
        else:
            results = conn.execute("SELECT * FROM historical_jobs").fetchall()
        
        columns = [desc[0] for desc in conn.description]
        
        # Also load scraped data if available
        historical_data = [dict(zip(columns, row)) for row in results]
        scraped_data = load_scraped_data(conn, [str(record['control_number']) for record in historical_data])
        
        # Merge scraped data with historical data
        for hist_record in historical_data:
            control_num = str(hist_record['control_number'])
            if control_num in scraped_data:
                hist_record['scraped_content'] = scraped_data[control_num]
        
        conn.close()
        return historical_data
        
    except Exception as e:
        print(f"Error loading historical data: {e}")
        return []

def load_scraped_data(conn, control_numbers: List[str]) -> Dict[str, Dict]:
    """Load scraped data for given control numbers"""
    if not control_numbers:
        return {}
    
    try:
        placeholders = ','.join(['?' for _ in control_numbers])
        query = f"""
            SELECT control_number, scraped_content 
            FROM scraped_jobs 
            WHERE control_number IN ({placeholders}) 
            AND scraping_success = true 
            AND scraped_content IS NOT NULL
        """
        results = conn.execute(query, control_numbers).fetchall()
        
        scraped_lookup = {}
        for control_num, content_json in results:
            try:
                scraped_lookup[str(control_num)] = json.loads(content_json)
            except:
                continue
                
        return scraped_lookup
        
    except Exception as e:
        print(f"Error loading scraped data: {e}")
        return {}

def load_current_data(json_path: str, min_date: str = None) -> List[Dict]:
    """Load current API data from JSON with optional date filtering"""
    if not os.path.exists(json_path):
        return []
    
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        # Handle both raw API structure and flattened data
        if 'SearchResult' in data and 'SearchResultItems' in data['SearchResult']:
            # Raw API structure - extract MatchedObjectDescriptor
            raw_jobs = data['SearchResult']['SearchResultItems']
            jobs = []
            for item in raw_jobs:
                if 'MatchedObjectDescriptor' in item:
                    job_data = item['MatchedObjectDescriptor'].copy()
                    # Add the numeric MatchedObjectId as the control_number
                    if 'MatchedObjectId' in item:
                        job_data['MatchedObjectId'] = item['MatchedObjectId']
                    jobs.append(job_data)
                else:
                    jobs.append(item)
        else:
            jobs = [data] if isinstance(data, dict) else data
        
        # Filter by date if specified
        if min_date and jobs:
            print(f"   📅 Filtering current jobs to only include those posted >= {min_date}")
            filtered_jobs = []
            
            for job in jobs:
                job_date = None
                
                # Try to get publication/start date
                if 'PublicationStartDate' in job:
                    job_date = job['PublicationStartDate'][:10]  # Get just date part
                elif 'PositionStartDate' in job:
                    job_date = job['PositionStartDate'][:10]
                
                # Include job if date is >= min_date
                if job_date and job_date >= min_date:
                    filtered_jobs.append(job)
                elif not job_date:
                    # Include jobs without dates (better safe than sorry)
                    filtered_jobs.append(job)
            
            print(f"   📊 Filtered {len(jobs)} → {len(filtered_jobs)} jobs (removed {len(jobs) - len(filtered_jobs)} pre-{min_date})")
            return filtered_jobs
        
        return jobs
            
    except Exception as e:
        print(f"Error loading current data: {e}")
        return []

def save_to_duckdb(records: List[Dict], output_path: str, schema: Dict[str, str]):
    """Save rationalized records to DuckDB"""
    import duckdb
    
    # Create or connect to database
    conn = duckdb.connect(output_path)
    
    # Drop and create table with appropriate schema
    conn.execute("DROP TABLE IF EXISTS unified_jobs")
    conn.execute("DROP TABLE IF EXISTS overlap_samples")
    create_table_sql = """CREATE TABLE unified_jobs (
        control_number VARCHAR PRIMARY KEY,
        position_title VARCHAR,
        agency_name VARCHAR,
        department_name VARCHAR,
        sub_agency VARCHAR,
        job_series VARCHAR,
        min_grade VARCHAR,
        max_grade VARCHAR,
        pay_scale VARCHAR,
        min_salary DOUBLE,
        max_salary DOUBLE,
        salary_text VARCHAR,
        open_date DATE,
        close_date DATE,
        locations VARCHAR,
        work_schedule VARCHAR,
        telework_eligible VARCHAR,
        security_clearance_required VARCHAR,
        hiring_path VARCHAR,
        qualification_summary TEXT,
        major_duties TEXT,
        requirements TEXT,
        how_to_apply TEXT,
        apply_url VARCHAR,
        position_uri VARCHAR,
        evaluations TEXT,
        benefits TEXT,
        other_information TEXT,
        required_documents TEXT,
        what_to_expect_next TEXT,
        education TEXT,
        job_summary TEXT,
        total_openings VARCHAR,
        promotion_potential VARCHAR,
        relocation_assistance VARCHAR,
        scraped_sections JSON,
        full_content TEXT,
        extraction_metadata JSON,
        data_sources JSON,
        rationalization_date TIMESTAMP
    )"""
    
    conn.execute(create_table_sql)
    
    # Create overlap samples table for comparison analysis
    overlap_table_sql = """CREATE TABLE overlap_samples (
        control_number VARCHAR,
        source_type VARCHAR,  -- 'historical' or 'current'
        position_title VARCHAR,
        agency_name VARCHAR,
        department_name VARCHAR,
        sub_agency VARCHAR,
        job_series VARCHAR,
        min_grade VARCHAR,
        max_grade VARCHAR,
        pay_scale VARCHAR,
        min_salary DOUBLE,
        max_salary DOUBLE,
        salary_text VARCHAR,
        open_date DATE,
        close_date DATE,
        locations VARCHAR,
        work_schedule VARCHAR,
        travel_requirement VARCHAR,
        telework_eligible VARCHAR,
        security_clearance_required VARCHAR,
        hiring_path VARCHAR,
        qualification_summary TEXT,
        major_duties TEXT,
        requirements TEXT,
        how_to_apply TEXT,
        apply_url VARCHAR,
        position_uri VARCHAR,
        evaluations TEXT,
        benefits TEXT,
        other_information TEXT,
        required_documents TEXT,
        what_to_expect_next TEXT,
        education TEXT,
        job_summary TEXT,
        total_openings VARCHAR,
        promotion_potential VARCHAR,
        remote_indicator VARCHAR,
        relocation_assistance VARCHAR
    )"""
    
    conn.execute(overlap_table_sql)
    
    # Clear existing data
    conn.execute("DELETE FROM unified_jobs")
    conn.execute("DELETE FROM overlap_samples")
    
    # Insert records
    if records:
        # Convert records to dataframe-like structure
        df = pd.DataFrame(records)
        
        # Ensure all schema columns are present with default values
        for column_name in schema.keys():
            if column_name not in df.columns:
                df[column_name] = None
        
        # Reorder columns to match schema
        df = df.reindex(columns=list(schema.keys()))
        
        # Insert using DuckDB's register function
        conn.register('temp_df', df)
        conn.execute("INSERT INTO unified_jobs SELECT * FROM temp_df")
        conn.unregister('temp_df')
    
    # Create summary stats
    conn.execute("""
        CREATE OR REPLACE VIEW unified_jobs_summary AS
        SELECT 
            COUNT(*) as total_jobs,
            COUNT(DISTINCT agency_name) as unique_agencies,
            COUNT(DISTINCT job_series) as unique_series,
            MIN(open_date) as earliest_posting,
            MAX(open_date) as latest_posting
        FROM unified_jobs
    """)
    
    # Commit and close
    conn.commit()
    conn.close()
    
    return True

def main():
    parser = argparse.ArgumentParser(description='Rationalize job fields from multiple sources')
    parser.add_argument('--historical-db', help='Path to historical DuckDB file')
    parser.add_argument('--current-json', help='Path to current API JSON file')
    parser.add_argument('--scraped-data', help='Path to scraped data JSON file')
    parser.add_argument('--output', required=True, help='Output file for rationalized data')
    parser.add_argument('--control-numbers', nargs='+', help='Specific control numbers to process')
    parser.add_argument('--limit', type=int, default=50, help='Limit number of records to process')
    parser.add_argument('--min-date', help='Minimum date for current jobs (YYYY-MM-DD), filters out older jobs')
    parser.add_argument('--output-format', choices=['json'], default='json', help='Output format (default: json)')
    
    args = parser.parse_args()
    
    print("🔄 Starting field rationalization...")
    
    # Initialize rationalizer
    rationalizer = FieldRationalizer()
    
    # Load data sources
    historical_data = []
    current_data = []
    
    if args.historical_db:
        print(f"📖 Loading historical data from {args.historical_db}")
        historical_data = load_historical_data(args.historical_db, args.control_numbers)
        print(f"   Loaded {len(historical_data)} historical records")
    
    if args.current_json:
        print(f"📖 Loading current API data from {args.current_json}")
        current_data = load_current_data(args.current_json, args.min_date)
        print(f"   Loaded {len(current_data)} current records")
    
    # Create lookup dictionaries
    historical_lookup = {str(record.get('usajobsControlNumber', '')): record for record in historical_data}
    # Current data uses MatchedObjectId (raw API) or controlNumber (flattened)
    current_lookup = {str(record.get('MatchedObjectId', record.get('controlNumber', ''))): record for record in current_data}
    
    # Process records with proper deduplication
    rationalized_records = []
    processed_control_numbers = set()
    duplicate_count = 0
    
    print(f"\n🔄 Processing records with deduplication...")
    
    # Create all possible control numbers for comparison
    all_control_numbers = set()
    hist_controls = {str(record.get('usajobsControlNumber', '')) for record in historical_data if record.get('usajobsControlNumber')}
    curr_controls = {str(record.get('MatchedObjectId', record.get('controlNumber', ''))) for record in current_data if record.get('MatchedObjectId') or record.get('controlNumber')}
    
    overlapping_controls = hist_controls.intersection(curr_controls)
    
    print(f"📊 Deduplication Analysis:")
    print(f"   Historical records: {len(historical_data)}")
    print(f"   Current records: {len(current_data)}")
    print(f"   Historical control numbers: {len(hist_controls)}")
    print(f"   Current control numbers: {len(curr_controls)}")
    print(f"   🔄 Overlapping control numbers: {len(overlapping_controls)}")
    
    if overlapping_controls:
        print(f"   📝 Sample overlapping controls: {list(overlapping_controls)[:5]}")
    
    # Save ALL overlap samples for analysis
    overlap_samples = []
    
    # Process historical records first (will handle duplicates)
    for hist_record in historical_data:
        control_num = str(hist_record.get('usajobsControlNumber', ''))
        if control_num and control_num not in processed_control_numbers:
            current_record = current_lookup.get(control_num)
            
            if current_record:
                duplicate_count += 1
                
                # Save ALL overlaps for comparison analysis
                # Save historical version with scraped content
                hist_mapped = rationalizer._map_fields(hist_record, 'historical_to_unified')
                
                # Add scraped content to historical sample if available
                if hist_record.get('scraped_content'):
                    scraped_mapped = rationalizer._map_fields(hist_record['scraped_content'], 'scraped_to_unified')
                    scraped_content = rationalizer._extract_scraped_content(hist_record['scraped_content'])
                    scraped_mapped.update(scraped_content)
                    
                    # Add scraped data where historical doesn't have it
                    for field, value in scraped_mapped.items():
                        if field not in hist_mapped or not hist_mapped[field]:
                            hist_mapped[field] = value
                
                hist_sample = {k: v for k, v in hist_mapped.items() if k in rationalizer.unified_schema}
                hist_sample['control_number'] = control_num
                hist_sample['source_type'] = 'historical'
                overlap_samples.append(hist_sample)
                
                # Save current version  
                curr_mapped = rationalizer._map_fields(current_record, 'current_to_unified')
                curr_sample = {k: v for k, v in curr_mapped.items() if k in rationalizer.unified_schema}
                curr_sample['control_number'] = control_num
                curr_sample['source_type'] = 'current'
                overlap_samples.append(curr_sample)
            
            unified_record = rationalizer.rationalize_job_record(
                historical_data=hist_record,
                current_data=current_record,
                scraped_data=hist_record.get('scraped_content')
            )
            
            rationalized_records.append(unified_record)
            processed_control_numbers.add(control_num)
    
    # Process remaining current records (those not in historical data)
    remaining_current = 0
    for curr_record in current_data:
        control_num = str(curr_record.get('MatchedObjectId', curr_record.get('controlNumber', '')))
        if control_num and control_num not in processed_control_numbers:
            unified_record = rationalizer.rationalize_job_record(
                current_data=curr_record
            )
            
            rationalized_records.append(unified_record)
            processed_control_numbers.add(control_num)
            remaining_current += 1
            
            # Process all records - no limit
    
    print(f"\n📈 Processing Results:")
    print(f"   🔄 Duplicates found and merged: {duplicate_count}")
    print(f"   📊 Historical-only records: {len(rationalized_records) - duplicate_count - remaining_current}")
    print(f"   🆕 Current-only records: {remaining_current}")
    print(f"   📋 Total unified records: {len(rationalized_records)}")
    
    # Save results based on format
    if args.output_format == 'duckdb' or args.output.endswith('.duckdb'):
        # Save to DuckDB
        success = save_to_duckdb(rationalized_records, args.output, rationalizer.unified_schema)
        
        # Save overlap samples if any exist
        if overlap_samples:
            conn = duckdb.connect(args.output)
            for sample in overlap_samples:
                # Prepare values for fields that exist in overlap_samples table
                overlap_table_fields = [
                    'position_title', 'agency_name', 'department_name', 'sub_agency', 'job_series',
                    'min_grade', 'max_grade', 'pay_scale', 'min_salary', 'max_salary', 'salary_text',
                    'open_date', 'close_date', 'locations', 'work_schedule',
                    'telework_eligible', 'security_clearance_required', 'hiring_path',
                    'qualification_summary', 'major_duties', 'requirements', 'how_to_apply',
                    'apply_url', 'position_uri', 'evaluations', 'benefits', 'other_information',
                    'required_documents', 'what_to_expect_next', 'education', 'job_summary',
                    'total_openings', 'promotion_potential', 'relocation_assistance'
                ]
                field_names = ['control_number', 'source_type'] + overlap_table_fields
                values = [sample.get('control_number'), sample.get('source_type')] + [sample.get(field) for field in overlap_table_fields]
                placeholders = ','.join(['?' for _ in field_names])
                field_list = ','.join(field_names)
                
                conn.execute(f"INSERT INTO overlap_samples ({field_list}) VALUES ({placeholders})", values)
            
            conn.commit()
            conn.close()
            print(f"   💾 Saved {len(overlap_samples)} overlap samples for analysis")
        
        if success:
            print(f"\n✅ Rationalization complete!")
            print(f"   📊 Processed {len(rationalized_records)} unified records")
            print(f"   🦆 Saved to DuckDB: {args.output}")
            
            # Show database stats
            conn = duckdb.connect(args.output)
            stats = conn.execute("SELECT * FROM unified_jobs_summary").fetchone()
            conn.close()
            
            if stats:
                print(f"\n📈 Database Summary:")
                print(f"   Total jobs: {stats[0]:,}")
                print(f"   Unique agencies: {stats[1]}")
                print(f"   Unique job series: {stats[2]}")
                print(f"   Date range: {stats[3]} to {stats[4]}")
    else:
        # Save as JSON (original behavior)
        output_data = {
            'metadata': {
                'total_records': len(rationalized_records),
                'processing_date': datetime.now().isoformat(),
                'data_sources': {
                    'historical': len(historical_data),
                    'current': len(current_data),
                    'scraped': 0  # TODO: Add scraped data support
                }
            },
            'unified_schema': rationalizer.unified_schema,
            'records': rationalized_records
        }
        
        with open(args.output, 'w') as f:
            json.dump(output_data, f, indent=2, default=str)
        
        print(f"\n✅ Rationalization complete!")
        print(f"   📊 Processed {len(rationalized_records)} unified records")
        print(f"   💾 Saved to JSON: {args.output}")
    
    # Show sample unified record
    if rationalized_records:
        sample = rationalized_records[0]
        print(f"\n📝 Sample unified record:")
        print(f"   Control Number: {sample.get('control_number')}")
        print(f"   Position Title: {sample.get('position_title')}")
        print(f"   Agency: {sample.get('agency_name')}")
        print(f"   Data Sources: {sample.get('data_sources')}")

if __name__ == "__main__":
    main()