#!/usr/bin/env python3
"""
Enhanced scrape job posting content from USAJobs to match current API fields

Extracts structured data that can be rationalized with current API:
- Detailed job information sections
- Salary and grade information
- Location details
- Application process information
- Qualification requirements
"""

import requests
from bs4 import BeautifulSoup, NavigableString
import time
import argparse
import json
import re
from urllib.parse import urljoin
from datetime import datetime

def extract_salary_info(soup):
    """Extract salary and grade information"""
    salary_data = {}
    
    # Look for salary information in various places
    salary_patterns = [
        r'\$[\d,]+\s*-\s*\$[\d,]+',
        r'\$[\d,]+\.?\d*',
        r'GS-(\d+)',
        r'Grade\s*(\d+)',
    ]
    
    text = soup.get_text()
    for pattern in salary_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        if matches:
            if '$' in pattern:
                salary_data['salary_text'] = matches
            elif 'GS' in pattern:
                salary_data['grade'] = matches
    
    # Look for pay scale information
    pay_elements = soup.find_all(text=re.compile(r'Pay scale|Grade|GS-\d+', re.IGNORECASE))
    if pay_elements:
        salary_data['pay_scale_context'] = [elem.strip() for elem in pay_elements if elem.strip()]
    
    return salary_data

def extract_location_info(soup):
    """Extract detailed location information"""
    location_data = {}
    
    # Look for location elements
    location_selectors = [
        '.usajobs-joa-location',
        '.location',
        '[data-location]',
        '.position-location'
    ]
    
    for selector in location_selectors:
        location_elem = soup.select_one(selector)
        if location_elem:
            location_data['primary_location'] = location_elem.get_text(strip=True)
            break
    
    # Look for multiple locations
    location_list = soup.find_all(text=re.compile(r'locations?:', re.IGNORECASE))
    if location_list:
        location_data['location_context'] = [loc.strip() for loc in location_list]
    
    # Extract from structured data
    location_text = soup.get_text()
    location_patterns = [
        r'Location:\s*([^\n]+)',
        r'Work Location:\s*([^\n]+)',
        r'Duty Location:\s*([^\n]+)',
    ]
    
    for pattern in location_patterns:
        match = re.search(pattern, location_text, re.IGNORECASE)
        if match:
            location_data['extracted_location'] = match.group(1).strip()
            break
    
    return location_data

def extract_dates(soup):
    """Extract important dates"""
    dates_data = {}
    
    text = soup.get_text()
    
    # Date patterns
    date_patterns = {
        'open_date': [
            r'Open[ing]*\s*[:\-]\s*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})',
            r'Applications\s*(?:will\s*be\s*)?accepted\s*(?:from|starting)?\s*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})',
        ],
        'close_date': [
            r'Clos[ing]*\s*[:\-]\s*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})',
            r'Applications\s*(?:must\s*be\s*)?(?:received|submitted)\s*by\s*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})',
            r'Deadline\s*[:\-]\s*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})',
        ]
    }
    
    for date_type, patterns in date_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                dates_data[date_type] = match.group(1)
                break
    
    return dates_data

def extract_agency_info(soup):
    """Extract detailed agency information"""
    agency_data = {}
    
    # Agency selectors
    agency_selectors = [
        '.usajobs-joa-department-name',
        '.department-name',
        '.agency-name',
        '[data-agency]'
    ]
    
    for selector in agency_selectors:
        agency_elem = soup.select_one(selector)
        if agency_elem:
            agency_data['department'] = agency_elem.get_text(strip=True)
            break
    
    # Sub-agency
    sub_agency_selectors = [
        '.usajobs-joa-agency-name',
        '.sub-agency',
        '.organization-name'
    ]
    
    for selector in sub_agency_selectors:
        sub_elem = soup.select_one(selector)
        if sub_elem:
            agency_data['sub_agency'] = sub_elem.get_text(strip=True)
            break
    
    return agency_data

def extract_job_details(soup):
    """Extract job-specific details"""
    details = {}
    
    text = soup.get_text()
    
    # Job series
    series_patterns = [
        r'Series:\s*(\d{4})',
        r'Job\s*Series:\s*(\d{4})',
        r'Position\s*Series:\s*(\d{4})',
    ]
    
    for pattern in series_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            details['job_series'] = match.group(1)
            break
    
    # Work schedule
    schedule_patterns = [
        r'Work\s*Schedule:\s*([^\n]+)',
        r'Schedule:\s*([^\n]+)',
        r'(Full[- ]time|Part[- ]time|Intermittent)',
    ]
    
    for pattern in schedule_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            details['work_schedule'] = match.group(1).strip()
            break
    
    # Security clearance
    if re.search(r'security\s*clearance', text, re.IGNORECASE):
        clearance_match = re.search(r'(Secret|Top\s*Secret|Confidential|Public\s*Trust)(?:\s*clearance)?', text, re.IGNORECASE)
        if clearance_match:
            details['security_clearance'] = clearance_match.group(1)
    
    # Travel requirement
    travel_patterns = [
        r'Travel:\s*([^\n]+)',
        r'Travel\s*Required:\s*([^\n]+)',
        r'(\d{1,2}%)\s*travel',
    ]
    
    for pattern in travel_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            details['travel'] = match.group(1).strip()
            break
    
    return details

def extract_structured_sections(soup):
    """Extract structured content sections"""
    sections = {}
    
    # Enhanced section mapping to match current API
    section_mappings = {
        'summary': ['Summary', 'Job Summary', 'Position Summary', 'Overview'],
        'duties': ['Duties', 'Major Duties', 'Key Duties', 'Responsibilities', 'What You Will Do'],
        'qualifications': ['Qualifications', 'Requirements', 'Required Qualifications', 'Minimum Qualifications'],
        'specialized_experience': ['Specialized Experience', 'Experience Requirements'],
        'education': ['Education', 'Educational Requirements', 'Education Requirements'],
        'additional_info': ['Additional Information', 'Other Information', 'Benefits'],
        'how_to_apply': ['How to Apply', 'Application Process', 'Application Instructions'],
        'evaluations': ['Evaluation', 'How You Will Be Evaluated', 'Rating and Ranking'],
        'benefits': ['Benefits', 'What We Offer', 'Compensation']
    }
    
    for section_key, header_variations in section_mappings.items():
        for header_text in header_variations:
            # Look for headers
            headers = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'dt', 'strong'], 
                                  string=lambda text: text and header_text.lower() in text.lower() if text else False)
            
            # Also look for divs with these class patterns
            class_headers = soup.find_all(['div', 'section'], 
                                        class_=lambda x: x and any(header_text.lower().replace(' ', '-') in cls.lower() for cls in x) if x else False)
            
            all_headers = headers + class_headers
            
            for header in all_headers:
                content = extract_section_content(header)
                if content and len(content) > 50:  # Only keep substantial content
                    sections[section_key] = content
                    break
            
            if section_key in sections:
                break
    
    return sections

def extract_section_content(header_elem):
    """Extract content following a header element"""
    content = []
    
    # Try multiple approaches to get content
    approaches = [
        lambda h: get_next_siblings_until_header(h),
        lambda h: get_parent_content(h),
        lambda h: get_following_content(h)
    ]
    
    for approach in approaches:
        try:
            result = approach(header_elem)
            if result and len(result) > len(content):
                content = result
        except:
            continue
    
    return '\n'.join(content) if content else ''

def get_next_siblings_until_header(elem):
    """Get text from next siblings until hitting another header"""
    content = []
    next_elem = elem.find_next_sibling()
    
    while next_elem and next_elem.name not in ['h1', 'h2', 'h3', 'h4', 'h5']:
        if next_elem.name:
            text = next_elem.get_text(strip=True)
            if text and len(text) > 10:  # Skip very short text
                content.append(text)
        next_elem = next_elem.find_next_sibling()
    
    return content

def get_parent_content(elem):
    """Get content from parent element"""
    parent = elem.find_parent()
    if parent:
        # Remove the header itself
        header_text = elem.get_text(strip=True)
        parent_text = parent.get_text(strip=True)
        if header_text in parent_text:
            content_text = parent_text.replace(header_text, '', 1).strip()
            return [content_text] if content_text else []
    return []

def get_following_content(elem):
    """Get content from following elements"""
    content = []
    for next_elem in elem.find_all_next(['p', 'div', 'li', 'ul', 'ol']):
        if next_elem.find(['h1', 'h2', 'h3', 'h4', 'h5']):
            break
        text = next_elem.get_text(strip=True)
        if text and len(text) > 10:
            content.append(text)
        if len(content) > 5:  # Don't get too much
            break
    return content

def scrape_enhanced_job_posting(control_number, save_html=False):
    """
    Enhanced scrape job posting with structured field extraction
    
    Args:
        control_number: Job control number
        save_html: Whether to save raw HTML for debugging
    
    Returns:
        dict: Enhanced extracted job content
    """
    url = f"https://www.usajobs.gov/job/{control_number}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    try:
        print(f"Fetching: {url}")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Save raw HTML if requested
        if save_html:
            with open(f"job_{control_number}.html", 'w', encoding='utf-8') as f:
                f.write(response.text)
            print(f"Saved raw HTML to job_{control_number}.html")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract basic info
        job_data = {
            'control_number': control_number,
            'url': url,
            'status': 'success',
            'scraped_at': datetime.now().isoformat(),
            'scraper_version': 'enhanced_v1'
        }
        
        # Basic fields (original functionality)
        title_elem = soup.find('h1')
        if title_elem:
            job_data['position_title'] = title_elem.get_text(strip=True)
        
        # Enhanced field extraction
        job_data.update({
            'salary_info': extract_salary_info(soup),
            'location_info': extract_location_info(soup),
            'dates': extract_dates(soup),
            'agency_info': extract_agency_info(soup),
            'job_details': extract_job_details(soup),
            'structured_sections': extract_structured_sections(soup)
        })
        
        # Get full text (cleaned)
        for script in soup(["script", "style", "nav", "header", "footer"]):
            script.decompose()
        
        main_content = soup.find('main') or soup.find('div', class_='usajobs-joa') or soup.body
        if main_content:
            job_data['full_text'] = main_content.get_text(separator='\n', strip=True)
        else:
            job_data['full_text'] = soup.get_text(separator='\n', strip=True)
        
        # Field mapping for rationalization
        job_data['rationalized_fields'] = create_field_mapping(job_data)
        
        return job_data
        
    except requests.exceptions.RequestException as e:
        return {
            'control_number': control_number,
            'url': url,
            'status': 'error',
            'error': str(e),
            'scraped_at': datetime.now().isoformat()
        }
    except Exception as e:
        return {
            'control_number': control_number,
            'url': url,
            'status': 'error',
            'error': f"Parsing error: {str(e)}",
            'scraped_at': datetime.now().isoformat()
        }

def create_field_mapping(job_data):
    """Create field mapping that aligns with current API structure"""
    mapping = {}
    
    # Map to current API field names
    if 'position_title' in job_data:
        mapping['PositionTitle'] = job_data['position_title']
    
    # Agency mapping
    if 'agency_info' in job_data:
        if 'department' in job_data['agency_info']:
            mapping['DepartmentName'] = job_data['agency_info']['department']
        if 'sub_agency' in job_data['agency_info']:
            mapping['SubAgency'] = job_data['agency_info']['sub_agency']
    
    # Location mapping
    if 'location_info' in job_data:
        if 'primary_location' in job_data['location_info']:
            mapping['PositionLocationDisplay'] = job_data['location_info']['primary_location']
    
    # Job details mapping
    if 'job_details' in job_data:
        if 'job_series' in job_data['job_details']:
            mapping['JobCategory'] = [{'Code': job_data['job_details']['job_series']}]
        if 'work_schedule' in job_data['job_details']:
            mapping['PositionSchedule'] = [{'Name': job_data['job_details']['work_schedule']}]
        if 'security_clearance' in job_data['job_details']:
            mapping['SecurityClearance'] = job_data['job_details']['security_clearance']
        if 'travel' in job_data['job_details']:
            mapping['TravelRequirement'] = job_data['job_details']['travel']
    
    # Dates mapping
    if 'dates' in job_data:
        if 'open_date' in job_data['dates']:
            mapping['PositionStartDate'] = job_data['dates']['open_date']
        if 'close_date' in job_data['dates']:
            mapping['PositionEndDate'] = job_data['dates']['close_date']
    
    # Structured content mapping
    if 'structured_sections' in job_data:
        sections = job_data['structured_sections']
        
        # Map to current API content fields
        content_mapping = {
            'qualifications': 'QualificationSummary',
            'duties': 'MajorDuties',
            'education': 'EducationRequirements',
            'how_to_apply': 'HowToApply',
            'additional_info': 'AdditionalInformation'
        }
        
        for section_key, api_field in content_mapping.items():
            if section_key in sections:
                mapping[api_field] = sections[section_key]
    
    return mapping

def main():
    parser = argparse.ArgumentParser(description='Enhanced scrape USAJobs posting')
    parser.add_argument('control_number', help='Job control number')
    parser.add_argument('--save-html', action='store_true', help='Save raw HTML file')
    parser.add_argument('--output', '-o', help='Output JSON file')
    
    args = parser.parse_args()
    
    result = scrape_enhanced_job_posting(args.control_number, save_html=args.save_html)
    
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print(f"Results saved to {args.output}")
    else:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    
    # Add delay to be respectful
    time.sleep(1)

if __name__ == "__main__":
    main()