#!/usr/bin/env python3
"""
Optimized scraper for USAJobs historical data enhancement

This scraper focuses ONLY on extracting content sections that are NOT available
in the historical API, avoiding redundant extraction of metadata fields.

Content sections extracted:
- majorDuties: Job responsibilities and daily tasks
- qualifications: Required qualifications and experience
- specializedExperience: Specific experience requirements
- education: Education requirements
- howToApply: Application instructions and process
- evaluations: How candidates will be evaluated
- benefits: Compensation and benefits information
- additionalInfo: Other important information
- requirements: General requirements for the position
- requiredDocuments: Documents needed for application
- whatToExpectNext: Next steps in the hiring process
"""

import requests
from bs4 import BeautifulSoup
import time
import argparse
import json
import re
from datetime import datetime


# Content sections using Current API field names
TARGET_SECTIONS = {
    'Summary': ['Summary', 'Overview'],
    'MajorDuties': ['Duties', 'Major Duties', 'Key Duties', 'Responsibilities', 'What You Will Do'],
    'QualificationSummary': ['Qualifications', 'Required Qualifications', 'Minimum Qualifications', 'Qualification Summary'],
    'Requirements': ['Conditions of Employment', 'Specialized Experience', 'Experience Requirements'],
    'Education': ['Education', 'Educational Requirements', 'Education Requirements', 'If you are relying on your education to meet qualification requirements:'],
    'HowToApply': ['How to Apply', 'Application Process', 'Application Instructions'],
    'Evaluations': ['Evaluation', 'How You Will Be Evaluated', 'Rating and Ranking'],
    'Benefits': ['Benefits', 'What We Offer', 'Compensation'],
    'RequiredDocuments': ['Required Documents', 'Documents Required'],
    'WhatToExpectNext': ['What to Expect Next', 'Next Steps'],
    'OtherInformation': ['Additional Information', 'Other Information']
}


def extract_content_sections(soup):
    """Extract ONLY the content sections we need"""
    sections = {}
    
    for section_key, header_variations in TARGET_SECTIONS.items():
        content = extract_section_by_headers(soup, header_variations)
        if content:
            sections[section_key] = content
    
    return sections


def extract_section_by_headers(soup, header_variations):
    """Extract content for a section using multiple header variations"""
    
    for header_text in header_variations:
        # Try multiple approaches to find headers
        content = None
        
        # Approach 1: Direct header search
        headers = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'dt', 'strong'], 
                               string=lambda text: text and header_text.lower() in text.lower() if text else False)
        
        if headers:
            # Try each header until we find one with reasonable content
            for header in headers:
                content = extract_content_after_header(header)
                if content and content.strip():
                    # Prefer content that's a reasonable length (not too long, not too short)
                    content_len = len(content)
                    if 500 < content_len < 5000:  # Reasonable content length
                        return content
            
            # If no reasonable content found, use the first non-empty result
            for header in headers:
                content = extract_content_after_header(header)
                if content and content.strip():
                    return content
        
        # Approach 2: Class-based search
        class_name = header_text.lower().replace(' ', '-')
        class_elements = soup.find_all(['div', 'section'], 
                                     class_=lambda x: x and class_name in ' '.join(x).lower() if x else False)
        
        for elem in class_elements:
            content = extract_element_content(elem)
            if content and content.strip():
                return content
        
        # Approach 3: ID-based search
        id_elements = soup.find_all(['div', 'section'],
                                   id=lambda x: x and class_name in x.lower() if x else False)
        
        for elem in id_elements:
            content = extract_element_content(elem)
            if content and content.strip():
                return content
    
    return None


def extract_content_after_header(header_elem):
    """Extract all content following a header until the next header"""
    content_parts = []
    
    # Get all following siblings
    current = header_elem.find_next_sibling()
    
    while current:
        # Stop at next header (direct sibling)
        if current.name in ['h1', 'h2', 'h3', 'h4', 'h5'] or (
            current.name in ['dt', 'strong'] and len(current.get_text(strip=True)) < 100
        ):
            break
        
        # Also stop if current element contains a header as its first child
        if current.name and hasattr(current, 'children'):
            first_child = next((child for child in current.children if hasattr(child, 'name') and child.name), None)
            if first_child and first_child.name in ['h1', 'h2', 'h3', 'h4', 'h5']:
                break
        
        # Extract text content
        if current.name:
            text = current.get_text(separator=' ', strip=True)
            if text and text.strip():
                content_parts.append(text)
        
        current = current.find_next_sibling()
    
    # Also try parent container approach - but be more selective
    if not content_parts:
        parent = header_elem.find_parent(['div', 'section', 'dd'])
        if parent:
            # Instead of taking all parent text, look for the first immediate content
            # after this header but before any other headers
            header_text = header_elem.get_text(strip=True)
            
            # Find the position of this header in the parent
            header_found = False
            for child in parent.descendants:
                if child == header_elem:
                    header_found = True
                    continue
                    
                if header_found and hasattr(child, 'name'):
                    # Stop if we hit another header
                    if child.name in ['h1', 'h2', 'h3', 'h4', 'h5']:
                        break
                    # Collect text from this element if it's substantial
                    if child.name and child.get_text(strip=True):
                        text = child.get_text(separator=' ', strip=True)
                        if len(text) > 50:  # Only substantial content
                            content_parts.append(text)
                            break  # Take only the first substantial content block
    
    # Special handling for education content
    result = '\n\n'.join(content_parts)
    if not result.strip() and header_elem.get_text(strip=True).lower() in ['education', 'educational requirements']:
        # Look harder for education content
        parent = header_elem.find_parent(['div', 'section'])
        if parent:
            # Find any paragraphs or divs after the header
            for elem in parent.find_all(['p', 'div', 'ul']):
                text = elem.get_text(strip=True)
                if text and text not in result:
                    result += ' ' + text
    
    return result


def extract_element_content(elem):
    """Extract clean content from an element"""
    # Remove any nested headers first
    for header in elem.find_all(['h1', 'h2', 'h3', 'h4', 'h5']):
        header.decompose()
    
    return elem.get_text(separator=' ', strip=True)


def extract_full_text(soup):
    """Extract complete job posting text for NLP analysis"""
    # Remove script, style, nav elements
    for elem in soup(['script', 'style', 'nav', 'header', 'footer']):
        elem.decompose()
    
    # Focus on main content area
    main = soup.find('main') or soup.find('div', class_='usajobs-joa') or soup.body
    
    if main:
        return main.get_text(separator='\n', strip=True)
    
    return soup.get_text(separator='\n', strip=True)


def scrape_enhanced_job_posting(control_number, save_html=False):
    """
    Scrape job posting content sections missing from historical API
    
    Args:
        control_number: Job control number
        save_html: Whether to save raw HTML for debugging
    
    Returns:
        dict: Extracted content sections and metadata
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
        
        # Extract content
        job_data = {
            'control_number': control_number,
            'url': url,
            'status': 'success',
            'scraped_at': datetime.now().isoformat(),
            'scraper_version': 'optimized_v2',
            'content_sections': extract_content_sections(soup),
            'full_text': extract_full_text(soup)
        }
        
        # Add extraction statistics
        job_data['extraction_stats'] = {
            'sections_found': len(job_data['content_sections']),
            'total_content_length': sum(len(v) for v in job_data['content_sections'].values()),
            'full_text_length': len(job_data['full_text']),
            'sections_extracted': list(job_data['content_sections'].keys())
        }
        
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


def main():
    parser = argparse.ArgumentParser(description='Scrape USAJobs content sections')
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