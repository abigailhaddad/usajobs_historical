#!/usr/bin/env python3
"""
Optimized scraper that ONLY extracts content missing from historical API

This scraper focuses exclusively on text content sections that are NOT
available in the historical API, avoiding redundant extraction of metadata.
"""

import requests
from bs4 import BeautifulSoup
import time
import argparse
import json
import re
from datetime import datetime


# Content sections we need (not in historical API)
TARGET_SECTIONS = {
    'majorDuties': ['Duties', 'Major Duties', 'Key Duties', 'Responsibilities', 'What You Will Do'],
    'qualifications': ['Qualifications', 'Requirements', 'Required Qualifications', 'Minimum Qualifications'],
    'specializedExperience': ['Specialized Experience', 'Experience Requirements'],
    'education': ['Education', 'Educational Requirements', 'Education Requirements'],
    'howToApply': ['How to Apply', 'Application Process', 'Application Instructions'],
    'evaluations': ['Evaluation', 'How You Will Be Evaluated', 'Rating and Ranking'],
    'benefits': ['Benefits', 'What We Offer', 'Compensation'],
    'additionalInfo': ['Additional Information', 'Other Information'],
    'requirements': ['Requirements', 'Conditions of Employment'],
    'requiredDocuments': ['Required Documents', 'Documents Required'],
    'whatToExpectNext': ['What to Expect Next', 'Next Steps']
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
            content = extract_content_after_header(headers[0])
            if content and len(content) > 50:  # Minimum content threshold
                return content
        
        # Approach 2: Class-based search
        class_name = header_text.lower().replace(' ', '-')
        class_elements = soup.find_all(['div', 'section'], 
                                     class_=lambda x: x and class_name in ' '.join(x).lower() if x else False)
        
        for elem in class_elements:
            content = extract_element_content(elem)
            if content and len(content) > 50:
                return content
        
        # Approach 3: ID-based search
        id_elements = soup.find_all(['div', 'section'],
                                   id=lambda x: x and class_name in x.lower() if x else False)
        
        for elem in id_elements:
            content = extract_element_content(elem)
            if content and len(content) > 50:
                return content
    
    return None


def extract_content_after_header(header_elem):
    """Extract all content following a header until the next header"""
    content_parts = []
    
    # Get all following siblings
    current = header_elem.find_next_sibling()
    
    while current:
        # Stop at next header
        if current.name in ['h1', 'h2', 'h3', 'h4', 'h5'] or (
            current.name in ['dt', 'strong'] and len(current.get_text(strip=True)) < 100
        ):
            break
        
        # Extract text content
        if current.name:
            text = current.get_text(separator=' ', strip=True)
            if text and len(text) > 10:
                content_parts.append(text)
        
        current = current.find_next_sibling()
    
    # Also try parent container approach
    if not content_parts:
        parent = header_elem.find_parent(['div', 'section', 'dd'])
        if parent:
            # Get all text except the header itself
            header_text = header_elem.get_text(strip=True)
            full_text = parent.get_text(separator=' ', strip=True)
            content = full_text.replace(header_text, '', 1).strip()
            if content:
                content_parts.append(content)
    
    return '\n\n'.join(content_parts)


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


def scrape_job_content(control_number):
    """
    Scrape ONLY the content fields missing from historical API
    
    Returns:
        dict: Contains only content sections and full text
    """
    url = f"https://www.usajobs.gov/job/{control_number}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract only what we need
        result = {
            'controlNumber': control_number,
            'scrapedAt': datetime.now().isoformat(),
            'status': 'success',
            'content': extract_content_sections(soup),
            'fullText': extract_full_text(soup)
        }
        
        # Add extraction statistics
        result['stats'] = {
            'sectionsFound': len(result['content']),
            'totalContentLength': sum(len(v) for v in result['content'].values()),
            'fullTextLength': len(result['fullText'])
        }
        
        return result
        
    except Exception as e:
        return {
            'controlNumber': control_number,
            'scrapedAt': datetime.now().isoformat(),
            'status': 'error',
            'error': str(e)
        }


def compare_with_enhanced_scraper(control_number):
    """Compare optimized scraper with the enhanced scraper results"""
    
    print(f"\nComparing scrapers for job {control_number}")
    print("=" * 60)
    
    # Run optimized scraper
    optimized = scrape_job_content(control_number)
    
    if optimized['status'] == 'success':
        print("\nOPTIMIZED SCRAPER RESULTS:")
        print("-" * 40)
        print(f"Sections found: {optimized['stats']['sectionsFound']}")
        print(f"Total content length: {optimized['stats']['totalContentLength']:,} chars")
        print(f"Full text length: {optimized['stats']['fullTextLength']:,} chars")
        
        print("\nContent sections extracted:")
        for section, content in optimized['content'].items():
            print(f"  ✓ {section}: {len(content)} chars")
            if len(content) > 100:
                preview = content[:100] + "..."
            else:
                preview = content
            print(f"    Preview: {preview}")
    else:
        print(f"Error: {optimized['error']}")
    
    return optimized


def main():
    parser = argparse.ArgumentParser(description='Optimized content-only scraper')
    parser.add_argument('control_number', help='Job control number')
    parser.add_argument('--compare', action='store_true', help='Compare with enhanced scraper')
    parser.add_argument('--output', '-o', help='Output JSON file')
    
    args = parser.parse_args()
    
    if args.compare:
        result = compare_with_enhanced_scraper(args.control_number)
    else:
        result = scrape_job_content(args.control_number)
        
        if result['status'] == 'success':
            print(f"✓ Successfully scraped {result['stats']['sectionsFound']} content sections")
            print(f"  Total content: {result['stats']['totalContentLength']:,} chars")
        else:
            print(f"✗ Error: {result['error']}")
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"\nSaved to {args.output}")


if __name__ == "__main__":
    main()