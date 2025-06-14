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
from pathlib import Path
import os

# Default cache directory in root
DEFAULT_CACHE_DIR = Path(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))) / "html_cache"

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


def parse_all_sections(soup):
    """
    Parse the entire job posting into sections based on headers.
    Returns a dict mapping header text to content.
    """
    sections = {}
    
    # Remove script, style elements first (but keep nav as it may contain content)
    for elem in soup(['script', 'style']):
        elem.decompose()
    
    # Focus on job announcement content area - check multiple possible containers
    main = (soup.find('div', class_='usajobs-joa-overview') or 
            soup.find('div', class_='usajobs-joa') or
            soup.find('div', {'id': 'duties'}).parent if soup.find('div', {'id': 'duties'}) else None or
            soup.find('main') or 
            soup.body)
    
    if not main:
        return sections
    
    # Find all potential headers
    headers = main.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'dt', 'strong'])
    
    # Sort headers by their position in the document
    headers.sort(key=lambda h: (h.sourceline or 0, h.sourcepos or 0) if hasattr(h, 'sourceline') else 0)
    
    for i, header in enumerate(headers):
        header_text = header.get_text(strip=True)
        
        # Skip very long "headers" (probably not actual headers)
        if not header_text or len(header_text) > 100:
            continue
            
        # Skip if this header is inside another header's content
        if any(header.find_parent() == h for h in headers[:i]):
            continue
            
        # Extract content until the next header at same or higher level
        content_parts = []
        
        # Get the next header at same or higher level
        next_header = None
        for j in range(i + 1, len(headers)):
            # Check if it's at same level (sibling) or higher level
            if headers[j].find_parent() != header.find_parent():
                next_header = headers[j]
                break
        
        # Collect all elements between this header and the next
        current = header.next_sibling
        while current:
            # Stop if we've reached the next header
            if next_header and current == next_header:
                break
                
            # Stop if current contains the next header
            if next_header and hasattr(current, 'find_all'):
                if next_header in current.find_all():
                    break
                
            # Extract text from elements
            if hasattr(current, 'name') and current.name:
                # Skip if this is another header
                if current in headers:
                    break
                    
                text = current.get_text(separator=' ', strip=True)
                if text:
                    content_parts.append(text)
            elif isinstance(current, str):
                # Handle text nodes
                text = current.strip()
                if text:
                    content_parts.append(text)
                    
            current = current.next_sibling
        
        # Store the section
        content = '\n\n'.join(content_parts).strip()
        if content:
            # Normalize header text for matching
            normalized_header = header_text.lower().strip()
            # Handle duplicates by appending tag type
            if normalized_header in sections:
                normalized_header = f"{normalized_header}_{header.name}"
                
            sections[normalized_header] = {
                'original_header': header_text,
                'content': content,
                'tag': header.name
            }
    
    return sections


def map_sections_to_fields(parsed_sections):
    """
    Map parsed sections to our target field names.
    Uses fuzzy matching to handle variations in header text.
    """
    mapped = {}
    
    # For each target field, find the best matching section
    for field_name, header_variations in TARGET_SECTIONS.items():
        best_match = None
        best_content = None
        
        # Try each variation
        for variation in header_variations:
            variation_lower = variation.lower().strip()
            
            # Look for exact matches first
            if variation_lower in parsed_sections:
                best_match = variation_lower
                best_content = parsed_sections[variation_lower]['content']
                break
                
            # Then try partial matches
            for section_header, section_data in parsed_sections.items():
                if variation_lower in section_header or section_header in variation_lower:
                    # Prefer shorter headers (more specific)
                    if best_match is None or len(section_header) < len(best_match):
                        best_match = section_header
                        best_content = section_data['content']
        
        if best_content:
            mapped[field_name] = best_content
            
    return mapped


def extract_content_sections(soup):
    """Extract content sections using improved parsing"""
    # First parse all sections
    all_sections = parse_all_sections(soup)
    
    # Then map to our target fields
    mapped_sections = map_sections_to_fields(all_sections)
    
    return mapped_sections




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


def get_cache_path(control_number, cache_dir=None):
    """
    Get the cache file path for a control number.
    Uses subdirectories to avoid too many files in one directory.
    
    Example: 837670300 -> html_cache/837/670/837670300.html
    """
    if cache_dir is None:
        cache_dir = DEFAULT_CACHE_DIR
    
    control_str = str(control_number)
    
    # Create subdirectory structure based on control number
    if len(control_str) >= 6:
        # Split into chunks for directory structure
        subdir1 = control_str[:3]
        subdir2 = control_str[3:6]
        cache_path = cache_dir / subdir1 / subdir2 / f"{control_number}.html"
    else:
        # Fallback for short control numbers
        cache_path = cache_dir / f"{control_number}.html"
    
    return cache_path


def load_from_cache(control_number, cache_dir=None):
    """
    Load HTML from cache if it exists.
    
    Returns:
        str: Cached HTML content if found, None otherwise
    """
    cache_path = get_cache_path(control_number, cache_dir)
    
    if cache_path.exists():
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            print(f"Error reading cache for {control_number}: {e}")
            return None
    
    return None


def save_to_cache(control_number, html_content, cache_dir=None):
    """
    Save HTML content to cache.
    """
    cache_path = get_cache_path(control_number, cache_dir)
    
    try:
        # Create directory structure if needed
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(cache_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
    except Exception as e:
        print(f"Error caching {control_number}: {e}")


def scrape_enhanced_job_posting(control_number, save_html=False, cache_dir=None, force_refresh=False):
    """
    Scrape job posting content sections missing from historical API
    
    Args:
        control_number: Job control number
        save_html: Whether to save additional copy for debugging
        cache_dir: Cache directory (defaults to root html_cache)
        force_refresh: Force fetch from web even if cached
    
    Returns:
        dict: Extracted content sections and metadata
    
    Note: HTML is always cached to html_cache/ folder for future use
    """
    url = f"https://www.usajobs.gov/job/{control_number}"
    html_content = None
    cache_used = False
    
    # Try to load from cache first (unless force refresh)
    if not force_refresh:
        html_content = load_from_cache(control_number, cache_dir)
        if html_content:
            cache_used = True
            print(f"💾 Using cached HTML for {control_number}")
    
    # Fetch from web if not cached or force refresh
    if html_content is None:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    
        try:
            print(f"📡 Fetching from web: {url}")
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            html_content = response.text
            
            # Always save to cache (which saves HTML to root/html_cache/)
            save_to_cache(control_number, html_content, cache_dir)
            
            # Optional: save additional copy for debugging
            if save_html:
                # Save to root directory, not scripts folder
                root_dir = Path(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
                html_debug_dir = root_dir / "html_debug"
                html_debug_dir.mkdir(exist_ok=True)
                html_file_path = html_debug_dir / f"job_{control_number}.html"
                
                with open(html_file_path, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                print(f"Saved additional copy to {html_file_path}")
        
        except requests.exceptions.RequestException as e:
            return {
                'control_number': control_number,
                'url': url,
                'status': 'error',
                'error': str(e),
                'scraped_at': datetime.now().isoformat(),
                'cache_used': False
            }
    
    # Parse HTML (whether from cache or web)
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Parse all sections first for debugging
        all_sections = parse_all_sections(soup)
        
        # Extract content
        job_data = {
            'control_number': control_number,
            'url': url,
            'status': 'success',
            'scraped_at': datetime.now().isoformat(),
            'scraper_version': 'section_parser_v1',
            'cache_used': cache_used,
            'content_sections': map_sections_to_fields(all_sections),
            'full_text': extract_full_text(soup)
        }
        
        # Add extraction statistics
        job_data['extraction_stats'] = {
            'sections_found': len(job_data['content_sections']),
            'total_sections_parsed': len(all_sections),
            'all_section_headers': [data['original_header'] for data in all_sections.values()],
            'total_content_length': sum(len(v) for v in job_data['content_sections'].values()),
            'full_text_length': len(job_data['full_text']),
            'sections_extracted': list(job_data['content_sections'].keys())
        }
        
        return job_data
    except Exception as e:
        return {
            'control_number': control_number,
            'url': url,
            'status': 'error',
            'error': f"Parsing error: {str(e)}",
            'scraped_at': datetime.now().isoformat(),
            'cache_used': cache_used
        }


def main():
    parser = argparse.ArgumentParser(description='Scrape USAJobs content sections (HTML always cached)')
    parser.add_argument('control_number', help='Job control number')
    parser.add_argument('--save-html', action='store_true', help='Save additional debug copy')
    parser.add_argument('--force-refresh', action='store_true', help='Force fetch from web even if cached')
    parser.add_argument('--cache-dir', help='Cache directory (default: root html_cache)')
    parser.add_argument('--output', '-o', help='Output JSON file')
    
    args = parser.parse_args()
    
    cache_dir = Path(args.cache_dir) if args.cache_dir else None
    
    result = scrape_enhanced_job_posting(
        args.control_number, 
        save_html=args.save_html,
        cache_dir=cache_dir,
        force_refresh=args.force_refresh
    )
    
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print(f"Results saved to {args.output}")
    else:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    
    # Add delay to be respectful (only if we fetched from web)
    if not result.get('cache_used', False):
        time.sleep(1)


if __name__ == "__main__":
    main()