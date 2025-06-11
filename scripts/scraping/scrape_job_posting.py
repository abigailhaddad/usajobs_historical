#!/usr/bin/env python3
"""
Scrape job posting content from USAJobs given a control number
"""

import requests
from bs4 import BeautifulSoup
import time
import argparse
import json
from urllib.parse import urljoin

def scrape_job_posting(control_number, save_html=False):
    """
    Scrape job posting content from USAJobs
    
    Args:
        control_number: Job control number
        save_html: Whether to save raw HTML for debugging
    
    Returns:
        dict: Extracted job content
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
            'scraped_at': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Try to get job title
        title_elem = soup.find('h1')
        if title_elem:
            job_data['title'] = title_elem.get_text(strip=True)
        
        # Try to get agency
        agency_elem = soup.find('span', class_='usajobs-joa-department-name')
        if agency_elem:
            job_data['agency'] = agency_elem.get_text(strip=True)
        
        # Get all text content (we'll parse this more intelligently later)
        # Remove script and style elements
        for script in soup(["script", "style", "nav", "header", "footer"]):
            script.decompose()
        
        # Get main content area
        main_content = soup.find('main') or soup.find('div', class_='usajobs-joa') or soup.body
        if main_content:
            job_data['full_text'] = main_content.get_text(separator='\n', strip=True)
        else:
            job_data['full_text'] = soup.get_text(separator='\n', strip=True)
        
        # Try to extract structured sections
        sections = {}
        
        # Look for common section headers
        section_headers = [
            'Summary', 'Job Summary', 'Position Summary',
            'Duties', 'Major Duties', 'Key Duties', 'Responsibilities',
            'Requirements', 'Qualifications', 'Required Qualifications',
            'Specialized Experience', 'Education', 'Additional Information'
        ]
        
        for header_text in section_headers:
            # Look for headers containing this text
            headers = soup.find_all(['h1', 'h2', 'h3', 'h4', 'dt'], 
                                  string=lambda text: text and header_text.lower() in text.lower())
            
            for header in headers:
                # Get content after this header until next header
                content = []
                next_elem = header.find_next_sibling()
                
                while next_elem and next_elem.name not in ['h1', 'h2', 'h3', 'h4', 'dt']:
                    if next_elem.name:
                        text = next_elem.get_text(strip=True)
                        if text:
                            content.append(text)
                    next_elem = next_elem.find_next_sibling()
                
                if content:
                    sections[header_text.lower().replace(' ', '_')] = '\n'.join(content)
        
        job_data['sections'] = sections
        
        return job_data
        
    except requests.exceptions.RequestException as e:
        return {
            'control_number': control_number,
            'url': url,
            'status': 'error',
            'error': str(e),
            'scraped_at': time.strftime('%Y-%m-%d %H:%M:%S')
        }
    except Exception as e:
        return {
            'control_number': control_number,
            'url': url,
            'status': 'error',
            'error': f"Parsing error: {str(e)}",
            'scraped_at': time.strftime('%Y-%m-%d %H:%M:%S')
        }

def main():
    parser = argparse.ArgumentParser(description='Scrape USAJobs posting')
    parser.add_argument('control_number', help='Job control number')
    parser.add_argument('--save-html', action='store_true', help='Save raw HTML file')
    parser.add_argument('--output', '-o', help='Output JSON file')
    
    args = parser.parse_args()
    
    result = scrape_job_posting(args.control_number, save_html=args.save_html)
    
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