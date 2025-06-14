#!/usr/bin/env python3
"""
Debug parsing in detail to see why Duties is not captured
"""

import sys
from pathlib import Path
from bs4 import BeautifulSoup

# Add the pipeline scripts to path
sys.path.append('usajobs_pipeline/scripts')

def debug_parse_all_sections(soup):
    """
    Debug version of parse_all_sections with detailed logging
    """
    sections = {}
    
    # Remove script, style elements first
    for elem in soup(['script', 'style']):
        elem.decompose()
    
    # Focus on job announcement content area
    main = (soup.find('div', class_='usajobs-joa-overview') or 
            soup.find('div', class_='usajobs-joa') or
            soup.find('div', {'id': 'duties'}).parent if soup.find('div', {'id': 'duties'}) else None or
            soup.find('main') or 
            soup.body)
    
    print(f"Main container found: {main.name if main else 'None'}")
    if main and hasattr(main, 'get'):
        print(f"Main container class: {main.get('class', [])}")
    
    if not main:
        return sections
    
    # Find all potential headers
    headers = main.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'dt', 'strong'])
    print(f"Found {len(headers)} potential headers")
    
    # Look specifically for Duties header
    duties_headers = [h for h in headers if 'duties' in h.get_text(strip=True).lower()]
    print(f"Found {len(duties_headers)} duties-related headers:")
    for i, h in enumerate(duties_headers):
        print(f"  {i+1}. '{h.get_text(strip=True)}' (tag: {h.name})")
        print(f"      Parent: {h.parent.name if h.parent else 'None'}")
        if h.parent and hasattr(h.parent, 'get'):
            print(f"      Parent class: {h.parent.get('class', [])}")
    
    # Sort headers by their position in the document
    headers.sort(key=lambda h: (h.sourceline or 0, h.sourcepos or 0) if hasattr(h, 'sourceline') else 0)
    
    for i, header in enumerate(headers):
        header_text = header.get_text(strip=True)
        
        # Skip very long "headers"
        if not header_text or len(header_text) > 100:
            continue
        
        # Debug specific "Duties" header
        if 'duties' in header_text.lower():
            print(f"\n=== PROCESSING DUTIES HEADER ===")
            print(f"Header text: '{header_text}'")
            print(f"Header tag: {header.name}")
            print(f"Header index: {i}")
            
            # Skip if this header is inside another header's content
            is_inside_other = any(header.find_parent() == h for h in headers[:i])
            print(f"Is inside other header: {is_inside_other}")
            if is_inside_other:
                print("SKIPPING: Header is inside another header")
                continue
            
            # Extract content until the next header at same or higher level
            content_parts = []
            
            # Get the next header at same or higher level
            next_header = None
            for j in range(i + 1, len(headers)):
                if headers[j].find_parent() != header.find_parent():
                    next_header = headers[j]
                    break
            
            print(f"Next header: {next_header.get_text(strip=True) if next_header else 'None'}")
            
            # Collect all elements between this header and the next
            current = header.next_sibling
            sibling_count = 0
            while current and sibling_count < 10:  # Limit for debugging
                sibling_count += 1
                print(f"  Sibling {sibling_count}: {type(current).__name__}")
                
                # Stop if we've reached the next header
                if next_header and current == next_header:
                    print("    STOP: Reached next header")
                    break
                    
                # Stop if current contains the next header
                if next_header and hasattr(current, 'find_all'):
                    if next_header in current.find_all():
                        print("    STOP: Contains next header")
                        break
                    
                # Extract text from elements
                if hasattr(current, 'name') and current.name:
                    # Skip if this is another header
                    if current in headers:
                        print("    STOP: Found another header")
                        break
                        
                    text = current.get_text(separator=' ', strip=True)
                    if text:
                        print(f"    Adding text ({len(text)} chars): {text[:100]}...")
                        content_parts.append(text)
                elif isinstance(current, str):
                    # Handle text nodes
                    text = current.strip()
                    if text:
                        print(f"    Adding text node: {text[:50]}...")
                        content_parts.append(text)
                        
                current = current.next_sibling
            
            # Store the section
            content = '\n\n'.join(content_parts).strip()
            print(f"Final content length: {len(content)}")
            print(f"Content preview: {content[:200]}...")
            
            if content:
                normalized_header = header_text.lower().strip()
                if normalized_header in sections:
                    normalized_header = f"{normalized_header}_{header.name}"
                    
                sections[normalized_header] = {
                    'original_header': header_text,
                    'content': content,
                    'tag': header.name
                }
                print(f"STORED as key: '{normalized_header}'")
            else:
                print("NO CONTENT - not storing")
            print("=== END DUTIES PROCESSING ===\n")
    
    return sections

def debug_html_file(html_path):
    """Debug a specific HTML file"""
    print(f"=== DEBUGGING: {html_path} ===")
    
    with open(html_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    soup = BeautifulSoup(html_content, 'html.parser')
    sections = debug_parse_all_sections(soup)
    
    print(f"\nFinal sections keys: {list(sections.keys())}")
    return sections

if __name__ == "__main__":
    html_file = "/Users/abigailhaddad/Documents/repos/usajobs_historic/html_cache/837/322/837322200.html"
    debug_html_file(html_file)