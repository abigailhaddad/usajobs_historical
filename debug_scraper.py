#!/usr/bin/env python3
"""
Debug what the scraper is actually extracting from the HTML
"""

from bs4 import BeautifulSoup

# Load the HTML file
with open('job_837718400.html', 'r', encoding='utf-8') as f:
    html_content = f.read()

soup = BeautifulSoup(html_content, 'html.parser')

print('=== DEBUGGING REQUIREMENTS SECTION EXTRACTION ===')

# This is what the scraper looks for
TARGET_SECTIONS = {
    'Requirements': ['Requirements', 'Conditions of Employment', 'Specialized Experience', 'Experience Requirements'],
}

def debug_header_search(soup, header_variations):
    """Debug version of extract_section_by_headers"""
    
    for i, header_text in enumerate(header_variations):
        print(f'\n--- Trying header variation {i+1}: "{header_text}" ---')
        
        # Approach 1: Direct header search
        headers = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'dt', 'strong'], 
                               string=lambda text: text and header_text.lower() in text.lower() if text else False)
        
        print(f'Found {len(headers)} headers matching "{header_text}"')
        for j, header in enumerate(headers):
            print(f'  Header {j+1}: <{header.name}> "{header.get_text().strip()[:100]}..."')
            print(f'    Parent: <{header.parent.name if header.parent else None}>')
            
            # See what content would be extracted
            content_parts = []
            current = header.find_next_sibling()
            part_count = 0
            
            while current and part_count < 5:  # Limit to first 5 parts for debugging
                # Stop at next header
                if current.name in ['h1', 'h2', 'h3', 'h4', 'h5'] or (
                    current.name in ['dt', 'strong'] and len(current.get_text(strip=True)) < 100
                ):
                    print(f'    Stopped at: <{current.name}> "{current.get_text().strip()[:50]}..."')
                    break
                
                # Extract text content
                if current.name:
                    text = current.get_text(separator=' ', strip=True)
                    if text and text.strip():
                        content_parts.append(text[:200])  # Limit for debugging
                        print(f'    Part {part_count+1}: "{text[:100]}..."')
                        part_count += 1
                
                current = current.find_next_sibling()
            
            total_content = '\n\n'.join(content_parts)
            print(f'    Total content length: {len(total_content)} chars')
            
            if headers and j == 0:  # Show first match result
                print(f'\n🎯 SCRAPER WOULD RETURN (first {500} chars):')
                print(f'"{total_content[:500]}..."')
                return total_content

# Debug the Requirements section
header_variations = TARGET_SECTIONS['Requirements']
debug_result = debug_header_search(soup, header_variations)

print(f'\n=== COMPARISON ===')
print(f'Scraper result length: {len(debug_result) if debug_result else 0} chars')
print('API correct length: 2448 chars')

# Also check what requirements-related text exists in the page
print(f'\n=== ALL REQUIREMENTS-RELATED TEXT ON PAGE ===')
all_text = soup.get_text()
if 'Conditions of Employment' in all_text:
    start_idx = all_text.find('Conditions of Employment')
    snippet = all_text[start_idx:start_idx+2500]  # Get correct amount
    print(f'Found "Conditions of Employment" at position {start_idx}')
    print(f'Text around it: "{snippet[:500]}..."')