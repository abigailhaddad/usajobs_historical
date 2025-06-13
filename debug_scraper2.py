#!/usr/bin/env python3
"""
Debug specifically the "Conditions of Employment" header
"""

from bs4 import BeautifulSoup

# Load the HTML file
with open('job_837718400.html', 'r', encoding='utf-8') as f:
    html_content = f.read()

soup = BeautifulSoup(html_content, 'html.parser')

print('=== DEBUGGING "CONDITIONS OF EMPLOYMENT" HEADER ===')

# Look for "Conditions of Employment" specifically
headers = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'dt', 'strong'], 
                       string=lambda text: text and 'conditions of employment' in text.lower() if text else False)

print(f'Found {len(headers)} "Conditions of Employment" headers')

for i, header in enumerate(headers):
    print(f'\nHeader {i+1}: <{header.name}> "{header.get_text().strip()}"')
    print(f'  Parent: <{header.parent.name if header.parent else None}>')
    
    # Extract content after this header
    content_parts = []
    current = header.find_next_sibling()
    part_count = 0
    
    print(f'  Following siblings:')
    while current and part_count < 10:
        if current.name in ['h1', 'h2', 'h3', 'h4', 'h5'] or (
            current.name in ['dt', 'strong'] and len(current.get_text(strip=True)) < 100
        ):
            print(f'    STOP at: <{current.name}> "{current.get_text().strip()[:50]}..."')
            break
        
        if current.name:
            text = current.get_text(separator=' ', strip=True)
            if text and text.strip():
                content_parts.append(text)
                print(f'    Part {part_count+1}: "{text[:100]}..." ({len(text)} chars)')
                part_count += 1
        
        current = current.find_next_sibling()
    
    total_content = '\n\n'.join(content_parts)
    print(f'  Total extracted: {len(total_content)} chars')
    
    if len(total_content) > 2000:  # This might be what we want
        print(f'\n🎯 POTENTIAL CORRECT EXTRACTION (first 500 chars):')
        print(f'"{total_content[:500]}..."')
        
        # Compare with API
        print(f'\n=== COMPARISON WITH API ===')
        api_requirements = """Conditions of Employment You must possess U.S. Citizenship or be a U.S. National. You must have reached the minimum age (18) at the time of application. Selective Service registration is required."""
        
        if api_requirements[:100].lower() in total_content[:500].lower():
            print("✅ Content matches API requirements!")
        else:
            print("❌ Content doesn't match API requirements")
            
        # Check if we're getting extra content
        if len(total_content) > 3000:
            print(f"⚠️ Extracted {len(total_content)} chars, but API only has 2448 - likely grabbing extra content")
            
            # Show where the extra content might be coming from
            print(f"\nExtra content preview (chars 2500+):")
            print(f'"{total_content[2500:2800]}..."')

# Also check the structure around these headers
print(f'\n=== HTML STRUCTURE ANALYSIS ===')
for header in headers[:1]:  # Just check first one
    print(f'Header: {header}')
    print(f'Parent: {header.parent}')
    print(f'Parent class: {header.parent.get("class") if header.parent else None}')
    print(f'Grandparent: {header.parent.parent.name if header.parent and header.parent.parent else None}')