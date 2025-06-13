#!/usr/bin/env python3
"""
Debug the sibling structure to see why the scraper isn't stopping
"""

from bs4 import BeautifulSoup

# Load the HTML file
with open('job_837718400.html', 'r', encoding='utf-8') as f:
    html_content = f.read()

soup = BeautifulSoup(html_content, 'html.parser')

# Find the "Conditions of Employment" h3 header
header = soup.find('h3', string='Conditions of Employment')
print(f'Found header: {header}')
print(f'Header parent: {header.parent}')

print(f'\n=== EXAMINING SIBLINGS ===')
current = header.find_next_sibling()
sibling_count = 0

while current and sibling_count < 20:
    print(f'Sibling {sibling_count + 1}: <{current.name}> "{current.get_text().strip()[:100]}..." (tag: {current.name})')
    
    # Check if this should trigger a stop
    if current.name in ['h1', 'h2', 'h3', 'h4', 'h5']:
        print(f'  ⛔ Should STOP here! Found header: {current.name}')
        break
    elif current.name in ['dt', 'strong'] and len(current.get_text(strip=True)) < 100:
        print(f'  ⛔ Should STOP here! Found short strong/dt: {current.get_text().strip()}')
        break
    
    current = current.find_next_sibling()
    sibling_count += 1

print(f'\n=== CHECKING FOR NESTED STRUCTURE ===')
# Maybe the structure is nested and we need to look at descendants instead
parent_div = header.parent
print(f'Parent div: {parent_div}')
print(f'Parent div children:')

for i, child in enumerate(parent_div.children):
    if hasattr(child, 'name') and child.name:
        text_preview = child.get_text().strip()[:100] if child.get_text() else ""
        print(f'  Child {i}: <{child.name}> "{text_preview}"')
        if child.name in ['h1', 'h2', 'h3', 'h4', 'h5']:
            print(f'    🎯 This is a header that should cause stopping!')