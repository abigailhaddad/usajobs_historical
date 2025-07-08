#!/usr/bin/env python3
"""
Update README.md and index.html with current data from docs_data.json

This script reads the generated documentation data and updates the static
files with current statistics, field information, and data coverage.

Usage:
    python update/update_docs.py
"""

import json
import re
from datetime import datetime

def update_readme():
    """Update README.md with current data"""
    with open('../docs_data.json', 'r') as f:
        data = json.load(f)
    
    with open('../README.md', 'r') as f:
        content = f.read()
    
    # Update data collection date - it must already exist
    if data.get('latest_job_date'):
        # Look for existing data collection line and replace it
        date_pattern = r'\*\*Data collection last run: \d{4}-\d{2}-\d{2}\*\*'
        new_date_text = f'**Data collection last run: {data["latest_job_date"]}**'
        
        if re.search(date_pattern, content):
            # Replace existing date
            content = re.sub(date_pattern, new_date_text, content)
        else:
            # If date line not found, this is an error
            raise ValueError("Expected to find 'Data collection last run' line in README.md but it was not found")
    
    # Update total jobs count in the header
    old_pattern = r'\*\*Job dataset with [\d,\.M]+ job announcements'
    new_text = f"**Job dataset with {data['total_jobs']:,} job announcements"
    content = re.sub(old_pattern, new_text, content)
    
    # Update file size
    old_size_pattern = r'This provides \d+MB of'
    new_size_text = f"This provides {data['file_size']} of"
    content = re.sub(old_size_pattern, new_size_text, content)
    
    # Update Data Coverage section header with latest date
    if data.get('latest_job_date'):
        # Only update the date in the Data Coverage section, not the whole text
        coverage_pattern = r'(## Data Coverage\n\nData collection last run: )\d{4}-\d{2}-\d{2}'
        coverage_replacement = f'\\g<1>{data["latest_job_date"]}'
        content = re.sub(coverage_pattern, coverage_replacement, content)
    
    # Update data coverage table
    coverage_rows = []
    for item in data['data_coverage']:
        year = item['year']
        opened = f"{item['jobs_opened']:,}" if item['jobs_opened'] else "0"
        closed = f"{item['jobs_closed']:,}" if item['jobs_closed'] else "0" 
        coverage_rows.append(f"| {year} | {opened} | {closed} |")
    
    # Find and replace the coverage table
    table_start = content.find("| Year | Jobs Opened | Jobs Closed |")
    table_end = content.find("\n\n", table_start)
    
    if table_start != -1 and table_end != -1:
        new_table = "| Year | Jobs Opened | Jobs Closed |\n|------|-------------|-------------|\n" + "\n".join(coverage_rows)
        content = content[:table_start] + new_table + content[table_end:]
    
    with open('../README.md', 'w') as f:
        f.write(content)
    
    print("✅ Updated README.md")

def update_index_html():
    """Update index.html with current data"""
    with open('../docs_data.json', 'r') as f:
        data = json.load(f)
    
    with open('../index.html', 'r') as f:
        content = f.read()
    
    # Update data collection date - it must already exist
    if data.get('latest_job_date'):
        # Look for existing data collection line and replace it
        date_pattern = r'<p><strong>Data collection last run: \d{4}-\d{2}-\d{2}</strong></p>'
        new_date_text = f'<p><strong>Data collection last run: {data["latest_job_date"]}</strong></p>'
        
        if re.search(date_pattern, content):
            # Replace existing date
            content = re.sub(date_pattern, new_date_text, content)
        else:
            # If date line not found, this is an error
            raise ValueError("Expected to find 'Data collection last run' line in index.html but it was not found")
    
    # Update dataset stats in header
    old_pattern = r'<strong>Dataset:</strong> [\d,]+ total job postings'
    new_text = f'<strong>Dataset:</strong> {data["total_jobs"]:,} total job postings'
    content = re.sub(old_pattern, new_text, content)
    
    # Update file size in header
    old_size_pattern = r'<strong>Files:</strong> \d+MB total'
    new_size_text = f'<strong>Files:</strong> {data["file_size"]} total'
    content = re.sub(old_size_pattern, new_size_text, content)
    
    # Update current date references  
    current_date = datetime.now().strftime('%B %d, %Y')
    old_date_pattern = r'Current through [A-Za-z]+ \d+, \d+'
    new_date_text = f'Current through {current_date}'
    content = re.sub(old_date_pattern, new_date_text, content)
    
    # Update data coverage table
    coverage_rows = []
    for item in data['data_coverage']:
        year = item['year']
        opened = f"{item['jobs_opened']:,}" if item['jobs_opened'] else "0"
        closed = f"{item['jobs_closed']:,}" if item['jobs_closed'] else "0"
        coverage = item['coverage']
        coverage_rows.append(f"                <tr><td>{year}</td><td>{opened}</td><td>{closed}</td><td>{coverage}</td></tr>")
    
    # Find and replace coverage table tbody
    table_start = content.find('<h2 id="data-coverage">Data Coverage by Year</h2>')
    tbody_start = content.find('<tbody>', table_start)
    tbody_end = content.find('</tbody>', tbody_start)
    
    if tbody_start != -1 and tbody_end != -1:
        new_tbody = "<tbody>\n" + "\n".join(coverage_rows) + "\n            "
        content = content[:tbody_start] + new_tbody + content[tbody_end:]
    
    # Update all fields table  
    field_rows = []
    for field in data['all_fields']:
        name = field['field_name']
        field_type = field['field_type']
        examples = field['examples']
        completeness = f"{field['completeness_percent']}%"
        css_class = field['completeness_class']
        
        field_rows.append(f"""                <tr>
                    <td><code class="field-name">{name}</code></td>
                    <td>{field_type}</td>
                    <td class="examples">{examples}</td>
                    <td class="completeness {css_class}">{completeness}</td>
                </tr>""")
    
    # Find and replace fields table tbody
    fields_start = content.find('<h2 id="all-fields">All Fields</h2>')
    fields_tbody_start = content.find('<tbody>', fields_start)
    fields_tbody_end = content.find('</tbody>', fields_tbody_start)
    
    if fields_tbody_start != -1 and fields_tbody_end != -1:
        new_fields_tbody = "<tbody>\n" + "\n".join(field_rows) + "\n            "
        content = content[:fields_tbody_start] + new_fields_tbody + content[fields_tbody_end:]
    
    with open('../index.html', 'w') as f:
        f.write(content)
    
    print("✅ Updated index.html")

def main():
    """Main function to update all documentation"""
    if not os.path.exists('../docs_data.json'):
        print("❌ docs_data.json not found. Run 'python generate_docs_data.py' first.")
        return
    
    print("📝 Updating documentation files...")
    
    try:
        update_readme()
        update_index_html()
        print("🎉 Documentation updated successfully!")
        
        # Show what was updated
        with open('../docs_data.json', 'r') as f:
            data = json.load(f)
        
        print(f"\nUpdated with data from: {data['generated_at']}")
        print(f"Total jobs: {data['total_jobs']:,}")
        print(f"Total fields: {data['total_fields']}")
        
    except Exception as e:
        print(f"❌ Error updating documentation: {e}")

if __name__ == "__main__":
    import os
    main()