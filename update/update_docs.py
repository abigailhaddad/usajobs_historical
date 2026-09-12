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

def sub_or_raise(pattern, replacement, content, what, where):
    """re.sub, but a pattern that matches nothing is an error rather than a no-op.

    Every substitution here has drifted at least once: the wording in the file
    changed, the pattern stopped matching, and the page kept serving a stale
    number with nothing to show anything was wrong.
    """
    if not re.search(pattern, content):
        raise ValueError(
            f"Expected to find {what} in {where} but it was not found -- "
            f"if the wording changed, change the pattern with it")
    return re.sub(pattern, replacement, content)


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
    
    # Update the headline count and the span it covers.
    #
    # The pattern this replaced looked for "**Job dataset with N job
    # announcements", which is not what the line has said for a long time -- so
    # re.sub replaced nothing, silently, and the header sat at "~2.85M ...
    # 2018-2026" while the data went to 3.2M starting in 2013. Hence the raise:
    # a header this cannot find is a header that is about to go stale.
    years = [item['year'] for item in data['data_coverage']]
    header_pattern = (r'\*\*[\d,\.M~]+ job announcements from \d{4}-\d{4} '
                      r'via the Historical \+ Current APIs\*\*')
    if not re.search(header_pattern, content):
        raise ValueError(
            "Expected the '**N job announcements from YYYY-YYYY via the "
            "Historical + Current APIs**' header in README.md but it was not "
            "found -- if the wording changed, change header_pattern with it")
    content = re.sub(
        header_pattern,
        f"**{data['total_jobs']:,} job announcements from {min(years)}-"
        f"{max(years)} via the Historical + Current APIs**",
        content)
    
    # Update file size. The README does not currently carry this string; it is
    # only rewritten when it is there.
    old_size_pattern = r'This provides [\d\.]+ ?[MG]B of data'
    if re.search(old_size_pattern, content):
        content = re.sub(old_size_pattern,
                         f"This provides {data['file_size']} of data", content)
    
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
    years = [item['year'] for item in data['data_coverage']]
    content = sub_or_raise(
        r'<strong>Dataset:</strong> [\d,]+ total job postings',
        f'<strong>Dataset:</strong> {data["total_jobs"]:,} total job postings',
        content, "the 'Dataset: N total job postings' header", 'index.html')
    content = sub_or_raise(
        r'<strong>Coverage:</strong> \d{4}-\d{4}',
        f'<strong>Coverage:</strong> {min(years)}-{max(years)}',
        content, "the 'Coverage: YYYY-YYYY' header", 'index.html')
    content = sub_or_raise(
        r'<strong>Files:</strong> [\d\.]+ ?[MG]B total',
        f'<strong>Files:</strong> {data["file_size"]} total',
        content, "the 'Files: N total' header", 'index.html')
    
    # Update current date references  
    current_date = datetime.now().strftime('%B %d, %Y')
    old_date_pattern = r'Current through [A-Za-z]+ \d+, \d+'
    new_date_text = f'Current through {current_date}'
    content = re.sub(old_date_pattern, new_date_text, content)
    
    # Say which year's file the field table was measured on
    if data.get('field_year'):
        content = sub_or_raise(
            r'<code>historical_jobs_\d{4}\.parquet</code> \(field sample year: \d{4}\)',
            f'<code>historical_jobs_{data["field_year"]}.parquet</code> '
            f'(field sample year: {data["field_year"]})',
            content, "the field-table sample-year note", 'index.html')

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