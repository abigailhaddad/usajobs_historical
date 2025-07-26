#!/usr/bin/env python3
"""
Run questionnaire analysis after update_all.py
"""
import subprocess
import sys
import os
import pandas as pd
from pathlib import Path

# Check existing questionnaires before extraction
existing_csv = Path('questionnaire_links.csv')
existing_count = 0
if existing_csv.exists():
    existing_df = pd.read_csv(existing_csv)
    existing_count = len(existing_df)
    print(f"Existing questionnaire links: {existing_count:,}")

# Run extraction and scraping
print("\nExtracting and scraping questionnaires...")
result = subprocess.run([
    sys.executable,
    'extract_questionnaires.py'
], capture_output=True, text=True)

# Print the output from extraction
print(result.stdout)
if result.stderr:
    print(result.stderr)

# Check how many new questionnaires were found
if existing_csv.exists():
    updated_df = pd.read_csv(existing_csv)
    updated_count = len(updated_df)
    new_count = updated_count - existing_count
    
    print(f"\n=== QUESTIONNAIRE EXTRACTION SUMMARY ===")
    print(f"Previous total: {existing_count:,}")
    print(f"Current total: {updated_count:,}")
    print(f"New questionnaires found: {new_count:,}")
    
    # Count scraped files
    raw_dir = Path('raw_questionnaires')
    if raw_dir.exists():
        scraped_files = list(raw_dir.glob('*.txt'))
        print(f"Total scraped questionnaire files: {len(scraped_files):,}")

# Render Quarto analysis
print("\nRendering analysis...")
os.chdir('analysis')
subprocess.run(['quarto', 'render', 'executive_order_analysis.qmd'])
os.chdir('..')

print("\nDone! Open analysis/executive_order_analysis.html to view results.")

# Git operations
if new_count > 0:
    print("\n=== GIT OPERATIONS ===")
    print("Adding questionnaires folder to git...")
    
    # Add all changes in questionnaires folder
    subprocess.run(['git', 'add', '.'])
    
    # Create commit message
    commit_message = f"""Update questionnaires: {new_count:,} new links found

- Extracted {new_count:,} new questionnaire links
- Total questionnaire links: {updated_count:,}
- Total scraped files: {len(scraped_files):,}

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>"""
    
    # Commit
    print("\nCommitting changes...")
    result = subprocess.run(['git', 'commit', '-m', commit_message], capture_output=True, text=True)
    if result.returncode == 0:
        print("Changes committed successfully")
    else:
        print(f"Commit output: {result.stdout}")
        if result.stderr:
            print(f"Commit error: {result.stderr}")
    
    # Push
    print("\nPushing to remote...")
    result = subprocess.run(['git', 'push'], capture_output=True, text=True)
    if result.returncode == 0:
        print("Successfully pushed to remote repository")
    else:
        print(f"Push output: {result.stdout}")
        if result.stderr:
            print(f"Push error: {result.stderr}")
else:
    print("\nNo new questionnaires found, skipping git operations")