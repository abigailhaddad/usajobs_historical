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
# Don't capture output so we see progress in real-time
result = subprocess.run([
    sys.executable,
    'extract_questionnaires.py'
])

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
subprocess.run(['quarto', 'render', 'executive_order_analysis.qmd', '-o', 'index.html'])
os.chdir('..')

print("\nDone! Open analysis/index.html to view results.")

# Check for untracked questionnaire files
untracked_files = subprocess.run(['git', 'ls-files', '-o', 'raw_questionnaires/'], 
                                capture_output=True, text=True).stdout.strip()
has_untracked_questionnaires = bool(untracked_files)

# Git operations
if new_count > 0 or has_untracked_questionnaires:
    print("\n=== GIT OPERATIONS ===")
    print("Adding questionnaires folder to git...")
    
    # Add all changes in questionnaires folder
    subprocess.run(['git', 'add', '.'])
    
    # Create commit message
    if new_count > 0 and has_untracked_questionnaires:
        commit_message = f"""Update questionnaires: {new_count:,} new links found

- Extracted {new_count:,} new questionnaire links
- Total questionnaire links: {updated_count:,}
- Total scraped files: {len(scraped_files):,}
- New questionnaire files scraped

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>"""
    elif new_count > 0:
        commit_message = f"""Update questionnaires: {new_count:,} new links found

- Extracted {new_count:,} new questionnaire links
- Total questionnaire links: {updated_count:,}
- Total scraped files: {len(scraped_files):,}

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>"""
    else:
        # Count new files
        new_files_count = len(untracked_files.splitlines()) if untracked_files else 0
        commit_message = f"""Update questionnaires: new scraped files added

- No new questionnaire links found
- Total questionnaire links: {updated_count:,}
- Total scraped files: {len(scraped_files):,}
- Added {new_files_count} new questionnaire files

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