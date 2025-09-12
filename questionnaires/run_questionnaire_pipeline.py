#!/usr/bin/env python3
"""
Run questionnaire analysis after update_all.py
"""
import subprocess
import sys
import os
import json
import pandas as pd
from pathlib import Path
from questionnaire_utils import (
    transform_monster_url, extract_questionnaire_id, get_questionnaire_filepath,
    create_git_commit_message, QUESTIONNAIRE_LINKS_CSV, RAW_QUESTIONNAIRES_DIR
)

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

# Always check for CSV updates, even if the script failed
if existing_csv.exists():
    updated_df = pd.read_csv(existing_csv)
    updated_count = len(updated_df)
    new_count = updated_count - existing_count
    
    # If we found new links but the script failed (likely timeout), commit the CSV
    if new_count > 0 and result.returncode != 0 and os.environ.get('GITHUB_ACTIONS', 'false').lower() != 'true':
        print(f"\n⚠️  Script exited with code {result.returncode} but found {new_count} new links")
        print("Committing CSV changes before exit...")
        subprocess.run(['git', 'add', 'questionnaire_links.csv'])
        commit_msg = f"""Save {new_count:,} new questionnaire links (scraping interrupted)

- Extracted {new_count:,} new questionnaire links before timeout
- Total questionnaire links: {updated_count:,}
- Scraping was interrupted - will continue in next run

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>"""
        subprocess.run(['git', 'commit', '-m', commit_msg])
        subprocess.run(['git', 'push'])
        print("✅ CSV changes saved for next run")
    
    print(f"\n=== QUESTIONNAIRE EXTRACTION SUMMARY ===")
    print(f"Previous total: {existing_count:,}")
    print(f"Current total: {updated_count:,}")
    print(f"New questionnaires found: {new_count:,}")
    
    # Count scraped files
    raw_dir = Path('raw_questionnaires')
    if raw_dir.exists():
        scraped_files = list(raw_dir.glob('*.txt'))
        print(f"Total scraped questionnaire files: {len(scraped_files):,}")

# Generate analysis data
print("\nGenerating analysis data...")
subprocess.run([sys.executable, 'generate_website_json.py'])

print("\nDone! Open analysis/index.html to view results.")

# Check for untracked questionnaire files
untracked_files = subprocess.run(['git', 'ls-files', '-o', 'raw_questionnaires/'], 
                                capture_output=True, text=True).stdout.strip()
has_untracked_questionnaires = bool(untracked_files)

# Check if there are unscraped questionnaires in the CSV
unique_questionnaires_needed = set()
newly_found_scraped = 0

if existing_csv.exists():
    # Get existing questionnaire URLs from before this run
    existing_urls = set()
    if existing_count > 0:
        for _, row in existing_df.iterrows():
            existing_urls.add(row['questionnaire_url'])
    
    # Track which unique questionnaires we need
    for _, row in updated_df.iterrows():
        url = row['questionnaire_url']
        is_new = url not in existing_urls
        
        # Get the file path for this questionnaire
        txt_path = get_questionnaire_filepath(url)
        unique_questionnaires_needed.add(str(txt_path))
        if is_new and txt_path.exists():
            newly_found_scraped += 1
    
    # Count how many unique questionnaires we're missing
    unscraped_count = sum(1 for path in unique_questionnaires_needed if not Path(path).exists())

print(f"Unique questionnaires still needed: {unscraped_count:,}")
print(f"Newly found links that were scraped: {newly_found_scraped:,}")

# Git operations
if new_count > 0 or has_untracked_questionnaires or unscraped_count > 0:
    # Skip git operations when running in GitHub Actions
    if os.environ.get('GITHUB_ACTIONS', 'false').lower() == 'true':
        print("\n=== GIT OPERATIONS ===")
        print("Running in GitHub Actions - skipping git operations")
    else:
        print("\n=== GIT OPERATIONS ===")
        print("Adding questionnaires folder to git...")
        
        # Add all changes in questionnaires folder
        subprocess.run(['git', 'add', '.'])
        
        # Create commit message
        new_files_count = len(untracked_files.splitlines()) if untracked_files else 0
        
        # We no longer track failed scrapes in a file
        failed_scrapes = 0
        
        if new_count > 0:
            commit_message = f"""Update questionnaires: {new_count:,} new links found, {newly_found_scraped:,} scraped

- Extracted {new_count:,} new questionnaire links
- Scraped {newly_found_scraped:,} questionnaire files  
- Failed to scrape: {failed_scrapes} files
- Total questionnaire links: {updated_count:,}
- Total scraped files: {len(scraped_files):,}
- Unique questionnaires still needed: {unscraped_count:,}

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>"""
        elif new_files_count > 0:
            commit_message = f"""Update questionnaires: scraped {new_files_count} previously unscraped files

- No new questionnaire links found
- Scraped {new_files_count} previously unscraped questionnaires
- Failed to scrape: {failed_scrapes} files
- Total questionnaire links: {updated_count:,}
- Total scraped files: {len(scraped_files):,}
- Unique questionnaires still needed: {unscraped_count:,}

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>"""
        else:
            # This shouldn't happen now, but just in case
            commit_message = f"""Update questionnaires: processing unscraped files

- Failed to scrape: {failed_scrapes} files
- Total questionnaire links: {updated_count:,}
- Total scraped files: {len(scraped_files):,}
- Unique questionnaires still needed: {unscraped_count:,}

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