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

# Always check for CSV updates, even if the script failed
if existing_csv.exists():
    updated_df = pd.read_csv(existing_csv)
    updated_count = len(updated_df)
    new_count = updated_count - existing_count
    
    # If we found new links but the script failed (likely timeout), commit the CSV
    if new_count > 0 and result.returncode != 0:
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
os.chdir('analysis')
subprocess.run([sys.executable, 'generate_analysis_data.py'])
os.chdir('..')

print("\nDone! Open analysis/index.html to view results.")

# Check for untracked questionnaire files
untracked_files = subprocess.run(['git', 'ls-files', '-o', 'raw_questionnaires/'], 
                                capture_output=True, text=True).stdout.strip()
has_untracked_questionnaires = bool(untracked_files)

# Check if there are unscraped questionnaires in the CSV
unscraped_count = 0
newly_found_scraped = 0
if existing_csv.exists():
    import re
    
    # Get existing questionnaire IDs from before this run
    existing_ids = set()
    if existing_count > 0:
        for _, row in existing_df.iterrows():
            existing_ids.add(row['questionnaire_url'])
    
    for _, row in updated_df.iterrows():
        url = row['questionnaire_url']
        is_new = url not in existing_ids
        
        # Extract ID from URL for filename
        if 'usastaffing.gov' in url:
            match = re.search(r'ViewQuestionnaire/(\d+)', url)
            file_id = match.group(1) if match else None
            if file_id:
                txt_path = f'raw_questionnaires/usastaffing_{file_id}.txt'
                if not Path(txt_path).exists():
                    unscraped_count += 1
                elif is_new:
                    newly_found_scraped += 1
        elif 'monstergovt.com' in url:
            match = re.search(r'jnum=(\d+)', url)
            if not match:
                match = re.search(r'J=(\d+)', url)
            file_id = match.group(1) if match else None
            if file_id:
                txt_path = f'raw_questionnaires/monster_{file_id}.txt'
                if not Path(txt_path).exists():
                    unscraped_count += 1
                elif is_new:
                    newly_found_scraped += 1

print(f"Unscraped questionnaires in CSV: {unscraped_count:,}")
print(f"Newly found links that were scraped: {newly_found_scraped:,}")

# Git operations
if new_count > 0 or has_untracked_questionnaires or unscraped_count > 0:
    print("\n=== GIT OPERATIONS ===")
    print("Adding questionnaires folder to git...")
    
    # Add all changes in questionnaires folder
    subprocess.run(['git', 'add', '.'])
    
    # Create commit message
    new_files_count = len(untracked_files.splitlines()) if untracked_files else 0
    
    if new_count > 0:
        commit_message = f"""Update questionnaires: {new_count:,} new links found, {newly_found_scraped:,} scraped

- Extracted {new_count:,} new questionnaire links
- Scraped {newly_found_scraped:,} questionnaire files  
- Total questionnaire links: {updated_count:,}
- Total scraped files: {len(scraped_files):,}
- Unscraped questionnaires remaining: {unscraped_count:,}

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>"""
    elif new_files_count > 0:
        commit_message = f"""Update questionnaires: scraped {new_files_count} previously unscraped files

- No new questionnaire links found
- Scraped {new_files_count} previously unscraped questionnaires
- Total questionnaire links: {updated_count:,}
- Total scraped files: {len(scraped_files):,}
- Unscraped questionnaires remaining: {unscraped_count:,}

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>"""
    else:
        # This shouldn't happen now, but just in case
        commit_message = f"""Update questionnaires: processing unscraped files

- Total questionnaire links: {updated_count:,}
- Total scraped files: {len(scraped_files):,}
- Unscraped questionnaires remaining: {unscraped_count:,}

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