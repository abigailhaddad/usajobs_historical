#!/usr/bin/env python3
"""
Retry scraping questionnaires that failed with error messages
"""
import os
import re
import time
import requests
from pathlib import Path
from datetime import datetime
import pandas as pd

def is_error_file(filepath):
    """Check if a file contains an error message instead of questionnaire content"""
    try:
        # Check file size first - error files are typically under 1KB
        if filepath.stat().st_size >= 1000:
            return False
            
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Check for the specific error pattern
        if "We are not able to display the page requested at this time" in content:
            return True
            
        return False
        
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return False

def extract_url_from_csv(questionnaire_id, source='usastaffing'):
    """Get the questionnaire URL from the CSV file"""
    try:
        df = pd.read_csv('questionnaire_links.csv')
        
        if source == 'usastaffing':
            # Look for USAStaffing URL with this ID
            pattern = f'ViewQuestionnaire/{questionnaire_id}'
            matches = df[df['questionnaire_url'].str.contains(pattern, na=False)]
        else:  # monster
            # Look for Monster URL with this job number
            pattern = f'jnum={questionnaire_id}|J={questionnaire_id}'
            matches = df[df['questionnaire_url'].str.contains(pattern, regex=True, na=False)]
        
        if not matches.empty:
            return matches.iloc[0]['questionnaire_url']
        return None
        
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return None

def scrape_questionnaire(url, questionnaire_id, source='usastaffing'):
    """Attempt to scrape a single questionnaire"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        # For Monster URLs, transform to preview format
        if 'monstergovt.com' in url:
            match = re.search(r'https://jobs\.monstergovt\.com/([^/]+)/ros/rosDashboard\.hms\?O=(\d+)&J=(\d+)', url)
            if match:
                subdomain = match.group(1)
                org_id = match.group(2)
                job_num = match.group(3)
                url = f'https://jobs.monstergovt.com/{subdomain}/vacancy/previewVacancyQuestions.hms?orgId={org_id}&jnum={job_num}'
                print(f"  Transformed Monster URL to: {url}")
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            content = response.text
            
            # Check if we got an error page
            if "We are not able to display the page requested at this time" in content:
                return False, "Error page returned"
            
            # Save the content
            filename = f'raw_questionnaires/{source}_{questionnaire_id}.txt'
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(content)
            
            return True, "Success"
        else:
            return False, f"HTTP {response.status_code}"
            
    except requests.Timeout:
        return False, "Timeout"
    except Exception as e:
        return False, str(e)

def main():
    questionnaire_dir = Path('raw_questionnaires')
    
    if not questionnaire_dir.exists():
        print("raw_questionnaires directory not found!")
        return
    
    # Find all error files
    error_files = []
    for txt_file in questionnaire_dir.glob('*.txt'):
        if is_error_file(txt_file):
            error_files.append(txt_file)
    
    print(f"Found {len(error_files)} error files to retry")
    
    if not error_files:
        print("No error files found!")
        return
    
    # Track results
    success_count = 0
    fail_count = 0
    failed_urls = []
    
    # Process each error file
    for i, error_file in enumerate(error_files, 1):
        # Extract source and ID from filename
        match = re.search(r'(\w+)_(\d+)\.txt', error_file.name)
        if not match:
            print(f"Skipping {error_file.name} - can't parse filename")
            continue
            
        source = match.group(1)
        questionnaire_id = match.group(2)
        
        print(f"\n[{i}/{len(error_files)}] Retrying {source}_{questionnaire_id}")
        
        # Get URL from CSV
        url = extract_url_from_csv(questionnaire_id, source)
        if not url:
            print(f"  Could not find URL in CSV for {questionnaire_id}")
            fail_count += 1
            failed_urls.append({
                'file': error_file.name,
                'reason': 'URL not found in CSV',
                'url': None
            })
            continue
        
        print(f"  URL: {url}")
        
        # Delete the error file first
        error_file.unlink()
        
        # Try to scrape
        success, message = scrape_questionnaire(url, questionnaire_id, source)
        
        if success:
            print(f"  ✓ Successfully scraped")
            success_count += 1
        else:
            print(f"  ✗ Failed: {message}")
            fail_count += 1
            failed_urls.append({
                'file': error_file.name,
                'reason': message,
                'url': url
            })
        
        # Be polite to the server
        time.sleep(2)
    
    # Summary
    print(f"\n{'='*60}")
    print(f"Retry Summary:")
    print(f"  Total error files: {len(error_files)}")
    print(f"  Successfully re-scraped: {success_count}")
    print(f"  Failed: {fail_count}")
    print(f"  Success rate: {(success_count / len(error_files) * 100):.1f}%")
    
    # Save failed URLs for analysis
    if failed_urls:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        failed_file = f'failed_retries_{timestamp}.txt'
        with open(failed_file, 'w') as f:
            f.write(f"Failed to re-scrape {len(failed_urls)} questionnaires\n")
            f.write(f"Timestamp: {datetime.now()}\n\n")
            for item in failed_urls:
                f.write(f"File: {item['file']}\n")
                f.write(f"URL: {item['url']}\n")
                f.write(f"Reason: {item['reason']}\n")
                f.write("-" * 40 + "\n")
        print(f"\nFailed URLs saved to: {failed_file}")

if __name__ == "__main__":
    main()