#!/usr/bin/env python3
"""
Extract questionnaire links from USAJobs current job parquet files and fetch questionnaire text
"""
import pandas as pd
import json
import re
import time
import os
import sys
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
except ImportError:
    print("Selenium not installed. Install with: pip install selenium")
    exit(1)


def extract_questionnaire_links_from_job(job_row):
    """Extract all questionnaire links from a job record"""
    links = []
    
    # Convert the job row to string to search everywhere
    job_str = str(job_row.to_dict())
    
    # Also search in MatchedObjectDescriptor if it exists
    if pd.notna(job_row.get('MatchedObjectDescriptor')):
        try:
            mod = json.loads(job_row['MatchedObjectDescriptor'])
            job_str += json.dumps(mod)
            
            # Specifically check known fields
            if 'UserArea' in mod and 'Details' in mod['UserArea']:
                details = mod['UserArea']['Details']
                
                # Check Evaluations field for USAStaffing links
                evaluations = details.get('Evaluations', '')
                if evaluations:
                    job_str += ' ' + evaluations
                
                # Check ApplyOnlineUrl for Monster links
                apply_url = details.get('ApplyOnlineUrl', '')
                if apply_url:
                    job_str += ' ' + apply_url
        except:
            pass
    
    # Look for USAStaffing questionnaire links
    usastaffing_pattern = r'https://apply\.usastaffing\.gov/ViewQuestionnaire/\d+'
    usastaffing_matches = re.findall(usastaffing_pattern, job_str)
    for match in usastaffing_matches:
        if match not in links:
            links.append(match)
    
    # Look for Monster Government questionnaire links
    # Note: The preview questionnaire links might not be in the data, but let's check
    monster_patterns = [
        r'https://jobs\.monstergovt\.com/[^/]+/vacancy/previewVacancyQuestions\.hms\?[^"\'\s<>]+',
        r'https://jobs\.monstergovt\.com/[^/]+/ros/rosDashboard\.hms\?[^"\'\s<>]+'
    ]
    
    for pattern in monster_patterns:
        monster_matches = re.findall(pattern, job_str)
        for match in monster_matches:
            if match not in links:
                links.append(match)
    
    return links


def scrape_questionnaire(url, output_dir):
    """Scrape a single questionnaire and return the text content"""
    
    # Extract ID from URL for filename
    if 'usastaffing.gov' in url:
        match = re.search(r'ViewQuestionnaire/(\d+)', url)
        file_id = match.group(1) if match else 'unknown'
        prefix = 'usastaffing'
    elif 'monstergovt.com' in url:
        # Try different patterns for Monster IDs
        match = re.search(r'jnum=(\d+)', url)
        if not match:
            match = re.search(r'J=(\d+)', url)
        file_id = match.group(1) if match else 'unknown'
        prefix = 'monster'
    else:
        file_id = str(hash(url))[:8]
        prefix = 'other'
    
    txt_path = os.path.join(output_dir, f'{prefix}_{file_id}.txt')
    
    # Check if already scraped
    if os.path.exists(txt_path):
        print(f"  Already scraped: {txt_path}")
        with open(txt_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        print(f"  Scraping {url}...")
        
        driver.get(url)
        
        # Wait for content to load
        wait = WebDriverWait(driver, 30)
        
        # Try common selectors for questionnaires
        selectors = [
            # USAStaffing selectors
            "div.question-text",
            "div.assessment-question",
            "div#questionnaire",
            ".questionText",
            
            # Monster selectors
            "div[id*='question']",
            "div[class*='question']",
            "form[id*='questionnaire']",
            "div[class*='assessment']",
            
            # Generic selectors
            ".questionnaire-content",
            "#assessment-questions",
            "main",
            "body"
        ]
        
        for selector in selectors:
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                break
            except:
                continue
        
        # Extra wait for dynamic content
        time.sleep(2)
        
        # Get text content
        page_text = driver.find_element(By.TAG_NAME, "body").text
        
        # Save text file
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write(page_text)
        
        print(f"    Saved: {txt_path}")
        driver.quit()
        return page_text
        
    except Exception as e:
        print(f"    Error scraping {url}: {e}")
        if 'driver' in locals():
            driver.quit()
        return None


def find_questionnaire_links(data_dir='../data', limit=None):
    """Find all questionnaire links in current job parquet files"""
    # Find all current job parquet files
    current_job_files = sorted(Path(data_dir).glob('current_jobs_*.parquet'))
    print(f"Found {len(current_job_files)} current job parquet files")
    
    all_questionnaire_data = []
    seen_urls = set()
    
    for parquet_file in current_job_files:
        print(f"\nProcessing {parquet_file.name}...")
        df = pd.read_parquet(parquet_file)
        print(f"  {len(df)} jobs in file")
        
        jobs_with_questionnaires = 0
        
        for idx, row in df.iterrows():
            # Extract questionnaire links
            links = extract_questionnaire_links_from_job(row)
            
            if links:
                jobs_with_questionnaires += 1
                
                for link in links:
                    if link not in seen_urls:
                        seen_urls.add(link)
                        
                        # Create questionnaire record
                        questionnaire_data = {
                            'questionnaire_url': link,
                            'usajobs_control_number': row.get('usajobsControlNumber'),
                            'position_title': row.get('positionTitle'),
                            'announcement_number': row.get('announcementNumber'),
                            'hiring_agency': row.get('hiringAgencyName'),
                            'extracted_from_file': parquet_file.name,
                            'extracted_date': datetime.now().isoformat()
                        }
                        
                        all_questionnaire_data.append(questionnaire_data)
                        
                        if limit and len(all_questionnaire_data) >= limit:
                            break
            
            if limit and len(all_questionnaire_data) >= limit:
                break
        
        print(f"  Found {jobs_with_questionnaires} jobs with questionnaire links")
        
        if limit and len(all_questionnaire_data) >= limit:
            break
    
    print(f"\nTotal unique questionnaire URLs found: {len(all_questionnaire_data)}")
    
    # Count by type
    usastaffing_count = sum(1 for q in all_questionnaire_data if 'usastaffing.gov' in q['questionnaire_url'])
    monster_count = sum(1 for q in all_questionnaire_data if 'monstergovt.com' in q['questionnaire_url'])
    print(f"Breakdown: {usastaffing_count} USAStaffing, {monster_count} Monster Government")
    
    return all_questionnaire_data


def scrape_questionnaire_worker(args):
    """Worker function for concurrent scraping"""
    questionnaire, output_dir, index, total = args
    print(f"\n[{index}/{total}] {questionnaire['position_title']}")
    
    questionnaire_text = scrape_questionnaire(questionnaire['questionnaire_url'], output_dir)
    
    if questionnaire_text:
        questionnaire['questionnaire_text'] = questionnaire_text
        questionnaire['scrape_status'] = 'success'
        return questionnaire, True
    else:
        questionnaire['questionnaire_text'] = None
        questionnaire['scrape_status'] = 'failed'
        return questionnaire, False


def main():
    """Main function"""
    data_dir = '../data'  # Adjust path as needed
    output_dir = './raw_questionnaires'
    
    # Check for command-line arguments
    limit = None
    max_workers = 5  # Default number of concurrent scrapers
    
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
            print(f"Limiting to {limit} questionnaires")
        except ValueError:
            print(f"Invalid limit argument: {sys.argv[1]}")
            sys.exit(1)
    
    if len(sys.argv) > 2:
        try:
            max_workers = int(sys.argv[2])
            print(f"Using {max_workers} concurrent workers")
        except ValueError:
            print(f"Invalid workers argument: {sys.argv[2]}")
            sys.exit(1)
    
    # Just find the links for now
    print("Finding questionnaire links...")
    questionnaire_data = find_questionnaire_links(data_dir, limit)
    
    # Show first few examples
    if questionnaire_data:
        print("\nFirst few questionnaire links found:")
        for i, q in enumerate(questionnaire_data[:5]):
            print(f"\n{i+1}. {q['position_title']}")
            print(f"   Agency: {q['hiring_agency']}")
            print(f"   URL: {q['questionnaire_url']}")
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Scrape questionnaires concurrently
    print(f"\nScraping questionnaires using {max_workers} workers...")
    success_count = 0
    
    # Prepare arguments for workers
    worker_args = [(q, output_dir, i+1, len(questionnaire_data)) 
                   for i, q in enumerate(questionnaire_data)]
    
    # Use ThreadPoolExecutor for concurrent scraping
    completed_questionnaires = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_questionnaire = {
            executor.submit(scrape_questionnaire_worker, args): args[0] 
            for args in worker_args
        }
        
        # Process completed futures
        for future in as_completed(future_to_questionnaire):
            try:
                questionnaire, success = future.result()
                completed_questionnaires.append(questionnaire)
                if success:
                    success_count += 1
            except Exception as e:
                print(f"Error in worker: {e}")
                questionnaire = future_to_questionnaire[future]
                questionnaire['questionnaire_text'] = None
                questionnaire['scrape_status'] = 'failed'
                completed_questionnaires.append(questionnaire)
    
    print(f"\nCompleted: {success_count}/{len(questionnaire_data)} questionnaires scraped successfully")
    
    # Create DataFrame with results
    result_df = pd.DataFrame(completed_questionnaires)
    
    # Save as parquet
    output_parquet = './questionnaires_data.parquet'
    result_df.to_parquet(output_parquet, index=False)
    print(f"\nSaved questionnaire data to: {output_parquet}")
    
    # Also save a summary CSV for easy viewing
    summary_columns = ['questionnaire_url', 'position_title', 'hiring_agency', 'scrape_status']
    summary_df = result_df[summary_columns]
    summary_csv = './questionnaires_summary.csv'
    summary_df.to_csv(summary_csv, index=False)
    print(f"Saved summary to: {summary_csv}")
    
    print(f"\nRaw text files saved in: {output_dir}")


if __name__ == "__main__":
    main()