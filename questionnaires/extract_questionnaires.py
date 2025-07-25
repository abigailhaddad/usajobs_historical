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
import subprocess
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("Playwright not installed. Install with: pip install playwright && playwright install chromium")
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
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process'
            ]
        )
        
        try:
            # Create a new page with optimizations
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                ignore_https_errors=True,
                java_script_enabled=True,
                bypass_csp=True,
                extra_http_headers={
                    'Accept-Language': 'en-US,en;q=0.9'
                }
            )
            
            page = context.new_page()
            
            # Block images and other unnecessary resources
            def route_handler(route):
                if route.request.resource_type in ["image", "stylesheet", "media", "font"]:
                    route.abort()
                else:
                    route.continue_()
            
            page.route("**/*", route_handler)
            
            print(f"  Scraping {url}...")
            
            # Navigate with timeout
            page.goto(url, timeout=10000, wait_until='domcontentloaded')
            
            # Wait for questionnaire content
            try:
                # Try common selectors
                selectors = [
                    "div.question-text",
                    "div.assessment-question", 
                    "div#questionnaire",
                    ".questionText",
                    "div[id*='question']",
                    "div[class*='question']",
                    ".questionnaire-content",
                    "#assessment-questions"
                ]
                
                for selector in selectors:
                    try:
                        page.wait_for_selector(selector, timeout=5000)
                        break
                    except:
                        continue
                
                # Additional wait for dynamic content
                page.wait_for_timeout(1000)
                
            except:
                # If no specific selector found, just wait a bit
                page.wait_for_timeout(2000)
            
            # Get text content
            page_text = page.inner_text('body')
            
            # Save text file
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(page_text)
            
            print(f"    Saved: {txt_path}")
            
            browser.close()
            return page_text
            
        except Exception as e:
            print(f"    Error scraping {url}: {e}")
            browser.close()
            return None


def find_questionnaire_links(data_dir='../data', limit=None):
    """Find all questionnaire links in current job parquet files"""
    # Find all current job parquet files
    current_job_files = sorted(Path(data_dir).glob('current_jobs_*.parquet'))
    print(f"Found {len(current_job_files)} current job parquet files")
    
    if not current_job_files:
        print(f"No current_jobs_*.parquet files found in {data_dir}")
        print(f"Looking for any parquet files...")
        all_parquet_files = sorted(Path(data_dir).glob('*.parquet'))
        if all_parquet_files:
            print(f"Found these parquet files: {[f.name for f in all_parquet_files[:5]]}")
        return []
    
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


def extract_all_links_to_csv(data_dir='../data', cutoff_date='2025-06-01'):
    """Extract all questionnaire links to CSV - fast operation"""
    csv_file = Path('./questionnaire_links.csv')
    
    # Check if we already have a CSV file
    existing_urls = set()
    if csv_file.exists():
        print(f"Found existing {csv_file.name}")
        existing_df = pd.read_csv(csv_file)
        existing_urls = set(existing_df['questionnaire_url'].values)
        print(f"  Contains {len(existing_urls)} unique URLs")
    
    # Find all current job parquet files
    current_job_files = sorted(Path(data_dir).glob('current_jobs_*.parquet'))
    print(f"\nFound {len(current_job_files)} current job parquet files to check")
    print(f"Filtering for jobs posted on or after {cutoff_date}")
    
    # Process all files and collect links
    all_new_links = []
    batch_size = 100  # Write to CSV every 100 new links
    cutoff_dt = pd.to_datetime(cutoff_date)
    
    for parquet_file in current_job_files:
        file_size_mb = parquet_file.stat().st_size / (1024 * 1024)
        print(f"\nProcessing {parquet_file.name} ({file_size_mb:.1f} MB)...")
        
        # Read parquet file
        df = pd.read_parquet(parquet_file)
        total_jobs = len(df)
        
        # Filter by date if positionOpenDate exists
        if 'positionOpenDate' in df.columns:
            df['positionOpenDate'] = pd.to_datetime(df['positionOpenDate'])
            df_filtered = df[df['positionOpenDate'] >= cutoff_dt]
            print(f"  {total_jobs} total jobs, {len(df_filtered)} after date filter")
            df = df_filtered
        else:
            print(f"  {total_jobs} jobs in file (no date filtering - positionOpenDate not found)")
        
        jobs_with_links = 0
        new_links_in_file = 0
        
        # Process each job
        for idx, row in df.iterrows():
            if idx % 1000 == 0:
                print(f"  Processing job {idx}/{len(df)} ({jobs_with_links} with links, {new_links_in_file} new)...", end='\r')
            
            # Extract links from this job
            links = extract_questionnaire_links_from_job(row)
            
            if links:
                jobs_with_links += 1
                
                for link in links:
                    # Only add if we haven't seen this URL before
                    if link not in existing_urls:
                        existing_urls.add(link)
                        new_links_in_file += 1
                        
                        # Create record with all needed fields
                        link_record = {
                            'questionnaire_url': link,
                            'usajobs_control_number': row.get('usajobsControlNumber'),
                            'position_title': row.get('positionTitle'),
                            'announcement_number': row.get('announcementNumber'),
                            'hiring_agency': row.get('hiringAgencyName'),
                            'position_open_date': row.get('positionOpenDate'),
                            'position_close_date': row.get('positionCloseDate'),
                            'position_location': row.get('positionLocation'),
                            'grade_code': row.get('gradeCode'),
                            'position_schedule': row.get('positionSchedule'),
                            'extracted_from_file': parquet_file.name,
                            'extracted_date': datetime.now().isoformat()
                        }
                        all_new_links.append(link_record)
                        
                        # Write batch if we've collected enough
                        if len(all_new_links) >= batch_size:
                            batch_df = pd.DataFrame(all_new_links)
                            if csv_file.exists():
                                batch_df.to_csv(csv_file, mode='a', header=False, index=False)
                            else:
                                batch_df.to_csv(csv_file, index=False)
                            print(f"\n  Wrote batch of {len(all_new_links)} links to CSV")
                            all_new_links = []  # Clear the batch
        
        print(f"\n  Found {jobs_with_links} jobs with questionnaire links")
        print(f"  {new_links_in_file} new unique URLs from this file")
    
    # Write any remaining links
    if all_new_links:
        print(f"\nWriting final batch of {len(all_new_links)} links")
        final_df = pd.DataFrame(all_new_links)
        if csv_file.exists():
            final_df.to_csv(csv_file, mode='a', header=False, index=False)
        else:
            final_df.to_csv(csv_file, index=False)
    
    return csv_file


def main():
    """Main function"""
    data_dir = '../data'  # Adjust path as needed
    output_dir = './raw_questionnaires'
    
    # Start caffeinate to prevent sleep
    caffeinate_process = None
    if sys.platform == 'darwin':  # macOS
        try:
            caffeinate_process = subprocess.Popen(['caffeinate'])
            print("Started caffeinate to prevent system sleep")
        except:
            print("Could not start caffeinate - system may sleep during long operations")
    
    # Check for command-line arguments
    limit = None
    max_workers = 5  # Default number of concurrent scrapers
    skip_extract = False
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--skip-extract':
            skip_extract = True
            print("Skipping link extraction, using existing CSV")
        else:
            try:
                limit = int(sys.argv[1])
                print(f"Limiting to {limit} questionnaires")
            except ValueError:
                print(f"Invalid limit argument: {sys.argv[1]}")
                sys.exit(1)
    
    if len(sys.argv) > 2 and not skip_extract:
        try:
            max_workers = int(sys.argv[2])
            print(f"Using {max_workers} concurrent workers")
        except ValueError:
            print(f"Invalid workers argument: {sys.argv[2]}")
            sys.exit(1)
    elif skip_extract and len(sys.argv) > 2:
        try:
            limit = int(sys.argv[2])
            print(f"Limiting to {limit} questionnaires")
        except ValueError:
            pass
        
        if len(sys.argv) > 3:
            try:
                max_workers = int(sys.argv[3])
                print(f"Using {max_workers} concurrent workers")
            except ValueError:
                pass
    
    # Step 1: Extract all links to CSV (unless skipped)
    if not skip_extract:
        print("="*60)
        print("STEP 1: Extracting questionnaire links from parquet files")
        print("="*60)
        csv_file = extract_all_links_to_csv(data_dir)
    else:
        csv_file = Path('./questionnaire_links.csv')
        if not csv_file.exists():
            print(f"Error: {csv_file} not found! Run without --skip-extract first")
            sys.exit(1)
    
    # Step 2: Read CSV and scrape questionnaires
    print("\n" + "="*60)
    print("STEP 2: Scraping questionnaires from CSV")
    print("="*60)
    
    # Read the CSV
    df = pd.read_csv(csv_file)
    
    # Sort by extracted_date descending (most recent first)
    if 'extracted_date' in df.columns:
        df['extracted_date'] = pd.to_datetime(df['extracted_date'])
        df = df.sort_values('extracted_date', ascending=False)
        print("Sorted questionnaires by date (most recent first)")
    
    total_links = len(df)
    print(f"\nTotal questionnaire links in CSV: {total_links}")
    
    # Apply limit if specified
    if limit:
        df = df.iloc[:limit]
        print(f"Limited to {len(df)} questionnaires")
    
    # Show first few examples
    if len(df) > 0:
        print("\nFirst few questionnaire links to process:")
        for i, (_, row) in enumerate(df.head().iterrows()):
            print(f"\n{i+1}. {row['position_title']}")
            print(f"   Agency: {row['hiring_agency']}")
            print(f"   From: {row.get('extracted_from_file', 'Unknown')}")
            print(f"   URL: {row['questionnaire_url']}")
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Check how many already scraped
    already_scraped = 0
    to_scrape = []
    
    for idx, (_, row) in enumerate(df.iterrows()):
        url = row['questionnaire_url']
        # Check if file exists
        if 'usastaffing.gov' in url:
            match = re.search(r'ViewQuestionnaire/(\d+)', url)
            file_id = match.group(1) if match else 'unknown'
            prefix = 'usastaffing'
        elif 'monstergovt.com' in url:
            match = re.search(r'jnum=(\d+)', url)
            if not match:
                match = re.search(r'J=(\d+)', url)
            file_id = match.group(1) if match else 'unknown'
            prefix = 'monster'
        else:
            file_id = str(hash(url))[:8]
            prefix = 'other'
        
        txt_path = os.path.join(output_dir, f'{prefix}_{file_id}.txt')
        if os.path.exists(txt_path):
            already_scraped += 1
        else:
            to_scrape.append((row.to_dict(), idx + 1))
    
    print(f"\n{already_scraped} questionnaires already scraped")
    print(f"{len(to_scrape)} questionnaires to scrape")
    
    if not to_scrape:
        print("\nNothing to scrape!")
        return
    
    # Scrape questionnaires concurrently
    print(f"\nScraping questionnaires using {max_workers} workers...")
    success_count = 0
    failed_count = 0
    start_time = time.time()
    
    # Prepare arguments for workers
    worker_args = [(q[0], output_dir, q[1], len(df)) for q in to_scrape]
    
    # Use ThreadPoolExecutor for concurrent scraping
    completed_questionnaires = []
    completed_count = 0
    
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
                else:
                    failed_count += 1
                completed_count += 1
                
                # Progress and time estimate
                elapsed = time.time() - start_time
                rate = completed_count / elapsed
                remaining = len(to_scrape) - completed_count
                eta_seconds = remaining / rate if rate > 0 else 0
                eta_minutes = eta_seconds / 60
                
                print(f"\nProgress: {completed_count}/{len(to_scrape)} "
                      f"({completed_count/len(to_scrape)*100:.1f}%) | "
                      f"Success: {success_count} | Failed: {failed_count} | "
                      f"Rate: {rate:.1f}/sec | ETA: {eta_minutes:.1f} min")
                
            except Exception as e:
                print(f"Error in worker: {e}")
                questionnaire = future_to_questionnaire[future]
                questionnaire['questionnaire_text'] = None
                questionnaire['scrape_status'] = 'failed'
                completed_questionnaires.append(questionnaire)
                failed_count += 1
                completed_count += 1
    
    total_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"SCRAPING COMPLETE")
    print(f"{'='*60}")
    print(f"Completed: {success_count}/{len(to_scrape)} questionnaires scraped successfully")
    print(f"Failed: {failed_count}")
    print(f"Total scraped files: {already_scraped + success_count}")
    print(f"Total time: {total_time/60:.1f} minutes")
    print(f"Average rate: {completed_count/total_time:.2f} questionnaires/second")
    
    print(f"\nRaw text files saved in: {output_dir}")
    
    # Clean up caffeinate
    if caffeinate_process:
        caffeinate_process.terminate()
        print("\nStopped caffeinate")


if __name__ == "__main__":
    main()