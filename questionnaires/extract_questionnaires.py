#!/usr/bin/env python3
"""
Robust version of extract_questionnaires.py with better error handling and timeouts
"""
import pandas as pd
import json
import re
import time
import os
import sys
import subprocess
import signal
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from threading import Lock, Event
import threading

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
except ImportError:
    print("Playwright not installed. Install with: pip install playwright && playwright install chromium")
    exit(1)

# Global shutdown event
shutdown_event = Event()
progress_lock = Lock()
scraped_count = 0
failed_count = 0

def signal_handler(signum, frame):
    """Handle interrupt signals gracefully"""
    print("\n\n⚠️  Interrupt received! Shutting down gracefully...")
    print("   (This may take a moment while active scrapers finish)")
    shutdown_event.set()

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def extract_questionnaire_links_from_job(job_row):
    """Extract all questionnaire links from a job record"""
    links = []
    
    # Convert the job row to string to search everywhere
    job_str = str(job_row.to_dict())
    
    # Extract occupation series from MatchedObjectDescriptor
    occupation_series = None
    occupation_name = None
    if pd.notna(job_row.get('MatchedObjectDescriptor')):
        try:
            mod = json.loads(job_row['MatchedObjectDescriptor'])
            job_str += json.dumps(mod)
            
            # Get occupation series code and name
            if 'JobCategory' in mod and isinstance(mod['JobCategory'], list) and len(mod['JobCategory']) > 0:
                occupation_series = mod['JobCategory'][0].get('Code')
                occupation_name = mod['JobCategory'][0].get('Name')
            
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
    monster_patterns = [
        r'https://jobs\.monstergovt\.com/[^/]+/vacancy/previewVacancyQuestions\.hms\?[^"\'\s<>]+',
        r'https://jobs\.monstergovt\.com/[^/]+/ros/rosDashboard\.hms\?[^"\'\s<>]+'
    ]
    
    for pattern in monster_patterns:
        monster_matches = re.findall(pattern, job_str)
        for match in monster_matches:
            if match not in links:
                links.append(match)
    
    return links, occupation_series, occupation_name


def scrape_questionnaire(url, output_dir, timeout_seconds=60):
    """Scrape a single questionnaire with timeout and error handling"""
    
    # Extract ID from URL for filename
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
    
    # Check if already scraped
    if os.path.exists(txt_path):
        print(f"  Already scraped: {txt_path}")
        with open(txt_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    start_time = time.time()
    
    try:
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
                # Create context with shorter default timeout
                context = browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    ignore_https_errors=True,
                    java_script_enabled=True,
                    bypass_csp=True,
                    extra_http_headers={
                        'Accept-Language': 'en-US,en;q=0.9'
                    }
                )
                
                # Set default timeout for all operations
                context.set_default_timeout(timeout_seconds * 1000)
                
                page = context.new_page()
                
                # Block unnecessary resources
                def route_handler(route):
                    if route.request.resource_type in ["image", "stylesheet", "media", "font"]:
                        route.abort()
                    else:
                        route.continue_()
                
                page.route("**/*", route_handler)
                
                print(f"  Scraping {url}...")
                
                # Navigate with timeout
                print(f"    [DEBUG] Starting page.goto at {time.strftime('%H:%M:%S')}")
                page.goto(url, timeout=15000, wait_until='domcontentloaded')
                print(f"    [DEBUG] page.goto completed at {time.strftime('%H:%M:%S')}")
                
                # Wait for questionnaire content (shorter timeout)
                try:
                    print(f"    [DEBUG] Waiting for selectors at {time.strftime('%H:%M:%S')}")
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
                            page.wait_for_selector(selector, timeout=3000)
                            print(f"    [DEBUG] Found selector: {selector}")
                            break
                        except:
                            continue
                    
                    # Brief wait for dynamic content
                    print(f"    [DEBUG] Waiting 1s for dynamic content...")
                    page.wait_for_timeout(1000)
                    
                except:
                    # If no selector found, just wait briefly
                    print(f"    [DEBUG] No selector found, waiting 1.5s...")
                    page.wait_for_timeout(1500)
                
                # Get text content with timeout
                print(f"    [DEBUG] Getting page text at {time.strftime('%H:%M:%S')}")
                page_text = page.inner_text('body', timeout=5000)
                print(f"    [DEBUG] Got {len(page_text)} characters at {time.strftime('%H:%M:%S')}")
                
                # Save text file
                with open(txt_path, 'w', encoding='utf-8') as f:
                    f.write(page_text)
                
                elapsed = time.time() - start_time
                print(f"    Saved: {txt_path} ({elapsed:.1f}s)")
                
                browser.close()
                return page_text
                
            except PlaywrightTimeoutError:
                elapsed = time.time() - start_time
                print(f"    ⏱️  Timeout after {elapsed:.1f}s: {url}")
                browser.close()
                return None
            except Exception as e:
                elapsed = time.time() - start_time
                print(f"    ❌ Error after {elapsed:.1f}s: {str(e)[:80]}")
                browser.close()
                return None
                
    except Exception as e:
        print(f"    ❌ Failed to launch browser: {e}")
        return None


def scrape_questionnaire_worker(args):
    """Worker function for concurrent scraping with shutdown check"""
    global scraped_count, failed_count
    
    questionnaire, output_dir, index, total = args
    
    # Check if shutdown requested
    if shutdown_event.is_set():
        return questionnaire, False
    
    with progress_lock:
        print(f"\n[{index}/{total}] {questionnaire['position_title']}")
    
    # Use a hard timeout via threading
    result = [None]
    exception = [None]
    
    def scrape_with_timeout():
        try:
            result[0] = scrape_questionnaire(
                questionnaire['questionnaire_url'], 
                output_dir,
                timeout_seconds=60  # 60 second timeout per questionnaire
            )
        except Exception as e:
            exception[0] = e
    
    thread = threading.Thread(target=scrape_with_timeout)
    thread.daemon = True
    thread.start()
    thread.join(timeout=90)  # 90 second hard timeout
    
    if thread.is_alive():
        print(f"    ⚠️  HARD TIMEOUT: Thread still running after 90 seconds!")
        questionnaire['questionnaire_text'] = None
        questionnaire['scrape_status'] = 'timeout'
        with progress_lock:
            failed_count += 1
        return questionnaire, False
    
    if exception[0]:
        print(f"    ❌ Thread exception: {str(exception[0])[:80]}")
        questionnaire['questionnaire_text'] = None
        questionnaire['scrape_status'] = 'error'
        with progress_lock:
            failed_count += 1
        return questionnaire, False
    
    questionnaire_text = result[0]
    
    if questionnaire_text:
        questionnaire['questionnaire_text'] = questionnaire_text
        questionnaire['scrape_status'] = 'success'
        with progress_lock:
            scraped_count += 1
        return questionnaire, True
    else:
        questionnaire['questionnaire_text'] = None
        questionnaire['scrape_status'] = 'failed'
        with progress_lock:
            failed_count += 1
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
            
            # Extract links and occupation series from this job
            links, occupation_series, occupation_name = extract_questionnaire_links_from_job(row)
            
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
                            'occupation_series': occupation_series,
                            'occupation_name': occupation_name,
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


def save_progress_and_exit(completed_questionnaires, start_time):
    """Save progress and exit gracefully"""
    print("\n" + "="*60)
    print("GRACEFUL SHUTDOWN - SAVING PROGRESS")
    print("="*60)
    
    # Save completed questionnaires to a progress file
    if completed_questionnaires:
        progress_file = f"questionnaire_progress_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(progress_file, 'w') as f:
            json.dump({
                'completed_count': len(completed_questionnaires),
                'completed_urls': [q['questionnaire_url'] for q in completed_questionnaires],
                'timestamp': datetime.now().isoformat()
            }, f, indent=2)
        print(f"✅ Saved progress to {progress_file}")
    
    total_time = time.time() - start_time
    print(f"\n📊 Session Summary:")
    print(f"   Scraped: {scraped_count} questionnaires")
    print(f"   Failed: {failed_count}")
    print(f"   Total time: {total_time/60:.1f} minutes")
    print(f"   Files saved in: ./raw_questionnaires/")
    
    print("\n⚠️  Note: Run the script again to continue from where you left off.")
    print("   Already scraped files will be skipped automatically.")
    
    sys.exit(0)


def main():
    """Main function with robust error handling"""
    global scraped_count, failed_count
    
    data_dir = '../data'
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
    max_workers = 3  # Reduced default for stability
    skip_extract = False
    max_scrape_time = 180  # Maximum minutes to spend scraping
    
    # Simple argument parsing
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == '--skip-extract':
            skip_extract = True
            print("Skipping link extraction, using existing CSV")
        elif args[i] == '--workers' and i + 1 < len(args):
            try:
                max_workers = int(args[i + 1])
                print(f"Using {max_workers} concurrent workers")
                i += 1
            except ValueError:
                print(f"Invalid workers value: {args[i + 1]}")
                sys.exit(1)
        elif args[i] == '--max-time' and i + 1 < len(args):
            try:
                max_scrape_time = int(args[i + 1])
                print(f"Maximum scraping time: {max_scrape_time} minutes")
                i += 1
            except ValueError:
                print(f"Invalid max-time value: {args[i + 1]}")
                sys.exit(1)
        else:
            try:
                limit = int(args[i])
                print(f"Limiting to {limit} questionnaires")
            except ValueError:
                print(f"Unknown argument: {args[i]}")
                print("Usage: python extract_questionnaires_robust.py [limit] [--skip-extract] [--workers N] [--max-time MINUTES]")
                sys.exit(1)
        i += 1
    
    # Step 1: Extract all links to CSV
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
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Check how many already scraped - always process ALL unscraped questionnaires
    already_scraped = 0
    to_scrape = []
    
    print("Checking for unscraped questionnaires (will process ALL unscraped, not just new)...")
    
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
    
    # Scrape questionnaires with timeout
    print(f"\nScraping questionnaires using {max_workers} workers...")
    print(f"Maximum time limit: {max_scrape_time} minutes")
    print("Press Ctrl+C to stop gracefully and save progress\n")
    
    start_time = time.time()
    max_seconds = max_scrape_time * 60
    
    # Prepare arguments for workers
    worker_args = [(q[0], output_dir, q[1], len(df)) for q in to_scrape]
    
    completed_questionnaires = []
    completed_count = 0
    
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_questionnaire = {
                executor.submit(scrape_questionnaire_worker, args): args[0] 
                for args in worker_args
            }
            
            # Process completed futures with timeout check
            for future in as_completed(future_to_questionnaire):
                try:
                    # Check if we've exceeded time limit
                    elapsed = time.time() - start_time
                    if elapsed > max_seconds:
                        print(f"\n⏰ Time limit reached ({max_scrape_time} minutes)")
                        shutdown_event.set()
                        save_progress_and_exit(completed_questionnaires, start_time)
                    
                    # Check if shutdown requested
                    if shutdown_event.is_set():
                        save_progress_and_exit(completed_questionnaires, start_time)
                    
                    # Get result with timeout
                    questionnaire, success = future.result(timeout=60)  # 60 second timeout per future
                    completed_questionnaires.append(questionnaire)
                    completed_count += 1
                    
                    # Progress and time estimate
                    rate = completed_count / elapsed if elapsed > 0 else 0
                    remaining = len(to_scrape) - completed_count
                    eta_seconds = remaining / rate if rate > 0 else 0
                    eta_minutes = eta_seconds / 60
                    
                    with progress_lock:
                        print(f"\nProgress: {completed_count}/{len(to_scrape)} "
                              f"({completed_count/len(to_scrape)*100:.1f}%) | "
                              f"Success: {scraped_count} | Failed: {failed_count} | "
                              f"Rate: {rate:.1f}/sec | ETA: {eta_minutes:.1f} min | "
                              f"Elapsed: {elapsed/60:.1f} min")
                    
                except FuturesTimeoutError:
                    print(f"\n⚠️  Future timed out after 60 seconds")
                    failed_count += 1
                    completed_count += 1
                except Exception as e:
                    print(f"\n❌ Error in worker: {e}")
                    questionnaire = future_to_questionnaire[future]
                    questionnaire['questionnaire_text'] = None
                    questionnaire['scrape_status'] = 'failed'
                    completed_questionnaires.append(questionnaire)
                    failed_count += 1
                    completed_count += 1
                    
    except KeyboardInterrupt:
        print("\n\n⚠️  Keyboard interrupt received!")
        shutdown_event.set()
        save_progress_and_exit(completed_questionnaires, start_time)
    
    # Normal completion
    total_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"SCRAPING COMPLETE")
    print(f"{'='*60}")
    print(f"Completed: {scraped_count}/{len(to_scrape)} questionnaires scraped successfully")
    print(f"Failed: {failed_count}")
    print(f"Total scraped files: {already_scraped + scraped_count}")
    print(f"Total time: {total_time/60:.1f} minutes")
    print(f"Average rate: {completed_count/total_time:.2f} questionnaires/second")
    
    print(f"\nRaw text files saved in: {output_dir}")
    
    # Clean up caffeinate
    if caffeinate_process:
        caffeinate_process.terminate()
        print("\nStopped caffeinate")


if __name__ == "__main__":
    main()