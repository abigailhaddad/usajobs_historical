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
import hashlib
import requests
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from threading import Lock, Event
import threading
from questionnaire_utils import (
    transform_monster_url, extract_questionnaire_id, get_questionnaire_filename,
    get_questionnaire_filepath, questionnaire_exists, RAW_QUESTIONNAIRES_DIR
)

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
    has_monster_link = False
    
    # Convert the job row to string to search everywhere
    job_str = str(job_row.to_dict())
    
    # Extract fields from MatchedObjectDescriptor
    occupation_series = None
    occupation_name = None
    position_location = None
    grade_code = None
    position_schedule = None
    service_type = None
    low_grade = None
    high_grade = None
    
    if pd.notna(job_row.get('MatchedObjectDescriptor')):
        try:
            mod = json.loads(job_row['MatchedObjectDescriptor'])
            job_str += json.dumps(mod)
            
            # Get occupation series code and name
            if 'JobCategory' in mod and isinstance(mod['JobCategory'], list) and len(mod['JobCategory']) > 0:
                occupation_series = mod['JobCategory'][0].get('Code')
                occupation_name = mod['JobCategory'][0].get('Name')
            
            # Get location
            if 'PositionLocation' in mod and isinstance(mod['PositionLocation'], list) and len(mod['PositionLocation']) > 0:
                # Take first location
                loc = mod['PositionLocation'][0]
                city = loc.get('CityName', '')
                state = loc.get('CountrySubDivisionCode', '')
                
                # Handle DC special case where city already includes state
                if city and state:
                    if state.lower() in city.lower():
                        # State is already in city name, just use city
                        position_location = city
                    else:
                        # Normal case: combine city and state
                        position_location = f"{city}, {state}"
                elif city:
                    position_location = city
                elif state:
                    position_location = state
                else:
                    position_location = None
            
            # Get grade code - for current jobs API, use top-level min/max grades
            # These will be available from the row, not from MatchedObjectDescriptor
            
            # Get position schedule
            if 'PositionSchedule' in mod and isinstance(mod['PositionSchedule'], list) and len(mod['PositionSchedule']) > 0:
                position_schedule = mod['PositionSchedule'][0].get('Name')
            
            # Specifically check known fields
            if 'UserArea' in mod and 'Details' in mod['UserArea']:
                details = mod['UserArea']['Details']
                
                # Get service type from UserArea.Details.ServiceType
                service_type_code = details.get('ServiceType')
                if service_type_code:
                    service_type_map = {
                        '01': 'Competitive',
                        '02': 'Excepted', 
                        '03': 'Senior Executive'
                    }
                    service_type = service_type_map.get(service_type_code, service_type_code)
                
                # Get numeric grade levels from UserArea.Details
                low_grade = details.get('LowGrade')
                high_grade = details.get('HighGrade')
                pay_plan = details.get('PayPlan')
                
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
        r'https://jobs\.monstergovt\.com/[^/]+/(?:nga/)?ros/rosDashboard\.hms\?[^"\'\s<>]+',
        r'https://jobs\.monstergovt\.com/[^/]+/rospost/\?[^"\'\s<>]+'
    ]
    
    for pattern in monster_patterns:
        monster_matches = re.findall(pattern, job_str)
        for match in monster_matches:
            if match not in links:
                links.append(match)
                has_monster_link = True
    
    return links, occupation_series, occupation_name, position_location, grade_code, position_schedule, service_type, low_grade, high_grade, has_monster_link


def scrape_questionnaire(url, output_dir, timeout_seconds=60, headless=True, session_file=None):
    """Scrape a single questionnaire with timeout and error handling"""
    
    # Transform Monster URLs to preview format
    url = transform_monster_url(url)
    if 'monstergovt.com' in url and '/vacancy/previewVacancyQuestions.hms' in url:
        print(f"  Transformed Monster URL to preview: {url}")
    
    # Get filename for this questionnaire
    txt_path = os.path.join(output_dir, get_questionnaire_filename(url))
    
    # Check if already scraped
    if os.path.exists(txt_path):
        print(f"  Already scraped: {txt_path}")
        with open(txt_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    start_time = time.time()
    
    # Use requests for Monster preview URLs (they don't require JavaScript)
    if 'monstergovt.com' in url and '/vacancy/previewVacancyQuestions.hms' in url:
        try:
            print(f"  Scraping Monster preview URL with requests...")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=timeout_seconds)
            
            if response.status_code == 200:
                # Extract text from HTML
                content = response.text
                
                # Remove HTML tags and clean up text
                text_content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL)
                text_content = re.sub(r'<style[^>]*>.*?</style>', '', text_content, flags=re.DOTALL)
                text_content = re.sub(r'<[^>]+>', ' ', text_content)
                text_content = ' '.join(text_content.split())
                
                # Content validation - check for error pages
                error_indicators = [
                    "We're sorry, we encountered an unexpected error",
                    "404 - Page Not Found",
                    "404 Error",
                    "Error 404",
                    "HTTP 404",
                    "Page not found",
                    "This page cannot be displayed",
                    "Access Denied",
                    "Forbidden",
                    "The page you requested is unavailable"
                ]
                
                content_lower = text_content.lower()
                for error_text in error_indicators:
                    if error_text.lower() in content_lower:
                        print(f"    ❌ ERROR PAGE DETECTED: Contains '{error_text}'")
                        return None  # Return None to trigger retry
                
                # Check if content is too small (likely an error)
                if len(text_content) < 1000:
                    print(f"    ⚠️  Content too small ({len(text_content)} chars) - likely an error")
                    return None  # Return None to trigger retry
                
                # Save text file only if content passes validation
                with open(txt_path, 'w', encoding='utf-8') as f:
                    f.write(text_content)
                
                elapsed = time.time() - start_time
                print(f"    Saved: {txt_path} ({elapsed:.1f}s)")
                return text_content
            else:
                print(f"    ❌ Monster preview returned status {response.status_code}")
                return None
                
        except requests.exceptions.Timeout:
            elapsed = time.time() - start_time
            print(f"    ⏱️  Timeout after {elapsed:.1f}s: {url}")
            return None
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"    ❌ Error after {elapsed:.1f}s: {str(e)[:80]}")
            return None
    
    # Use Playwright for USAStaffing URLs
    try:
        with sync_playwright() as p:
            # Connect to existing browser if running in non-headless mode
            if not headless:
                try:
                    # Try to connect to existing Chrome instance
                    browser = p.chromium.connect_over_cdp("http://localhost:9222")
                    print(f"    Connected to existing Chrome browser")
                except Exception as e:
                    print(f"    Could not connect to existing Chrome. Make sure Chrome is running with:")
                    print(f"    /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome --remote-debugging-port=9222")
                    print(f"    Falling back to launching new browser...")
                    browser = p.chromium.launch(
                        headless=False,
                        args=[
                            '--disable-blink-features=AutomationControlled',
                            '--disable-dev-shm-usage',
                            '--no-sandbox',
                            '--disable-web-security',
                            '--disable-features=IsolateOrigins,site-per-process'
                        ]
                    )
            else:
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
                # When connecting to existing browser, use existing context
                if not headless and 'connect_over_cdp' in str(type(browser)):
                    # Get existing browser contexts
                    contexts = browser.contexts
                    if contexts:
                        # Use the first existing context (your logged-in session)
                        context = contexts[0]
                        print(f"    Using existing browser context with {len(context.pages)} open pages")
                    else:
                        # Create new context if none exist
                        context = browser.new_context()
                else:
                    # Create new context for headless or new browser
                    context_options = {
                        'viewport': {'width': 1920, 'height': 1080},
                        'ignore_https_errors': True,
                        'java_script_enabled': True,
                        'bypass_csp': True,
                        'extra_http_headers': {
                            'Accept-Language': 'en-US,en;q=0.9'
                        }
                    }
                    
                    # Use saved session if provided
                    if session_file and os.path.exists(session_file):
                        context_options['storage_state'] = session_file
                        print(f"    Using saved session from {session_file}")
                    
                    context = browser.new_context(**context_options)
                
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
                
                # Check if we got the login/authentication page by content
                content_hash = hashlib.md5(page_text.encode('utf-8')).hexdigest()
                
                # Known bad hash for the login/terms page
                if content_hash == 'e3b45ca2cbef98b3604cb4ab6a536c56' or "An official website of the United States government Here's how you know" in page_text:
                    print(f"    ❌ Detected login/authentication page (MD5: {content_hash})")
                    browser.close()
                    return None
                
                # Content validation - check for error pages
                error_indicators = [
                    "We're sorry, we encountered an unexpected error",
                    "404 - Page Not Found",
                    "404 Error",
                    "Error 404",
                    "HTTP 404",
                    "Page not found",
                    "This page cannot be displayed",
                    "Access Denied",
                    "Forbidden",
                    "The page you requested is unavailable"
                ]
                
                content_lower = page_text.lower()
                for error_text in error_indicators:
                    if error_text.lower() in content_lower:
                        print(f"    ❌ ERROR PAGE DETECTED: Contains '{error_text}'")
                        browser.close()
                        return None  # Return None to trigger retry
                
                # Check if content is too small (likely an error)
                if len(page_text) < 1000:
                    print(f"    ⚠️  Content too small ({len(page_text)} chars) - likely an error")
                    browser.close()
                    return None  # Return None to trigger retry
                
                # Save text file only if content passes validation
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
    
    questionnaire, output_dir, index, total, headless, session_file = args
    
    # Check if shutdown requested
    if shutdown_event.is_set():
        return questionnaire, False
    
    with progress_lock:
        print(f"\n[{index}/{total}] {questionnaire['position_title']}")
    
    # Try scraping with retry logic
    max_retries = 2
    retry_count = 0
    questionnaire_text = None
    
    while retry_count < max_retries and questionnaire_text is None:
        if retry_count > 0:
            print(f"    🔄 Retry attempt {retry_count}/{max_retries-1}")
            time.sleep(2)  # Wait before retry
        
        # Use a hard timeout via threading
        result = [None]
        exception = [None]
        
        def scrape_with_timeout():
            try:
                result[0] = scrape_questionnaire(
                    questionnaire['questionnaire_url'], 
                    output_dir,
                    timeout_seconds=60,  # 60 second timeout per questionnaire
                    headless=headless,
                    session_file=session_file
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
        retry_count += 1
    
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
    
    # Find all job parquet files - both current and historical
    current_job_files = sorted(Path(data_dir).glob('current_jobs_*.parquet'))
    historical_job_files = sorted(Path(data_dir).glob('historical_jobs_*.parquet'))
    all_job_files = current_job_files + historical_job_files
    
    print(f"\nFound {len(current_job_files)} current job parquet files")
    print(f"Found {len(historical_job_files)} historical job parquet files")
    print(f"Total: {len(all_job_files)} parquet files to check")
    print(f"Filtering for jobs posted on or after {cutoff_date}")
    
    # Process all files and collect links
    all_new_links = []
    batch_size = 100  # Write to CSV every 100 new links
    cutoff_dt = pd.to_datetime(cutoff_date)
    
    # Track Monster statistics
    total_jobs_processed = 0
    jobs_with_monster_links = 0
    
    # Track processed job control numbers to avoid duplicates
    processed_jobs = set()
    duplicate_jobs_count = 0
    
    for parquet_file in all_job_files:
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
            
            # Skip if we've already processed this job
            control_number = row.get('usajobsControlNumber')
            if control_number in processed_jobs:
                duplicate_jobs_count += 1
                continue
            processed_jobs.add(control_number)
            
            # Extract links and other fields from this job
            links, occupation_series, occupation_name, position_location, grade_code, position_schedule, service_type, low_grade, high_grade, has_monster_link = extract_questionnaire_links_from_job(row)
            
            # Track Monster links
            if has_monster_link:
                jobs_with_monster_links += 1
            
            if links:
                jobs_with_links += 1
                
                for link in links:
                    # Only add if we haven't seen this URL before
                    if link not in existing_urls:
                        existing_urls.add(link)
                        new_links_in_file += 1
                        
                        # Construct grade code from pay plan and numeric grades
                        # First get the pay plan from top level (e.g., GS, WG)
                        pay_scale = row.get('minimumGrade', '')
                        
                        # Use numeric grades from extraction if available
                        if pay_scale and low_grade and high_grade:
                            if low_grade == high_grade:
                                grade_code = f"{pay_scale}-{low_grade}"
                            else:
                                grade_code = f"{pay_scale}-{low_grade}/{high_grade}"
                        elif pay_scale:
                            # Fallback to just pay scale if no numeric grades
                            grade_code = pay_scale
                        else:
                            grade_code = None
                        
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
                            'position_location': position_location,  # From MatchedObjectDescriptor
                            'grade_code': grade_code,  # From top-level min/max grades
                            'position_schedule': position_schedule,  # From MatchedObjectDescriptor
                            'service_type': service_type,  # From MatchedObjectDescriptor
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
        
        total_jobs_processed += len(df)
    
    # Write any remaining links
    if all_new_links:
        print(f"\nWriting final batch of {len(all_new_links)} links")
        final_df = pd.DataFrame(all_new_links)
        if csv_file.exists():
            final_df.to_csv(csv_file, mode='a', header=False, index=False)
        else:
            final_df.to_csv(csv_file, index=False)
    
    # Print Monster statistics (don't save to file)
    percentage_with_monster = (jobs_with_monster_links / total_jobs_processed * 100) if total_jobs_processed > 0 else 0
    
    print(f"\n\nProcessing Summary:")
    print(f"  Total unique jobs processed: {len(processed_jobs):,}")
    print(f"  Duplicate jobs skipped: {duplicate_jobs_count:,}")
    print(f"  Jobs with Monster links: {jobs_with_monster_links:,}")
    print(f"  Percentage with Monster links: {percentage_with_monster:.2f}%")
    
    return csv_file


def save_progress_and_exit(completed_questionnaires, start_time):
    """Save progress and exit gracefully"""
    print("\n" + "="*60)
    print("GRACEFUL SHUTDOWN - SAVING PROGRESS")
    print("="*60)
    
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
    
    # Check if running in GitHub Actions
    is_github_actions = os.environ.get('GITHUB_ACTIONS', 'false').lower() == 'true'
    
    # Check for command-line arguments
    limit = None
    max_workers = 3  # Reduced default for stability
    skip_extract = False
    max_scrape_time = 180 if is_github_actions else None  # No time limit when running locally
    
    # Simple argument parsing
    headless = True
    session_file = "usajobs_session.json" if os.path.exists("usajobs_session.json") else None
    if session_file:
        print(f"Found saved session file: {session_file}")
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == '--skip-extract':
            skip_extract = True
            print("Skipping link extraction, using existing CSV")
        elif args[i] == '--no-headless':
            headless = False
            max_workers = 1  # Force single worker for manual login
            print("Running in visible browser mode (for manual login)")
            print("Forcing single worker mode for manual login")
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
                print("Usage: python extract_questionnaires.py [limit] [--skip-extract] [--no-headless] [--workers N] [--max-time MINUTES]")
                sys.exit(1)
        i += 1
    
    # Step 1: Extract all links to CSV
    if not skip_extract:
        print("="*60)
        print("STEP 1: Extracting questionnaire links from parquet files")
        print("="*60)
        csv_file = extract_all_links_to_csv(data_dir)
        
        # If running in GitHub Actions, ensure CSV is saved
        if is_github_actions:
            print("\nRunning in GitHub Actions - ensuring CSV is saved...")
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
        # Transform Monster URLs for checking
        url = transform_monster_url(url)
        
        # Check if file exists
        txt_path = get_questionnaire_filepath(url)
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
    if max_scrape_time:
        print(f"Maximum time limit: {max_scrape_time} minutes")
    else:
        print("No time limit (running locally)")
    print("Press Ctrl+C to stop gracefully and save progress\n")
    
    start_time = time.time()
    max_seconds = max_scrape_time * 60 if max_scrape_time else float('inf')
    
    # Prepare arguments for workers
    worker_args = [(q[0], output_dir, q[1], len(df), headless, session_file) for q in to_scrape]
    
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
                    # Check if we've exceeded time limit (only if max_scrape_time is set)
                    elapsed = time.time() - start_time
                    if max_scrape_time and elapsed > max_seconds:
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
    
    if failed_count > 0:
        print(f"\n⚠️  {failed_count} questionnaires failed to scrape")
    print(f"Average rate: {completed_count/total_time:.2f} questionnaires/second")
    
    print(f"\nRaw text files saved in: {output_dir}")
    
    # Clean up caffeinate
    if caffeinate_process:
        caffeinate_process.terminate()
        print("\nStopped caffeinate")


if __name__ == "__main__":
    main()