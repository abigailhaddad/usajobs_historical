#!/usr/bin/env python3
"""
Worker-based parallel scraper - each worker writes to its own database
No locking issues, clean separation of concerns
"""

import duckdb
import json
import time
import random
import argparse
import signal
import sys
import os
from datetime import datetime
from scrape_job_posting import scrape_job_posting

class WorkerScraper:
    def __init__(self, year, worker_id, total_workers, delay_range=(0.1, 0.5)):
        self.year = year
        self.worker_id = worker_id
        self.total_workers = total_workers
        self.main_db_path = f"../../data/duckdb/usajobs_{year}.duckdb"
        self.worker_db_path = f"../../data/duckdb/usajobs_{year}_worker_{worker_id}.duckdb"
        self.delay_range = delay_range
        self.should_stop = False
        self.stats = {
            'total': 0,
            'scraped': 0,
            'failed': 0
        }
        
        # Set up logging
        os.makedirs('logs', exist_ok=True)
        self.log_file = f"logs/scrape_{year}_worker_{worker_id}.log"
        self.start_time = time.time()
        
        # Set up graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle graceful shutdown"""
        self.log(f"Worker {self.worker_id} received signal {signum}. Shutting down gracefully...")
        self.should_stop = True
    
    def log(self, message):
        """Log message to both file and stdout"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] Worker {self.worker_id}: {message}"
        
        # Write to file
        with open(self.log_file, 'a') as f:
            f.write(log_message + '\n')
        
        # Also print to stdout
        print(log_message)
    
    def setup_worker_database(self):
        """Create worker's database and table"""
        # Remove existing worker database if it exists
        if os.path.exists(self.worker_db_path):
            os.remove(self.worker_db_path)
        
        # Create new database with scraped data table
        conn = duckdb.connect(self.worker_db_path)
        conn.execute("""
            CREATE TABLE scraped_jobs (
                control_number TEXT PRIMARY KEY,
                position_title TEXT,
                job_summary TEXT,
                job_duties TEXT,
                job_qualifications TEXT,
                job_requirements TEXT,
                scraped_at TIMESTAMP,
                scrape_status TEXT
            )
        """)
        conn.close()
        self.log(f"Created worker database: {self.worker_db_path}")
    
    def get_assigned_jobs(self):
        """Get jobs assigned to this worker based on modulo assignment"""
        # Connect to main database in read-only mode
        conn = duckdb.connect(self.main_db_path, read_only=True)
        
        # Get jobs assigned to this worker using ROW_NUMBER for even distribution
        query = f"""
            WITH numbered_jobs AS (
                SELECT control_number, position_title,
                       ROW_NUMBER() OVER (ORDER BY control_number) as rn
                FROM historical_jobs 
                WHERE (scrape_status IS NULL OR scrape_status = 'failed')
            )
            SELECT control_number, position_title
            FROM numbered_jobs
            WHERE (rn - 1) % {self.total_workers} = {self.worker_id - 1}
            ORDER BY control_number
        """
        
        result = conn.execute(query).fetchall()
        conn.close()
        
        my_jobs = [(row[0], row[1]) for row in result]
        return my_jobs
    
    def save_to_worker_db(self, control_number, position_title, scraped_data):
        """Save scraped data to worker's database"""
        conn = duckdb.connect(self.worker_db_path)
        
        try:
            if scraped_data['status'] == 'success':
                sections = scraped_data.get('sections', {})
                
                conn.execute("""
                    INSERT INTO scraped_jobs (
                        control_number, position_title, job_summary, job_duties,
                        job_qualifications, job_requirements, scraped_at, scrape_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    control_number,
                    position_title,
                    sections.get('summary', ''),
                    sections.get('duties', ''),
                    sections.get('qualifications', ''),
                    sections.get('requirements', ''),
                    datetime.now(),
                    'completed'
                ))
            else:
                # Mark as failed
                conn.execute("""
                    INSERT INTO scraped_jobs (
                        control_number, position_title, scraped_at, scrape_status
                    ) VALUES (?, ?, ?, ?)
                """, (control_number, position_title, datetime.now(), 'failed'))
            
            conn.commit()
        finally:
            conn.close()
    
    def run(self):
        """Main worker loop"""
        self.log("Starting worker-based parallel scraper")
        self.log(f"Year: {self.year}")
        self.log(f"Worker: {self.worker_id}/{self.total_workers}")
        self.log(f"Main database: {self.main_db_path}")
        self.log(f"Worker database: {self.worker_db_path}")
        self.log(f"Delay range: {self.delay_range[0]}-{self.delay_range[1]} seconds")
        self.log("-" * 60)
        
        # Set up worker database
        self.setup_worker_database()
        
        # Get jobs assigned to this worker
        my_jobs = self.get_assigned_jobs()
        self.stats['total'] = len(my_jobs)
        self.log(f"Assigned {self.stats['total']} jobs to scrape")
        
        if self.stats['total'] == 0:
            self.log("No jobs to scrape for this worker")
            return
        
        # Process each job
        for idx, (control_number, position_title) in enumerate(my_jobs):
            if self.should_stop:
                self.log("Received stop signal, shutting down...")
                break
            
            # Progress tracking
            self.log(f"[{idx+1}/{self.stats['total']}] Scraping {control_number}: {position_title[:50]}")
            
            try:
                # Scrape the job
                result = scrape_job_posting(control_number)
                
                if result['status'] == 'success':
                    self.stats['scraped'] += 1
                else:
                    self.stats['failed'] += 1
                
                # Save to worker database
                self.save_to_worker_db(control_number, position_title, result)
                
                # Add random delay
                delay = random.uniform(*self.delay_range)
                time.sleep(delay)
                
            except Exception as e:
                self.log(f"Exception scraping {control_number}: {e}")
                self.stats['failed'] += 1
                # Try to save failed status
                try:
                    self.save_to_worker_db(control_number, position_title, {'status': 'failed'})
                except:
                    pass
            
            # Progress report every 25 jobs
            if (idx + 1) % 25 == 0:
                elapsed = time.time() - self.start_time
                rate = (idx + 1) / elapsed
                eta = (self.stats['total'] - (idx + 1)) / rate / 60
                
                self.log(f"Progress: {idx+1}/{self.stats['total']} "
                        f"(Success: {self.stats['scraped']}, Failed: {self.stats['failed']}) "
                        f"Rate: {rate:.2f}/sec, ETA: {eta:.1f}min")
        
        # Final summary
        elapsed = (time.time() - self.start_time) / 60
        self.log("-" * 60)
        self.log(f"Worker {self.worker_id} completed!")
        self.log(f"Total jobs: {self.stats['total']}")
        self.log(f"Successful: {self.stats['scraped']}")
        self.log(f"Failed: {self.stats['failed']}")
        self.log(f"Time elapsed: {elapsed:.1f} minutes")
        self.log(f"Average rate: {(self.stats['scraped'] + self.stats['failed']) / (elapsed * 60):.2f} jobs/sec")
        self.log(f"Results saved to: {self.worker_db_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Worker-based parallel scraper')
    parser.add_argument('year', type=int, help='Year to scrape')
    parser.add_argument('worker_id', type=int, help='Worker ID (1-based)')
    parser.add_argument('total_workers', type=int, help='Total number of workers')
    parser.add_argument('--delay-min', type=float, default=0.1, help='Minimum delay between requests')
    parser.add_argument('--delay-max', type=float, default=0.5, help='Maximum delay between requests')
    
    args = parser.parse_args()
    
    scraper = WorkerScraper(
        args.year, 
        args.worker_id, 
        args.total_workers,
        delay_range=(args.delay_min, args.delay_max)
    )
    
    try:
        scraper.run()
    except KeyboardInterrupt:
        print(f"\nWorker {args.worker_id} interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\nWorker {args.worker_id} crashed: {e}")
        sys.exit(1)