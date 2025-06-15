#!/usr/bin/env python3
"""
Simplified Parquet-based storage for USAJobs pipeline

Just stores the final unified dataset and temporary overlap data for analysis.
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Dict, Any, Optional

class ParquetJobStorage:
    """
    Simplified storage for USAJobs data - just one unified dataset
    """
    
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.base_path.mkdir(exist_ok=True)
        
        # Single files approach
        self.jobs_file = self.base_path / "usajobs.parquet"
        self.overlap_file = self.base_path / "overlap_samples.parquet"
        
        # Keep track of intermediate data in memory for analysis
        self.current_jobs_data = []
        self.historical_jobs_data = []
    
    def save_current_jobs(self, jobs: List[Dict]):
        """Store current jobs in memory for processing"""
        self.current_jobs_data = jobs
        print(f"📥 Stored {len(jobs)} current jobs in memory")
    
    def save_historical_jobs(self, jobs: List[Dict], batch_id: str = None):
        """Store historical jobs in memory for processing"""
        self.historical_jobs_data = jobs
        print(f"📥 Stored {len(jobs)} historical jobs in memory")
    
    def save_unified_jobs(self, jobs: List[Dict]):
        """Save final unified jobs to single Parquet file"""
        if not jobs:
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(jobs)
        
        # Add timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save to single file with timestamp in filename
        timestamped_file = self.base_path / f"usajobs_{timestamp}.parquet"
        df.to_parquet(timestamped_file, engine='pyarrow')
        
        # Also save as main file (overwrite previous)
        df.to_parquet(self.jobs_file, engine='pyarrow')
        
        print(f"💾 Saved {len(jobs)} unified jobs to {self.jobs_file}")
        print(f"📦 Backup saved to {timestamped_file}")
    
    def save_overlap_samples(self, overlap_data: List[Dict]):
        """Save overlap samples for analysis"""
        if not overlap_data:
            return
        
        df = pd.DataFrame(overlap_data)
        
        # Add timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        timestamped_file = self.base_path / f"overlap_samples_{timestamp}.parquet"
        
        # Save with timestamp
        df.to_parquet(timestamped_file, engine='pyarrow')
        
        # Also save as main file (for analysis)
        df.to_parquet(self.overlap_file, engine='pyarrow')
        
        print(f"📊 Saved {len(overlap_data)} overlap samples")
    
    def load_unified_jobs(self) -> pd.DataFrame:
        """Load the main unified jobs dataset"""
        if self.jobs_file.exists():
            df = pd.read_parquet(self.jobs_file)
            print(f"📖 Loaded {len(df)} unified jobs")
            return df
        return pd.DataFrame()
    
    def load_overlap_samples(self) -> pd.DataFrame:
        """Load overlap samples for analysis"""
        if self.overlap_file.exists():
            df = pd.read_parquet(self.overlap_file)
            print(f"📊 Loaded {len(df)} overlap samples")
            return df
        return pd.DataFrame()
    
    def load_current_jobs(self) -> pd.DataFrame:
        """Load current jobs from memory"""
        if self.current_jobs_data:
            return pd.DataFrame(self.current_jobs_data)
        return pd.DataFrame()
    
    def load_historical_jobs(self) -> pd.DataFrame:
        """Load historical jobs from memory"""
        if self.historical_jobs_data:
            return pd.DataFrame(self.historical_jobs_data)
        return pd.DataFrame()
    
    def get_control_numbers_with_scraping(self) -> set:
        """Get control numbers that already have scraping data"""
        # Check memory first
        control_numbers = set()
        
        for job in self.historical_jobs_data + self.current_jobs_data:
            if job.get('scraped_content'):
                control_num = str(job.get('usajobsControlNumber') or job.get('MatchedObjectId') or job.get('control_number', ''))
                if control_num:
                    control_numbers.add(control_num)
        
        # Also check saved data if exists
        if self.jobs_file.exists():
            df = pd.read_parquet(self.jobs_file)
            for _, row in df.iterrows():
                # Check for various scraping indicators
                has_scraping = (
                    pd.notna(row.get('major_duties')) or
                    pd.notna(row.get('qualification_summary')) or
                    pd.notna(row.get('education')) or
                    pd.notna(row.get('benefits'))
                )
                if has_scraping:
                    control_num = str(row.get('control_number', ''))
                    if control_num:
                        control_numbers.add(control_num)
        
        # Check HTML cache directory for already scraped jobs
        html_cache_dir = Path("html_cache")
        if html_cache_dir.exists():
            # Walk through all subdirectories to find .html files
            for html_file in html_cache_dir.rglob("*.html"):
                # Extract control number from filename (e.g., "832123456.html" -> "832123456")
                control_num = html_file.stem
                if control_num.isdigit():
                    control_numbers.add(control_num)
        
        return control_numbers
    
    def cleanup_old_files(self, keep_latest_n: int = 3):
        """Clean up old timestamped files, keeping only the latest N"""
        # Clean up old unified jobs files
        job_files = list(self.base_path.glob("usajobs_*.parquet"))
        job_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
        
        for old_file in job_files[keep_latest_n:]:
            try:
                old_file.unlink()
                print(f"🗑️ Removed old file: {old_file.name}")
            except Exception as e:
                print(f"⚠️ Could not remove {old_file.name}: {e}")
        
        # Clean up old overlap files
        overlap_files = list(self.base_path.glob("overlap_samples_*.parquet"))
        overlap_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
        
        for old_file in overlap_files[keep_latest_n:]:
            try:
                old_file.unlink()
                print(f"🗑️ Removed old overlap file: {old_file.name}")
            except Exception as e:
                print(f"⚠️ Could not remove {old_file.name}: {e}")


# Legacy migration function (if needed)
def migrate_from_duckdb(duckdb_path: str, storage: ParquetJobStorage):
    """Migrate data from old DuckDB format"""
    print(f"🔄 Migration from DuckDB not implemented in simplified version")
    print(f"   Old data: {duckdb_path}")
    print(f"   Please re-run the pipeline to collect fresh data")