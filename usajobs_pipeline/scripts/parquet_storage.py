#!/usr/bin/env python3
"""
Parquet-based storage for USAJobs pipeline

Replaces DuckDB to enable parallel processing without locking issues.
Uses Parquet files for efficient columnar storage and easy parallelization.
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
    Handles storage and retrieval of job data using Parquet files
    """
    
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.base_path.mkdir(exist_ok=True)
        
        # Define storage paths
        self.historical_path = self.base_path / "historical_jobs"
        self.current_path = self.base_path / "current_jobs" 
        self.unified_path = self.base_path / "unified_jobs"
        self.overlap_path = self.base_path / "overlap_samples"
        
        # Create directories
        for path in [self.historical_path, self.current_path, self.unified_path, self.overlap_path]:
            path.mkdir(exist_ok=True)
    
    def save_historical_jobs(self, jobs: List[Dict], batch_id: str):
        """Save historical jobs batch to Parquet"""
        if not jobs:
            return
        
        # Flatten scraped content for better storage
        flattened_jobs = []
        for job in jobs:
            flat_job = job.copy()
            
            # Handle scraped content
            if 'scraped_content' in flat_job and flat_job['scraped_content']:
                scraped = flat_job['scraped_content']
                if isinstance(scraped, dict):
                    # Store as JSON string for complex nested data
                    flat_job['scraped_sections'] = json.dumps(scraped.get('content_sections', {}))
                    flat_job['scraped_metadata'] = json.dumps({
                        k: v for k, v in scraped.items() 
                        if k != 'content_sections'
                    })
                else:
                    flat_job['scraped_sections'] = json.dumps(scraped)
                    flat_job['scraped_metadata'] = '{}'
                
                del flat_job['scraped_content']
            else:
                flat_job['scraped_sections'] = '{}'
                flat_job['scraped_metadata'] = '{}'
            
            # Convert other complex fields to JSON strings
            for field in ['raw_data', 'UserArea']:
                if field in flat_job and isinstance(flat_job[field], (dict, list)):
                    flat_job[field] = json.dumps(flat_job[field])
            
            flattened_jobs.append(flat_job)
        
        df = pd.DataFrame(flattened_jobs)
        
        # Add batch metadata
        df['batch_id'] = batch_id
        df['created_at'] = datetime.now().isoformat()
        
        # Save to Parquet
        file_path = self.historical_path / f"batch_{batch_id}.parquet"
        df.to_parquet(file_path, index=False)
        
        print(f"💾 Saved {len(jobs)} historical jobs to {file_path}")
    
    def save_current_jobs(self, jobs: List[Dict]):
        """Save current API jobs to Parquet"""
        if not jobs:
            return
        
        # Flatten complex fields
        flattened_jobs = []
        for job in jobs:
            flat_job = job.copy()
            
            # Convert complex fields to JSON strings
            for field in ['PositionFormattedDescription', 'ApplicationDetails', 'UserArea']:
                if field in flat_job and isinstance(flat_job[field], (dict, list)):
                    flat_job[field] = json.dumps(flat_job[field])
            
            flattened_jobs.append(flat_job)
        
        df = pd.DataFrame(flattened_jobs)
        df['created_at'] = datetime.now().isoformat()
        
        # Save to Parquet
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = self.current_path / f"current_jobs_{timestamp}.parquet"
        df.to_parquet(file_path, index=False)
        
        print(f"💾 Saved {len(jobs)} current jobs to {file_path}")
        return str(file_path)
    
    def load_historical_jobs(self) -> pd.DataFrame:
        """Load all historical jobs from Parquet files"""
        parquet_files = list(self.historical_path.glob("*.parquet"))
        
        if not parquet_files:
            return pd.DataFrame()
        
        # Read and combine all historical batches
        dfs = []
        for file_path in parquet_files:
            df = pd.read_parquet(file_path)
            dfs.append(df)
        
        combined_df = pd.concat(dfs, ignore_index=True)
        print(f"📊 Loaded {len(combined_df)} historical jobs from {len(parquet_files)} files")
        
        return combined_df
    
    def load_current_jobs(self) -> pd.DataFrame:
        """Load current jobs from Parquet files"""
        parquet_files = list(self.current_path.glob("*.parquet"))
        
        if not parquet_files:
            return pd.DataFrame()
        
        # Get the most recent current jobs file
        latest_file = max(parquet_files, key=lambda f: f.stat().st_mtime)
        df = pd.read_parquet(latest_file)
        
        print(f"📊 Loaded {len(df)} current jobs from {latest_file}")
        return df
    
    def save_unified_jobs(self, jobs: List[Dict]):
        """Save unified/rationalized jobs to Parquet"""
        if not jobs:
            return
        
        # Flatten complex fields
        flattened_jobs = []
        for job in jobs:
            flat_job = job.copy()
            
            # Convert complex fields to JSON strings
            for field in ['scraped_sections', 'data_sources']:
                if field in flat_job and isinstance(flat_job[field], (dict, list)):
                    flat_job[field] = json.dumps(flat_job[field])
            
            flattened_jobs.append(flat_job)
        
        df = pd.DataFrame(flattened_jobs)
        df['created_at'] = datetime.now().isoformat()
        
        # Save to Parquet
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = self.unified_path / f"unified_jobs_{timestamp}.parquet"
        df.to_parquet(file_path, index=False)
        
        print(f"💾 Saved {len(jobs)} unified jobs to {file_path}")
        return str(file_path)
    
    def save_overlap_samples(self, overlap_samples: List[Dict]):
        """Save overlap samples for API vs scraping comparison"""
        if not overlap_samples:
            return
        
        df = pd.DataFrame(overlap_samples)
        df['created_at'] = datetime.now().isoformat()
        
        # Save to Parquet
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = self.overlap_path / f"overlap_samples_{timestamp}.parquet"
        df.to_parquet(file_path, index=False)
        
        print(f"💾 Saved {len(overlap_samples)} overlap samples to {file_path}")
        return str(file_path)
    
    def load_unified_jobs(self) -> pd.DataFrame:
        """Load unified jobs from Parquet files"""
        parquet_files = list(self.unified_path.glob("*.parquet"))
        
        if not parquet_files:
            return pd.DataFrame()
        
        # Get the most recent unified jobs file
        latest_file = max(parquet_files, key=lambda f: f.stat().st_mtime)
        df = pd.read_parquet(latest_file)
        
        print(f"📊 Loaded {len(df)} unified jobs from {latest_file}")
        return df
    
    def load_overlap_samples(self) -> pd.DataFrame:
        """Load overlap samples from Parquet files"""
        parquet_files = list(self.overlap_path.glob("*.parquet"))
        
        if not parquet_files:
            return pd.DataFrame()
        
        # Get the most recent overlap samples file
        latest_file = max(parquet_files, key=lambda f: f.stat().st_mtime)
        df = pd.read_parquet(latest_file)
        
        print(f"📊 Loaded {len(df)} overlap samples from {latest_file}")
        return df
    
    def get_control_numbers_with_scraping(self) -> set:
        """Get set of control numbers that already have scraping data"""
        df = self.load_historical_jobs()
        
        if df.empty:
            return set()
        
        # Find jobs with non-empty scraped content
        has_scraping = df[
            (df['scraped_sections'].notna()) & 
            (df['scraped_sections'] != '{}') &
            (df['scraped_sections'] != '')
        ]
        
        # Historical API uses 'usajobsControlNumber' 
        control_field = 'usajobsControlNumber' if 'usajobsControlNumber' in df.columns else 'control_number'
        return set(has_scraping[control_field].astype(str))
    
    def cleanup_old_files(self, keep_latest_n: int = 3):
        """Clean up old Parquet files, keeping only the most recent ones"""
        for directory in [self.current_path, self.unified_path, self.overlap_path]:
            files = list(directory.glob("*.parquet"))
            if len(files) > keep_latest_n:
                # Sort by modification time
                files.sort(key=lambda f: f.stat().st_mtime)
                
                # Remove older files
                for old_file in files[:-keep_latest_n]:
                    old_file.unlink()
                    print(f"🗑️ Removed old file: {old_file}")

def migrate_from_duckdb(duckdb_path: str, parquet_storage: ParquetJobStorage):
    """
    Migrate existing data from DuckDB to Parquet format
    """
    try:
        import duckdb
        
        conn = duckdb.connect(duckdb_path)
        
        # Migrate historical jobs
        try:
            hist_df = conn.execute("SELECT * FROM historical_jobs").df()
            if not hist_df.empty:
                # Convert to list of dicts for processing
                hist_jobs = hist_df.to_dict('records')
                parquet_storage.save_historical_jobs(hist_jobs, "migrated_from_duckdb")
                print(f"✅ Migrated {len(hist_jobs)} historical jobs")
        except Exception as e:
            print(f"⚠️ Could not migrate historical jobs: {e}")
        
        # Migrate current jobs  
        try:
            curr_df = conn.execute("SELECT * FROM current_jobs").df()
            if not curr_df.empty:
                curr_jobs = curr_df.to_dict('records')
                parquet_storage.save_current_jobs(curr_jobs)
                print(f"✅ Migrated {len(curr_jobs)} current jobs")
        except Exception as e:
            print(f"⚠️ Could not migrate current jobs: {e}")
        
        # Migrate unified jobs
        try:
            unified_df = conn.execute("SELECT * FROM unified_jobs").df()
            if not unified_df.empty:
                unified_jobs = unified_df.to_dict('records')
                parquet_storage.save_unified_jobs(unified_jobs)
                print(f"✅ Migrated {len(unified_jobs)} unified jobs")
        except Exception as e:
            print(f"⚠️ Could not migrate unified jobs: {e}")
        
        # Migrate overlap samples
        try:
            overlap_df = conn.execute("SELECT * FROM overlap_samples").df()
            if not overlap_df.empty:
                overlap_samples = overlap_df.to_dict('records')
                parquet_storage.save_overlap_samples(overlap_samples)
                print(f"✅ Migrated {len(overlap_samples)} overlap samples")
        except Exception as e:
            print(f"⚠️ Could not migrate overlap samples: {e}")
        
        conn.close()
        print("✅ Migration from DuckDB completed")
        
    except ImportError:
        print("⚠️ DuckDB not available for migration")
    except Exception as e:
        print(f"⚠️ Migration failed: {e}")

if __name__ == "__main__":
    # Example usage
    storage = ParquetJobStorage("data_parquet")
    
    # Example of loading data
    hist_df = storage.load_historical_jobs()
    curr_df = storage.load_current_jobs()
    unified_df = storage.load_unified_jobs()
    
    print(f"Historical: {len(hist_df)} jobs")
    print(f"Current: {len(curr_df)} jobs") 
    print(f"Unified: {len(unified_df)} jobs")