#!/usr/bin/env python3
"""
Update existing current job parquet files with HiringPaths field.
This is a one-time update to add the harmonized field to existing data.
"""

import pandas as pd
import json
import glob
from pathlib import Path

# Map current API hiring path codes to historical API descriptions
HIRING_PATH_MAPPING = {
    'public': 'The public',
    'fed-internal-search': 'Internal to an agency', 
    'fed-competitive': 'Federal employees - Competitive service',
    'fed-excepted': 'Federal employees - Excepted service',
    'fed-transition': 'Career transition (CTAP, ICTAP, RPL)',
    'vet': 'Veterans',
    'mspouse': 'Military spouses',
    'disability': 'Individuals with disabilities',
    'special-authorities': 'Special authorities',
    'overseas': 'Family of overseas employees',
    'peace': 'Peace Corps & AmeriCorps Vista',
    'nguard': 'National Guard and reserves',
    'native': 'Native Americans',
    'land': 'Land and base management',
    'student': 'Students',
    'graduates': 'Recent graduates',
    'ses': 'Senior executives'
}

def extract_hiring_paths_from_current_api(matched_object_descriptor):
    """Extract hiring paths from current API's MatchedObjectDescriptor and convert to historical format"""
    if pd.isna(matched_object_descriptor):
        return None
    
    try:
        data = json.loads(matched_object_descriptor)
        
        # Navigate to HiringPath in UserArea.Details
        if 'UserArea' in data and 'Details' in data['UserArea'] and 'HiringPath' in data['UserArea']['Details']:
            codes = data['UserArea']['Details']['HiringPath']
            
            # Convert codes to the historical format
            # Historical format: [{"hiringPath": "Description"}, ...]
            hiring_paths = []
            for code in codes:
                if code in HIRING_PATH_MAPPING:
                    hiring_paths.append({"hiringPath": HIRING_PATH_MAPPING[code]})
                else:
                    # If we don't have a mapping, use the code as-is
                    print(f"Warning: No mapping for hiring path code: {code}")
                    hiring_paths.append({"hiringPath": code})
            
            # Return as JSON string to match historical format
            return json.dumps(hiring_paths)
    except Exception as e:
        print(f"Error extracting hiring paths: {e}")
    
    return None

def update_current_jobs_file(filepath):
    """Add HiringPaths field to current jobs file"""
    print(f"\nProcessing {filepath}...")
    
    # Create backup
    backup_path = filepath.replace('.parquet', '_backup.parquet')
    
    # Load the parquet file
    df = pd.read_parquet(filepath)
    original_shape = df.shape
    
    # Check if HiringPaths already exists
    if 'HiringPaths' in df.columns:
        print(f"  HiringPaths already exists in {filepath}, checking if any are null...")
        null_count = df['HiringPaths'].isna().sum()
        if null_count == 0:
            print(f"  All records already have HiringPaths, skipping...")
            return
        else:
            print(f"  {null_count} records missing HiringPaths, updating...")
    
    # Backup the original file
    print(f"  Creating backup at {backup_path}")
    df.to_parquet(backup_path, index=False)
    
    # Create HiringPaths from MatchedObjectDescriptor
    if 'MatchedObjectDescriptor' in df.columns:
        print(f"  Extracting HiringPaths from MatchedObjectDescriptor...")
        df['HiringPaths'] = df['MatchedObjectDescriptor'].apply(extract_hiring_paths_from_current_api)
        
        # Count how many records got hiring paths
        has_paths = df['HiringPaths'].notna().sum()
        print(f"  Added HiringPaths to {has_paths:,} out of {len(df):,} records ({has_paths/len(df)*100:.1f}%)")
        
        # Verify shape didn't change
        if df.shape[0] != original_shape[0]:
            print(f"  ERROR: Row count changed from {original_shape[0]} to {df.shape[0]}")
            print(f"  Restoring from backup...")
            backup_df = pd.read_parquet(backup_path)
            backup_df.to_parquet(filepath, index=False)
            return
        
        # Save the updated file
        df.to_parquet(filepath, index=False)
        print(f"  Successfully updated {filepath}")
        print(f"  Shape: {df.shape}")
    else:
        print(f"  No MatchedObjectDescriptor column found, skipping...")

def main():
    """Update all current job files with HiringPaths"""
    print("Updating current job files with HiringPaths field...")
    
    # Find all current job files
    current_files = glob.glob('data/current_jobs_*.parquet')
    print(f"\nFound {len(current_files)} current job files to update")
    
    if not current_files:
        print("No current job files found!")
        return
    
    # Process each file
    for filepath in sorted(current_files):
        update_current_jobs_file(filepath)
    
    print("\n\nUpdate complete!")
    
    # Verify the updates
    print("\nVerifying updates...")
    for filepath in sorted(current_files):
        df = pd.read_parquet(filepath)
        hiring_paths_count = df['HiringPaths'].notna().sum()
        print(f"  {Path(filepath).name}: {hiring_paths_count:,} records with HiringPaths out of {len(df):,} total")

if __name__ == "__main__":
    main()