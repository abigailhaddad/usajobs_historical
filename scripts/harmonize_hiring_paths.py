#!/usr/bin/env python3
"""
Harmonize hiring paths between historical and current APIs.
Creates a unified HiringPaths field with the same structure.
"""

import pandas as pd
import json
import glob
from pathlib import Path

# Map current API codes to historical API descriptions
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

def harmonize_current_jobs_file(filepath):
    """Add HiringPaths field to current jobs file"""
    print(f"\nProcessing {filepath}...")
    
    # Load the parquet file
    df = pd.read_parquet(filepath)
    
    # Check if HiringPaths already exists
    if 'HiringPaths' in df.columns:
        print(f"  HiringPaths already exists in {filepath}")
        return
    
    # Create HiringPaths from MatchedObjectDescriptor
    if 'MatchedObjectDescriptor' in df.columns:
        print(f"  Creating HiringPaths from MatchedObjectDescriptor...")
        df['HiringPaths'] = df['MatchedObjectDescriptor'].apply(extract_hiring_paths_from_current_api)
        
        # Count how many records got hiring paths
        has_paths = df['HiringPaths'].notna().sum()
        print(f"  Added HiringPaths to {has_paths:,} out of {len(df):,} records ({has_paths/len(df)*100:.1f}%)")
        
        # Save the updated file
        df.to_parquet(filepath, index=False)
        print(f"  Saved updated file")
    else:
        print(f"  No MatchedObjectDescriptor column found")

def verify_historical_format(filepath):
    """Verify historical files have the expected HiringPaths format"""
    print(f"\nVerifying {filepath}...")
    
    df = pd.read_parquet(filepath)
    
    if 'HiringPaths' in df.columns:
        # Sample a few non-null entries
        sample_paths = df[df['HiringPaths'].notna()]['HiringPaths'].head(3)
        print(f"  HiringPaths column exists with {df['HiringPaths'].notna().sum():,} non-null values")
        for idx, paths in enumerate(sample_paths):
            try:
                data = json.loads(paths)
                print(f"  Sample {idx+1}: {data}")
            except:
                print(f"  Sample {idx+1}: Could not parse JSON")
    else:
        print(f"  No HiringPaths column found")

def test_conversion():
    """Test the conversion without modifying any files"""
    print("Testing hiring paths conversion (read-only)...")
    
    # Load sample data from current API
    print("\n=== CURRENT API ANALYSIS ===")
    df_curr = pd.read_parquet('data/current_jobs_2025.parquet')
    
    if 'MatchedObjectDescriptor' in df_curr.columns:
        print(f"Processing {len(df_curr):,} current jobs...")
        
        # Test conversion on all records
        converted_paths = df_curr['MatchedObjectDescriptor'].apply(extract_hiring_paths_from_current_api)
        
        # Stats
        has_paths = converted_paths.notna().sum()
        print(f"\nConversion results:")
        print(f"  Records with hiring paths: {has_paths:,} ({has_paths/len(df_curr)*100:.1f}%)")
        print(f"  Records without hiring paths: {len(df_curr) - has_paths:,}")
        
        # Show some examples
        print("\nExample conversions:")
        examples_shown = 0
        for idx, (orig, converted) in enumerate(zip(df_curr['MatchedObjectDescriptor'], converted_paths)):
            if pd.notna(converted) and examples_shown < 5:
                try:
                    orig_data = json.loads(orig)
                    conv_data = json.loads(converted)
                    
                    # Get original codes
                    orig_codes = orig_data.get('UserArea', {}).get('Details', {}).get('HiringPath', [])
                    
                    print(f"\nExample {examples_shown + 1}:")
                    print(f"  Original codes: {orig_codes}")
                    print(f"  Converted: {json.dumps(conv_data, indent=2)}")
                    
                    examples_shown += 1
                except:
                    pass
    
    # Compare with historical format
    print("\n\n=== HISTORICAL API COMPARISON ===")
    df_hist = pd.read_parquet('data/historical_jobs_2025.parquet')
    
    if 'HiringPaths' in df_hist.columns:
        print(f"Analyzing {len(df_hist):,} historical jobs...")
        
        # Get sample of historical hiring paths
        print("\nHistorical format examples:")
        examples_shown = 0
        for hp in df_hist['HiringPaths'].dropna():
            if examples_shown < 5:
                try:
                    data = json.loads(hp)
                    print(f"\nExample {examples_shown + 1}:")
                    print(f"  {json.dumps(data, indent=2)}")
                    examples_shown += 1
                except:
                    pass
    
    # Analyze unique values
    print("\n\n=== UNIQUE HIRING PATHS ANALYSIS ===")
    
    # Get all unique historical hiring paths
    hist_paths = set()
    for hp in df_hist['HiringPaths'].dropna():
        try:
            data = json.loads(hp)
            for item in data:
                if 'hiringPath' in item:
                    hist_paths.add(item['hiringPath'])
        except:
            pass
    
    # Get all unique converted paths
    conv_paths = set()
    for hp in converted_paths.dropna():
        try:
            data = json.loads(hp)
            for item in data:
                if 'hiringPath' in item:
                    conv_paths.add(item['hiringPath'])
        except:
            pass
    
    print(f"Unique historical hiring paths: {len(hist_paths)}")
    for path in sorted(hist_paths):
        print(f"  - {path}")
    
    print(f"\nUnique converted hiring paths: {len(conv_paths)}")
    for path in sorted(conv_paths):
        print(f"  - {path}")
    
    # Check for unmapped codes
    print("\n\n=== UNMAPPED CODES CHECK ===")
    unmapped = set()
    for desc in df_curr['MatchedObjectDescriptor'].dropna():
        try:
            data = json.loads(desc)
            if 'UserArea' in data and 'Details' in data['UserArea'] and 'HiringPath' in data['UserArea']['Details']:
                codes = data['UserArea']['Details']['HiringPath']
                for code in codes:
                    if code not in HIRING_PATH_MAPPING:
                        unmapped.add(code)
        except:
            pass
    
    if unmapped:
        print(f"Found {len(unmapped)} unmapped codes:")
        for code in sorted(unmapped):
            print(f"  - {code}")
    else:
        print("All codes are mapped!")

def main():
    """Test the harmonization without changing any files"""
    test_conversion()

def test_harmonization():
    """Test that both historical and current files have compatible HiringPaths"""
    # Load samples from both
    hist_2025 = pd.read_parquet('data/historical_jobs_2025.parquet', columns=['HiringPaths'])
    curr_2025 = pd.read_parquet('data/current_jobs_2025.parquet', columns=['HiringPaths'])
    
    print(f"Historical 2025 - HiringPaths non-null: {hist_2025['HiringPaths'].notna().sum():,}")
    print(f"Current 2025 - HiringPaths non-null: {curr_2025['HiringPaths'].notna().sum():,}")
    
    # Show a sample from each
    print("\nSample from historical:")
    for hp in hist_2025['HiringPaths'].dropna().head(2):
        print(f"  {hp}")
    
    print("\nSample from current:")
    for hp in curr_2025['HiringPaths'].dropna().head(2):
        print(f"  {hp}")

if __name__ == "__main__":
    main()