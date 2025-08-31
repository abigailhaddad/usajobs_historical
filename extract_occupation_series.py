import pandas as pd
import json
import glob
from collections import defaultdict, Counter

def extract_occupation_series_from_parquet():
    """Extract occupation series and their most common titles from parquet files"""
    
    # Dictionary to store series -> title mappings
    series_to_titles = defaultdict(Counter)
    
    # Get all parquet files
    parquet_files = glob.glob('data/historical_jobs_*.parquet') + glob.glob('data/current_jobs_*.parquet')
    print(f"Found {len(parquet_files)} parquet files to process")
    
    for file_path in parquet_files:
        print(f"\nProcessing: {file_path}")
        try:
            df = pd.read_parquet(file_path)
            
            # Extract series from JobCategories column
            if 'JobCategories' in df.columns:
                for idx, row in df.iterrows():
                    if pd.notna(row['JobCategories']) and pd.notna(row['positionTitle']):
                        try:
                            # JobCategories is a JSON string like [{"series": "0301"}]
                            job_cats = eval(row['JobCategories']) if isinstance(row['JobCategories'], str) else row['JobCategories']
                            if isinstance(job_cats, list):
                                for cat in job_cats:
                                    if isinstance(cat, dict) and 'series' in cat:
                                        series = cat['series']
                                        title = str(row['positionTitle']).strip()
                                        if series and title:
                                            series_to_titles[series][title] += 1
                        except:
                            pass
                            
            print(f"  Processed {len(df)} records")
            
        except Exception as e:
            print(f"  Error processing file: {e}")
    
    # Now create the final mapping using the most common title for each series
    occupation_series_map = {}
    
    print("\n\nGenerating occupation series mapping...")
    print("="*80)
    
    for series, title_counts in sorted(series_to_titles.items()):
        if title_counts:
            # Get the most common title
            most_common_title, count = title_counts.most_common(1)[0]
            
            # Try to extract a generic occupation name from the most common title
            # Remove common prefixes/suffixes
            generic_title = most_common_title.upper()
            
            # Remove common job-specific terms
            remove_terms = [
                'SENIOR', 'JUNIOR', 'LEAD', 'SUPERVISORY', 'CHIEF', 'ASSISTANT',
                'ASSOCIATE', 'ENTRY LEVEL', 'EXPERIENCED', 'SPECIALIST',
                'I', 'II', 'III', 'IV', 'V', '(', ')', '-', 'LEVEL'
            ]
            
            for term in remove_terms:
                generic_title = generic_title.replace(f' {term} ', ' ')
                generic_title = generic_title.replace(f'{term} ', '')
                generic_title = generic_title.replace(f' {term}', '')
            
            # Clean up extra spaces
            generic_title = ' '.join(generic_title.split()).strip()
            
            # Store both with and without leading zeros
            occupation_series_map[series] = generic_title
            if series.startswith('0') and len(series) == 4:
                occupation_series_map[series.lstrip('0')] = generic_title
            
            # Show some examples
            if series in ['0182', '0183', '0610', '0301', '2210']:
                print(f"\n{series}: {generic_title}")
                print(f"  Most common title: {most_common_title} ({count} occurrences)")
                print(f"  Other titles:")
                for title, cnt in title_counts.most_common(5)[1:6]:
                    print(f"    - {title} ({cnt})")
    
    print(f"\n\nTotal occupation series found: {len(series_to_titles)}")
    print(f"Total mappings created: {len(occupation_series_map)}")
    
    # Save to JSON file
    output_file = 'tracking/occupation_series_from_data.json'
    with open(output_file, 'w') as f:
        json.dump(occupation_series_map, f, indent=2, sort_keys=True)
    
    print(f"\nSaved occupation series mapping to: {output_file}")
    
    # Show some statistics
    print("\n\nSome interesting statistics:")
    series_list = list(series_to_titles.keys())
    series_with_zeros = [s for s in series_list if s.startswith('0')]
    series_without_zeros = [s for s in series_list if not s.startswith('0')]
    
    print(f"Series starting with 0: {len(series_with_zeros)}")
    print(f"Series not starting with 0: {len(series_without_zeros)}")
    
    # Check for our specific series
    if '0182' in occupation_series_map:
        print(f"\n0182 mapped to: {occupation_series_map['0182']}")
    if '0183' in occupation_series_map:
        print(f"0183 mapped to: {occupation_series_map['0183']}")

if __name__ == "__main__":
    extract_occupation_series_from_parquet()