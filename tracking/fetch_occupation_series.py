#!/usr/bin/env python3
"""
Fetch occupation series from USAJobs API and save as a mapping file
"""
import requests
import json
from pathlib import Path

def fetch_occupation_series():
    """Fetch occupation series from USAJobs API"""
    url = "https://data.usajobs.gov/api/codelist/occupationalseries"
    
    print(f"Fetching occupation series from {url}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract the occupation series mappings
        occupation_mapping = {}
        
        if 'CodeList' in data and len(data['CodeList']) > 0:
            valid_values = data['CodeList'][0].get('ValidValue', [])
            
            for item in valid_values:
                code = item.get('Code')
                name = item.get('Value')
                is_disabled = item.get('IsDisabled', 'No')
                
                if code and name and is_disabled == 'No':
                    # Store both with and without leading zeros for compatibility
                    occupation_mapping[code] = name.upper()
                    if code.startswith('0') and len(code) == 4:
                        occupation_mapping[code.lstrip('0')] = name.upper()
            
            print(f"Found {len(valid_values)} total occupation series ({len(occupation_mapping)} active mappings)")
            
            # Save the mapping
            output_file = Path(__file__).parent / 'occupation_series_from_api.json'
            with open(output_file, 'w') as f:
                json.dump(occupation_mapping, f, indent=2, sort_keys=True)
            
            print(f"Saved to {output_file}")
            
            # Show some examples
            print("\nSample mappings:")
            for code in ['0301', '2210', '0610', '0183', '1102']:
                if code in occupation_mapping:
                    print(f"  {code} -> {occupation_mapping[code]}")
                else:
                    print(f"  {code} -> NOT FOUND")
            
            return True
            
        else:
            print("ERROR: Unexpected response structure")
            print(json.dumps(data, indent=2)[:500])
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"ERROR fetching data: {e}")
        return False
    except json.JSONDecodeError as e:
        print(f"ERROR parsing JSON: {e}")
        return False

if __name__ == '__main__':
    success = fetch_occupation_series()
    exit(0 if success else 1)