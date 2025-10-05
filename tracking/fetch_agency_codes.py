#!/usr/bin/env python3
"""
Fetch agency codes from USAJobs API and save as a mapping file
"""
import requests
import json
from pathlib import Path

def fetch_agency_codes():
    """Fetch agency codes from USAJobs API"""
    url = "https://data.usajobs.gov/api/codelist/agencysubelements"
    
    print(f"Fetching agency codes from {url}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract the agency mappings
        agency_mapping = {}
        
        if 'CodeList' in data and len(data['CodeList']) > 0:
            valid_values = data['CodeList'][0].get('ValidValue', [])
            
            for item in valid_values:
                code = item.get('Code')
                name = item.get('Value')
                is_disabled = item.get('IsDisabled', 'No')
                
                if code and name and is_disabled == 'No':
                    agency_mapping[code] = {
                        'name': name,
                        'last_modified': item.get('LastModified', ''),
                        'is_disabled': is_disabled
                    }
            
            print(f"Found {len(agency_mapping)} active agency codes")
            
            # Save the mapping
            output_file = Path(__file__).parent / 'usajobs_agency_codes.json'
            with open(output_file, 'w') as f:
                json.dump(agency_mapping, f, indent=2, sort_keys=True)
            
            print(f"Saved to {output_file}")
            
            # Show some examples
            print("\nSample mappings:")
            for code in ['AF', 'DD16', 'NV', 'AR']:
                if code in agency_mapping:
                    print(f"  {code} -> {agency_mapping[code]['name']}")
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
    success = fetch_agency_codes()
    exit(0 if success else 1)