#!/usr/bin/env python3
"""
Fetch appointment types (position offering types) from USAJobs API and save as a mapping file
"""
import requests
import json
from pathlib import Path

def fetch_appointment_types():
    """Fetch appointment types from USAJobs API"""
    url = "https://data.usajobs.gov/api/codelist/positionofferingtypes"
    
    print(f"Fetching appointment types from {url}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract the appointment type mappings
        appointment_mapping = {}
        
        if 'CodeList' in data and len(data['CodeList']) > 0:
            valid_values = data['CodeList'][0].get('ValidValue', [])
            
            for item in valid_values:
                code = item.get('Code')
                name = item.get('Value')
                is_disabled = item.get('IsDisabled', 'No')
                
                if code and name and is_disabled == 'No':
                    appointment_mapping[code] = name
            
            print(f"Found {len(valid_values)} total appointment types ({len(appointment_mapping)} active mappings)")
            
            # Save the mapping
            output_file = Path(__file__).parent / 'appointment_types_from_api.json'
            with open(output_file, 'w') as f:
                json.dump(appointment_mapping, f, indent=2, sort_keys=True)
            
            print(f"Saved to {output_file}")
            
            # Show some examples
            print("\nSample mappings:")
            for code in ['15522', '15667', '15668', '15317', '15318']:
                if code in appointment_mapping:
                    print(f"  {code} -> {appointment_mapping[code]}")
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
    success = fetch_appointment_types()
    exit(0 if success else 1)