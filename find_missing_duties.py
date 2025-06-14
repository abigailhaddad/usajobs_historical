#!/usr/bin/env python3
"""
Find jobs where MajorDuties extraction failed
"""

import json

def find_missing_duties():
    """Find job IDs where MajorDuties field is missing"""
    
    with open('test_results.json', 'r') as f:
        data = json.load(f)
    
    missing_duties = []
    has_duties = []
    
    for result in data['results']:
        if result['status'] == 'success':
            control_number = result['control_number']
            mapped_fields = result['mapped_fields']
            
            if 'MajorDuties' not in mapped_fields:
                missing_duties.append({
                    'control_number': control_number,
                    'file_path': result['file_path'],
                    'mapped_fields': mapped_fields,
                    'all_headers': result['all_headers']
                })
            else:
                has_duties.append(control_number)
    
    print(f"Jobs WITH MajorDuties: {len(has_duties)}")
    print(f"Jobs MISSING MajorDuties: {len(missing_duties)}")
    print("\nJobs missing MajorDuties:")
    
    for i, job in enumerate(missing_duties[:10]):  # Show first 10
        print(f"{i+1}. {job['control_number']}")
        print(f"   File: {job['file_path']}")
        print(f"   Has fields: {', '.join(job['mapped_fields'])}")
        
        # Look for duty-related headers
        duty_headers = [h for h in job['all_headers'] if any(word in h.lower() for word in ['dut', 'major', 'responsib'])]
        if duty_headers:
            print(f"   Duty-related headers found: {duty_headers[:5]}")  # Show first 5
        else:
            print("   No obvious duty-related headers found")
        print()
    
    return missing_duties[:3]  # Return first 3 for detailed analysis

if __name__ == "__main__":
    missing_jobs = find_missing_duties()