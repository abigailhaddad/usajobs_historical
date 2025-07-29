#!/usr/bin/env python3
import pandas as pd
import json
from pathlib import Path

# Load current jobs
df = pd.read_parquet('../data/current_jobs_2025.parquet')

# Find a job with a questionnaire
found = False
for idx, row in df.iterrows():
    if pd.notna(row.get('MatchedObjectDescriptor')):
        try:
            mod = json.loads(row['MatchedObjectDescriptor'])
            if 'UserArea' in mod and 'Details' in mod['UserArea']:
                details = mod['UserArea']['Details']
                if 'Evaluations' in details and 'ViewQuestionnaire' in str(details.get('Evaluations', '')):
                    print(f"Found job with questionnaire:")
                    print(f"Control Number: {row.get('usajobsControlNumber')}")
                    print(f"Title: {row.get('positionTitle')}")
                    print(f"\nTop-level fields:")
                    print(f"  minimumGrade: {row.get('minimumGrade')}")
                    print(f"  maximumGrade: {row.get('maximumGrade')}")
                    
                    # Extract location
                    position_location = None
                    if 'PositionLocation' in mod and isinstance(mod['PositionLocation'], list) and len(mod['PositionLocation']) > 0:
                        loc = mod['PositionLocation'][0]
                        location_parts = []
                        if 'CityName' in loc:
                            location_parts.append(loc['CityName'])
                        if 'CountrySubDivisionCode' in loc:
                            location_parts.append(loc['CountrySubDivisionCode'])
                        position_location = ', '.join(location_parts) if location_parts else None
                    
                    print(f"\nExtracted location: {position_location}")
                    
                    # Extract service type
                    service_type = None
                    service_type_code = details.get('ServiceType')
                    if service_type_code:
                        service_type_map = {
                            '01': 'Competitive',
                            '02': 'Excepted', 
                            '03': 'Senior Executive'
                        }
                        service_type = service_type_map.get(service_type_code, service_type_code)
                    
                    print(f"Service type code: {service_type_code} -> {service_type}")
                    
                    # Extract grade
                    min_grade = row.get('minimumGrade', '')
                    max_grade = row.get('maximumGrade', '')
                    if min_grade and max_grade and min_grade == max_grade:
                        grade_code = min_grade
                    elif min_grade and max_grade:
                        grade_code = f"{min_grade}-{max_grade}"
                    else:
                        grade_code = min_grade or max_grade or None
                    
                    print(f"Grade code: {grade_code}")
                    
                    found = True
                    break
        except Exception as e:
            print(f"Error: {e}")
            
if not found:
    print("No job with questionnaire found!")