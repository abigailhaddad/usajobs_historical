#!/usr/bin/env python3
"""
Create Comprehensive Field Mapping

Based on the field analysis, create a comprehensive mapping strategy
to extract ALL valuable information from USAJobs postings.
"""

import json
from pathlib import Path

def load_analysis_results():
    """Load the field analysis results"""
    with open('all_fields_analysis.json', 'r') as f:
        return json.load(f)

def create_comprehensive_mapping():
    """Create a comprehensive field mapping strategy"""
    
    # Load the analysis
    data = load_analysis_results()
    
    # Enhanced field mapping with ALL discovered fields
    comprehensive_mapping = {
        # Core Job Information
        'job_identification': {
            'description': 'Basic job identifiers and metadata',
            'fields': [
                'Summary', 'Overview', 'Control number', 'Announcement number',
                'Position Description/PD#:', 'About the Position:', 'About the position:'
            ]
        },
        
        # Job Details & Requirements  
        'job_specifications': {
            'description': 'Detailed job specifications and requirements',
            'fields': [
                'Job family (Series)', 'Pay scale & grade', 'Salary', 'Work schedule', 
                'Appointment type', 'Promotion potential', 'Supervisory status',
                'Security clearance', 'Drug test', 'Telework eligible', 'Remote job',
                'Travel Required', 'Location', 'Relocation expenses reimbursed',
                'Service', 'Bargaining unit status', 'Financial disclosure',
                'Position sensitivity and risk', 'Trust determination process'
            ]
        },
        
        # Duties & Responsibilities
        'duties_responsibilities': {
            'description': 'Job duties, responsibilities, and daily tasks',
            'fields': [
                'Duties', 'Major Duties', 'Key Duties', 'Responsibilities', 
                'What You Will Do', 'Job Duties', 'Primary Duties', 'Essential Duties'
            ]
        },
        
        # Qualifications & Requirements
        'qualifications_requirements': {
            'description': 'Required and preferred qualifications',
            'fields': [
                'Qualifications', 'Requirements', 'Basic Requirements', 
                'SPECIALIZED EXPERIENCE:', 'Specialized Experience:', 'Preferred Experience:',
                'Physical Requirements:', 'PART-TIME OR UNPAID EXPERIENCE:',
                'VOLUNTEER WORK EXPERIENCE:', 'Working Conditions:', 'Conditions of Employment'
            ]
        },
        
        # Education Requirements
        'education_requirements': {
            'description': 'Education requirements and degree information',
            'fields': [
                'Education', 'Education:', 'FOREIGN EDUCATION:', 'IF USING EDUCATION TO QUALIFY:',
                'Transcripts', 'Degree:', 'GRADUATE EDUCATION:', 'OREDUCATION:',
                'If you are relying on your education to meet qualification requirements:'
            ]
        },
        
        # Application Process
        'application_process': {
            'description': 'How to apply and application requirements',
            'fields': [
                'How to Apply', 'Apply', 'Apply Online', 'Application Process',
                'Application Instructions', 'Who May Apply', 'Who May Apply:',
                'Proof of Eligibility to Apply:', 'This job is open to',
                'click to continue with the application process',
                'You must re-select your resume and/or other documents from your USAJOBS account or your application will be incomplete.'
            ]
        },
        
        # Required Documents
        'required_documents': {
            'description': 'Documents needed for application',
            'fields': [
                'Required Documents', 'Required Documents:', 'Documentation is required to award preference.',
                'Time-in-grade documentation:', 'Veterans\' Preference Documentation:',
                'select your resume and/or other supporting documents'
            ]
        },
        
        # Evaluation Process
        'evaluation_process': {
            'description': 'How candidates will be evaluated and selected',
            'fields': [
                'How You Will Be Evaluated', 'BASIS OF RATING:', 'Direct Hire Evaluation:',
                'Evaluation', 'Rating and Ranking', 'Selection Process'
            ]
        },
        
        # Benefits & Compensation
        'benefits_compensation': {
            'description': 'Salary, benefits, and compensation details',
            'fields': [
                'Benefits', 'Compensation', 'Pay:', 'What We Offer', 'Perks',
                'Pay-Band (NF or CY) Allowances and Differentials',
                'Education Debt Reduction Program (Student Loan Repayment):',
                'If authorized, only regular (full-time, part-time, limited tenure and seasonal) employees may be paid Sunday premium pay.'
            ]
        },
        
        # Timeline & Next Steps
        'timeline_next_steps': {
            'description': 'Application timeline and what happens next',
            'fields': [
                'Open & closing dates', 'Next steps', 'What to Expect Next',
                'After You Apply', 'Selection Process'
            ]
        },
        
        # Agency Information
        'agency_information': {
            'description': 'Information about the hiring agency',
            'fields': [
                'Agency contact information', 'Clarification from the agency', 
                'About the Agency', 'Department', 'Office', 'Organization',
                'Interagency Career Transition Assistance Program (ICTAP):',
                'Career Transition Assistance Program (CTAP)/Interagency Career Transition Assistance Program (ICTAP)'
            ]
        },
        
        # Contact Information
        'contact_information': {
            'description': 'Contact details for questions',
            'fields': [
                'Address', 'Email', 'Phone', 'Website', 'Fax'
            ]
        },
        
        # Additional Information
        'additional_information': {
            'description': 'Other important information and notes',
            'fields': [
                'Additional information', 'Other Information', 'Notes', 
                'IMPORTANT:', 'IMPORTANT', 'NOTE:', 'Note:',
                'For Important General Applicant Information and Definitions go to:',
                'Fair & Transparent', 'Videos'
            ]
        }
    }
    
    return comprehensive_mapping

def create_updated_scraper_config():
    """Create an updated configuration for the scraper"""
    
    mapping = create_comprehensive_mapping()
    
    # Convert to the format used by the scraper
    target_sections = {}
    
    for category, info in mapping.items():
        for field in info['fields']:
            # Create variations for fuzzy matching
            variations = [field]
            
            # Add common variations
            base_field = field.rstrip(':').rstrip('.')
            if base_field != field:
                variations.append(base_field)
            
            # Add lowercase version
            variations.append(field.lower())
            
            # Store with category as key
            field_key = f"{category}_{field.replace(' ', '_').replace(':', '').replace('.', '').replace('(', '').replace(')', '').replace('/', '_')}"
            target_sections[field_key] = variations
    
    return target_sections

def analyze_coverage(analysis_data, mapping):
    """Analyze how much of the discovered data our mapping covers"""
    
    header_frequencies = analysis_data['header_frequencies']
    
    # Count covered vs uncovered headers
    all_mapped_headers = set()
    for category, info in mapping.items():
        all_mapped_headers.update(info['fields'])
    
    covered_count = 0
    covered_frequency = 0
    total_frequency = sum(header_frequencies.values())
    
    for header, freq in header_frequencies.items():
        if header in all_mapped_headers:
            covered_count += 1
            covered_frequency += freq
    
    print(f"📊 COVERAGE ANALYSIS")
    print("=" * 40)
    print(f"📋 Total unique headers: {len(header_frequencies)}")
    print(f"✅ Headers covered by mapping: {covered_count} ({covered_count/len(header_frequencies)*100:.1f}%)")
    print(f"📈 Frequency coverage: {covered_frequency:,}/{total_frequency:,} ({covered_frequency/total_frequency*100:.1f}%)")
    
    # Show uncovered high-frequency headers
    print(f"\n❓ HIGH-FREQUENCY UNCOVERED HEADERS")
    print("-" * 40)
    uncovered = [(h, f) for h, f in header_frequencies.items() if h not in all_mapped_headers and f >= 50]
    uncovered.sort(key=lambda x: x[1], reverse=True)
    
    for header, freq in uncovered[:20]:
        print(f"   {freq:4d} {header}")

def main():
    print("🔧 Creating Comprehensive Field Mapping Strategy")
    print("=" * 60)
    
    # Load analysis results
    try:
        analysis_data = load_analysis_results()
        print(f"✅ Loaded analysis of {analysis_data['analysis_info']['files_analyzed']} files")
    except FileNotFoundError:
        print("❌ Run analyze_all_fields.py first to generate analysis data")
        return
    
    # Create comprehensive mapping
    mapping = create_comprehensive_mapping()
    
    print(f"\n📂 COMPREHENSIVE FIELD CATEGORIES")
    print("-" * 40)
    total_fields = 0
    for category, info in mapping.items():
        field_count = len(info['fields'])
        total_fields += field_count
        print(f"{category:25}: {field_count:3d} fields - {info['description']}")
    
    print(f"\n📊 Total fields mapped: {total_fields}")
    
    # Analyze coverage
    analyze_coverage(analysis_data, mapping)
    
    # Create updated scraper configuration
    scraper_config = create_updated_scraper_config()
    
    # Save configurations
    output_data = {
        'comprehensive_mapping': mapping,
        'scraper_target_sections': scraper_config,
        'analysis_summary': {
            'total_categories': len(mapping),
            'total_mapped_fields': total_fields,
            'generated_at': analysis_data['analysis_info']['timestamp']
        }
    }
    
    with open('comprehensive_field_mapping.json', 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Comprehensive mapping saved to: comprehensive_field_mapping.json")
    
    # Show sample category
    print(f"\n🔍 SAMPLE CATEGORY: job_specifications")
    print("-" * 40)
    sample_fields = mapping['job_specifications']['fields'][:10]
    for field in sample_fields:
        freq = analysis_data['header_frequencies'].get(field, 0)
        print(f"   {freq:4d} {field}")
    
    print(f"\n✅ Comprehensive field mapping strategy created!")
    print(f"📋 This covers the major data points from {len(analysis_data['header_frequencies'])} unique headers")

if __name__ == "__main__":
    main()