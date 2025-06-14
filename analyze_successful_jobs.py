#!/usr/bin/env python3
"""
Analyze Successfully Extracted Jobs

Find jobs where the old scraper was already working well (getting most fields)
and show detailed before/after comparison to ensure we didn't break anything.
"""

import json
from pathlib import Path

def analyze_successful_old_jobs():
    """Find jobs where old scraper was already successful"""
    
    # Load the comprehensive comparison results
    try:
        with open('comprehensive_comparison_results.json', 'r') as f:
            results = json.load(f)
    except FileNotFoundError:
        print("❌ Run comprehensive_before_after_test.py first")
        return
    
    TARGET_FIELDS = [
        'Summary', 'MajorDuties', 'QualificationSummary', 'Requirements', 
        'Education', 'HowToApply', 'Evaluations', 'Benefits', 
        'RequiredDocuments', 'WhatToExpectNext', 'OtherInformation'
    ]
    
    # Find jobs where old scraper got many fields
    successful_old_jobs = []
    
    for result in results:
        if 'error' in result:
            continue
            
        # Count how many fields the old scraper got
        old_field_count = sum(1 for field in TARGET_FIELDS 
                             if result['field_comparison'][field]['old_has_content'])
        
        new_field_count = sum(1 for field in TARGET_FIELDS 
                             if result['field_comparison'][field]['new_has_content'])
        
        lost_fields = sum(1 for field in TARGET_FIELDS 
                         if result['field_comparison'][field]['lost_content'])
        
        # Include jobs where old scraper got 6+ fields (out of 11)
        if old_field_count >= 6:
            successful_old_jobs.append({
                'job_id': result['job_id'],
                'old_field_count': old_field_count,
                'new_field_count': new_field_count,
                'lost_fields': lost_fields,
                'gained_fields': new_field_count - old_field_count,
                'result': result
            })
    
    # Sort by old field count (most successful first)
    successful_old_jobs.sort(key=lambda x: x['old_field_count'], reverse=True)
    
    print(f"🔍 Found {len(successful_old_jobs)} jobs where old scraper got 6+ fields")
    print(f"📊 Out of {len([r for r in results if 'error' not in r])} total jobs")
    
    return successful_old_jobs

def show_detailed_comparison(successful_jobs, num_examples=10):
    """Show detailed before/after for successful old jobs"""
    
    TARGET_FIELDS = [
        'Summary', 'MajorDuties', 'QualificationSummary', 'Requirements', 
        'Education', 'HowToApply', 'Evaluations', 'Benefits', 
        'RequiredDocuments', 'WhatToExpectNext', 'OtherInformation'
    ]
    
    print(f"\n{'='*80}")
    print(f"📋 DETAILED COMPARISON: Jobs Where Old Scraper Was Already Working Well")
    print(f"{'='*80}")
    
    for i, job_data in enumerate(successful_jobs[:num_examples], 1):
        result = job_data['result']
        job_id = job_data['job_id']
        
        print(f"\n🔍 JOB {i}: {job_id}")
        print(f"📊 Old: {job_data['old_field_count']}/11 fields | New: {job_data['new_field_count']}/11 fields")
        print(f"📈 Gained: {job_data['gained_fields']} | 📉 Lost: {job_data['lost_fields']}")
        print("-" * 80)
        
        for field in TARGET_FIELDS:
            field_data = result['field_comparison'][field]
            
            old_status = "✅ HAS" if field_data['old_has_content'] else "❌ MISSING"
            new_status = "✅ HAS" if field_data['new_has_content'] else "❌ MISSING"
            
            # Highlight changes
            status_change = ""
            if field_data['gained_content']:
                status_change = " 🆕 GAINED"
            elif field_data['lost_content']:
                status_change = " ⚠️ LOST"
            elif field_data['improved']:
                status_change = " 📈 IMPROVED"
            
            print(f"  {field:20} | OLD: {old_status:10} ({field_data['old_length']:4d} chars) | NEW: {new_status:10} ({field_data['new_length']:4d} chars){status_change}")
            
            # Show content preview for lost content or significant changes
            if field_data['lost_content']:
                print(f"    ⚠️ LOST CONTENT: {field_data['old_preview']}")
            elif field_data['gained_content'] and field_data['new_length'] > 100:
                print(f"    🆕 NEW CONTENT: {field_data['new_preview']}")
            elif field_data['improved'] and abs(field_data['new_length'] - field_data['old_length']) > 500:
                print(f"    📈 SIGNIFICANT CHANGE: {field_data['old_length']} → {field_data['new_length']} chars")

def analyze_potential_regressions():
    """Look for any potential regressions in successful jobs"""
    
    with open('comprehensive_comparison_results.json', 'r') as f:
        results = json.load(f)
    
    TARGET_FIELDS = [
        'Summary', 'MajorDuties', 'QualificationSummary', 'Requirements', 
        'Education', 'HowToApply', 'Evaluations', 'Benefits', 
        'RequiredDocuments', 'WhatToExpectNext', 'OtherInformation'
    ]
    
    regressions = []
    
    for result in results:
        if 'error' in result:
            continue
            
        job_issues = []
        
        for field in TARGET_FIELDS:
            field_data = result['field_comparison'][field]
            
            # Check for lost content
            if field_data['lost_content']:
                job_issues.append(f"Lost {field} ({field_data['old_length']} chars)")
            
            # Check for significant content reduction (>50% reduction and >100 chars lost)
            if (field_data['old_has_content'] and field_data['new_has_content'] and 
                field_data['old_length'] > 200 and 
                field_data['new_length'] < field_data['old_length'] * 0.5):
                job_issues.append(f"Reduced {field} ({field_data['old_length']} → {field_data['new_length']} chars)")
        
        if job_issues:
            regressions.append({
                'job_id': result['job_id'],
                'issues': job_issues,
                'old_total': sum(1 for field in TARGET_FIELDS if result['field_comparison'][field]['old_has_content']),
                'new_total': sum(1 for field in TARGET_FIELDS if result['field_comparison'][field]['new_has_content'])
            })
    
    return regressions

def main():
    print("🔎 Analyzing Jobs Where Old Scraper Was Already Successful")
    print("=" * 70)
    
    # Find successful old jobs
    successful_jobs = analyze_successful_old_jobs()
    
    if not successful_jobs:
        print("❌ No successful old jobs found")
        return
    
    # Show summary stats
    print(f"\n📈 SUMMARY OF SUCCESSFUL OLD JOBS")
    print("-" * 50)
    
    # Group by old field count
    field_count_groups = {}
    for job in successful_jobs:
        count = job['old_field_count']
        if count not in field_count_groups:
            field_count_groups[count] = []
        field_count_groups[count].append(job)
    
    for count in sorted(field_count_groups.keys(), reverse=True):
        jobs = field_count_groups[count]
        avg_gained = sum(j['gained_fields'] for j in jobs) / len(jobs)
        avg_lost = sum(j['lost_fields'] for j in jobs) / len(jobs)
        
        print(f"  {count}/11 fields: {len(jobs):3d} jobs | Avg gained: {avg_gained:4.1f} | Avg lost: {avg_lost:4.1f}")
    
    # Show detailed examples
    show_detailed_comparison(successful_jobs, num_examples=5)
    
    # Check for regressions
    print(f"\n⚠️ CHECKING FOR POTENTIAL REGRESSIONS")
    print("-" * 50)
    
    regressions = analyze_potential_regressions()
    
    if regressions:
        print(f"Found {len(regressions)} jobs with potential issues:")
        
        for reg in regressions[:10]:  # Show first 10
            print(f"  Job {reg['job_id']}: {reg['old_total']}→{reg['new_total']} fields")
            for issue in reg['issues']:
                print(f"    - {issue}")
    else:
        print("✅ No significant regressions found!")
    
    # Overall assessment
    all_lost = sum(job['lost_fields'] for job in successful_jobs)
    all_gained = sum(job['gained_fields'] for job in successful_jobs)
    
    print(f"\n🎯 OVERALL ASSESSMENT FOR PREVIOUSLY SUCCESSFUL JOBS")
    print("-" * 60)
    print(f"✅ Jobs analyzed: {len(successful_jobs)}")
    print(f"📈 Total fields gained: {all_gained}")
    print(f"📉 Total fields lost: {all_lost}")
    print(f"📊 Net improvement: +{all_gained - all_lost} fields")
    
    if all_lost == 0:
        print(f"🏆 PERFECT: No content lost from previously successful jobs!")
    elif all_lost < all_gained / 10:
        print(f"🎉 EXCELLENT: Minimal losses compared to gains!")
    else:
        print(f"⚠️  REVIEW NEEDED: Significant losses detected")

if __name__ == "__main__":
    main()