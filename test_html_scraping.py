#!/usr/bin/env python3
"""
Test HTML Scraping Validation

Randomly samples existing HTML files from the cache and tests our scraping process
to validate field extraction is working properly.
"""

import random
import json
import sys
from pathlib import Path
from collections import defaultdict, Counter
import pandas as pd
from datetime import datetime

# Add the pipeline scripts to path
sys.path.append('usajobs_pipeline/scripts')
from scrape_enhanced_job_posting import parse_all_sections, map_sections_to_fields, TARGET_SECTIONS
from bs4 import BeautifulSoup

def find_all_html_files():
    """Find all HTML files in the cache"""
    html_cache = Path("html_cache")
    if not html_cache.exists():
        print("❌ html_cache directory not found")
        return []
    
    html_files = list(html_cache.rglob("*.html"))
    print(f"📁 Found {len(html_files)} HTML files in cache")
    return html_files

def extract_control_number_from_path(html_path):
    """Extract control number from file path like html_cache/831/518/831518100.html"""
    return html_path.stem

def test_single_html_file(html_path):
    """Test scraping on a single HTML file"""
    control_number = extract_control_number_from_path(html_path)
    
    try:
        # Read the HTML file
        with open(html_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Test our parsing functions
        all_sections = parse_all_sections(soup)
        mapped_sections = map_sections_to_fields(all_sections)
        
        # Calculate some stats
        total_headers = len(all_sections)
        mapped_count = len(mapped_sections)
        total_content_length = sum(len(v) for v in mapped_sections.values())
        
        return {
            'control_number': control_number,
            'file_path': str(html_path),
            'status': 'success',
            'total_headers_found': total_headers,
            'target_fields_mapped': mapped_count,
            'all_headers': list(all_sections.keys()),
            'mapped_fields': list(mapped_sections.keys()),
            'content_sections': mapped_sections,
            'total_content_length': total_content_length,
            'file_size_bytes': html_path.stat().st_size
        }
        
    except Exception as e:
        return {
            'control_number': control_number,
            'file_path': str(html_path),
            'status': 'error',
            'error': str(e)
        }

def analyze_results(results):
    """Analyze the test results and generate statistics"""
    print("\n📊 ANALYSIS RESULTS")
    print("=" * 50)
    
    # Basic stats
    total_files = len(results)
    successful = [r for r in results if r['status'] == 'success']
    failed = [r for r in results if r['status'] == 'error']
    
    print(f"📋 Total files tested: {total_files}")
    print(f"✅ Successful extractions: {len(successful)} ({len(successful)/total_files*100:.1f}%)")
    print(f"❌ Failed extractions: {len(failed)} ({len(failed)/total_files*100:.1f}%)")
    
    if not successful:
        print("⚠️ No successful extractions to analyze")
        return
    
    # Field mapping success rates
    print(f"\n🎯 FIELD EXTRACTION RATES")
    field_counts = Counter()
    for result in successful:
        for field in result['mapped_fields']:
            field_counts[field] += 1
    
    target_field_names = list(TARGET_SECTIONS.keys())
    for field in target_field_names:
        count = field_counts[field]
        percentage = count / len(successful) * 100
        print(f"   {field:20}: {count:4d}/{len(successful)} ({percentage:5.1f}%)")
    
    # Content length stats
    content_lengths = [r['total_content_length'] for r in successful]
    avg_content = sum(content_lengths) / len(content_lengths)
    print(f"\n📝 CONTENT STATISTICS")
    print(f"   Average content length: {avg_content:,.0f} characters")
    print(f"   Min content length: {min(content_lengths):,} characters")
    print(f"   Max content length: {max(content_lengths):,} characters")
    
    # Header discovery stats
    header_counts = [r['total_headers_found'] for r in successful]
    avg_headers = sum(header_counts) / len(header_counts)
    print(f"   Average headers found: {avg_headers:.1f}")
    print(f"   Min headers found: {min(header_counts)}")
    print(f"   Max headers found: {max(header_counts)}")
    
    # Field mapping ratio
    mapping_ratios = [r['target_fields_mapped'] / r['total_headers_found'] * 100 
                     for r in successful if r['total_headers_found'] > 0]
    if mapping_ratios:
        avg_mapping_ratio = sum(mapping_ratios) / len(mapping_ratios)
        print(f"   Average mapping efficiency: {avg_mapping_ratio:.1f}% (mapped/total headers)")
    
    # Show some examples
    print(f"\n🔍 SAMPLE EXTRACTIONS")
    for i, result in enumerate(successful[:3]):
        print(f"\n   Example {i+1}: {result['control_number']}")
        print(f"   Headers found: {result['total_headers_found']}")
        print(f"   Fields mapped: {result['target_fields_mapped']}")
        print(f"   Mapped fields: {', '.join(result['mapped_fields'])}")
        
        # Show a snippet of content
        for field, content in result['content_sections'].items():
            snippet = content[:100] + "..." if len(content) > 100 else content
            print(f"   {field}: {snippet}")
            break  # Just show first field
    
    # Show errors if any
    if failed:
        print(f"\n❌ ERRORS ENCOUNTERED")
        error_types = Counter(r['error'] for r in failed)
        for error, count in error_types.most_common(5):
            print(f"   {error}: {count} occurrences")

def save_detailed_results(results, output_file):
    """Save detailed results to JSON file"""
    output_data = {
        'timestamp': datetime.now().isoformat(),
        'total_files_tested': len(results),
        'successful_extractions': len([r for r in results if r['status'] == 'success']),
        'failed_extractions': len([r for r in results if r['status'] == 'error']),
        'results': results
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"💾 Detailed results saved to: {output_file}")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Test HTML scraping on cached files')
    parser.add_argument('--sample-size', type=int, default=200, 
                       help='Number of files to randomly sample (default: 200)')
    parser.add_argument('--output', '-o', 
                       help='Output file for detailed results (JSON)')
    parser.add_argument('--seed', type=int, 
                       help='Random seed for reproducible sampling')
    
    args = parser.parse_args()
    
    if args.seed:
        random.seed(args.seed)
        print(f"🎲 Using random seed: {args.seed}")
    
    # Find all HTML files
    html_files = find_all_html_files()
    if not html_files:
        return
    
    # Sample files
    sample_size = min(args.sample_size, len(html_files))
    sampled_files = random.sample(html_files, sample_size)
    
    print(f"🎯 Testing scraping on {sample_size} randomly sampled files...")
    print(f"📂 Sample includes files from: {min(f.name for f in sampled_files)} to {max(f.name for f in sampled_files)}")
    
    # Test each file
    results = []
    for i, html_file in enumerate(sampled_files, 1):
        if i % 50 == 0:
            print(f"   Processed {i}/{sample_size} files...")
        
        result = test_single_html_file(html_file)
        results.append(result)
    
    # Analyze results
    analyze_results(results)
    
    # Save detailed results if requested
    if args.output:
        save_detailed_results(results, args.output)
    
    print(f"\n✅ Testing complete!")

if __name__ == "__main__":
    main()