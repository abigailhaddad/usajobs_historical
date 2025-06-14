#!/usr/bin/env python3
"""
Comprehensive Field Discovery

Analyzes all HTML files to discover ALL possible headers/sections that exist
in USAJobs postings so we can decide what to extract and how to categorize them.
"""

import random
import json
import sys
from pathlib import Path
from collections import defaultdict, Counter
import pandas as pd
from datetime import datetime
import re

# Add the pipeline scripts to path
sys.path.append('usajobs_pipeline/scripts')
from scrape_enhanced_job_posting import parse_all_sections
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

def extract_all_headers_and_content(html_path):
    """Extract ALL headers and their content from an HTML file"""
    try:
        with open(html_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script, style elements first
        for elem in soup(['script', 'style']):
            elem.decompose()
        
        # Focus on main content area
        main = (soup.find('div', class_='usajobs-joa-overview') or 
                soup.find('div', class_='usajobs-joa') or
                soup.find('div', {'id': 'duties'}).parent if soup.find('div', {'id': 'duties'}) else None or
                soup.find('main') or 
                soup.body)
        
        if not main:
            return {}
        
        # Find all potential headers with their tag types
        headers = main.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'dt', 'strong'])
        
        all_sections = {}
        
        for header in headers:
            header_text = header.get_text(strip=True)
            
            # Skip very long "headers" or empty ones
            if not header_text or len(header_text) > 200:
                continue
            
            # Get basic content after this header (simplified extraction)
            content_parts = []
            current = header.find_next_sibling()
            content_found = False
            
            while current and len(content_parts) < 3:  # Limit to avoid getting too much
                if current.name in ['h1', 'h2', 'h3', 'h4', 'h5']:
                    break
                if current.name in ['dt', 'strong'] and len(current.get_text(strip=True)) < 100:
                    break
                    
                if current.name:
                    text = current.get_text(separator=' ', strip=True)
                    if text:
                        content_parts.append(text)
                        content_found = True
                        
                current = current.find_next_sibling()
            
            # Store with metadata
            content = '\n'.join(content_parts).strip()
            
            all_sections[header_text] = {
                'tag': header.name,
                'content': content,
                'content_length': len(content),
                'has_content': len(content) > 0
            }
        
        return all_sections
        
    except Exception as e:
        print(f"❌ Error processing {html_path}: {e}")
        return {}

def categorize_headers(all_headers_data):
    """Categorize headers into logical groups"""
    
    # Define categories with keywords/patterns
    categories = {
        'job_overview': [
            'summary', 'overview', 'position summary', 'job summary', 'about', 'description'
        ],
        'duties_responsibilities': [
            'duties', 'major duties', 'responsibilities', 'key duties', 'what you will do', 
            'job duties', 'primary duties', 'essential duties'
        ],
        'qualifications': [
            'qualifications', 'qualification', 'required qualifications', 'minimum qualifications',
            'qualification summary', 'basic requirements', 'requirements', 'skills', 'experience'
        ],
        'education': [
            'education', 'educational requirements', 'education requirements', 'degree', 'transcript'
        ],
        'application_process': [
            'how to apply', 'application', 'application process', 'application instructions',
            'apply', 'application procedures'
        ],
        'evaluation': [
            'evaluation', 'how you will be evaluated', 'rating', 'ranking', 'selection', 
            'assessment', 'evaluation process'
        ],
        'benefits_compensation': [
            'benefits', 'compensation', 'salary', 'pay', 'what we offer', 'perks'
        ],
        'required_documents': [
            'required documents', 'documents required', 'documentation', 'documents needed'
        ],
        'next_steps': [
            'what to expect next', 'next steps', 'after you apply', 'selection process'
        ],
        'additional_info': [
            'additional information', 'other information', 'additional info', 'notes', 'important'
        ],
        'job_details': [
            'location', 'travel', 'schedule', 'work schedule', 'appointment type', 'series',
            'grade', 'security clearance', 'telework', 'remote'
        ],
        'conditions': [
            'conditions of employment', 'conditions', 'employment conditions', 'requirements'
        ],
        'agency_info': [
            'agency', 'organization', 'about the agency', 'department', 'office'
        ]
    }
    
    categorized = defaultdict(list)
    uncategorized = []
    
    for header, data in all_headers_data.items():
        header_lower = header.lower()
        found_category = False
        
        for category, keywords in categories.items():
            for keyword in keywords:
                if keyword in header_lower:
                    categorized[category].append((header, data))
                    found_category = True
                    break
            if found_category:
                break
        
        if not found_category:
            uncategorized.append((header, data))
    
    return dict(categorized), uncategorized

def analyze_header_patterns(all_headers_counter):
    """Analyze patterns in header text to find common variations"""
    
    # Group similar headers
    patterns = defaultdict(list)
    
    for header, count in all_headers_counter.items():
        # Normalize header for pattern matching
        normalized = re.sub(r'[^\w\s]', '', header.lower()).strip()
        normalized = re.sub(r'\s+', ' ', normalized)
        
        # Group by first few words
        words = normalized.split()
        if len(words) >= 2:
            pattern = ' '.join(words[:2])
        elif len(words) == 1:
            pattern = words[0]
        else:
            pattern = 'empty'
            
        patterns[pattern].append((header, count))
    
    return patterns

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Discover all possible fields in HTML files')
    parser.add_argument('--sample-size', type=int, default=1000, 
                       help='Number of files to analyze (default: 1000)')
    parser.add_argument('--output', '-o', default='all_fields_analysis.json',
                       help='Output file for results')
    parser.add_argument('--min-frequency', type=int, default=5,
                       help='Minimum frequency to show a header (default: 5)')
    parser.add_argument('--seed', type=int, default=42,
                       help='Random seed for reproducible sampling')
    
    args = parser.parse_args()
    
    random.seed(args.seed)
    print(f"🎲 Using random seed: {args.seed}")
    
    # Find all HTML files
    html_files = find_all_html_files()
    if not html_files:
        return
    
    # Sample files
    sample_size = min(args.sample_size, len(html_files))
    sampled_files = random.sample(html_files, sample_size)
    
    print(f"🔍 Analyzing ALL headers in {sample_size} files...")
    
    # Collect all headers across all files
    all_headers_counter = Counter()
    all_headers_by_tag = defaultdict(Counter)
    all_headers_data = {}
    header_content_examples = defaultdict(list)
    
    for i, html_file in enumerate(sampled_files, 1):
        if i % 100 == 0:
            print(f"   Processed {i}/{sample_size} files...")
        
        sections = extract_all_headers_and_content(html_file)
        
        for header_text, data in sections.items():
            all_headers_counter[header_text] += 1
            all_headers_by_tag[data['tag']][header_text] += 1
            
            # Store detailed data for the most common occurrence
            if header_text not in all_headers_data or all_headers_counter[header_text] == 1:
                all_headers_data[header_text] = data
            
            # Collect content examples
            if data['has_content'] and len(header_content_examples[header_text]) < 3:
                content_sample = data['content'][:200] + '...' if len(data['content']) > 200 else data['content']
                header_content_examples[header_text].append(content_sample)
    
    print(f"\n📊 COMPREHENSIVE FIELD ANALYSIS")
    print("=" * 60)
    print(f"📋 Total unique headers found: {len(all_headers_counter)}")
    print(f"📋 Headers appearing ≥{args.min_frequency} times: {len([h for h, c in all_headers_counter.items() if c >= args.min_frequency])}")
    
    # Show most common headers
    print(f"\n🏆 TOP HEADERS (by frequency)")
    print("-" * 40)
    for header, count in all_headers_counter.most_common(30):
        percentage = count / sample_size * 100
        tag = all_headers_data[header]['tag']
        has_content = '✓' if all_headers_data[header]['has_content'] else '✗'
        print(f"{count:4d} ({percentage:5.1f}%) [{tag}] {has_content} {header}")
    
    # Show by HTML tag type
    print(f"\n🏷️  HEADERS BY TAG TYPE")
    print("-" * 40)
    for tag in ['h1', 'h2', 'h3', 'h4', 'h5', 'dt', 'strong']:
        tag_headers = all_headers_by_tag[tag]
        if tag_headers:
            total = sum(tag_headers.values())
            unique = len(tag_headers)
            print(f"{tag.upper():8}: {total:5d} occurrences, {unique:3d} unique headers")
            
            # Show top headers for this tag
            for header, count in tag_headers.most_common(5):
                if count >= args.min_frequency:
                    percentage = count / sample_size * 100
                    print(f"         {count:4d} ({percentage:4.1f}%) {header}")
    
    # Categorize headers
    print(f"\n📂 SUGGESTED CATEGORIZATION")
    print("-" * 40)
    categorized, uncategorized = categorize_headers(all_headers_data)
    
    for category, headers in categorized.items():
        if headers:
            category_total = sum(all_headers_counter[h[0]] for h in headers)
            print(f"\n{category.upper().replace('_', ' ')} ({len(headers)} headers, {category_total} total occurrences):")
            
            # Sort by frequency within category
            sorted_headers = sorted(headers, key=lambda x: all_headers_counter[x[0]], reverse=True)
            for header, data in sorted_headers[:10]:  # Show top 10 per category
                count = all_headers_counter[header]
                if count >= args.min_frequency:
                    percentage = count / sample_size * 100
                    print(f"   {count:4d} ({percentage:4.1f}%) {header}")
    
    # Show uncategorized high-frequency headers
    print(f"\n❓ UNCATEGORIZED HIGH-FREQUENCY HEADERS")
    print("-" * 40)
    uncategorized_sorted = sorted(uncategorized, key=lambda x: all_headers_counter[x[0]], reverse=True)
    for header, data in uncategorized_sorted[:20]:
        count = all_headers_counter[header]
        if count >= args.min_frequency:
            percentage = count / sample_size * 100
            tag = data['tag']
            print(f"   {count:4d} ({percentage:4.1f}%) [{tag}] {header}")
    
    # Analyze header patterns
    print(f"\n🔍 HEADER PATTERNS")
    print("-" * 40)
    patterns = analyze_header_patterns(all_headers_counter)
    common_patterns = sorted(patterns.items(), key=lambda x: sum(count for _, count in x[1]), reverse=True)
    
    for pattern, headers in common_patterns[:15]:
        total_count = sum(count for _, count in headers)
        if total_count >= args.min_frequency * 2:
            print(f"\n'{pattern}' pattern ({total_count} total occurrences):")
            for header, count in sorted(headers, key=lambda x: x[1], reverse=True)[:5]:
                if count >= args.min_frequency:
                    percentage = count / sample_size * 100
                    print(f"   {count:4d} ({percentage:4.1f}%) {header}")
    
    # Save detailed results
    output_data = {
        'analysis_info': {
            'timestamp': datetime.now().isoformat(),
            'files_analyzed': sample_size,
            'total_unique_headers': len(all_headers_counter),
            'min_frequency_threshold': args.min_frequency
        },
        'header_frequencies': dict(all_headers_counter.most_common()),
        'headers_by_tag': {tag: dict(counter.most_common()) for tag, counter in all_headers_by_tag.items()},
        'categorized_headers': {cat: [(h[0], all_headers_counter[h[0]]) for h in headers] for cat, headers in categorized.items()},
        'uncategorized_headers': [(h[0], all_headers_counter[h[0]]) for h in uncategorized],
        'header_content_examples': dict(header_content_examples),
        'header_patterns': {pattern: [(h, c) for h, c in headers] for pattern, headers in patterns.items()}
    }
    
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Detailed analysis saved to: {args.output}")
    print(f"✅ Analysis complete! Found {len(all_headers_counter)} unique header types.")

if __name__ == "__main__":
    main()