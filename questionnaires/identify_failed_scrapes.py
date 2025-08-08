#!/usr/bin/env python3
"""
Identify questionnaires that failed to scrape properly by checking for error messages
"""
import os
from pathlib import Path
import re

def is_error_file(filepath):
    """Check if a file contains an error message instead of questionnaire content"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Check for known error patterns
        error_patterns = [
            "We are not able to display the page requested at this time",
            "Please try refreshing the page",
            "Reference ID:",
            "404",
            "403",
            "Internal Server Error",
            "Access Denied",
            "Page Not Found"
        ]
        
        for pattern in error_patterns:
            if pattern in content:
                return True
                
        # Also check if file is suspiciously small (under 1KB) and doesn't contain question markers
        if len(content) < 1000:
            # Check if it has typical questionnaire content
            has_questions = any(marker in content for marker in ['?', 'experience', 'qualification', 'education'])
            if not has_questions:
                return True
                
        return False
        
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return False

def main():
    questionnaire_dir = Path('raw_questionnaires')
    
    if not questionnaire_dir.exists():
        print("raw_questionnaires directory not found!")
        return
        
    failed_files = []
    total_files = 0
    
    # Check all txt files
    for txt_file in questionnaire_dir.glob('*.txt'):
        total_files += 1
        if is_error_file(txt_file):
            failed_files.append(txt_file.name)
    
    print(f"Total questionnaire files: {total_files:,}")
    print(f"Failed/error files: {len(failed_files):,}")
    print(f"Success rate: {((total_files - len(failed_files)) / total_files * 100):.1f}%")
    
    if failed_files:
        print("\nFailed files:")
        
        # Group by source
        usastaffing_failed = [f for f in failed_files if f.startswith('usastaffing_')]
        monster_failed = [f for f in failed_files if f.startswith('monster_')]
        
        if usastaffing_failed:
            print(f"\nUSAStaffing ({len(usastaffing_failed)} files):")
            for f in sorted(usastaffing_failed)[:10]:
                print(f"  {f}")
            if len(usastaffing_failed) > 10:
                print(f"  ... and {len(usastaffing_failed) - 10} more")
                
        if monster_failed:
            print(f"\nMonster ({len(monster_failed)} files):")
            for f in sorted(monster_failed)[:10]:
                print(f"  {f}")
            if len(monster_failed) > 10:
                print(f"  ... and {len(monster_failed) - 10} more")
        
        # Save list of failed files for potential re-scraping
        with open('failed_questionnaires.txt', 'w') as f:
            for filename in sorted(failed_files):
                # Extract the ID from the filename
                match = re.search(r'(\w+)_(\d+)\.txt', filename)
                if match:
                    source = match.group(1)
                    id_num = match.group(2)
                    f.write(f"{source},{id_num}\n")
        
        print(f"\nList of failed questionnaires saved to: failed_questionnaires.txt")

if __name__ == "__main__":
    main()