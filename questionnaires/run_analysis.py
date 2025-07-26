#!/usr/bin/env python3
"""
Run questionnaire analysis after update_all.py
"""
import subprocess
import sys
import os

# Run extraction and scraping
print("Extracting and scraping questionnaires...")
subprocess.run([
    sys.executable,
    'extract_questionnaires.py'
])

# Render Quarto analysis
print("\nRendering analysis...")
os.chdir('analysis')
subprocess.run(['quarto', 'render', 'executive_order_analysis.qmd'])
os.chdir('..')

print("\nDone! Open analysis/executive_order_analysis.html to view results.")