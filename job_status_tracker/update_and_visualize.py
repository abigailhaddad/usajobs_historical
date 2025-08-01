#!/usr/bin/env python3
"""
Update all 2025 job statuses and regenerate visualization data

This script:
1. Updates all active job statuses for 2025
2. Regenerates the visualization data
"""

import subprocess
import sys
import os
from datetime import datetime

def main():
    print("🚀 USAJobs Status Update & Visualization Pipeline")
    print("=" * 80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Step 1: Update active job statuses for 2025
    print("📊 Step 1: Updating active job statuses for 2025...")
    print("-" * 40)
    
    update_cmd = [
        sys.executable,
        "update_active_statuses.py",
        "--year", "2025",
        "--data-dir", "../data"
    ]
    
    try:
        result = subprocess.run(update_cmd, check=True)
        print("✅ Status update completed successfully")
    except subprocess.CalledProcessError as e:
        print(f"❌ Status update failed: {e}")
        sys.exit(1)
    
    print()
    
    # Step 2: Regenerate visualization data
    print("📈 Step 2: Regenerating visualization data...")
    print("-" * 40)
    
    extract_cmd = [
        sys.executable,
        "extract_job_status_data.py"
    ]
    
    try:
        result = subprocess.run(extract_cmd, check=True)
        print("✅ Visualization data generated successfully")
    except subprocess.CalledProcessError as e:
        print(f"❌ Data extraction failed: {e}")
        sys.exit(1)
    
    print()
    print("🎉 Pipeline completed successfully!")
    print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("📌 Next steps:")
    print("   1. Start the web server: python3 -m http.server 8000")
    print("   2. Open http://localhost:8000 in your browser")

if __name__ == "__main__":
    main()