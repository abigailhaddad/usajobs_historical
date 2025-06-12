#!/usr/bin/env python3
"""
USAJobs Historic Data Puller - Historical API Workflow Wrapper

This is a wrapper script that calls the shared historical API module
with the appropriate output directory for the historical_api workflow.

Usage:
    python historic_pull_parallel.py --start-date 2023-01-01 --end-date 2023-12-31
    python historic_pull_parallel.py --start-date 2023-01-01 --end-date 2023-12-31 --load-to-postgres --workers 8
"""

import sys
import subprocess
from pathlib import Path

def main():
    # Get the shared script path
    shared_script = Path(__file__).parent.parent.parent.parent.parent / "shared" / "api" / "historic_pull_parallel.py"
    
    # Get the data directory for this workflow
    data_dir = Path(__file__).parent.parent.parent / "data"
    
    # Add output-dir to the arguments if not already present
    args = sys.argv[1:]
    if "--output-dir" not in args:
        args.extend(["--output-dir", str(data_dir)])
    
    # Call the shared script
    cmd = [sys.executable, str(shared_script)] + args
    result = subprocess.run(cmd, cwd=shared_script.parent)
    
    return result.returncode

if __name__ == "__main__":
    sys.exit(main())