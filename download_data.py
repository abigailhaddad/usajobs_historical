#!/usr/bin/env python3
"""
Download USAJobs data from Cloudflare R2.

Usage:
    python download_data.py                # Download all full parquet files (40+ fields)
    python download_data.py --web-only     # Download just the web file (14 columns, single file)
    python download_data.py --zip          # Download all and create a zip archive
    python download_data.py --out-dir .    # Download to current directory
"""

import argparse
import os
import urllib.request
import zipfile

R2_BASE = "https://pub-317c58882ec04f329b63842c1eb65b0c.r2.dev"

HISTORICAL_FILES = [f"data/historical_jobs_{y}.parquet" for y in range(2013, 2027)]
CURRENT_FILES = [f"data/current_jobs_{y}.parquet" for y in range(2024, 2027)]
WEB_FILE = "web/jobs_5yr.parquet"


def download_file(url, dest):
    """Download a file with progress indication."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "usajobs-download/1.0"})
        response = urllib.request.urlopen(req)
        size = int(response.headers.get("Content-Length", 0))
        size_mb = size / (1024 * 1024) if size else 0

        with open(dest, "wb") as f:
            downloaded = 0
            while True:
                chunk = response.read(1024 * 1024)  # 1MB chunks
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                if size:
                    pct = downloaded / size * 100
                    print(f"\r  {downloaded / 1024 / 1024:.1f} / {size_mb:.1f} MB ({pct:.0f}%)", end="", flush=True)

        print()
        return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            print(f"  not found (skipping)")
            return False
        raise


def main():
    parser = argparse.ArgumentParser(description="Download USAJobs dataset from Cloudflare R2")
    parser.add_argument("--out-dir", default="data", help="Output directory (default: data)")
    parser.add_argument("--web-only", action="store_true",
                        help="Download just the web parquet (14 columns, ~100MB, single file)")
    parser.add_argument("--zip", action="store_true", help="Also create a zip archive")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    if args.web_only:
        print("Downloading web dataset (14 columns, deduplicated, all years in one file):\n")
        dest = os.path.join(args.out_dir, "jobs_5yr.parquet")
        print("jobs_5yr.parquet:")
        if download_file(f"{R2_BASE}/{WEB_FILE}", dest):
            print(f"\nSaved to {dest}")
            print("Columns: usajobsControlNumber, positionTitle, hiringDepartmentName,")
            print("  hiringAgencyName, grade, minimumSalary, maximumSalary, openDate,")
            print("  closeDate, appointmentType, serviceType, locations, status,")
            print("  occupationalSeries")
        return

    files = HISTORICAL_FILES + CURRENT_FILES
    downloaded = []
    skipped = []

    print(f"Downloading full USAJobs dataset (40+ fields per file) to {args.out_dir}/\n")

    for r2_key in files:
        fname = os.path.basename(r2_key)
        url = f"{R2_BASE}/{r2_key}"
        dest = os.path.join(args.out_dir, fname)
        print(f"{fname}:")
        if download_file(url, dest):
            downloaded.append(dest)
        else:
            skipped.append(fname)

    print(f"\nDownloaded {len(downloaded)} files to {args.out_dir}/")
    if skipped:
        print(f"Skipped {len(skipped)} files (not found): {', '.join(skipped)}")

    if args.zip and downloaded:
        zip_path = "usajobs_data.zip"
        print(f"\nCreating {zip_path}...")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for path in downloaded:
                zf.write(path, os.path.basename(path))
        zip_size = os.path.getsize(zip_path) / (1024 * 1024)
        print(f"Created {zip_path} ({zip_size:.1f} MB)")


if __name__ == "__main__":
    main()
