#!/usr/bin/env python3
"""
Upload parquet files to Cloudflare R2 (S3-compatible).

GitHub Actions integration:
    Add a step after "Commit and push to data-updates branch" and before
    "Create Pull Request" in .github/workflows/daily-data-update.yml:

        - name: Sync data to Cloudflare R2
          if: env.CHANGES_MADE == 'true'
          env:
            R2_ACCESS_KEY_ID: ${{ secrets.R2_ACCESS_KEY_ID }}
            R2_SECRET_ACCESS_KEY: ${{ secrets.R2_SECRET_ACCESS_KEY }}
            R2_ENDPOINT_URL: ${{ secrets.R2_ENDPOINT_URL }}
          run: |
            pip install boto3
            python scripts/sync_to_r2.py

    Required GitHub repository secrets:
        - R2_ACCESS_KEY_ID: Cloudflare R2 access key
        - R2_SECRET_ACCESS_KEY: Cloudflare R2 secret key
        - R2_ENDPOINT_URL: e.g. https://<account_id>.r2.cloudflarestorage.com
"""

import argparse
import glob
import os
import sys

import boto3


BUCKET = "usajobs-data"


def get_r2_client():
    access_key = os.environ.get("R2_ACCESS_KEY_ID")
    secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")
    endpoint = os.environ.get("R2_ENDPOINT_URL")

    missing = []
    if not access_key:
        missing.append("R2_ACCESS_KEY_ID")
    if not secret_key:
        missing.append("R2_SECRET_ACCESS_KEY")
    if not endpoint:
        missing.append("R2_ENDPOINT_URL")

    if missing:
        print(f"Error: missing environment variables: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def upload_file(client, local_path, r2_key):
    size = os.path.getsize(local_path)
    size_mb = size / (1024 * 1024)
    print(f"  Uploading {local_path} -> {r2_key} ({size_mb:.1f} MB)")
    client.upload_file(local_path, BUCKET, r2_key)


def main():
    parser = argparse.ArgumentParser(description="Upload parquet files to Cloudflare R2")
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Directory containing parquet files (default: data)",
    )
    parser.add_argument(
        "--web-parquet",
        default="web/data/jobs_5yr.parquet",
        help="Path to web parquet file (default: web/data/jobs_5yr.parquet)",
    )
    args = parser.parse_args()

    client = get_r2_client()

    # Upload data/*.parquet files
    pattern = os.path.join(args.data_dir, "*.parquet")
    parquet_files = sorted(glob.glob(pattern))

    if not parquet_files:
        print(f"Warning: no parquet files found in {args.data_dir}/", file=sys.stderr)

    uploaded = 0

    print(f"Uploading data parquet files from {args.data_dir}/:")
    for path in parquet_files:
        filename = os.path.basename(path)
        r2_key = f"data/{filename}"
        upload_file(client, path, r2_key)
        uploaded += 1

    # Upload web parquet
    if os.path.exists(args.web_parquet):
        print(f"\nUploading web parquet:")
        upload_file(client, args.web_parquet, "web/jobs_5yr.parquet")
        uploaded += 1
    else:
        print(f"Warning: web parquet not found at {args.web_parquet}", file=sys.stderr)

    print(f"\nDone. {uploaded} file(s) uploaded to r2://{BUCKET}/")


if __name__ == "__main__":
    main()
