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
import time

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


BUCKET = "usajobs-data"

# current_jobs_2026.parquet passed 1.2 GB in August 2026 and grows ~10 MB/day.
# At boto3's default 8 MB chunk that is ~150 parts per upload, and R2 fails
# CompleteMultipartUpload more often the more parts it has to assemble (it took
# down the 2026-08-08 sync). Bigger chunks mean an order of magnitude fewer
# parts and far fewer chances to flake.
_CHUNK_SIZE = 64 * 1024 * 1024
TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=_CHUNK_SIZE,
    multipart_chunksize=_CHUNK_SIZE,
)

# boto3's default retry mode is 'legacy', which gives up after 4 attempts --
# that is the "reached max retries: 4" in the 2026-08-08 failure.
BOTO_CONFIG = Config(retries={"max_attempts": 10, "mode": "standard"})

# Retries above boto3's own, because a failed CompleteMultipartUpload needs the
# whole transfer restarted rather than the single request retried.
_UPLOAD_ATTEMPTS = 4
_BACKOFF_SECONDS = 5


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
        config=BOTO_CONFIG,
    )


def upload_file(client, local_path, r2_key):
    """Upload one file, retrying the whole transfer on a transient R2 error.

    Returns True on success. Raising here would abandon every file after this
    one, so a flake on a single parquet must not end the sync.
    """
    size_mb = os.path.getsize(local_path) / (1024 * 1024)
    print(f"  Uploading {local_path} -> {r2_key} ({size_mb:.1f} MB)", flush=True)

    for attempt in range(1, _UPLOAD_ATTEMPTS + 1):
        try:
            client.upload_file(local_path, BUCKET, r2_key, Config=TRANSFER_CONFIG)
            return True
        except (ClientError, BotoCoreError) as e:
            if attempt == _UPLOAD_ATTEMPTS:
                print(f"    FAILED after {attempt} attempts: {e}", file=sys.stderr, flush=True)
                return False
            delay = _BACKOFF_SECONDS * (2 ** (attempt - 1))
            print(f"    attempt {attempt} failed ({e}); retrying in {delay}s",
                  file=sys.stderr, flush=True)
            time.sleep(delay)


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

    uploaded = 0
    failed = []

    def send(local_path, r2_key):
        nonlocal uploaded
        if upload_file(client, local_path, r2_key):
            uploaded += 1
        else:
            failed.append(r2_key)

    # The website reads web/jobs_5yr.parquet and web/static.json, and nothing
    # else here. Upload them FIRST: on 2026-08-08 an R2 flake partway through
    # the data/ loop aborted the sync before these two were sent, so the site
    # served day-old data even though it had been generated successfully.
    if os.path.exists(args.web_parquet):
        print("Uploading web parquet:")
        send(args.web_parquet, "web/jobs_5yr.parquet")
    else:
        print(f"Warning: web parquet not found at {args.web_parquet}", file=sys.stderr)

    static_path = os.path.join(os.path.dirname(args.web_parquet), "static.json")
    if os.path.exists(static_path):
        print("\nUploading static data:")
        send(static_path, "web/static.json")

    # Then the per-year source files.
    pattern = os.path.join(args.data_dir, "*.parquet")
    parquet_files = sorted(glob.glob(pattern))

    if not parquet_files:
        print(f"Warning: no parquet files found in {args.data_dir}/", file=sys.stderr)

    print(f"\nUploading data parquet files from {args.data_dir}/:")
    for path in parquet_files:
        send(path, f"data/{os.path.basename(path)}")

    print(f"\nDone. {uploaded} file(s) uploaded to r2://{BUCKET}/")

    if failed:
        print(f"\n{len(failed)} file(s) FAILED to upload:", file=sys.stderr)
        for key in failed:
            print(f"  - {key}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
