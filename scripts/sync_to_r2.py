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
import json
import os
import sys
import time

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


BUCKET = "usajobs-data"

# Where --snapshot records what the data files looked like straight after the
# R2 download, so --only-changed can tell which ones collection actually wrote.
# Dot-prefixed and .json, so it matches neither the "*.parquet" upload glob nor
# anything the workflow commits. Also listed in .gitignore.
BASELINE_NAME = ".r2_baseline.json"

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


def _fingerprint(path):
    """(size, mtime_ns) — enough to tell whether our own pipeline rewrote a file.

    Only this pipeline writes these files during a run, and every writer either
    creates the file or renames a fresh temp over it, so a rewrite always moves
    mtime. A rewrite that produced byte-identical content would still be
    re-uploaded, which is the harmless direction to be wrong in.
    """
    st = os.stat(path)
    return {"size": st.st_size, "mtime_ns": st.st_mtime_ns}


def write_baseline(data_dir):
    """Record the post-download state of data/*.parquet. Needs no R2 access."""
    files = sorted(glob.glob(os.path.join(data_dir, "*.parquet")))
    baseline = {os.path.basename(p): _fingerprint(p) for p in files}

    path = os.path.join(data_dir, BASELINE_NAME)
    with open(path, "w") as f:
        json.dump(baseline, f, indent=2, sort_keys=True)

    total_mb = sum(e["size"] for e in baseline.values()) / (1024 * 1024)
    print(f"Baseline written to {path}: {len(baseline)} file(s), {total_mb:,.1f} MB")
    return 0


def load_baseline(data_dir):
    """Return the recorded baseline, or None if it is missing or unusable.

    None means "upload everything". Skipping an upload because we could not
    read the baseline would leave stale data in R2 with no error, so every
    failure path here has to fall back to the current, safe behaviour.
    """
    path = os.path.join(data_dir, BASELINE_NAME)
    if not os.path.exists(path):
        print(f"Warning: {path} not found — uploading every file.", file=sys.stderr)
        return None
    try:
        with open(path) as f:
            baseline = json.load(f)
        if not isinstance(baseline, dict):
            raise ValueError("baseline is not an object")
        return baseline
    except (OSError, ValueError) as e:
        print(f"Warning: could not read {path} ({e}) — uploading every file.",
              file=sys.stderr)
        return None


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
    parser.add_argument(
        "--snapshot",
        action="store_true",
        help="Record the current state of data/*.parquet and exit. Run this "
             "right after downloading from R2, before collection rewrites "
             "anything. Requires no R2 credentials.",
    )
    parser.add_argument(
        "--only-changed",
        action="store_true",
        help="Skip data files that are byte-for-byte what --snapshot recorded. "
             "Falls back to uploading everything if the baseline is missing.",
    )
    args = parser.parse_args()

    if args.snapshot:
        return write_baseline(args.data_dir)

    baseline = load_baseline(args.data_dir) if args.only_changed else None

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
    skipped = 0
    skipped_bytes = 0
    for path in parquet_files:
        name = os.path.basename(path)
        # A file absent from the baseline is new (e.g. the first file of a new
        # year), so it must be uploaded.
        if baseline is not None and baseline.get(name) == _fingerprint(path):
            skipped += 1
            skipped_bytes += os.path.getsize(path)
            print(f"  Unchanged, skipping {name}")
            continue
        send(path, f"data/{name}")

    if skipped:
        print(f"\nSkipped {skipped} unchanged file(s), {skipped_bytes / (1024 * 1024):,.1f} MB not re-uploaded.")

    print(f"\nDone. {uploaded} file(s) uploaded to r2://{BUCKET}/")

    if failed:
        print(f"\n{len(failed)} file(s) FAILED to upload:", file=sys.stderr)
        for key in failed:
            print(f"  - {key}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
