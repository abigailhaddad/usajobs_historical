import os
import time


# Local fallback path (relative to this file)
_LOCAL_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'jobs_5yr.parquet')
_TMP_PATH = '/tmp/jobs_5yr.parquet'
_MAX_AGE_SECONDS = 300  # 5 minutes


def get_parquet_path():
    """Return the path to the parquet file, downloading from R2 if needed.

    When R2 environment variables are set, downloads the file to /tmp/ and
    caches it for up to 5 minutes. Falls back to the local file path when
    the env vars are not configured (local development).
    """
    endpoint_url = os.environ.get('R2_ENDPOINT_URL')
    access_key = os.environ.get('R2_ACCESS_KEY_ID')
    secret_key = os.environ.get('R2_SECRET_ACCESS_KEY')
    bucket_name = 'usajobs-data'

    # Fall back to local path if R2 is not configured
    if not all([endpoint_url, access_key, secret_key]):
        return _LOCAL_PATH

    # Check if cached file exists and is recent enough
    if os.path.exists(_TMP_PATH):
        age = time.time() - os.path.getmtime(_TMP_PATH)
        if age < _MAX_AGE_SECONDS:
            return _TMP_PATH

    # Download from R2
    import boto3

    s3 = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    s3.download_file(bucket_name, 'web/jobs_5yr.parquet', _TMP_PATH)

    return _TMP_PATH
