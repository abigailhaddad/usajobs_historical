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

    # Download from R2.
    #
    # Fluid Compute serves concurrent requests from a single instance, so we
    # must never write the cached file in place: boto3's download_file streams
    # to its destination and is not atomic, so another in-flight request could
    # read a half-written parquet (a DuckDB error, or silently wrong counts).
    # Download to a unique temp file, then os.replace() it into the cache path
    # — replace is atomic on the same filesystem, so readers always see either
    # the old complete file or the new complete file, never a partial one.
    import boto3
    import tempfile

    s3 = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    fd, tmp_download = tempfile.mkstemp(dir='/tmp', prefix='jobs_', suffix='.parquet')
    os.close(fd)
    try:
        s3.download_file(bucket_name, 'web/jobs_5yr.parquet', tmp_download)
        os.replace(tmp_download, _TMP_PATH)
    except BaseException:
        if os.path.exists(tmp_download):
            os.remove(tmp_download)
        raise

    return _TMP_PATH


def get_conn():
    """Return a DuckDB connection."""
    import duckdb
    return duckdb.connect(':memory:', read_only=False)
