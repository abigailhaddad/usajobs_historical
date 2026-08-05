import glob
import os
import time


# Local fallback path (relative to this file)
_LOCAL_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'jobs_5yr.parquet')
_TMP_PATH = '/tmp/jobs_5yr.parquet'
_LOCK_PATH = '/tmp/jobs_5yr.parquet.lock'

_BUCKET = 'usajobs-data'
_KEY = 'web/jobs_5yr.parquet'

# The parquet only changes on the daily data update, so a 5-minute TTL bought
# nothing and forced a ~120 MB re-download every 5 minutes.
_MAX_AGE_SECONDS = 1800
# A lock held longer than this belongs to an instance that died mid-download.
_LOCK_STALE_SECONDS = 300
# Cold start with no cached file: how long a waiter gives the downloader.
_LOCK_WAIT_SECONDS = 90


def _is_fresh(path):
    try:
        return time.time() - os.path.getmtime(path) < _MAX_AGE_SECONDS
    except OSError:
        return False


def _try_create_lock():
    try:
        fd = os.open(_LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        return True
    except FileExistsError:
        return False


def _acquire_lock():
    """Try to become the one request that downloads. True if we hold the lock."""
    if _try_create_lock():
        return True
    # A lock left behind by an instance killed mid-download would block every
    # later request forever, so time it out and take over.
    try:
        if time.time() - os.path.getmtime(_LOCK_PATH) > _LOCK_STALE_SECONDS:
            os.unlink(_LOCK_PATH)
            return _try_create_lock()
    except OSError:
        pass
    return False


def _release_lock():
    try:
        os.unlink(_LOCK_PATH)
    except OSError:
        pass


def _sweep_orphans():
    """Delete temp downloads abandoned by a request that died before replacing."""
    for path in glob.glob('/tmp/jobs_*.parquet'):
        if path == _TMP_PATH:
            continue
        try:
            if time.time() - os.path.getmtime(path) > _LOCK_STALE_SECONDS:
                os.remove(path)
        except OSError:
            pass


def _wait_for_download():
    """Block until the lock holder publishes the cache. Only used on a cold start."""
    deadline = time.time() + _LOCK_WAIT_SECONDS
    while time.time() < deadline:
        time.sleep(0.5)
        if os.path.exists(_TMP_PATH):
            return _TMP_PATH
        if not os.path.exists(_LOCK_PATH):
            # The holder released without publishing a file — its download failed.
            break
    raise RuntimeError('timed out waiting for the parquet download to finish')


def _download(endpoint_url, access_key, secret_key):
    # Never write the cached file in place: boto3's download_file streams to its
    # destination and is not atomic, so another in-flight request could read a
    # half-written parquet (a DuckDB error, or silently wrong counts). Download
    # to a unique temp file, then os.replace() it into the cache path — replace
    # is atomic on the same filesystem, so readers always see either the old
    # complete file or the new complete file, never a partial one.
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
        s3.download_file(_BUCKET, _KEY, tmp_download)
        os.replace(tmp_download, _TMP_PATH)
    except BaseException:
        if os.path.exists(tmp_download):
            os.remove(tmp_download)
        raise


def get_parquet_path():
    """Return the path to the parquet file, downloading from R2 if needed.

    When R2 environment variables are set, downloads the file to /tmp/ and
    caches it. Falls back to the local file path when the env vars are not
    configured (local development).

    Fluid Compute serves concurrent requests from one instance sharing one
    512 MB /tmp, and the parquet is ~120 MB. Letting every request in a cache
    refresh window download its own copy filled the disk, so every one of them
    500'd. Exactly one request downloads; the rest serve the stale cache, which
    is the right trade for a file that changes once a day.
    """
    endpoint_url = os.environ.get('R2_ENDPOINT_URL')
    access_key = os.environ.get('R2_ACCESS_KEY_ID')
    secret_key = os.environ.get('R2_SECRET_ACCESS_KEY')

    # Fall back to local path if R2 is not configured
    if not all([endpoint_url, access_key, secret_key]):
        return _LOCAL_PATH

    if _is_fresh(_TMP_PATH):
        return _TMP_PATH

    have_stale = os.path.exists(_TMP_PATH)

    if not _acquire_lock():
        if have_stale:
            return _TMP_PATH
        return _wait_for_download()

    try:
        _sweep_orphans()
        _download(endpoint_url, access_key, secret_key)
    except Exception:
        if not have_stale:
            raise
        # Serving a stale parquet beats serving a 500.
        return _TMP_PATH
    finally:
        _release_lock()

    return _TMP_PATH


def get_conn():
    """Return a DuckDB connection."""
    import duckdb
    return duckdb.connect(':memory:', read_only=False)
