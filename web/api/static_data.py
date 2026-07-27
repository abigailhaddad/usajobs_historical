import json
import os
import time
from http.server import BaseHTTPRequestHandler


_LOCAL_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'static.json')
_TMP_PATH = '/tmp/static.json'
_MAX_AGE_SECONDS = 300


def _get_static_data():
    """Return static.json contents, downloading from R2 if needed."""
    endpoint_url = os.environ.get('R2_ENDPOINT_URL')
    access_key = os.environ.get('R2_ACCESS_KEY_ID')
    secret_key = os.environ.get('R2_SECRET_ACCESS_KEY')

    if not all([endpoint_url, access_key, secret_key]):
        with open(_LOCAL_PATH) as f:
            return f.read()

    if os.path.exists(_TMP_PATH):
        age = time.time() - os.path.getmtime(_TMP_PATH)
        if age < _MAX_AGE_SECONDS:
            with open(_TMP_PATH) as f:
                return f.read()

    # Never write the cache in place: boto3's download_file streams to its
    # destination and is not atomic, so a concurrent request on the same Fluid
    # instance could read a half-written file and fail to parse it. Download to
    # a unique temp file, then os.replace() it in — replace is atomic on the
    # same filesystem, so readers see either the old or the new file, never a
    # partial one. (Same reasoning as get_parquet_path in data_loader.py; this
    # file is small enough not to need that one's single-flight lock.)
    import boto3
    import tempfile

    s3 = boto3.client('s3', endpoint_url=endpoint_url,
                       aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    fd, tmp_download = tempfile.mkstemp(dir='/tmp', prefix='static_', suffix='.json')
    os.close(fd)
    try:
        s3.download_file('usajobs-data', 'web/static.json', tmp_download)
        os.replace(tmp_download, _TMP_PATH)
    except BaseException:
        if os.path.exists(tmp_download):
            os.remove(tmp_download)
        raise

    with open(_TMP_PATH) as f:
        return f.read()


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            data = _get_static_data()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Cache-Control', 'public, max-age=3600')
            self.end_headers()
            self.wfile.write(data.encode('utf-8'))
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))
