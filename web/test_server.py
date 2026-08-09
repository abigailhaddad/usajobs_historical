#!/usr/bin/env python3
"""Local static file server for web/, with Vercel-style routing for whatever
Python functions are left in api/.

The site itself no longer needs the /api/ half. index.html, pivot.html,
poster.html and overlay-poster.html query the parquet on Cloudflare R2 directly
from the browser via shared/wasm-api.js (DuckDB-WASM over HTTP range requests),
so for those pages this is just a static file server — which is all
tests/test_frontend.py needs it for.

api/hiring_stats.py is the one endpoint still routed here, and hiring.html is
the only page that calls it. Both are deliberately parked, unfinished work; they
are excluded from deploys by .vercelignore, so this server is the only place
/api/hiring_stats resolves at all.

The six endpoints this used to serve — aggregate, jobs, pivot, filter_options,
download, static_data — were deleted on 2026-08-09. Requests for them get a 404
naming the replacement rather than a stack trace.
"""
import http.server
import json
import os
import sys
import importlib.util
from urllib.parse import urlparse, parse_qs

PORT = 3333
WEB_DIR = os.path.dirname(os.path.abspath(__file__))

# Deleted 2026-08-09 -> the shared/wasm-api.js export that replaced each one.
# Named explicitly so a stale caller gets told where the function went instead
# of a bare "not found".
DELETED_ENDPOINTS = {
    "aggregate": "call aggregate() from web/shared/wasm-api.js in the browser",
    "jobs": "call jobs() from web/shared/wasm-api.js in the browser",
    "pivot": "call pivot() from web/shared/wasm-api.js in the browser",
    "filter_options": "call filterOptions() from web/shared/wasm-api.js in the browser",
    "download": "call downloadCsv() from web/shared/wasm-api.js in the browser",
    "static_data": "index.html now fetches static.json straight from R2",
}

class LocalHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path.startswith("/api/"):
            self._handle_api(parsed)
        else:
            super().do_GET()

    def _json_error(self, code, message):
        body = json.dumps({"error": message}).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_api(self, parsed):
        # Map /api/hiring_stats -> web/api/hiring_stats.py
        endpoint = parsed.path.split("/api/")[1].split("?")[0]
        module_path = os.path.join(WEB_DIR, "api", f"{endpoint}.py")

        if endpoint in DELETED_ENDPOINTS:
            self._json_error(404, (
                f"/api/{endpoint} was deleted on 2026-08-09 — the site has no "
                f"server API. Instead: {DELETED_ENDPOINTS[endpoint]}."
            ))
            return

        if not os.path.exists(module_path):
            self._json_error(404, f"no such endpoint: api/{endpoint}.py does not exist")
            return

        # Load the module
        spec = importlib.util.spec_from_file_location(endpoint, module_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        # Not every module in api/ is an endpoint — columns.py and data_loader.py
        # are helper modules imported by hiring_stats.py. Without this check,
        # requesting one raises AttributeError outside the try below, which kills
        # the connection with a traceback and no response at all.
        if not hasattr(mod, "handler"):
            self._json_error(404, (
                f"api/{endpoint}.py defines no `handler` class, so it is not an "
                f"endpoint — it is a helper module imported by another one."
            ))
            return

        # Create a fake request object that mimics Vercel's BaseHTTPRequestHandler
        # The handler classes in the API files expect self.path, self.send_response, etc.
        # So we just call their do_GET with our own self
        handler_class = mod.handler

        # Create instance without calling __init__ (which tries to handle the request)
        fake = object.__new__(handler_class)
        fake.path = self.path
        fake.requestline = self.requestline
        fake.request_version = self.request_version
        fake.command = self.command
        fake.headers = self.headers
        fake.wfile = self.wfile
        fake.rfile = self.rfile
        fake._headers_buffer = []
        fake.responses = http.server.BaseHTTPRequestHandler.responses

        # Patch send_response and friends to use our connection
        fake.send_response = self.send_response
        fake.send_header = self.send_header
        fake.end_headers = self.end_headers
        fake.log_message = self.log_message
        fake.log_request = lambda *a: None

        try:
            fake.do_GET()
        except Exception as e:
            print(f"Error in {endpoint}: {e}")
            import traceback
            traceback.print_exc()
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())

if __name__ == "__main__":
    print(f"Starting local test server on http://localhost:{PORT}")
    print(f"Serving files from {WEB_DIR}")
    server = http.server.HTTPServer(("", PORT), LocalHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
