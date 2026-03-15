#!/usr/bin/env python3
"""Local test server that mimics Vercel's routing for Python API functions."""
import http.server
import json
import os
import sys
import importlib.util
from urllib.parse import urlparse, parse_qs

PORT = 3333
WEB_DIR = os.path.dirname(os.path.abspath(__file__))

class LocalHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path.startswith("/api/"):
            self._handle_api(parsed)
        else:
            super().do_GET()

    def _handle_api(self, parsed):
        # Map /api/jobs -> web/api/jobs.py
        endpoint = parsed.path.split("/api/")[1].split("?")[0]
        module_path = os.path.join(WEB_DIR, "api", f"{endpoint}.py")

        if not os.path.exists(module_path):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'{"error": "not found"}')
            return

        # Load the module
        spec = importlib.util.spec_from_file_location(endpoint, module_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

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
