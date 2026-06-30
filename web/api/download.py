import csv
import io
import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

import duckdb

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from data_loader import get_parquet_path, get_conn
from columns import COLUMNS, COLUMN_HEADERS, parse_filters

# Max rows we will export in one CSV. A full unfiltered export is ~2.9M rows /
# ~140 MB, which exceeds the serverless response limits; streamed exports in the
# tens-of-MB range work fine. Above this we return a clear 413 so the UI can ask
# the user to add filters instead of failing opaquely.
MAX_ROWS = 150000


class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def _send_json(self, status, obj):
        body = json.dumps(obj).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)

            # Build WHERE from filters
            filter_clauses, bind_values = parse_filters(params)
            where_sql = f'WHERE {" AND ".join(filter_clauses)}' if filter_clauses else ''

            conn = get_conn()
            parquet_path = get_parquet_path()

            # Guard oversized exports up front so the client gets an actionable
            # message rather than an opaque function failure.
            row_count = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{parquet_path}') {where_sql}",
                bind_values,
            ).fetchone()[0]
            if row_count > MAX_ROWS:
                conn.close()
                self._send_json(413, {
                    'error': 'too_many_rows',
                    'rows': row_count,
                    'limit': MAX_ROWS,
                    'message': (
                        f'This export has {row_count:,} rows, which is too large to '
                        f'download here. Add filters to narrow it to {MAX_ROWS:,} rows or fewer.'
                    ),
                })
                return

            col_list = ', '.join([f'COALESCE(CAST("{c}" AS VARCHAR), \'\')' for c in COLUMNS])
            query = (
                f"SELECT {col_list} FROM read_parquet('{parquet_path}') "
                f"{where_sql} "
                f"ORDER BY COALESCE(CAST(\"openDate\" AS VARCHAR), '') DESC "
                f"LIMIT {MAX_ROWS}"
            )
            cursor = conn.execute(query, bind_values)

            self.send_response(200)
            self.send_header('Content-Type', 'text/csv; charset=utf-8')
            self.send_header('Content-Disposition', 'attachment; filename="usajobs_export.csv"')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()

            # Stream the CSV in batches (Vercel Fluid Compute streams the body),
            # so we never hold the whole result + its encoded copy in memory.
            buf = io.StringIO()
            writer = csv.writer(buf)

            def flush():
                self.wfile.write(buf.getvalue().encode('utf-8'))
                buf.seek(0)
                buf.truncate(0)

            writer.writerow(COLUMN_HEADERS)
            flush()
            while True:
                batch = cursor.fetchmany(10000)
                if not batch:
                    break
                writer.writerows(batch)
                flush()
            conn.close()

        except Exception:
            import traceback
            traceback.print_exc()
            self._send_json(500, {'error': 'internal server error'})
