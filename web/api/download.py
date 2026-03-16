import csv
import io
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

import duckdb

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from data_loader import get_parquet_path, get_conn
from columns import COLUMNS, COLUMN_HEADERS, parse_filters

MAX_ROWS = 500000


class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)

            # Build WHERE from filters
            filter_clauses, bind_values = parse_filters(params)
            where_sql = f'WHERE {" AND ".join(filter_clauses)}' if filter_clauses else ''

            col_list = ', '.join([f'COALESCE(CAST("{c}" AS VARCHAR), \'\')' for c in COLUMNS])

            conn = get_conn()
            parquet_path = get_parquet_path()

            query = (
                f"SELECT {col_list} FROM read_parquet('{parquet_path}') "
                f"{where_sql} "
                f"ORDER BY COALESCE(CAST(\"openDate\" AS VARCHAR), '') DESC "
                f"LIMIT {MAX_ROWS}"
            )
            rows = conn.execute(query, bind_values).fetchall()
            conn.close()

            # Write CSV to buffer
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(COLUMN_HEADERS)
            writer.writerows(rows)

            csv_bytes = buf.getvalue().encode('utf-8')

            self.send_response(200)
            self.send_header('Content-Type', 'text/csv; charset=utf-8')
            self.send_header('Content-Disposition', 'attachment; filename="usajobs_export.csv"')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Content-Length', str(len(csv_bytes)))
            self.end_headers()
            self.wfile.write(csv_bytes)

        except Exception as e:
            import json
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))
