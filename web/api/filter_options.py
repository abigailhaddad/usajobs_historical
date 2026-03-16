import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

import duckdb

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from data_loader import get_parquet_path
from columns import DROPDOWN_FIELDS, MULTI_VALUE_FIELDS, parse_filters


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

            field = params.get('field', [''])[0].strip()

            if not field or field not in DROPDOWN_FIELDS:
                self.send_response(400)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({
                    'error': f'field must be one of: {", ".join(sorted(DROPDOWN_FIELDS))}'
                }).encode('utf-8'))
                return

            clauses, bind_values = parse_filters(params)
            where_sql = f'WHERE {" AND ".join(clauses)}' if clauses else ''

            conn = duckdb.connect(':memory:', read_only=False)
            parquet_path = get_parquet_path()

            if field in MULTI_VALUE_FIELDS:
                # For semicolon-delimited fields, split and return individual values
                query = (
                    f"SELECT DISTINCT TRIM(unnest(string_split(CAST(\"{field}\" AS VARCHAR), '; '))) AS val "
                    f"FROM read_parquet('{parquet_path}') "
                    f"{where_sql} "
                    f"{'AND' if where_sql else 'WHERE'} \"{field}\" IS NOT NULL "
                    f"AND TRIM(CAST(\"{field}\" AS VARCHAR)) != '' "
                    f"ORDER BY val"
                )
            else:
                query = (
                    f"SELECT DISTINCT TRIM(CAST(\"{field}\" AS VARCHAR)) AS val "
                    f"FROM read_parquet('{parquet_path}') "
                    f"{where_sql} "
                    f"{'AND' if where_sql else 'WHERE'} \"{field}\" IS NOT NULL "
                    f"AND TRIM(CAST(\"{field}\" AS VARCHAR)) != '' "
                    f"ORDER BY val"
                )

            rows = conn.execute(query, bind_values).fetchall()
            conn.close()

            values = [r[0] for r in rows if r[0] is not None and r[0].strip()]

            response = {
                'values': values,
                'count': len(values),
            }

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Cache-Control', 'public, max-age=3600')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode('utf-8'))

        except Exception as e:
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))
