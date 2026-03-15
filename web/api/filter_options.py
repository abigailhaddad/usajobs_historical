import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

import duckdb

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from data_loader import get_parquet_path

ALLOWED_FIELDS = {
    'positionTitle', 'hiringAgencyName', 'hiringDepartmentName',
    'grade', 'appointmentType', 'serviceType', 'status',
}

VALID_COLUMNS = ALLOWED_FIELDS | {
    'minimumSalary', 'maximumSalary',
    'openDate', 'closeDate',
    'locations', 'usajobsControlNumber',
}


def _parse_filters(params):
    """Parse filter_ prefixed query params into WHERE clauses and bind values."""
    clauses = []
    bind_values = []

    for key, values in params.items():
        if not key.startswith('filter_'):
            continue

        param_name = key[len('filter_'):]
        value = values[0] if values else ''

        if not value:
            continue

        if param_name.endswith('_min'):
            col = param_name[:-4]
            if col in VALID_COLUMNS:
                clauses.append(f'CAST("{col}" AS DOUBLE) >= ?')
                bind_values.append(float(value))
            continue

        if param_name.endswith('_max'):
            col = param_name[:-4]
            if col in VALID_COLUMNS:
                clauses.append(f'CAST("{col}" AS DOUBLE) <= ?')
                bind_values.append(float(value))
            continue

        if param_name not in VALID_COLUMNS:
            continue

        if '|' in value:
            parts = [v.strip() for v in value.split('|') if v.strip()]
            placeholders = ', '.join(['?'] * len(parts))
            clauses.append(f'LOWER(CAST("{param_name}" AS VARCHAR)) IN ({placeholders})')
            bind_values.extend([p.lower() for p in parts])
        else:
            clauses.append(f'LOWER(CAST("{param_name}" AS VARCHAR)) LIKE ?')
            bind_values.append(f'%{value.lower()}%')

    return clauses, bind_values


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

            if not field or field not in ALLOWED_FIELDS:
                self.send_response(400)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({
                    'error': f'field must be one of: {", ".join(sorted(ALLOWED_FIELDS))}'
                }).encode('utf-8'))
                return

            clauses, bind_values = _parse_filters(params)
            where_sql = f'WHERE {" AND ".join(clauses)}' if clauses else ''

            conn = duckdb.connect(':memory:', read_only=False)
            parquet_path = get_parquet_path()

            query = (
                f"SELECT DISTINCT INITCAP(TRIM(CAST(\"{field}\" AS VARCHAR))) AS val "
                f"FROM read_parquet('{parquet_path}') "
                f"{where_sql} "
                f"{'AND' if where_sql else 'WHERE'} \"{field}\" IS NOT NULL "
                f"AND TRIM(CAST(\"{field}\" AS VARCHAR)) != '' "
                f"ORDER BY val"
            )

            rows = conn.execute(query, bind_values).fetchall()
            conn.close()

            values = [r[0] for r in rows if r[0] is not None]

            response = {
                'values': values,
                'count': len(values),
            }

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode('utf-8'))

        except Exception as e:
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))
