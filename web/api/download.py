import csv
import io
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

import duckdb

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from data_loader import get_parquet_path

COLUMNS = [
    'positionTitle',
    'hiringDepartmentName',
    'hiringAgencyName',
    'grade',
    'minimumSalary',
    'maximumSalary',
    'openDate',
    'closeDate',
    'appointmentType',
    'serviceType',
    'locations',
    'status',
    'usajobsControlNumber',
]

COLUMN_HEADERS = [
    'Position Title',
    'Department',
    'Agency',
    'Grade',
    'Min Salary',
    'Max Salary',
    'Open Date',
    'Close Date',
    'Appointment Type',
    'Service',
    'Locations',
    'Status',
    'Control Number',
]

MAX_ROWS = 500000

SORTABLE_COLUMNS = {i: col for i, col in enumerate(COLUMNS)}


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

        # Range filters: filter_minimumSalary_min, filter_minimumSalary_max
        if param_name.endswith('_min'):
            col = param_name[:-4]
            if col in SORTABLE_COLUMNS.values():
                clauses.append(f'CAST("{col}" AS DOUBLE) >= ?')
                bind_values.append(float(value))
            continue

        if param_name.endswith('_max'):
            col = param_name[:-4]
            if col in SORTABLE_COLUMNS.values():
                clauses.append(f'CAST("{col}" AS DOUBLE) <= ?')
                bind_values.append(float(value))
            continue

        # Column must be valid
        if param_name not in SORTABLE_COLUMNS.values():
            continue

        # Pipe-separated multiselect
        if '|' in value:
            parts = [v.strip() for v in value.split('|') if v.strip()]
            placeholders = ', '.join(['?'] * len(parts))
            clauses.append(f'LOWER(COALESCE(CAST("{param_name}" AS VARCHAR), \'\')) IN ({placeholders})')
            bind_values.extend([p.lower() for p in parts])
        else:
            # Text search with LIKE
            clauses.append(f'LOWER(COALESCE(CAST("{param_name}" AS VARCHAR), \'\')) LIKE ?')
            bind_values.append(f'%{value.lower()}%')

    return clauses, bind_values


def _get_conn():
    conn = duckdb.connect(':memory:', read_only=False)
    return conn


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
            where_clauses = []
            bind_values = []

            filter_clauses, filter_values = _parse_filters(params)
            where_clauses.extend(filter_clauses)
            bind_values.extend(filter_values)

            where_sql = f'WHERE {" AND ".join(where_clauses)}' if where_clauses else ''

            col_list = ', '.join([f'COALESCE(CAST("{c}" AS VARCHAR), \'\')' for c in COLUMNS])

            conn = _get_conn()
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
