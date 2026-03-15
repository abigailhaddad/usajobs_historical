import json
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

SORTABLE_COLUMNS = {i: col for i, col in enumerate(COLUMNS)}

# Text-searchable columns for global search
TEXT_COLUMNS = [
    'positionTitle',
    'hiringDepartmentName',
    'hiringAgencyName',
    'grade',
    'locations',
    'appointmentType',
    'serviceType',
    'status',
]


def _get_conn():
    conn = duckdb.connect(':memory:', read_only=False)
    return conn


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

            draw = int(params.get('draw', ['1'])[0])
            start = max(0, int(params.get('start', ['0'])[0]))
            length = min(100, max(1, int(params.get('length', ['25'])[0])))

            # Sort — support up to 3 sort columns
            order_parts = []
            for i in range(3):
                col_key = f'order[{i}][column]'
                dir_key = f'order[{i}][dir]'
                if col_key not in params:
                    break
                col_idx = int(params[col_key][0])
                dir_raw = params.get(dir_key, ['desc'])[0].lower()
                direction = 'ASC' if dir_raw != 'desc' else 'DESC'
                col_name = SORTABLE_COLUMNS.get(col_idx)
                if col_name:
                    order_parts.append((col_name, direction))

            if not order_parts:
                order_parts = [('openDate', 'DESC')]

            # Global search
            search_value = params.get('search[value]', [''])[0].strip()

            # Build WHERE
            where_clauses = []
            bind_values = []

            if search_value:
                search_parts = []
                for col in TEXT_COLUMNS:
                    search_parts.append(f'LOWER(COALESCE(CAST("{col}" AS VARCHAR), \'\')) LIKE ?')
                    bind_values.append(f'%{search_value.lower()}%')
                where_clauses.append(f'({" OR ".join(search_parts)})')

            # Custom filters
            filter_clauses, filter_values = _parse_filters(params)
            where_clauses.extend(filter_clauses)
            bind_values.extend(filter_values)

            where_sql = f'WHERE {" AND ".join(where_clauses)}' if where_clauses else ''

            col_list = ', '.join([f'COALESCE(CAST("{c}" AS VARCHAR), \'\')' for c in COLUMNS])

            conn = _get_conn()
            parquet_path = get_parquet_path()

            # Total records (unfiltered)
            total_result = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')"
            ).fetchone()
            records_total = total_result[0] if total_result else 0

            # Filtered count
            if where_clauses:
                filtered_result = conn.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{parquet_path}') {where_sql}",
                    bind_values,
                ).fetchone()
                records_filtered = filtered_result[0] if filtered_result else 0
            else:
                records_filtered = records_total

            # Data query
            order_clause = ", ".join(
                f"COALESCE(CAST(\"{col}\" AS VARCHAR), '') {d}"
                for col, d in order_parts
            )
            query = (
                f'SELECT {col_list} FROM read_parquet(\'{parquet_path}\') '
                f'{where_sql} '
                f'ORDER BY {order_clause} '
                f'LIMIT ? OFFSET ?'
            )
            data_binds = bind_values + [length, start]
            rows = conn.execute(query, data_binds).fetchall()

            conn.close()

            data = [list(row) for row in rows]

            response = {
                'draw': draw,
                'recordsTotal': records_total,
                'recordsFiltered': records_filtered,
                'data': data,
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
