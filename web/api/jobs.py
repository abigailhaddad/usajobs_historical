import json
import traceback
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

import duckdb

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from data_loader import get_parquet_path, get_conn
from columns import COLUMNS, SORTABLE_COLUMNS, TEXT_SEARCH_COLUMNS, parse_filters


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

            try:
                draw = int(params.get('draw', ['1'])[0])
                start = max(0, int(params.get('start', ['0'])[0]))
                length = min(100, max(1, int(params.get('length', ['25'])[0])))
            except (ValueError, TypeError):
                self.send_response(400)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps(
                    {'error': 'draw, start, and length must be integers'}
                ).encode('utf-8'))
                return

            # Sort — support up to 3 sort columns
            order_parts = []
            for i in range(3):
                col_key = f'order[{i}][column]'
                dir_key = f'order[{i}][dir]'
                if col_key not in params:
                    break
                try:
                    col_idx = int(params[col_key][0])
                except (ValueError, TypeError):
                    break
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
                for col in TEXT_SEARCH_COLUMNS:
                    search_parts.append(f'LOWER(COALESCE(CAST("{col}" AS VARCHAR), \'\')) LIKE ?')
                    bind_values.append(f'%{search_value.lower()}%')
                where_clauses.append(f'({" OR ".join(search_parts)})')

            # Custom filters
            filter_clauses, filter_values = parse_filters(params)
            where_clauses.extend(filter_clauses)
            bind_values.extend(filter_values)

            where_sql = f'WHERE {" AND ".join(where_clauses)}' if where_clauses else ''

            col_list = ', '.join([f'COALESCE(CAST("{c}" AS VARCHAR), \'\')' for c in COLUMNS])

            conn = get_conn()
            parquet_path = get_parquet_path()

            # Total records (unfiltered)
            total_result = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')"
            ).fetchone()
            records_total = total_result[0] if total_result else 0

            # Data query with COUNT(*) OVER() to get filtered count for free
            order_clause = ", ".join(
                f"COALESCE(CAST(\"{col}\" AS VARCHAR), '') {d}"
                for col, d in order_parts
            )
            query = (
                f'SELECT {col_list}, COUNT(*) OVER() AS _total_filtered '
                f'FROM read_parquet(\'{parquet_path}\') '
                f'{where_sql} '
                f'ORDER BY {order_clause} '
                f'LIMIT ? OFFSET ?'
            )
            data_binds = bind_values + [length, start]
            rows = conn.execute(query, data_binds).fetchall()

            conn.close()

            if rows:
                records_filtered = rows[0][-1]
                data = [list(row[:-1]) for row in rows]
            else:
                records_filtered = 0 if where_clauses else records_total
                data = []

            response = {
                'draw': draw,
                'recordsTotal': records_total,
                'recordsFiltered': records_filtered,
                'data': data,
            }

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Cache-Control', 'public, max-age=60')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode('utf-8'))

        except Exception:
            # Log the real error to the server logs; don't leak internals
            # (SQL text, file paths) to the client.
            traceback.print_exc()
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'error': 'internal server error'}).encode('utf-8'))
