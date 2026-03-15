import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

import duckdb

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from data_loader import get_parquet_path

VALID_COLUMNS = {
    'positionTitle', 'hiringAgencyName', 'hiringDepartmentName',
    'grade', 'minimumSalary', 'maximumSalary',
    'openDate', 'closeDate',
    'workSchedule', 'appointmentType',
    'locations', 'usajobsControlNumber', 'status', 'occupationalSeries',
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


def _build_where(params):
    clauses, bind_values = _parse_filters(params)
    where_sql = f'WHERE {" AND ".join(clauses)}' if clauses else ''
    return where_sql, bind_values


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

            group_by = params.get('group_by', ['month'])[0]
            if group_by not in ('month', 'agency', 'department', 'grade'):
                self.send_response(400)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({
                    'error': 'group_by must be one of: month, agency, department, grade'
                }).encode('utf-8'))
                return

            where_sql, bind_values = _build_where(params)
            conn = duckdb.connect(':memory:', read_only=False)
            parquet_path = get_parquet_path()

            if group_by == 'month':
                query = (
                    f"SELECT strftime(CAST(\"openDate\" AS DATE), '%Y-%m') AS month_label, "
                    f"COUNT(*) AS cnt, "
                    f"ROUND(AVG(CAST(\"maximumSalary\" AS DOUBLE)), 0) AS avg_sal "
                    f"FROM read_parquet('{parquet_path}') "
                    f"{where_sql} "
                    f"AND \"openDate\" IS NOT NULL "
                    f"GROUP BY month_label ORDER BY month_label"
                ) if where_sql else (
                    f"SELECT strftime(CAST(\"openDate\" AS DATE), '%Y-%m') AS month_label, "
                    f"COUNT(*) AS cnt, "
                    f"ROUND(AVG(CAST(\"maximumSalary\" AS DOUBLE)), 0) AS avg_sal "
                    f"FROM read_parquet('{parquet_path}') "
                    f"WHERE \"openDate\" IS NOT NULL "
                    f"GROUP BY month_label ORDER BY month_label"
                )

                rows = conn.execute(query, bind_values).fetchall()

                # Distinct occupational series count
                series_where = where_sql if where_sql else ''
                series_query = (
                    f"SELECT COUNT(DISTINCT \"occupationalSeries\") "
                    f"FROM read_parquet('{parquet_path}') "
                    f"{series_where} "
                    f"{'AND' if series_where else 'WHERE'} \"occupationalSeries\" IS NOT NULL"
                )
                series_count = conn.execute(series_query, bind_values).fetchone()[0]

                # Fill missing months so the X-axis is continuous
                row_map = {r[0]: r for r in rows}
                if rows:
                    min_month = rows[0][0]
                    max_month = rows[-1][0]
                    all_months = []
                    y, m = int(min_month[:4]), int(min_month[5:7])
                    end_y, end_m = int(max_month[:4]), int(max_month[5:7])
                    while (y, m) <= (end_y, end_m):
                        all_months.append(f'{y:04d}-{m:02d}')
                        m += 1
                        if m > 12:
                            m = 1
                            y += 1
                    labels = all_months
                    datasets = {
                        'count': [row_map[mo][1] if mo in row_map else 0 for mo in all_months],
                        'avg_salary': [int(row_map[mo][2]) if mo in row_map and row_map[mo][2] is not None else 0 for mo in all_months],
                        'distinct_series': series_count,
                    }
                else:
                    labels = []
                    datasets = {'count': [], 'avg_salary': [], 'distinct_series': 0}

            elif group_by == 'agency':
                query = (
                    f"SELECT COALESCE(CAST(\"hiringAgencyName\" AS VARCHAR), 'Unknown') AS agency, "
                    f"COUNT(*) AS cnt "
                    f"FROM read_parquet('{parquet_path}') "
                    f"{where_sql} "
                    f"GROUP BY agency ORDER BY cnt DESC LIMIT 20"
                )
                rows = conn.execute(query, bind_values).fetchall()
                labels = [r[0] for r in rows]
                datasets = {'count': [r[1] for r in rows]}

            elif group_by == 'department':
                query = (
                    f"SELECT COALESCE(CAST(\"hiringDepartmentName\" AS VARCHAR), 'Unknown') AS dept, "
                    f"COUNT(*) AS cnt "
                    f"FROM read_parquet('{parquet_path}') "
                    f"{where_sql} "
                    f"GROUP BY dept ORDER BY cnt DESC LIMIT 15"
                )
                rows = conn.execute(query, bind_values).fetchall()
                labels = [r[0] for r in rows]
                datasets = {'count': [r[1] for r in rows]}

            elif group_by == 'grade':
                # grade column is like "GS-7", "GS-7/9", "GS-7/9/11", etc.
                # Extract min and max grade numbers, then expand the range so
                # e.g. GS-7/9 counts once at each of GS-7, GS-8, GS-9.
                query = (
                    f"WITH raw AS ("
                    f"  SELECT CAST(\"grade\" AS VARCHAR) AS g "
                    f"  FROM read_parquet('{parquet_path}') "
                    f"  {where_sql} "
                    f"  {'AND' if where_sql else 'WHERE'} "
                    f"  CAST(\"grade\" AS VARCHAR) LIKE 'GS-%'"
                    f"), "
                    f"min_max AS ("
                    f"  SELECT "
                    f"    CAST(regexp_extract(g, 'GS-(\\d+)', 1) AS INTEGER) AS lo, "
                    f"    CAST(regexp_extract_all(g, '(\\d+)')[length(regexp_extract_all(g, '(\\d+)'))] AS INTEGER) AS hi "
                    f"  FROM raw"
                    f"), "
                    f"expanded AS ("
                    f"  SELECT unnest(generate_series(lo, hi)) AS gs_grade "
                    f"  FROM min_max "
                    f"  WHERE lo IS NOT NULL AND hi IS NOT NULL"
                    f") "
                    f"SELECT gs_grade AS grade, COUNT(*) AS cnt "
                    f"FROM expanded WHERE gs_grade BETWEEN 1 AND 15 "
                    f"GROUP BY gs_grade ORDER BY gs_grade"
                )
                rows = conn.execute(query, bind_values).fetchall()
                labels = [r[0] for r in rows]
                datasets = {'count': [r[1] for r in rows]}

            conn.close()

            response = {'labels': labels, 'datasets': datasets}

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
