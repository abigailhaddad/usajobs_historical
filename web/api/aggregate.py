import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

import duckdb

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from data_loader import get_parquet_path, get_conn
from columns import parse_filters


def _build_where(params):
    clauses, bind_values = parse_filters(params)
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
            if group_by not in ('month', 'agency', 'department', 'grade', 'series'):
                self.send_response(400)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({
                    'error': 'group_by must be one of: month, agency, department, grade'
                }).encode('utf-8'))
                return

            where_sql, bind_values = _build_where(params)
            conn = get_conn()
            parquet_path = get_parquet_path()

            if group_by == 'month':
                # Single CTE-based query: reads parquet once, computes all aggregations
                query = (
                    f"WITH base AS ("
                    f"  SELECT * FROM read_parquet('{parquet_path}') {where_sql}"
                    f"), "
                    f"monthly AS ("
                    f"  SELECT strftime(CAST(\"openDate\" AS DATE), '%Y-%m') AS month_label, "
                    f"  COUNT(*) AS cnt, "
                    f"  ROUND(AVG(CAST(\"maximumSalary\" AS DOUBLE)), 0) AS avg_sal "
                    f"  FROM base WHERE \"openDate\" IS NOT NULL "
                    f"  GROUP BY month_label"
                    f"), "
                    f"dates AS ("
                    f"  SELECT MIN(CAST(\"openDate\" AS DATE)) AS min_date, "
                    f"  MAX(CAST(\"openDate\" AS DATE)) AS max_date "
                    f"  FROM base WHERE \"openDate\" IS NOT NULL"
                    f"), "
                    f"series AS ("
                    f"  SELECT COUNT(DISTINCT TRIM(s.v)) AS distinct_series "
                    f"  FROM base, "
                    f"  LATERAL (SELECT unnest(string_split(CAST(\"occupationalSeries\" AS VARCHAR), '; ')) AS v) s "
                    f"  WHERE \"occupationalSeries\" IS NOT NULL"
                    f") "
                    f"SELECT m.month_label, m.cnt, m.avg_sal, "
                    f"d.min_date, d.max_date, s.distinct_series "
                    f"FROM monthly m CROSS JOIN dates d CROSS JOIN series s "
                    f"ORDER BY m.month_label"
                )
                rows = conn.execute(query, bind_values).fetchall()

                if rows:
                    min_date = str(rows[0][3]) if rows[0][3] else None
                    max_date = str(rows[0][4]) if rows[0][4] else None
                    series_count = rows[0][5]
                else:
                    min_date = None
                    max_date = None
                    series_count = 0

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
                        'min_date': min_date,
                        'max_date': max_date,
                    }
                else:
                    labels = []
                    datasets = {'count': [], 'avg_salary': [], 'distinct_series': 0, 'min_date': None, 'max_date': None}

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
                    f"GROUP BY dept ORDER BY cnt DESC LIMIT 20"
                )
                rows = conn.execute(query, bind_values).fetchall()
                total_depts = conn.execute(
                    f"SELECT COUNT(DISTINCT COALESCE(CAST(\"hiringDepartmentName\" AS VARCHAR), 'Unknown')) "
                    f"FROM read_parquet('{parquet_path}') {where_sql}", bind_values
                ).fetchone()[0]
                labels = [r[0] for r in rows]
                datasets = {'count': [r[1] for r in rows], 'total_distinct': total_depts}

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

            elif group_by == 'series':
                # Split semicolon-delimited series, count each individually
                occ_not_null = f"AND \"occupationalSeries\" IS NOT NULL" if where_sql else f"WHERE \"occupationalSeries\" IS NOT NULL"
                query = (
                    f"SELECT val, COUNT(*) AS cnt FROM ("
                    f"  SELECT TRIM(unnest(string_split(CAST(\"occupationalSeries\" AS VARCHAR), '; '))) AS val "
                    f"  FROM read_parquet('{parquet_path}') {where_sql} {occ_not_null}"
                    f") WHERE val != '' GROUP BY val ORDER BY cnt DESC LIMIT 10"
                )
                rows = conn.execute(query, bind_values).fetchall()
                labels = [r[0] for r in rows]
                datasets = {'count': [r[1] for r in rows]}

            conn.close()

            response = {'labels': labels, 'datasets': datasets}

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Cache-Control', 'public, max-age=300')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode('utf-8'))

        except Exception:
            import traceback
            traceback.print_exc()
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'error': 'internal server error'}).encode('utf-8'))
