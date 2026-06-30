"""Pivot / counts endpoint.

Groups the (deduplicated) job listings by any number of selected dimensions
and returns the counts in long / tidy format — one row per combination of
dimension values, plus a Count column. Reuses the same `filter_*` query
params as every other endpoint, so callers can filter first and pivot the
filtered subset.

Examples:
  /api/pivot?dims=department,year
  /api/pivot?dims=agency,grade&filter_status=open
  /api/pivot?dims=series&format=csv          (streams a CSV download)

Dimension column order in the output follows the order given in `dims`.

Note on multi-value dimensions (series, location): a single listing can carry
several occupational series or locations (stored as a '; '-delimited list).
Selecting one of those dimensions unnests the list, so a listing is counted
once per value it carries — the same semantics as the occupational-series
chart on the main page. With no multi-value dimension selected, Count is a
clean count of distinct listings (the data is already deduplicated to one row
per usajobsControlNumber).
"""

import csv
import io
import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from data_loader import get_parquet_path, get_conn
from columns import parse_filters

# key -> (header label, single-value SQL expression, multi-value source column)
# Exactly one of (expr, multi_col) is set. For multi_col the value list is
# split on '; ' and unnested.
DIMENSIONS = {
    'department':      ('Department',       'COALESCE(CAST("hiringDepartmentName" AS VARCHAR), \'Unknown\')', None),
    'agency':          ('Agency',           'COALESCE(CAST("hiringAgencyName" AS VARCHAR), \'Unknown\')', None),
    'year':            ('Year',             'strftime(CAST("openDate" AS DATE), \'%Y\')', None),
    'month':           ('Month',            'strftime(CAST("openDate" AS DATE), \'%Y-%m\')', None),
    'grade':           ('Grade',            'COALESCE(CAST("grade" AS VARCHAR), \'Unknown\')', None),
    'appointmentType': ('Appointment Type', 'COALESCE(CAST("appointmentType" AS VARCHAR), \'Unknown\')', None),
    'serviceType':     ('Service',          'COALESCE(CAST("serviceType" AS VARCHAR), \'Unknown\')', None),
    'status':          ('Status',           'COALESCE(CAST("status" AS VARCHAR), \'Unknown\')', None),
    'series':          ('Occ. Series',      None, 'occupationalSeries'),
    'location':        ('Location',         None, 'locations'),
}

MAX_DIMS = 6            # cap dimensions to keep the grid from exploding
MAX_PREVIEW_ROWS = 500  # rows returned to the on-page preview
MAX_CSV_ROWS = 200000   # safety cap on a CSV download


def _build_query(dims, where_sql):
    """Build the pivot SQL for the given ordered dimension keys.

    Returns (sql, headers). The SQL ends without LIMIT so the caller appends one.
    """
    parquet_path = get_parquet_path()

    selects = []        # "<expr> AS d0"
    lateral_joins = []   # ", LATERAL (...) AS s0"
    group_cols = []      # "d0"
    nonempty = []        # "d0 <> ''"  (multi-value dims only)
    headers = []

    for i, key in enumerate(dims):
        label, expr, multi_col = DIMENSIONS[key]
        alias = f'd{i}'
        headers.append(label)
        group_cols.append(alias)
        if multi_col:
            join_alias = f's{i}'
            lateral_joins.append(
                f", LATERAL (SELECT TRIM(unnest(string_split("
                f"CAST(\"{multi_col}\" AS VARCHAR), '; '))) AS v) AS {join_alias}"
            )
            selects.append(f"{join_alias}.v AS {alias}")
            nonempty.append(f"{alias} <> ''")
        else:
            selects.append(f"{expr} AS {alias}")

    inner = (
        f"SELECT {', '.join(selects)} "
        f"FROM read_parquet('{parquet_path}') AS t"
        f"{''.join(lateral_joins)} "
        f"{where_sql}"
    )

    outer_where = f"WHERE {' AND '.join(nonempty)} " if nonempty else ''
    sql = (
        f"SELECT {', '.join(group_cols)}, COUNT(*) AS cnt "
        f"FROM ({inner}) sub "
        f"{outer_where}"
        f"GROUP BY {', '.join(group_cols)} "
        f"ORDER BY cnt DESC, {', '.join(group_cols)}"
    )
    return sql, headers


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
        self.send_header('Cache-Control', 'public, max-age=300')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)

            raw_dims = params.get('dims', [''])[0]
            dims = [d.strip() for d in raw_dims.split(',') if d.strip()]

            if not dims:
                self._send_json(400, {'error': 'dims is required, e.g. dims=department,year'})
                return
            bad = [d for d in dims if d not in DIMENSIONS]
            if bad:
                self._send_json(400, {
                    'error': f'unknown dimension(s): {", ".join(bad)}',
                    'allowed': list(DIMENSIONS.keys()),
                })
                return
            if len(dims) > MAX_DIMS:
                self._send_json(400, {'error': f'at most {MAX_DIMS} dimensions allowed'})
                return

            want_csv = params.get('format', [''])[0] == 'csv'
            limit = MAX_CSV_ROWS if want_csv else MAX_PREVIEW_ROWS + 1

            filter_clauses, bind_values = parse_filters(params)
            where_sql = f'WHERE {" AND ".join(filter_clauses)}' if filter_clauses else ''

            sql, headers = _build_query(dims, where_sql)
            sql += f' LIMIT {limit}'

            conn = get_conn()
            rows = conn.execute(sql, bind_values).fetchall()
            conn.close()

            if want_csv:
                buf = io.StringIO()
                writer = csv.writer(buf)
                writer.writerow(headers + ['Count'])
                writer.writerows(rows)
                csv_bytes = buf.getvalue().encode('utf-8')

                self.send_response(200)
                self.send_header('Content-Type', 'text/csv; charset=utf-8')
                self.send_header('Content-Disposition', 'attachment; filename="usajobs_pivot.csv"')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Content-Length', str(len(csv_bytes)))
                self.end_headers()
                self.wfile.write(csv_bytes)
                return

            truncated = len(rows) > MAX_PREVIEW_ROWS
            if truncated:
                rows = rows[:MAX_PREVIEW_ROWS]

            self._send_json(200, {
                'headers': headers + ['Count'],
                'rows': [list(r) for r in rows],
                'truncated': truncated,
                'preview_limit': MAX_PREVIEW_ROWS,
            })

        except Exception as e:
            self._send_json(500, {'error': str(e)})
