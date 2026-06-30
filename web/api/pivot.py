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

Multi-value dimensions (series, location)
-----------------------------------------
A single listing can carry several occupational series or locations (stored
as a '; '-delimited list). Selecting one of those dimensions unnests the list
so the listing is counted once in EVERY value it actually has — e.g. a posting
open in DC and Atlanta is counted under both. Counts therefore will NOT sum to
the total number of listings; that is intentional and the UI flags it.

The Count is always the number of DISTINCT listings in a group (we count
distinct usajobsControlNumber), so a listing that happens to repeat a value
in its own list is still only counted once for that value.

To keep the result well-defined (and fast), at most ONE multi-value dimension
may be used per pivot; combining two would produce a meaningless positional
zip of the two lists.
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

# key -> (header label, single-value SQL expression, multi-value source column).
# Exactly one of (expr, multi_col) is set. For multi_col the value list is
# split on '; ', unnested, and counted with COUNT(DISTINCT control number).
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

CONTROL = '"usajobsControlNumber"'

MAX_DIMS = 6            # cap dimensions to keep the grid from exploding
MAX_PREVIEW_ROWS = 500  # rows returned to the on-page preview
MAX_CSV_ROWS = 200000   # safety cap on a CSV download


def _build_query(dims, where_sql):
    """Build the pivot SQL for the given ordered dimension keys.

    Returns (sql, headers). The SQL ends without LIMIT so the caller appends one.
    Assumes at most one multi-value dimension (validated by the caller).
    """
    parquet_path = get_parquet_path()
    headers = [DIMENSIONS[k][0] for k in dims]
    group_cols = [f'd{i}' for i in range(len(dims))]

    multi_idx = next((i for i, k in enumerate(dims) if DIMENSIONS[k][2]), None)

    if multi_idx is None:
        # All single-value: the data is one row per listing, so COUNT(*) is the
        # exact distinct-listing count and we can group directly.
        selects = [f'{DIMENSIONS[k][1]} AS d{i}' for i, k in enumerate(dims)]
        sql = (
            f"SELECT {', '.join(selects)}, COUNT(*) AS cnt "
            f"FROM read_parquet('{parquet_path}') "
            f"{where_sql} "
            f"GROUP BY {', '.join(group_cols)} "
            f"ORDER BY cnt DESC, {', '.join(group_cols)}"
        )
        return sql, headers

    # One multi-value dimension: unnest its list in a flat subquery, keep the
    # control number, then COUNT(DISTINCT control) so each listing is counted
    # once per value it actually carries.
    multi_col = DIMENSIONS[dims[multi_idx]][2]
    inner_selects = [f'{CONTROL} AS _cn']
    for i, k in enumerate(dims):
        if i == multi_idx:
            inner_selects.append(
                f"TRIM(unnest(string_split(CAST(\"{multi_col}\" AS VARCHAR), '; '))) AS d{i}"
            )
        else:
            inner_selects.append(f'{DIMENSIONS[k][1]} AS d{i}')

    inner = (
        f"SELECT {', '.join(inner_selects)} "
        f"FROM read_parquet('{parquet_path}') "
        f"{where_sql}"
    )
    multi_alias = f'd{multi_idx}'
    sql = (
        f"SELECT {', '.join(group_cols)}, COUNT(DISTINCT _cn) AS cnt "
        f"FROM ({inner}) sub "
        f"WHERE {multi_alias} IS NOT NULL AND {multi_alias} <> '' "
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

            multi = [d for d in dims if DIMENSIONS[d][2]]
            if len(multi) > 1:
                self._send_json(400, {
                    'error': 'pick at most one multi-value field (occupational series or '
                             'location) at a time',
                    'multi_value_fields': multi,
                })
                return

            want_csv = params.get('format', [''])[0] == 'csv'
            limit = MAX_CSV_ROWS if want_csv else MAX_PREVIEW_ROWS + 1

            filter_clauses, bind_values = parse_filters(params)
            where_sql = f'WHERE {" AND ".join(filter_clauses)}' if filter_clauses else ''

            sql, headers = _build_query(dims, where_sql)
            sql += f' LIMIT {limit}'

            conn = get_conn()

            if want_csv:
                # Execute before sending headers so a query error still becomes
                # a 500, then stream in batches to bound memory.
                cursor = conn.execute(sql, bind_values)

                self.send_response(200)
                self.send_header('Content-Type', 'text/csv; charset=utf-8')
                self.send_header('Content-Disposition', 'attachment; filename="usajobs_pivot.csv"')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()

                buf = io.StringIO()
                writer = csv.writer(buf)

                def flush():
                    self.wfile.write(buf.getvalue().encode('utf-8'))
                    buf.seek(0)
                    buf.truncate(0)

                writer.writerow(headers + ['Count'])
                flush()
                while True:
                    batch = cursor.fetchmany(10000)
                    if not batch:
                        break
                    writer.writerows(batch)
                    flush()
                conn.close()
                return

            rows = conn.execute(sql, bind_values).fetchall()
            conn.close()

            truncated = len(rows) > MAX_PREVIEW_ROWS
            if truncated:
                rows = rows[:MAX_PREVIEW_ROWS]

            self._send_json(200, {
                'headers': headers + ['Count'],
                'rows': [list(r) for r in rows],
                'truncated': truncated,
                'preview_limit': MAX_PREVIEW_ROWS,
                'multi_value_dims': multi,
            })

        except Exception:
            import traceback
            traceback.print_exc()
            self._send_json(500, {'error': 'internal server error'})
