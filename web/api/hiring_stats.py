"""
Hiring status aggregations for the Hiring Status page.

Returns, for postings opened in a given window (default: current calendar year):
  - overall status mix
  - status mix + median posting duration broken out by agency, series, grade
  - agencies/series with the highest share of <=5 day application windows

Accepts standard filter_* query params (parsed via columns.parse_filters)
plus start/end as the open-date window.

This is a SNAPSHOT view: USAJobs status reflects current state, not history.
Many agencies never report 'Candidate selected'; treat that as a reporting
artifact, not as evidence of slow hiring.
"""

import json
import os
import sys
from datetime import date
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.dirname(__file__))
from data_loader import get_parquet_path, get_conn
from columns import parse_filters


STATUS_ORDER = [
    'Job canceled',
    'Accepting applications',
    'Applications under review',
    'Job closed',
    'Candidate selected',
    '(no status)',
]

TERMINAL_STATUSES = ('Job closed', 'Job canceled', 'Candidate selected')

TOP_N = 20
MIN_N = 50
SHORT_WINDOW_DAYS = 5


def _window(params):
    """Resolve the open-date window.

    Priority: explicit start/end → filter_openDate_min/max from the shared
    filter bar → default (current calendar year through today).
    """
    today = date.today()
    default_start = date(today.year, 1, 1).isoformat()
    default_end = today.isoformat()
    start = (
        params.get('start', [None])[0]
        or params.get('filter_openDate_min', [None])[0]
        or default_start
    )
    end = (
        params.get('end', [None])[0]
        or params.get('filter_openDate_max', [None])[0]
        or default_end
    )
    return start, end


def _extra_where(params):
    """Build optional extra WHERE clauses from filter_* query params.

    The openDate filter is consumed by _window() instead, so we strip it
    from parse_filters' input to avoid double-filtering with the same column.
    """
    cleaned = {k: v for k, v in params.items()
               if k not in ('filter_openDate_min', 'filter_openDate_max')}
    clauses, binds = parse_filters(cleaned)
    if not clauses:
        return '', []
    return ' AND ' + ' AND '.join(clauses), binds


def _baseline_total(conn, parquet_path, start, end):
    """Total postings in the open-date window with NO filters applied."""
    return conn.execute(
        f"""
        SELECT COUNT(*) FROM read_parquet('{parquet_path}')
        WHERE CAST("openDate" AS DATE) >= CAST(? AS DATE)
          AND CAST("openDate" AS DATE) <  CAST(? AS DATE)
        """,
        [start, end],
    ).fetchone()[0]


def _overall(conn, parquet_path, start, end, extra_where, extra_binds):
    rows = conn.execute(
        f"""
        SELECT COALESCE(status, '(no status)') AS s, COUNT(*)
        FROM read_parquet('{parquet_path}')
        WHERE CAST("openDate" AS DATE) >= CAST(? AS DATE)
          AND CAST("openDate" AS DATE) <  CAST(? AS DATE)
          {extra_where}
        GROUP BY s
        """,
        [start, end] + extra_binds,
    ).fetchall()
    total = sum(r[1] for r in rows)
    median_days = conn.execute(
        f"""
        SELECT MEDIAN(DATE_DIFF('day', CAST("openDate" AS DATE), CAST("closeDate" AS DATE)))
        FROM read_parquet('{parquet_path}')
        WHERE CAST("openDate" AS DATE) >= CAST(? AS DATE)
          AND CAST("openDate" AS DATE) <  CAST(? AS DATE)
          AND status IN {TERMINAL_STATUSES}
          AND closeDate IS NOT NULL
          {extra_where}
        """,
        [start, end] + extra_binds,
    ).fetchone()[0]
    return {
        'total': total,
        'counts': {r[0]: r[1] for r in rows},
        'median_terminal_days': round(float(median_days)) if median_days is not None else None,
    }


def _breakout(conn, parquet_path, start, end, group_expr, extra_where, extra_binds):
    # When the user has narrowed with a filter, drop the min-N floor so a
    # narrow filter (e.g. single series) doesn't blank the breakouts.
    min_n = 1 if extra_where else MIN_N
    top = conn.execute(
        f"""
        SELECT {group_expr} AS label, COUNT(*) AS n
        FROM read_parquet('{parquet_path}')
        WHERE CAST("openDate" AS DATE) >= CAST(? AS DATE)
          AND CAST("openDate" AS DATE) <  CAST(? AS DATE)
          AND {group_expr} IS NOT NULL
          {extra_where}
        GROUP BY label
        HAVING n >= {min_n}
        ORDER BY n DESC
        LIMIT {TOP_N}
        """,
        [start, end] + extra_binds,
    ).fetchall()
    if not top:
        return []
    labels = [r[0] for r in top]
    totals = {r[0]: r[1] for r in top}

    placeholders = ', '.join(['?'] * len(labels))
    status_rows = conn.execute(
        f"""
        SELECT {group_expr} AS label, COALESCE(status, '(no status)') AS s, COUNT(*)
        FROM read_parquet('{parquet_path}')
        WHERE CAST("openDate" AS DATE) >= CAST(? AS DATE)
          AND CAST("openDate" AS DATE) <  CAST(? AS DATE)
          AND {group_expr} IN ({placeholders})
          {extra_where}
        GROUP BY label, s
        """,
        [start, end] + labels + extra_binds,
    ).fetchall()

    median_rows = conn.execute(
        f"""
        SELECT {group_expr} AS label,
               MEDIAN(DATE_DIFF('day', CAST("openDate" AS DATE), CAST("closeDate" AS DATE)))
        FROM read_parquet('{parquet_path}')
        WHERE CAST("openDate" AS DATE) >= CAST(? AS DATE)
          AND CAST("openDate" AS DATE) <  CAST(? AS DATE)
          AND {group_expr} IN ({placeholders})
          AND status IN {TERMINAL_STATUSES}
          AND closeDate IS NOT NULL
          {extra_where}
        GROUP BY label
        """,
        [start, end] + labels + extra_binds,
    ).fetchall()
    medians = {r[0]: round(float(r[1])) if r[1] is not None else None for r in median_rows}

    by_label = {lab: {} for lab in labels}
    for lab, status, n in status_rows:
        by_label[lab][status] = n

    return [
        {
            'label': lab,
            'total': totals[lab],
            'counts': by_label[lab],
            'median_terminal_days': medians.get(lab),
        }
        for lab in labels
    ]


def _short_windows(conn, parquet_path, start, end, group_expr, extra_where, extra_binds):
    min_n = 1 if extra_where else MIN_N
    rows = conn.execute(
        f"""
        SELECT {group_expr} AS label,
               COUNT(*) AS n,
               SUM(CASE
                   WHEN DATE_DIFF('day', CAST("openDate" AS DATE), CAST("closeDate" AS DATE)) <= {SHORT_WINDOW_DAYS}
                   THEN 1 ELSE 0 END) AS short_n,
               MEDIAN(DATE_DIFF('day', CAST("openDate" AS DATE), CAST("closeDate" AS DATE))) AS median_days
        FROM read_parquet('{parquet_path}')
        WHERE CAST("openDate" AS DATE) >= CAST(? AS DATE)
          AND CAST("openDate" AS DATE) <  CAST(? AS DATE)
          AND {group_expr} IS NOT NULL
          AND closeDate IS NOT NULL
          {extra_where}
        GROUP BY label
        HAVING n >= {min_n}
        ORDER BY (short_n * 1.0 / n) DESC
        LIMIT {TOP_N}
        """,
        [start, end] + extra_binds,
    ).fetchall()
    return [
        {
            'label': r[0],
            'total': r[1],
            'short_count': r[2],
            'short_pct': round(100.0 * r[2] / r[1]) if r[1] else 0,
            'median_days': round(float(r[3])) if r[3] is not None else None,
        }
        for r in rows
    ]


class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        try:
            params = parse_qs(urlparse(self.path).query)
            start, end = _window(params)
            extra_where, extra_binds = _extra_where(params)

            conn = get_conn()
            parquet_path = get_parquet_path()

            response = {
                'window': {'start': start, 'end': end},
                'status_order': STATUS_ORDER,
                'baseline_total': _baseline_total(conn, parquet_path, start, end),
                'overall': _overall(conn, parquet_path, start, end, extra_where, extra_binds),
                'by_agency': _breakout(
                    conn, parquet_path, start, end,
                    '"hiringAgencyName"', extra_where, extra_binds,
                ),
                'by_series': _breakout(
                    conn, parquet_path, start, end,
                    '"occupationalSeries"', extra_where, extra_binds,
                ),
                'by_grade': _breakout(
                    conn, parquet_path, start, end,
                    '"grade"', extra_where, extra_binds,
                ),
                'short_window_days': SHORT_WINDOW_DAYS,
                'short_windows_agency': _short_windows(
                    conn, parquet_path, start, end,
                    '"hiringAgencyName"', extra_where, extra_binds,
                ),
                'short_windows_series': _short_windows(
                    conn, parquet_path, start, end,
                    '"occupationalSeries"', extra_where, extra_binds,
                ),
            }
            conn.close()

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Cache-Control', 'public, max-age=600')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode('utf-8'))
        except Exception as e:
            import traceback
            traceback.print_exc()
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({'error': str(e)}).encode('utf-8'))
