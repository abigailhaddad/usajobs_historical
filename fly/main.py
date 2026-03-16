"""
FastAPI app for USAJobs Historical Data API.
Runs on Fly.io with DuckDB querying a parquet file in memory.
"""
import json
import os
import time

import boto3
import duckdb
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
import csv
import io

app = FastAPI()


@app.on_event("startup")
async def startup():
    """Download data and initialize DuckDB on startup, not on first request."""
    print("Starting up — downloading data from R2...")
    get_conn()
    get_static()
    print("Startup complete!")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# --- Data loading ---

PARQUET_PATH = "/tmp/jobs_5yr.parquet"
STATIC_PATH = "/tmp/static.json"
_conn = None
_static_data = None
_last_download = 0
_MAX_AGE = 3600  # Re-download hourly


def _download_from_r2():
    """Download parquet and static.json from R2."""
    global _last_download
    s3 = boto3.client(
        's3',
        endpoint_url=os.environ['R2_ENDPOINT_URL'],
        aws_access_key_id=os.environ['R2_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'],
    )
    s3.download_file('usajobs-data', 'web/jobs_5yr.parquet', PARQUET_PATH)
    s3.download_file('usajobs-data', 'web/static.json', STATIC_PATH)
    _last_download = time.time()


_db_path = '/tmp/jobs.duckdb'


def get_conn():
    """Get a DuckDB connection to the persistent database."""
    global _last_download
    now = time.time()
    if not os.path.exists(_db_path) or (now - _last_download > _MAX_AGE):
        _download_from_r2()
        # Create a persistent DuckDB file with the data loaded
        if os.path.exists(_db_path):
            os.remove(_db_path)
        conn = duckdb.connect(_db_path)
        conn.execute(f"CREATE TABLE jobs AS SELECT * FROM read_parquet('{PARQUET_PATH}')")
        count = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        print(f"Loaded {count:,} rows into DuckDB file")
        conn.close()
    # Return a new connection each time (safe for concurrent requests)
    return duckdb.connect(_db_path, read_only=True)


def get_static():
    """Get cached static data."""
    global _static_data
    if _static_data is None:
        if os.path.exists(STATIC_PATH):
            with open(STATIC_PATH) as f:
                _static_data = json.load(f)
        else:
            _download_from_r2()
            with open(STATIC_PATH) as f:
                _static_data = json.load(f)
    return _static_data


# --- Column config (same as web/api/columns.py) ---

COLUMNS = [
    'positionTitle', 'occupationalSeries', 'hiringDepartmentName',
    'hiringAgencyName', 'grade', 'minimumSalary', 'maximumSalary',
    'openDate', 'closeDate', 'appointmentType', 'serviceType',
    'locations', 'status', 'usajobsControlNumber',
]

FILTERABLE_COLUMNS = set(COLUMNS) | {'workSchedule'}
DATE_COLUMNS = {'openDate', 'closeDate'}
MULTI_VALUE_FIELDS = {'occupationalSeries'}
DROPDOWN_FIELDS = {
    'positionTitle', 'hiringAgencyName', 'hiringDepartmentName',
    'grade', 'appointmentType', 'serviceType', 'status', 'occupationalSeries',
}


def parse_filters(params: dict):
    """Parse filter_ params into WHERE clauses."""
    clauses = []
    bind_values = []

    for key, value in params.items():
        if not key.startswith('filter_') or not value:
            continue

        param_name = key[len('filter_'):]

        if param_name.endswith('_min'):
            col = param_name[:-4]
            if col in FILTERABLE_COLUMNS:
                if col in DATE_COLUMNS:
                    clauses.append(f'CAST("{col}" AS DATE) >= CAST(? AS DATE)')
                    bind_values.append(value)
                else:
                    clauses.append(f'CAST("{col}" AS DOUBLE) >= ?')
                    bind_values.append(float(value))
            continue

        if param_name.endswith('_max'):
            col = param_name[:-4]
            if col in FILTERABLE_COLUMNS:
                if col in DATE_COLUMNS:
                    clauses.append(f'CAST("{col}" AS DATE) <= CAST(? AS DATE)')
                    bind_values.append(value)
                else:
                    clauses.append(f'CAST("{col}" AS DOUBLE) <= ?')
                    bind_values.append(float(value))
            continue

        if param_name not in FILTERABLE_COLUMNS:
            continue

        if '|' in value:
            parts = [v.strip() for v in value.split('|') if v.strip()]
            if param_name in MULTI_VALUE_FIELDS:
                like_parts = []
                for p in parts:
                    like_parts.append(f'LOWER(COALESCE(CAST("{param_name}" AS VARCHAR), \'\')) LIKE ?')
                    bind_values.append(f'%{p.lower()}%')
                clauses.append(f'({" OR ".join(like_parts)})')
            else:
                placeholders = ', '.join(['?'] * len(parts))
                clauses.append(f'LOWER(COALESCE(CAST("{param_name}" AS VARCHAR), \'\')) IN ({placeholders})')
                bind_values.extend([p.lower() for p in parts])
        else:
            terms = [t.strip() for t in value.split(',') if t.strip()] if ',' in value else [value]
            if len(terms) > 1:
                like_parts = []
                for t in terms:
                    like_parts.append(f'LOWER(COALESCE(CAST("{param_name}" AS VARCHAR), \'\')) LIKE ?')
                    bind_values.append(f'%{t.lower()}%')
                clauses.append(f'({" OR ".join(like_parts)})')
            else:
                clauses.append(f'LOWER(COALESCE(CAST("{param_name}" AS VARCHAR), \'\')) LIKE ?')
                bind_values.append(f'%{value.lower()}%')

    return clauses, bind_values


# --- Endpoints ---

@app.get("/api/static_data")
def static_data():
    return JSONResponse(get_static(), headers={"Cache-Control": "public, max-age=3600"})


@app.get("/api/jobs")
def jobs(request: Request):
    conn = get_conn()
    params = dict(request.query_params)

    draw = int(params.get('draw', '1'))
    start = max(0, int(params.get('start', '0')))
    length = min(100, max(1, int(params.get('length', '25'))))

    # Sort
    sortable = {i: col for i, col in enumerate(COLUMNS)}
    order_parts = []
    for i in range(3):
        col_key = f'order[{i}][column]'
        dir_key = f'order[{i}][dir]'
        if col_key not in params:
            break
        col_idx = int(params[col_key])
        direction = 'ASC' if params.get(dir_key, 'desc').lower() != 'desc' else 'DESC'
        col_name = sortable.get(col_idx)
        if col_name:
            order_parts.append((col_name, direction))
    if not order_parts:
        order_parts = [('openDate', 'DESC')]

    # Search
    where_clauses = []
    bind_values = []
    search_value = params.get('search[value]', '').strip()
    if search_value:
        text_cols = ['positionTitle', 'hiringDepartmentName', 'hiringAgencyName',
                     'grade', 'locations', 'appointmentType', 'serviceType', 'status']
        search_parts = []
        for col in text_cols:
            search_parts.append(f'LOWER(COALESCE(CAST("{col}" AS VARCHAR), \'\')) LIKE ?')
            bind_values.append(f'%{search_value.lower()}%')
        where_clauses.append(f'({" OR ".join(search_parts)})')

    filter_clauses, filter_values = parse_filters(params)
    where_clauses.extend(filter_clauses)
    bind_values.extend(filter_values)

    where_sql = f'WHERE {" AND ".join(where_clauses)}' if where_clauses else ''
    col_list = ', '.join([f'COALESCE(CAST("{c}" AS VARCHAR), \'\')' for c in COLUMNS])

    records_total = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]

    # Separate count query (fast on in-memory table, avoids slow window function)
    if where_clauses:
        records_filtered = conn.execute(f"SELECT COUNT(*) FROM jobs {where_sql}", bind_values).fetchone()[0]
    else:
        records_filtered = records_total

    order_clause = ", ".join(f'COALESCE(CAST("{col}" AS VARCHAR), \'\') {d}' for col, d in order_parts)
    query = f'SELECT {col_list} FROM jobs {where_sql} ORDER BY {order_clause} LIMIT ? OFFSET ?'
    rows = conn.execute(query, bind_values + [length, start]).fetchall()
    data = [list(row) for row in rows]

    return {"draw": draw, "recordsTotal": records_total, "recordsFiltered": records_filtered, "data": data}


@app.get("/api/aggregate")
def aggregate(request: Request):
    conn = get_conn()
    params = dict(request.query_params)
    group_by = params.get('group_by', 'month')

    if group_by not in ('month', 'agency', 'department', 'grade'):
        return JSONResponse({"error": "group_by must be one of: month, agency, department, grade"}, status_code=400)

    filter_clauses, bind_values = parse_filters(params)
    where_sql = f'WHERE {" AND ".join(filter_clauses)}' if filter_clauses else ''

    if group_by == 'month':
        not_null = f"AND \"openDate\" IS NOT NULL" if where_sql else f"WHERE \"openDate\" IS NOT NULL"
        rows = conn.execute(
            f"SELECT strftime(CAST(\"openDate\" AS DATE), '%Y-%m') AS month_label, "
            f"COUNT(*) AS cnt, ROUND(AVG(CAST(\"maximumSalary\" AS DOUBLE)), 0) AS avg_sal "
            f"FROM jobs {where_sql} {not_null} GROUP BY month_label ORDER BY month_label",
            bind_values
        ).fetchall()

        date_row = conn.execute(
            f"SELECT MIN(CAST(\"openDate\" AS DATE)), MAX(CAST(\"openDate\" AS DATE)) "
            f"FROM jobs {where_sql} {not_null}", bind_values
        ).fetchone()
        min_date = str(date_row[0]) if date_row and date_row[0] else None
        max_date = str(date_row[1]) if date_row and date_row[1] else None

        # Simple distinct count (no unnest — close enough for filtered views, static.json has exact count)
        occ_null = f"AND \"occupationalSeries\" IS NOT NULL" if where_sql else f"WHERE \"occupationalSeries\" IS NOT NULL"
        series_count = conn.execute(
            f"SELECT COUNT(DISTINCT \"occupationalSeries\") FROM jobs {where_sql} {occ_null}", bind_values
        ).fetchone()[0]

        row_map = {r[0]: r for r in rows}
        all_months = []
        if rows:
            y, m = int(rows[0][0][:4]), int(rows[0][0][5:7])
            end_y, end_m = int(rows[-1][0][:4]), int(rows[-1][0][5:7])
            while (y, m) <= (end_y, end_m):
                all_months.append(f'{y:04d}-{m:02d}')
                m += 1
                if m > 12: m = 1; y += 1

        labels = all_months
        datasets = {
            'count': [row_map[mo][1] if mo in row_map else 0 for mo in all_months],
            'avg_salary': [int(row_map[mo][2]) if mo in row_map and row_map[mo][2] is not None else 0 for mo in all_months],
            'distinct_series': series_count,
            'min_date': min_date, 'max_date': max_date,
        }

    elif group_by == 'agency':
        rows = conn.execute(
            f"SELECT COALESCE(CAST(\"hiringAgencyName\" AS VARCHAR), 'Unknown'), COUNT(*) "
            f"FROM jobs {where_sql} GROUP BY 1 ORDER BY 2 DESC LIMIT 20", bind_values
        ).fetchall()
        labels = [r[0] for r in rows]
        datasets = {'count': [r[1] for r in rows]}

    elif group_by == 'department':
        rows = conn.execute(
            f"SELECT COALESCE(CAST(\"hiringDepartmentName\" AS VARCHAR), 'Unknown'), COUNT(*) "
            f"FROM jobs {where_sql} GROUP BY 1 ORDER BY 2 DESC LIMIT 20", bind_values
        ).fetchall()
        total_depts = conn.execute(
            f"SELECT COUNT(DISTINCT COALESCE(CAST(\"hiringDepartmentName\" AS VARCHAR), 'Unknown')) "
            f"FROM jobs {where_sql}", bind_values
        ).fetchone()[0]
        labels = [r[0] for r in rows]
        datasets = {'count': [r[1] for r in rows], 'total_distinct': total_depts}

    elif group_by == 'grade':
        rows = conn.execute(
            f"WITH raw AS (SELECT CAST(\"grade\" AS VARCHAR) AS g FROM jobs {where_sql} "
            f"{'AND' if where_sql else 'WHERE'} CAST(\"grade\" AS VARCHAR) LIKE 'GS-%'), "
            f"min_max AS (SELECT CAST(regexp_extract(g, 'GS-(\\d+)', 1) AS INTEGER) AS lo, "
            f"CAST(regexp_extract_all(g, '(\\d+)')[length(regexp_extract_all(g, '(\\d+)'))] AS INTEGER) AS hi FROM raw), "
            f"expanded AS (SELECT unnest(generate_series(lo, hi)) AS gs_grade FROM min_max WHERE lo IS NOT NULL AND hi IS NOT NULL) "
            f"SELECT gs_grade, COUNT(*) FROM expanded WHERE gs_grade BETWEEN 1 AND 15 GROUP BY 1 ORDER BY 1",
            bind_values
        ).fetchall()
        labels = [r[0] for r in rows]
        datasets = {'count': [r[1] for r in rows]}

    return JSONResponse({"labels": labels, "datasets": datasets}, headers={"Cache-Control": "public, max-age=300"})


@app.get("/api/filter_options")
def filter_options(request: Request):
    conn = get_conn()
    params = dict(request.query_params)
    field = params.get('field', '').strip()

    if not field or field not in DROPDOWN_FIELDS:
        return JSONResponse({"error": f"field must be one of: {', '.join(sorted(DROPDOWN_FIELDS))}"}, status_code=400)

    filter_clauses, bind_values = parse_filters(params)
    where_sql = f'WHERE {" AND ".join(filter_clauses)}' if filter_clauses else ''

    if field in MULTI_VALUE_FIELDS:
        query = (f"SELECT DISTINCT TRIM(unnest(string_split(CAST(\"{field}\" AS VARCHAR), '; '))) AS val "
                 f"FROM jobs {where_sql} {'AND' if where_sql else 'WHERE'} \"{field}\" IS NOT NULL "
                 f"AND TRIM(CAST(\"{field}\" AS VARCHAR)) != '' ORDER BY val")
    else:
        query = (f"SELECT DISTINCT TRIM(CAST(\"{field}\" AS VARCHAR)) AS val "
                 f"FROM jobs {where_sql} {'AND' if where_sql else 'WHERE'} \"{field}\" IS NOT NULL "
                 f"AND TRIM(CAST(\"{field}\" AS VARCHAR)) != '' ORDER BY val")

    rows = conn.execute(query, bind_values).fetchall()
    values = [r[0] for r in rows if r[0] and r[0].strip()]

    return JSONResponse({"values": values, "count": len(values)}, headers={"Cache-Control": "public, max-age=3600"})


@app.get("/api/download")
def download(request: Request):
    conn = get_conn()
    params = dict(request.query_params)

    filter_clauses, bind_values = parse_filters(params)
    where_sql = f'WHERE {" AND ".join(filter_clauses)}' if filter_clauses else ''

    col_list = ', '.join([f'COALESCE(CAST("{c}" AS VARCHAR), \'\')' for c in COLUMNS])
    rows = conn.execute(
        f"SELECT {col_list} FROM jobs {where_sql} ORDER BY COALESCE(CAST(\"openDate\" AS VARCHAR), '') DESC LIMIT 500000",
        bind_values
    ).fetchall()

    headers = ['Position Title', 'Occ. Series', 'Department', 'Agency', 'Grade',
               'Min Salary', 'Max Salary', 'Open Date', 'Close Date',
               'Appointment Type', 'Service', 'Locations', 'Status', 'Control Number']

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(headers)
    writer.writerows(rows)

    return StreamingResponse(
        io.BytesIO(buf.getvalue().encode('utf-8')),
        media_type='text/csv',
        headers={'Content-Disposition': 'attachment; filename="usajobs_export.csv"'}
    )


@app.get("/health")
def health():
    conn = get_conn()
    count = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    return {"status": "ok", "rows": count}
