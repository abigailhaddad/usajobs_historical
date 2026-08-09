"""Differential test: shared/filters.js must behave identically to api/columns.py.

The DuckDB-WASM rewrite moves filter parsing from Python into the browser. The
filter rules carry real scar tissue — grade zero-padding, exact-vs-substring
matching for agency names containing commas, semicolon-delimited series — and
every one of those fails SILENTLY when ported wrong: no error, just the wrong
rows. So this compares the two implementations three ways:

  1. generated SQL clauses, textually
  2. bind values
  3. the row count each actually produces against the real parquet

(3) is the one that matters. (1) and (2) just localize the failure when it trips.

Run:  pytest tests/test_filters_parity.py -v
Needs: node on PATH, and a parquet (env PARQUET, else data/jobs_5yr.parquet).
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

HERE = Path(__file__).resolve().parent
WEB = HERE.parent
sys.path.insert(0, str(WEB / 'api'))

from columns import parse_filters  # noqa: E402

PARQUET = os.environ.get('PARQUET', str(WEB / 'data' / 'jobs_5yr.parquet'))

# Real values pulled from the live parquet. The comma in the agency name and
# the coexistence of GS-07 and GS-7 are the actual bugs these rules exist for.
AGENCY_WITH_COMMA = 'Commander, Navy Installations Command'
DEPT_A = 'Department of Justice'
DEPT_B = 'Department of the Treasury'

CORPUS = [
    # --- the reason EXACT_MATCH_FIELDS exists: comma inside a dropdown value
    {'filter_hiringAgencyName': [AGENCY_WITH_COMMA]},
    # --- the reason grade canonicalization exists: both forms live in the data
    {'filter_grade': ['GS-7']},
    {'filter_grade': ['GS-07']},
    {'filter_grade': ['GS-07/09']},
    {'filter_grade': ['XX-00']},          # second digit 0 -> must NOT be stripped
    # --- multiselect
    {'filter_hiringDepartmentName': [f'{DEPT_A}|{DEPT_B}']},
    {'filter_grade': ['GS-07|GS-9']},
    # --- semicolon-delimited multi-value field -> LIKE, not IN
    {'filter_occupationalSeries': ['2210']},
    {'filter_occupationalSeries': ['2210|0343']},
    # --- free-text: comma means OR across terms on non-dropdown fields
    {'filter_positionTitle': ['analyst']},
    {'filter_positionTitle': ['analyst, engineer']},
    {'filter_locations': ['washington']},
    # --- numeric ranges
    {'filter_minimumSalary_min': ['50000']},
    {'filter_maximumSalary_max': ['100000']},
    {'filter_minimumSalary_min': ['50000'], 'filter_maximumSalary_max': ['150000']},
    # --- date ranges
    {'filter_openDate_min': ['2024-01-01']},
    {'filter_closeDate_max': ['2024-12-31']},
    {'filter_openDate_min': ['2024-01-01'], 'filter_openDate_max': ['2024-06-30']},
    # --- was listed as filterable but absent from the parquet -> live 500.
    #     Must now be ignored like any unknown column.
    {'filter_workSchedule': ['full']},
    # --- ignored / no-op inputs
    {'filter_bogus': ['x']},
    {'filter_positionTitle': ['']},
    {'not_a_filter': ['x']},
    {},
    # --- combinations
    {'filter_hiringAgencyName': [AGENCY_WITH_COMMA], 'filter_grade': ['GS-7']},
    {'filter_hiringDepartmentName': [DEPT_A], 'filter_positionTitle': ['analyst'],
     'filter_minimumSalary_min': ['60000']},
    # --- whitespace / casing
    {'filter_hiringDepartmentName': [f'  {DEPT_A}  |{DEPT_B}']},
    {'filter_positionTitle': ['ANALYST']},
]


def _js_results():
    proc = subprocess.run(
        ['node', str(HERE / '_filters_runner.mjs')],
        input=json.dumps(CORPUS), capture_output=True, text=True,
    )
    if proc.returncode != 0:
        raise AssertionError(f'node runner failed:\n{proc.stderr}')
    return json.loads(proc.stdout)


def _py_results():
    out = []
    for params in CORPUS:
        try:
            clauses, binds = parse_filters(params)
            out.append({'clauses': clauses, 'binds': binds, 'error': None})
        except Exception as e:  # mirror the JS runner's shape
            out.append({'clauses': None, 'binds': None, 'error': str(e)})
    return out


def _norm_binds(binds):
    # 5.0 (Python float) and 5 (JS number) are the same bind.
    return [round(b, 6) if isinstance(b, (int, float)) and not isinstance(b, bool)
            else b for b in (binds or [])]


@pytest.fixture(scope='module')
def both():
    if not Path(PARQUET).exists():
        pytest.skip(f'parquet not found: {PARQUET} (set PARQUET=...)')
    return _py_results(), _js_results()


@pytest.mark.parametrize('i', range(len(CORPUS)))
def test_clauses_match(both, i):
    py, js = both
    assert js[i]['clauses'] == py[i]['clauses'], (
        f'SQL differs for {CORPUS[i]}\n  py: {py[i]["clauses"]}\n  js: {js[i]["clauses"]}')


@pytest.mark.parametrize('i', range(len(CORPUS)))
def test_binds_match(both, i):
    py, js = both
    assert _norm_binds(js[i]['binds']) == _norm_binds(py[i]['binds']), (
        f'binds differ for {CORPUS[i]}\n  py: {py[i]["binds"]}\n  js: {js[i]["binds"]}')


@pytest.mark.parametrize('i', range(len(CORPUS)))
def test_row_counts_match(both, i):
    """The one that actually matters: same rows out of the same parquet."""
    py, js = both
    if py[i]['error'] or js[i]['error']:
        assert bool(py[i]['error']) == bool(js[i]['error'])
        return
    con = duckdb.connect()
    counts = []
    for side in (py[i], js[i]):
        where = f'WHERE {" AND ".join(side["clauses"])}' if side['clauses'] else ''
        sql = f"SELECT count(*) FROM read_parquet('{PARQUET}') {where}"
        counts.append(con.execute(sql, side['binds']).fetchone()[0])
    assert counts[0] == counts[1], f'row counts differ for {CORPUS[i]}: {counts}'


def test_every_filterable_column_exists_in_the_parquet():
    """Regression guard for the workSchedule 500.

    An unknown filter_ param is ignored; a *listed* one builds SQL. So any name
    in FILTERABLE_COLUMNS that is missing from the file turns a user's filter
    into a 500. Catch it here instead of in production.
    """
    from columns import FILTERABLE_COLUMNS
    con = duckdb.connect()
    actual = {r[0] for r in
              con.execute(f"DESCRIBE SELECT * FROM read_parquet('{PARQUET}')").fetchall()}
    missing = sorted(FILTERABLE_COLUMNS - actual)
    assert not missing, f'FILTERABLE_COLUMNS names columns absent from the parquet: {missing}'


def test_filterable_columns_agree_across_implementations():
    """The JS port must not drift from columns.py on which columns are filterable."""
    from columns import FILTERABLE_COLUMNS
    src = (WEB / 'shared' / 'filters.js').read_text()
    proc = subprocess.run(
        ['node', '--input-type=module', '-e',
         'const m = await import(process.argv[1]);'
         'process.stdout.write(JSON.stringify([...m.FILTERABLE_COLUMNS].sort()))',
         'data:text/javascript;base64,' +
         __import__('base64').b64encode(src.encode()).decode()],
        capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout) == sorted(FILTERABLE_COLUMNS)


def test_static_json_row_order_matches_the_table():
    """The precomputed first page must be in the order the table renders.

    scripts/prep_web_data.py writes static.json's jobs.data with
    DISPLAY_COLUMNS; the table renders shared/filters.js COLUMNS. If those two
    lists ever drift, every value lands in the wrong column with no error — the
    rows still look like plausible data. They were written in the parquet's
    storage order (control number first) until 2026-08-09.
    """
    import re
    prep = (WEB.parent / 'scripts' / 'prep_web_data.py').read_text()
    m = re.search(r'DISPLAY_COLUMNS = \[(.*?)\]', prep, re.S)
    assert m, 'DISPLAY_COLUMNS not found in scripts/prep_web_data.py'
    py_cols = re.findall(r'"([^"]+)"', m.group(1))

    js = (WEB / 'shared' / 'filters.js').read_text()
    m2 = re.search(r'export const COLUMNS = \[(.*?)\];', js, re.S)
    assert m2, 'COLUMNS not found in shared/filters.js'
    js_cols = re.findall(r"'([^']+)'", m2.group(1))

    assert py_cols == js_cols, (
        'static.json row order has drifted from the table column order:\n'
        f'  prep_web_data.py: {py_cols}\n  filters.js:       {js_cols}')


def test_grade_canonicalization_is_load_bearing():
    """GS-7 must also return the GS-07 rows. If this passes trivially (one form
    absent from the data) the corpus above is no longer testing anything."""
    con = duckdb.connect()
    padded, unpadded = [
        con.execute(f"SELECT count(*) FROM read_parquet('{PARQUET}') WHERE grade = ?",
                    [g]).fetchone()[0] for g in ('GS-07', 'GS-7')
    ]
    assert padded > 0 and unpadded > 0, (
        f'both grade spellings must exist for this test to mean anything '
        f'(GS-07={padded}, GS-7={unpadded})')

    clauses, binds = parse_filters({'filter_grade': ['GS-7']})
    got = con.execute(
        f"SELECT count(*) FROM read_parquet('{PARQUET}') WHERE {clauses[0]}", binds
    ).fetchone()[0]
    assert got == padded + unpadded, (
        f'filtering GS-7 returned {got}, expected {padded + unpadded} '
        f'(both spellings)')
