"""Semantics tests for shared/filters.js, the site's only filter implementation.

This began as a differential test against api/columns.py while the port to
DuckDB-WASM was in progress (28/28 endpoint cases matched, see
tests/wasm_api_probe.html). columns.py is now deleted, so there is nothing left
to diff against and filters.js is the single source of truth. What remains is
the part that was always doing the real work: running the SQL filters.js
generates against the actual parquet and asserting the ROWS are right.

These rules encode bugs that were found the hard way, and every one of them
fails silently when broken — no error, just quietly wrong rows:

  - grade zero-padding      GS-07 and GS-7 both exist in the data and must be
                            treated as the same grade
  - exact vs substring      an agency name containing a comma ("Commander, Navy
                            Installations Command") must not be split into an OR
                            substring search that matches other agencies
  - multi-value fields      occupationalSeries is '; '-delimited, so a filter
                            must match any single value in the list
  - unknown columns         must be IGNORED, not turned into SQL — a column
                            listed as filterable but absent from the parquet
                            returned HTTP 500 in production until 2026-08-09

Run:  pytest tests/test_filters.py -v
Needs: node on PATH. Uses env PARQUET, else the local data/jobs_5yr.parquet,
else the live file on R2 (read over httpfs), so it runs with no local data.
"""

import json
import os
import re
import subprocess
from pathlib import Path

import duckdb
import pytest

HERE = Path(__file__).resolve().parent
WEB = HERE.parent
REPO = WEB.parent

R2_PARQUET = 'https://pub-317c58882ec04f329b63842c1eb65b0c.r2.dev/web/jobs_5yr.parquet'
_LOCAL = WEB / 'data' / 'jobs_5yr.parquet'
PARQUET = os.environ.get('PARQUET') or (str(_LOCAL) if _LOCAL.exists() else R2_PARQUET)
IS_REMOTE = PARQUET.startswith('http')

AGENCY_WITH_COMMA = 'Commander, Navy Installations Command'
DEPT_A = 'Department of Justice'
DEPT_B = 'Department of the Treasury'

# name -> filter params handed to parseFilters()
CASES = {
    'agency_with_comma':   {'filter_hiringAgencyName': AGENCY_WITH_COMMA},
    'grade_unpadded':      {'filter_grade': 'GS-7'},
    'grade_padded':        {'filter_grade': 'GS-07'},
    'grade_range':         {'filter_grade': 'GS-07/09'},
    'grade_ungraded':      {'filter_grade': 'XX-00'},
    'dept_multiselect':    {'filter_hiringDepartmentName': f'{DEPT_A}|{DEPT_B}'},
    'grade_multiselect':   {'filter_grade': 'GS-07|GS-9'},
    'series_single':       {'filter_occupationalSeries': '2210'},
    'series_multi':        {'filter_occupationalSeries': '2210|0343'},
    'title_text':          {'filter_positionTitle': 'analyst'},
    'title_text_or':       {'filter_positionTitle': 'analyst, engineer'},
    'locations_text':      {'filter_locations': 'washington'},
    'salary_min':          {'filter_minimumSalary_min': '50000'},
    'salary_max':          {'filter_maximumSalary_max': '100000'},
    'salary_range':        {'filter_minimumSalary_min': '50000',
                            'filter_maximumSalary_max': '150000'},
    'date_min':            {'filter_openDate_min': '2024-01-01'},
    'date_max':            {'filter_closeDate_max': '2024-12-31'},
    'date_range':          {'filter_openDate_min': '2024-01-01',
                            'filter_openDate_max': '2024-06-30'},
    'absent_column':       {'filter_workSchedule': 'full'},
    'unknown_column':      {'filter_bogus': 'x'},
    'empty_value':         {'filter_positionTitle': ''},
    'not_a_filter':        {'not_a_filter': 'x'},
    'no_filters':          {},
    'combined':            {'filter_hiringAgencyName': AGENCY_WITH_COMMA,
                            'filter_grade': 'GS-7'},
    'combined_three':      {'filter_hiringDepartmentName': DEPT_A,
                            'filter_positionTitle': 'analyst',
                            'filter_minimumSalary_min': '60000'},
    'whitespace_values':   {'filter_hiringDepartmentName': f'  {DEPT_A}  |{DEPT_B}'},
    'uppercase_value':     {'filter_positionTitle': 'ANALYST'},
}


def _connect():
    con = duckdb.connect()
    if IS_REMOTE:
        con.execute('INSTALL httpfs; LOAD httpfs;')
    return con


@pytest.fixture(scope='module')
def parsed():
    """{case name: {clauses, binds}} straight out of shared/filters.js."""
    proc = subprocess.run(
        ['node', str(HERE / '_filters_runner.mjs')],
        input=json.dumps([CASES[k] for k in CASES]), capture_output=True, text=True)
    if proc.returncode != 0:
        raise AssertionError(f'node runner failed:\n{proc.stderr}')
    return dict(zip(CASES, json.loads(proc.stdout)))


@pytest.fixture(scope='module')
def con():
    return _connect()


def _count(con, side):
    where = f'WHERE {" AND ".join(side["clauses"])}' if side['clauses'] else ''
    return con.execute(
        f"SELECT count(*) FROM read_parquet('{PARQUET}') {where}", side['binds']
    ).fetchone()[0]


@pytest.mark.parametrize('name', list(CASES))
def test_every_case_produces_runnable_sql(parsed, con, name):
    """Nothing filters.js emits may raise. A malformed clause or a column that
    does not exist is exactly how the workSchedule 500 happened."""
    side = parsed[name]
    assert side['error'] is None, f'{name}: filters.js raised {side["error"]}'
    _count(con, side)


@pytest.mark.parametrize('name', ['absent_column', 'unknown_column', 'empty_value',
                                  'not_a_filter', 'no_filters'])
def test_ignored_inputs_generate_no_sql(parsed, name):
    """These must be dropped entirely. Turning them into SQL is what produced a
    500 for ?filter_workSchedule=... — workSchedule is not in the parquet."""
    assert parsed[name]['clauses'] == [], (
        f'{name} should be ignored, got {parsed[name]["clauses"]}')


def test_unfiltered_count_is_the_whole_file(parsed, con):
    total = con.execute(f"SELECT count(*) FROM read_parquet('{PARQUET}')").fetchone()[0]
    assert _count(con, parsed['no_filters']) == total


def test_grade_canonicalization_is_load_bearing(parsed, con):
    """Filtering GS-7 must also return the GS-07 rows.

    Guarded against passing vacuously: if either spelling disappears from the
    data this fails loudly rather than silently testing nothing.
    """
    padded, unpadded = [
        con.execute(f"SELECT count(*) FROM read_parquet('{PARQUET}') WHERE grade = ?",
                    [g]).fetchone()[0] for g in ('GS-07', 'GS-7')
    ]
    assert padded > 0 and unpadded > 0, (
        f'both spellings must exist for this test to mean anything '
        f'(GS-07={padded}, GS-7={unpadded})')
    assert _count(con, parsed['grade_unpadded']) == padded + unpadded
    # and the padded spelling must select the same set
    assert _count(con, parsed['grade_padded']) == padded + unpadded


def test_ungraded_is_not_collapsed(parsed, con):
    """XX-00 keeps its zero — only -0[1-9] collapses. If this broke, every
    ungraded posting would silently vanish from an XX-00 filter."""
    direct = con.execute(
        f"SELECT count(*) FROM read_parquet('{PARQUET}') "
        f"WHERE lower(grade) = 'xx-00'").fetchone()[0]
    assert _count(con, parsed['grade_ungraded']) == direct


def test_comma_in_agency_name_is_not_split(parsed, con):
    """The value contains a comma. It must match that agency exactly, not become
    an OR of substrings that also matches unrelated agencies."""
    exact = con.execute(
        f"SELECT count(*) FROM read_parquet('{PARQUET}') WHERE hiringAgencyName = ?",
        [AGENCY_WITH_COMMA]).fetchone()[0]
    assert exact > 0, 'test value no longer present in the data'
    assert _count(con, parsed['agency_with_comma']) == exact


def test_multi_value_series_matches_any_value_in_the_list(parsed, con):
    """occupationalSeries is '; '-delimited; a filter must match a listing that
    carries 2210 among several series, not only listings that are exactly 2210."""
    got = _count(con, parsed['series_single'])
    exact_only = con.execute(
        f"SELECT count(*) FROM read_parquet('{PARQUET}') "
        f"WHERE occupationalSeries LIKE '2210%' AND occupationalSeries NOT LIKE '%;%'"
    ).fetchone()[0]
    assert got > exact_only, (
        f'multi-value matching looks broken: {got} rows vs {exact_only} '
        f'single-series rows — expected strictly more')


def test_multiselect_is_the_union_of_its_values(parsed, con):
    a, b = [con.execute(
        f"SELECT count(*) FROM read_parquet('{PARQUET}') WHERE hiringDepartmentName = ?",
        [d]).fetchone()[0] for d in (DEPT_A, DEPT_B)]
    assert _count(con, parsed['dept_multiselect']) == a + b


def test_whitespace_around_multiselect_values_is_trimmed(parsed, con):
    assert _count(con, parsed['whitespace_values']) == _count(con, parsed['dept_multiselect'])


def test_text_search_is_case_insensitive(parsed, con):
    assert _count(con, parsed['uppercase_value']) == _count(con, parsed['title_text'])


def test_comma_separated_text_terms_are_an_or(parsed, con):
    """'analyst, engineer' matches either term, so it must return at least as
    many rows as 'analyst' alone."""
    assert _count(con, parsed['title_text_or']) >= _count(con, parsed['title_text'])


def test_range_filters_narrow_the_result(parsed, con):
    total = _count(con, parsed['no_filters'])
    for name in ('salary_min', 'salary_max', 'salary_range', 'date_min',
                 'date_max', 'date_range'):
        n = _count(con, parsed[name])
        assert 0 < n < total, f'{name} returned {n} of {total} — not filtering'


# ---- structural guards -------------------------------------------------------

def _js_const(name):
    js = (WEB / 'shared' / 'filters.js').read_text()
    m = re.search(rf'export const {name} = \[(.*?)\];', js, re.S)
    assert m, f'{name} not found in shared/filters.js'
    return re.findall(r"'([^']+)'", m.group(1))


def test_every_filterable_column_exists_in_the_parquet(con):
    """Regression guard for the workSchedule 500. An unknown filter_ param is
    ignored, but a *listed* one builds SQL — so a name here that is missing from
    the file turns a user's filter into an error."""
    js = (WEB / 'shared' / 'filters.js').read_text()
    m = re.search(r'export const FILTERABLE_COLUMNS = new Set\(\[(.*?)\]\);', js, re.S)
    assert m, 'FILTERABLE_COLUMNS not found in shared/filters.js'
    extra = re.findall(r"'([^']+)'", m.group(1))
    declared = set(_js_const('COLUMNS')) | set(extra)

    actual = {r[0] for r in
              con.execute(f"DESCRIBE SELECT * FROM read_parquet('{PARQUET}')").fetchall()}
    missing = sorted(declared - actual)
    assert not missing, f'FILTERABLE_COLUMNS names columns absent from the parquet: {missing}'


def test_static_json_row_order_matches_the_table():
    """static.json's precomputed first page must be in the order the table
    renders. If prep_web_data.py's DISPLAY_COLUMNS and filters.js's COLUMNS
    drift, every value lands in the wrong column with no error — the rows still
    look like plausible data."""
    prep = (REPO / 'scripts' / 'prep_web_data.py').read_text()
    m = re.search(r'DISPLAY_COLUMNS = \[(.*?)\]', prep, re.S)
    assert m, 'DISPLAY_COLUMNS not found in scripts/prep_web_data.py'
    assert re.findall(r'"([^"]+)"', m.group(1)) == _js_const('COLUMNS')
