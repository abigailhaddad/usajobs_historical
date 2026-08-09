// The site's whole data layer. This IS the API — there is no server side left.
//
// It began as browser-side replacements for the Python functions in api/
// (aggregate.py, jobs.py, pivot.py, filter_options.py, download.py,
// static_data.py), all six deleted on 2026-08-09. Each function here still
// returns exactly the JSON shape its endpoint returned, which is why the
// calling code in index.html / pivot.html changed only where the data comes
// from, not how it is used. tests/wasm_api_probe.html is the record of that
// port being diffed against the live Python API while it still existed
// (28/28 cases matched); it cannot re-run now that the endpoints are gone.
//
// Deliberately plain functions taking an explicit `conn`. No client object, no
// wrapper class, no patched global fetch — the call sites say what they fetch.
//
// IMPORTANT: query the parquet with read_parquet('https://…') in the SQL text.
// db.registerFileURL() downloads the ENTIRE file up front (measured: one GET
// 200 for all 51 MB) no matter what directIO says. Only the SQL path issues
// Range requests. See memory/reference_duckdb_wasm_range_reads.md.

import * as duckdb from 'https://esm.sh/@duckdb/duckdb-wasm';

import {
  COLUMNS, COLUMN_HEADERS, TEXT_SEARCH_COLUMNS, DROPDOWN_FIELDS,
  MULTI_VALUE_FIELDS, canonicalGrade, parseFilters,
} from './filters.js';

// Matches MAX_ROWS in the old api/download.py.
export const DOWNLOAD_ROW_LIMIT = 100000;

/** Boot DuckDB-WASM. Returns a connection; `src` is the SQL-ready table ref. */
export async function initDb(parquetUrl) {
  const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' }));
  const worker = new Worker(workerUrl);
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerUrl);

  const conn = await db.connect();
  await conn.query('INSTALL httpfs; LOAD httpfs;');
  conn.__src = `read_parquet('${parquetUrl}')`;
  return conn;
}

function src(conn) {
  if (!conn.__src) throw new Error('connection was not created by initDb()');
  return conn.__src;
}

/** Run SQL with positional binds and return plain row arrays. */
async function run(conn, sql, binds = []) {
  if (binds.length === 0) {
    const res = await conn.query(sql);
    return res.toArray().map(r => Object.values(r.toJSON()));
  }
  const stmt = await conn.prepare(sql);
  try {
    const res = await stmt.query(...binds);
    return res.toArray().map(r => Object.values(r.toJSON()));
  } finally {
    await stmt.close();
  }
}

// DuckDB-WASM returns BigInt for COUNT(*). JSON.stringify throws on BigInt and
// comparisons against numbers silently fail, so normalize at the boundary.
const num = (v) => (typeof v === 'bigint' ? Number(v) : v);

function whereFrom(params) {
  const { clauses, bindValues } = parseFilters(params);
  return {
    whereSql: clauses.length ? `WHERE ${clauses.join(' AND ')}` : '',
    binds: bindValues,
    any: clauses.length > 0,
  };
}

// ── /api/aggregate ──────────────────────────────────────────────────────────

/** group_by: month | agency | department | grade | series */
export async function aggregate(conn, groupBy, params = {}) {
  const t = src(conn);
  const { whereSql, binds, any } = whereFrom(params);

  if (groupBy === 'month') {
    const sql = `
      WITH base AS (SELECT * FROM ${t} ${whereSql}),
      monthly AS (
        SELECT strftime(CAST("openDate" AS DATE), '%Y-%m') AS month_label,
               COUNT(*) AS cnt,
               ROUND(AVG(CAST("maximumSalary" AS DOUBLE)), 0) AS avg_sal
        FROM base WHERE "openDate" IS NOT NULL GROUP BY month_label),
      dates AS (
        -- Cast to VARCHAR in SQL: duckdb-wasm hands DATE back as epoch
        -- MILLISECONDS, and formatting that in JS produced a plausible-looking
        -- but wrong 10-char string ("1786233600" instead of "2026-08-09").
        SELECT CAST(MIN(CAST("openDate" AS DATE)) AS VARCHAR) AS min_date,
               CAST(MAX(CAST("openDate" AS DATE)) AS VARCHAR) AS max_date
        FROM base WHERE "openDate" IS NOT NULL),
      series AS (
        SELECT COUNT(DISTINCT TRIM(s.v)) AS distinct_series
        FROM base,
        LATERAL (SELECT unnest(string_split(CAST("occupationalSeries" AS VARCHAR), '; ')) AS v) s
        WHERE "occupationalSeries" IS NOT NULL)
      SELECT m.month_label, m.cnt, m.avg_sal, d.min_date, d.max_date, s.distinct_series
      FROM monthly m CROSS JOIN dates d CROSS JOIN series s
      ORDER BY m.month_label`;
    const rows = await run(conn, sql, binds);

    if (rows.length === 0) {
      return { labels: [], datasets: { count: [], avg_salary: [], distinct_series: 0, min_date: null, max_date: null } };
    }

    const minDate = rows[0][3] == null ? null : String(rows[0][3]);
    const maxDate = rows[0][4] == null ? null : String(rows[0][4]);
    const seriesCount = num(rows[0][5]);

    // Fill missing months so the X axis stays continuous.
    const rowMap = new Map(rows.map(r => [r[0], r]));
    const labels = [];
    let [y, m] = [parseInt(rows[0][0].slice(0, 4), 10), parseInt(rows[0][0].slice(5, 7), 10)];
    const last = rows[rows.length - 1][0];
    const [endY, endM] = [parseInt(last.slice(0, 4), 10), parseInt(last.slice(5, 7), 10)];
    while (y < endY || (y === endY && m <= endM)) {
      labels.push(`${String(y).padStart(4, '0')}-${String(m).padStart(2, '0')}`);
      m += 1;
      if (m > 12) { m = 1; y += 1; }
    }

    return {
      labels,
      datasets: {
        count: labels.map(mo => (rowMap.has(mo) ? num(rowMap.get(mo)[1]) : 0)),
        avg_salary: labels.map(mo => {
          const r = rowMap.get(mo);
          return r && r[2] != null ? Math.trunc(Number(r[2])) : 0;
        }),
        distinct_series: seriesCount,
        min_date: minDate,
        max_date: maxDate,
      },
    };
  }

  if (groupBy === 'agency' || groupBy === 'department') {
    const col = groupBy === 'agency' ? 'hiringAgencyName' : 'hiringDepartmentName';
    const rows = await run(conn, `
      SELECT COALESCE(CAST("${col}" AS VARCHAR), 'Unknown') AS k, COUNT(*) AS cnt
      FROM ${t} ${whereSql} GROUP BY k ORDER BY cnt DESC LIMIT 20`, binds);
    const datasets = { count: rows.map(r => num(r[1])) };
    if (groupBy === 'department') {
      const [[total]] = await run(conn, `
        SELECT COUNT(DISTINCT COALESCE(CAST("${col}" AS VARCHAR), 'Unknown')) FROM ${t} ${whereSql}`, binds);
      datasets.total_distinct = num(total);
    }
    return { labels: rows.map(r => r[0]), datasets };
  }

  if (groupBy === 'grade') {
    // grade is "GS-7", "GS-7/9", "GS-7/9/11". Expand the range so GS-7/9
    // counts once at each of GS-7, GS-8, GS-9.
    const rows = await run(conn, `
      WITH raw AS (
        SELECT CAST("grade" AS VARCHAR) AS g FROM ${t} ${whereSql}
        ${any ? 'AND' : 'WHERE'} CAST("grade" AS VARCHAR) LIKE 'GS-%'),
      min_max AS (
        SELECT CAST(regexp_extract(g, 'GS-(\\d+)', 1) AS INTEGER) AS lo,
               CAST(regexp_extract_all(g, '(\\d+)')[length(regexp_extract_all(g, '(\\d+)'))] AS INTEGER) AS hi
        FROM raw),
      expanded AS (
        SELECT unnest(generate_series(lo, hi)) AS gs_grade
        FROM min_max WHERE lo IS NOT NULL AND hi IS NOT NULL)
      SELECT gs_grade AS grade, COUNT(*) AS cnt FROM expanded
      WHERE gs_grade BETWEEN 1 AND 15 GROUP BY gs_grade ORDER BY gs_grade`, binds);
    return { labels: rows.map(r => num(r[0])), datasets: { count: rows.map(r => num(r[1])) } };
  }

  if (groupBy === 'series') {
    const rows = await run(conn, `
      SELECT val, COUNT(*) AS cnt FROM (
        SELECT TRIM(unnest(string_split(CAST("occupationalSeries" AS VARCHAR), '; '))) AS val
        FROM ${t} ${whereSql} ${any ? 'AND' : 'WHERE'} "occupationalSeries" IS NOT NULL)
      WHERE val != '' GROUP BY val ORDER BY cnt DESC LIMIT 10`, binds);
    return { labels: rows.map(r => r[0]), datasets: { count: rows.map(r => num(r[1])) } };
  }

  throw new Error(`group_by must be one of: month, agency, department, grade, series (got ${groupBy})`);
}

// ── /api/jobs ───────────────────────────────────────────────────────────────

/** DataTables server-side payload. `params` is the DataTables request object. */
export async function jobs(conn, params = {}) {
  const t = src(conn);
  const draw = parseInt(params.draw ?? 1, 10);
  const start = Math.max(0, parseInt(params.start ?? 0, 10));
  const length = Math.min(100, Math.max(1, parseInt(params.length ?? 25, 10)));

  // Sort — up to 3 columns, defaulting to openDate DESC.
  const orderParts = [];
  for (let i = 0; i < 3; i += 1) {
    const colIdx = params[`order[${i}][column]`];
    if (colIdx === undefined) break;
    const name = COLUMNS[parseInt(colIdx, 10)];
    if (name) {
      const dir = String(params[`order[${i}][dir]`] ?? 'desc').toLowerCase();
      orderParts.push([name, dir === 'desc' ? 'DESC' : 'ASC']);
    }
  }
  if (orderParts.length === 0) orderParts.push(['openDate', 'DESC']);

  const clauses = [];
  const binds = [];

  const searchValue = String(params['search[value]'] ?? '').trim();
  if (searchValue) {
    const parts = TEXT_SEARCH_COLUMNS.map((c) => {
      binds.push(`%${searchValue.toLowerCase()}%`);
      return `LOWER(COALESCE(CAST("${c}" AS VARCHAR), '')) LIKE ?`;
    });
    clauses.push(`(${parts.join(' OR ')})`);
  }

  const f = parseFilters(params);
  clauses.push(...f.clauses);
  binds.push(...f.bindValues);

  const whereSql = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
  const colList = COLUMNS.map(c => `COALESCE(CAST("${c}" AS VARCHAR), '')`).join(', ');
  const orderClause = orderParts
    .map(([c, d]) => `COALESCE(CAST("${c}" AS VARCHAR), '') ${d}`).join(', ');

  const [[recordsTotal]] = await run(conn, `SELECT COUNT(*) FROM ${t}`);
  const rows = await run(conn, `
    SELECT ${colList}, COUNT(*) OVER() AS _total_filtered
    FROM ${t} ${whereSql} ORDER BY ${orderClause} LIMIT ? OFFSET ?`,
  [...binds, length, start]);

  return {
    draw,
    recordsTotal: num(recordsTotal),
    recordsFiltered: rows.length ? num(rows[0][rows[0].length - 1]) : (clauses.length ? 0 : num(recordsTotal)),
    data: rows.map(r => r.slice(0, -1)),
  };
}

// ── /api/filter_options ─────────────────────────────────────────────────────

export async function filterOptions(conn, field, params = {}) {
  if (!DROPDOWN_FIELDS.has(field)) {
    throw new Error(`field must be one of: ${[...DROPDOWN_FIELDS].sort().join(', ')}`);
  }
  const t = src(conn);
  const { whereSql, binds, any } = whereFrom(params);
  const expr = MULTI_VALUE_FIELDS.has(field)
    ? `TRIM(unnest(string_split(CAST("${field}" AS VARCHAR), '; ')))`
    : `TRIM(CAST("${field}" AS VARCHAR))`;

  const rows = await run(conn, `
    SELECT DISTINCT ${expr} AS val FROM ${t} ${whereSql}
    ${any ? 'AND' : 'WHERE'} "${field}" IS NOT NULL
    AND TRIM(CAST("${field}" AS VARCHAR)) != '' ORDER BY val`, binds);

  let values = rows.map(r => r[0]).filter(v => v != null && v.trim());
  if (field === 'grade') values = collapseGrades(values);
  return { values, count: values.length };
}

/** Collapse zero-padded grade variants (GS-07 -> GS-7) to one option each,
 *  preferring the un-padded spelling for display.
 *
 *  Exported because the precomputed lists in static.json are NOT collapsed —
 *  scripts/prep_web_data.py takes a plain DISTINCT — so priming a dropdown
 *  from static.json would offer GS-07 and GS-7 as separate choices (3123
 *  options instead of 2608). Both paths run this, so they agree. Idempotent. */
export function collapseGrades(values) {
  const seen = new Map();
  for (const v of values) {
    const key = canonicalGrade(v);
    const canonDisplay = key.toUpperCase();
    if (v.toUpperCase() === canonDisplay) seen.set(key, canonDisplay);
    else if (!seen.has(key)) seen.set(key, v);
  }
  return [...seen.values()].sort();
}

// ── /api/pivot ──────────────────────────────────────────────────────────────

// key -> [header label, single-value SQL expression, multi-value source column].
// Exactly one of expr / multiCol is set. multiCol values are split on '; ',
// unnested, and counted with COUNT(DISTINCT control number).
const DIMENSIONS = {
  department:      ['Department',       `COALESCE(CAST("hiringDepartmentName" AS VARCHAR), 'Unknown')`, null],
  agency:          ['Agency',           `COALESCE(CAST("hiringAgencyName" AS VARCHAR), 'Unknown')`, null],
  year:            ['Year',             `strftime(CAST("openDate" AS DATE), '%Y')`, null],
  month:           ['Month',            `strftime(CAST("openDate" AS DATE), '%Y-%m')`, null],
  grade:           ['Grade',            `COALESCE(CAST("grade" AS VARCHAR), 'Unknown')`, null],
  appointmentType: ['Appointment Type', `COALESCE(CAST("appointmentType" AS VARCHAR), 'Unknown')`, null],
  serviceType:     ['Service',          `COALESCE(CAST("serviceType" AS VARCHAR), 'Unknown')`, null],
  status:          ['Status',           `COALESCE(CAST("status" AS VARCHAR), 'Unknown')`, null],
  series:          ['Occ. Series',      null, 'occupationalSeries'],
  location:        ['Location',         null, 'locations'],
};

const CONTROL = '"usajobsControlNumber"';
const MAX_DIMS = 6;
const MAX_PREVIEW_ROWS = 500;
const MAX_CSV_ROWS = 200000;

/** Group by leading dimensions ascending, then rank by count within the
 *  innermost level: [Year, Department] -> each year's top departments. */
function orderClause(groupCols) {
  return [...groupCols.slice(0, -1), 'cnt DESC', groupCols[groupCols.length - 1]].join(', ');
}

function buildPivotQuery(t, dims, whereSql) {
  const headers = dims.map(k => DIMENSIONS[k][0]);
  const groupCols = dims.map((_, i) => `d${i}`);
  const orderBy = orderClause(groupCols);
  const multiIdx = dims.findIndex(k => DIMENSIONS[k][2]);

  if (multiIdx === -1) {
    // One row per listing, so COUNT(*) is already the distinct-listing count.
    const selects = dims.map((k, i) => `${DIMENSIONS[k][1]} AS d${i}`);
    return {
      headers,
      sql: `SELECT ${selects.join(', ')}, COUNT(*) AS cnt FROM ${t} ${whereSql} ` +
           `GROUP BY ${groupCols.join(', ')} ORDER BY ${orderBy}`,
    };
  }

  // One multi-value dimension: unnest it in a flat subquery keeping the control
  // number, then COUNT(DISTINCT control) so a listing counts once per value it
  // actually carries.
  const multiCol = DIMENSIONS[dims[multiIdx]][2];
  const innerSelects = [`${CONTROL} AS _cn`, ...dims.map((k, i) => (
    i === multiIdx
      ? `TRIM(unnest(string_split(CAST("${multiCol}" AS VARCHAR), '; '))) AS d${i}`
      : `${DIMENSIONS[k][1]} AS d${i}`))];
  const alias = `d${multiIdx}`;
  return {
    headers,
    sql: `SELECT ${groupCols.join(', ')}, COUNT(DISTINCT _cn) AS cnt FROM (` +
         `SELECT ${innerSelects.join(', ')} FROM ${t} ${whereSql}) sub ` +
         `WHERE ${alias} IS NOT NULL AND ${alias} <> '' ` +
         `GROUP BY ${groupCols.join(', ')} ORDER BY ${orderBy}`,
  };
}

/** Validate dims the way the endpoint did. Returns an error string or null. */
function validateDims(dims) {
  if (!dims.length) return 'dims is required, e.g. dims=department,year';
  const bad = dims.filter(d => !(d in DIMENSIONS));
  if (bad.length) return `unknown dimension(s): ${bad.join(', ')}`;
  if (dims.length > MAX_DIMS) return `at most ${MAX_DIMS} dimensions allowed`;
  if (dims.filter(d => DIMENSIONS[d][2]).length > 1) {
    return 'pick at most one multi-value field (occupational series or location) at a time';
  }
  return null;
}

export async function pivot(conn, dims, params = {}) {
  const err = validateDims(dims);
  if (err) return { error: err };
  const { whereSql, binds } = whereFrom(params);
  const { sql, headers } = buildPivotQuery(src(conn), dims, whereSql);
  const rows = await run(conn, `${sql} LIMIT ${MAX_PREVIEW_ROWS + 1}`, binds);
  const truncated = rows.length > MAX_PREVIEW_ROWS;
  return {
    headers: [...headers, 'Count'],
    rows: (truncated ? rows.slice(0, MAX_PREVIEW_ROWS) : rows).map(r => r.map(num)),
    truncated,
    preview_limit: MAX_PREVIEW_ROWS,
    multi_value_dims: dims.filter(d => DIMENSIONS[d][2]),
  };
}

/** The same pivot as a CSV string (the old ?format=csv path). */
export async function pivotCsv(conn, dims, params = {}) {
  const err = validateDims(dims);
  if (err) throw new Error(err);
  const { whereSql, binds } = whereFrom(params);
  const { sql, headers } = buildPivotQuery(src(conn), dims, whereSql);
  const rows = await run(conn, `${sql} LIMIT ${MAX_CSV_ROWS}`, binds);
  return toCsv([...headers, 'Count'], rows.map(r => r.map(num)));
}

// ── /api/download ───────────────────────────────────────────────────────────

/** Rows matching the filters, as a CSV string. Throws over the row limit. */
export async function downloadCsv(conn, params = {}) {
  const t = src(conn);
  const { whereSql, binds } = whereFrom(params);

  const [[count]] = await run(conn, `SELECT COUNT(*) FROM ${t} ${whereSql}`, binds);
  const rowCount = num(count);
  if (rowCount > DOWNLOAD_ROW_LIMIT) {
    throw new Error(
      `This export has ${rowCount.toLocaleString()} rows, over the ` +
      `${DOWNLOAD_ROW_LIMIT.toLocaleString()}-row CSV limit.`);
  }

  const colList = COLUMNS.map(c => `COALESCE(CAST("${c}" AS VARCHAR), '')`).join(', ');
  const rows = await run(conn, `
    SELECT ${colList} FROM ${t} ${whereSql}
    ORDER BY COALESCE(CAST("openDate" AS VARCHAR), '') DESC LIMIT ${DOWNLOAD_ROW_LIMIT}`, binds);

  return toCsv(COLUMN_HEADERS, rows);
}

/** RFC4180-ish CSV. Shared by the job export and the pivot export. */
function toCsv(headers, rows) {
  const esc = (v) => {
    const s = v == null ? '' : String(v);
    return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [headers, ...rows].map(r => r.map(esc).join(',')).join('\r\n');
}
