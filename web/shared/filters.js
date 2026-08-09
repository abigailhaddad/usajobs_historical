// Port of api/columns.py to the browser. Single source of truth for column
// definitions and filter parsing once the Python functions are deleted.
//
// This file is checked against columns.py by tests/test_filters_parity.py,
// which runs both implementations over a corpus of filter params and compares
// the generated SQL, the bind values, AND the row counts they produce against
// the real parquet. Change one side without the other and that test fails.

// Ordered list of columns returned by the jobs listing and the CSV download.
// The index here is the column index DataTables sends in order[] params.
export const COLUMNS = [
  'positionTitle',
  'occupationalSeries',
  'hiringDepartmentName',
  'hiringAgencyName',
  'grade',
  'minimumSalary',
  'maximumSalary',
  'openDate',
  'closeDate',
  'appointmentType',
  'serviceType',
  'locations',
  'status',
  'usajobsControlNumber',
];

export const COLUMN_HEADERS = [
  'Position Title', 'Occ. Series', 'Department', 'Agency', 'Grade',
  'Min Salary', 'Max Salary', 'Open Date', 'Close Date', 'Appointment Type',
  'Service', 'Locations', 'Status', 'Control Number',
];

export const SORTABLE_COLUMNS = Object.fromEntries(COLUMNS.map((c, i) => [i, c]));

// Every name here MUST exist in the parquet: an unknown filter_ param is
// ignored, but a listed one builds SQL and a missing column is a hard error.
// 'workSchedule' was listed and absent, which returned 500 in production until
// 2026-08-09. tests/test_filters_parity.py asserts this set matches the file.
export const FILTERABLE_COLUMNS = new Set([...COLUMNS, 'occupationalSeries']);

export const TEXT_SEARCH_COLUMNS = [
  'positionTitle', 'hiringDepartmentName', 'hiringAgencyName', 'grade',
  'locations', 'appointmentType', 'serviceType', 'status',
];

export const DROPDOWN_FIELDS = new Set([
  'positionTitle', 'hiringAgencyName', 'hiringDepartmentName', 'grade',
  'appointmentType', 'serviceType', 'status', 'occupationalSeries',
]);

// Date columns compare as DATE, not DOUBLE, for range filters.
export const DATE_COLUMNS = new Set(['openDate', 'closeDate']);

// Columns storing semicolon-delimited lists — filters use substring matching
// so any single value in the list can match.
export const MULTI_VALUE_FIELDS = new Set(['occupationalSeries']);

// Values that come from a known dropdown must match the whole literal, not a
// substring. Without this an agency name containing a comma (e.g.
// "Treasury, Financial Crimes Enforcement Network") gets split into an OR
// substring search and silently matches the wrong rows.
export const EXACT_MATCH_FIELDS = new Set([
  'hiringAgencyName', 'hiringDepartmentName', 'grade',
  'appointmentType', 'serviceType', 'status',
]);

// Pay-plan codes appear both zero-padded ("GS-07") and not ("GS-7"). Treat them
// as equivalent everywhere, so picking "GS-7" returns all GS-7 listings and not
// just the agencies that happen to write it unpadded.
const GRADE_ZERO_PAD = /([-/])0([1-9])/g;
const GRADE_DUCKDB_REGEX = '([-/])0([1-9])';

/** Strip leading zeros after '-' or '/': GS-07 -> GS-7, GS-07/09 -> GS-7/9.
 *  Leaves XX-00 ("ungraded") alone because the second digit is 0. */
export function canonicalGrade(value) {
  if (!value) return '';
  return String(value).toLowerCase().replace(GRADE_ZERO_PAD, '$1$2');
}

/** SQL for the column side of a comparison. For 'grade', strips zero-padding
 *  on the column side too so it matches the canonicalGrade() bind value. */
function filterColExpr(col) {
  const base = `LOWER(COALESCE(CAST("${col}" AS VARCHAR), ''))`;
  if (col === 'grade') {
    return `regexp_replace(${base}, '${GRADE_DUCKDB_REGEX}', '\\1\\2', 'g')`;
  }
  return base;
}

function normalizeFilterValue(col, value) {
  if (col === 'grade') return canonicalGrade(value);
  return String(value).toLowerCase();
}

/**
 * Parse filter_-prefixed params into WHERE clauses and bind values.
 *
 * Supported shapes, matching columns.py exactly:
 *   filter_col=val1|val2  -> LOWER(col) IN (?, ?)      multiselect
 *   filter_col_min=N      -> CAST(col AS DOUBLE) >= ?  range min
 *   filter_col_max=N      -> CAST(col AS DOUBLE) <= ?  range max
 *   filter_col=text       -> LOWER(col) LIKE ?         text search
 *
 * @param params URLSearchParams, or a plain object of key -> string | string[]
 * @returns {{clauses: string[], bindValues: Array}}
 */
export function parseFilters(params) {
  const clauses = [];
  const bindValues = [];

  // Normalize to [key, firstValue] pairs, preserving insertion order so the
  // generated clause order matches Python's dict iteration order.
  const entries = [];
  if (params instanceof URLSearchParams) {
    const seen = new Set();
    for (const key of params.keys()) {
      if (seen.has(key)) continue;
      seen.add(key);
      entries.push([key, params.get(key)]);
    }
  } else {
    for (const [key, v] of Object.entries(params)) {
      entries.push([key, Array.isArray(v) ? (v.length ? v[0] : '') : v]);
    }
  }

  for (const [key, raw] of entries) {
    if (!key.startsWith('filter_')) continue;

    const paramName = key.slice('filter_'.length);
    const value = raw == null ? '' : String(raw);
    if (!value) continue;

    // Range filters: filter_minimumSalary_min, filter_openDate_max, ...
    for (const [suffix, op] of [['_min', '>='], ['_max', '<=']]) {
      if (!paramName.endsWith(suffix)) continue;
      const col = paramName.slice(0, -suffix.length);
      if (FILTERABLE_COLUMNS.has(col)) {
        if (DATE_COLUMNS.has(col)) {
          clauses.push(`CAST("${col}" AS DATE) ${op} CAST(? AS DATE)`);
          bindValues.push(value);
        } else {
          const n = Number(value);
          // columns.py does float(value), which raises on junk and surfaces as
          // a 500. Mirror that rather than silently dropping the filter.
          if (Number.isNaN(n)) throw new Error(`could not convert to float: ${value}`);
          clauses.push(`CAST("${col}" AS DOUBLE) ${op} ?`);
          bindValues.push(n);
        }
      }
      break;
    }
    if (paramName.endsWith('_min') || paramName.endsWith('_max')) continue;

    if (!FILTERABLE_COLUMNS.has(paramName)) continue;

    const colExpr = filterColExpr(paramName);

    if (value.includes('|')) {
      const parts = value.split('|').map(v => v.trim()).filter(Boolean);
      if (MULTI_VALUE_FIELDS.has(paramName)) {
        const likeParts = [];
        for (const p of parts) {
          likeParts.push(`${colExpr} LIKE ?`);
          bindValues.push(`%${normalizeFilterValue(paramName, p)}%`);
        }
        clauses.push(`(${likeParts.join(' OR ')})`);
      } else {
        const placeholders = parts.map(() => '?').join(', ');
        clauses.push(`${colExpr} IN (${placeholders})`);
        for (const p of parts) bindValues.push(normalizeFilterValue(paramName, p));
      }
    } else if (EXACT_MATCH_FIELDS.has(paramName)) {
      clauses.push(`${colExpr} = ?`);
      bindValues.push(normalizeFilterValue(paramName, value));
    } else {
      const terms = value.includes(',')
        ? value.split(',').map(t => t.trim()).filter(Boolean)
        : [value];
      if (terms.length > 1) {
        const likeParts = [];
        for (const t of terms) {
          likeParts.push(`${colExpr} LIKE ?`);
          bindValues.push(`%${normalizeFilterValue(paramName, t)}%`);
        }
        clauses.push(`(${likeParts.join(' OR ')})`);
      } else {
        clauses.push(`${colExpr} LIKE ?`);
        bindValues.push(`%${normalizeFilterValue(paramName, value)}%`);
      }
    }
  }

  return { clauses, bindValues };
}

/** WHERE fragment (empty string when no filters), matching _build_where(). */
export function buildWhere(params) {
  const { clauses, bindValues } = parseFilters(params);
  return {
    whereSql: clauses.length ? `WHERE ${clauses.join(' AND ')}` : '',
    bindValues,
  };
}
