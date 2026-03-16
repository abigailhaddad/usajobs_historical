"""
Single source of truth for column definitions and filter parsing.

Every API endpoint imports from here so that column lists, filter types,
and WHERE-clause generation stay consistent.
"""

# Ordered list of columns returned by /api/jobs and /api/download.
# The index in this list is the column index DataTables sends in order[] params.
COLUMNS = [
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
]

COLUMN_HEADERS = [
    'Position Title',
    'Occ. Series',
    'Department',
    'Agency',
    'Grade',
    'Min Salary',
    'Max Salary',
    'Open Date',
    'Close Date',
    'Appointment Type',
    'Service',
    'Locations',
    'Status',
    'Control Number',
]

# Map column index -> column name (for DataTables sort params)
SORTABLE_COLUMNS = {i: col for i, col in enumerate(COLUMNS)}

# All columns that can appear in filter_* query params.
# This is a superset of COLUMNS — includes extra parquet columns
# that are filterable but not displayed in the table.
FILTERABLE_COLUMNS = set(COLUMNS) | {
    'occupationalSeries',
    'workSchedule',
}

# Columns searchable via the global search box
TEXT_SEARCH_COLUMNS = [
    'positionTitle',
    'hiringDepartmentName',
    'hiringAgencyName',
    'grade',
    'locations',
    'appointmentType',
    'serviceType',
    'status',
]

# Columns that appear in the filter_options dropdown endpoint
DROPDOWN_FIELDS = {
    'positionTitle', 'hiringAgencyName', 'hiringDepartmentName',
    'grade', 'appointmentType', 'serviceType', 'status',
    'occupationalSeries',
}

# Date columns use DATE comparison instead of DOUBLE for range filters
DATE_COLUMNS = {'openDate', 'closeDate'}

# Columns that store semicolon-delimited lists of values
# Filters on these columns use substring matching (any value in the list)
MULTI_VALUE_FIELDS = {'occupationalSeries'}


def parse_filters(params):
    """Parse filter_ prefixed query params into WHERE clauses and bind values.

    Supports three filter shapes:
      - filter_col=val1|val2     → LOWER(col) IN (?, ?)       (multiselect)
      - filter_col_min=N         → CAST(col AS DOUBLE) >= ?    (range min)
      - filter_col_max=N         → CAST(col AS DOUBLE) <= ?    (range max)
      - filter_col=text          → LOWER(col) LIKE ?           (text search)
    """
    clauses = []
    bind_values = []

    for key, values in params.items():
        if not key.startswith('filter_'):
            continue

        param_name = key[len('filter_'):]
        value = values[0] if values else ''

        if not value:
            continue

        # Range filters: filter_minimumSalary_min, filter_openDate_min, etc.
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

        # Column must be valid
        if param_name not in FILTERABLE_COLUMNS:
            continue

        # Pipe-separated multiselect
        if '|' in value:
            parts = [v.strip() for v in value.split('|') if v.strip()]
            if param_name in MULTI_VALUE_FIELDS:
                # For semicolon-delimited fields, check if any selected value
                # appears as a substring (matches individual items in the list)
                like_parts = []
                for p in parts:
                    like_parts.append(
                        f'LOWER(COALESCE(CAST("{param_name}" AS VARCHAR), \'\')) LIKE ?'
                    )
                    bind_values.append(f'%{p.lower()}%')
                clauses.append(f'({" OR ".join(like_parts)})')
            else:
                placeholders = ', '.join(['?'] * len(parts))
                clauses.append(
                    f'LOWER(COALESCE(CAST("{param_name}" AS VARCHAR), \'\')) IN ({placeholders})'
                )
                bind_values.extend([p.lower() for p in parts])
        else:
            # Text search with LIKE — comma-separated terms match any (OR)
            terms = [t.strip() for t in value.split(',') if t.strip()] if ',' in value else [value]
            if len(terms) > 1:
                like_parts = []
                for t in terms:
                    like_parts.append(
                        f'LOWER(COALESCE(CAST("{param_name}" AS VARCHAR), \'\')) LIKE ?'
                    )
                    bind_values.append(f'%{t.lower()}%')
                clauses.append(f'({" OR ".join(like_parts)})')
            else:
                clauses.append(
                    f'LOWER(COALESCE(CAST("{param_name}" AS VARCHAR), \'\')) LIKE ?'
                )
                bind_values.append(f'%{value.lower()}%')

    return clauses, bind_values
