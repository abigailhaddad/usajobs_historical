"""
Utilities for combining historical and current USAJobs parquet files.

Background
----------
Two parquet families live side-by-side:

  historical_jobs_{year}.parquet  — built from the historical search API, which
      always returns the bureau-level agency name (e.g. "Internal Revenue Service",
      "Executive Office for U.S. Attorneys and the Office of the U.S. Attorneys").

  current_jobs_{year}.parquet  — built from the current search API, which uses
      OrganizationName for the bureau name.  When OrganizationName is null the
      code falls back to DepartmentName (e.g. "Department of Justice"), collapsing
      every sub-bureau into its parent department.

The same job can therefore appear in both families with *different* hiringAgencyName
values: the historical record has the specific bureau name while the current record
has only the department name.

Naively unioning both (e.g. via read_parquet([hist_urls + curr_urls])) and then
GROUP BY hiringAgencyName will double-count those jobs — once under the specific
bureau name and once under the department name.

Correct approach
----------------
Deduplicate by usajobsControlNumber, preferring whichever record has the more
specific agency name (hiringAgencyName != hiringDepartmentName).  As a secondary
tiebreak prefer historical records, which tend to have richer metadata (locations,
etc.).  Finally, for any surviving current-API records that still have the
department-level fallback, attempt a retroactive fix by reading OrganizationName
out of the stored MatchedObjectDescriptor JSON.

Use ``build_deduped_query`` for DuckDB-based analyses, or ``combine_and_fix``
for pandas-based workflows.
"""

import json
from typing import List, Optional

import pandas as pd

# The historical API sends hiring paths as "HiringPaths" through 2024 and
# increasingly as "hiringPaths" from 2025 on; 2026 has only the second. Some
# files carry both columns (never both filled on one row), and duckdb is
# case-insensitive, so it reads the second as "hiringpaths_1". Reading
# HiringPaths alone left it null for every 2026 posting. The historical side
# folds every spelling into HiringPaths; _no_hiring_paths is a null
# placeholder so the coalesce has an argument when a file has neither.
HIRING_PATHS_SPELLINGS = "(?i)^(hiringpaths(_[0-9]+)?|_no_hiring_paths)$"


def build_deduped_query(
    hist_urls: List[str],
    curr_urls: List[str],
    select: str = "*",
    where: str = "",
    coalesce_fields: Optional[List[str]] = None,
) -> str:
    """
    Return a DuckDB SQL string that combines historical and current parquets,
    deduplicates by usajobsControlNumber, and prefers specific agency names.

    Parameters
    ----------
    hist_urls:
        List of quoted URL strings for historical parquets, e.g.
        ["'https://.../historical_jobs_2024.parquet'"]
    curr_urls:
        List of quoted URL strings for current parquets.
    select:
        Columns to SELECT in the final query (default "*").
    where:
        Optional WHERE clause (without the WHERE keyword).
    coalesce_fields:
        Column names to prefer non-null on, across whichever side (historical
        or current) has a value, before picking a winning row. The dedup below
        picks one winning row per control number, and a historical row wins
        whenever agency-name specificity ties (the common case: 99%+ of
        current_jobs control numbers also appear in historical_jobs) -- which
        silently drops a field even when the *other* side's row for that same
        control number has it. Two situations call for this:
          - Fields with no equivalent anywhere in the historicjoa API, so only
            current rows are ever non-null: applicationCap, remoteIndicator,
            financialDisclosureRequired, representedByUnion,
            positionSensitivity.
          - Fields that exist on both sides but are null on one for some
            postings. HiringPaths is the one to watch, but most of what
            looked like a gap there was a column name; see
            HIRING_PATHS_SPELLINGS.
        Passing a name here coalesces it across every row for a control number
        *before* picking a winner, so whichever row wins already carries a
        value if either side had one. Every name passed must actually be a
        column on at least one side (union_by_name adds it as null on the
        other) -- referencing one that exists nowhere is a DuckDB "column not
        found" error, not a silent no-op. Omit this (the default) to get the
        exact query this function has always produced.

    Example
    -------
    >>> sql = build_deduped_query(
    ...     hist_urls=["'https://example.com/historical_jobs_2025.parquet'"],
    ...     curr_urls=["'https://example.com/current_jobs_2025.parquet'"],
    ...     select="usajobsControlNumber, hiringAgencyName, positionTitle",
    ...     where="hiringDepartmentName = 'Department of Justice'",
    ... )
    >>> df = duckdb.connect().execute(sql).df()
    """
    hist_list = ", ".join(hist_urls)
    curr_list = ", ".join(curr_urls)
    where_clause = f"WHERE {where}" if where else ""

    if coalesce_fields:
        exclude_list = ", ".join(coalesce_fields)
        # FIRST_VALUE over "non-null first, then historical (_src_priority=0)
        # before current" -- not MAX(), which would pick whichever side's
        # string sorts alphabetically greater whenever BOTH sides are non-null
        # (true for e.g. HiringPaths in most years). That's wrong: it should
        # still prefer historical when both have a value, matching the row
        # dedup's own preference below, and only fall back to current when
        # historical's is null. MAX() only gives correct coalesce semantics
        # for fields where at most one side is ever non-null (e.g.
        # applicationCap) -- this ordering is correct for both cases.
        coalesced_cols = ",\n           ".join(
            f"first_value({f}) OVER (\n"
            f"               PARTITION BY usajobsControlNumber\n"
            f"               ORDER BY ({f} IS NULL) ASC, _src_priority ASC\n"
            f"               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\n"
            f"           ) AS {f}"
            for f in coalesce_fields
        )
        merged_cte = f"""merged AS (
    SELECT * EXCLUDE ({exclude_list}),
           {coalesced_cols}
    FROM combined
),
"""
        merged_from = "merged"
    else:
        merged_cte = ""
        merged_from = "combined"

    return f"""
WITH hist AS (
    SELECT *, NULL::VARCHAR AS _no_hiring_paths
    FROM read_parquet([{hist_list}], union_by_name=true)
),
combined AS (
    SELECT COLUMNS(c -> NOT regexp_matches(c, '{HIRING_PATHS_SPELLINGS}')),
           coalesce(*COLUMNS('{HIRING_PATHS_SPELLINGS}')) AS HiringPaths,
           0 AS _src_priority
    FROM hist
    UNION ALL BY NAME
    SELECT *, 1 AS _src_priority
    FROM read_parquet([{curr_list}], union_by_name=true)
),
{merged_cte}deduped AS (
    SELECT *
    FROM {merged_from}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY usajobsControlNumber
        ORDER BY
            -- prefer records where agency name is more specific than dept name
            CASE WHEN lower(hiringAgencyName) != lower(hiringDepartmentName) THEN 0 ELSE 1 END,
            -- tiebreak: prefer historical (src_priority=0) over current (1)
            _src_priority
    ) = 1
)
SELECT {select}
FROM deduped
{where_clause}
"""


def _extract_org_name(mod_value) -> Optional[str]:
    """Extract OrganizationName from a stored MatchedObjectDescriptor value."""
    if mod_value is None:
        return None
    if isinstance(mod_value, float):
        return None
    try:
        obj = mod_value if isinstance(mod_value, dict) else json.loads(mod_value)
        name = (obj or {}).get("OrganizationName")
        if name and isinstance(name, str):
            name = name.strip()
            return name if name else None
    except Exception:
        return None
    return None


def fix_agency_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Retroactively correct hiringAgencyName for current-API records where it
    collapsed to the department name.

    Looks for rows where hiringAgencyName == hiringDepartmentName (the telltale
    sign of the OrganizationName-null fallback) and replaces with the value
    stored in MatchedObjectDescriptor.OrganizationName when available.

    Returns a copy of the DataFrame with corrections applied.
    """
    df = df.copy()

    if "MatchedObjectDescriptor" not in df.columns:
        return df
    if "hiringAgencyName" not in df.columns or "hiringDepartmentName" not in df.columns:
        return df

    ag = df["hiringAgencyName"].astype("string").str.strip()
    dept = df["hiringDepartmentName"].astype("string").str.strip()
    buggy = ag.notna() & dept.notna() & (ag.str.lower() == dept.str.lower())

    if not buggy.any():
        return df

    org_names = df.loc[buggy, "MatchedObjectDescriptor"].apply(_extract_org_name)
    org_names = org_names.astype("string").str.strip()
    usable = org_names.notna() & (org_names != "")
    df.loc[org_names[usable].index, "hiringAgencyName"] = org_names[usable].values

    return df


def combine_and_fix(
    hist_frames: List[pd.DataFrame],
    curr_frames: List[pd.DataFrame],
    control_col: str = "usajobsControlNumber",
) -> pd.DataFrame:
    """
    Combine historical and current DataFrames, deduplicate by control number
    (preferring specific agency names and historical records), then apply the
    OrganizationName retroactive fix to any remaining dept-level agency names.

    This mirrors the logic used in prep_web_data.py for the web viewer output.
    """
    for df in hist_frames:
        df["_source"] = "historical"
    for df in curr_frames:
        df["_source"] = "current"

    combined = pd.concat(hist_frames + curr_frames, ignore_index=True)

    # Coalesce control number column variants
    if control_col not in combined.columns and "usajobs_control_number" in combined.columns:
        combined[control_col] = combined["usajobs_control_number"]
    if control_col in combined.columns:
        combined[control_col] = combined[control_col].astype(str)

    # Sort: specific-agency records last so keep="last" picks them
    def _specificity(row):
        ag = str(row.get("hiringAgencyName", "") or "").strip().lower()
        dept = str(row.get("hiringDepartmentName", "") or "").strip().lower()
        # 0 = dept-level fallback (less preferred), 1 = specific (preferred)
        return 1 if (ag and dept and ag != dept) else 0

    combined["_specificity"] = combined.apply(_specificity, axis=1)
    # Within same specificity, historical beats current
    source_order = {"current": 0, "historical": 1}
    combined["_src_order"] = combined["_source"].map(source_order).fillna(0)
    combined = combined.sort_values(["_specificity", "_src_order"], kind="mergesort")

    combined = combined.drop_duplicates(subset=[control_col], keep="last")
    combined = fix_agency_names(combined)

    combined = combined.drop(columns=["_source", "_specificity", "_src_order"], errors="ignore")
    return combined.reset_index(drop=True)
