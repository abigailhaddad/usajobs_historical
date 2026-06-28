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


def build_deduped_query(
    hist_urls: List[str],
    curr_urls: List[str],
    select: str = "*",
    where: str = "",
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

    return f"""
WITH combined AS (
    SELECT *, 0 AS _src_priority
    FROM read_parquet([{hist_list}])
    UNION ALL BY NAME
    SELECT *, 1 AS _src_priority
    FROM read_parquet([{curr_list}], union_by_name=true)
),
deduped AS (
    SELECT *
    FROM combined
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
