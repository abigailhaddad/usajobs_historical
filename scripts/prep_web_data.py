#!/usr/bin/env python3
"""
Prepare a slim 5-year parquet file for the web viewer.

Reads historical and current parquet files, deduplicates (preferring current
API records which have richer data), selects key columns, and writes a
compact parquet for the web frontend.

Usage:
    python scripts/prep_web_data.py
"""

import json
import os
import sys

import pandas as pd
import requests

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
OUT_PATH = os.path.join(os.path.dirname(__file__), "..", "web", "data", "jobs_5yr.parquet")

# Source files to load
HISTORICAL_FILES = [f"historical_jobs_{y}.parquet" for y in range(2018, 2027)]
CURRENT_FILES = [f"current_jobs_{y}.parquet" for y in range(2024, 2027)]


def _extract_all_locations(value):
    """Extract all locations as a single searchable text string.

    Returns something like "Washington, DC; New York, NY; Remote"
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    locations = value
    if isinstance(locations, str):
        locations = locations.strip()
        if not locations:
            return None
        try:
            locations = json.loads(locations)
        except (json.JSONDecodeError, ValueError):
            return None

    if isinstance(locations, list) and len(locations) > 0:
        parts = []
        for loc in locations:
            if isinstance(loc, dict):
                city = loc.get("positionLocationCity") or loc.get("CityName") or ""
                state = loc.get("positionLocationState") or loc.get("StateName") or ""
                if city and state:
                    parts.append(f"{city}, {state}")
                elif city:
                    parts.append(city)
                elif state:
                    parts.append(state)
                else:
                    display = loc.get("positionLocationDisplay") or loc.get("LocationName") or ""
                    if display:
                        parts.append(display)
        if parts:
            return "; ".join(parts)

    return None


def _coalesce_control_number(df):
    """Ensure a unified usajobsControlNumber column exists."""
    if "usajobsControlNumber" not in df.columns and "usajobs_control_number" in df.columns:
        df["usajobsControlNumber"] = df["usajobs_control_number"]
    elif "usajobsControlNumber" in df.columns and "usajobs_control_number" in df.columns:
        df["usajobsControlNumber"] = df["usajobsControlNumber"].fillna(df["usajobs_control_number"])
    if "usajobsControlNumber" in df.columns:
        df["usajobsControlNumber"] = df["usajobsControlNumber"].astype(str)
    return df


def _format_grade(row):
    """Combine payScale, minimumGrade, maximumGrade into a single string like 'GS-7/9'."""
    ps = row.get("payScale") or ""
    min_g = row.get("minimumGrade") or ""
    max_g = row.get("maximumGrade") or ""

    ps = str(ps).strip() if pd.notna(ps) else ""
    min_g = str(min_g).strip() if pd.notna(min_g) else ""
    max_g = str(max_g).strip() if pd.notna(max_g) else ""

    if not ps and not min_g:
        return ""
    if not min_g:
        return ps
    if not ps:
        return f"{min_g}/{max_g}" if max_g and max_g != min_g else min_g

    if max_g and max_g != min_g:
        return f"{ps}-{min_g}/{max_g}"
    return f"{ps}-{min_g}"


def _clean_date(val):
    """Extract just the date part (YYYY-MM-DD) from a datetime string."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s:
        return None
    # Take just the date portion before T or space
    return s[:10] if len(s) >= 10 else s


def _fetch_series_mapping():
    """Fetch occupational series code->name mapping from USAJobs API."""
    url = "https://data.usajobs.gov/api/codelist/occupationalseries"
    print(f"  Fetching occupational series from {url}...")
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        mapping = {}
        if "CodeList" in data and len(data["CodeList"]) > 0:
            for item in data["CodeList"][0].get("ValidValue", []):
                code = item.get("Code")
                name = item.get("Value")
                if code and name and item.get("IsDisabled", "No") == "No":
                    mapping[code] = name.title()
                    # Also store without leading zeros for lookup flexibility
                    stripped = code.lstrip("0")
                    if stripped != code:
                        mapping[stripped] = name.title()
        print(f"  Fetched {len(mapping)} series mappings")
        return mapping
    except Exception as e:
        print(f"  WARNING: Failed to fetch series mapping: {e}")
        return {}


def _resolve_series(code, mapping):
    """Convert a 4-digit code to 'CODE - Name' format."""
    if not code or (isinstance(code, float) and pd.isna(code)):
        return None
    code = str(code).strip()
    if not code:
        return None
    name = mapping.get(code, mapping.get(code.lstrip("0"), ""))
    if name:
        return f"{code} - {name}"
    return code


def load_frames(file_list, source_label):
    """Load parquet files, returning a list of DataFrames."""
    frames = []
    for fname in file_list:
        path = os.path.join(DATA_DIR, fname)
        if os.path.exists(path):
            try:
                df = pd.read_parquet(path)
                df["_source"] = source_label
                frames.append(df)
                print(f"  Loaded {fname}: {len(df):,} rows, {len(df.columns)} cols")
            except Exception as e:
                print(f"  Skipped {fname} (unreadable: {e})")
        else:
            print(f"  Skipped {fname} (not found)")
    return frames


def main():
    print("Loading historical files...")
    hist_frames = load_frames(HISTORICAL_FILES, "historical")

    print("Loading current files...")
    curr_frames = load_frames(CURRENT_FILES, "current")

    all_frames = hist_frames + curr_frames
    if not all_frames:
        print("ERROR: No source files found.")
        sys.exit(1)

    combined = pd.concat(all_frames, ignore_index=True)
    print(f"\nCombined: {len(combined):,} rows before dedup")

    # Coalesce control number
    combined = _coalesce_control_number(combined)

    # Extract locations from ALL sources BEFORE dedup
    # Historical API: PositionLocations column
    # Current API: PositionLocation inside MatchedObjectDescriptor JSON
    if "PositionLocations" in combined.columns:
        combined["locations"] = combined["PositionLocations"].apply(_extract_all_locations)
    else:
        combined["locations"] = None

    # For records missing locations, try MatchedObjectDescriptor (current API)
    if "MatchedObjectDescriptor" in combined.columns:
        mask = combined["locations"].isna() | (combined["locations"] == "")
        def _extract_loc_from_mod(val):
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return None
            try:
                obj = val if isinstance(val, dict) else json.loads(val) if isinstance(val, str) else None
                if not obj:
                    return None
                locs = obj.get("PositionLocation", [])
                if locs:
                    return _extract_all_locations(locs)
                display = obj.get("PositionLocationDisplay", "")
                return display if display else None
            except:
                return None
        combined.loc[mask, "locations"] = combined.loc[mask, "MatchedObjectDescriptor"].apply(_extract_loc_from_mod)

    # Build a lookup of locations from ALL records that have them
    all_locs = combined[
        combined["locations"].notna() & (combined["locations"] != "")
    ][["usajobsControlNumber", "locations"]].drop_duplicates(subset=["usajobsControlNumber"], keep="last")
    loc_lookup = dict(zip(all_locs["usajobsControlNumber"], all_locs["locations"]))
    print(f"  Location lookup: {len(loc_lookup):,} jobs with locations from any source")

    # Extract ALL occupational series codes from JobCategories JSON
    def _extract_all_series(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        try:
            items = val if isinstance(val, list) else json.loads(val) if isinstance(val, str) else None
            if items and isinstance(items, list):
                codes = []
                for item in items:
                    code = item.get("series") or item.get("Code")
                    if code:
                        codes.append(str(code).strip())
                return codes if codes else None
        except:
            pass
        return None

    def _extract_all_series_from_mod(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        try:
            obj = val if isinstance(val, dict) else json.loads(val) if isinstance(val, str) else None
            if obj:
                cats = obj.get("JobCategory", [])
                codes = []
                for cat in cats:
                    code = cat.get("Code")
                    if code:
                        codes.append(str(code).strip())
                return codes if codes else None
        except:
            pass
        return None

    # Build series lookup from ALL records BEFORE dedup (like we do for locations)
    if "JobCategories" in combined.columns:
        combined["_series_codes"] = combined["JobCategories"].apply(_extract_all_series)
    else:
        combined["_series_codes"] = None

    if "MatchedObjectDescriptor" in combined.columns:
        mask = combined["_series_codes"].isna()
        combined.loc[mask, "_series_codes"] = combined.loc[mask, "MatchedObjectDescriptor"].apply(_extract_all_series_from_mod)

    # Build a lookup so we can fill after dedup
    series_rows = combined[combined["_series_codes"].notna()][["usajobsControlNumber", "_series_codes"]].drop_duplicates(
        subset=["usajobsControlNumber"], keep="last"
    )
    series_lookup = dict(zip(series_rows["usajobsControlNumber"], series_rows["_series_codes"]))
    print(f"  Series lookup: {len(series_lookup):,} jobs with occ series from any source")

    # Sort so historical records come last (preferred — they have richer data like locations)
    source_order = {"current": 0, "historical": 1}
    combined["_sort"] = combined["_source"].map(source_order)
    combined = combined.sort_values("_sort", kind="mergesort")

    combined = combined.drop_duplicates(subset=["usajobsControlNumber"], keep="last")
    print(f"After dedup: {len(combined):,} rows")

    # Fill missing locations from the lookup
    mask = combined["locations"].isna() | (combined["locations"] == "")
    combined.loc[mask, "locations"] = combined.loc[mask, "usajobsControlNumber"].map(loc_lookup)
    filled = mask.sum() - combined["locations"].isna().sum()
    print(f"  Filled {filled:,} missing locations from historical data")

    # Fill missing series codes from the lookup
    mask = combined["_series_codes"].isna()
    combined.loc[mask, "_series_codes"] = combined.loc[mask, "usajobsControlNumber"].map(series_lookup)
    filled = mask.sum() - combined["_series_codes"].isna().sum()
    print(f"  Filled {filled:,} missing occ series from cross-source lookup")

    # Build derived columns
    # Vectorized grade formatting (avoids slow apply(axis=1))
    _ps = combined["payScale"].fillna("").astype(str).str.strip() if "payScale" in combined.columns else pd.Series("", index=combined.index)
    _min_g = combined["minimumGrade"].fillna("").astype(str).str.strip() if "minimumGrade" in combined.columns else pd.Series("", index=combined.index)
    _max_g = combined["maximumGrade"].fillna("").astype(str).str.strip() if "maximumGrade" in combined.columns else pd.Series("", index=combined.index)
    _ps = _ps.where(_ps != "nan", "")
    _min_g = _min_g.where(_min_g != "nan", "")
    _max_g = _max_g.where(_max_g != "nan", "")
    _has_ps = _ps != ""
    _has_min = _min_g != ""
    _has_max = (_max_g != "") & (_max_g != _min_g)
    grade = pd.Series("", index=combined.index)
    grade = grade.where(~(_has_ps & ~_has_min), _ps)
    grade = grade.where(~(~_has_ps & _has_min & _has_max), _min_g + "/" + _max_g)
    grade = grade.where(~(~_has_ps & _has_min & ~_has_max), _min_g)
    grade = grade.where(~(_has_ps & _has_min & _has_max), _ps + "-" + _min_g + "/" + _max_g)
    grade = grade.where(~(_has_ps & _has_min & ~_has_max), _ps + "-" + _min_g)
    combined["grade"] = grade

    # Vectorized date cleaning — take first 10 chars (YYYY-MM-DD)
    for _date_col, _out_col in [("positionOpenDate", "openDate"), ("positionCloseDate", "closeDate")]:
        if _date_col in combined.columns:
            _s = combined[_date_col].fillna("").astype(str).str.strip()
            _s = _s.where(_s != "nan", "")
            combined[_out_col] = _s.str[:10]
            combined.loc[combined[_out_col] == "", _out_col] = None
        else:
            combined[_out_col] = None

    if "positionOpeningStatus" in combined.columns:
        combined["status"] = combined["positionOpeningStatus"]
    else:
        combined["status"] = None

    # Fetch mapping from USAJobs API and resolve codes to full names
    series_mapping = _fetch_series_mapping()

    def _resolve_all_series(codes):
        if not codes or not isinstance(codes, list):
            return None
        resolved = []
        for code in codes:
            name = series_mapping.get(code, series_mapping.get(code.lstrip("0"), ""))
            if name:
                resolved.append(f"{code} - {name}")
            else:
                resolved.append(code)
        return "; ".join(resolved) if resolved else None

    combined["occupationalSeries"] = combined["_series_codes"].apply(_resolve_all_series)
    filled_count = combined["occupationalSeries"].notna().sum()
    total_count = len(combined)
    print(f"  Occupational series resolved: {filled_count:,}/{total_count:,} ({100*filled_count/total_count:.1f}%)")

    # Backfill remaining missing occ series from USAJobs API
    missing_mask = combined["occupationalSeries"].isna()
    missing_cns = combined.loc[missing_mask, "usajobsControlNumber"].tolist()
    if missing_cns:
        api_key = os.environ.get("USAJOBS_API_TOKEN") or os.environ.get("USAJOBS_TOKEN")
        if api_key:
            import concurrent.futures
            print(f"  Backfilling {len(missing_cns):,} missing occ series from USAJobs API...")
            headers = {"Host": "data.usajobs.gov", "Authorization-Key": api_key}

            def _fetch_series_for_cn(cn):
                try:
                    resp = requests.get(
                        f"https://data.usajobs.gov/api/Search?ControlNumber={cn}",
                        headers=headers, timeout=10,
                    )
                    items = resp.json().get("SearchResult", {}).get("SearchResultItems", [])
                    if items:
                        cats = items[0].get("MatchedObjectDescriptor", {}).get("JobCategory", [])
                        codes = [c.get("Code") for c in cats if c.get("Code")]
                        if codes:
                            return cn, codes
                except:
                    pass
                return cn, None

            resolved_map = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
                results = pool.map(_fetch_series_for_cn, missing_cns)
                for cn, codes in results:
                    if codes:
                        resolved = _resolve_all_series(codes)
                        if resolved:
                            resolved_map[cn] = resolved
            # Vectorized update instead of row-by-row .loc[]
            if resolved_map:
                mask = combined["usajobsControlNumber"].isin(resolved_map)
                combined.loc[mask, "occupationalSeries"] = combined.loc[mask, "usajobsControlNumber"].map(resolved_map)
            print(f"  Backfilled {len(resolved_map):,} occ series from API")
        else:
            print(f"  Skipping API backfill ({len(missing_cns):,} missing) — set USAJOBS_API_TOKEN to enable")

    OUTPUT_COLUMNS = [
        "usajobsControlNumber",
        "positionTitle",
        "hiringDepartmentName",
        "hiringAgencyName",
        "grade",
        "minimumSalary",
        "maximumSalary",
        "openDate",
        "closeDate",
        "appointmentType",
        "serviceType",
        "locations",
        "status",
        "occupationalSeries",
    ]

    for col in OUTPUT_COLUMNS:
        if col not in combined.columns:
            combined[col] = None

    out = combined[OUTPUT_COLUMNS].copy()

    # Write output
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    out.to_parquet(OUT_PATH, index=False, compression="snappy")

    # --- Stats ---
    file_size = os.path.getsize(OUT_PATH)
    print(f"\nOutput: {OUT_PATH}")
    print(f"  Rows:    {len(out):,}")
    print(f"  Columns: {list(out.columns)}")
    print(f"  Size:    {file_size / 1_048_576:.1f} MB")

    # Rows per year
    if "openDate" in out.columns:
        dates = pd.to_datetime(out["openDate"], errors="coerce")
        year_counts = dates.dt.year.value_counts().sort_index()
        print("\n  Rows per year:")
        for year, count in year_counts.items():
            if pd.notna(year):
                print(f"    {int(year)}: {count:,}")


if __name__ == "__main__":
    main()
