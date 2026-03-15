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

    # Sort so historical records come last (preferred — they have richer data like locations)
    source_order = {"current": 0, "historical": 1}
    combined["_sort"] = combined["_source"].map(source_order)
    combined = combined.sort_values("_sort", kind="mergesort")

    combined = combined.drop_duplicates(subset=["usajobsControlNumber"], keep="last")
    print(f"After dedup: {len(combined):,} rows")

    # Fill missing locations from the lookup (for jobs that only exist in current API)
    mask = combined["locations"].isna() | (combined["locations"] == "")
    combined.loc[mask, "locations"] = combined.loc[mask, "usajobsControlNumber"].map(loc_lookup)
    filled = mask.sum() - combined["locations"].isna().sum()
    print(f"  Filled {filled:,} missing locations from historical data")

    # Build derived columns
    # Grade: combine payScale + min/max grade
    combined["grade"] = combined.apply(_format_grade, axis=1)

    # Clean dates to YYYY-MM-DD
    combined["openDate"] = combined["positionOpenDate"].apply(_clean_date)
    combined["closeDate"] = combined["positionCloseDate"].apply(_clean_date)

    # Select output columns
    # Map positionOpeningStatus to the output column
    if "positionOpeningStatus" in combined.columns:
        combined["status"] = combined["positionOpeningStatus"]
    else:
        combined["status"] = None

    # Extract occupational series code from JobCategories JSON
    def _extract_series(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        try:
            items = val if isinstance(val, list) else json.loads(val) if isinstance(val, str) else None
            if items and isinstance(items, list) and len(items) > 0:
                return items[0].get("series") or items[0].get("Code") or None
        except:
            pass
        return None

    if "JobCategories" in combined.columns:
        combined["occupationalSeries"] = combined["JobCategories"].apply(_extract_series)
    elif "MatchedObjectDescriptor" in combined.columns:
        def _extract_series_from_mod(val):
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return None
            try:
                obj = val if isinstance(val, dict) else json.loads(val) if isinstance(val, str) else None
                if obj:
                    cats = obj.get("JobCategory", [])
                    if cats and len(cats) > 0:
                        return cats[0].get("Code") or None
            except:
                pass
            return None
        combined["occupationalSeries"] = combined["MatchedObjectDescriptor"].apply(_extract_series_from_mod)
    else:
        combined["occupationalSeries"] = None

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
