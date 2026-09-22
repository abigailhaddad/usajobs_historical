#!/usr/bin/env python3
"""
Backfill fields that flatten_current_job() dropped when writing current_jobs_*.parquet.

collect_current_data.py has always stashed the full raw API response in the
MatchedObjectDescriptor column, even though it only promoted a subset of fields
to named columns. Two of the dropped fields matter:

  - OrganizationCodes (UserArea.Details, "DEPTCODE/AGENCYCODE") -- flatten_current_job
    read it from the wrong dict (top-level `job` instead of `user_area`), so
    hiringAgencyCode was always null.
  - AnnouncementClosingTypeOption (UserArea.Details) -- the exact application-count
    cap for "Applicant Cut-Off" (code 03) postings. Never extracted at all.

Both are recoverable from data already on disk/R2 by re-parsing the raw JSON
blob -- no new API calls needed. This only helps current_jobs_*.parquet
(2024-07 onward); the historicjoa API behind historical_jobs_*.parquet has no
equivalent field for either one.

Usage:
    python scripts/backfill_current_jobs_fields.py data/current_jobs_2026.parquet
    python scripts/backfill_current_jobs_fields.py data/current_jobs_2026.parquet --output /tmp/out.parquet
"""
import argparse
import json
import os
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(__file__))
from collect_current_data import _COMPRESSION, _COMPRESSION_LEVEL, _ROW_BATCH  # noqa: E402

_CLOSING_TYPE_MAP = {"01": "Closing Date", "02": "Open Continuous", "03": "Applicant Cut-Off"}


def _int_or_none(value):
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def extract_fields(raw_mod: str) -> dict:
    """Re-derive the fields flatten_current_job() should have set, from the raw blob."""
    empty = {
        "hiringDepartmentCode": None,
        "hiringAgencyCode": None,
        "announcementClosingTypeCode": None,
        "announcementClosingTypeDescription": None,
        "applicationCap": None,
    }
    if not raw_mod:
        return empty
    try:
        mod = json.loads(raw_mod)
    except (TypeError, json.JSONDecodeError):
        return empty

    details = (mod.get("UserArea") or {}).get("Details") or {}

    org_codes = (details.get("OrganizationCodes") or "").split("/")
    dept_code = org_codes[0] if org_codes and org_codes[0] else None
    agency_code = org_codes[1] if len(org_codes) > 1 and org_codes[1] else None

    closing_code = details.get("AnnouncementClosingType")
    closing_desc = _CLOSING_TYPE_MAP.get(str(closing_code)) if closing_code else None
    # For closing types other than "03", Option just echoes the type code itself
    # (e.g. "01"), not a cap -- only trust it for actual Applicant Cut-Off postings.
    cap = _int_or_none(details.get("AnnouncementClosingTypeOption")) if closing_code == "03" else None

    return {
        "hiringDepartmentCode": dept_code,
        "hiringAgencyCode": agency_code,
        "announcementClosingTypeCode": closing_code,
        "announcementClosingTypeDescription": closing_desc,
        "applicationCap": cap,
    }


def backfill(input_path: str, output_path: str):
    df = pd.read_parquet(input_path)
    if "MatchedObjectDescriptor" not in df.columns:
        raise ValueError(f"{input_path} has no MatchedObjectDescriptor column -- nothing to backfill from")

    print(f"Backfilling {len(df):,} rows from {input_path}")
    extracted = pd.DataFrame(
        [extract_fields(raw) for raw in df["MatchedObjectDescriptor"]],
        index=df.index,
    )
    for col in extracted.columns:
        df[col] = extracted[col]

    n_cap = df["applicationCap"].notna().sum()
    n_cutoff = (df["announcementClosingTypeCode"] == "03").sum()
    n_agency = df["hiringAgencyCode"].notna().sum()
    print(f"  hiringAgencyCode populated: {n_agency:,} / {len(df):,}")
    print(f"  Applicant Cut-Off postings: {n_cutoff:,}")
    print(f"  applicationCap recovered:   {n_cap:,} ({100 * n_cap / n_cutoff:.1f}% of cut-off postings)"
          if n_cutoff else "  applicationCap recovered:   0")

    # MatchedObjectDescriptor is ~99% of these files and compresses far better
    # under zstd than the pandas-default snappy (see collect_current_data.py's
    # own writer) -- a plain df.to_parquet() here bloats a 400MB file to 1.5GB.
    # Match that writer's compression + row-group batching exactly.
    table = pa.Table.from_pandas(df, preserve_index=False)
    tmp_path = output_path + ".tmp"
    try:
        with pq.ParquetWriter(tmp_path, table.schema, compression=_COMPRESSION,
                               compression_level=_COMPRESSION_LEVEL) as writer:
            for batch in table.to_batches(max_chunksize=_ROW_BATCH):
                writer.write_table(pa.Table.from_batches([batch], schema=table.schema))
        os.replace(tmp_path, output_path)
    except BaseException:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise
    print(f"  Wrote {output_path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input", help="Path to an existing current_jobs_*.parquet")
    parser.add_argument("--output", help="Output path (default: overwrite input)")
    args = parser.parse_args()
    backfill(args.input, args.output or args.input)


if __name__ == "__main__":
    main()
