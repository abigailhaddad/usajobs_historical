#!/usr/bin/env python3
"""
Aggregate OPM EHRI accessions or separations into web/{type}-monthly.json
for the workforce-flow chart.

Output schema:
    {
      "generated_at": "2026-05-12T...",
      "months": ["201801", ...],
      "agencies": {"<agency>": [count per month, ...]},
      "categories": {"<agency>": {"<category>": <total count>}},
      "categories_monthly": {"<agency>": {"<category>": [count per month]}},
      # separations only:
      "categories_drp_monthly": {"<agency>": {"<category>": {"Y"|"N": [...]}}}
    }

Usage:
    python scripts/aggregate_accessions.py
    python scripts/aggregate_accessions.py --type separations
    python scripts/aggregate_accessions.py --since 202001 --output /tmp/agg.json
"""
import argparse
import json
import os
import sys
from datetime import datetime

from ehri_utils import (
    HF_REPO,
    discover_urls,
    load_to_duckdb,
    has_column,
    agency_totals_by_month,
    breakdown_total,
    breakdown_by_month,
    drp_breakdown_by_month,
)


def aggregate(data_type, since_yyyymm):
    category_col = f"{data_type[:-1]}_category"  # accession_category or separation_category

    print(f"Discovering {data_type} files since {since_yyyymm}…")
    pairs = discover_urls(data_type, since_yyyymm)
    if not pairs:
        sys.exit(f"No {data_type} files found on HF since {since_yyyymm}")
    print(f"  {len(pairs)} months found ({pairs[0][0]} → {pairs[-1][0]})")

    months = [m for m, _ in pairs]
    urls = [u for _, u in pairs]

    print("Loading parquet files into DuckDB…")
    db = load_to_duckdb(urls)

    print("Aggregating monthly totals by agency…")
    agencies = agency_totals_by_month(db, months)

    print(f"Computing {category_col} breakdown by agency…")
    categories = breakdown_total(db, category_col)

    print(f"Computing monthly {category_col} breakdown by agency…")
    categories_monthly = breakdown_by_month(db, category_col, months)

    out = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source": f"https://huggingface.co/datasets/{HF_REPO}",
        "since": since_yyyymm,
        "months": months,
        "agencies": dict(sorted(agencies.items())),
        "categories": categories,
        "categories_monthly": categories_monthly,
    }

    # Separations carries a DRP indicator; emit a category×DRP cross-product
    # so the front-end can filter on DRP status as well.
    if has_column(db, "drp_indicator"):
        print(f"Computing monthly {category_col}×DRP breakdown by agency…")
        out["categories_drp_monthly"] = drp_breakdown_by_month(db, category_col, months)

    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--type", "-t", default="accessions",
        choices=["accessions", "separations"],
        help="Which EHRI dataset to aggregate (default: accessions)",
    )
    parser.add_argument(
        "--output", "-o", default=None,
        help="Output JSON path (default: web/<type>-monthly.json)",
    )
    parser.add_argument(
        "--since", default="201801",
        help="Earliest YYYYMM to include (default: 201801)",
    )
    args = parser.parse_args()

    output = args.output or os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "web", f"{args.type}-monthly.json",
    )

    data = aggregate(args.type, args.since)
    with open(output, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    size_kb = os.path.getsize(output) / 1024
    print(f"Saved: {output}  ({size_kb:.1f} KB)")
    print(f"  {len(data['months'])} months × {len(data['agencies'])} agencies")


if __name__ == "__main__":
    main()
