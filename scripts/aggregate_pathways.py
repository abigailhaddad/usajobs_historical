#!/usr/bin/env python3
"""
Aggregate OPM EHRI accessions data filtered to Pathways Program hires
(pathways_group IS NOT NULL) into web/pathways-monthly.json for the
workforce-pathways chart.

The Pathways Programs are a small but distinct early-career federal
hiring pipeline:
  - INTERN
  - STUDENT TRAINEE
  - RECENT GRAD

Output schema (mirrors aggregate_accessions.py, minus DRP):
    {
      "generated_at": "...",
      "months": [...],
      "agencies": {"<agency>": [Pathways count per month]},
      "categories": {"<agency>": {"<pathways_group>": <total>}},
      "categories_monthly": {"<agency>": {"<pathways_group>": [count per month]}}
    }

Usage:
    python scripts/aggregate_pathways.py
    python scripts/aggregate_pathways.py --since 202001 --output /tmp/p.json
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
    agency_totals_by_month,
    breakdown_total,
    breakdown_by_month,
)


PATHWAYS_WHERE = "pathways_group IS NOT NULL"


def aggregate(since_yyyymm):
    print(f"Discovering accessions files since {since_yyyymm}…")
    pairs = discover_urls("accessions", since_yyyymm)
    if not pairs:
        sys.exit(f"No accessions files found on HF since {since_yyyymm}")
    print(f"  {len(pairs)} months found ({pairs[0][0]} → {pairs[-1][0]})")

    months = [m for m, _ in pairs]
    urls = [u for _, u in pairs]

    print("Loading parquet files into DuckDB…")
    db = load_to_duckdb(urls)

    print("Aggregating Pathways monthly totals by agency…")
    agencies = agency_totals_by_month(db, months, where=PATHWAYS_WHERE)

    print("Computing pathways_group breakdown by agency…")
    categories = breakdown_total(db, "pathways_group", where=PATHWAYS_WHERE)

    print("Computing monthly pathways_group breakdown by agency…")
    categories_monthly = breakdown_by_month(db, "pathways_group", months, where=PATHWAYS_WHERE)

    return {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source": f"https://huggingface.co/datasets/{HF_REPO}",
        "since": since_yyyymm,
        "months": months,
        "agencies": dict(sorted(agencies.items())),
        "categories": categories,
        "categories_monthly": categories_monthly,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--output", "-o", default=None,
        help="Output JSON path (default: web/pathways-monthly.json)",
    )
    parser.add_argument(
        "--since", default="201801",
        help="Earliest YYYYMM to include (default: 201801)",
    )
    args = parser.parse_args()

    output = args.output or os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "web", "pathways-monthly.json",
    )

    data = aggregate(args.since)
    with open(output, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    size_kb = os.path.getsize(output) / 1024
    print(f"Saved: {output}  ({size_kb:.1f} KB)")
    print(f"  {len(data['months'])} months × {len(data['agencies'])} agencies")


if __name__ == "__main__":
    main()
