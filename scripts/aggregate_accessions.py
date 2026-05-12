#!/usr/bin/env python3
"""
Aggregate OPM EHRI accessions data from HuggingFace into a small JSON file
suitable for client-side rendering.

Output: web/accessions-monthly.json
    {
      "generated_at": "2026-05-12T...",
      "months": ["201801", "201802", ...],
      "agencies": {
        "Department of Veterans Affairs": [count, count, ...],
        ...
      },
      "categories": {              # accession_category breakdown for the
        "<agency>": {              # whole window, used for context only
          "<category>": <count>
        }
      }
    }

Source: https://huggingface.co/datasets/impactproject/opm-ehri-data
The dataset is public — no auth needed.

Usage:
    python scripts/aggregate_accessions.py
    python scripts/aggregate_accessions.py --since 202001 --output /tmp/agg.json
"""
import argparse
import json
import os
import re
import sys
from datetime import datetime

os.environ["HF_HUB_DISABLE_IMPLICIT_TOKEN"] = "1"

try:
    import duckdb
    from huggingface_hub import list_repo_files
except ImportError:
    sys.stderr.write("Need duckdb + huggingface_hub. Install with:\n")
    sys.stderr.write("  pip install duckdb huggingface_hub\n")
    sys.exit(1)


HF_REPO = "impactproject/opm-ehri-data"
HF_BASE = f"https://huggingface.co/datasets/{HF_REPO}/resolve/main"

# OPM uses uppercase + slightly different naming. Map back to the labels
# used in our USAJobs dataset (and in the postings poster) so the two
# datasets line up panel-by-panel.
AGENCY_NORMALIZE = {
    "DEPARTMENT OF VETERANS AFFAIRS": "Department of Veterans Affairs",
    "DEPARTMENT OF HOMELAND SECURITY": "Department of Homeland Security",
    "DEPARTMENT OF DEFENSE": "Department of Defense",
    "DEPARTMENT OF THE NAVY": "Department of the Navy",
    "DEPARTMENT OF THE ARMY": "Department of the Army",
    "DEPARTMENT OF THE AIR FORCE": "Department of the Air Force",
    "DEPARTMENT OF JUSTICE": "Department of Justice",
    "DEPARTMENT OF INTERIOR": "Department of the Interior",
    "DEPARTMENT OF TREASURY": "Department of the Treasury",
    "DEPARTMENT OF THE TREASURY": "Department of the Treasury",
    "DEPARTMENT OF AGRICULTURE": "Department of Agriculture",
    "DEPARTMENT OF HEALTH AND HUMAN SERVICES":
        "Department of Health and Human Services",
    "DEPARTMENT OF COMMERCE": "Department of Commerce",
    "DEPARTMENT OF TRANSPORTATION": "Department of Transportation",
    "DEPARTMENT OF ENERGY": "Department of Energy",
    "DEPARTMENT OF STATE": "Department of State",
    "DEPARTMENT OF LABOR": "Department of Labor",
    "DEPARTMENT OF EDUCATION": "Department of Education",
    # EHRI truncates names to 40 chars, so HUD shows up as "...DEVELOPM"
    "DEPARTMENT OF HOUSING AND URBAN DEVELOPMENT":
        "Department of Housing and Urban Development",
    "DEPARTMENT OF HOUSING AND URBAN DEVELOPM":
        "Department of Housing and Urban Development",
    "GENERAL SERVICES ADMINISTRATION": "General Services Administration",
    # No EHRI equivalent for "Other Agencies and Independent Organizations"
    # (which is a USAJobs grouping); EHRI lists independent agencies separately.
    # Legislative / Judicial Branch are not in EHRI (executive-branch only).
}


def discover_accession_urls(since_yyyymm):
    """Return [(yyyymm, url)] for the highest-versioned accessions parquet
    in each month >= since_yyyymm. Mirrors the demo notebook's behavior."""
    files = list_repo_files(HF_REPO, repo_type="dataset")
    versioned = re.compile(r"^accessions/accessions_(\d{6})_v(\d+)\.parquet$")
    legacy = re.compile(r"^accessions/accessions_(\d{6})\.parquet$")

    best = {}
    for f in files:
        m = versioned.match(f)
        if m:
            month, ver = m.group(1), int(m.group(2))
            if month < since_yyyymm:
                continue
            if month not in best or ver > best[month][0]:
                best[month] = (ver, f)
            continue
        m = legacy.match(f)
        if m:
            month = m.group(1)
            if month < since_yyyymm:
                continue
            if month not in best:
                best[month] = (0, f)

    return sorted((m, f"{HF_BASE}/{best[m][1]}") for m in best)


def aggregate(since_yyyymm):
    print(f"Discovering accessions files since {since_yyyymm}…")
    pairs = discover_accession_urls(since_yyyymm)
    print(f"  {len(pairs)} months found ({pairs[0][0]} → {pairs[-1][0]})")

    months = [m for m, _ in pairs]
    urls = [u for _, u in pairs]

    db = duckdb.connect()
    file_list = ", ".join(f"'{u}'" for u in urls)
    print("Loading parquet files into DuckDB…")
    db.execute(
        f"CREATE TABLE accessions AS "
        f"SELECT * FROM read_parquet([{file_list}], union_by_name=True)"
    )

    print("Aggregating monthly totals by agency…")
    rows = db.execute("""
        SELECT
            agency,
            personnel_action_effective_date_yyyymm AS month,
            SUM(CAST(count AS INTEGER)) AS total
        FROM accessions
        WHERE agency IS NOT NULL
        GROUP BY agency, month
    """).fetchall()

    print("Computing accession_category breakdown by agency…")
    cat_rows = db.execute("""
        SELECT
            agency,
            COALESCE(accession_category, '(unknown)') AS category,
            SUM(CAST(count AS INTEGER)) AS total
        FROM accessions
        WHERE agency IS NOT NULL
        GROUP BY agency, category
    """).fetchall()

    # Pivot to {agency: [count for each month]} keyed on the months list
    by_agency = {}
    month_index = {m: i for i, m in enumerate(months)}
    for agency_raw, month, total in rows:
        # Some rows have a month outside our requested window when the file
        # itself is windowed. Skip those defensively.
        if month not in month_index:
            continue
        normalized = AGENCY_NORMALIZE.get(agency_raw, agency_raw)
        if normalized not in by_agency:
            by_agency[normalized] = [0] * len(months)
        by_agency[normalized][month_index[month]] += int(total or 0)

    categories = {}
    for agency_raw, category, total in cat_rows:
        normalized = AGENCY_NORMALIZE.get(agency_raw, agency_raw)
        categories.setdefault(normalized, {})
        categories[normalized][category] = (
            categories[normalized].get(category, 0) + int(total or 0)
        )

    return {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source": f"https://huggingface.co/datasets/{HF_REPO}",
        "since": since_yyyymm,
        "months": months,
        "agencies": dict(sorted(by_agency.items())),
        "categories": categories,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    default_out = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "web", "accessions-monthly.json",
    )
    parser.add_argument(
        "--output", "-o", default=default_out,
        help=f"Output JSON path (default: {default_out})",
    )
    parser.add_argument(
        "--since", default="201801",
        help="Earliest YYYYMM to include (default: 201801)",
    )
    args = parser.parse_args()

    data = aggregate(args.since)
    with open(args.output, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    size_kb = os.path.getsize(args.output) / 1024
    print(f"Saved: {args.output}  ({size_kb:.1f} KB)")
    print(f"  {len(data['months'])} months × {len(data['agencies'])} agencies")


if __name__ == "__main__":
    main()
