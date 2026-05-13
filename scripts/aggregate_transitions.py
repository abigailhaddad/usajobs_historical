#!/usr/bin/env python3
"""
Aggregate OPM EHRI accessions data from January 2005 through the present
for the administration-transitions poster.

The poster shows one long monthly time series with vertical lines marking
each presidential transition (Jan 2009, 2017, 2021, 2025), so we can see
how accessions actually responded to each new administration.

Output: web/transitions-data.json
    {
      "generated_at": "...",
      "months": ["200501", ..., "202603"],
      "transitions": [
        {"label": "Obama (Jan 2009)",   "month": "200901", "freeze": null,   "blurb": "..."},
        {"label": "Trump 1 (Jan 2017)", "month": "201701", "freeze": "Jan 23, 2017", "blurb": "..."},
        {"label": "Biden (Jan 2021)",   "month": "202101", "freeze": null,   "blurb": "..."},
        {"label": "Trump 2 (Jan 2025)", "month": "202501", "freeze": "Jan 20, 2025", "blurb": "..."}
      ],
      "total":   [count, count, ...],         # one per month
      "by_dept": {"<dept>": [count, ...]}     # one per month, sparse depts allowed
    }
"""
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
    sys.stderr.write("Need duckdb + huggingface_hub\n")
    sys.exit(1)


HF_REPO = "impactproject/opm-ehri-data"
HF_BASE = f"https://huggingface.co/datasets/{HF_REPO}/resolve/main"
SINCE = "200501"

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
    "DEPARTMENT OF HOUSING AND URBAN DEVELOPMENT":
        "Department of Housing and Urban Development",
    "DEPARTMENT OF HOUSING AND URBAN DEVELOPM":
        "Department of Housing and Urban Development",
    "GENERAL SERVICES ADMINISTRATION": "General Services Administration",
}

TRANSITIONS = [
    {
        "label": "Obama (Jan 2009)",
        "month": "200901",
        "freeze": None,
        "blurb": "No formal hiring freeze. Accessions stayed within the normal seasonal range.",
    },
    {
        "label": "Trump 1 (Jan 2017)",
        "month": "201701",
        "freeze": "Jan 23, 2017",
        "blurb": "Hiring freeze by executive order Jan 23, 2017 (lifted April 2017). Spring 2017 accessions dropped ~27% vs the same months in 2016.",
    },
    {
        "label": "Biden (Jan 2021)",
        "month": "202101",
        "freeze": None,
        "blurb": "No formal hiring freeze. Biden rescinded Trump's lingering appointee freeze on day one; accessions ran roughly in line with 2020.",
    },
    {
        "label": "Trump 2 (Jan 2025)",
        "month": "202501",
        "freeze": "Jan 20, 2025",
        "blurb": "Hiring freeze by executive order Jan 20, 2025 (extended multiple times). Spring 2025 accessions fell ~65% vs 2024 — the steepest drop in the 21-year EHRI series.",
    },
]


def discover_urls(since_yyyymm):
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


def main():
    print(f"Discovering accessions files since {SINCE}…")
    pairs = discover_urls(SINCE)
    print(f"  {len(pairs)} months ({pairs[0][0]} → {pairs[-1][0]})")

    months = [m for m, _ in pairs]
    urls = [u for _, u in pairs]
    month_idx = {m: i for i, m in enumerate(months)}

    db = duckdb.connect()
    file_list = ", ".join(f"'{u}'" for u in urls)
    print("Loading parquet files via DuckDB (streaming from HF)…")
    db.execute(
        f"CREATE TABLE data AS "
        f"SELECT * FROM read_parquet([{file_list}], union_by_name=True)"
    )

    print("Aggregating monthly totals across all agencies…")
    total_rows = db.execute("""
        SELECT
            personnel_action_effective_date_yyyymm AS month,
            SUM(CAST(count AS INTEGER)) AS total
        FROM data
        GROUP BY month
    """).fetchall()

    total = [0] * len(months)
    for month, t in total_rows:
        if month in month_idx:
            total[month_idx[month]] = int(t or 0)

    print("Aggregating monthly totals by agency…")
    rows = db.execute("""
        SELECT
            agency,
            personnel_action_effective_date_yyyymm AS month,
            SUM(CAST(count AS INTEGER)) AS total
        FROM data
        WHERE agency IS NOT NULL
        GROUP BY agency, month
    """).fetchall()

    by_dept = {}
    for agency_raw, month, t in rows:
        normalized = AGENCY_NORMALIZE.get(agency_raw)
        if normalized is None or month not in month_idx:
            continue
        if normalized not in by_dept:
            by_dept[normalized] = [0] * len(months)
        by_dept[normalized][month_idx[month]] += int(t or 0)

    out = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source": f"https://huggingface.co/datasets/{HF_REPO}",
        "months": months,
        "transitions": TRANSITIONS,
        "total": total,
        "by_dept": dict(sorted(by_dept.items())),
    }

    output = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "web", "transitions-data.json",
    )
    with open(output, "w") as f:
        json.dump(out, f, separators=(",", ":"))
    kb = os.path.getsize(output) / 1024
    print(f"Saved: {output}  ({kb:.1f} KB)")
    print(f"  {len(months)} months × {len(by_dept)} depts")


if __name__ == "__main__":
    main()
