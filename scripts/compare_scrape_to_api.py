#!/usr/bin/env python3
"""
Check the scraped collection against the API collection.

The scrape (collect_scraped_data.py) runs in shadow: nothing downstream reads
it. This is what tells us whether it could. It answers two questions:

  Coverage  -- does scraping usajobs.gov find the same postings the API does?
  Fidelity  -- for postings both found, do the parsed fields agree?

Comparison window. The scrape only has data from the day it started, so
comparing all of a year would score every earlier posting as "API only" and
mean nothing. The window therefore starts at the scrape's own first run
(min inserted_at) and both sides are restricted to postings opened on or after
it. --since overrides.

Divergence past the thresholds below writes logs/SCRAPE_API_DIVERGENCE.txt,
which the daily workflow turns into a GitHub issue.

    python compare_scrape_to_api.py --data-dir data/
    python compare_scrape_to_api.py --data-dir data/ --since 2026-09-01
"""

import argparse
import os
import sys
from datetime import datetime
import pandas as pd

# Fields both collections carry, with the same meaning on each side. Anything
# derived only from the API (agency/department codes, vendor, agencyLevel) is
# deliberately absent: the announcement page does not publish it.
COMPARED_FIELDS = [
    "positionTitle",
    "announcementNumber",
    "positionOpenDate",
    "positionCloseDate",
    "payScale",
    "minimumGrade",
    "maximumGrade",
    "minimumSalary",
    "maximumSalary",
    "workSchedule",
    "appointmentType",
    "serviceType",
    "supervisoryStatus",
    "securityClearance",
    "teleworkEligible",
    "drugTestRequired",
    "relocationExpensesReimbursed",
    "promotionPotential",
    "totalOpenings",
    "JobCategories",
]

# The announcement body, section by section. These have no API counterpart to
# check against -- MatchedObjectDescriptor does not carry them -- so the only
# available signal is whether they are still being populated at all. A markup
# change on usajobs.gov shows up here as a fill rate falling off a cliff.
TEXT_FIELDS = [
    "jobSummary",
    "majorDuties",
    "requirements",
    "conditionsOfEmployment",
    "qualificationSummary",
    "education",
    "additionalInformation",
    "benefits",
    "howYouWillBeEvaluated",
    "requiredDocuments",
    "howToApply",
    "text",
]

# 'education' is genuinely absent from about one announcement in six, so the
# floor has to sit below that; everything else measured at 100% on a 50-page
# live sample (2026-09-02).
MIN_TEXT_FILL = {"education": 0.50}
MIN_TEXT_FILL_DEFAULT = 0.90

# Below these, say so loudly.
MIN_COVERAGE = 0.95      # share of the API's window postings the scrape found
MIN_AGREEMENT = 0.90     # share of comparable rows a field must match on

DIVERGENCE_FILE = os.path.join(os.path.dirname(__file__), "..", "logs",
                               "SCRAPE_API_DIVERGENCE.txt")


def parse_args():
    p = argparse.ArgumentParser(description="Diff scraped vs API job collections")
    p.add_argument("--data-dir", default="data")
    p.add_argument("--since", help="Compare postings opened on/after this date "
                                   "(YYYY-MM-DD). Default: the scrape's first run.")
    p.add_argument("--report", help="Write the report here "
                                    "(default: logs/scrape_vs_api_<date>.md)")
    p.add_argument("--quiet-on-missing", action="store_true",
                   help="Exit 0 when there is no scraped data yet")
    return p.parse_args()


def load(data_dir: str, prefix: str, columns: list) -> pd.DataFrame:
    """Every year file for one collection, keeping only the columns we compare.

    Columns are read selectively because current_jobs_*.parquet is gigabytes of
    announcement JSON and we need about twenty short fields out of it.
    """
    import pyarrow.parquet as pq

    frames = []
    for name in sorted(os.listdir(data_dir)):
        if not (name.startswith(prefix) and name.endswith(".parquet")):
            continue
        path = os.path.join(data_dir, name)
        available = set(pq.read_schema(path).names)
        wanted = [c for c in columns if c in available]
        if "usajobs_control_number" not in wanted:
            print(f"   skipping {name}: no usajobs_control_number column")
            continue
        frames.append(pd.read_parquet(path, columns=wanted))
    if not frames:
        return pd.DataFrame(columns=columns)
    return pd.concat(frames, ignore_index=True)


def normalize(series: pd.Series) -> pd.Series:
    """Put both sides in the same shape before comparing.

    Salary is a float on one side and a float-shaped string on the other; dates
    carry a time component from the API and none from the page; Y/N and Yes/No
    are the same answer. None of that is a real disagreement.
    """
    s = series.astype("string").str.strip()
    s = s.replace({"": pd.NA, "None": pd.NA, "nan": pd.NA, "<NA>": pd.NA})

    numeric = pd.to_numeric(s, errors="coerce")
    as_number = numeric.round(2).astype("string")

    lowered = s.str.lower()
    yes_no = lowered.map({"y": "Y", "yes": "Y", "true": "Y",
                          "n": "N", "no": "N", "false": "N"})

    # Dates: '2024-08-26T13:33:11.4500' and '2024-08-26' are the same day.
    dates = s.str.extract(r"^(\d{4}-\d{2}-\d{2})", expand=False)

    return (yes_no.fillna(as_number).fillna(dates)
            .fillna(lowered.str.replace(r"\s+", " ", regex=True)))


def compare_series(scraped: pd.Series, api: pd.Series) -> dict:
    """Agreement on the rows where both sides have a value."""
    a, b = normalize(scraped), normalize(api)
    comparable = a.notna() & b.notna()
    n = int(comparable.sum())
    if not n:
        return {"comparable": 0, "agree": 0, "rate": None,
                "scrape_null": int(a.isna().sum()), "api_null": int(b.isna().sum())}
    agree = int((a[comparable] == b[comparable]).sum())
    return {"comparable": n, "agree": agree, "rate": agree / n,
            "scrape_null": int(a.isna().sum()), "api_null": int(b.isna().sum())}


def text_field_health(data_dir: str) -> list:
    """Fill rate and median length for each long-text field.

    Scans the scraped parquets a row group at a time. These columns hold tens
    of kilobytes per row, so reading a whole year of one into memory to
    measure it would defeat the point.
    """
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    stats = {f: {"rows": 0, "filled": 0, "lengths": []} for f in TEXT_FIELDS}

    for name in sorted(os.listdir(data_dir)):
        if not (name.startswith("scraped_jobs_") and name.endswith(".parquet")):
            continue
        path = os.path.join(data_dir, name)
        present = set(pq.read_schema(path).names)
        for field in TEXT_FIELDS:
            if field not in present:
                stats[field]["rows"] += pq.read_metadata(path).num_rows
                continue
            for batch in pq.ParquetFile(path).iter_batches(
                    batch_size=2000, columns=[field]):
                column = batch.column(0)
                lengths = pc.utf8_length(column)
                stats[field]["rows"] += len(column)
                filled = pc.greater(lengths, 0)
                stats[field]["filled"] += int(
                    pc.sum(pc.fill_null(filled, False)).as_py() or 0)
                stats[field]["lengths"] += [
                    v for v in lengths.to_pylist() if v]

    rows = []
    for field in TEXT_FIELDS:
        st = stats[field]
        lengths = sorted(st["lengths"])
        rows.append({
            "field": field,
            "rows": st["rows"],
            "filled": st["filled"],
            "rate": st["filled"] / st["rows"] if st["rows"] else None,
            "median": lengths[len(lengths) // 2] if lengths else 0,
        })
    return rows


def flag(message: str) -> None:
    os.makedirs(os.path.dirname(DIVERGENCE_FILE), exist_ok=True)
    with open(DIVERGENCE_FILE, "a") as f:
        f.write(f"{datetime.now().isoformat()} | {message}\n")
    print(f"DIVERGENCE: {message}")


def main() -> int:
    args = parse_args()
    columns = ["usajobs_control_number", "inserted_at"] + COMPARED_FIELDS

    scraped = load(args.data_dir, "scraped_jobs_", columns)
    if scraped.empty:
        message = "No scraped data yet — nothing to compare."
        print(message)
        return 0 if args.quiet_on_missing else 0

    api = load(args.data_dir, "current_jobs_", columns)
    if api.empty:
        print("No API current-jobs data found — cannot compare.")
        return 1

    for df in (scraped, api):
        df.drop_duplicates("usajobs_control_number", keep="last", inplace=True)

    since = args.since
    if not since and "inserted_at" in scraped.columns:
        first = scraped["inserted_at"].dropna().astype(str).min()
        since = first[:10] if first else None
    if not since:
        since = datetime.now().strftime("%Y-%m-%d")

    opened = lambda df: df["positionOpenDate"].astype(str).str[:10]
    scraped_w = scraped[opened(scraped) >= since].copy()
    api_w = api[opened(api) >= since].copy()

    s_ids = set(scraped_w["usajobs_control_number"].dropna())
    a_ids = set(api_w["usajobs_control_number"].dropna())
    both = s_ids & a_ids

    coverage = len(both) / len(a_ids) if a_ids else None

    lines = [
        f"# Scraped vs API job collections",
        "",
        f"Generated {datetime.now().isoformat(timespec='seconds')}  ",
        f"Window: postings opened on or after **{since}**",
        "",
        "## Coverage",
        "",
        f"| | count |",
        f"|---|---:|",
        f"| Found by both | {len(both):,} |",
        f"| API only (scrape missed) | {len(a_ids - s_ids):,} |",
        f"| Scrape only (API missed) | {len(s_ids - a_ids):,} |",
        f"| API total in window | {len(a_ids):,} |",
        f"| Scrape total in window | {len(s_ids):,} |",
        "",
        f"Scrape found **{coverage:.1%}** of the API's postings."
        if coverage is not None else "No API postings in the window.",
        "",
    ]

    if coverage is not None and coverage < MIN_COVERAGE:
        flag(f"Scrape covered only {coverage:.1%} of the API's postings opened "
             f"since {since} ({len(both):,} of {len(a_ids):,}); "
             f"floor is {MIN_COVERAGE:.0%}.")

    # Fidelity, on the postings both sides found. A field absent from one side
    # is added as all-null first: without it pandas leaves that column
    # unsuffixed in the merge and the field looks missing from both.
    absent_from = {}
    for field in COMPARED_FIELDS:
        for label, df in (("scrape", scraped_w), ("API", api_w)):
            if field not in df.columns:
                df[field] = pd.NA
                absent_from[field] = label

    keep = ["usajobs_control_number"] + COMPARED_FIELDS
    merged = (scraped_w.loc[scraped_w["usajobs_control_number"].isin(both), keep]
              .merge(api_w.loc[api_w["usajobs_control_number"].isin(both), keep],
                     on="usajobs_control_number", suffixes=("_scrape", "_api")))

    lines += ["## Field agreement", "",
              "| field | comparable | agree | rate | scrape null | api null |",
              "|---|---:|---:|---:|---:|---:|"]

    low = []
    for field in COMPARED_FIELDS:
        cs, ca = f"{field}_scrape", f"{field}_api"
        if field in absent_from:
            # Which side is missing matters. A column only the scrape has is a
            # gain -- the current API collection never populated workSchedule
            # or promotionPotential. One only the API has is a gap in the parse.
            lines.append(f"| {field} | — | — | "
                         f"column absent from {absent_from[field]} | — | — |")
            continue
        r = compare_series(merged[cs], merged[ca])
        rate = "—" if r["rate"] is None else f"{r['rate']:.1%}"
        lines.append(f"| {field} | {r['comparable']:,} | {r['agree']:,} | "
                     f"{rate} | {r['scrape_null']:,} | {r['api_null']:,} |")
        if r["rate"] is not None and r["comparable"] >= 50 and r["rate"] < MIN_AGREEMENT:
            low.append((field, r["rate"], r["comparable"]))

    for field, rate, n in low:
        flag(f"Field '{field}' agrees on only {rate:.1%} of {n:,} comparable "
             f"postings; floor is {MIN_AGREEMENT:.0%}.")

    lines += ["", "Fields the announcement page does not publish and that are "
                  "therefore not compared: hiringAgencyCode, "
                  "hiringDepartmentCode, agencyLevel, agencyLevelSort, vendor, "
                  "whoMayApply, announcementClosingTypeDescription.", ""]

    # The long-text sections, which the API has no counterpart for.
    lines += ["## Announcement text (scraped only, no API counterpart)", "",
              "| field | rows | filled | fill rate | median chars |",
              "|---|---:|---:|---:|---:|"]
    for row in text_field_health(args.data_dir):
        rate = "—" if row["rate"] is None else f"{row['rate']:.1%}"
        lines.append(f"| {row['field']} | {row['rows']:,} | {row['filled']:,} | "
                     f"{rate} | {row['median']:,} |")

        floor = MIN_TEXT_FILL.get(row["field"], MIN_TEXT_FILL_DEFAULT)
        if row["rows"] >= 100 and row["rate"] is not None and row["rate"] < floor:
            flag(f"Announcement text field '{row['field']}' is populated on "
                 f"only {row['rate']:.1%} of {row['rows']:,} scraped postings; "
                 f"floor is {floor:.0%}. The page markup has probably changed.")
    lines.append("")

    report = "\n".join(lines)
    print(report)

    path = args.report or os.path.join(
        os.path.dirname(__file__), "..", "logs",
        f"scrape_vs_api_{datetime.now().strftime('%Y-%m-%d')}.md")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(report)
    print(f"Report written to {path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
