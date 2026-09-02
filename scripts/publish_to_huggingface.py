#!/usr/bin/env python3
"""
Publish the announcement dataset to HuggingFace.

Joins two things this pipeline already has on disk:

  data/historical_jobs_{year}.parquet  structured fields, from the historical
                                       API — which needs no key and carries
                                       closed postings, so it is the complete
                                       list of what exists
  data/scraped_jobs_{year}.parquet     announcement text, from the page — the
                                       part no API gives well

Output is one parquet per month plus a manifest, matching the layout the
dataset already has. Columns are only ever added, so anything reading it keeps
working.

This replaces the publish path that lived in abigailhaddad/joa. Two reasons it
moved. The dataset was missing `positionTitle` — the job title — because joa's
field list never selected it, and nobody noticed since its controls CSV aliases
announcementNumber as "title". And the eleven structured sections
(qualificationSummary, majorDuties, ...) come from a parse that lives here, so
publishing from here avoids a cross-repo dependency on it.

    python publish_to_huggingface.py --dry-run
    python publish_to_huggingface.py
    python publish_to_huggingface.py --refresh-all   # rewrite every month
"""

import argparse
import os
import sys
from pathlib import Path

import duckdb

REPO_ID = os.environ.get("HF_DATASET_REPO", "abigailhaddad/usajobs-scraping")

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
BUILD_DIR = Path(__file__).resolve().parent.parent / "build" / "hf"

# Structured fields, from the historical parquet.
#
# Against the field list this replaces, positionTitle and hiringSubelementName
# are new — the published dataset had no job title in it at all.
#
# Not selected: usajobs_control_number (duplicates usajobsControlNumber);
# HiringPaths / JobCategories / PositionLocations, which are empty integer
# columns superseded by the *_1 varchars; inserted_at / last_seen, which are
# bookkeeping for this pipeline's own collection runs.
METADATA_FIELDS = """
    h.usajobsControlNumber::varchar AS usajobsControlNumber,
    h.positionTitle,
    h.announcementNumber,
    h.hiringAgencyCode, h.hiringAgencyName,
    h.hiringDepartmentCode, h.hiringDepartmentName,
    h.hiringSubelementName,
    h.agencyLevel, h.agencyLevelSort,
    h.appointmentType, h.workSchedule, h.serviceType, h.whoMayApply,
    h.payScale, h.salaryType, h.minimumSalary, h.maximumSalary,
    h.minimumGrade, h.maximumGrade, h.promotionPotential, h.supervisoryStatus,
    h.totalOpenings, h.positionOpeningStatus,
    h.announcementClosingTypeCode, h.announcementClosingTypeDescription,
    substr(h.positionOpenDate, 1, 10)   AS positionOpenDate,
    substr(h.positionCloseDate, 1, 10)  AS positionCloseDate,
    substr(h.positionExpireDate, 1, 10) AS positionExpireDate,
    h.travelRequirement, h.teleworkEligible, h.relocationExpensesReimbursed,
    h.securityClearanceRequired, h.securityClearance, h.drugTestRequired,
    h.disableApplyOnline, h.vendor,
    h.hiringpaths_1        AS hiringPaths,
    h.jobcategories_1      AS jobCategories,
    h.positionlocations_1  AS positionLocations,
    array_to_string(
        regexp_extract_all(
            coalesce(CAST(h.jobcategories_1 AS VARCHAR), ''), '[0-9]{4}'), ' | ')
        AS occupationalSeries
"""

# The announcement body, from the page parse. The whole-page `text` stays for
# anything already built against it; the sections are what make it usable
# without re-parsing.
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


def prune_published_text(scraped_path, published_cns):
    """Blank the announcement text on rows that are now on HuggingFace.

    The text columns are 97.8% of scraped_jobs_{year}.parquet — 5.42 KB of a
    5.54 KB row — so a full year is 920 MB kept locally against 20 MB for
    everything else in it. Once a posting is published, the dataset is the
    store; what stays here is the structured shadow the API comparison reads,
    plus the text for anything not yet pushed.

    Rewrites row group by row group so peak memory does not track file size.
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    import tempfile

    if not published_cns or not os.path.exists(scraped_path):
        return 0

    pf = pq.ParquetFile(scraped_path)
    present = [c for c in TEXT_FIELDS if c in pf.schema_arrow.names]
    if not present:
        return 0

    drop = pa.array(sorted(published_cns), type=pa.string())
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(scraped_path) or ".",
                               prefix=".tmp_", suffix=".parquet")
    os.close(fd)
    cleared = 0
    try:
        with pq.ParquetWriter(tmp, pf.schema_arrow, compression="zstd",
                              compression_level=3) as writer:
            for batch in pf.iter_batches(batch_size=2000):
                table = pa.Table.from_batches([batch])
                ids = table.column("usajobs_control_number").cast(pa.string())
                hit = pc.fill_null(pc.is_in(ids, value_set=drop), False)
                cleared += int(pc.sum(hit).as_py() or 0)
                for name in present:
                    col = table.column(name)
                    blanked = pc.if_else(hit, pa.nulls(len(col), col.type), col)
                    table = table.set_column(
                        table.schema.get_field_index(name),
                        table.schema.field(name), blanked)
                writer.write_table(table)
        os.replace(tmp, scraped_path)
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    return cleared


def parse_args():
    p = argparse.ArgumentParser(description="Publish the dataset to HuggingFace")
    p.add_argument("--year", type=int,
                   default=__import__("datetime").date.today().year)
    p.add_argument("--repo", default=REPO_ID)
    p.add_argument("--dry-run", action="store_true",
                   help="Build locally and report; upload nothing")
    p.add_argument("--refresh-all", action="store_true",
                   help="Rewrite every month, not just the ones with new rows. "
                        "Metadata keeps changing after a posting appears "
                        "(close dates, opening status), so run this on a slower "
                        "schedule than the daily top-up.")
    p.add_argument("--month", help="Publish only this month (YYYY-MM)")
    p.add_argument("--keep-text", action="store_true",
                   help="Do not drop the published announcement text from the "
                        "local parquet afterwards. The text is 97.8%% of that "
                        "file — 920 MB for a year against 20 MB for the "
                        "structured columns — so by default HuggingFace is the "
                        "store for it and the local copy keeps only what has "
                        "not been published yet.")
    p.add_argument("--data-dir", default=str(DATA_DIR))
    return p.parse_args()


def connection():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; SET http_retries=5;")
    return con


def select_sql(hist_path: str, scraped_path: str, month: str = None) -> str:
    """The join. Scraped columns are coalesced against nothing — a posting with
    no scraped row is excluded, because a row with no announcement text is what
    the historical mirror already publishes."""
    text_cols = ",\n    ".join(f"s.{c}" for c in TEXT_FIELDS)
    where = ""
    if month:
        where = f"WHERE substr(h.positionOpenDate, 1, 7) = '{month}'"
    return f"""
        SELECT {METADATA_FIELDS},
    {text_cols}
        FROM read_parquet('{hist_path}') h
        JOIN read_parquet('{scraped_path}') s
          ON h.usajobsControlNumber::varchar = s.usajobs_control_number
        {where}
    """


def month_of_every_posting(con, hist_path):
    """Control number -> month, for the whole year, join or no join.

    Needed to tell whether rebuilding a month would drop rows the dataset
    already has: the manifest lists control numbers but not which month's file
    holds them.
    """
    rows = con.execute(f"""
        SELECT usajobsControlNumber::varchar AS cn,
               substr(positionOpenDate, 1, 7) AS month
        FROM read_parquet('{hist_path}')
        WHERE positionOpenDate IS NOT NULL
    """).fetchall()
    return {cn: month for cn, month in rows}


def available_months(con, hist_path, scraped_path):
    """Month -> control numbers we could publish, computed without reading the
    text columns. One varchar column per row, so it stays small even at the
    scale of a full year."""
    rows = con.execute(f"""
        SELECT substr(h.positionOpenDate, 1, 7) AS month,
               h.usajobsControlNumber::varchar AS cn
        FROM read_parquet('{hist_path}') h
        JOIN read_parquet('{scraped_path}') s
          ON h.usajobsControlNumber::varchar = s.usajobs_control_number
        WHERE h.positionOpenDate IS NOT NULL
    """).fetchall()
    months = {}
    for month, cn in rows:
        months.setdefault(month, set()).add(cn)
    return months


def partition_safe_months(todo, months, have, month_of):
    """Split the months to rebuild into (safe, refused).

    A month file is rewritten wholesale, so publishing one built from an
    incomplete local join would delete announcements the dataset already has.
    That is the normal state while the page backfill is still running, and it
    is not recoverable from the dataset side, so refuse rather than shrink.

    Returns refused entries as (month, would_drop, already_published).
    """
    safe, refused = [], []
    for month in todo:
        published_here = {cn for cn in have if month_of.get(cn) == month}
        dropping = published_here - months.get(month, set())
        if dropping:
            refused.append((month, len(dropping), len(published_here)))
        else:
            safe.append(month)
    return safe, refused


def published_control_numbers(repo: str, token) -> set:
    """What the dataset already holds. Downloads the manifest, a couple of MB,
    rather than the dataset."""
    from huggingface_hub import hf_hub_download
    try:
        path = hf_hub_download(repo, "manifest.csv", repo_type="dataset",
                               token=token)
    except Exception as e:
        print(f"No manifest on {repo} ({e}) — treating the dataset as empty")
        return set()
    with open(path) as f:
        next(f, None)  # header
        return {line.strip() for line in f if line.strip()}


def main() -> int:
    args = parse_args()
    data_dir = Path(args.data_dir)
    hist = data_dir / f"historical_jobs_{args.year}.parquet"
    scraped = data_dir / f"scraped_jobs_{args.year}.parquet"

    for path in (hist, scraped):
        if not path.exists():
            print(f"Missing {path} — nothing to publish.")
            return 0

    token = os.environ.get("HF_TOKEN")
    if not token and not args.dry_run:
        print("HF_TOKEN is not set — nothing published.")
        return 0

    con = connection()
    months = available_months(con, str(hist), str(scraped))
    if not months:
        print("No postings have both metadata and scraped text yet.")
        return 0

    have = published_control_numbers(args.repo, token)
    print(f"Dataset holds {len(have):,} announcements")

    total_available = sum(len(v) for v in months.values())
    new = {cn for cns in months.values() for cn in cns} - have
    print(f"Local join has {total_available:,}; {len(new):,} are new")

    if args.month:
        months = {m: cns for m, cns in months.items() if m == args.month}
        if not months:
            print(f"No postings opened in {args.month} have scraped text.")
            return 0

    if args.refresh_all:
        todo = sorted(months)
        print(f"Refreshing all {len(todo)} month(s)")
    else:
        todo = sorted(m for m, cns in months.items() if cns - have)
        print(f"{len(todo)} month(s) have new rows: {', '.join(todo) or '(none)'}")

    month_of = month_of_every_posting(con, str(hist))
    safe, refused = partition_safe_months(todo, months, have, month_of)

    for month, n, total in refused:
        print(f"  REFUSING {month}: rebuilding it would drop {n:,} of the "
              f"{total:,} announcements already published for that month. "
              f"The local scrape is missing them — run "
              f"backfill_scraped_pages.py first.")
    todo = safe

    if not todo:
        print("Nothing to publish")
        return 1 if refused else 0

    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    written = []
    for month in todo:
        name = f"data/{month.replace('-', '_')}.parquet"
        dest = BUILD_DIR / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        # COPY streams straight to disk. Materializing a month in Python would
        # be ~800 MB of announcement text.
        con.execute(f"""
            COPY ({select_sql(str(hist), str(scraped), month)}
                  ORDER BY usajobsControlNumber)
            TO '{dest}' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 19);
        """)
        rows = con.execute(
            f"SELECT count(*) FROM read_parquet('{dest}')").fetchone()[0]
        print(f"  {name}: {rows:,} rows, {dest.stat().st_size/1e6:.1f} MB")
        written.append(name)

    # Only control numbers in months we actually wrote, plus whatever was
    # already published. Adding one whose month was refused would claim the
    # dataset holds a posting that is not in any file.
    written_cns = {cn for m in todo for cn in months[m]}
    manifest = sorted(have | written_cns)
    (BUILD_DIR / "manifest.csv").write_text(
        "usajobsControlNumber\n" + "\n".join(manifest) + "\n")

    if args.dry_run:
        print(f"\nDry run — {len(written)} file(s) built in {BUILD_DIR}, "
              f"manifest would hold {len(manifest):,}")
        return 0

    # One commit for everything. File-by-file would mean a commit per month
    # plus another for the manifest, leaving the dataset briefly inconsistent.
    from huggingface_hub import CommitOperationAdd, HfApi
    api = HfApi(token=token)
    ops = [CommitOperationAdd(path_in_repo=n, path_or_fileobj=str(BUILD_DIR / n))
           for n in written + ["manifest.csv"]]
    api.create_commit(
        repo_id=args.repo, repo_type="dataset", operations=ops,
        commit_message=f"+{len(written_cns - have):,} announcements, "
                       f"{len(manifest):,} total")
    print(f"\nPushed {len(written)} month(s) to {args.repo}")

    if not args.keep_text:
        cleared = prune_published_text(str(scraped), written_cns)
        after = os.path.getsize(scraped) / 1e6
        print(f"Dropped local text for {cleared:,} published postings; "
              f"{scraped.name} is now {after:,.0f} MB")

    return 1 if refused else 0


if __name__ == "__main__":
    sys.exit(main())
