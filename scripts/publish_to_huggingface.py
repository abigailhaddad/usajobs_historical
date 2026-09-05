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
# Structured fields, from the historical parquet.
#
# The mirror's schema is not stable across years, so the list-valued columns
# are resolved per file rather than hardcoded:
#
#   hiringpaths_1 / jobcategories_1 / positionlocations_1
#       VARCHAR in 2017, 2018, 2020, 2024, 2025, 2026 -- absent in 2019,
#       2021, 2022, 2023
#   HiringPaths / JobCategories / PositionLocations
#       VARCHAR in every year except 2026, where they are empty integers
#
# Hardcoding the 2026 shape crashed on 2019 with 'Table "h" does not have a
# column named "hiringpaths_1"'. Preferring the *_1 varchar and falling back to
# the capitalised one covers every year.
#
# whoMayApply is cast because it is VARCHAR through 2024 and an empty INTEGER
# from 2025 -- without the cast, month files disagree on its type and reading
# the dataset as a whole breaks.
#
# Against the field list this replaces, positionTitle and hiringSubelementName
# are new -- the published dataset had no job title in it at all.
#
# Not selected: usajobs_control_number (duplicates usajobsControlNumber);
# inserted_at / last_seen, bookkeeping for this pipeline's own collection runs;
# backfilled, which only exists in 2024 and 2025.
# Three spellings have been observed for each of these, and which one holds
# the data changes both across years and over time as the mirror is rewritten:
#   hiringpaths_1   2017, 2018, 2020, 2024, 2025 and 2026 until 2026-09-05
#   HiringPaths     every year, but empty or null-typed in the recent ones
#   hiringpaths     the current 2026 mirror
# Order is most- to least-specific; whichever is a string type and present wins.
# Candidate names as DUCKDB reports them, most- to least-specific. The `_1`
# forms are duckdb's own de-collision of a case-insensitive clash in the file;
# they are not column names in the parquet.
_LIST_COLUMNS = {
    "hiringPaths": ("hiringpaths_1", "hiringpaths", "HiringPaths"),
    "jobCategories": ("jobcategories_1", "jobcategories", "JobCategories"),
    "positionLocations": ("positionlocations_1", "positionlocations",
                          "PositionLocations"),
}


def resolve_list_columns(con, hist_path):
    """Pick the column that actually holds each list field in this file.

    Resolved through duckdb's view of the schema, not pyarrow's, because the
    two disagree in a way that matters. The mirror carries both `HiringPaths`
    (a vestigial, entirely null column) and `hiringpaths` (the real data). SQL
    identifiers are case-insensitive, so duckdb sees a collision and exposes
    the second as `hiringpaths_1` -- and a query saying `h.hiringpaths` binds
    to the FIRST match, which is the empty one.

    Reading the parquet schema and emitting `h.hiringpaths` therefore produced
    175,926 rows of NULL hiring paths while looking entirely correct. The name
    to emit is the one duckdb reports.

    Years differ in which columns exist at all: 2019, 2021, 2022 and 2023 have
    a single spelling and so no collision and no `_1`, which is what crashed
    the publisher on 2019.
    """
    types = {n: t for n, t, *_ in
             con.execute(f"DESCRIBE SELECT * FROM read_parquet('{hist_path}')").fetchall()}
    out = {}
    for alias, candidates in _LIST_COLUMNS.items():
        pick = next((c for c in candidates if types.get(c) == "VARCHAR"), None)
        out[alias] = f"h.{pick}" if pick else "CAST(NULL AS VARCHAR)"
    return out


def metadata_fields(con, hist_path):
    cols = resolve_list_columns(con, hist_path)
    return f"""
    h.usajobsControlNumber::varchar AS usajobsControlNumber,
    h.positionTitle,
    h.announcementNumber,
    h.hiringAgencyCode, h.hiringAgencyName,
    h.hiringDepartmentCode, h.hiringDepartmentName,
    h.hiringSubelementName,
    h.agencyLevel, h.agencyLevelSort,
    h.appointmentType, h.workSchedule, h.serviceType,
    CAST(h.whoMayApply AS VARCHAR) AS whoMayApply,
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
    {cols['hiringPaths']}        AS hiringPaths,
    {cols['jobCategories']}      AS jobCategories,
    {cols['positionLocations']}  AS positionLocations,
    array_to_string(
        regexp_extract_all(
            coalesce(CAST({cols['jobCategories']} AS VARCHAR), ''),
            '[0-9]{{4}}'), ' | ')
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


def parquet_columns(path):
    import pyarrow.parquet as pq
    return set(pq.read_schema(path).names)


def select_sql(con, hist_path: str, scraped_path: str, month: str,
               prior_path: str = None) -> str:
    """Fresh metadata joined to announcement text from wherever it still lives.

    Text comes from the local scrape when it has it, and otherwise from the
    month file already on HuggingFace. That second source is what makes the
    local prune safe: once a posting is published the dataset holds its text,
    and a later republish — a metadata refresh, or a month gaining rows — reads
    it back rather than needing a local copy that no longer exists.

    A local row whose text has been pruned has NULL text, so it must not win
    over the published copy; hence the WHERE on the local side.
    """
    cols = ",\n        ".join(TEXT_FIELDS)
    parts = [f"""local_text AS (
        SELECT usajobs_control_number AS cn,
        {cols}
        FROM read_parquet('{scraped_path}')
        WHERE text IS NOT NULL
    )"""]
    union = "SELECT * FROM local_text"
    if prior_path:
        # A month published before a column existed does not have it. That is
        # the normal case on the first pass: every month on the dataset today
        # was written with `text` only, and the eleven sections are what this
        # run adds. Select what is there and null the rest so the shapes line
        # up for the UNION.
        available = parquet_columns(prior_path)
        prior_cols = ",\n        ".join(
            c if c in available else f"CAST(NULL AS VARCHAR) AS {c}"
            for c in TEXT_FIELDS)
        parts.append(f"""prior_text AS (
        SELECT usajobsControlNumber AS cn,
        {prior_cols}
        FROM read_parquet('{prior_path}')
        WHERE usajobsControlNumber NOT IN (SELECT cn FROM local_text)
    )""")
        union += " UNION ALL SELECT * FROM prior_text"

    text_cols = ",\n    ".join(f"t.{c}" for c in TEXT_FIELDS)
    return f"""
        WITH {", ".join(parts)}, txt AS ({union})
        SELECT {metadata_fields(con, hist_path)},
    {text_cols}
        FROM read_parquet('{hist_path}') h
        JOIN txt t ON h.usajobsControlNumber::varchar = t.cn
        WHERE substr(h.positionOpenDate, 1, 7) = '{month}'
    """


def download_month(repo, month, token):
    """The month file already on HuggingFace, or None if there is not one."""
    from huggingface_hub import hf_hub_download
    name = f"data/{month.replace('-', '_')}.parquet"
    try:
        return hf_hub_download(repo, name, repo_type="dataset", token=token)
    except Exception:
        return None


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
        WHERE h.positionOpenDate IS NOT NULL AND s.text IS NOT NULL
    """).fetchall()
    months = {}
    for month, cn in rows:
        months.setdefault(month, set()).add(cn)
    return months


def partition_safe_months(todo, months, have, month_of, priors=None):
    """Split the months to rebuild into (safe, refused).

    A month file is rewritten wholesale, so publishing one built from an
    incomplete local join would delete announcements the dataset already has,
    unrecoverably. The test is whether the rebuild would contain everything the
    existing file contains.

    The existing file is the authority for that, not the manifest. Which month
    file a posting lives in was fixed by its open date at publish time, while
    the manifest cross-referenced against *current* metadata says where it
    would go today — and those differ, because the daily collection refreshes
    the last 14 days and open dates shift. On 2026-09-04 that mismatch refused
    2026-09 over 4 postings that were sitting safely in another month's file.

    Only when there is no prior file to compare against — the download failed,
    or the month has never been published — does it fall back to the manifest.

    Returns refused entries as (month, would_drop, already_published).
    """
    priors = priors or {}
    safe, refused = [], []
    for month in todo:
        prior_cns = priors.get(month)
        if prior_cns is None:
            published_here = {cn for cn in have if month_of.get(cn) == month}
        else:
            published_here = prior_cns
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

    # HF_TOKEN in CI; the cached login from `huggingface-cli login` locally.
    from huggingface_hub import get_token
    token = os.environ.get("HF_TOKEN") or get_token()
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

    # Pull the month files we are about to rewrite. Their text is the source
    # for every posting whose local copy has been pruned, and their control
    # numbers are what makes the rebuild provably non-destructive.
    priors, availability, prior_sets = {}, {}, {}
    for month in todo:
        prior = download_month(args.repo, month, token) if token else None
        priors[month] = prior
        prior_cns = set()
        if prior:
            prior_cns = {r[0] for r in con.execute(
                f"SELECT usajobsControlNumber::varchar "
                f"FROM read_parquet('{prior}')").fetchall()}
            print(f"  {month}: {len(prior_cns):,} already published, "
                  f"{len(months[month] - prior_cns):,} new")
            prior_sets[month] = prior_cns
        availability[month] = months[month] | prior_cns

    month_of = month_of_every_posting(con, str(hist))
    safe, refused = partition_safe_months(todo, availability, have, month_of,
                                          prior_sets)

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
            COPY ({select_sql(con, str(hist), str(scraped), month, priors.get(month))}
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
