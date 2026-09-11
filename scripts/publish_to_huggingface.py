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
# positionTitle and hiringSubelementName are here because the field list this
# replaced omitted them -- the published dataset had no job title in it at all.
#
# Not selected: usajobs_control_number (duplicates usajobsControlNumber);
# inserted_at / last_seen, bookkeeping for this pipeline's own collection runs;
# backfilled, which only exists in 2024 and 2025.
#
# Every column is cast to the type it is published as; see NON_STRING_COLUMNS
# for why, and for the six files that were published before it was.
#
# The list-valued columns are resolved per file rather than named here; see
# resolve_list_columns for why nothing about the name is trustworthy.
#
# Candidate names below are as DUCKDB reports them. Order does not matter --
# the fullest column wins -- but every spelling has to be listed. The `_1`
# forms are duckdb's own de-collision of a case-insensitive clash in the file;
# they are not column names in any parquet.
_LIST_COLUMNS = {
    "hiringPaths": ("hiringpaths_1", "hiringpaths", "HiringPaths"),
    "jobCategories": ("jobcategories_1", "jobcategories", "JobCategories"),
    "positionLocations": ("positionlocations_1", "positionlocations",
                          "PositionLocations"),
}


def resolve_list_columns(con, hist_path):
    """Pick the column that actually holds each list field in this file.

    Chosen by which candidate has the most non-null values, because nothing
    about the name is a reliable signal. Two earlier rules both failed:

      hardcoding `hiringpaths_1`   crashed on 2019, which has no such column
      preferring `hiringpaths_1`   published nulls for 2017 and 2018, where
                                   HiringPaths holds 237,145 and 328,111 rows
                                   and hiringpaths_1 holds 1 and 1,245

    Measured across 2017-2026, HiringPaths is the populated column in every
    year except 2026, where it is empty and hiringpaths_1 has all 175,920.
    So the name tells you nothing and the data tells you everything.

    Resolution happens in duckdb's namespace, not the parquet's: the file
    carries two columns differing only in case, SQL identifiers are
    case-insensitive, and duckdb exposes the second as `hiringpaths_1`. Reading
    pyarrow's names and emitting them into SQL binds to the wrong one.
    """
    types = {n: t for n, t, *_ in
             con.execute(f"DESCRIBE SELECT * FROM read_parquet('{hist_path}')").fetchall()}
    out = {}
    for alias, candidates in _LIST_COLUMNS.items():
        best, best_n = None, 0
        for c in candidates:
            if types.get(c) != "VARCHAR":
                continue
            n = con.execute(
                f'SELECT count("{c}") FROM read_parquet(\'{hist_path}\')').fetchone()[0]
            if n > best_n:
                best, best_n = c, n
        out[alias] = f"h.{best}" if best else "CAST(NULL AS VARCHAR)"
    return out


# The type every published column must have, for anything not VARCHAR.
#
# Enforced rather than assumed because duckdb types a column of untyped NULLs
# as INT32, so a month where a metadata field happens to be entirely empty
# publishes a different type for it than every other month does.
#
# Not hypothetical. whoMayApply went out as INT32 for 2026-01, -03, -04 and -05
# before the cast below existed, and hiringSubelementName, serviceType and the
# two announcementClosingType columns went out as unannotated BYTE_ARRAY for
# 2013-09 and 2013-10, whose one and six rows leave them null.
#
# What that costs: arrow unifies string and binary to binary, and the 2013
# files sort first, so all four of those columns read back as *bytes* for
# everyone using the dataset -- which is what the HuggingFace viewer reports
# for them. datasets-server has also been unable to build its index for this
# dataset ("the dataset index is loading", then "the dataset index is corrupted
# and being rebuilt"), which takes filter and search down; whether the drift is
# the cause of that is unproven, but it is the only schema defect the dataset
# has.
# A refused month is an expected state, not a fault: it is what the guard does
# while backfill_scraped_pages.py is still filling in a month's text, and the
# daily workflow should note it and move on. Every other non-zero exit -- a
# column type that drifted, an unhandled traceback -- means nothing was
# published at all, which is worth waking someone for. Giving refusal its own
# code is what lets the workflow tell those apart; exit 1 stays the loud one
# because that is what an uncaught exception exits with.
EXIT_REFUSED = 3


NON_STRING_COLUMNS = {
    "agencyLevel": "BIGINT",
    "minimumSalary": "DOUBLE",
    "maximumSalary": "DOUBLE",
}


def typed(column):
    """`h.x` cast to the type x is published as."""
    return (f"CAST(h.{column} AS {NON_STRING_COLUMNS.get(column, 'VARCHAR')}) "
            f"AS {column}")


def metadata_fields(con, hist_path):
    cols = resolve_list_columns(con, hist_path)
    # Order is the dataset's existing column order; the dates sit in the
    # middle of it, so the pass-through columns come in two runs around them.
    before = ",\n    ".join(typed(c) for c in (
        "positionTitle", "announcementNumber",
        "hiringAgencyCode", "hiringAgencyName",
        "hiringDepartmentCode", "hiringDepartmentName", "hiringSubelementName",
        "agencyLevel", "agencyLevelSort",
        "appointmentType", "workSchedule", "serviceType", "whoMayApply",
        "payScale", "salaryType", "minimumSalary", "maximumSalary",
        "minimumGrade", "maximumGrade", "promotionPotential",
        "supervisoryStatus", "totalOpenings", "positionOpeningStatus",
        "announcementClosingTypeCode", "announcementClosingTypeDescription",
    ))
    after = ",\n    ".join(typed(c) for c in (
        "travelRequirement", "teleworkEligible", "relocationExpensesReimbursed",
        "securityClearanceRequired", "securityClearance", "drugTestRequired",
        "disableApplyOnline", "vendor",
    ))
    return f"""
    h.usajobsControlNumber::varchar AS usajobsControlNumber,
    {before},
    substr(h.positionOpenDate, 1, 10)   AS positionOpenDate,
    substr(h.positionCloseDate, 1, 10)  AS positionCloseDate,
    substr(h.positionExpireDate, 1, 10) AS positionExpireDate,
    {after},
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
    """A duckdb connection that spills to disk rather than being OOM-killed.

    The month COPY joins the historical mirror against twelve announcement-text
    columns and sorts the result. A month is ~30k rows of ~40 KB of text, and
    when a publish fails its text is not pruned, so the next attempt carries
    both months: 58,426 rows, around 2.3 GB, before the sort materialises it
    again. That took the process out three times on an 8 GB box, and each death
    made the next attempt bigger.

    memory_limit makes duckdb spill instead of dying, and temp_directory gives
    it somewhere to spill to. The limit is well under the box's 8 GB because
    the fetch workers and pyarrow need room alongside it.
    """
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; SET http_retries=5;")
    spill = BUILD_DIR / "duckdb-spill"
    spill.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{spill}'")
    con.execute(f"SET memory_limit='{os.environ.get('DUCKDB_MEMORY_LIMIT', '4GB')}'")
    # Both suggested by duckdb itself when it ran out at 2.7 GiB. Insertion
    # order costs a full buffer of the result, and fewer threads means fewer
    # concurrent buffers -- neither matters for a job that is IO-bound on the
    # upload anyway.
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET threads={os.environ.get('DUCKDB_THREADS', '2')}")
    return con


def parquet_columns(path):
    import pyarrow.parquet as pq
    return set(pq.read_schema(path).names)


def select_sql(con, hist_path: str, scraped_path: str, month: str,
               prior_path: str = None, days: tuple = None) -> str:
    """Fresh metadata joined to announcement text from wherever it still lives.

    Text comes from the local scrape when it has it, and otherwise from the
    month file already on HuggingFace. That second source is what makes the
    local prune safe: once a posting is published the dataset holds its text,
    and a later republish — a metadata refresh, or a month gaining rows — reads
    it back rather than needing a local copy that no longer exists.

    A local row whose text has been pruned has NULL text, so it must not win
    over the published copy; hence the WHERE on the local side.

    `days` narrows the whole query to a ('01', '15')-style range of open dates.
    It has to narrow the text sides too, not just the metadata: the text is the
    expensive half, and a day filter that only applies to the mirror leaves
    duckdb reading every unpublished row in the local scrape regardless. That
    is what makes a split build actually cheaper rather than merely slower.
    """
    cols = ",\n        ".join(TEXT_FIELDS)
    day_clause = ""
    wanted_cte = ""
    local_day = ""
    prior_day = ""
    if days:
        lo, hi = days
        day_clause = (f" AND substr(h.positionOpenDate, 9, 2) "
                      f"BETWEEN '{lo}' AND '{hi}'")
        wanted_cte = f"""wanted AS (
        SELECT usajobsControlNumber::varchar AS cn
        FROM read_parquet('{hist_path}')
        WHERE substr(positionOpenDate, 1, 7) = '{month}'
          AND substr(positionOpenDate, 9, 2) BETWEEN '{lo}' AND '{hi}'
    ), """
        local_day = " AND usajobs_control_number IN (SELECT cn FROM wanted)"
        prior_day = " AND usajobsControlNumber::varchar IN (SELECT cn FROM wanted)"

    parts, union = [], ""
    if scraped_path and os.path.exists(scraped_path):
        # Select what the file has and null the rest, exactly as the prior file
        # is handled below. save_jobs_to_parquet writes the keys the parsed
        # pages happened to carry, so a column only exists if some posting in
        # the file had it. With 30,000 postings a month something always does;
        # with the six that 2014-10 holds, whole columns are simply absent and
        # naming one is a binder error.
        local_have = parquet_columns(scraped_path)
        local_cols = ",\n        ".join(
            f"CAST({c} AS VARCHAR) AS {c}" if c in local_have
            else f"CAST(NULL AS VARCHAR) AS {c}"
            for c in TEXT_FIELDS)
        if "text" in local_have:
            parts.append(f"""local_text AS (
        SELECT usajobs_control_number AS cn,
        {local_cols}
        FROM read_parquet('{scraped_path}')
        WHERE text IS NOT NULL{local_day}
    )""")
            union = "SELECT * FROM local_text"
    if prior_path:
        # A month published before a column existed does not have it. That is
        # the normal case on the first pass: every month on the dataset today
        # was written with `text` only, and the eleven sections are what this
        # run adds. Select what is there and null the rest so the shapes line
        # up for the UNION.
        available = parquet_columns(prior_path)
        prior_cols = ",\n        ".join(
            f"CAST({c} AS VARCHAR) AS {c}" if c in available
            else f"CAST(NULL AS VARCHAR) AS {c}"
            for c in TEXT_FIELDS)
        exclude = ("WHERE usajobsControlNumber NOT IN (SELECT cn FROM local_text)"
                   if union else "")
        if prior_day:
            exclude = (exclude + prior_day) if exclude \
                else f"WHERE 1=1{prior_day}"
        parts.append(f"""prior_text AS (
        SELECT usajobsControlNumber AS cn,
        {prior_cols}
        FROM read_parquet('{prior_path}')
        {exclude}
    )""")
        union += (" UNION ALL " if union else "") + "SELECT * FROM prior_text"

    if not parts:
        # No local text and no published file: there is nothing to build this
        # month from. Say so, rather than emitting SQL with an empty WITH and
        # letting duckdb report a syntax error several frames away.
        raise ValueError(
            f"no text available for {month}: the local scrape has none and "
            f"the dataset has no file for it")

    text_cols = ",\n    ".join(f"t.{c}" for c in TEXT_FIELDS)
    return f"""
        WITH {wanted_cte}{", ".join(parts)}, txt AS ({union})
        SELECT {metadata_fields(con, hist_path)},
    {text_cols}
        FROM read_parquet('{hist_path}') h
        JOIN txt t ON h.usajobsControlNumber::varchar = t.cn
        WHERE substr(h.positionOpenDate, 1, 7) = '{month}'{day_clause}
    """


# A month is built in slices of about this many postings. Measured on 2022-03:
# 41,871 rows in one pass exhausted a 2.7 GiB duckdb limit, and a 21k-row slice
# still wanted 1.3 GiB, so the cost runs at roughly 60 KB of working memory per
# row. 12,000 keeps a slice near 700 MB, which leaves several times over under
# the 4.5 GB the unit grants and does not creep as months grow.
SLICE_ROWS = 12000


def day_partitions(con, hist_path, month, target=SLICE_ROWS):
    """Contiguous day ranges that each hold roughly `target` postings.

    Balanced by actual counts rather than by splitting the calendar in half:
    postings cluster hard around the start and end of a month, so even halves
    of the date range are not even halves of the work.

    Returns [] when the month fits in one pass, which keeps the common case on
    the original single-COPY path.
    """
    rows = con.execute(f"""
        SELECT substr(positionOpenDate, 9, 2) AS d, count(*) AS n
        FROM read_parquet('{hist_path}')
        WHERE substr(positionOpenDate, 1, 7) = '{month}'
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    total = sum(n for _, n in rows)
    if total <= target or not rows:
        return []

    ranges, lo, run = [], None, 0
    for day, n in rows:
        if lo is None:
            lo = day
        run += n
        if run >= target:
            ranges.append((lo, day))
            lo, run = None, 0
    # A trailing remainder becomes its own slice. Folding it into the previous
    # one looks tidier and defeats the point: a month that is 30k on the 1st
    # and 12k over the rest would merge back into a single 42k pass.
    if run:
        ranges.append((lo, rows[-1][0]))
    # Widen the ends so the ranges provably cover the whole month, including
    # any day the mirror has no rows for. Gaps here would silently drop rows.
    ranges[0] = ("01", ranges[0][1])
    ranges[-1] = (ranges[-1][0], "31")
    # One slice is the single-pass build with extra steps. Note that a single
    # day bigger than the target cannot be divided any further than this --
    # day granularity is the floor, and such a month still needs the memory.
    return ranges if len(ranges) > 1 else []


def stitch(parts, dest):
    """Concatenate slice files into one month file, a row group at a time.

    pyarrow rather than a second duckdb pass on purpose: this streams, so peak
    memory is one row group instead of the month that was too big to build in
    the first place.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    writer = None
    try:
        for part in parts:
            pf = pq.ParquetFile(part)
            if writer is None:
                writer = pq.ParquetWriter(dest, pf.schema_arrow,
                                          compression="zstd",
                                          compression_level=12)
            for batch in pf.iter_batches(batch_size=2000):
                writer.write_table(pa.Table.from_batches([batch]))
    finally:
        if writer is not None:
            writer.close()


def check_types(path):
    """Refuse to upload a month whose columns are not the published types.

    The casts in metadata_fields are what keep this true. This is what turns a
    future edit that drops one of them into a failed build rather than a
    dataset that silently stops being indexable -- the failure mode is invisible
    from here, since every month reads fine on its own and only the union of
    them breaks.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    arrow = {"BIGINT": pa.int64(), "DOUBLE": pa.float64()}
    wrong = [f"{f.name} is {f.type}, expected {want}"
             for f in pq.read_schema(path)
             for want in [arrow.get(NON_STRING_COLUMNS.get(f.name), pa.string())]
             if f.type != want]
    if wrong:
        raise SystemExit(f"{path} has the wrong column types, refusing to "
                         f"publish it: " + "; ".join(wrong))


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

    if not hist.exists():
        print(f"Missing {hist} — nothing to publish.")
        return 0
    # The scraped file is optional for a deliberate one-month republish: its
    # text may have been pruned after publishing, or never fetched on this
    # machine at all, in which case the dataset's own copy supplies it.
    republishing = bool(args.month and args.refresh_all)
    if not scraped.exists() and not republishing:
        print(f"Missing {scraped} — nothing to publish.")
        return 0

    # HF_TOKEN in CI; the cached login from `huggingface-cli login` locally.
    from huggingface_hub import get_token
    token = os.environ.get("HF_TOKEN") or get_token()
    if not token and not args.dry_run:
        print("HF_TOKEN is not set — nothing published.")
        return 0

    con = connection()
    months = (available_months(con, str(hist), str(scraped))
              if scraped.exists() else {})
    if not months and not republishing:
        print("No postings have both metadata and scraped text yet.")
        return 0

    have = published_control_numbers(args.repo, token)
    print(f"Dataset holds {len(have):,} announcements")

    total_available = sum(len(v) for v in months.values())
    new = {cn for cns in months.values() for cn in cns} - have
    print(f"Local join has {total_available:,}; {len(new):,} are new")

    if args.month:
        months = {m: cns for m, cns in months.items() if m == args.month}
        if not months and not republishing:
            print(f"No postings opened in {args.month} have scraped text.")
            return 0

    if args.month and args.refresh_all:
        # A deliberate republish of one month. Not gated on local text: the
        # point is usually to redo the metadata join for a month whose text
        # was pruned after publishing, or was never fetched on this machine.
        todo = [args.month]
        print(f"Republishing {args.month} from the dataset's own text")
    elif args.refresh_all:
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
                  f"{len(months.get(month, set()) - prior_cns):,} new")
            prior_sets[month] = prior_cns

            # If the local scrape already covers everything the published file
            # holds, the prior adds no text and only costs memory. Reading it
            # anyway meant materialising the month's announcement text twice --
            # ~1.2 GB each -- which is what drove the publish into constant
            # spilling and took a month from minutes to nearly an hour.
            if prior_cns and prior_cns <= months.get(month, set()):
                print(f"    local copy covers all of it; skipping the "
                      f"published text")
                priors[month] = None
        availability[month] = months.get(month, set()) | prior_cns

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
        return EXIT_REFUSED if refused else 0

    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    written = []
    for month in todo:
        name = f"data/{month.replace('-', '_')}.parquet"
        dest = BUILD_DIR / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        # COPY streams straight to disk. Materializing a month in Python would
        # be ~800 MB of announcement text.
        #
        # Deliberately unordered. ORDER BY forced duckdb to materialise and
        # sort ~2 GB of announcement text, which is what exhausted the memory
        # limit; row order in a parquet is not meaningful to consumers and
        # anyone who wants it can sort on read.
        #
        # Compression level 19 buffers far more than it saves here; 12 is
        # within a few percent on this data and materially cheaper in memory.
        copy_opts = "(FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 12)"

        # Big months are built in day slices and stitched. Streaming COPY still
        # has to hold the join, and the text is 35 KB a row: 2022-03 was 41,871
        # rows and took a 2.7 GiB duckdb limit out in one pass. Raising the
        # limit only moves that wall, since months keep growing -- 2022 runs
        # 35-42k against 2021's ~31k.
        slices = day_partitions(con, str(hist), month)
        if slices:
            print(f"  {month}: {len(slices)} slices "
                  + ", ".join(f"{lo}-{hi}" for lo, hi in slices))
            parts = []
            try:
                for i, days in enumerate(slices, 1):
                    part = dest.with_suffix(f".part{i:02d}.parquet")
                    con.execute(
                        f"COPY ({select_sql(con, str(hist), str(scraped), month, priors.get(month), days)}) "
                        f"TO '{part}' {copy_opts};")
                    parts.append(part)
                stitch(parts, dest)
            finally:
                for part in parts:
                    part.unlink(missing_ok=True)
        else:
            con.execute(
                f"COPY ({select_sql(con, str(hist), str(scraped), month, priors.get(month))}) "
                f"TO '{dest}' {copy_opts};")
        check_types(dest)
        rows = con.execute(
            f"SELECT count(*) FROM read_parquet('{dest}')").fetchone()[0]
        print(f"  {name}: {rows:,} rows, {dest.stat().st_size/1e6:.1f} MB")
        written.append(name)

    # Only control numbers in months we actually wrote, plus whatever was
    # already published. Adding one whose month was refused would claim the
    # dataset holds a posting that is not in any file.
    written_cns = {cn for m in todo for cn in months.get(m, set())}
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

    # Nothing to prune when the text never was local -- a republish takes it
    # from the dataset, so there is no file here to shrink.
    if not args.keep_text and scraped.exists():
        cleared = prune_published_text(str(scraped), written_cns)
        after = os.path.getsize(scraped) / 1e6
        print(f"Dropped local text for {cleared:,} published postings; "
              f"{scraped.name} is now {after:,.0f} MB")

    return EXIT_REFUSED if refused else 0


if __name__ == "__main__":
    sys.exit(main())
