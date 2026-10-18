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

A posting's month file is the month it opened in *when it was first
published*, and it stays there. USAJOBS moves open dates after the fact, so
positionOpenDate can name a later month than the file it sits in.
locations.parquet on the dataset records which file holds each posting.

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
    p.add_argument("--rebuild-locations", action="store_true",
                   help="Rebuild the map of which month file holds each "
                        "posting by reading every file's control numbers "
                        "(~1 s a file), instead of trusting the dataset's "
                        "locations.parquet.")
    p.add_argument("--locations-file",
                   help="Use this local locations.parquet -- e.g. the one a "
                        "dry run left in build/hf -- instead of the dataset's "
                        "or a fresh scan. The map is still corrected against "
                        "every month file this run downloads.")
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


def select_sql(con, hist_path: str, scraped_path: str,
               prior_path: str = None, days: tuple = None) -> str:
    """One month file's rows: fresh metadata joined to announcement text.

    Which postings go in the file is the `members` temp table (see
    month_members), not an open-date filter. A posting stays in the file it was
    first published in even after USAJOBS moves its open date, so selecting by
    today's open date is what used to drop rows from one file without adding
    them to another.

    Text comes from the local scrape when it has it, and otherwise from the
    month file already on HuggingFace. That second source is what makes the
    local prune safe: once a posting is published the dataset holds its text,
    and a later republish reads it back rather than needing a local copy that
    no longer exists. A local row whose text has been pruned has NULL text, so
    it must not win over the published copy; hence the WHERE on the local side.

    A member the year's mirror does not have -- its open date moved into
    another year -- has no fresh metadata to join, so its published row is
    carried over unchanged rather than dropped.

    `days` narrows the whole query to a ('01', '15')-style range of the
    members' open-date days. It has to narrow the text sides too: the text is
    the expensive half, and a filter on the metadata alone leaves duckdb
    reading every unpublished row in the local scrape regardless.
    """
    day_filter = (f" WHERE d BETWEEN '{days[0]}' AND '{days[1]}'"
                  if days else "")
    parts = [f"wanted AS (SELECT cn FROM members{day_filter})"]
    union = ""
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
        WHERE text IS NOT NULL
          AND usajobs_control_number IN (SELECT cn FROM wanted)
        QUALIFY row_number() OVER (PARTITION BY usajobs_control_number) = 1
    )""")
            union = "SELECT * FROM local_text"
    if prior_path:
        # A month published before a column existed does not have it. Select
        # what is there and null the rest so the shapes line up for the UNION.
        available = parquet_columns(prior_path)
        prior_cols = ",\n        ".join(
            f"CAST({c} AS VARCHAR) AS {c}" if c in available
            else f"CAST(NULL AS VARCHAR) AS {c}"
            for c in TEXT_FIELDS)
        exclude = (" AND usajobsControlNumber NOT IN (SELECT cn FROM local_text)"
                   if union else "")
        parts.append(f"""prior_text AS (
        SELECT usajobsControlNumber AS cn,
        {prior_cols}
        FROM read_parquet('{prior_path}')
        WHERE usajobsControlNumber IN (SELECT cn FROM wanted){exclude}
    )""")
        union += (" UNION ALL " if union else "") + "SELECT * FROM prior_text"

    if not union:
        # No local text and no published file: there is nothing to build this
        # month from. Say so, rather than emitting SQL with an empty UNION and
        # letting duckdb report a syntax error several frames away.
        raise ValueError("no text available: the local scrape has none and "
                         "the dataset has no file for this month")

    text_cols = ",\n    ".join(f"t.{c}" for c in TEXT_FIELDS)
    sql = f"""
        WITH {", ".join(parts)}, txt AS ({union})
        SELECT {metadata_fields(con, hist_path)},
    {text_cols}
        FROM read_parquet('{hist_path}') h
        JOIN txt t ON h.usajobsControlNumber::varchar = t.cn
    """
    if prior_path:
        sql = f"""
        {sql}
        UNION ALL BY NAME
        SELECT * FROM read_parquet('{prior_path}')
        WHERE usajobsControlNumber IN (SELECT cn FROM wanted)
          AND usajobsControlNumber NOT IN (
              SELECT usajobsControlNumber::varchar FROM read_parquet('{hist_path}'))
    """
    # The WITH has to scope over both halves of the UNION, so wrap the lot.
    return f"SELECT * FROM ({sql})" if prior_path else sql


# A month is built in slices of about this many postings. Measured on 2022-03:
# 41,871 rows in one pass exhausted a 2.7 GiB duckdb limit, and a 21k-row slice
# still wanted 1.3 GiB, so the cost runs at roughly 60 KB of working memory per
# row. 12,000 keeps a slice near 700 MB, which leaves several times over under
# the 4.5 GB the unit grants and does not creep as months grow.
SLICE_ROWS = 12000


def day_partitions(con, target=SLICE_ROWS):
    """Contiguous day ranges that each hold roughly `target` members.

    Balanced by actual counts rather than by splitting the calendar in half:
    postings cluster hard around the start and end of a month, so even halves
    of the date range are not even halves of the work.

    Returns [] when the month fits in one pass, which keeps the common case on
    the original single-COPY path.
    """
    rows = con.execute(
        "SELECT d, count(*) FROM members GROUP BY 1 ORDER BY 1").fetchall()
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
    # Widen the ends so the ranges provably cover every day value, including
    # ones no member has. Gaps here would silently drop rows.
    ranges[0] = ("00", ranges[0][1])
    ranges[-1] = (ranges[-1][0], "99")
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




def month_file(month):
    return f"data/{month.replace('-', '_')}.parquet"


def download_month(repo, month, token):
    """The month file already on HuggingFace, or None if there is not one.

    Only a 404 means "not one". Any other failure raises: a month file that
    exists but could not be fetched is not the same as a month that was never
    published, and treating it as one would rebuild the month without the rows
    it already holds.
    """
    from huggingface_hub import hf_hub_download
    from huggingface_hub.errors import EntryNotFoundError
    try:
        return hf_hub_download(repo, month_file(month), repo_type="dataset",
                               token=token)
    except EntryNotFoundError:
        return None


def dataset_months(repo, token):
    """Months that have a file on the dataset, as YYYY-MM."""
    from huggingface_hub import list_repo_files
    months = set()
    for f in list_repo_files(repo, repo_type="dataset", token=token):
        name = os.path.basename(f)
        if f.startswith("data/") and name.endswith(".parquet"):
            stem = name[:-len(".parquet")]
            if len(stem) == 7 and stem[4] == "_":
                months.add(stem.replace("_", "-"))
    return months


# Which month file each published posting lives in, kept on the dataset beside
# the manifest. It is what lets a posting stay put when its open date moves.
#
# Before it existed, a month was rebuilt from whatever the mirror *currently*
# said opened that month. USAJOBS moves open dates -- a reannounced posting
# reopens weeks later under the same control number -- so a rebuild dropped
# the posting from the file it was in, and the month it moved to had no copy of
# its text (pruned locally after the first publish), so it landed nowhere.
# Measured 2026-10-17: 164 postings listed in the manifest were in no month
# file, and 133 sat in two. The same mismatch is what stalled the daily publish
# from 2026-10-02: 2026-10 had no file yet, the guard fell back to the
# manifest, and refused over 144 "published October postings" that actually
# lived in September's file and earlier.
LOCATIONS_FILE = "locations.parquet"


def scan_locations(repo, months, token):
    """{control number: [months whose file holds it]}, read off the files.

    Only the control-number column is read. Every file has to be read: a scan
    that skips one would report its postings as unpublished and they would be
    published again, duplicated.

    A few files at a time. One at a time took ~6 s a file, 15 minutes for the
    dataset; many at once is what gets a reader 429'd.
    """
    import time
    from concurrent.futures import ThreadPoolExecutor
    import pyarrow.parquet as pq
    from huggingface_hub import HfFileSystem
    fs = HfFileSystem(token=token)

    def read(month):
        path = f"datasets/{repo}/{month_file(month)}"
        for attempt in range(6):
            try:
                with fs.open(path, "rb") as fh:
                    return month, pq.read_table(
                        fh, columns=["usajobsControlNumber"]).column(0).to_pylist()
            except Exception as e:
                # A busy reader gets a 429 partway through a full scan; back
                # off rather than give up on the file.
                if attempt == 5:
                    raise RuntimeError(f"could not read {path}: {e}") from e
                time.sleep(10 * (attempt + 1))

    found = {}
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as pool:
        for i, (month, cns) in enumerate(pool.map(read, sorted(months)), 1):
            for cn in cns:
                found.setdefault(str(cn), []).append(month)
            if i % 20 == 0:
                print(f"    scanned {i}/{len(months)} month files", flush=True)
    return found


SCAN_WORKERS = 4


def choose_locations(found, month_of):
    """One month per control number, from scan_locations' lists.

    A posting in two files keeps the one matching its current open date if
    either does, else the earliest. The other copy is then dropped the next
    time its month is rebuilt.
    """
    return {cn: (month_of[cn] if month_of.get(cn) in ms else min(ms))
            for cn, ms in found.items()}


def load_locations(repo, token):
    """{control number: month} from the dataset, or None if it has none yet."""
    import pyarrow.parquet as pq
    from huggingface_hub import hf_hub_download
    from huggingface_hub.errors import EntryNotFoundError
    try:
        path = hf_hub_download(repo, LOCATIONS_FILE, repo_type="dataset",
                               token=token)
    except EntryNotFoundError:
        return None
    t = pq.read_table(path)
    return dict(zip(t.column("usajobsControlNumber").to_pylist(),
                    t.column("month").to_pylist()))


def write_locations(located, path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    cns = sorted(located)
    pq.write_table(pa.table({
        "usajobsControlNumber": pa.array(cns, pa.string()),
        "month": pa.array([located[c] for c in cns], pa.string()),
    }), path, compression="zstd")


def month_members(month, located, local_by_month):
    """The control numbers month's file should hold after a rebuild.

    Everything already located there, plus every posting with local text whose
    current open date is in this month and which is in no file at all. A
    posting already published in another month's file is not added here even
    if the mirror now dates it to this month; that is the rule that keeps it
    from being in two files.

    "In no file" covers both brand-new postings and ones the old date-based
    rebuild lost, so a lost posting comes back as soon as its page is fetched
    again.
    """
    here = {cn for cn, m in located.items() if m == month}
    new = {cn for cn in local_by_month.get(month, set()) if cn not in located}
    return here | new


def months_to_rebuild(located, local_by_month):
    """Months holding at least one posting that is in no file yet."""
    return sorted(m for m, cns in local_by_month.items()
                  if any(cn not in located for cn in cns))


def reconcile_with_prior(month, located, prior_cns, on_dataset):
    """Correct the location map against the month file actually downloaded.

    The file is the authority. Returns (located, stale, moved_out):

      stale      control numbers the map placed in this month that the file
                 does not hold. They are in no file, so they leave the map and
                 become eligible to be published again.
      moved_out  control numbers the file holds that the map places in another
                 month's file. Those are duplicates, and the rebuild drops
                 this copy -- but only when that other file exists, since a
                 map pointing at a file that is not there would turn "drop a
                 duplicate" into "drop the only copy".
    """
    located = dict(located)
    here = {cn for cn, m in located.items() if m == month}
    stale = here - prior_cns
    for cn in stale:
        del located[cn]
    moved_out = set()
    for cn in prior_cns:
        elsewhere = located.get(cn)
        if elsewhere is None or (elsewhere != month
                                 and elsewhere not in on_dataset):
            located[cn] = month
        elif elsewhere != month:
            moved_out.add(cn)
    return located, stale, moved_out


def verify_build(month, built_cns, n_rows, members, prior_cns, moved_out):
    """Refuse a month whose built file is not exactly what was intended.

    Checked against the file actually written, not against a prediction of
    it: a month file is rewritten wholesale, and anything this misses is
    deleted from the dataset with no way back. Returns a reason, or None.
    """
    if n_rows != len(built_cns):
        return f"{n_rows - len(built_cns):,} duplicate rows"
    lost = (prior_cns - moved_out) - built_cns
    if lost:
        return (f"would drop {len(lost):,} of the {len(prior_cns):,} "
                f"announcements already in the file, e.g. "
                f"{', '.join(sorted(lost)[:5])}")
    missing = members - built_cns
    if missing:
        return (f"{len(missing):,} intended postings came out without a row, "
                f"e.g. {', '.join(sorted(missing)[:5])}")
    extra = built_cns - members
    if extra:
        return f"{len(extra):,} rows nobody asked for"
    return None


def local_months(con, hist_path, scraped_path):
    """Month -> control numbers that have local announcement text, by the
    mirror's current open date. Computed without reading the text columns.
    One varchar column per row, so it stays small even at the scale of a full
    year."""
    rows = con.execute(f"""
        SELECT DISTINCT substr(h.positionOpenDate, 1, 7) AS month,
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


def month_of_every_posting(con, hist_path):
    """Control number -> month by the mirror's current open date."""
    rows = con.execute(f"""
        SELECT usajobsControlNumber::varchar AS cn,
               substr(positionOpenDate, 1, 7) AS month
        FROM read_parquet('{hist_path}')
        WHERE positionOpenDate IS NOT NULL
    """).fetchall()
    return {cn: month for cn, month in rows}


def published_control_numbers(repo: str, token) -> set:
    """What the dataset's manifest lists. Downloads the manifest, a couple of
    MB, rather than the dataset."""
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


# The tripwire the daily workflow turns into a GitHub issue. A refused or
# stalled publish exits cleanly enough that the run stays green -- that is how
# the 2026-10-02 stall went unnoticed for two weeks -- so this file, not the
# exit code, is what makes it visible.
WARNING_FILE = Path(__file__).resolve().parent.parent / "logs" / \
    "HF_PUBLISH_WARNING.txt"

# A posting the mirror has had for this long should be on the dataset. The
# mirror itself trails the posting by a few days, and the daily gap-fill fetch
# is capped, so anything younger is in flight rather than missing.
STALE_AFTER_DAYS = 7
# Below this many, a stale backlog is the handful of pages usajobs.gov 404s
# for postings it has withdrawn, not a stuck pipeline.
STALE_TOLERANCE = 50


def warn(message):
    from datetime import datetime
    WARNING_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(WARNING_FILE, "a") as fh:
        fh.write(f"{datetime.now().isoformat()} | {message}\n")
    print(f"WARNING: {message}")


def stale_postings(con, hist_path, published, today=None):
    """{month: count} of mirror postings opened STALE_AFTER_DAYS or more ago
    that are not on the dataset."""
    from datetime import date, timedelta
    cutoff = ((today or date.today()) -
              timedelta(days=STALE_AFTER_DAYS)).isoformat()
    rows = con.execute(f"""
        SELECT usajobsControlNumber::varchar, substr(positionOpenDate, 1, 7)
        FROM read_parquet('{hist_path}')
        WHERE positionOpenDate IS NOT NULL
          AND substr(positionOpenDate, 1, 10) <= '{cutoff}'
    """).fetchall()
    out = {}
    for cn, month in rows:
        if cn not in published:
            out[month] = out.get(month, 0) + 1
    return out


def check_freshness(con, hist_path, published):
    stale = stale_postings(con, hist_path, published)
    total = sum(stale.values())
    print(f"{total:,} postings opened {STALE_AFTER_DAYS}+ days ago are not on "
          f"the dataset"
          + (": " + ", ".join(f"{m} ({n:,})" for m, n in sorted(stale.items()))
             if stale else ""))
    if total > STALE_TOLERANCE:
        warn(f"{total:,} postings the mirror has had for {STALE_AFTER_DAYS}+ "
             f"days are not on HuggingFace (tolerance {STALE_TOLERANCE}): "
             + ", ".join(f"{m}: {n:,}" for m, n in sorted(stale.items()))
             + ". Either the publish is refusing months or the gap-fill fetch "
               "is not reaching them.")


# Fewer already-published rows than this in the months being rebuilt -- early
# in a month -- and the previous month's file joins the baseline.
MIN_BASELINE_ROWS = 3000


def check_section_shape(con, files, cns, repo, token, months):
    """Warn if the postings this run adds open or close differently from the
    ones published just before them -- the sign of a markup change leaking
    page chrome into a section. See section_shape.py.

    Warns rather than refuses: the text is still the page's, and holding it
    back would stall the publish over a defect that can be repaired later.
    """
    from section_shape import SECTIONS, drift, profile
    if not files or not cns:
        return
    con.execute("CREATE OR REPLACE TEMP TABLE fresh (cn VARCHAR)")
    con.executemany("INSERT INTO fresh VALUES (?)", [(c,) for c in cns])
    paths = ", ".join(f"'{f}'" for f in files)
    cols = ", ".join(SECTIONS)
    new = con.execute(f"""
        SELECT {cols} FROM read_parquet([{paths}])
        WHERE usajobsControlNumber IN (SELECT cn FROM fresh)""").df()
    old = con.execute(f"""
        SELECT {cols} FROM read_parquet([{paths}])
        WHERE usajobsControlNumber NOT IN (SELECT cn FROM fresh)""").df()
    if len(old) < MIN_BASELINE_ROWS:
        y, m = map(int, min(months).split("-"))
        previous = f"{y - (m == 1)}-{12 if m == 1 else m - 1:02d}"
        try:
            prior = download_month(repo, previous, token)
        except Exception as e:
            print(f"Section shape: could not read {previous} for a baseline "
                  f"({e})")
            prior = None
        if prior:
            import pandas as pd
            old = pd.concat([old, con.execute(
                f"SELECT {cols} FROM read_parquet('{prior}')").df()])
    if len(old) < MIN_BASELINE_ROWS:
        print(f"Section shape: only {len(old):,} published rows to compare "
              f"against; not checked")
        return
    found = drift(profile({s: new[s].tolist() for s in SECTIONS}),
                  profile({s: old[s].tolist() for s in SECTIONS}))
    print(f"Section shape over {len(new):,} new postings against "
          f"{len(old):,} published: "
          f"{'as expected' if not found else f'{len(found)} change(s)'}")
    for line in found:
        warn(f"Section shape changed, probably usajobs.gov markup: {line}")


def main() -> int:
    args = parse_args()
    data_dir = Path(args.data_dir)
    hist = data_dir / f"historical_jobs_{args.year}.parquet"
    scraped = data_dir / f"scraped_jobs_{args.year}.parquet"

    if not hist.exists():
        warn(f"Missing {hist} — nothing published")
        return 0
    # The scraped file is optional for a deliberate one-month republish: its
    # text may have been pruned after publishing, or never fetched on this
    # machine at all, in which case the dataset's own copy supplies it.
    republishing = bool(args.month and args.refresh_all)
    if not scraped.exists() and not republishing:
        warn(f"Missing {scraped} — nothing published")
        return 0

    # HF_TOKEN in CI; the cached login from `huggingface-cli login` locally.
    from huggingface_hub import get_token
    token = os.environ.get("HF_TOKEN") or get_token()
    if not token and not args.dry_run:
        warn("HF_TOKEN is not set — nothing published")
        return 0

    con = connection()
    month_of = month_of_every_posting(con, str(hist))
    on_dataset = dataset_months(args.repo, token)
    have = published_control_numbers(args.repo, token)
    print(f"Manifest lists {len(have):,} announcements; "
          f"{len(on_dataset)} month files on the dataset")

    if args.locations_file:
        import pyarrow.parquet as pq
        t = pq.read_table(args.locations_file)
        located = dict(zip(t.column("usajobsControlNumber").to_pylist(),
                           t.column("month").to_pylist()))
        print(f"Location map from {args.locations_file}")
    else:
        located = None if args.rebuild_locations else \
            load_locations(args.repo, token)
    had_locations = located is not None
    if not had_locations:
        print(f"Scanning all {len(on_dataset)} month files for where each "
              f"posting lives")
        located = choose_locations(
            scan_locations(args.repo, on_dataset, token), month_of)
        print(f"  {len(located):,} postings located")
        # The dataset as scanned, before this run changes anything -- the
        # input --locations-file wants. The locations.parquet written below
        # already includes this run's builds.
        BUILD_DIR.mkdir(parents=True, exist_ok=True)
        write_locations(located, BUILD_DIR / "locations.scanned.parquet")
    nowhere = have - set(located)
    if nowhere:
        # Reported, not fixed here: they drop out of the manifest below, which
        # is what lets the gap-fill fetch see them as missing and bring them
        # back.
        print(f"  {len(nowhere):,} postings the manifest lists are in no "
              f"month file")

    by_month = (local_months(con, str(hist), str(scraped))
                if scraped.exists() else {})
    print(f"Local text for {sum(len(v) for v in by_month.values()):,} "
          f"postings; {sum(1 for v in by_month.values() for c in v if c not in located):,} "
          f"are in no month file")

    if args.month and args.refresh_all:
        todo = [args.month]
        print(f"Republishing {args.month}")
    elif args.refresh_all:
        todo = sorted({m for m in on_dataset if m[:4] == str(args.year)}
                      | set(months_to_rebuild(located, by_month)))
        print(f"Refreshing all {len(todo)} month(s) of {args.year}")
    else:
        todo = months_to_rebuild(located, by_month)
        if args.month:
            todo = [m for m in todo if m == args.month]
        print(f"{len(todo)} month(s) have new rows: {', '.join(todo) or '(none)'}")

    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    copy_opts = "(FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 12)"
    written, refused = {}, []
    fresh, fresh_files = [], []
    for month in todo:
        try:
            prior = download_month(args.repo, month, token) \
                if month in on_dataset else None
        except Exception as e:
            refused.append((month, f"could not download its current file ({e})"))
            continue
        prior_cns = set()
        moved_out = set()
        if prior:
            prior_cns = {r[0] for r in con.execute(
                f"SELECT usajobsControlNumber::varchar "
                f"FROM read_parquet('{prior}')").fetchall()}
            located, stale, moved_out = reconcile_with_prior(
                month, located, prior_cns, on_dataset)
            if stale:
                print(f"  {month}: {len(stale):,} postings mapped here are not "
                      f"in the file")

        members = month_members(month, located, by_month)
        if not members:
            continue
        print(f"  {month}: {len(prior_cns):,} already published, "
              f"{len(members - prior_cns):,} new, "
              f"{len(moved_out):,} duplicates of other months dropped")

        # The members, with the open-date day the slicing works on. A member
        # the year's mirror lacks gets day '01'; any value works as long as
        # it is inside the ranges day_partitions hands back.
        con.execute("CREATE OR REPLACE TEMP TABLE members (cn VARCHAR)")
        con.executemany("INSERT INTO members VALUES (?)",
                        [(cn,) for cn in sorted(members)])
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE members AS
            SELECT m.cn, coalesce(any_value(substr(h.positionOpenDate, 9, 2)),
                                  '01') AS d
            FROM members m
            LEFT JOIN read_parquet('{hist}') h
              ON h.usajobsControlNumber::varchar = m.cn
            GROUP BY m.cn
        """)

        # If the local scrape covers everything the published file holds, and
        # the mirror has metadata for all of it, the prior adds nothing and
        # only costs memory. Reading it anyway meant materialising the month's
        # announcement text twice -- ~1.2 GB each -- which is what drove the
        # publish into constant spilling and took a month from minutes to
        # nearly an hour.
        source = prior
        keep = prior_cns - moved_out
        if prior and keep <= by_month.get(month, set()) and \
                keep <= set(month_of):
            print(f"    local copy covers all of it; skipping the "
                  f"published text")
            source = None

        name = month_file(month)
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
        #
        # Big months are built in day slices and stitched. Streaming COPY still
        # has to hold the join, and the text is 35 KB a row: 2022-03 was 41,871
        # rows and took a 2.7 GiB duckdb limit out in one pass.
        local = str(scraped) if scraped.exists() else None
        slices = day_partitions(con)
        if slices:
            print(f"    {len(slices)} slices "
                  + ", ".join(f"{lo}-{hi}" for lo, hi in slices))
            parts = []
            try:
                for i, days in enumerate(slices, 1):
                    part = dest.with_suffix(f".part{i:02d}.parquet")
                    con.execute(
                        f"COPY ({select_sql(con, str(hist), local, source, days)}) "
                        f"TO '{part}' {copy_opts};")
                    parts.append(part)
                stitch(parts, dest)
            finally:
                for part in parts:
                    part.unlink(missing_ok=True)
        else:
            con.execute(
                f"COPY ({select_sql(con, str(hist), local, source)}) "
                f"TO '{dest}' {copy_opts};")
        check_types(dest)

        built = {r[0] for r in con.execute(
            f"SELECT usajobsControlNumber FROM read_parquet('{dest}')").fetchall()}
        rows = con.execute(
            f"SELECT count(*) FROM read_parquet('{dest}')").fetchone()[0]
        problem = verify_build(month, built, rows, members, prior_cns,
                               moved_out)
        if problem:
            refused.append((month, problem))
            dest.unlink()
            continue
        print(f"  {name}: {rows:,} rows, {dest.stat().st_size/1e6:.1f} MB")
        written[month] = built
        fresh.extend(built - prior_cns)
        fresh_files.append(str(dest))

    for month, why in refused:
        warn(f"REFUSED {month}: {why}. Nothing was published for it.")
    try:
        check_section_shape(con, fresh_files, fresh, args.repo, token,
                            list(written))
    except Exception as e:
        # A broken check must not cost a day's publish, nor pass silently.
        warn(f"Section shape check failed to run: {e!r}")

    # The map and the manifest follow the files actually written. A posting
    # moved out of a month as a duplicate is still located in its other file.
    for month, built in written.items():
        for cn in [c for c, m in located.items() if m == month]:
            if cn not in built:
                del located[cn]
        for cn in built:
            located[cn] = month
    manifest = sorted(located)
    added = len(set(manifest) - have)
    dropped = len(have - set(manifest))
    (BUILD_DIR / "manifest.csv").write_text(
        "usajobsControlNumber\n" + "\n".join(manifest) + "\n")
    write_locations(located, BUILD_DIR / LOCATIONS_FILE)

    # The manifest only shrinks by postings it listed that no file holds. A
    # large drop means the location map is wrong, not that the dataset lost
    # rows, and publishing it would make every consumer of the manifest think
    # thousands of postings need fetching again.
    if dropped > max(1000, len(have) // 1000):
        warn(f"the new manifest would drop {dropped:,} of {len(have):,} "
             f"listed postings; not publishing anything")
        return 1

    if args.dry_run:
        print(f"\nDry run — {len(written)} month file(s) built in {BUILD_DIR}, "
              f"manifest would hold {len(manifest):,} (+{added:,}, "
              f"-{dropped:,})")
        check_freshness(con, str(hist), set(manifest))
        return EXIT_REFUSED if refused else 0

    if written or dropped or not had_locations:
        # One commit for everything. File-by-file would mean a commit per
        # month plus the manifest, leaving the dataset briefly inconsistent.
        from huggingface_hub import CommitOperationAdd, HfApi
        api = HfApi(token=token)
        ops = [CommitOperationAdd(path_in_repo=month_file(m),
                                  path_or_fileobj=str(BUILD_DIR / month_file(m)))
               for m in written]
        ops += [CommitOperationAdd(path_in_repo=n,
                                   path_or_fileobj=str(BUILD_DIR / n))
                for n in ("manifest.csv", LOCATIONS_FILE)]
        message = f"+{added:,} announcements, {len(manifest):,} total"
        if dropped:
            message += f" ({dropped:,} listed but in no file removed)"
        api.create_commit(repo_id=args.repo, repo_type="dataset",
                          operations=ops, commit_message=message)
        print(f"\nPushed {len(written)} month(s) to {args.repo}: {message}")

    # Nothing to prune when the text never was local -- a republish takes it
    # from the dataset, so there is no file here to shrink.
    #
    # Everything with local text that is now in a month file, not just what
    # this run wrote: a posting that already lives in another month's file
    # keeps its local text otherwise, forever.
    published_now = {cn for cns in by_month.values() for cn in cns
                     if cn in located}
    if not args.keep_text and scraped.exists() and published_now:
        cleared = prune_published_text(str(scraped), published_now)
        after = os.path.getsize(scraped) / 1e6
        print(f"Dropped local text for {cleared:,} published postings; "
              f"{scraped.name} is now {after:,.0f} MB")

    check_freshness(con, str(hist), set(manifest))
    return EXIT_REFUSED if refused else 0


if __name__ == "__main__":
    sys.exit(main())
