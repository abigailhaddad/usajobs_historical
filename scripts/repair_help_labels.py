#!/usr/bin/env python3
"""Strip the "Help" button text that leaked into announcement sections.

In early September 2026 usajobs.gov moved its help pages from help.usajobs.gov
to usajobshelp.opm.gov. The parser removed help links by the old domain, so
from then on every page parsed with five sections carrying the button's label:
majorDuties, requirements, benefits, requiredDocuments and howToApply each
began "Help ", and benefits also picked up the sr-only "Opens in a new window"
and the text of its "Learn more about federal benefits" help link. The parser
is fixed; this repairs what was stored before the fix.

The repair is exactly the difference between the two parsers. Measured on 47
live pages: old-parser output with this repair applied equals new-parser
output on every section of every page.

A row is repaired only when all five sections start with "Help ". Genuine text
can start with the word -- "Help develop and guide short- and long-term
strategies" is a real July 2026 majorDuties -- but not in all five at once.

    python repair_help_labels.py --local data/scraped_jobs_2026.parquet
    python repair_help_labels.py --months 2026-06 2026-07 2026-08 2026-09 --dry-run
    python repair_help_labels.py --months 2026-06 2026-07 2026-08 2026-09
"""

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

AFFECTED = ["majorDuties", "requirements", "benefits", "requiredDocuments",
            "howToApply"]
LABEL = "Help "
BENEFITS_LINK = "Opens in a new window Learn more about federal benefits"


def _leaked(value) -> bool:
    # A section that held nothing but the button is the bare label: on some
    # pages benefits is empty apart from it, and the fixed parser leaves it
    # out altogether.
    return value.startswith(LABEL) or value == LABEL.strip()


def is_affected(row) -> bool:
    values = [row.get(f) for f in AFFECTED]
    return any(values) and all(_leaked(v) for v in values if v)


def repair_row(row):
    """The row with the leaked help text removed, or the row unchanged."""
    if not is_affected(row):
        return row
    row = dict(row)
    for f in AFFECTED:
        if row.get(f):
            row[f] = row[f][len(LABEL):] or None
    if row.get("benefits"):
        row["benefits"] = (row["benefits"]
                           .replace(BENEFITS_LINK + " ", "")
                           .replace(" " + BENEFITS_LINK, "")
                           .replace(BENEFITS_LINK, ""))
    return row


def repair_table(table):
    """(repaired pyarrow table, rows changed). Only AFFECTED columns move."""
    import pyarrow as pa
    present = [f for f in AFFECTED if f in table.column_names]
    columns = {f: table.column(f).to_pylist() for f in present}
    changed = 0
    for i in range(table.num_rows):
        row = {f: columns[f][i] for f in present}
        fixed = repair_row(row)
        if fixed is not row:
            changed += 1
            for f in present:
                columns[f][i] = fixed[f]
    for f in present:
        idx = table.column_names.index(f)
        table = table.set_column(idx, table.schema.field(f),
                                 pa.array(columns[f], table.schema.field(f).type))
    return table, changed


def verify(before, after):
    """Same rows in the same order, and nothing outside AFFECTED touched."""
    if before.num_rows != after.num_rows or before.schema != after.schema:
        raise SystemExit("repair changed the row count or the schema")
    for name in before.column_names:
        if name not in AFFECTED and not before.column(name).equals(after.column(name)):
            raise SystemExit(f"repair changed {name}, which it must not touch")


def repair_local(path):
    import pyarrow.parquet as pq
    before = pq.read_table(path)
    after, changed = repair_table(before)
    verify(before, after)
    if changed:
        tmp = path + ".tmp"
        pq.write_table(after, tmp, compression="zstd")
        os.replace(tmp, path)
    print(f"{path}: repaired {changed:,} of {before.num_rows:,} rows")


def repair_months(months, repo, token, dry_run):
    import pyarrow.parquet as pq
    from huggingface_hub import CommitOperationAdd, HfApi, hf_hub_download
    from publish_to_huggingface import BUILD_DIR, check_types, month_file

    ops, total = [], 0
    for month in months:
        name = month_file(month)
        src = hf_hub_download(repo, name, repo_type="dataset", token=token)
        before = pq.read_table(src)
        after, changed = repair_table(before)
        verify(before, after)
        print(f"{month}: repairing {changed:,} of {before.num_rows:,} rows")
        if not changed:
            continue
        dest = BUILD_DIR / "repair" / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(after, dest, compression="zstd", compression_level=12)
        check_types(dest)
        ops.append(CommitOperationAdd(path_in_repo=name,
                                      path_or_fileobj=str(dest)))
        total += changed

    if dry_run or not ops:
        print(f"{'Dry run — ' if dry_run else ''}{total:,} rows in "
              f"{len(ops)} file(s) would be repaired")
        return
    # One commit, so the dataset is never half-repaired.
    HfApi(token=token).create_commit(
        repo_id=repo, repo_type="dataset", operations=ops,
        commit_message=f"Strip leaked 'Help' button text from {total:,} "
                       f"announcements in {len(ops)} month(s)")
    print(f"Pushed {len(ops)} repaired month file(s)")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--local", help="a scraped_jobs_{year}.parquet to repair "
                                   "in place")
    p.add_argument("--months", nargs="*", default=[],
                   help="month files on the dataset to repair (YYYY-MM)")
    p.add_argument("--repo", default=os.environ.get(
        "HF_DATASET_REPO", "abigailhaddad/usajobs-scraping"))
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    if args.local:
        repair_local(args.local)
    if args.months:
        from huggingface_hub import get_token
        token = os.environ.get("HF_TOKEN") or get_token()
        repair_months(args.months, args.repo, token, args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
