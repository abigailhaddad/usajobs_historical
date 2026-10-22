#!/usr/bin/env python3
"""Re-fetch the announcements repair_section_leaks.py could not repair.

After that repair, a few hundred rows on the dataset still hold a run-on:
qualificationSummary (or conditionsOfEmployment) still contains the
"Education ..." / "Additional information ..." / "Qualifications ..." text
that follows it on the page, but not as an exact ending, so the repair left
it. In February 2022, the one month checked in full against the live pages,
21 of 25 were the old parser storing a fragment of education instead of the
whole field, and 4 were agency text that merely looks like a run-on.

Either way the stored text cannot be fixed from itself. This finds those rows
with the same test repair_section_leaks.leftovers() counts with, fetches each
page again, and replaces every section field with what the current parser
reads from it. Everything else in the row stays as it is.

The page is fetched as it is today, so an agency's edits since the original
scrape come with it. A page that 404s, errors, or parses to no section text
at all is left alone and listed.

Rows with the same control number in R2's scraped_jobs files get the same
text, so a later rebuild of a month from those files does not bring the old
text back.

    python refetch_leftover_rows.py --all --dry-run --report refetch.csv
    python refetch_leftover_rows.py --all --scraped data/scraped_jobs_*.parquet
"""

import argparse
import csv
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from repair_section_leaks import BATCH, all_months  # noqa: E402

# Every field parse_sections() writes.
SECTION_FIELDS = ["jobSummary", "majorDuties", "howYouWillBeEvaluated",
                  "requiredDocuments", "howToApply", "benefits",
                  "requirements", "conditionsOfEmployment",
                  "qualificationSummary", "education",
                  "additionalInformation"]

JOB_URL = "https://www.usajobs.gov/job/{}"


def leftover_kinds(row):
    """The run-on kinds repair_section_leaks.leftovers() would count this row
    under. Same test, one row at a time."""
    q = row.get("qualificationSummary")
    coe = row.get("conditionsOfEmployment")
    kinds = []
    if coe and q and ("Qualifications " + q[:150]) in coe:
        kinds.append("quals_in_conditionsOfEmployment")
    for f, heading in (("additionalInformation", "Additional information "),
                       ("education", "Education ")):
        v = row.get(f)
        if q and v and (heading + v[:150]) in q:
            kinds.append(f"{f}_in_qualificationSummary")
    return kinds


def find_leftovers(table):
    """{row index: [kinds]} for every leftover row in a month table."""
    present = [f for f in ("qualificationSummary", "conditionsOfEmployment",
                           "education", "additionalInformation")
               if f in table.column_names]
    cols = {f: table.column(f).to_pylist() for f in present}
    out = {}
    for i in range(table.num_rows):
        kinds = leftover_kinds({f: cols[f][i] for f in present})
        if kinds:
            out[i] = kinds
    return out


def fetch_sections(session, cn):
    """(section fields, problem) for one announcement, read fresh."""
    from bs4 import BeautifulSoup
    from usajobs_scrape import fetch_job_page, parse_sections
    html, err = fetch_job_page(session, cn)
    if html is None:
        return None, "404" if err is None else f"error: {err[:80]}"
    sections = parse_sections(BeautifulSoup(html, "html.parser"))
    if not any(sections.get(f) for f in ("qualificationSummary",
                                         "requirements")):
        return None, "page parsed to no requirements text"
    return {f: sections.get(f) for f in SECTION_FIELDS}, None


def replace_rows(table, fresh):
    """The table with each row whose control number is in `fresh` given that
    page's section fields. -> (table, rows replaced)."""
    import pyarrow as pa
    cns = table.column("usajobsControlNumber").cast(pa.string()).to_pylist()
    hits = [i for i, cn in enumerate(cns) if cn in fresh]
    if not hits:
        return table, 0
    for f in SECTION_FIELDS:
        if f not in table.column_names:
            continue
        values = table.column(f).to_pylist()
        for i in hits:
            values[i] = fresh[cns[i]][f]
        idx = table.column_names.index(f)
        field = table.schema.field(f)
        table = table.set_column(idx, field, pa.array(values, field.type))
    return table, len(hits)


def verify(before, after):
    """Same rows in the same order; nothing outside the section fields
    touched."""
    if before.num_rows != after.num_rows or before.schema != after.schema:
        raise SystemExit("refetch changed the row count or the schema")
    for name in before.column_names:
        if name not in SECTION_FIELDS and not before.column(name).equals(
                after.column(name)):
            raise SystemExit(f"refetch changed {name}, which it must not")


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--months", nargs="*", default=[])
    p.add_argument("--all", action="store_true")
    p.add_argument("--scraped", nargs="*", default=[],
                   help="R2 scraped_jobs files to give the same text, in place")
    p.add_argument("--report", default="refetch_leftovers.csv",
                   help="one line per leftover row: before, after, outcome")
    p.add_argument("--repo", default=os.environ.get(
        "HF_DATASET_REPO", "abigailhaddad/usajobs-scraping"))
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    import pyarrow.parquet as pq
    from huggingface_hub import (CommitOperationAdd, HfApi, get_token,
                                 hf_hub_download)
    from publish_to_huggingface import (BUILD_DIR, DOWNLOAD_DIR, check_types,
                                        month_file)
    from usajobs_scrape import new_session

    token = os.environ.get("HF_TOKEN") or get_token()
    api = HfApi(token=token)
    months = all_months(args.repo, token) if args.all else args.months
    session = new_session()
    out_dir = BUILD_DIR / "refetch"
    fresh_all = {}
    report_rows = []
    ops, revision = [], None
    totals = {"leftover_rows": 0, "replaced": 0, "still_leftover": 0,
              "not_fetched": 0}

    def commit():
        nonlocal ops, revision
        if ops and not args.dry_run:
            info = api.create_commit(
                repo_id=args.repo, repo_type="dataset", operations=ops,
                parent_commit=revision,
                commit_message="Re-fetch run-on sections the text repair "
                               "could not cut in " + ", ".join(
                                   o.path_in_repo for o in ops))
            print(f"Pushed {len(ops)} month file(s): {info.oid}")
        ops, revision = [], None
        shutil.rmtree(out_dir, ignore_errors=True)

    for month in months:
        name = month_file(month)
        if revision is None:
            revision = api.dataset_info(args.repo).sha
        src = hf_hub_download(args.repo, name, repo_type="dataset",
                              token=token, revision=revision,
                              cache_dir=DOWNLOAD_DIR)
        before = pq.read_table(src)
        shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
        found = find_leftovers(before)
        if not found:
            continue
        cns = before.column("usajobsControlNumber").to_pylist()
        quals = before.column("qualificationSummary").to_pylist()
        coes = before.column("conditionsOfEmployment").to_pylist()
        fresh = {}
        for i, kinds in found.items():
            cn = str(cns[i])
            sections, problem = fresh_all.get(cn, (None, None))
            if cn not in fresh_all:
                sections, problem = fetch_sections(session, cn)
                fresh_all[cn] = (sections, problem)
            if sections:
                fresh[cn] = sections
                after_kinds = leftover_kinds(sections)
            report_rows.append({
                "month": month, "control_number": cn,
                "url": JOB_URL.format(cn), "kinds": " ".join(kinds),
                "outcome": problem or ("replaced, still looks like a run-on"
                                       if after_kinds else "replaced"),
                "old_qualificationSummary": quals[i],
                "new_qualificationSummary": (sections or {}).get(
                    "qualificationSummary"),
                "old_conditionsOfEmployment": coes[i],
                "new_conditionsOfEmployment": (sections or {}).get(
                    "conditionsOfEmployment"),
            })
            totals["leftover_rows"] += 1
            if problem:
                totals["not_fetched"] += 1
            elif after_kinds:
                totals["still_leftover"] += 1
        after, n = replace_rows(before, fresh)
        verify(before, after)
        totals["replaced"] += n
        print(f"{month}: {len(found)} leftover row(s), {n} replaced from "
              f"{len(fresh)} page(s)")
        if n and not args.dry_run:
            dest = out_dir / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(after, dest, compression="zstd",
                           compression_level=12)
            check_types(dest)
            ops.append(CommitOperationAdd(path_in_repo=name,
                                          path_or_fileobj=str(dest)))
        del before, after
        if len(ops) >= BATCH:
            commit()
    commit()

    fetched = {cn: s for cn, (s, _) in fresh_all.items() if s}
    for path in args.scraped:
        before = pq.read_table(path)
        after, n = replace_rows(before, fetched)
        verify(before, after)
        print(f"{path}: {n} row(s) given the re-fetched text")
        if n and not args.dry_run:
            pq.write_table(after, path + ".tmp", compression="zstd")
            os.replace(path + ".tmp", path)

    with open(args.report, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(report_rows[0]) if report_rows
                           else ["month"])
        w.writeheader()
        w.writerows(report_rows)

    print(f"{'Dry run: ' if args.dry_run else ''}"
          f"{totals['leftover_rows']:,} leftover rows "
          f"({len(fresh_all):,} distinct announcements); "
          f"{totals['replaced']:,} rows replaced, "
          f"{totals['still_leftover']:,} of them still look like a run-on "
          f"after re-fetching; {totals['not_fetched']:,} not fetched "
          f"(see {args.report})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
