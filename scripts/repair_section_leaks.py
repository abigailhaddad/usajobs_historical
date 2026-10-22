#!/usr/bin/env python3
"""Repair the section text the parser stored before PR #668.

Three of the things that PR fixed can be undone from the stored text alone,
exactly:

1. Run-on sections. The walk from a Requirements <h3> stopped only at a bare
   <h3>, but 'Education' and 'Additional information' each sit in a wrapper
   <div>, so the section before them ran on through both. Almost always that
   was qualificationSummary; when 'Additional information' comes before
   'Qualifications' on the page it was conditionsOfEmployment. The run-on is
   the wrapper's text, which is the heading followed by the field it holds:

       qualificationSummary = <qualifications> + " Education " + education
                                + " Additional information " + additionalInformation

   so it is cut only when the value ends with exactly that, built from the
   row's own education and additionalInformation. Anything else is left
   alone and counted.

   Where agency HTML is broken badly enough that the later headings end up
   nested in a <p> (429653100, 2016), conditionsOfEmployment ran on through
   Qualifications as well, and the same exact-ending rule cuts it.

2. Button labels. "Expand Hide additional information", "Expand Hide how to
   apply" and "Expand Hide next steps" inside the sections, and "Learn more
   about this agency" ending jobSummary. "Learn more about this agency" also
   appears mid-text in a few majorDuties, where an agency wrote it, so only
   the jobSummary ending is removed.

3. The site's "Preview job questionnaire" block, a button-styled link with a
   fixed note under it, which some announcements have after the
   qualifications. Removed wherever the exact text appears.

Not repairable here: agency text inside links to help.usajobs.gov, which the
old parser deleted, and the space it put before punctuation after a link
("The Fair Chance Act ."). The first needs the page; the second cannot be told
apart from a space the agency typed.

Measured on 10 October 2026 postings against their live pages: repaired
qualificationSummary equals the new parser's, word for word, on 9; the tenth
was edited by the agency after it was scraped.

    python repair_section_leaks.py --local data/scraped_jobs_2026.parquet
    python repair_section_leaks.py --months 2026-09 2026-10 --dry-run
    python repair_section_leaks.py --all --dry-run
    python repair_section_leaks.py --all
"""

import argparse
import itertools
import os
import re
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

RUN_ON = ["qualificationSummary", "conditionsOfEmployment"]
LABELLED = ["requirements", "conditionsOfEmployment", "qualificationSummary",
            "additionalInformation", "howToApply"]
AFFECTED = ["jobSummary"] + LABELLED
# Read, never written: the run-on is checked against them.
WRAPPED = ["education", "additionalInformation"]

BUTTONS = re.compile(
    r" ?Expand Hide (?:additional information|how to apply|next steps)\b")
AGENCY_LINK = "Learn more about this agency"

# Month files are 79 MB at the median and 171 MB at most; this many repaired
# copies wait on disk for each commit.
BATCH = 4


# The site's "Preview job questionnaire" block: a button-styled link and a
# fixed note under it, after the qualifications on some announcements.
QUESTIONNAIRE = ("Preview job questionnaire Make sure your resume includes "
                 "detailed information to support your qualifications and "
                 "answers to the job questionnaire.")


def _run_on_tails(row, field):
    """Every way the wrapped sections could have trailed a run-on `field`,
    longest first. Built from the row as stored."""
    parts = []
    # Agency HTML broken badly enough nests the later headings in a <p>, and
    # conditionsOfEmployment then runs on through Qualifications too.
    if field == "conditionsOfEmployment" and row.get("qualificationSummary"):
        parts.append("Qualifications " + row["qualificationSummary"])
    if row.get("education"):
        parts.append("Education " + row["education"])
    if row.get("additionalInformation"):
        parts.append("Additional information " + row["additionalInformation"])
    tails = [" ".join(p) for n in range(len(parts), 0, -1)
             for p in itertools.permutations(parts, n)]
    return sorted(tails, key=len, reverse=True)


def _drop(value, text):
    return " ".join(value.replace(text, " ").split()) if text in value else value


def repair_row(row):
    """(repaired row, {fix: 1}) -- the row unchanged if nothing applied."""
    fixes = {}
    out = dict(row)

    for f in ("requirements", "conditionsOfEmployment", "qualificationSummary"):
        value = out.get(f)
        if value and QUESTIONNAIRE in value:
            out[f] = _drop(value, QUESTIONNAIRE) or None
            fixes[f"questionnaire_{f}"] = 1

    # Before the labels come out: the run-on holds additionalInformation as
    # stored, labels and all.
    for f in RUN_ON:
        value = out.get(f)
        if not value:
            continue
        for tail in _run_on_tails(row, f):
            # Equal when the section had no text of its own: the run-on is
            # all there is, and the page has no such section.
            if value == tail or value.endswith(" " + tail):
                out[f] = value[:-len(tail)].rstrip() or None
                fixes[f"run_on_{f}"] = 1
                break

    for f in LABELLED:
        value = out.get(f)
        if value and "Expand Hide" in value:
            fixed = BUTTONS.sub("", value).strip()
            if fixed != value:
                out[f] = fixed or None
                fixes[f"buttons_{f}"] = 1

    # Rows parsed after the fix can end with these words because the agency
    # wrote them. Every row the old parser stored carries "Expand Hide"
    # labels (3,258,010 of 3,258,011 on the dataset), so only those lose it.
    old_parse = any("Expand Hide" in (row.get(f) or "") for f in LABELLED)
    summary = out.get("jobSummary")
    if old_parse and summary and (summary == AGENCY_LINK
                    or summary.endswith(" " + AGENCY_LINK)):
        out["jobSummary"] = summary[:-len(AGENCY_LINK)].rstrip() or None
        fixes["agency_link_jobSummary"] = 1

    return (out, fixes) if fixes else (row, fixes)


def repair_table(table):
    """(repaired pyarrow table, {fix: rows}, rows changed)."""
    import pyarrow as pa
    present = [f for f in dict.fromkeys(AFFECTED + WRAPPED)
               if f in table.column_names]
    columns = {f: table.column(f).to_pylist() for f in present}
    counts, changed = {}, 0
    for i in range(table.num_rows):
        row = {f: columns[f][i] for f in present}
        fixed, fixes = repair_row(row)
        if not fixes:
            continue
        changed += 1
        for k in fixes:
            counts[k] = counts.get(k, 0) + 1
        for f in present:
            columns[f][i] = fixed[f]
    for f in present:
        if f not in AFFECTED:
            continue
        idx = table.column_names.index(f)
        field = table.schema.field(f)
        table = table.set_column(idx, field, pa.array(columns[f], field.type))
    return table, counts, changed


def leftovers(table):
    """Rows the repair could not account for, by kind."""
    import pyarrow.compute as pc
    out = {}
    for f in LABELLED:
        if f in table.column_names:
            out[f"expand_hide_{f}"] = pc.sum(pc.match_substring(
                table.column(f), "Expand Hide")).as_py() or 0
    # Not counted: a jobSummary still ending "Learn more about this agency".
    # Some agencies end their summary with those words, before the button;
    # the live pages checked (495604800, 495949500, 495960200) confirm it.
    for f in ("requirements", "conditionsOfEmployment", "qualificationSummary"):
        if f in table.column_names:
            out[f"questionnaire_{f}"] = pc.sum(pc.match_substring(
                table.column(f), "Preview job questionnaire")).as_py() or 0
    # A run-on left behind still holds the wrapper's heading followed by the
    # start of the field it wraps.
    q = table.column("qualificationSummary").to_pylist()
    coe = table.column("conditionsOfEmployment").to_pylist()
    out["run_on_qualificationSummary_in_conditionsOfEmployment"] = sum(
        1 for a, b in zip(coe, q)
        if a and b and ("Qualifications " + b[:150]) in a)
    for f, heading in (("additionalInformation", "Additional information "),
                       ("education", "Education ")):
        vals = table.column(f).to_pylist()
        out[f"run_on_{f}_in_qualificationSummary"] = sum(
            1 for a, b in zip(q, vals)
            if a and b and (heading + b[:150]) in a)
    return out


def verify(before, after):
    """Same rows in the same order, and nothing outside AFFECTED touched."""
    if before.num_rows != after.num_rows or before.schema != after.schema:
        raise SystemExit("repair changed the row count or the schema")
    for name in before.column_names:
        if name not in AFFECTED and not before.column(name).equals(
                after.column(name)):
            raise SystemExit(f"repair changed {name}, which it must not touch")


def report(label, n, counts, changed, left):
    print(f"{label}: repaired {changed:,} of {n:,} rows; "
          + ", ".join(f"{k}={v:,}" for k, v in sorted(counts.items())))
    remaining = {k: v for k, v in left.items() if v}
    print(f"{label}: left after repair: {remaining or 'none'}")


def repair_local(path, dry_run):
    import pyarrow.parquet as pq
    before = pq.read_table(path)
    after, counts, changed = repair_table(before)
    verify(before, after)
    report(path, before.num_rows, counts, changed, leftovers(after))
    if changed and not dry_run:
        tmp = path + ".tmp"
        pq.write_table(after, tmp, compression="zstd")
        os.replace(tmp, path)


def all_months(repo, token):
    from huggingface_hub import HfApi
    files = HfApi(token=token).list_repo_files(repo, repo_type="dataset")
    return sorted(f[len("data/"):-len(".parquet")].replace("_", "-")
                  for f in files
                  if f.startswith("data/") and f.endswith(".parquet"))


def repair_months(months, repo, token, dry_run):
    """Repair month files, committing every BATCH of them so that only a
    batch's worth of copies is ever on disk."""
    import pyarrow.parquet as pq
    from huggingface_hub import CommitOperationAdd, HfApi, hf_hub_download
    from publish_to_huggingface import (BUILD_DIR, DOWNLOAD_DIR, check_types,
                                        month_file)

    api = HfApi(token=token)
    repaired_dir = BUILD_DIR / "repair"
    totals, rows, files = {}, 0, 0
    ops = []
    # The daily publish writes month files too. Each batch is read at one
    # revision and committed on top of exactly that revision, so a publish
    # landing in between makes the commit fail instead of being overwritten.
    revision = None

    def commit():
        nonlocal ops, revision
        if ops and not dry_run:
            info = api.create_commit(
                repo_id=repo, repo_type="dataset", operations=ops,
                parent_commit=revision,
                commit_message="Repair run-on qualifications and button "
                               "labels in " + ", ".join(
                                   o.path_in_repo for o in ops))
            print(f"Pushed {len(ops)} repaired month file(s): "
                  f"{info.oid}")
        ops = []
        revision = None
        shutil.rmtree(repaired_dir, ignore_errors=True)

    for month in months:
        name = month_file(month)
        if revision is None:
            revision = api.dataset_info(repo).sha
        src = hf_hub_download(repo, name, repo_type="dataset", token=token,
                              revision=revision, cache_dir=DOWNLOAD_DIR)
        before = pq.read_table(src)
        after, counts, changed = repair_table(before)
        verify(before, after)
        report(month, before.num_rows, counts, changed, leftovers(after))
        for k, v in counts.items():
            totals[k] = totals.get(k, 0) + v
        rows += changed
        del before
        shutil.rmtree(DOWNLOAD_DIR, ignore_errors=True)
        if changed:
            files += 1
            if not dry_run:
                dest = repaired_dir / name
                dest.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(after, dest, compression="zstd",
                               compression_level=12)
                check_types(dest)
                ops.append(CommitOperationAdd(path_in_repo=name,
                                              path_or_fileobj=str(dest)))
        del after
        if len(ops) >= BATCH:
            commit()
    commit()

    print(f"{'Dry run: ' if dry_run else ''}{rows:,} rows in {files} file(s) "
          f"{'would be ' if dry_run else ''}repaired; "
          + ", ".join(f"{k}={v:,}" for k, v in sorted(totals.items())))


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--local", help="a scraped_jobs_{year}.parquet to repair "
                                   "in place")
    p.add_argument("--months", nargs="*", default=[],
                   help="month files on the dataset to repair (YYYY-MM)")
    p.add_argument("--all", action="store_true",
                   help="every month file on the dataset")
    p.add_argument("--repo", default=os.environ.get(
        "HF_DATASET_REPO", "abigailhaddad/usajobs-scraping"))
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    if args.local:
        repair_local(args.local, args.dry_run)
    if args.months or args.all:
        from huggingface_hub import get_token
        token = os.environ.get("HF_TOKEN") or get_token()
        months = all_months(args.repo, token) if args.all else args.months
        repair_months(months, args.repo, token, args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
