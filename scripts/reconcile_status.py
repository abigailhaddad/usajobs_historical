#!/usr/bin/env python3
"""Check every non-final posting's status against the live Historical API.

repoll_status.py visits one year a day and skips the last 14 days, so a status
can sit stale for most of two weeks. This asks the API about every open date
that still holds a non-final row, all at once, and reports what differs.

Rows are matched on control number AND open date, never control number alone:
USAJOBS reuses control numbers, and it re-opens postings under new dates. Two
cases are left alone and listed instead of fixed:

  unmatched  the API has no record with that control number on that open date.
             Usually the posting was re-opened under new dates (the API keeps
             only the latest), so the row describes an earlier listing whose
             final status the API can no longer tell us.
  reused     the API has the control number on that date, but for a different
             announcement number. A different job; its status is not ours.

Reports only, unless --apply:

    python reconcile_status.py --years 2025 2026
    python reconcile_status.py --years 2025 2026 --apply
"""

import argparse
import csv
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from repoll_status import FINAL_STATUSES, fetch_all_for_date, log

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs")


def day(value):
    return str(value)[:10] if value else None


def nonfinal(status):
    return status is None or status not in FINAL_STATUSES


def open_dates(table):
    """Distinct open dates that still hold a non-final row."""
    cols = table.select(["positionOpenDate", "positionOpeningStatus"]).to_pydict()
    return sorted({day(d) for d, s in zip(cols["positionOpenDate"],
                                          cols["positionOpeningStatus"])
                   if d and nonfinal(s)})


def api_index(jobs):
    """{(control number, open date): (status, announcement number)}."""
    out = {}
    for j in jobs:
        out[(str(j["usajobsControlNumber"]), day(j.get("positionOpenDate")))] = (
            j.get("positionOpeningStatus"), j.get("announcementNumber"))
    return out


def reconcile(table, api, fetched_dates, elsewhere=frozenset()):
    """Compare a year's non-final rows with the API.

    api: api_index() over every fetched date. fetched_dates: the dates that were
    fetched without error; rows on other dates are skipped, not called unmatched.
    elsewhere: control numbers held in more than one row across the files.

    Returns (statuses, changes, unmatched, reused, skipped): the full status
    column (changed where the API differs), and one dict per row in each list.
    """
    d = table.select(["usajobsControlNumber", "positionOpenDate", "positionOpeningStatus",
                      "announcementNumber"]).to_pydict()
    statuses = list(d["positionOpeningStatus"])
    changes, unmatched, reused, skipped = [], [], [], 0
    for i, (cn, opened, status, ann) in enumerate(zip(
            d["usajobsControlNumber"], d["positionOpenDate"], statuses,
            d["announcementNumber"])):
        if not nonfinal(status):
            continue
        opened = day(opened)
        if opened not in fetched_dates:
            skipped += 1
            continue
        row = {"row": i, "usajobsControlNumber": cn, "positionOpenDate": opened,
               "announcementNumber": ann, "our_status": status}
        hit = api.get((str(cn), opened))
        if hit is None:
            row["control_number_in_other_rows"] = str(cn) in elsewhere
            unmatched.append(row)
            continue
        api_status, api_ann = hit
        if api_ann != ann:
            row["api_announcementNumber"] = api_ann
            reused.append(row)
        elif api_status and api_status != status:
            row["api_status"] = api_status
            changes.append(row)
            statuses[i] = api_status
    return statuses, changes, unmatched, reused, skipped


def repeated_control_numbers(data_dir):
    """Control numbers held by more than one row across the year files, as
    strings. Explains the unmatched rows: a posting re-opened under new dates
    shows up once per listing."""
    seen, repeated = set(), set()
    for name in sorted(os.listdir(data_dir)):
        if name.startswith("historical_jobs_") and name.endswith(".parquet") \
                and "backup" not in name:
            column = pq.read_table(os.path.join(data_dir, name),
                                   columns=["usajobsControlNumber"]).column(0)
            for cn in map(str, column.to_pylist()):
                (repeated if cn in seen else seen).add(cn)
    return repeated


def fetch_with_retries(date, attempts=3):
    """fetch_all_for_date retries the HTTP call itself; this covers the odd
    response that is not JSON, which on 2025-04-01 and 2025-06-23 beat it."""
    for attempt in range(1, attempts + 1):
        try:
            return fetch_all_for_date(date)
        except Exception:
            if attempt == attempts:
                raise
            time.sleep(10 * attempt)


def fetch_dates(dates, workers):
    """Fetch every date in parallel. Returns (api index, dates fetched, errors)."""
    api, ok, errors = {}, set(), {}
    start = time.time()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_with_retries, d): d for d in dates}
        for n, fut in enumerate(as_completed(futures), 1):
            date = futures[fut]
            try:
                api.update(api_index(fut.result()))
                ok.add(date)
            except Exception as e:
                errors[date] = repr(e)
                log(f"  ERROR {date}: {e!r}")
            if n % 25 == 0 or n == len(dates):
                rate = n / ((time.time() - start) / 60)
                log(f"  [{n}/{len(dates)}] dates, {rate:.0f}/min")
    return api, ok, errors


def write_csv(name, rows, year):
    if not rows:
        return
    os.makedirs(LOG_DIR, exist_ok=True)
    path = os.path.join(LOG_DIR, f"{name}_{year}.csv")
    fields = list(dict.fromkeys(k for r in rows for k in r))
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def apply_changes(path, table, statuses, changes):
    """Rewrite the file with only the status (and last_seen) of changed rows
    touched, so every other column keeps its type and value."""
    i = table.schema.get_field_index("positionOpeningStatus")
    table = table.set_column(i, "positionOpeningStatus", pa.array(statuses, type=pa.string()))
    if "last_seen" in table.schema.names and \
            pa.types.is_string(table.schema.field("last_seen").type):
        seen = table.column("last_seen").to_pylist()
        now = datetime.now().isoformat()
        for c in changes:
            seen[c["row"]] = now
        j = table.schema.get_field_index("last_seen")
        table = table.set_column(j, "last_seen", pa.array(seen, type=pa.string()))
    tmp = path + ".tmp"
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, path)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--years", nargs="+", type=int, required=True)
    p.add_argument("--apply", action="store_true", help="write the corrections")
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--data-dir", default=DATA_DIR)
    args = p.parse_args()

    elsewhere = repeated_control_numbers(args.data_dir)

    summary = []
    for year in args.years:
        path = os.path.join(args.data_dir, f"historical_jobs_{year}.parquet")
        table = pq.read_table(path)
        dates = open_dates(table)
        log(f"{year}: {table.num_rows:,} rows, {len(dates)} open dates with a non-final row")
        api, ok, errors = fetch_dates(dates, args.workers)
        statuses, changes, unmatched, reused, skipped = reconcile(table, api, ok, elsewhere)
        for name, rows in (("status_changes", changes), ("status_unmatched", unmatched),
                           ("status_reused_ids", reused)):
            write_csv(name, rows, year)
        transitions = {}
        for c in changes:
            k = f"{c['our_status']} -> {c['api_status']}"
            transitions[k] = transitions.get(k, 0) + 1
        summary.append(f"### {year}\n- {len(changes):,} statuses differ from the API"
                       f"{' (applied)' if args.apply else ' (not applied)'}\n"
                       + "".join(f"  - {k}: {v:,}\n" for k, v in sorted(transitions.items()))
                       + f"- {len(unmatched):,} non-final rows the API has no record of "
                         f"under their open date ({sum(r['control_number_in_other_rows'] for r in unmatched):,} "
                         f"of them share a control number with another row)\n"
                       f"- {len(reused):,} rows where the API has that control number and "
                       f"date for a different announcement (left alone)\n"
                       f"- {skipped:,} rows on {len(errors)} date(s) that failed to fetch\n")
        if errors:
            summary.append(f"- WARNING: not checked, after retries: {', '.join(sorted(errors))}\n")
        if args.apply and changes:
            apply_changes(path, table, statuses, changes)
            log(f"{year}: wrote {len(changes):,} corrections")

    text = "## Status reconciliation\n\n" + "\n".join(summary)
    print("\n" + text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
