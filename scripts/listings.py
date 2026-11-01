#!/usr/bin/env python3
"""One row per control number, holding the posting's latest listing.

USAJOBS re-opens postings under the same control number with new open and close
dates, and the Historical API keeps only the latest listing. Before 2026-11-01
the mirror handled that two ways, both wrong:

  * Re-opened into another year: the collector filed the new listing under its
    new open date's year and nothing retired the old one, so the control number
    sat in two (sometimes three) year files. 289 did, every one with a
    different open date in each file, and 262 of the 308 left-behind copies
    still said "Accepting applications" or "Applications under review" --
    e.g. 547366900, open since 2024-08-18 in the 2024 file, re-listed
    2025-08-04 in the 2025 one.
  * Extended or re-opened within a year: repoll_status.py overwrote the status
    and nothing else, so a row carried the new listing's status beside the old
    listing's dates -- 835811200 said it closed 2026-06-30 while the API said
    2026-10-30.

The rule now, everywhere a historical row is written: the latest listing wins
the row, and every earlier listing's dates, status, announcement number and
title are kept in a `priorListings` JSON column on it, oldest first. "Latest"
is the later open date; the API has no listing sequence number, and a re-open
never moves an open date backwards.

collapse_across_years() applies the rule across the year files. It is
idempotent and cheap (it reads two columns per file unless something is
duplicated), and is run at the end of every writer -- collect_data.py and
repoll_status.py -- rather than being trusted to each of them.
"""

import json
import os
import re
from typing import Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

PRIOR_COL = "priorListings"

# What a listing is, for the record of earlier ones. Title and announcement
# number are kept because the API does occasionally hand a control number to a
# different announcement (reconcile_status.py calls those "reused"); keeping
# them means collapsing never hides that it happened.
PRIOR_FIELDS = ("positionOpenDate", "positionCloseDate", "positionOpeningStatus",
                "announcementNumber", "positionTitle")

HISTORICAL = "historical_jobs_"
SCRAPED = "scraped_jobs_"


def _day(value) -> Optional[str]:
    if value is None:
        return None
    s = str(value)
    return s[:10] if s and s.lower() not in ("nan", "nat", "none") else None


def listing_of(row: Dict) -> Dict:
    """The fields of one listing, as stored in priorListings."""
    out = {}
    for f in PRIOR_FIELDS:
        v = row.get(f)
        if f in ("positionOpenDate", "positionCloseDate"):
            v = _day(v)
        elif v is not None and not isinstance(v, str):
            # pandas hands back NaN for a missing string
            v = None if v != v else str(v)
        out[f] = v
    return out


def _parse(prior) -> List[Dict]:
    if prior is None:
        return []
    if isinstance(prior, float) and prior != prior:
        return []
    if isinstance(prior, list):
        return list(prior)
    try:
        parsed = json.loads(prior)
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def merge_prior(*priors, add: Optional[List[Dict]] = None,
                current_open: Optional[str] = None) -> Optional[str]:
    """Combine priorListings values and extra listings into one JSON string.

    Drops exact repeats and any entry for the current listing's own open date
    (a row is not its own prior listing), and sorts oldest first. None when
    nothing is left, so rows that were never re-opened stay NULL.
    """
    seen, out = set(), []
    for entry in [e for p in priors for e in _parse(p)] + list(add or []):
        if not isinstance(entry, dict):
            continue
        if current_open and _day(entry.get("positionOpenDate")) == current_open:
            continue
        key = json.dumps(entry, sort_keys=True)
        if key not in seen:
            seen.add(key)
            out.append(entry)
    if not out:
        return None
    out.sort(key=lambda e: (e.get("positionOpenDate") or "",
                            e.get("positionCloseDate") or ""))
    return json.dumps(out)


def carry_listing(old: Dict, new: Dict) -> Optional[str]:
    """priorListings for `new` replacing `old` for the same control number.

    Carries old's priorListings forward, and adds old itself as a prior listing
    when the open date changed. An extension -- same open date, later close
    date -- is the same listing, so it is updated in place and not recorded.
    """
    new_open = _day(new.get("positionOpenDate"))
    old_open = _day(old.get("positionOpenDate"))
    add = [listing_of(old)] if old_open and new_open and old_open != new_open else []
    return merge_prior(old.get(PRIOR_COL), new.get(PRIOR_COL), add=add,
                       current_open=new_open)


def year_files(data_dir: str, prefix: str = HISTORICAL) -> Dict[int, str]:
    """{year: path} for every year file with this prefix, backups excluded."""
    pattern = re.compile(rf"^{re.escape(prefix)}(\d{{4}})\.parquet$")
    out = {}
    if not os.path.isdir(data_dir):
        return out
    for name in sorted(os.listdir(data_dir)):
        m = pattern.match(name)
        if m:
            out[int(m.group(1))] = os.path.join(data_dir, name)
    return out


def _id_column(names) -> Optional[str]:
    for col in ("usajobsControlNumber", "usajobs_control_number"):
        if col in names:
            return col
    return None


def _ids(table: pa.Table) -> pa.Array:
    """Control numbers as strings, coalescing the two spellings the files use
    (see collect_data._control_numbers for why both exist)."""
    cols = [c for c in ("usajobsControlNumber", "usajobs_control_number")
            if c in table.column_names]
    out = None
    for c in cols:
        col = table.column(c)
        if pa.types.is_floating(col.type):
            col = pc.cast(col, pa.int64())
        col = pc.cast(col, pa.string())
        out = col if out is None else pc.coalesce(out, col)
    return out.combine_chunks() if out is not None else pa.array([], pa.string())


def collapse_across_years(data_dir: str, log=print,
                          prefix: str = HISTORICAL) -> List[Dict]:
    """Leave each control number in exactly one year file: its latest listing's.

    prefix picks the file family: the historical mirror, or the scraped text
    (where a re-listed posting's page is fetched again and filed under its new
    open date's year; see collect_scraped_data.py).

    The copies in other files are deleted, and their listings recorded in the
    surviving row's priorListings. Returns one dict per deleted copy (the
    move), which is also what the caller logs. Files nobody's copy was removed
    from or added to are not rewritten.
    """
    files = year_files(data_dir, prefix)
    where: Dict[str, List[tuple]] = {}
    for year, path in files.items():
        names = pq.read_schema(path).names
        if not _id_column(names) or "positionOpenDate" not in names:
            continue
        t = pq.read_table(path, columns=[c for c in (
            "usajobsControlNumber", "usajobs_control_number", "positionOpenDate")
            if c in names])
        ids = _ids(t).to_pylist()
        opens = t.column("positionOpenDate").to_pylist()
        for cn, od in zip(ids, opens):
            if cn:
                where.setdefault(cn, []).append((_day(od) or "", year))

    dupes = {cn: locs for cn, locs in where.items() if len({y for _, y in locs}) > 1}
    if not dupes:
        return []

    # Winner: latest open date, then the later file. Everything else loses.
    winner = {cn: max(locs) for cn, locs in dupes.items()}
    lose_in: Dict[int, set] = {}
    for cn, locs in dupes.items():
        for od, y in locs:
            if y != winner[cn][1]:
                lose_in.setdefault(y, set()).add(cn)

    # Read the losing rows' listings before deleting them.
    lost_listings: Dict[str, List[Dict]] = {}
    lost_priors: Dict[str, List] = {}
    moves = []
    for year, cns in sorted(lose_in.items()):
        t = pq.read_table(files[year])
        ids = _ids(t)
        hit = pc.fill_null(pc.is_in(ids, value_set=pa.array(sorted(cns))), False)
        lost = t.filter(hit).to_pylist()
        for row, cn in zip(lost, ids.filter(hit).to_pylist()):
            lost_listings.setdefault(cn, []).append(listing_of(row))
            lost_priors.setdefault(cn, []).append(row.get(PRIOR_COL))
            moves.append({"control_number": cn,
                          "from_file": os.path.basename(files[year]),
                          "to_file": os.path.basename(files[winner[cn][1]]),
                          "from_open": _day(row.get("positionOpenDate")),
                          "to_open": winner[cn][0] or None,
                          "from_status": row.get("positionOpeningStatus")})
        _write(files[year], t.filter(pc.invert(hit)))

    # Record them on the winners.
    win_in: Dict[int, set] = {}
    for cn, (_, y) in winner.items():
        win_in.setdefault(y, set()).add(cn)
    for year, cns in sorted(win_in.items()):
        t = pq.read_table(files[year])
        ids = _ids(t).to_pylist()
        opens = t.column("positionOpenDate").to_pylist()
        current = (t.column(PRIOR_COL).to_pylist() if PRIOR_COL in t.column_names
                   else [None] * len(ids))
        updated = [
            merge_prior(prior, *lost_priors.get(cn, []), add=lost_listings.get(cn),
                        current_open=_day(od)) if cn in cns else prior
            for cn, od, prior in zip(ids, opens, current)]
        col = pa.array(updated, type=pa.string())
        if PRIOR_COL in t.column_names:
            t = t.set_column(t.column_names.index(PRIOR_COL), PRIOR_COL, col)
        else:
            t = t.append_column(PRIOR_COL, col)
        _write(files[year], t)

    log(f"Re-opened postings ({prefix}*): {len(dupes):,} control number(s) were in more "
        f"than one year file; kept each in its latest listing's year and "
        f"removed {len(moves):,} older cop(ies)")
    return moves


def _write(path: str, table: pa.Table):
    """Atomic rewrite, zstd like save_jobs_to_parquet."""
    tmp = path + ".tmp"
    pq.write_table(table, tmp, compression="zstd", compression_level=3)
    os.replace(tmp, path)


MOVES_GLOB = "listing_moves_*.csv"


def write_moves_csv(moves: List[Dict], log_dir: str, stamp: str,
                    tag: str = "") -> Optional[str]:
    """Record what collapse_across_years moved.

    Two readers: the workflow artifact, and update/test_data_integrity.py,
    which accepts a year file shrinking only by control numbers recorded here
    as moved out of it -- and only if each is in the file it moved to.
    """
    if not moves:
        return None
    import csv
    os.makedirs(log_dir, exist_ok=True)
    path = os.path.join(log_dir, f"listing_moves_{stamp}{tag}.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(moves[0].keys()))
        w.writeheader()
        w.writerows(moves)
    return path
