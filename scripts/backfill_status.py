#!/usr/bin/env python3
"""
Is the backfill actually making progress?

Written because `systemctl is-active` said "activating" for seven hours while
the run published nothing. The service was alive and restarting cleanly; it was
just failing, pruning nothing, and carrying more text into each attempt until
the OOM killer took it again. Liveness is not progress, and the only signal
that would have caught it was the one nobody was watching.

Three questions, in the order they go wrong:

  Is it publishing?   Months pushed recently. Zero over a window that should
                      hold several is the headline failure.
  Is it spiralling?   A failed publish leaves its text unpruned, so the next
                      attempt joins more rows than the last. "Local join has
                      N" climbing across attempts is the signature, and it
                      ends in the OOM killer every time.
  Is it dying?        OOM kills and restarts in the window.

Exit code is 0 when healthy and 1 when not, so it can drive an alert.

    python backfill_status.py                # last 6 hours
    python backfill_status.py --hours 24
"""

import argparse
import re
import subprocess
import sys

# A month is 20k-31k postings, so a single month's join sits in that range.
# Meaningfully above it means an earlier month's text was never pruned.
SPIRAL_ROWS = 40_000


def parse_args():
    p = argparse.ArgumentParser(description="Report backfill progress")
    p.add_argument("--hours", type=float, default=6.0,
                   help="Window to judge over (default 6)")
    p.add_argument("--unit", default="usajobs-backfill")
    return p.parse_args()


def journal(unit, hours):
    try:
        out = subprocess.run(
            ["journalctl", "-u", unit, "--no-pager", "-o", "cat",
             "--since", f"-{hours} hours"],
            capture_output=True, text=True, errors="replace", timeout=120)
    except (OSError, subprocess.SubprocessError) as e:
        sys.exit(f"could not read the journal: {e}")
    return out.stdout.splitlines()


def analyse(lines, hours):
    """Journal lines -> (numbers, problems). Split out so the verdict is
    testable without a systemd box to read from."""
    published = [l for l in lines if "Pushed 1 month" in l]
    ooms = [l for l in lines if "oom-kill" in l or "OOM killer" in l]
    starts = [l for l in lines if "Starting usajobs" in l or l.startswith("=== ")]
    joins = [int(m.group(1).replace(",", "")) for l in lines
             if (m := re.search(r"Local join has ([\d,]+)", l))]
    pages = [int(m.group(1).replace(",", "")) for l in lines
             if (m := re.search(r"Fetched ([\d,]+) pages", l))]

    stats = {"published": len(published), "pages": sum(pages),
             "months_fetched": len(pages), "ooms": len(ooms),
             "restarts": len(starts), "joins": joins}

    problems = []
    if not published:
        problems.append(
            f"nothing published in {hours:g}h — the service can be alive and "
            f"restarting cleanly while publishing nothing at all")
    # The latest join, not the largest. A spiral is a join that is big *now*;
    # the maximum over the window keeps alarming long after it has unwound,
    # which is how an alarm stops being worth reading.
    if joins and joins[-1] > SPIRAL_ROWS:
        problems.append(
            f"the last publish joined {joins[-1]:,} rows, above {SPIRAL_ROWS:,} "
            f"— a failed publish leaves its text unpruned and the next attempt "
            f"carries both months")
    if ooms:
        problems.append(f"{len(ooms)} OOM kill(s)")
    return stats, problems


def main() -> int:
    args = parse_args()
    lines = journal(args.unit, args.hours)
    if not lines:
        print(f"No journal entries in the last {args.hours:g}h — is the unit "
              f"named {args.unit}?")
        return 1

    stats, problems = analyse(lines, args.hours)
    print(f"Last {args.hours:g} hours")
    print(f"  months published   {stats['published']}")
    print(f"  pages fetched      {stats['pages']:,} across "
          f"{stats['months_fetched']} month(s)")
    print(f"  OOM kills          {stats['ooms']}")
    print(f"  run restarts       {stats['restarts']}")
    if stats["joins"]:
        trend = "" if len(stats["joins"]) < 2 else \
            f", was {stats['joins'][0]:,} at the start of the window"
        print(f"  publish join rows  {stats['joins'][-1]:,} now{trend}")

    if problems:
        print("\nNOT HEALTHY")
        for p in problems:
            print(f"  - {p}")
        return 1

    print("\nHealthy: publishing, no spiral, no OOM.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
