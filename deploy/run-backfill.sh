#!/usr/bin/env bash
# Walk the backlog year by year. Started by usajobs-backfill.service.
#
# Deliberately gentle: 6 concurrent fetches. usajobs.gov has served 300k+ pages
# at this rate with zero failures and zero 404s.
#
# The "~8-9 pages/sec, about four days" this used to claim was never true on
# this box, and neither was "going faster is a decision about load on their
# servers, not about the box". Parsing costs ~122 ms of CPU per page here and
# the GIL pinned the whole job to one core, so it ran at 3.7 pages/sec with
# three cores idle -- a box limit, and usajobs.gov was being asked for well
# under 6 concurrent requests because the threads queued on the GIL rather than
# on the network. Parsing now happens in worker processes (--parse-workers,
# default one per core past the first), measured 4.41 -> 9.67 pages/sec.
#
# With parsing off the critical path, BACKFILL_WORKERS finally is what the old
# comment said it was: the number of concurrent requests, and a decision about
# load on their servers. Lower it to slow the fetch rate down.
#
# Every step is resumable, so a crash or reboot costs at most one month's
# partial fetch: --known-from-hf asks the dataset what is already published,
# and within a month, pages land in shards that fold in at the end.

set -uo pipefail

YEARS="${BACKFILL_YEARS:-2018 2019 2020 2021 2022 2023 2024 2025}"
WORKERS="${BACKFILL_WORKERS:-6}"
# Processes that parse pages. Empty means "one per core past the first", which
# is 3 on a cx33. This is the throughput knob; WORKERS above is the request
# rate. Set to 1 to go back to parsing on the fetch threads.
PARSE_WORKERS="${BACKFILL_PARSE_WORKERS:--1}"
# A month is republished wholesale, so a year whose only outstanding work is a
# few stragglers costs several full month rebuilds -- and pays that again on
# every restart, before reaching any year with real work left. Deferred here
# and swept up by a final pass.
MIN_MONTH_WORK="${BACKFILL_MIN_MONTH_WORK:-200}"
REPO="${REPO_DIR:-/srv/repos/usajobs_historical}"
MIRROR="https://pub-317c58882ec04f329b63842c1eb65b0c.r2.dev/data"

cd "$REPO"
mkdir -p data logs

for year in $YEARS; do
  echo "=== $year ==="

  # Metadata only, a few tens of MB, and public -- no R2 credentials anywhere.
  if [ ! -s "data/historical_jobs_${year}.parquet" ]; then
    echo "--- fetching the historical mirror for $year"
    curl -sSf --retry 5 --retry-delay 10 \
      -o "data/historical_jobs_${year}.parquet" \
      "$MIRROR/historical_jobs_${year}.parquet" || {
        echo "!!! could not fetch the mirror for $year; skipping"; continue; }
  fi

  ./.venv/bin/python -u scripts/backfill_scraped_pages.py \
      --year "$year" \
      --known-from-hf \
      --workers "$WORKERS" \
      --min-month-work "$MIN_MONTH_WORK" \
      --parse-workers "$PARSE_WORKERS" \
      --max-cpu 0 \
      --nice 0
  status=$?
  echo "--- $year exited $status"

  # The year's text is on HuggingFace now and pruned locally; the mirror copy
  # is the only thing worth reclaiming.
  rm -f "data/historical_jobs_${year}.parquet"
done

# No sweep for the deferred months. Each holds a handful of postings -- about
# ten in total across 2018 and 2019 -- and publishing one rewrites and
# re-uploads a whole 28,000-row month file. Not worth it against 3.2 million.
# Run with BACKFILL_MIN_MONTH_WORK=0 if they are ever wanted.

echo "=== all years done ==="
