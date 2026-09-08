#!/usr/bin/env bash
# Walk the backlog year by year. Started by usajobs-backfill.service.
#
# Deliberately gentle: 6 concurrent fetches, which measured ~8-9 pages/sec, so
# 2018-2025 (~2.9M pages) takes about four days. usajobs.gov has served
# 300k+ pages at this rate with zero failures and zero 404s. Going faster is a
# decision about load on their servers, not about the box.
#
# Every step is resumable, so a crash or reboot costs at most one month's
# partial fetch: --known-from-hf asks the dataset what is already published,
# and within a month, pages land in shards that fold in at the end.

set -uo pipefail

YEARS="${BACKFILL_YEARS:-2018 2019 2020 2021 2022 2023 2024 2025}"
WORKERS="${BACKFILL_WORKERS:-6}"
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
