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
      --max-cpu 0 \
      --nice 0
  status=$?
  echo "--- $year exited $status"

  # The year's text is on HuggingFace now and pruned locally; the mirror copy
  # is the only thing worth reclaiming.
  rm -f "data/historical_jobs_${year}.parquet"
done

echo "=== all years done ==="
