#!/usr/bin/env bash
# Check out the repo, build its venv, install the systemd unit, start it.
#
# This is the per-repo half; hetzner-bootstrap.sh is the generic half. Copy
# this file as the template for the next repo you move onto the box: the only
# repo-specific parts are REPO_URL, the pip line, and the unit.
#
#   ssh root@YOUR_SERVER_IP 'bash -s' < deploy/install-backfill.sh
#
# Idempotent. Rerunning updates the checkout and restarts the job.

set -euo pipefail

WORKER=worker
REPOS=/srv/repos
NAME=usajobs_historical
REPO_URL=https://github.com/abigailhaddad/usajobs_historical
DIR="$REPOS/$NAME"

[ -s /etc/usajobs-backfill.env ] || {
  echo "Missing /etc/usajobs-backfill.env with HF_TOKEN=..."
  echo "  printf 'HF_TOKEN=hf_xxx\n' > /etc/usajobs-backfill.env"
  echo "  chmod 600 /etc/usajobs-backfill.env"
  exit 1; }
chmod 600 /etc/usajobs-backfill.env

echo "==> checkout"
if [ -d "$DIR/.git" ]; then
  sudo -u "$WORKER" git -C "$DIR" fetch --quiet origin
  sudo -u "$WORKER" git -C "$DIR" reset --hard --quiet origin/main
else
  sudo -u "$WORKER" git clone --quiet "$REPO_URL" "$DIR"
fi

echo "==> venv"
sudo -u "$WORKER" python3 -m venv "$DIR/.venv"
sudo -u "$WORKER" "$DIR/.venv/bin/pip" install --quiet --upgrade pip
sudo -u "$WORKER" "$DIR/.venv/bin/pip" install --quiet \
  requests beautifulsoup4 pandas pyarrow duckdb tqdm huggingface_hub python-dotenv

echo "==> systemd unit"
cat > /etc/systemd/system/usajobs-backfill.service <<UNIT
[Unit]
Description=USAJOBS announcement-page backfill
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=$WORKER
WorkingDirectory=$DIR
EnvironmentFile=/etc/usajobs-backfill.env
# duckdb spills past this rather than growing, but it still needs room to
# work: at 2GB it died at 1.8 GiB on an ordinary month publish that had been
# fine at 4. 3GB sits under MemoryHigh with space for pyarrow beside it.
Environment=DUCKDB_MEMORY_LIMIT=3GB
ExecStart=$DIR/deploy/run-backfill.sh
# Let the cgroup apply back-pressure and, at worst, kill just this service
# rather than letting the kernel pick a victim. Two OOM kills in six hours on
# 2026-09-07 were the kernel's doing at 7.7 GB; the publish never needed that
# much, it simply had no ceiling to push back against.
#
# 4G was too tight and turned those crashes into something quieter and worse.
# The fetch parent sat at 2.7 GB while the publish child needed 1.9, so the
# cgroup reclaimed without pause -- 96% full memory pressure -- and evicted
# the parquet pages the publish was mid-read on. It ran 2h36m for 2m27s of
# CPU. The parent now trims its heap before spawning the child (see
# release_memory in backfill_scraped_pages.py); this leaves headroom so a bad
# month throttles instead of grinding.
MemoryHigh=6G
MemoryMax=7G
# With 4G of swap on the box, reclaim can page out the parent's idle heap
# instead of evicting the file pages the child is actively reading. Capped so
# a runaway swaps a little and then dies rather than swapping the box flat.
MemorySwapMax=4G
# Every step is resumable, so restarting after a crash re-reads what is
# already published and continues rather than redoing work.
Restart=on-failure
RestartSec=120
TimeoutStartSec=infinity
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
# enable, then start --no-block. `enable --now` waits for ExecStart to finish,
# and this is a Type=oneshot unit whose ExecStart is the whole backfill --
# days. The install would sit there looking hung with the job running fine
# behind it, which is exactly what it did on 2026-09-09.
systemctl enable usajobs-backfill.service
systemctl start --no-block usajobs-backfill.service

echo
echo "Started. Watch it with:"
echo "  journalctl -u usajobs-backfill -f"
echo "or from your laptop:  ./deploy/server.sh logs"
