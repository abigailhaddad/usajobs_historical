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
ExecStart=$DIR/deploy/run-backfill.sh
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
systemctl enable --now usajobs-backfill.service

echo
echo "Started. Watch it with:"
echo "  journalctl -u usajobs-backfill -f"
echo "or from your laptop:  ./deploy/server.sh logs"
