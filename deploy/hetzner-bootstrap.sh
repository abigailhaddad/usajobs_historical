#!/usr/bin/env bash
# One-time setup for a Hetzner (or any Ubuntu 24.04) box that runs long jobs
# for this and other repos.
#
# Deliberately generic: it makes a worker user, a place for checkouts, and a
# per-repo virtualenv. Nothing here is specific to the USAJOBS backfill --
# that is deploy/usajobs-backfill.service, which is the pattern to copy for
# the next repo.
#
#   ssh root@YOUR_SERVER_IP 'bash -s' < deploy/hetzner-bootstrap.sh
#
# Idempotent. Safe to rerun.

set -euo pipefail

WORKER=worker
REPOS=/srv/repos

echo "==> packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq \
  python3 python3-venv python3-pip git tmux curl ca-certificates \
  ufw unattended-upgrades >/dev/null

echo "==> firewall (ssh only; nothing here listens)"
ufw --force reset >/dev/null
ufw default deny incoming >/dev/null
ufw default allow outgoing >/dev/null
ufw allow OpenSSH >/dev/null
ufw --force enable >/dev/null

echo "==> unattended security updates"
dpkg-reconfigure -f noninteractive unattended-upgrades >/dev/null 2>&1 || true

echo "==> worker user and $REPOS"
id -u "$WORKER" >/dev/null 2>&1 || useradd --create-home --shell /bin/bash "$WORKER"
mkdir -p "$REPOS"
chown -R "$WORKER:$WORKER" "$REPOS"

# Root's authorized_keys came from the Hetzner create form; give the worker the
# same access so you can ssh in as worker instead of root.
if [ -f /root/.ssh/authorized_keys ]; then
  install -d -m 700 -o "$WORKER" -g "$WORKER" "/home/$WORKER/.ssh"
  install -m 600 -o "$WORKER" -g "$WORKER" \
    /root/.ssh/authorized_keys "/home/$WORKER/.ssh/authorized_keys"
fi

echo
echo "Done. Next:"
echo "  1. Put your HuggingFace token on the box:"
echo "       printf 'HF_TOKEN=hf_xxx\\n' > /etc/usajobs-backfill.env"
echo "       chmod 600 /etc/usajobs-backfill.env"
echo "  2. bash -s < deploy/install-backfill.sh   (checkout, venv, systemd unit)"
