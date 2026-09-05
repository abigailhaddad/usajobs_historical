#!/usr/bin/env bash
# Manage the worker box from the repo instead of the Hetzner console.
#
# The console is needed exactly once: make a project and an API token
# (Security > API tokens > Generate, read/write). Everything after that is
# here.
#
#   export HCLOUD_TOKEN=...            # or put it in deploy/.hcloud.env
#   ./deploy/server.sh create          # make the box and provision it
#   ./deploy/server.sh status          # what exists, and what it costs
#   ./deploy/server.sh logs            # follow the backfill
#   ./deploy/server.sh ssh             # shell on it
#   ./deploy/server.sh destroy         # stop paying for it
#
# Billing is hourly with a monthly cap, so a job that runs four days costs
# roughly a seventh of the monthly price. Destroy it when the work is done.

set -euo pipefail

NAME="${WORKER_NAME:-usajobs-worker}"
TYPE="${WORKER_TYPE:-cx32}"          # 4 vCPU / 8 GB — the compact step peaks ~1.6 GB
IMAGE="${WORKER_IMAGE:-ubuntu-24.04}"
LOCATION="${WORKER_LOCATION:-nbg1}"  # override if a location is out of stock
SSH_KEY_NAME="${WORKER_SSH_KEY:-}"   # name of the key as hcloud knows it

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$HERE/.hcloud.env" ] && . "$HERE/.hcloud.env"

need() { command -v "$1" >/dev/null || { echo "missing: $1"; exit 1; }; }

hc() {
  need hcloud
  : "${HCLOUD_TOKEN:?set HCLOUD_TOKEN, or put it in deploy/.hcloud.env}"
  hcloud "$@"
}

server_ip() { hc server ip "$NAME" 2>/dev/null; }

cmd_create() {
  if server_ip >/dev/null 2>&1; then
    echo "$NAME already exists at $(server_ip)"
  else
    if [ -z "$SSH_KEY_NAME" ]; then
      SSH_KEY_NAME=$(hc ssh-key list -o noheader -o columns=name | head -1)
      [ -n "$SSH_KEY_NAME" ] || {
        echo "No SSH key registered. Add one first:"
        echo "  hcloud ssh-key create --name laptop --public-key-from-file ~/.ssh/id_ed25519.pub"
        exit 1; }
      echo "using ssh key: $SSH_KEY_NAME"
    fi
    echo "==> creating $NAME ($TYPE, $IMAGE, $LOCATION)"
    hc server create --name "$NAME" --type "$TYPE" --image "$IMAGE" \
      --location "$LOCATION" --ssh-key "$SSH_KEY_NAME"
  fi
  cmd_provision
}

cmd_provision() {
  local ip; ip=$(server_ip)
  echo "==> waiting for ssh on $ip"
  for _ in $(seq 1 60); do
    ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=5 \
        "root@$ip" true 2>/dev/null && break
    sleep 5
  done
  echo "==> bootstrap"
  ssh "root@$ip" 'bash -s' < "$HERE/hetzner-bootstrap.sh"
  echo
  echo "Now put the HuggingFace token on the box, then install the job:"
  echo "  ssh root@$ip \"printf 'HF_TOKEN=%s\\n' YOUR_TOKEN > /etc/usajobs-backfill.env && chmod 600 /etc/usajobs-backfill.env\""
  echo "  ssh root@$ip 'bash -s' < $HERE/install-backfill.sh"
}

cmd_status() {
  hc server list -o columns=name,status,ipv4,type,location,created
  echo
  echo "Type $TYPE is billed hourly; 'destroy' stops the meter."
  local ip; ip=$(server_ip 2>/dev/null) || return 0
  ssh -o ConnectTimeout=5 "root@$ip" \
    'systemctl is-active usajobs-backfill 2>/dev/null || true' 2>/dev/null \
    | sed 's/^/backfill service: /'
}

cmd_logs() { ssh "root@$(server_ip)" 'journalctl -u usajobs-backfill -f -n 60'; }
cmd_ssh()  { ssh "root@$(server_ip)"; }

cmd_destroy() {
  local ip; ip=$(server_ip) || { echo "$NAME does not exist"; return 0; }
  read -r -p "Destroy $NAME ($ip)? Everything on it is lost. [y/N] " ok
  [ "$ok" = "y" ] || { echo "cancelled"; return 0; }
  hc server delete "$NAME"
}

case "${1:-}" in
  create)    cmd_create ;;
  provision) cmd_provision ;;
  status)    cmd_status ;;
  logs)      cmd_logs ;;
  ssh)       cmd_ssh ;;
  destroy)   cmd_destroy ;;
  *) sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' ; exit 1 ;;
esac
