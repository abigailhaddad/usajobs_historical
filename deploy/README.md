# Running long jobs on a box instead of Actions

GitHub's free runners queue. On 2026-09-04 a 12-job backfill spent 4h33m
waiting and 5h working — just over half the wall clock was queue. The daily
pipeline is small and belongs in Actions; bulk backfills do not.

This directory manages a worker box from the repo. The Hetzner console is
needed exactly once, to make a project and an API token.

## Setup

**Once, in the console:** create a project, then Security → API tokens →
Generate with read/write. Put it in `deploy/.hcloud.env` (gitignored):

```
HCLOUD_TOKEN=...
```

**Once, on your laptop:** install the CLI (`brew install hcloud`) and register
your SSH key if you have not:

```bash
hcloud ssh-key create --name laptop --public-key-from-file ~/.ssh/id_ed25519.pub
```

**Then:**

```bash
./deploy/server.sh create      # create + bootstrap
ssh root@$IP "printf 'HF_TOKEN=%s\n' YOUR_TOKEN > /etc/usajobs-backfill.env"
ssh root@$IP 'bash -s' < deploy/install-backfill.sh
```

## Day to day

```bash
./deploy/server.sh status    # what exists
./deploy/server.sh logs      # follow the job
./deploy/server.sh ssh       # shell
./deploy/server.sh destroy   # stop paying
```

## Cost

`cx32` (4 vCPU, 8 GB) is around €7–8/month, **billed hourly**. The backlog is
about four days of work, so running it and destroying the box costs roughly a
euro or two. The monthly figure is a cap, not a commitment — check the exact
price in the create form, and destroy the server when the work is done.

8 GB rather than 4 because `compact()` concatenates a month's shards in pandas
and peaks near 1.6 GB.

## Rate

`run-backfill.sh` uses 6 concurrent fetches — about 8–9 pages/sec, so
2018–2025 (~2.9M pages) takes roughly four days. usajobs.gov has served
300k+ pages at this rate with zero failures and zero 404s. Raising
`BACKFILL_WORKERS` is a decision about load on their servers, not about the
box.

## Resumability

Nothing here needs babysitting. `--known-from-hf` asks the dataset what is
already published, so a restart continues rather than redoing work, and within
a month pages land in shards that fold in at the end. A crash or reboot costs
at most one month's partial fetch. The unit restarts on failure.

## Adding another repo

`hetzner-bootstrap.sh` is generic — a worker user, `/srv/repos`, a venv per
checkout. `install-backfill.sh` is the per-repo half and is meant to be copied:
change `REPO_URL`, the pip line, and the unit body.
