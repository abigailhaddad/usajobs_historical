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

`cx33` (4 vCPU, 8 GB) is €9.99/month or €0.016/hour, **billed hourly**. The backlog is
about four days of work, so running it and destroying the box costs roughly a
euro or two. The monthly figure is a cap, not a commitment — check the exact
price in the create form, and destroy the server when the work is done.

8 GB rather than 4 because `compact()` concatenates a month's shards in pandas
and peaks near 1.6 GB.

## Rate

`run-backfill.sh` uses 6 concurrent fetches and parses in worker processes.

The job is **CPU-bound, not network-bound** — the opposite of what this file
used to say. A page costs ~209 ms of CPU on a cx33 (~122 ms parsing, ~87 ms for
TLS, gzip and the shard write), and the GIL confined all of it to one core:
measured 91.3% of a single core with three idle and 3.7 pages/sec, against a
hard ceiling of 4.8 regardless of `BACKFILL_WORKERS`. The old "8–9 pages/sec,
four days" figure came from a developer laptop, which is 4.4x faster per core.

Parsing now runs in a process pool (`--parse-workers`, default one per core
past the first). Measured 4.41 → 9.67 pages/sec on the box.

lxml was tried and rejected: 129.5 ms/page against `html.parser`'s 113.4 here,
and it changes parse output.

With parsing off the critical path, `BACKFILL_WORKERS` is finally what it was
always described as — the number of concurrent requests, and so a decision
about load on usajobs.gov. Lower it to fetch more slowly.

## Resumability

Nothing here needs babysitting. `--known-from-hf` asks the dataset what is
already published, so a restart continues rather than redoing work, and within
a month pages land in shards that fold in at the end. A crash or reboot costs
at most one month's partial fetch. The unit restarts on failure.

## Adding another repo

`hetzner-bootstrap.sh` is generic — a worker user, `/srv/repos`, a venv per
checkout. `install-backfill.sh` is the per-repo half and is meant to be copied:
change `REPO_URL`, the pip line, and the unit body.
