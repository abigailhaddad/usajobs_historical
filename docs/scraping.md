# Scraping usajobs.gov, and the announcement dataset

How the announcement text is collected and published. For what the dataset
holds and how to query it, see the
[README](../README.md#announcement-text-huggingface).

## Scraping usajobs.gov instead of the API (shadow run)

Since 2026-09-02 the daily pipeline also collects the same postings without an
API key, by scraping usajobs.gov. It writes `data/scraped_jobs_*.parquet`, which is
where `publish_to_huggingface.py` gets the announcement text. The other point is
to find out whether the site could replace the Current API if the key ever
stopped working.

Two keyless sources. `POST https://www.usajobs.gov/Search/ExecuteSearch` is the
JSON endpoint behind the search page — no auth, no cookie — and returns about
25 fields per posting plus a facet block. The facets are computed over the
whole result set rather than the 10,000 the `Total` field reports, so one call
gives the true open-inventory size and its breakdown by occupational series;
that's how the collection slices itself under the same 10,000 ceiling the API
has. Then `GET https://www.usajobs.gov/job/{control_number}` for postings we
haven't stored, which is server-rendered HTML where every overview field is a
`<dt>`/`<dd>` pair.

It also pulls the announcement body apart by the page's own headings, which the
API can't do at all: `jobSummary`, `majorDuties`, `requirements`,
`conditionsOfEmployment`, `qualificationSummary`, `education`,
`additionalInformation`, `benefits`, `howYouWillBeEvaluated`,
`requiredDocuments`, `howToApply`, plus the whole-page
`text` the [announcement dataset](https://huggingface.co/datasets/abigailhaddad/usajobs-scraping)
stores. `MatchedObjectDescriptor` drops content the page shows, and even the
page's own ld+json carries a truncated `qualifications` — 1,503 characters
against the page's 1,845 on the announcement the tests use. Across a 50-page
live sample every field but `education` was populated on every announcement,
and `education` is genuinely missing from about one in six.

`scripts/compare_scrape_to_api.py` diffs the two collections after each run and
writes a report to `logs/scrape_vs_api_<date>.md`, uploaded as a workflow
artifact. On the first full check — all 354 open 2210 postings, 2026-09-02 —
coverage was 354/354 and every comparable field matched exactly.
`COMPARED_FIELDS` in that script is the list, currently 20. The scrape also
carries `workSchedule` and `promotionPotential`, which the Current API
collection doesn't map at all. The text fields have no API
counterpart to check against, so the report tracks their fill rate instead: if
usajobs.gov changes its markup they go empty, and that's the signal.

Duty stations come out as a `PositionLocations` column in the historical API's
`{positionLocationCity, positionLocationState}` shape, so `prep_web_data.py`
consumes them through the path it already has. Rendered through that same
extractor, the location string matched the API exactly on 337 of 348 postings.
The other 11 are the page's own doing rather than a parse failure: a posting
with 31 API entries across 22 cities renders 12, and some collapse to a label
like "Location Negotiable After Selection". The search endpoint's own
`positionLocationCount` matched the API on all 348, so the accurate count is
kept alongside the page's list in `scrapedLocationCount` and the gap stays
measurable.

What scraping can't give you: `hiringAgencyCode`, `hiringDepartmentCode`,
`agencyLevel`, `agencyLevelSort`, `vendor`, `whoMayApply`. Those are API-only
and the announcement page never shows them. Nothing reads them off the current
parquets — `repoll_status.py` is the only consumer and it writes them into the
historical files, from the keyless `/api/historicjoa`. It also can't backfill — search
only lists open postings, so this accumulates forward from the day it started.
Closed announcement pages do stay up indefinitely, which is what the
[announcement dataset](https://huggingface.co/datasets/abigailhaddad/usajobs-scraping) relies on.

Coverage shortfalls and field disagreements past the thresholds in
`compare_scrape_to_api.py` open a GitHub issue. Neither the scrape nor the
comparison can fail the daily run.

## The HuggingFace announcement dataset

[abigailhaddad/usajobs-scraping](https://huggingface.co/datasets/abigailhaddad/usajobs-scraping)
is one parquet per month plus a manifest, published daily by
`scripts/publish_to_huggingface.py`. It joins the two halves this pipeline
already has on disk: structured fields from `historical_jobs_{year}.parquet`,
and announcement text from `scraped_jobs_{year}.parquet`. Nothing is fetched at
publish time.

This replaced a publish path that lived in a separate repo
([joa](https://github.com/abigailhaddad/joa)) and keyed off the historical
mirror alone. Two things were wrong with it. Its field list never selected
`positionTitle`, so the published dataset had no job title in it — easy to miss,
because its controls CSV aliased `announcementNumber` as "title". And it could
not carry the eleven structured announcement sections, which come from a parse
that lives here.

The dataset went from 40 columns to 53: `positionTitle` and
`hiringSubelementName`, plus `jobSummary`, `majorDuties`, `requirements`,
`conditionsOfEmployment`, `qualificationSummary`, `education`,
`additionalInformation`, `benefits`, `howYouWillBeEvaluated`,
`requiredDocuments` and `howToApply`. The whole-page `text` column stays, so
anything built against it keeps working — columns are only ever added.

A month file is rewritten wholesale, so the publisher refuses to write a month
when the local join is missing announcements the dataset already holds, rather
than silently shrinking it. That is the expected state while the page backfill
is still running.

```bash
python scripts/publish_to_huggingface.py --dry-run
python scripts/publish_to_huggingface.py
python scripts/publish_to_huggingface.py --refresh-all   # rewrite every month
```

### Querying it

DuckDB reads the month files over HTTP, so a query costs the columns it touches
rather than the 10.5 GB:

```sql
SELECT count(*)
FROM read_parquet('hf://datasets/abigailhaddad/usajobs-scraping/data/2025_*.parquet');
```

Go a year at a time. Globbing all 152 files at once gets an anonymous reader a
429 partway through.

The dataset viewer's filter and search are a different thing. HuggingFace
indexes a fixed slice of a dataset this size, and here that slice was 99,059
rows of about 3.2 million — counts off the viewer are roughly 3% of the data,
and the page does not say so. Use DuckDB for anything you plan to quote.

`notebooks/announcement_text_queries.ipynb` works through four questions the API cannot answer —
who needs a doctorate, where the structured fields and the prose disagree, direct hire authority,
and one query that looks great and means nothing.

Every column is published as a string except `agencyLevel` (BIGINT) and the two
salary columns (DOUBLE). That is enforced rather than assumed: duckdb types a
column of untyped NULLs as INT32, so a month where some field happens to be
entirely empty used to publish the wrong type for it, and `check_types()`
refuses to upload a month whose columns have drifted.

### What stays on disk

Announcement text is 97.8% of `scraped_jobs_{year}.parquet` — 5.42 KB of a
5.54 KB row — so a full year would be 920 MB local against 20 MB for
everything else in it. HuggingFace is the store for the text instead: after a
month publishes, the publisher blanks those rows' text columns locally, and
what stays is the structured shadow the API comparison reads plus anything not
yet pushed. Measured on real rows, that takes a 162,000-row year from 920 MB to
34 MB. `--keep-text` opts out.

### Backfilling announcement pages

The scraped collection only starts the day it was switched on, so postings from
earlier in the year have metadata but no text. usajobs.gov serves closed
announcements indefinitely, so they can be filled in — roughly 160k pages for a
full year.

It works a month at a time and publishes each one as it lands, so the working
set stays near a single month rather than piling up the year, and a killed run
resumes at month granularity. Within a month it writes immutable shards and
folds them in once at the end.

It is network-bound, not CPU-bound: a page costs ~32 ms to parse, so a full
year is about 86 CPU-minutes over those three hours. It runs niced and under a
CPU governor by default. `--max-cpu` is a share of **one** core, not of the
machine — measured, an uncapped run sits at 53% of a core, and `--max-cpu 15`
holds it to 18% at three times the wall clock.

```bash
python scripts/backfill_scraped_pages.py --year 2026 --dry-run
python scripts/backfill_scraped_pages.py --year 2026
python scripts/backfill_scraped_pages.py --year 2026 --max-cpu 20   # gentler
python scripts/backfill_scraped_pages.py --year 2026 --no-publish   # keep local
```

2026 ran locally in 175 minutes for 160k pages: zero failures, zero 404s, 68.8
CPU-minutes, and the local parquet ended at 15 MB because each month's text is
pruned once published.

### The 2013-2025 backfill (done)

Done as of 2026-09-11: every announcement the historical mirror knows about is
published except 25 that usajobs.gov will not serve — 24 that return 503 forever
and one 404. `scripts/audit_completeness.py` checks this against the manifest
without downloading the dataset, and exits non-zero if anything new goes
missing. The known-unreachable list is `scripts/unreachable_announcements.csv`.

The workflow that did it is still there for a future gap. The
**Backfill Announcement Pages** workflow chunks the work one month per job, ~30k
pages and ~45 minutes each. Dispatch it with a year, a list, or a range
(`2017`, `2017,2018`, `2019-2022`).

Month jobs are stateless. `--known-from-hf` takes the already-done set from the
dataset's manifest instead of a shared parquet, so nothing is pulled from R2
beforehand or written back after and jobs cannot race. `max-parallel` is
deliberately 3: it multiplies with each job's `workers`, so 3 x 6 is already 18
concurrent requests against the 8 a daily run uses.

Old pages are all still served — a sample across 2017, 2019, 2021, 2023 and
2025 came back 40/40 alive with every section parsing.
