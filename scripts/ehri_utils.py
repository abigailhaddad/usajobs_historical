"""Shared helpers for the EHRI aggregation scripts.

Functions here load OPM EHRI parquet files from the impactproject HF
dataset and roll them up by (agency, month, ...) for the static charts
under web/. Both aggregate_accessions.py (accessions + separations) and
aggregate_pathways.py call into this module; keep changes here
behavior-preserving across all callers.
"""
import os
import re

os.environ.setdefault("HF_HUB_DISABLE_IMPLICIT_TOKEN", "1")

import duckdb
from huggingface_hub import list_repo_files


HF_REPO = "impactproject/opm-ehri-data"
HF_BASE = f"https://huggingface.co/datasets/{HF_REPO}/resolve/main"

# OPM uses uppercase + slightly different naming. Map back to the labels
# used in our USAJobs dataset (and in the postings poster) so the two
# datasets line up panel-by-panel.
AGENCY_NORMALIZE = {
    "DEPARTMENT OF VETERANS AFFAIRS": "Department of Veterans Affairs",
    "DEPARTMENT OF HOMELAND SECURITY": "Department of Homeland Security",
    "DEPARTMENT OF DEFENSE": "Department of Defense",
    "DEPARTMENT OF THE NAVY": "Department of the Navy",
    "DEPARTMENT OF THE ARMY": "Department of the Army",
    "DEPARTMENT OF THE AIR FORCE": "Department of the Air Force",
    "DEPARTMENT OF JUSTICE": "Department of Justice",
    "DEPARTMENT OF INTERIOR": "Department of the Interior",
    "DEPARTMENT OF TREASURY": "Department of the Treasury",
    "DEPARTMENT OF THE TREASURY": "Department of the Treasury",
    "DEPARTMENT OF AGRICULTURE": "Department of Agriculture",
    "DEPARTMENT OF HEALTH AND HUMAN SERVICES":
        "Department of Health and Human Services",
    "DEPARTMENT OF COMMERCE": "Department of Commerce",
    "DEPARTMENT OF TRANSPORTATION": "Department of Transportation",
    "DEPARTMENT OF ENERGY": "Department of Energy",
    "DEPARTMENT OF STATE": "Department of State",
    "DEPARTMENT OF LABOR": "Department of Labor",
    "DEPARTMENT OF EDUCATION": "Department of Education",
    # EHRI truncates names to 40 chars, so HUD shows up as "...DEVELOPM"
    "DEPARTMENT OF HOUSING AND URBAN DEVELOPMENT":
        "Department of Housing and Urban Development",
    "DEPARTMENT OF HOUSING AND URBAN DEVELOPM":
        "Department of Housing and Urban Development",
    "GENERAL SERVICES ADMINISTRATION": "General Services Administration",
}


def normalize_agency(raw):
    return AGENCY_NORMALIZE.get(raw, raw)


def discover_urls(data_type, since_yyyymm):
    """[(yyyymm, url)] for the highest-versioned <data_type> parquet
    in each month >= since_yyyymm. Picks v3 over v2 over v1 etc.,
    falls back to unversioned legacy files."""
    files = list_repo_files(HF_REPO, repo_type="dataset")
    versioned = re.compile(rf"^{data_type}/{data_type}_(\d{{6}})_v(\d+)\.parquet$")
    legacy = re.compile(rf"^{data_type}/{data_type}_(\d{{6}})\.parquet$")

    best = {}
    for f in files:
        m = versioned.match(f)
        if m:
            month, ver = m.group(1), int(m.group(2))
            if month < since_yyyymm:
                continue
            if month not in best or ver > best[month][0]:
                best[month] = (ver, f)
            continue
        m = legacy.match(f)
        if m:
            month = m.group(1)
            if month < since_yyyymm:
                continue
            if month not in best:
                best[month] = (0, f)

    return sorted((m, f"{HF_BASE}/{best[m][1]}") for m in best)


def load_to_duckdb(urls):
    """Return a DuckDB connection with the parquet files loaded into a
    table called `data`. union_by_name handles schema drift across
    months (e.g. column added partway through the time series)."""
    db = duckdb.connect()
    file_list = ", ".join(f"'{u}'" for u in urls)
    db.execute(
        f"CREATE TABLE data AS "
        f"SELECT * FROM read_parquet([{file_list}], union_by_name=True)"
    )
    return db


def has_column(db, col):
    return db.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name = 'data' AND column_name = ? LIMIT 1",
        [col],
    ).fetchone() is not None


def agency_totals_by_month(db, months, where=""):
    """{agency: [total for each month in `months`]}.
    `where` is appended to the base WHERE clause (no leading AND)."""
    extra = f" AND ({where})" if where else ""
    rows = db.execute(f"""
        SELECT agency,
               personnel_action_effective_date_yyyymm AS month,
               SUM(CAST(count AS INTEGER)) AS total
        FROM data
        WHERE agency IS NOT NULL{extra}
        GROUP BY agency, month
    """).fetchall()
    month_index = {m: i for i, m in enumerate(months)}
    out = {}
    for agency_raw, month, total in rows:
        if month not in month_index:
            continue
        a = normalize_agency(agency_raw)
        if a not in out:
            out[a] = [0] * len(months)
        out[a][month_index[month]] += int(total or 0)
    return out


def breakdown_total(db, group_col, where=""):
    """{agency: {category: total}}"""
    extra = f" AND ({where})" if where else ""
    rows = db.execute(f"""
        SELECT agency,
               COALESCE({group_col}, '(unknown)') AS category,
               SUM(CAST(count AS INTEGER)) AS total
        FROM data
        WHERE agency IS NOT NULL{extra}
        GROUP BY agency, category
    """).fetchall()
    out = {}
    for agency_raw, category, total in rows:
        a = normalize_agency(agency_raw)
        out.setdefault(a, {})
        out[a][category] = out[a].get(category, 0) + int(total or 0)
    return out


def breakdown_by_month(db, group_col, months, where=""):
    """{agency: {category: [total for each month]}}"""
    extra = f" AND ({where})" if where else ""
    rows = db.execute(f"""
        SELECT agency,
               COALESCE({group_col}, '(unknown)') AS category,
               personnel_action_effective_date_yyyymm AS month,
               SUM(CAST(count AS INTEGER)) AS total
        FROM data
        WHERE agency IS NOT NULL{extra}
        GROUP BY agency, category, month
    """).fetchall()
    month_index = {m: i for i, m in enumerate(months)}
    out = {}
    for agency_raw, category, month, total in rows:
        if month not in month_index:
            continue
        a = normalize_agency(agency_raw)
        out.setdefault(a, {})
        series = out[a].setdefault(category, [0] * len(months))
        series[month_index[month]] += int(total or 0)
    return dict(sorted(out.items()))


def drp_breakdown_by_month(db, group_col, months):
    """{agency: {category: {drp: [total for each month]}}}
    Only meaningful when the table has a drp_indicator column."""
    rows = db.execute(f"""
        SELECT agency,
               COALESCE({group_col}, '(unknown)') AS category,
               COALESCE(drp_indicator, 'N') AS drp,
               personnel_action_effective_date_yyyymm AS month,
               SUM(CAST(count AS INTEGER)) AS total
        FROM data
        WHERE agency IS NOT NULL
        GROUP BY agency, category, drp, month
    """).fetchall()
    month_index = {m: i for i, m in enumerate(months)}
    out = {}
    for agency_raw, category, drp, month, total in rows:
        if month not in month_index:
            continue
        a = normalize_agency(agency_raw)
        out.setdefault(a, {})
        out[a].setdefault(category, {})
        series = out[a][category].setdefault(drp, [0] * len(months))
        series[month_index[month]] += int(total or 0)
    # Drop all-zero series to keep the file small.
    for agency, cats in list(out.items()):
        for cat, drps in list(cats.items()):
            for d, s in list(drps.items()):
                if not any(s):
                    del drps[d]
            if not drps:
                del cats[cat]
    return dict(sorted(out.items()))
