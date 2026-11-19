"""The site's pages read their data from the R2 custom domain, never r2.dev.

2026-11-19: a reader's Mac resolved pub-317c58882ec04f329b63842c1eb65b0c.r2.dev
to 0.0.0.0 (a content filter), so the table and charts never loaded. Network
and security filters commonly block *.r2.dev, and Cloudflare documents r2.dev
as a rate-limited development URL. The bucket is now served at
data.usajobs-historical.abigailhaddad.com; this test keeps every published
page and shared script on it.
"""
from pathlib import Path
import re

WEB = Path(__file__).resolve().parent.parent
DATA_HOST = "data.usajobs-historical.abigailhaddad.com"


def _site_files():
    for p in sorted(WEB.rglob("*")):
        if p.suffix in (".html", ".js", ".css") and "tests" not in p.parts and "dist" not in p.parts:
            yield p


def test_no_page_reads_from_r2_dev():
    hits = [f"{p.relative_to(WEB)}:{i}" for p in _site_files()
            for i, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1)
            if re.search(r"https?://[^\s'\"]*\.r2\.dev", line)]
    assert not hits, hits


def test_data_pages_use_the_custom_domain():
    for name in ("index.html", "pivot.html"):
        html = (WEB / name).read_text(encoding="utf-8")
        assert f"https://{DATA_HOST}/web/jobs_5yr.parquet" in html, name
        assert f"https://{DATA_HOST}/web/static.json" in html, name
    assert f"https://{DATA_HOST}/web/jobs_5yr.parquet" in (WEB / "shared" / "shared.js").read_text(encoding="utf-8")
