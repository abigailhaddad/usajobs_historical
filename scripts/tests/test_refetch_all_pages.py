"""Tests for refetch_all_pages.py -- what counts as a page that came back."""
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import refetch_all_pages as r  # noqa: E402
import usajobs_scrape  # noqa: E402

PAGE = ('<html><div id="joa-requirements"><h3>Qualifications</h3>'
        '<p><strong>Fi</strong><strong>nancial</strong> experience.</p>'
        '</div></html>')


def test_pages_that_come_back_are_parsed_and_the_rest_are_counted(
        monkeypatch):
    answers = {"1": (PAGE, None), "2": (None, None),
               "3": (None, "timed out"), "4": ("<html></html>", None)}
    monkeypatch.setattr(usajobs_scrape, "fetch_job_page",
                        lambda session, cn: answers[cn])
    fresh, problems = r.fetch_all(list(answers), workers=2, pool=None)
    assert fresh["1"]["qualificationSummary"] == "Financial experience."
    assert problems == {"2": "404", "3": "error: timed out",
                        "4": "no requirements text"}


def test_progress_is_read_from_commit_titles():
    api = SimpleNamespace(list_repo_commits=lambda repo, repo_type: [
        SimpleNamespace(title=r.COMMIT_PREFIX + "data/2026_09.parquet"),
        SimpleNamespace(title="Repair run-on qualifications in data/2026_08"
                              ".parquet"),
        # The first pass, before the Benefits-link fix: not done.
        SimpleNamespace(title="Re-fetch every announcement page in "
                              "data/2026_07.parquet")])
    assert r.done_months(api, "x") == {"data/2026_09.parquet"}
