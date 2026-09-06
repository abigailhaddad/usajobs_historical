"""Tests for the backfill progress check.

Written because `systemctl is-active` reported "activating" for seven hours
while the run published nothing — it was failing, restarting cleanly, and
carrying more unpruned text into each attempt until the OOM killer took it
again. These assert the check notices what liveness could not.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from backfill_status import SPIRAL_ROWS, analyse


def lines(published=0, joins=(), ooms=0, pages=()):
    out = []
    out += ["Pushed 1 month(s) to abigailhaddad/usajobs-scraping"] * published
    out += [f"Local join has {n:,}; {n:,} are new" for n in joins]
    out += ["usajobs-backfill.service: Failed with result 'oom-kill'."] * ooms
    out += [f"Fetched {n:,} pages in 50.0 min, 0 failed" for n in pages]
    return out


class TestVerdict:
    def test_a_publishing_run_is_healthy(self):
        _, problems = analyse(lines(published=3, joins=(24_697,),
                                    pages=(24_697,)), 6)
        assert problems == []

    def test_alive_but_publishing_nothing_is_caught(self):
        # The exact failure: pages fetched, no months published.
        _, problems = analyse(lines(published=0, pages=(31_041, 27_386)), 6)
        assert len(problems) == 1
        assert "nothing published" in problems[0]

    def test_the_spiral_signature_is_caught(self):
        # A failed publish leaves text unpruned, so the join grows past one
        # month's worth. This is what preceded every OOM.
        _, problems = analyse(lines(published=1, joins=(31_041, 58_426)), 6)
        assert any("carries both months" in p for p in problems)

    def test_one_month_of_join_rows_is_not_a_spiral(self):
        _, problems = analyse(lines(published=2, joins=(31_041,)), 6)
        assert not any("carries both" in p for p in problems)

    def test_oom_kills_are_reported_even_when_publishing(self):
        _, problems = analyse(lines(published=2, joins=(24_000,), ooms=1), 6)
        assert any("OOM kill" in p for p in problems)

    def test_counts_are_reported(self):
        stats, _ = analyse(lines(published=4, joins=(20_000,), ooms=2,
                                 pages=(24_697, 28_797)), 6)
        assert stats["published"] == 4
        assert stats["pages"] == 53_494
        assert stats["ooms"] == 2

    def test_the_spiral_threshold_sits_above_a_real_month(self):
        # The largest month in the corpus is 2018-10 at 31,358.
        assert SPIRAL_ROWS > 31_358
