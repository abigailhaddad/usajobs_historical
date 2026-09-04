"""Tests for the announcement-section health check.

Regression cover for issue #569. The check originally scanned
scraped_jobs_{year}.parquet and treated a NULL text column as a parse failure.
prune_published_text was added later and deliberately NULLs text on every
posting it publishes, so the scan started measuring the unpublished fraction
and reporting it as markup drift: on 2026-09-04 it flagged twelve fields at
47.2% while live pages were parsing at 12/12.

It now measures the pages parsed in the run, which is the one population that
cannot have been pruned.
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import collect_scraped_data as csd
from usajobs_scrape import SECTION_FIELDS


@pytest.fixture(autouse=True)
def isolate(tmp_path, monkeypatch):
    monkeypatch.setattr(csd, "HEALTH_FILE", str(tmp_path / "health.json"))
    monkeypatch.setattr(csd, "WARNING_FILE", str(tmp_path / "warning.txt"))
    return tmp_path


def row(**overrides):
    r = {f: f"{f} content" for f in SECTION_FIELDS}
    r.update(overrides)
    return r


def warnings(tmp_path):
    path = tmp_path / "warning.txt"
    return path.read_text() if path.exists() else ""


def health(tmp_path):
    return json.loads((tmp_path / "health.json").read_text())


class TestRecordFieldHealth:
    def test_fully_parsed_pages_raise_nothing(self, isolate):
        csd.record_field_health([row() for _ in range(200)])
        assert warnings(isolate) == ""
        assert health(isolate)["fields"]["majorDuties"]["rate"] == 1.0

    def test_a_section_that_stops_parsing_is_flagged(self, isolate):
        rows = [row(majorDuties=None) for _ in range(100)]
        csd.record_field_health(rows)
        assert "majorDuties" in warnings(isolate)
        assert "markup has probably changed" in warnings(isolate)

    def test_education_has_a_lower_floor(self, isolate):
        # Genuinely absent from roughly one announcement in six, so 80% fill
        # must not raise while the same rate would for any other field.
        rows = [row(education=None if i % 5 == 0 else "x") for i in range(100)]
        csd.record_field_health(rows)
        assert "education" not in warnings(isolate)

    def test_education_still_flags_when_it_truly_breaks(self, isolate):
        csd.record_field_health([row(education=None) for _ in range(100)])
        assert "education" in warnings(isolate)

    def test_pruned_rows_cannot_affect_the_measurement(self, isolate):
        # The bug: published rows have their text nulled on disk. Those rows
        # are not in the parsed list, so they cannot drag the rate down.
        parsed = [row() for _ in range(50)]
        csd.record_field_health(parsed)
        assert warnings(isolate) == ""
        assert health(isolate)["pages_parsed"] == 50

    def test_a_run_that_parsed_nothing_writes_nothing(self, isolate):
        csd.record_field_health([])
        assert not os.path.exists(csd.HEALTH_FILE)
        assert warnings(isolate) == ""

    def test_every_parser_section_is_measured(self, isolate):
        csd.record_field_health([row()])
        assert set(health(isolate)["fields"]) == set(SECTION_FIELDS)
