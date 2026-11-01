"""repoll_status.fetch_all_for_date: how the end of a date's results arrives."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class FakeResponse:
    def __init__(self, status_code, text, payload=None):
        self.status_code, self.text, self._payload = status_code, text, payload
        self.url = "https://data.usajobs.gov/api/historicjoa"

    def raise_for_status(self):
        pass

    def json(self):
        import json
        return self._payload if self._payload is not None else json.loads(self.text)


def test_repoll_treats_an_empty_204_as_the_end_of_results(monkeypatch):
    # A date with exactly 500 postings: page one is full and links to a next
    # page, which comes back as HTTP 204 with no body at all.
    import repoll_status
    pages = [FakeResponse(200, '{"data": [{"usajobsControlNumber": 1}], "paging": {"next": "/x"}}'),
             FakeResponse(204, "")]
    monkeypatch.setattr(repoll_status.requests, "get", lambda *a, **k: pages.pop(0))
    jobs = repoll_status.fetch_all_for_date("2025-04-01")
    assert [j["usajobsControlNumber"] for j in jobs] == [1]


def test_repoll_still_accepts_the_204_text_body(monkeypatch):
    import repoll_status
    monkeypatch.setattr(repoll_status.requests, "get",
                        lambda *a, **k: FakeResponse(200, "204 No Content"))
    assert repoll_status.fetch_all_for_date("2025-04-02") == []


def test_exactly_500_postings_is_not_a_cap_hit_when_the_api_total_agrees(monkeypatch):
    import repoll_status
    jobs = [{"usajobsControlNumber": i} for i in range(500)]
    import json
    pages = [FakeResponse(200, json.dumps({"data": jobs, "paging": {
                 "metadata": {"totalCount": 500}, "next": "/x"}})),
             FakeResponse(204, "")]
    monkeypatch.setattr(repoll_status.requests, "get", lambda *a, **k: pages.pop(0))
    capped = []
    monkeypatch.setattr(repoll_status, "check_cap", lambda n, what: capped.append(n))
    assert len(repoll_status.fetch_all_for_date("2025-04-01")) == 500
    assert capped == []


def test_short_page_with_a_larger_api_total_is_under_collection_not_a_cap_hit(monkeypatch):
    import repoll_status, json
    jobs = [{"usajobsControlNumber": i} for i in range(500)]
    # Short on both fetches: the retry (#705) comes up short too.
    short = json.dumps({"data": jobs, "paging": {"metadata": {"totalCount": 612}}})
    pages = [FakeResponse(200, short), FakeResponse(200, short)]
    monkeypatch.setattr(repoll_status.requests, "get", lambda *a, **k: pages.pop(0))
    monkeypatch.setattr(repoll_status.time, "sleep", lambda s: None)
    capped = []
    monkeypatch.setattr(repoll_status, "check_cap", lambda n, what: capped.append(n))
    alerts = os.path.join(os.path.dirname(repoll_status.__file__), "..", "logs",
                          "PAGINATION_UNDER_COLLECTION.txt")
    before = open(alerts).read() if os.path.exists(alerts) else None
    try:
        repoll_status.fetch_all_for_date("2026-01-01")
        assert capped == []  # under-collection path owns this case, not the cap
    finally:
        _restore(alerts, before)  # the alert file is real; don't leave one behind


def test_failed_dates_are_written_to_a_marker_the_workflow_can_alert_on(tmp_path):
    import repoll_status
    path = repoll_status.write_date_error_marker(
        {"2025-06-23": "boom", "2021-02-08": "bang"}, [2021, 2025], str(tmp_path))
    text = open(path).read()
    assert path.endswith("REPOLL_DATE_ERRORS.txt")
    assert "2 date(s)" in text
    assert text.index("2021-02-08: bang") < text.index("2025-06-23: boom")


def test_no_failed_dates_writes_no_marker(tmp_path):
    import repoll_status
    assert repoll_status.write_date_error_marker({}, [2025], str(tmp_path)) is None
    assert not (tmp_path / "REPOLL_DATE_ERRORS.txt").exists()


# --- a short date is fetched again before anything is alerted (#705) -------

def _short_then(second, monkeypatch, module):
    """Page one of a 612-row date with no `next` link (what the API sent for
    2026-01-01 on 2026-11-01), then `second` on the retry."""
    import json
    first = FakeResponse(200, json.dumps({
        "data": [{"usajobsControlNumber": i} for i in range(500)],
        "paging": {"metadata": {"totalCount": 612}}}))
    pages = [first, second]
    monkeypatch.setattr(module.requests, "get", lambda *a, **k: pages.pop(0))
    monkeypatch.setattr(module.time, "sleep", lambda s: None)
    alerts = os.path.join(os.path.dirname(module.__file__), "..", "logs",
                          "PAGINATION_UNDER_COLLECTION.txt")
    before = open(alerts).read() if os.path.exists(alerts) else None
    return pages, alerts, before


def _restore(alerts, before):
    if before is None:
        if os.path.exists(alerts):
            os.remove(alerts)
    else:
        open(alerts, "w").write(before)


def test_repoll_retries_a_short_date_and_stays_quiet_when_the_retry_is_whole(monkeypatch):
    import repoll_status, json
    whole = FakeResponse(200, json.dumps({"data": [{"usajobsControlNumber": i}
                                                   for i in range(97)]}))
    pages, alerts, before = _short_then(whole, monkeypatch, repoll_status)
    try:
        jobs = repoll_status.fetch_all_for_date("2026-01-01")
        assert len(jobs) == 97 and pages == []
        assert (open(alerts).read() if os.path.exists(alerts) else None) == before
    finally:
        _restore(alerts, before)


def test_repoll_alerts_when_the_retry_is_short_too(monkeypatch):
    import repoll_status, json
    again = FakeResponse(200, json.dumps({
        "data": [{"usajobsControlNumber": i} for i in range(500)],
        "paging": {"metadata": {"totalCount": 612}}}))
    pages, alerts, before = _short_then(again, monkeypatch, repoll_status)
    try:
        repoll_status.fetch_all_for_date("2026-01-01")
        assert "repoll date 2026-01-01: expected 612, got 500" in open(alerts).read()
    finally:
        _restore(alerts, before)


def test_daily_collection_retries_a_short_fetch_too(monkeypatch):
    import collect_data, json
    whole = FakeResponse(200, json.dumps({"data": [{"usajobsControlNumber": i}
                                                   for i in range(97)]}))
    pages, alerts, before = _short_then(whole, monkeypatch, collect_data)
    try:
        jobs, complete = collect_data.fetch_all_pages(
            {"StartPositionOpenDate": "2026-01-01", "EndPositionOpenDate": "2026-01-01"},
            "2026-01-01")
        assert len(jobs) == 97 and complete and pages == []
        assert (open(alerts).read() if os.path.exists(alerts) else None) == before
    finally:
        _restore(alerts, before)


def test_daily_collection_alerts_and_marks_incomplete_when_short_twice(monkeypatch):
    import collect_data, json
    again = FakeResponse(200, json.dumps({
        "data": [{"usajobsControlNumber": i} for i in range(500)],
        "paging": {"metadata": {"totalCount": 612}}}))
    pages, alerts, before = _short_then(again, monkeypatch, collect_data)
    try:
        jobs, complete = collect_data.fetch_all_pages({}, "2026-01-01")
        assert not complete
        assert "2026-01-01: expected 612, got 500" in open(alerts).read()
    finally:
        _restore(alerts, before)
