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
    pages = [FakeResponse(200, json.dumps({"data": jobs, "paging": {
                 "metadata": {"totalCount": 612}}}))]
    monkeypatch.setattr(repoll_status.requests, "get", lambda *a, **k: pages.pop(0))
    capped = []
    monkeypatch.setattr(repoll_status, "check_cap", lambda n, what: capped.append(n))
    repoll_status.fetch_all_for_date("2026-01-01")
    assert capped == []  # under-collection path owns this case, not the cap
