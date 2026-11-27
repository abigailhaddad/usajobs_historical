"""repoll_status.retry_failed_dates: the second try for dates that errored."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import repoll_status as rs  # noqa: E402


def fake_fetch(outcomes):
    """outcomes: date -> list of jobs (success) or an error string."""
    calls = []

    def fetch(date_str):
        calls.append(date_str)
        out = outcomes[date_str]
        return (date_str, [], out) if isinstance(out, str) else (date_str, out, None)
    fetch.calls = calls
    return fetch


def no_sleep(_):
    pass


def test_a_burst_of_503s_recovers_on_the_second_try():
    # #787: twelve dates 503'd within six minutes; all worked by hand later.
    failed = {f"2021-05-{d}": "503 Service Unavailable after 5 attempts" for d in range(16, 28)}
    fetch = fake_fetch({d: [{"usajobsControlNumber": 1}] for d in failed})
    recovered = rs.retry_failed_dates(failed, fetch=fetch, sleep=no_sleep)
    assert failed == {}
    assert len(recovered) == 12
    assert all(jobs == [{"usajobsControlNumber": 1}] for _, jobs in recovered)


def test_a_date_that_fails_again_stays_failed_with_the_new_error():
    failed = {"2021-05-16": "503 first", "2021-05-17": "503 first"}
    fetch = fake_fetch({"2021-05-17": "503 again", "2021-05-16": []})
    recovered = rs.retry_failed_dates(failed, fetch=fetch, sleep=no_sleep)
    assert failed == {"2021-05-17": "503 again"}
    assert recovered == [("2021-05-16", [])]


def test_stops_after_consecutive_failures_and_leaves_the_rest():
    failed = {f"2021-05-{d}": "503 first" for d in range(16, 22)}
    fetch = fake_fetch({d: "503 again" for d in failed})
    rs.retry_failed_dates(failed, fetch=fetch, sleep=no_sleep)
    assert len(fetch.calls) == rs.RETRY_PASS_MAX_CONSECUTIVE_FAILURES
    assert len(failed) == 6
    untried = set(failed) - set(fetch.calls)
    assert all(failed[d] == "503 first" for d in untried)


def test_a_success_resets_the_consecutive_failure_count():
    dates = ["2021-05-20", "2021-05-19", "2021-05-18", "2021-05-17", "2021-05-16"]
    outcomes = dict(zip(dates, ["503", [], "503", [], "503"]))
    failed = {d: "503 first" for d in dates}
    fetch = fake_fetch(outcomes)
    rs.retry_failed_dates(failed, fetch=fetch, sleep=no_sleep)
    assert fetch.calls == dates          # newest first, all tried
    assert sorted(failed) == ["2021-05-16", "2021-05-18", "2021-05-20"]


def test_skipped_when_the_time_budget_cannot_cover_the_wait():
    failed = {"2021-05-16": "503 first"}
    fetch = fake_fetch({"2021-05-16": []})
    waited = []
    rs.retry_failed_dates(failed, fetch=fetch, sleep=waited.append,
                          time_left=lambda: rs.RETRY_PASS_WAIT - 1)
    assert fetch.calls == [] and waited == []
    assert failed == {"2021-05-16": "503 first"}


def test_stops_when_the_budget_runs_out_mid_pass():
    failed = {"2021-05-16": "e", "2021-05-17": "e", "2021-05-18": "e"}
    fetch = fake_fetch({d: [] for d in failed})
    budget = iter([1000, 10, 0, 0])    # before the wait, then before each date
    rs.retry_failed_dates(failed, fetch=fetch, sleep=no_sleep, time_left=lambda: next(budget))
    assert fetch.calls == ["2021-05-18"]
    assert sorted(failed) == ["2021-05-16", "2021-05-17"]


def test_nothing_to_do_without_failures():
    fetch = fake_fetch({})
    assert rs.retry_failed_dates({}, fetch=fetch, sleep=no_sleep) == []
    assert fetch.calls == []
