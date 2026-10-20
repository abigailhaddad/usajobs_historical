"""reconcile_status: match on control number + open date, fix only real drift."""
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import reconcile_status as rs


def table(rows):
    cols = ["usajobsControlNumber", "positionOpenDate", "positionOpeningStatus",
            "announcementNumber", "last_seen", "other"]
    data = {c: [r.get(c) for r in rows] for c in cols}
    return pa.table(data, schema=pa.schema([
        ("usajobsControlNumber", pa.int64()), ("positionOpenDate", pa.string()),
        ("positionOpeningStatus", pa.string()), ("announcementNumber", pa.string()),
        ("last_seen", pa.string()), ("other", pa.string())]))


def job(cn, opened, status, ann):
    return {"usajobsControlNumber": cn, "positionOpenDate": opened,
            "positionOpeningStatus": status, "announcementNumber": ann}


def row(cn, opened, status, ann, other="x"):
    return {"usajobsControlNumber": cn, "positionOpenDate": opened,
            "positionOpeningStatus": status, "announcementNumber": ann, "other": other}


def run(rows, jobs, fetched=("2026-01-09", "2025-11-06"), elsewhere=frozenset()):
    return rs.reconcile(table(rows), rs.api_index(jobs), set(fetched), elsewhere)


def test_status_drift_is_corrected():
    statuses, changes, unmatched, reused, _ = run(
        [row(1, "2026-01-09", "Applications under review", "A-1")],
        [job(1, "2026-01-09", "Candidate selected", "A-1")])
    assert statuses == ["Candidate selected"]
    assert [c["api_status"] for c in changes] == ["Candidate selected"]
    assert not unmatched and not reused


def test_final_rows_are_never_touched():
    statuses, changes, *_ = run(
        [row(1, "2026-01-09", "Job closed", "A-1")],
        [job(1, "2026-01-09", "Candidate selected", "A-1")])
    assert statuses == ["Job closed"] and not changes


def test_redated_posting_is_unmatched_not_borrowed_from_the_new_date():
    # The API now holds control number 1 under 2026-01-09 only. The 2025 row
    # must not pick up the 2026 listing's status.
    statuses, changes, unmatched, _, _ = run(
        [row(1, "2025-11-06", "Accepting applications", "A-1")],
        [job(1, "2026-01-09", "Job canceled", "A-1")], elsewhere={"1"})
    assert statuses == ["Accepting applications"] and not changes
    assert unmatched[0]["control_number_in_other_rows"] is True


def test_reused_control_number_with_other_announcement_is_left_alone():
    statuses, changes, _, reused, _ = run(
        [row(1, "2026-01-09", "Accepting applications", "A-1")],
        [job(1, "2026-01-09", "Job closed", "B-9")])
    assert statuses == ["Accepting applications"] and not changes
    assert reused[0]["api_announcementNumber"] == "B-9"


def test_missing_api_status_does_not_blank_ours():
    statuses, changes, *_ = run(
        [row(1, "2026-01-09", "Accepting applications", "A-1")],
        [job(1, "2026-01-09", None, "A-1")])
    assert statuses == ["Accepting applications"] and not changes


def test_null_status_is_nonfinal_and_gets_filled():
    statuses, changes, *_ = run(
        [row(1, "2026-01-09", None, "A-1")],
        [job(1, "2026-01-09", "Accepting applications", "A-1")])
    assert statuses == ["Accepting applications"] and len(changes) == 1


def test_dates_that_failed_to_fetch_are_skipped_not_unmatched():
    _, changes, unmatched, _, skipped = run(
        [row(1, "2026-01-09", "Accepting applications", "A-1")], [], fetched=())
    assert skipped == 1 and not unmatched and not changes


def test_same_control_number_on_two_dates_is_handled_separately():
    statuses, changes, *_ = run(
        [row(1, "2025-11-06", "Accepting applications", "A-1"),
         row(1, "2026-01-09", "Accepting applications", "A-1")],
        [job(1, "2026-01-09", "Job canceled", "A-1"),
         job(1, "2025-11-06", "Job closed", "A-1")])
    assert statuses == ["Job closed", "Job canceled"]


def test_apply_changes_touches_only_status_and_last_seen(tmp_path):
    rows = [row(1, "2026-01-09", "Applications under review", "A-1", other="keep"),
            row(2, "2026-01-09", "Job closed", "A-2", other="keep2")]
    t = table(rows)
    path = str(tmp_path / "historical_jobs_2026.parquet")
    pq.write_table(t, path)
    statuses, changes, *_ = rs.reconcile(
        t, rs.api_index([job(1, "2026-01-09", "Candidate selected", "A-1")]),
        {"2026-01-09"})
    rs.apply_changes(path, t, statuses, changes)
    after = pq.read_table(path)
    assert after.schema == t.schema
    assert after.column("positionOpeningStatus").to_pylist() == ["Candidate selected", "Job closed"]
    assert after.column("other").to_pylist() == ["keep", "keep2"]
    seen = after.column("last_seen").to_pylist()
    assert seen[0] and seen[1] is None


def test_control_number_held_by_two_year_files_is_reported_as_repeated(tmp_path):
    one = table([row(7, "2025-11-06", "Job closed", "A-7"),
                 row(8, "2025-11-06", "Job closed", "A-8")])
    two = table([row(7, "2026-01-09", "Job canceled", "A-7")])
    pq.write_table(one, str(tmp_path / "historical_jobs_2025.parquet"))
    pq.write_table(two, str(tmp_path / "historical_jobs_2026.parquet"))
    pq.write_table(two, str(tmp_path / "historical_jobs_2024_backup.parquet"))
    assert rs.repeated_control_numbers(str(tmp_path)) == {"7"}

