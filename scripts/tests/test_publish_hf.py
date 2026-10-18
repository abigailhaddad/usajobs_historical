"""Tests for publish_to_huggingface.py — above all, that a rebuild never loses
or duplicates an announcement.

Month files are rewritten wholesale, so anything a rebuild leaves out is gone
from the dataset with no way back. And USAJOBS moves open dates after a posting
is published, which is what made the old date-based rebuild drop postings from
one file without adding them to another (164 of them by 2026-10-17), put others
in two files (133), and finally stall the daily publish from 2026-10-02.
"""
import os
import sys
from datetime import date

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from publish_to_huggingface import (EXIT_REFUSED, NON_STRING_COLUMNS,
                                    STALE_TOLERANCE, TEXT_FIELDS,
                                    check_types, choose_locations,
                                    day_partitions, metadata_fields,
                                    month_members, months_to_rebuild,
                                    prune_published_text, reconcile_with_prior,
                                    resolve_list_columns, select_sql,
                                    stale_postings, verify_build)


class TestMonthMembers:
    def test_new_postings_join_their_open_month(self):
        members = month_members("2026-10", located={"1": "2026-10"},
                                local_by_month={"2026-10": {"1", "2"}})
        assert members == {"1", "2"}

    def test_a_posting_stays_in_its_file_when_its_open_date_moves(self):
        # "1" was published in September; the mirror now dates it October.
        assert "1" in month_members("2026-09", {"1": "2026-09"},
                                    {"2026-10": {"1"}})
        assert "1" not in month_members("2026-10", {"1": "2026-09"},
                                        {"2026-10": {"1"}})

    def test_a_posting_in_no_file_comes_back_once_its_text_is_local(self):
        # Lost by the old date-based rebuild, then fetched again.
        assert month_members("2026-09", located={},
                             local_by_month={"2026-09": {"7"}}) == {"7"}

    def test_the_october_stall(self):
        # 2026-10 had no file. 144 "published October postings" by today's
        # dates lived in other months' files. The month must build from its
        # genuinely new postings alone, without the 144 and without refusing.
        located = {str(i): "2026-09" for i in range(144)}
        local = {"2026-10": {str(i) for i in range(20)} | {"new1", "new2"}}
        assert months_to_rebuild(located, local) == ["2026-10"]
        assert month_members("2026-10", located, local) == {"new1", "new2"}

    def test_nothing_new_means_nothing_to_rebuild(self):
        assert months_to_rebuild({"1": "2026-09"}, {"2026-10": {"1"}}) == []


class TestReconcileWithPrior:
    def test_the_file_fills_in_what_the_map_lacks(self):
        located, stale, moved = reconcile_with_prior(
            "2026-09", {}, prior_cns={"1"}, on_dataset={"2026-09"})
        assert located == {"1": "2026-09"}
        assert stale == set() and moved == set()

    def test_a_mapped_posting_missing_from_the_file_is_unmapped(self):
        located, stale, _ = reconcile_with_prior(
            "2026-09", {"1": "2026-09", "2": "2026-09"}, prior_cns={"1"},
            on_dataset={"2026-09"})
        assert stale == {"2"}
        assert "2" not in located

    def test_a_duplicate_is_dropped_only_when_its_other_file_exists(self):
        _, _, moved = reconcile_with_prior(
            "2026-09", {"1": "2026-08"}, prior_cns={"1"},
            on_dataset={"2026-08", "2026-09"})
        assert moved == {"1"}

        located, _, moved = reconcile_with_prior(
            "2026-09", {"1": "2026-08"}, prior_cns={"1"},
            on_dataset={"2026-09"})
        assert moved == set()
        assert located["1"] == "2026-09"


class TestChooseLocations:
    def test_a_single_file_is_the_answer(self):
        assert choose_locations({"1": ["2025-03"]}, {}) == {"1": "2025-03"}

    def test_a_duplicate_keeps_the_copy_matching_its_open_date(self):
        assert choose_locations({"1": ["2025-10", "2026-01"]},
                                {"1": "2026-01"}) == {"1": "2026-01"}

    def test_otherwise_the_earliest(self):
        assert choose_locations({"1": ["2025-10", "2026-01"]},
                                {"1": "2026-05"}) == {"1": "2025-10"}


class TestVerifyBuild:
    def test_an_exact_build_passes(self):
        assert verify_build("m", {"1", "2"}, 2, {"1", "2"}, {"1"}, set()) is None

    def test_dropping_a_published_row_is_refused(self):
        assert "would drop" in verify_build(
            "m", {"2"}, 1, {"1", "2"}, prior_cns={"1"}, moved_out=set())

    def test_dropping_a_duplicate_is_allowed(self):
        assert verify_build("m", {"2"}, 1, {"2"}, prior_cns={"1", "2"},
                            moved_out={"1"}) is None

    def test_duplicate_rows_are_refused(self):
        assert "duplicate" in verify_build("m", {"1"}, 2, {"1"}, set(), set())

    def test_rows_nobody_asked_for_are_refused(self):
        assert "nobody" in verify_build("m", {"1", "9"}, 2, {"1"}, set(), set())


class TestStalePostings:
    def _mirror(self, tmp_path, rows):
        import pandas as pd
        path = tmp_path / "historical_jobs_2026.parquet"
        pd.DataFrame(rows, columns=["usajobsControlNumber", "positionOpenDate"]
                     ).to_parquet(path, index=False)
        return str(path)

    def test_only_old_unpublished_postings_count(self, tmp_path):
        import duckdb
        path = self._mirror(tmp_path, [
            (1, "2026-09-01"), (2, "2026-09-02"), (3, "2026-10-16")])
        stale = stale_postings(duckdb.connect(), path, published={"1"},
                               today=date(2026, 10, 17))
        assert stale == {"2026-09": 1}

    def test_the_tolerance_is_small(self):
        # The October stall left 11,351 unpublished. Anything near that has to
        # trip it; a few withdrawn pages must not.
        assert 0 < STALE_TOLERANCE <= 100


class TestBuildSql:
    """select_sql against real files, the way main() drives it."""

    def _setup(self, tmp_path):
        import duckdb
        import pandas as pd
        import re
        hist = tmp_path / "historical_jobs_2026.parquet"
        frame = pd.DataFrame({
            "usajobsControlNumber": [1, 2, 3],
            "positionTitle": ["A", "B", "C"],
            # "2" reopened: published in September, the mirror now says October.
            "positionOpenDate": ["2026-09-01", "2026-10-03", "2026-09-20"],
            "hiringpaths": [None, None, None],
        })
        frame.to_parquet(hist, index=False)
        # Every other column the metadata select names, as the mirror has it.
        for column in re.findall(r"h\.(\w+)",
                                 metadata_fields(duckdb.connect(), str(hist))):
            if column not in frame:
                frame[column] = None
        frame.to_parquet(hist, index=False)
        scraped = tmp_path / "scraped_jobs_2026.parquet"
        pd.DataFrame({
            "usajobs_control_number": ["1", "2", "3"],
            # 1 and 2 were published and pruned; 3 is new.
            "text": [None, None, "new page three"],
        }).to_parquet(scraped, index=False)
        prior = tmp_path / "prior.parquet"
        pd.DataFrame({
            "usajobsControlNumber": ["1", "2", "9"],
            "positionTitle": ["A", "B", "carried"],
            "text": ["page one", "page two", "page nine"],
        }).to_parquet(prior, index=False)
        con = duckdb.connect()
        return con, str(hist), str(scraped), str(prior)

    def _build(self, con, hist, scraped, prior, members, days=None):
        con.execute("CREATE OR REPLACE TEMP TABLE members (cn VARCHAR, d VARCHAR)")
        con.executemany("INSERT INTO members VALUES (?, ?)",
                        [(cn, d) for cn, d in members])
        return con.execute(select_sql(con, hist, scraped, prior, days)).df() \
            .set_index("usajobsControlNumber")

    def test_moved_posting_keeps_its_published_text(self, tmp_path):
        con, hist, scraped, prior = self._setup(tmp_path)
        out = self._build(con, hist, scraped, prior,
                          [("1", "01"), ("2", "03"), ("3", "20")])
        assert out.loc["2", "text"] == "page two"
        assert out.loc["3", "text"] == "new page three"
        assert out.loc["1", "text"] == "page one"

    def test_a_member_the_mirror_lacks_is_carried_over(self, tmp_path):
        # "9" moved into another mirror year: no fresh metadata, but its row
        # must survive the rebuild.
        con, hist, scraped, prior = self._setup(tmp_path)
        out = self._build(con, hist, scraped, prior,
                          [("1", "01"), ("9", "01")])
        assert set(out.index) == {"1", "9"}
        assert out.loc["9", "text"] == "page nine"
        assert out.loc["9", "positionTitle"] == "carried"

    def test_non_members_are_left_out(self, tmp_path):
        con, hist, scraped, prior = self._setup(tmp_path)
        out = self._build(con, hist, scraped, prior, [("3", "20")])
        assert list(out.index) == ["3"]

    def test_day_slices_cover_every_member(self, tmp_path):
        import duckdb
        con = duckdb.connect()
        con.execute("CREATE TEMP TABLE members (cn VARCHAR, d VARCHAR)")
        con.executemany("INSERT INTO members VALUES (?, ?)",
                        [(str(i), f"{1 + i % 28:02d}") for i in range(100)]
                        + [("x", "01")])
        slices = day_partitions(con, target=30)
        assert len(slices) > 1
        covered = sum(con.execute(
            f"SELECT count(*) FROM members WHERE d BETWEEN '{lo}' AND '{hi}'"
        ).fetchone()[0] for lo, hi in slices)
        assert covered == 101


class TestResolveListColumns:
    """Regression for two bugs, one of which hid inside the fix for the other.

    First: hardcoding the 2026 column name crashed on 2019 with 'does not have
    a column named hiringpaths_1'.

    Then: resolving against the *parquet* schema and emitting `h.hiringpaths`
    silently produced 175,926 rows of NULL, because the file carries both
    `HiringPaths` (vestigial, all null) and `hiringpaths` (the real data), SQL
    identifiers are case-insensitive, and the query bound to the first match.
    duckdb exposes the second as `hiringpaths_1`, and that is the name to emit.
    """

    def _mirror(self, tmp_path, columns):
        import pyarrow as pa
        import pyarrow.parquet as pq
        path = tmp_path / "mirror.parquet"
        pq.write_table(pa.table({n: pa.array(v, type=t)
                                 for n, (t, v) in columns.items()}), path)
        return str(path)

    def test_the_fullest_column_wins_not_the_suffixed_one(self, tmp_path):
        # The 2017/2018 shape: both spellings are VARCHAR, and the one duckdb
        # suffixes is the near-empty one. Preferring _1 by name published
        # 237,145 rows of NULL for 2017.
        import duckdb, pyarrow as pa
        path = self._mirror(tmp_path, {
            "HiringPaths": (pa.string(), ["a", "b", "c"]),
            "hiringpaths": (pa.string(), [None, None, "x"])})
        con = duckdb.connect()
        picked = resolve_list_columns(con, path)["hiringPaths"]
        assert picked == "h.HiringPaths"
        got = con.execute(
            f"SELECT {picked} FROM read_parquet('{path}') h").fetchall()
        assert [r[0] for r in got] == ["a", "b", "c"]

    def test_the_2026_shape_picks_the_suffixed_one(self, tmp_path):
        # Here the capitalised column is the empty one, so the suffixed form
        # wins on data even though it loses on name order.
        import duckdb, pyarrow as pa
        path = self._mirror(tmp_path, {
            "HiringPaths": (pa.null(), [None, None, None]),
            "hiringpaths": (pa.string(), ["a", "b", "c"])})
        con = duckdb.connect()
        picked = resolve_list_columns(con, path)["hiringPaths"]
        assert picked == "h.hiringpaths_1"
        got = con.execute(
            f"SELECT {picked} FROM read_parquet('{path}') h").fetchall()
        assert [r[0] for r in got] == ["a", "b", "c"]

    def test_the_2019_shape_has_no_collision(self, tmp_path):
        import duckdb, pyarrow as pa
        path = self._mirror(tmp_path, {
            "HiringPaths": (pa.string(), ["a", "b", "c"])})
        assert resolve_list_columns(duckdb.connect(), path)["hiringPaths"] \
            == "h.HiringPaths"

    def test_an_entirely_empty_candidate_never_wins(self, tmp_path):
        import duckdb, pyarrow as pa
        path = self._mirror(tmp_path, {
            "HiringPaths": (pa.string(), [None, None, None])})
        assert resolve_list_columns(duckdb.connect(), path)["hiringPaths"] \
            == "CAST(NULL AS VARCHAR)"

    def test_no_candidate_at_all_yields_null_not_a_crash(self, tmp_path):
        import duckdb, pyarrow as pa
        path = self._mirror(tmp_path, {"positionTitle": (pa.string(), ["x"] * 3)})
        cols = resolve_list_columns(duckdb.connect(), path)
        assert all(v == "CAST(NULL AS VARCHAR)" for v in cols.values())


class TestPublishedSchema:
    def _fields(self, tmp_path):
        import duckdb, pyarrow as pa
        import pyarrow.parquet as pq
        path = tmp_path / "m.parquet"
        pq.write_table(pa.table({"hiringpaths": pa.array([None], pa.string())}),
                       path)
        return metadata_fields(duckdb.connect(), str(path))

    def test_the_job_title_is_selected(self, tmp_path):
        # The field list this replaced omitted positionTitle, so the published
        # dataset had no job title in it at all.
        fields = self._fields(tmp_path)
        assert "AS positionTitle" in fields
        assert "AS hiringSubelementName" in fields

    def test_bookkeeping_columns_are_not_published(self, tmp_path):
        fields = self._fields(tmp_path)
        for column in ("inserted_at", "last_seen", "usajobs_control_number",
                       "backfilled"):
            assert f"h.{column}" not in fields

    def test_who_may_apply_is_cast_for_a_stable_column_type(self, tmp_path):
        # VARCHAR through 2024, empty INTEGER from 2025. Without the cast the
        # month files disagree and the dataset cannot be read as a whole.
        assert "CAST(h.whoMayApply AS VARCHAR)" in self._fields(tmp_path)

    def test_every_metadata_column_is_cast(self, tmp_path):
        # Not just whoMayApply. duckdb types a column of untyped NULLs as
        # INT32, so any metadata field that happens to be empty for a month
        # publishes the wrong type for that month unless it is cast.
        fields = self._fields(tmp_path)
        for column in ("hiringSubelementName", "serviceType",
                       "announcementClosingTypeCode",
                       "announcementClosingTypeDescription", "totalOpenings"):
            assert f"CAST(h.{column} AS VARCHAR)" in fields

    def test_the_numeric_columns_stay_numeric(self, tmp_path):
        fields = self._fields(tmp_path)
        assert "CAST(h.agencyLevel AS BIGINT)" in fields
        assert "CAST(h.minimumSalary AS DOUBLE)" in fields
        assert "CAST(h.maximumSalary AS DOUBLE)" in fields

    def test_every_announcement_section_is_published(self):
        for section in ("jobSummary", "majorDuties", "qualificationSummary",
                        "education", "requiredDocuments", "howToApply", "text"):
            assert section in TEXT_FIELDS
        assert len(TEXT_FIELDS) == 12


class TestCheckTypes:
    """The guard that refuses to upload a month with drifted column types.

    whoMayApply shipped as INT32 for four 2026 months and four other columns
    shipped as unannotated binary for the two 2013 months, because each was
    entirely null there and nothing pinned the type. Every one of those files
    reads fine alone -- only the union of them is wrong, which is why it went
    unnoticed for years.
    """
    def _write(self, tmp_path, columns):
        import pyarrow as pa
        import pyarrow.parquet as pq
        path = tmp_path / "m.parquet"
        pq.write_table(pa.table(columns), path)
        return path

    def _good(self):
        import pyarrow as pa
        return {"usajobsControlNumber": pa.array(["1"], pa.string()),
                "whoMayApply": pa.array([None], pa.string()),
                "agencyLevel": pa.array([2], pa.int64()),
                "minimumSalary": pa.array([1.0], pa.float64())}

    def test_the_published_types_pass(self, tmp_path):
        check_types(self._write(tmp_path, self._good()))

    def test_an_all_null_string_column_typed_as_int_is_refused(self, tmp_path):
        import pyarrow as pa
        columns = self._good()
        columns["whoMayApply"] = pa.array([None], pa.int32())
        with pytest.raises(SystemExit, match="whoMayApply"):
            check_types(self._write(tmp_path, columns))

    def test_binary_is_refused_too(self, tmp_path):
        # The 2013 flavour: BYTE_ARRAY with no UTF8 annotation.
        import pyarrow as pa
        columns = self._good()
        columns["serviceType"] = pa.array([None], pa.binary())
        with pytest.raises(SystemExit, match="serviceType"):
            check_types(self._write(tmp_path, columns))

    def test_a_numeric_column_turned_string_is_refused(self, tmp_path):
        import pyarrow as pa
        columns = self._good()
        columns["agencyLevel"] = pa.array(["2"], pa.string())
        with pytest.raises(SystemExit, match="agencyLevel"):
            check_types(self._write(tmp_path, columns))

    def test_every_non_string_column_is_one_duckdb_can_cast_to(self):
        assert set(NON_STRING_COLUMNS.values()) <= {"BIGINT", "DOUBLE"}

    def test_refusal_has_its_own_exit_code(self):
        # The daily workflow warns on a refused month and fails on anything
        # else, so refusal must not collide with 0 or with the 1 an uncaught
        # exception exits with -- a drifted type has to stay distinguishable
        # from "backfill is still running".
        assert EXIT_REFUSED not in (0, 1)


class TestPrunePublishedText:
    """Announcement text is 97.8% of scraped_jobs_{year}.parquet — 5.42 KB of a
    5.54 KB row, so a full year is 920 MB against 20 MB for everything else.
    Once a posting is on HuggingFace the dataset is the store for its text, and
    what stays local is the structured shadow the API comparison reads.
    """

    def _parquet(self, tmp_path, rows):
        import pandas as pd
        path = tmp_path / "scraped_jobs_2026.parquet"
        pd.DataFrame(rows).to_parquet(path, index=False, compression="zstd")
        return str(path)

    def _rows(self):
        return [
            {"usajobs_control_number": "1", "positionTitle": "Analyst",
             "minimumSalary": 50000.0, "text": "page one",
             "qualificationSummary": "quals one", "majorDuties": "duties one"},
            {"usajobs_control_number": "2", "positionTitle": "Engineer",
             "minimumSalary": 90000.0, "text": "page two",
             "qualificationSummary": "quals two", "majorDuties": "duties two"},
        ]

    def test_text_is_dropped_only_on_published_rows(self, tmp_path):
        import pandas as pd
        path = self._parquet(tmp_path, self._rows())
        assert prune_published_text(path, {"1"}) == 1

        out = pd.read_parquet(path).set_index("usajobs_control_number")
        assert pd.isna(out.loc["1", "text"])
        assert pd.isna(out.loc["1", "qualificationSummary"])
        assert out.loc["2", "text"] == "page two"

    def test_structured_columns_survive(self, tmp_path):
        import pandas as pd
        path = self._parquet(tmp_path, self._rows())
        prune_published_text(path, {"1", "2"})
        out = pd.read_parquet(path).set_index("usajobs_control_number")
        assert out.loc["1", "positionTitle"] == "Analyst"
        assert out.loc["2", "minimumSalary"] == 90000.0

    def test_no_rows_are_lost(self, tmp_path):
        import pandas as pd
        path = self._parquet(tmp_path, self._rows())
        prune_published_text(path, {"1", "2"})
        assert len(pd.read_parquet(path)) == 2

    def test_the_file_actually_shrinks(self, tmp_path):
        path = self._parquet(tmp_path, self._rows())
        before = os.path.getsize(path)
        prune_published_text(path, {"1", "2"})
        assert os.path.getsize(path) < before

    def test_publishing_nothing_changes_nothing(self, tmp_path):
        path = self._parquet(tmp_path, self._rows())
        assert prune_published_text(path, set()) == 0

    def test_a_missing_file_is_not_an_error(self, tmp_path):
        assert prune_published_text(str(tmp_path / "absent.parquet"), {"1"}) == 0

    def test_a_file_with_no_text_columns_is_left_alone(self, tmp_path):
        import pandas as pd
        path = self._parquet(tmp_path, [
            {"usajobs_control_number": "1", "positionTitle": "Analyst"}])
        assert prune_published_text(path, {"1"}) == 0
        assert pd.read_parquet(path).loc[0, "positionTitle"] == "Analyst"
