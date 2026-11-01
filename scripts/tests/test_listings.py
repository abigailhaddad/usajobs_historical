"""Re-opened postings: one row per control number, holding the latest listing.

Fixtures are real postings from the 2026-11-01 mirror:
  547366900  FAA, re-listed into another year: the 2024 file still said
             "Accepting applications" (opened 2024-08-18) while the 2025 file
             had it re-opened 2025-08-04, "Applications under review".
  835811200  Air Mobility Command, extended within its year: the mirror said it
             closed 2026-06-30, the Historical API 2026-10-30.
"""
import csv
import json
import os
import sys

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import listings  # noqa: E402
from listings import PRIOR_COL  # noqa: E402

FAA_2024 = {"usajobsControlNumber": 547366900, "positionOpenDate": "2024-08-18",
            "positionCloseDate": "2025-08-17",
            "positionOpeningStatus": "Accepting applications",
            "announcementNumber": "AGL-ERR-20-2101-63651", "positionTitle": "FAA"}
FAA_2025 = {**FAA_2024, "positionOpenDate": "2025-08-04",
            "positionCloseDate": "2025-09-26",
            "positionOpeningStatus": "Applications under review"}


def write(path, rows):
    pd.DataFrame(rows).to_parquet(path, index=False)


def ids(path):
    return set(pd.read_parquet(path)["usajobsControlNumber"].astype(str))


class TestCarryListing:
    def test_extension_is_the_same_listing(self):
        old = {"positionOpenDate": "2025-12-02", "positionCloseDate": "2026-06-30"}
        new = {"positionOpenDate": "2025-12-02", "positionCloseDate": "2026-10-30"}
        assert listings.carry_listing(old, new) is None

    def test_reopen_records_the_earlier_listing(self):
        prior = json.loads(listings.carry_listing(FAA_2024, FAA_2025))
        assert prior == [{"positionOpenDate": "2024-08-18",
                          "positionCloseDate": "2025-08-17",
                          "positionOpeningStatus": "Accepting applications",
                          "announcementNumber": "AGL-ERR-20-2101-63651",
                          "positionTitle": "FAA"}]

    def test_priors_accumulate_oldest_first_without_repeats(self):
        second = {**FAA_2025, PRIOR_COL: listings.carry_listing(FAA_2024, FAA_2025)}
        third = {**FAA_2025, "positionOpenDate": "2026-01-05"}
        third[PRIOR_COL] = listings.carry_listing(second, third)
        later = {**FAA_2025, "positionOpenDate": "2026-03-01"}
        prior = json.loads(listings.carry_listing(third, later))
        assert [p["positionOpenDate"] for p in prior] == \
            ["2024-08-18", "2025-08-04", "2026-01-05"]

    def test_a_row_is_never_its_own_prior_listing(self):
        stale = {**FAA_2025, PRIOR_COL: json.dumps([listings.listing_of(FAA_2025)])}
        assert listings.carry_listing(stale, FAA_2025) is None


class TestCollapseAcrossYears:
    def test_the_later_listing_wins_and_keeps_the_earlier(self, tmp_path):
        write(tmp_path / "historical_jobs_2024.parquet",
              [FAA_2024, {**FAA_2024, "usajobsControlNumber": 1}])
        write(tmp_path / "historical_jobs_2025.parquet",
              [FAA_2025, {**FAA_2025, "usajobsControlNumber": 2}])
        moves = listings.collapse_across_years(str(tmp_path), log=lambda m: None)

        assert ids(tmp_path / "historical_jobs_2024.parquet") == {"1"}
        kept = pd.read_parquet(tmp_path / "historical_jobs_2025.parquet") \
            .set_index("usajobsControlNumber")
        assert kept.loc[547366900, "positionOpeningStatus"] == "Applications under review"
        prior = json.loads(kept.loc[547366900, PRIOR_COL])
        assert [(p["positionOpenDate"], p["positionOpeningStatus"]) for p in prior] == \
            [("2024-08-18", "Accepting applications")]
        assert pd.isna(kept.loc[2, PRIOR_COL])
        assert moves == [{"control_number": "547366900",
                          "from_file": "historical_jobs_2024.parquet",
                          "to_file": "historical_jobs_2025.parquet",
                          "from_open": "2024-08-18", "to_open": "2025-08-04",
                          "from_status": "Accepting applications"}]

    def test_idempotent(self, tmp_path):
        write(tmp_path / "historical_jobs_2024.parquet", [FAA_2024])
        write(tmp_path / "historical_jobs_2025.parquet", [FAA_2025])
        listings.collapse_across_years(str(tmp_path), log=lambda m: None)
        before = (tmp_path / "historical_jobs_2025.parquet").stat().st_mtime_ns
        assert listings.collapse_across_years(str(tmp_path), log=lambda m: None) == []
        assert (tmp_path / "historical_jobs_2025.parquet").stat().st_mtime_ns == before

    def test_three_files(self, tmp_path):
        third = {**FAA_2025, "positionOpenDate": "2026-02-01",
                 "positionOpeningStatus": "Accepting applications"}
        write(tmp_path / "historical_jobs_2024.parquet", [FAA_2024])
        write(tmp_path / "historical_jobs_2025.parquet", [FAA_2025])
        write(tmp_path / "historical_jobs_2026.parquet", [third])
        moves = listings.collapse_across_years(str(tmp_path), log=lambda m: None)
        assert len(moves) == 2
        assert ids(tmp_path / "historical_jobs_2024.parquet") == set()
        assert ids(tmp_path / "historical_jobs_2025.parquet") == set()
        kept = pd.read_parquet(tmp_path / "historical_jobs_2026.parquet")
        assert [p["positionOpenDate"] for p in json.loads(kept[PRIOR_COL][0])] == \
            ["2024-08-18", "2025-08-04"]

    def test_backups_and_other_families_are_untouched(self, tmp_path):
        write(tmp_path / "historical_jobs_2024.parquet", [FAA_2024])
        write(tmp_path / "historical_jobs_2025_backup.parquet", [FAA_2025])
        write(tmp_path / "scraped_jobs_2025.parquet", [FAA_2025])
        assert listings.collapse_across_years(str(tmp_path), log=lambda m: None) == []

    def test_scraped_files_by_their_own_id_column(self, tmp_path):
        old = {"usajobs_control_number": "547366900", "positionOpenDate": "2024-08-18",
               "text": None}
        new = {"usajobs_control_number": "547366900", "positionOpenDate": "2025-08-04",
               "text": "re-fetched page"}
        write(tmp_path / "scraped_jobs_2024.parquet", [old])
        write(tmp_path / "scraped_jobs_2025.parquet", [new])
        moves = listings.collapse_across_years(str(tmp_path), log=lambda m: None,
                                               prefix=listings.SCRAPED)
        assert [m["from_file"] for m in moves] == ["scraped_jobs_2024.parquet"]
        assert len(pd.read_parquet(tmp_path / "scraped_jobs_2024.parquet")) == 0


class TestCollectorReplace:
    def test_same_year_reopen_keeps_the_earlier_listing(self, tmp_path):
        import collect_data
        path = str(tmp_path / "historical_jobs_2025.parquet")
        first = {**FAA_2025, "positionOpenDate": "2025-02-01",
                 "positionCloseDate": "2025-02-28",
                 "positionOpeningStatus": "Job closed"}
        collect_data.save_jobs_to_parquet([dict(first)], path)
        collect_data.save_jobs_to_parquet([dict(FAA_2025)], path)
        df = pd.read_parquet(path)
        assert len(df) == 1
        assert str(df["positionOpenDate"][0])[:10] == "2025-08-04"
        assert [p["positionOpenDate"] for p in json.loads(df[PRIOR_COL][0])] == \
            ["2025-02-01"]

    def test_a_plain_refresh_keeps_existing_priors(self, tmp_path):
        import collect_data
        path = str(tmp_path / "historical_jobs_2025.parquet")
        collect_data.save_jobs_to_parquet([dict(FAA_2025)], path)
        df = pd.read_parquet(path)
        df[PRIOR_COL] = listings.carry_listing(FAA_2024, FAA_2025)
        df.to_parquet(path, index=False)
        collect_data.save_jobs_to_parquet(
            [{**FAA_2025, "positionOpeningStatus": "Candidate selected"}], path)
        df = pd.read_parquet(path)
        assert df["positionOpeningStatus"][0] == "Candidate selected"
        assert json.loads(df[PRIOR_COL][0])[0]["positionOpenDate"] == "2024-08-18"


class TestRepollRefresh:
    def _file(self, tmp_path, row):
        path = str(tmp_path / "historical_jobs_2026.parquet")
        write(path, [row])
        return path

    def test_extension_updates_the_close_date(self, tmp_path):
        import repoll_status
        row = {"usajobsControlNumber": 835811200, "positionOpenDate": "2025-12-02T00:00:00",
               "positionCloseDate": "2026-06-30T00:00:00", "positionExpireDate": None,
               "positionOpeningStatus": "Accepting applications",
               "announcementNumber": "x", "positionTitle": "GUEST SERVICES REPRESENTATIVE"}
        path = self._file(tmp_path, row)
        api = {**row, "positionOpenDate": "2025-12-02", "positionCloseDate": "2026-10-30"}
        changed, inserted, _ = repoll_status.update_and_insert(
            path, {"835811200": repoll_status.listing_fields(api)}, [])
        df = pd.read_parquet(path)
        assert (changed, inserted) == (1, 0)
        assert df["positionCloseDate"][0][:10] == "2026-10-30"
        assert df["positionOpeningStatus"][0] == "Accepting applications"
        assert PRIOR_COL not in df.columns  # an extension is not a re-open

    def test_same_dates_in_another_format_are_not_a_change(self, tmp_path):
        import repoll_status
        row = {"usajobsControlNumber": 1, "positionOpenDate": "2026-03-02T00:00:00",
               "positionCloseDate": "2026-03-20T00:00:00", "positionExpireDate": None,
               "positionOpeningStatus": "Job closed"}
        path = self._file(tmp_path, row)
        api = {**row, "positionOpenDate": "2026-03-02", "positionCloseDate": "2026-03-20"}
        changed, _, _ = repoll_status.update_and_insert(
            path, {"1": repoll_status.listing_fields(api)}, [])
        assert changed == 0

    def test_reopen_within_the_year_records_the_earlier_listing(self, tmp_path):
        import repoll_status
        row = {"usajobsControlNumber": 2, "positionOpenDate": "2026-02-01",
               "positionCloseDate": "2026-02-15", "positionExpireDate": None,
               "positionOpeningStatus": "Job closed", "announcementNumber": "A",
               "positionTitle": "T"}
        path = self._file(tmp_path, row)
        api = {**row, "positionOpenDate": "2026-09-01", "positionCloseDate": "2026-09-30",
               "positionOpeningStatus": "Accepting applications"}
        changed, _, trans = repoll_status.update_and_insert(
            path, {"2": repoll_status.listing_fields(api)}, [])
        df = pd.read_parquet(path)
        assert changed == 1
        assert df["positionOpenDate"][0] == "2026-09-01"
        assert json.loads(df[PRIOR_COL][0])[0]["positionOpeningStatus"] == "Job closed"
        assert trans[0][1:3] == ("Job closed", "Accepting applications")


class TestScraperRelisted:
    def test_later_open_date_means_fetch_again(self):
        import collect_scraped_data as csd
        stored = {"547366900": {"positionOpenDate": "2024-08-18"},
                  "1": {"positionOpenDate": "2026-10-01"}}
        rows = [{"usajobs_control_number": "547366900", "positionOpenDate": "2025-08-04"},
                {"usajobs_control_number": "1", "positionOpenDate": "2026-10-01"},
                {"usajobs_control_number": "2", "positionOpenDate": "2026-10-02"}]
        assert csd.relisted(rows, stored) == {"547366900"}

    def test_stored_listings_reads_every_year(self, tmp_path):
        import collect_scraped_data as csd
        write(tmp_path / "scraped_jobs_2024.parquet",
              [{"usajobs_control_number": "547366900", "positionOpenDate": "2024-08-18"}])
        write(tmp_path / "scraped_jobs_2026.parquet",
              [{"usajobs_control_number": "1", "positionOpenDate": "2026-10-01"}])
        got = csd.stored_listings(str(tmp_path))
        assert set(got) == {"547366900", "1"}
        assert got["547366900"]["positionOpenDate"] == "2024-08-18"


class TestIntegrityMoves:
    def _load(self):
        import importlib.util
        path = os.path.join(os.path.dirname(__file__), "..", "..", "update",
                            "test_data_integrity.py")
        spec = importlib.util.spec_from_file_location("tdi", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def _log(self, tmp_path, rows):
        logs = tmp_path / "logs"
        logs.mkdir(exist_ok=True)
        with open(logs / "listing_moves_x.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["control_number", "from_file", "to_file"])
            w.writeheader()
            w.writerows(rows)
        return str(logs)

    def test_a_move_counts_only_if_the_target_holds_it(self, tmp_path, capsys):
        tdi = self._load()
        data = tmp_path / "data"
        data.mkdir()
        write(data / "historical_jobs_2025.parquet", [FAA_2025])
        logs = self._log(tmp_path, [
            {"control_number": "547366900", "from_file": "historical_jobs_2024.parquet",
             "to_file": "historical_jobs_2025.parquet"},
            {"control_number": "999", "from_file": "historical_jobs_2024.parquet",
             "to_file": "historical_jobs_2025.parquet"}])
        moved, ok = tdi._moved_out(logs, str(data))
        assert moved == {"historical_jobs_2024.parquet": {"547366900"}}
        assert not ok
