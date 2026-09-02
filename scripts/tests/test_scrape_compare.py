"""Tests for compare_scrape_to_api.py — the normalization that decides
whether two values 'disagree'.

This is the part that raises the alarms, so its false-positive behavior
matters more than its true-positive behavior: the two collections encode the
same answer differently all over (float vs float-shaped string, Y vs Yes,
timestamp vs date) and none of that is a real divergence.
"""
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from compare_scrape_to_api import compare_series, normalize


def rate(scraped, api):
    return compare_series(pd.Series(scraped), pd.Series(api))


class TestNormalize:
    def test_yes_no_and_y_n_are_the_same_answer(self):
        assert rate(['Y', 'N', 'Y'], ['Yes', 'No', 'true'])['rate'] == 1.0

    def test_salary_float_matches_salary_string(self):
        assert rate([99200.0, 50000.5], ['99200', '50000.50'])['rate'] == 1.0

    def test_date_matches_timestamp_of_the_same_day(self):
        assert rate(['2024-08-26'],
                    ['2024-08-26T13:33:11.4500'])['rate'] == 1.0

    def test_case_and_spacing_are_ignored(self):
        assert rate(['Full-time', 'IT  Specialist'],
                    ['full-time', 'IT Specialist'])['rate'] == 1.0

    def test_genuine_difference_is_still_caught(self):
        r = rate(['Permanent', 'Term'], ['Permanent', 'Temporary'])
        assert r['comparable'] == 2
        assert r['agree'] == 1
        assert r['rate'] == 0.5

    def test_empty_placeholders_count_as_missing_not_as_a_value(self):
        n = normalize(pd.Series(['', 'None', 'nan', 'real']))
        assert list(n.isna()) == [True, True, True, False]


class TestComparableRows:
    def test_only_rows_with_both_sides_present_are_scored(self):
        r = rate(['GS', None, 'GS'], ['GS', 'GS', None])
        assert r['comparable'] == 1
        assert r['agree'] == 1
        assert r['scrape_null'] == 1
        assert r['api_null'] == 1

    def test_no_overlap_gives_no_rate_rather_than_a_zero(self):
        # A field one side never populates must not read as 0% agreement and
        # trip the divergence alarm.
        r = rate(['N', 'N'], [None, None])
        assert r['comparable'] == 0
        assert r['rate'] is None
