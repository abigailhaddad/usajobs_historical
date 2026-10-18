"""Tests for the markup-drift check and the September 2026 "Help" repair."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from repair_help_labels import AFFECTED, is_affected, repair_row, repair_table
from section_shape import SECTIONS, drift, merge, profile


def columns(rows):
    return {s: [r.get(s) for r in rows] for s in SECTIONS}


def clean_row(i):
    return {
        "majorDuties": f"The incumbent does task {i} and more",
        "requirements": f"Conditions of employment item {i} ends here",
        "benefits": "A career with the U.S. government provides benefits offered.",
        "requiredDocuments": f"To apply for this {i} the overall rating.",
        "howToApply": f"Please read the {i} Hide next steps",
        "jobSummary": f"This position {i} about this agency",
    }


def leaked(row):
    row = dict(row)
    for f in AFFECTED:
        row[f] = "Help " + row[f]
    # Where the live pages put it: mid-text, between two words.
    row["benefits"] = row["benefits"].replace(
        "provides ", "provides Opens in a new window Learn more about federal "
                     "benefits ")
    return row


class TestDrift:
    def test_the_same_shape_is_quiet(self):
        base = profile(columns([clean_row(i) for i in range(500)]))
        assert drift(profile(columns([clean_row(i) for i in range(500, 900)])),
                     base) == []

    def test_the_september_leak_is_caught(self):
        base = profile(columns([clean_row(i) for i in range(500)]))
        rows = [leaked(clean_row(i)) if i % 2 else clean_row(i)
                for i in range(400)]
        found = drift(profile(columns(rows)), base)
        assert any("majorDuties" in f and "'Help'" in f for f in found)
        assert any("requirements" in f and "'Conditions'" in f for f in found)

    def test_a_small_batch_is_not_judged(self):
        base = profile(columns([clean_row(i) for i in range(500)]))
        rows = [leaked(clean_row(i)) for i in range(50)]
        assert drift(profile(columns(rows)), base) == []

    def test_merge_weights_by_rows(self):
        a = profile(columns([clean_row(i) for i in range(300)]))
        b = profile(columns([leaked(clean_row(i)) for i in range(100)]))
        merged = merge([a, b])
        assert abs(merged["majorDuties"]["first"]["Help"] - 0.25) < 1e-9


class TestRepair:
    def test_a_leaked_row_is_restored(self):
        assert repair_row(leaked(clean_row(1))) == clean_row(1)

    def test_genuine_help_text_is_left_alone(self):
        # A real July 2026 majorDuties starts with the word.
        row = clean_row(1)
        row["majorDuties"] = "Help develop and guide short- and long-term strategies"
        assert not is_affected(row)
        assert repair_row(row) is row

    def test_an_empty_section_does_not_block_the_repair(self):
        row = leaked(clean_row(1))
        row["requiredDocuments"] = None
        fixed = repair_row(row)
        assert fixed["majorDuties"] == clean_row(1)["majorDuties"]
        assert fixed["requiredDocuments"] is None

    def test_a_section_that_was_only_the_button_becomes_null(self):
        # 885523500: benefits was just "Help"; the fixed parser omits it.
        row = leaked(clean_row(1))
        row["benefits"] = "Help"
        assert is_affected(row)
        assert repair_row(row)["benefits"] is None

    def test_repair_table_touches_only_the_affected_columns(self):
        import pyarrow as pa
        rows = [leaked(clean_row(1)), clean_row(2)]
        for r in rows:
            r["text"] = "Help whole page text stays as it was"
        t = pa.Table.from_pylist(rows)
        fixed, changed = repair_table(t)
        assert changed == 1
        assert fixed.column("text").equals(t.column("text"))
        assert fixed.column("majorDuties").to_pylist()[0] == \
            clean_row(1)["majorDuties"]
