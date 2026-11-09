"""backfill_scraped_pages.published_control_numbers: a manifest that cannot be read is
not an empty dataset."""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import backfill_scraped_pages as bsp  # noqa: E402


def _patch(monkeypatch, exc):
    import huggingface_hub

    def boom(*a, **k):
        raise exc
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", boom)


def test_missing_manifest_means_nothing_published(monkeypatch):
    from huggingface_hub.errors import EntryNotFoundError
    _patch(monkeypatch, EntryNotFoundError("404"))
    assert bsp.published_control_numbers() == set()


def test_other_errors_raise_instead_of_returning_empty(monkeypatch):
    _patch(monkeypatch, TimeoutError("429 / timeout"))
    with pytest.raises(TimeoutError):
        bsp.published_control_numbers()
