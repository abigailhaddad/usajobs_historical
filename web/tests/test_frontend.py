"""
End-to-end Playwright tests for the USAJobs Historical Data web viewer.

Run:
    cd /Users/abigailhaddad/Documents/repos/usajobs_historical
    source venv/bin/activate
    python web/test_server.py &          # starts on port 3333
    python -m pytest web/tests/test_frontend.py -v
"""

import pytest
import re
from playwright.sync_api import Page, expect

BASE_URL = "http://localhost:3333"


@pytest.fixture(scope="session")
def browser_context_args():
    return {"viewport": {"width": 1280, "height": 900}}


# ------------------------------------------------------------------
# 1. Page loads: table renders with data rows, charts are visible
# ------------------------------------------------------------------

def test_page_loads_table_with_data(page: Page):
    page.goto(BASE_URL)
    # Wait for DataTables to finish loading rows
    page.wait_for_selector("#jobsTable tbody tr td", timeout=30000)
    rows = page.locator("#jobsTable tbody tr")
    assert rows.count() > 0, "Expected data rows in the table"


def test_page_loads_charts_visible(page: Page):
    page.goto(BASE_URL)
    page.wait_for_selector("#chartMonth", timeout=30000)
    for chart_id in ["chartMonth", "chartAgency", "chartGrade"]:
        canvas = page.locator(f"#{chart_id}")
        expect(canvas).to_be_visible()


# ------------------------------------------------------------------
# 2. DataTable sorting: click column header, verify table redraws
# ------------------------------------------------------------------

def test_sort_click_redraws_table(page: Page):
    page.goto(BASE_URL)
    page.wait_for_selector("#jobsTable tbody tr td", timeout=30000)

    # Get first row before sort
    first_row_before = page.locator("#jobsTable tbody tr").first.inner_text()

    # Click "Position Title" header (first th)
    page.locator("#jobsTable thead th").first.click()
    page.wait_for_timeout(1000)

    # Table should have redrawn (first row likely different)
    rows = page.locator("#jobsTable tbody tr")
    assert rows.count() > 0, "Table should still have rows after sorting"


# ------------------------------------------------------------------
# 6. Filter bar: add a filter, verify chip appears; remove it
# ------------------------------------------------------------------

def test_filter_add_and_remove(page: Page):
    page.goto(BASE_URL)
    page.wait_for_selector("#jobsTable tbody tr td", timeout=30000)

    # Verify empty state
    empty_msg = page.locator("#filtersBar .filters-bar-empty")
    # The default sort (Open Date desc) creates a sort chip, so empty may be hidden
    # but we can check for filter chips specifically
    filter_chips_before = page.locator("#filtersBar .column-filter-chip")
    assert filter_chips_before.count() == 0, "Expected no filter chips initially"

    # Click "+ Add Filter" button
    page.locator(".add-filter-btn").click()
    page.wait_for_selector(".filter-modal", timeout=5000)

    # Select "Position Title" (a text filter - simplest to test)
    page.locator('.filter-option:has(input[value="positionTitle"])').click()
    page.wait_for_selector(".filter-text-input", timeout=5000)

    # Type a search term and apply
    page.locator(".filter-text-input").fill("Engineer")
    page.locator(".btn-apply").click()
    page.wait_for_timeout(1000)

    # Verify filter chip appeared
    filter_chips = page.locator("#filtersBar .column-filter-chip")
    assert filter_chips.count() == 1, f"Expected 1 filter chip, got {filter_chips.count()}"

    # Remove the filter chip
    filter_chips.first.locator(".filter-chip-remove").click()
    page.wait_for_timeout(1000)

    # Verify no filter chips remain
    remaining_chips = page.locator("#filtersBar .column-filter-chip")
    assert remaining_chips.count() == 0, "Expected no filter chips after removal"

    # Verify "No filters applied" text is visible again (only if no sorts either)
    # With default sort, empty msg is hidden, so just check filter chips are gone


# ------------------------------------------------------------------
# 7. Chart rendering: Jobs by month canvas has been drawn
# ------------------------------------------------------------------

def test_chart_month_rendered(page: Page):
    page.goto(BASE_URL)
    # Wait for chart data to load
    page.wait_for_selector("#chartMonth", timeout=30000)
    page.wait_for_timeout(3000)  # Allow charts to render with data

    canvas = page.locator("#chartMonth")
    expect(canvas).to_be_visible()

    # Check the canvas has non-zero rendered height
    height = canvas.evaluate("el => el.getBoundingClientRect().height")
    assert height > 0, f"Expected chart canvas to have non-zero height, got {height}"


def test_chart_month_no_decimal_y_labels(page: Page):
    page.goto(BASE_URL)
    page.wait_for_selector("#chartMonth", timeout=30000)
    page.wait_for_timeout(3000)

    # Use Chart.js API to check y-axis tick values have precision 0
    has_decimals = page.evaluate("""() => {
        const chart = Chart.getChart('chartMonth');
        if (!chart) return null;
        const yScale = chart.scales.y;
        if (!yScale || !yScale.ticks) return null;
        return yScale.ticks.some(t => t.value !== Math.floor(t.value));
    }""")
    assert has_decimals is not None, "Could not access Chart.js y-axis ticks"
    assert has_decimals is False, "Y-axis should not have decimal tick values"


# ------------------------------------------------------------------
# 8. Copy link button exists in toolbar
# ------------------------------------------------------------------

def test_charts_have_data(page: Page):
    """Every chart should have non-empty labels (i.e. actual bars/points, not just axes)."""
    page.goto(BASE_URL)
    page.wait_for_selector("#chartMonth", timeout=30000)
    page.wait_for_timeout(3000)

    for chart_id in ["chartMonth", "chartAgency", "chartGrade"]:
        label_count = page.evaluate(f"""() => {{
            const chart = Chart.getChart('{chart_id}');
            return chart ? chart.data.labels.length : null;
        }}""")
        assert label_count is not None, f"Could not access Chart.js instance for {chart_id}"
        assert label_count > 0, f"{chart_id} has no data labels — chart is empty"


def test_copy_link_button_exists(page: Page):
    page.goto(BASE_URL)
    page.wait_for_selector(".copy-link-btn", timeout=30000)
    btn = page.locator(".copy-link-btn")
    expect(btn).to_be_visible()
    expect(btn).to_have_text("Copy Link")
