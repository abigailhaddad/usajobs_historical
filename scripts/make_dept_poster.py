#!/usr/bin/env python3
"""
Build a multi-panel poster of monthly USAJobs postings, one panel per
cabinet department / branch. Queries the web parquet on Cloudflare R2 with
DuckDB over HTTP range requests — no local data needed.

This used to call /api/aggregate on usajobs-historical.vercel.app. That
endpoint, and the rest of the Python API, was deleted on 2026-08-09 when the
site moved to DuckDB-WASM in the browser; the script now runs the same GROUP BY
itself. web/poster.html renders the same numbers live, from the same parquet.

Output: dept-poster.png in web/ (or --output PATH).

Usage:
    python scripts/make_dept_poster.py
    python scripts/make_dept_poster.py --output /tmp/poster.png
"""
import argparse
import os

import duckdb
from PIL import Image, ImageDraw, ImageFont

PARQUET_URL = "https://pub-317c58882ec04f329b63842c1eb65b0c.r2.dev/web/jobs_5yr.parquet"

# Visual palette — matches the live site
BG = (250, 245, 232)
BAR = (45, 110, 78)
TEXT_DARK = (40, 40, 40)
TEXT_MUTED = (110, 110, 110)
CLIFF = (160, 60, 60)
TILE_BORDER = (225, 218, 200)

# Departments to plot. Filtered to those with meaningful pre-freeze volume.
DEPTS = [
    "Department of Veterans Affairs",
    "Department of the Army",
    "Department of the Navy",
    "Department of the Air Force",
    "Department of Defense",
    "Department of Homeland Security",
    "Other Agencies and Independent Organizations",
    "Department of the Interior",
    "Department of Health and Human Services",
    "Department of Agriculture",
    "Department of the Treasury",
    "Department of Justice",
    "Department of Transportation",
    "Department of Commerce",
    "Department of Energy",
    "General Services Administration",
    "Department of State",
    "Department of Labor",
    "Legislative Branch",
    "Department of Housing and Urban Development",
]

SHORT_NAMES = {
    "Department of Veterans Affairs": "Veterans Affairs",
    "Department of the Army": "Army",
    "Department of the Navy": "Navy",
    "Department of the Air Force": "Air Force",
    "Department of Defense": "Defense (DoD)",
    "Department of Homeland Security": "Homeland Security",
    "Other Agencies and Independent Organizations": "Other Independent Agencies",
    "Department of the Interior": "Interior",
    "Department of Health and Human Services": "Health & Human Services",
    "Department of Agriculture": "Agriculture",
    "Department of the Treasury": "Treasury",
    "Department of Justice": "Justice",
    "Department of Transportation": "Transportation",
    "Department of Commerce": "Commerce",
    "Department of Energy": "Energy",
    "General Services Administration": "GSA",
    "Department of State": "State",
    "Department of Labor": "Labor",
    "Legislative Branch": "Legislative Branch",
    "Department of Housing and Urban Development": "Housing & Urban Dev",
}

# Display window
WINDOW_START = "2020-01"
WINDOW_END = "2026-04"
# Cliff = first full month after the Jan 20, 2025 hiring-freeze EO
CLIFF_MONTH = "2025-02"


def font(size, bold=False):
    candidates = [
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold
        else "/System/Library/Fonts/Helvetica.ttc",
        "/System/Library/Fonts/Helvetica.ttc",
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size)
            except OSError:
                pass
    return ImageFont.load_default()


def fetch_monthly_all():
    """{dept: (labels, counts)} for every department in DEPTS, in one pass.

    Same result /api/aggregate?group_by=month&filter_hiringDepartmentName=…
    returned, including the gap fill: months with no postings still get a label
    and a zero, so the X axis stays continuous and the bars line up with the
    year ticks and the cliff marker.
    """
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    rows = con.execute(
        """
        SELECT "hiringDepartmentName" AS dept,
               strftime(CAST("openDate" AS DATE), '%Y-%m') AS month_label,
               COUNT(*) AS cnt
        FROM read_parquet(?)
        WHERE "openDate" IS NOT NULL
          AND "hiringDepartmentName" IN ?
        GROUP BY dept, month_label
        ORDER BY dept, month_label
        """,
        [PARQUET_URL, DEPTS],
    ).fetchall()

    by_dept = {}
    for dept, month_label, cnt in rows:
        by_dept.setdefault(dept, {})[month_label] = cnt

    out = {}
    for dept in DEPTS:
        months = by_dept.get(dept)
        if not months:
            out[dept] = ([], [])
            continue
        first, last = min(months), max(months)
        labels, counts = [], []
        year, month = int(first[:4]), int(first[5:7])
        end_year, end_month = int(last[:4]), int(last[5:7])
        while (year, month) <= (end_year, end_month):
            label = f"{year:04d}-{month:02d}"
            labels.append(label)
            counts.append(months.get(label, 0))
            month += 1
            if month > 12:
                month, year = 1, year + 1
        out[dept] = (labels, counts)
    return out


def post_pre_pct(labels, counts):
    """Post-freeze monthly avg / pre-freeze monthly avg. Used only for sorting."""
    pre, post = [], []
    for label, count in zip(labels, counts):
        if label <= "2025-01":
            pre.append(count)
        elif label <= WINDOW_END:
            post.append(count)
    pre_avg = sum(pre) / len(pre) if pre else 0
    post_avg = sum(post) / len(post) if post else 0
    return (post_avg / pre_avg) if pre_avg else 0


def filter_window(labels, counts):
    out_l, out_c = [], []
    for label, count in zip(labels, counts):
        if WINDOW_START <= label <= WINDOW_END:
            out_l.append(label)
            out_c.append(count)
    return out_l, out_c


def draw_tile(dept, labels, counts, tile_w, tile_h, scale=1):
    pad_l, pad_r = 50 * scale, 20 * scale
    pad_t, pad_b = 70 * scale, 50 * scale
    title_font = font(28 * scale, bold=True)
    axis_font = font(14 * scale)

    img = Image.new("RGB", (tile_w, tile_h), BG)
    draw = ImageDraw.Draw(img)
    draw.rectangle([0, 0, tile_w - 1, tile_h - 1], outline=TILE_BORDER, width=scale)

    draw.text((pad_l - 12 * scale, 18 * scale), SHORT_NAMES[dept], fill=TEXT_DARK, font=title_font)

    cx0, cy0 = pad_l, pad_t
    cx1, cy1 = tile_w - pad_r, tile_h - pad_b
    chart_w = cx1 - cx0
    chart_h = cy1 - cy0
    if not counts:
        return img

    n = len(counts)
    bar_w = chart_w / n
    max_v = max(counts) or 1

    for i, v in enumerate(counts):
        h = (v / max_v) * chart_h
        x0 = cx0 + i * bar_w
        x1 = x0 + bar_w * 0.9
        draw.rectangle([x0, cy1 - h, x1, cy1], fill=BAR)

    if CLIFF_MONTH in labels:
        cliff_idx = labels.index(CLIFF_MONTH)
        x_cliff = cx0 + cliff_idx * bar_w
        dash = 8 * scale
        gap_d = 4 * scale
        for y in range(int(cy0), int(cy1), dash):
            draw.line([(x_cliff, y), (x_cliff, min(y + gap_d, cy1))],
                      fill=CLIFF, width=3 * scale)

    seen_years = set()
    for i, label in enumerate(labels):
        year, month = label.split("-")
        if int(month) == 1 and year not in seen_years:
            seen_years.add(year)
            if int(year) % 2 == 0:
                x = cx0 + i * bar_w
                draw.text((x - 6 * scale, cy1 + 6 * scale), year,
                          fill=TEXT_MUTED, font=axis_font)

    draw.text((6 * scale, cy0 - 8 * scale), f"{max_v:,.0f}",
              fill=TEXT_MUTED, font=axis_font)
    return img


def build_poster(output_path, scale=2):
    """Render the poster. scale=2 produces a high-DPI image that stays crisp
    when zoomed in; scale=1 gives a smaller file for quick previews."""
    print("Querying monthly data for", len(DEPTS), "departments from R2...")
    monthly = fetch_monthly_all()
    series = {}
    for dept in DEPTS:
        labels, counts = monthly[dept]
        pct = post_pre_pct(labels, counts)
        labels, counts = filter_window(labels, counts)
        series[dept] = (labels, counts, pct)

    # Sort by recovery (least to most), purely for layout
    ordered = sorted(DEPTS, key=lambda d: series[d][2])

    cols = 4
    rows = (len(ordered) + cols - 1) // cols
    tile_w, tile_h = 720 * scale, 280 * scale
    gap = 16 * scale
    header_h = 280 * scale
    footer_h = 50 * scale

    grid_w = cols * tile_w + (cols + 1) * gap
    grid_h = header_h + rows * tile_h + (rows + 1) * gap + footer_h

    poster = Image.new("RGB", (grid_w, grid_h), BG)
    draw = ImageDraw.Draw(poster)

    h1 = font(72 * scale, bold=True)
    h2 = font(26 * scale)
    h3 = font(22 * scale)
    foot = font(20 * scale)

    draw.text((gap * 2, 30 * scale), "USAJobs postings by month",
              fill=TEXT_DARK, font=h1)
    draw.text(
        (gap * 2, 115 * scale),
        "By cabinet department / branch  •  2020 – April 2026  •  "
        "Dashed red line = federal hiring freeze (EO signed Jan 20, 2025)",
        fill=TEXT_MUTED, font=h2,
    )
    draw.text(
        (gap * 2, 165 * scale),
        "USAJobs is the federal government's public job board. "
        "Not all federal jobs are posted there",
        fill=TEXT_MUTED, font=h3,
    )
    draw.text(
        (gap * 2, 200 * scale),
        "(e.g. FBI, CIA, USPS use their own systems), and a single "
        "posting can result in 0, 1, or many actual hires.",
        fill=TEXT_MUTED, font=h3,
    )

    for i, dept in enumerate(ordered):
        row, col = divmod(i, cols)
        x = gap + col * (tile_w + gap)
        y = header_h + gap + row * (tile_h + gap)
        labels, counts, _ = series[dept]
        poster.paste(draw_tile(dept, labels, counts, tile_w, tile_h, scale=scale), (x, y))

    draw.text(
        (gap * 2, grid_h - 38 * scale),
        "Source: USAJobs Historical | usajobs-historical.abigailhaddad.com",
        fill=TEXT_MUTED, font=foot,
    )

    poster.save(output_path, "PNG", optimize=True)
    print(f"Saved: {output_path}  ({grid_w}x{grid_h})")


def main():
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    default_out = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "web", "dept-poster.png",
    )
    parser.add_argument(
        "--output", "-o", default=default_out,
        help=f"Output PNG path (default: {default_out})",
    )
    parser.add_argument(
        "--scale", "-s", type=int, default=2,
        help="Resolution multiplier — 2 (default) is high-DPI for crisp zooming",
    )
    args = parser.parse_args()
    build_poster(args.output, scale=args.scale)


if __name__ == "__main__":
    main()
