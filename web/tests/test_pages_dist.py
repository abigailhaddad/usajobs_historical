"""Guard rails on what gets published to Cloudflare Pages.

Two sibling sites in this account shipped their whole repo root — CLAUDE.md,
pipeline scripts, CI config, all HTTP 200 — because their deploy used a
deny-list. This file is the regression test for the fix: the deploy publishes
web/dist/, and dist/ is scanned here as a *tree on disk*.

That distinction matters. These tests deliberately do not import the build
script's PAGES / DENY_* constants or re-run its reference resolution. They walk
whatever the build actually produced and judge it on its own. A bug in the build
(a bad glob, a wrong resolve(), a stray shutil.copytree) shows up as a real file
in dist/, and the scan below fails on it — which would not happen if the test
re-derived the same expectations from the same code.

Run:  pytest web/tests/test_pages_dist.py
"""

from __future__ import annotations

import fnmatch
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
WEB_DIR = REPO_ROOT / "web"
BUILD_SCRIPT = REPO_ROOT / "scripts" / "build_pages_dist.py"

# Cloudflare Pages hard limits.
MAX_FILE_BYTES = 26_214_400  # 25 MiB
MAX_FILE_COUNT = 20_000

# Nothing matching any of these may exist anywhere under dist/.
FORBIDDEN_NAME_GLOBS = [
    "*.py",
    "*.pyc",
    "*.pyo",
    "*.parquet",
    "*.csv",
    ".DS_Store",
    ".env",
    "*.env",
    "r2-cors.json",
    "hiring.html",
    "test_server.py",
    ".vercelignore",
    "vercel.json",
]

# Nothing may live under a directory with any of these names.
FORBIDDEN_DIR_NAMES = {
    "api",
    "tests",
    "data",
    "__pycache__",
    ".vercel",
    ".wrangler",
    ".pytest_cache",
    ".git",
    "node_modules",
}

SKIP_PREFIXES = ("http://", "https://", "//", "data:", "mailto:", "tel:", "javascript:", "#")

ATTR_RE = re.compile(r"""\b(?:href|src)\s*=\s*(['"])(?P<url>[^'"]+)\1""", re.IGNORECASE)
FETCH_RE = re.compile(r"""\bfetch\s*\(\s*(['"])(?P<url>[^'"]+)\1""")
IMPORT_FROM_RE = re.compile(r"""\bfrom\s*(['"])(?P<url>[^'"\s]+)\1""")
DYNAMIC_IMPORT_RE = re.compile(r"""\bimport\s*\(\s*(['"])(?P<url>[^'"]+)\1""")
CSS_URL_RE = re.compile(r"""url\(\s*(['"]?)(?P<url>[^'")]+)\1\s*\)""")

REF_PATTERNS = {
    ".html": [ATTR_RE, FETCH_RE, IMPORT_FROM_RE, DYNAMIC_IMPORT_RE],
    ".js": [FETCH_RE, IMPORT_FROM_RE, DYNAMIC_IMPORT_RE],
    ".mjs": [FETCH_RE, IMPORT_FROM_RE, DYNAMIC_IMPORT_RE],
    ".css": [CSS_URL_RE],
}


@pytest.fixture(scope="module")
def dist() -> Path:
    """Run the real build script and hand back the directory it produced."""
    result = subprocess.run(
        [sys.executable, str(BUILD_SCRIPT)],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
    )
    assert result.returncode == 0, (
        f"build_pages_dist.py exited {result.returncode}\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    dist_dir = WEB_DIR / "dist"
    assert dist_dir.is_dir(), "build script did not create web/dist/"
    return dist_dir


def all_files(dist: Path) -> list[Path]:
    return [p for p in dist.rglob("*") if p.is_file()]


def local_refs(path: Path) -> list[str]:
    patterns = REF_PATTERNS.get(path.suffix.lower())
    if not patterns:
        return []
    text = path.read_text(encoding="utf-8", errors="replace")
    out: list[str] = []
    for pattern in patterns:
        for match in pattern.finditer(text):
            url = match.group("url").strip()
            if not url or url.lower().startswith(SKIP_PREFIXES) or "${" in url:
                continue
            url = url.split("#", 1)[0].split("?", 1)[0]
            if url:
                out.append(url)
    return out


# --- (a) every referenced asset resolves inside dist -------------------------


def test_every_local_reference_resolves_inside_dist(dist: Path):
    """A published page may not point at a file that was not published."""
    broken: list[str] = []
    for src in all_files(dist):
        rel_src = src.relative_to(dist).as_posix()
        for ref in local_refs(src):
            if ref.startswith("/"):
                target = dist / ref.lstrip("/")
            else:
                target = src.parent / ref
            try:
                target.resolve().relative_to(dist.resolve())
            except ValueError:
                broken.append(f"{rel_src} -> {ref} (escapes dist/)")
                continue
            if not target.is_file():
                broken.append(f"{rel_src} -> {ref} (not in dist/)")
    assert not broken, "Dangling references in dist/:\n  " + "\n  ".join(broken)


def test_entry_pages_are_present(dist: Path):
    """The pages the site is actually made of."""
    for page in [
        "index.html",
        "pivot.html",
        "poster.html",
        "overlay-poster.html",
        "accessions-poster.html",
        "workforce-flow.html",
        "workforce-pathways.html",
        "transitions.html",
    ]:
        assert (dist / page).is_file(), f"{page} missing from dist/"
    for asset in ["shared/shared.css", "shared/shared.js", "shared/wasm-api.js"]:
        assert (dist / asset).is_file(), f"{asset} missing from dist/"


# --- (b) denylist scan over the ACTUAL dist tree -----------------------------


def test_no_forbidden_files_anywhere_in_dist(dist: Path):
    """Scan what was built, not what the build meant to build."""
    offenders: list[str] = []
    for path in all_files(dist):
        rel = path.relative_to(dist)
        for part in rel.parts[:-1]:
            if part in FORBIDDEN_DIR_NAMES:
                offenders.append(f"{rel.as_posix()} (under forbidden dir {part!r})")
                break
        else:
            for glob in FORBIDDEN_NAME_GLOBS:
                if fnmatch.fnmatch(rel.name, glob):
                    offenders.append(f"{rel.as_posix()} (matches {glob!r})")
                    break
    assert not offenders, "Forbidden files in dist/:\n  " + "\n  ".join(offenders)


def test_no_forbidden_directories_exist(dist: Path):
    """Empty forbidden dirs would not be caught by the file scan above."""
    present = [
        d.relative_to(dist).as_posix()
        for d in dist.rglob("*")
        if d.is_dir() and d.name in FORBIDDEN_DIR_NAMES
    ]
    assert not present, f"Forbidden directories in dist/: {present}"


def test_no_python_source_in_dist(dist: Path):
    assert not list(dist.rglob("*.py")), (
        "Python source in dist/: " + str([p.name for p in dist.rglob("*.py")])
    )


def test_no_parquet_in_dist(dist: Path):
    assert not list(dist.rglob("*.parquet")), "Parquet data in dist/"


def test_hiring_page_is_not_published(dist: Path):
    """Deliberately-unfinished work. It 404s in production; keep it that way."""
    assert not (dist / "hiring.html").exists(), "hiring.html is unfinished — do not publish"
    assert not list(dist.rglob("hiring*")), "hiring.* leaked into dist/"


def test_dist_is_a_subset_of_expected_source_files(dist: Path):
    """Every file in dist/ must have come from web/ — nothing invented, nothing
    dragged in from the repo root."""
    strays = []
    for path in all_files(dist):
        rel = path.relative_to(dist)
        if not (WEB_DIR / rel).is_file():
            strays.append(rel.as_posix())
    assert not strays, f"Files in dist/ with no counterpart in web/: {strays}"


# --- (c) 404 page ------------------------------------------------------------


def test_404_page_exists_and_links_home(dist: Path):
    """Cloudflare Pages has no default 404 — without this file it serves
    index.html with HTTP 200 for every unmatched path."""
    page = dist / "404.html"
    assert page.is_file(), "404.html missing — Pages would return 200 for bad URLs"
    html = page.read_text(encoding="utf-8")
    assert re.search(r"""href=["']/?index\.html["']""", html), (
        "404.html must link back to index.html"
    )


def test_404_page_uses_root_absolute_asset_paths(dist: Path):
    """404.html is served at arbitrary depths (/a/b/c). Relative asset paths
    would resolve against that depth and 404 in turn."""
    html = (dist / "404.html").read_text(encoding="utf-8")
    bad = [
        ref
        for ref in local_refs(dist / "404.html")
        if not ref.startswith("/")
    ]
    assert not bad, f"404.html must use root-absolute local paths, found: {bad}"
    assert html.strip(), "404.html is empty"


# --- (d) + (e) Cloudflare Pages limits ---------------------------------------


def test_no_file_exceeds_cloudflare_25mib_limit(dist: Path):
    too_big = [
        (p.relative_to(dist).as_posix(), p.stat().st_size)
        for p in all_files(dist)
        if p.stat().st_size > MAX_FILE_BYTES
    ]
    assert not too_big, f"Files over 25 MiB (Pages rejects the deploy): {too_big}"


def test_file_count_under_cloudflare_limit(dist: Path):
    count = len(all_files(dist))
    assert count < MAX_FILE_COUNT, f"{count} files exceeds Pages' {MAX_FILE_COUNT} limit"
    assert count > 0, "dist/ is empty"


# --- idempotency -------------------------------------------------------------


def test_build_is_idempotent(dist: Path):
    """A stale file from a previous build must not survive into the next one."""
    stale = dist / "STALE-LEFTOVER.txt"
    stale.write_text("this should be wiped by the next build")
    before = sorted(p.relative_to(dist).as_posix() for p in all_files(dist))

    result = subprocess.run(
        [sys.executable, str(BUILD_SCRIPT), "--quiet"],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
    )
    assert result.returncode == 0, result.stderr

    after = sorted(p.relative_to(dist).as_posix() for p in all_files(dist))
    assert "STALE-LEFTOVER.txt" in before
    assert "STALE-LEFTOVER.txt" not in after, "build did not clear dist/ first"
    assert after == [f for f in before if f != "STALE-LEFTOVER.txt"]
