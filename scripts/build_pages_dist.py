#!/usr/bin/env python3
"""Assemble web/dist/ — the exact set of files Cloudflare Pages should publish.

Why this exists
---------------
Two sibling sites in this account were publishing their entire repo root
(CLAUDE.md, pipeline scripts, CI config, all HTTP 200) because their deploy used
a *deny*-list: anything nobody remembered to exclude went live. The rule adopted
after that: deploy an ALLOW-LIST directory, built from scratch every time, and
have CI fail if anything else lands in it.

So this script does not exclude bad files from web/. It starts from the site's
entry pages and copies only what those pages actually reference, transitively:

    web/*.html (allow-listed pages)
      -> href= / src= / fetch('...') / import ... from '...'
        -> .js  -> import / fetch of local paths
        -> .css -> url(...) of local paths

Anything not reachable from an entry page never enters dist/. The 122 MB
parquet, api/*.py, tests/, .DS_Store, __pycache__ etc. are not referenced by any
page (the browser reads the parquet and static.json straight from R2), so they
are excluded by construction rather than by remembering to list them.

Two belt-and-suspenders guards on top of that:
  * PAGES / EXCLUDED_PAGES are explicit. A new .html dropped into web/ that is
    in neither list aborts the build, so unfinished work can never drift into
    the deploy the way it would with a deny-list.
  * DENY_* patterns abort the build if a *reference* ever resolves to something
    that must not ship. A bad ref should be a loud failure, not a silent skip.

Usage:  python scripts/build_pages_dist.py [--web DIR] [--dist DIR] [--quiet]
"""

from __future__ import annotations

import argparse
import fnmatch
import re
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
WEB_DIR = REPO_ROOT / "web"

# --- Entry points -----------------------------------------------------------
# Every page the live site serves. Everything else in dist/ is pulled in because
# one of these references it.
PAGES = [
    "index.html",
    "pivot.html",
    "poster.html",
    "overlay-poster.html",
    "accessions-poster.html",
    "workforce-flow.html",
    "workforce-pathways.html",
    "transitions.html",
    "404.html",
]

# Deliberately-unfinished work. hiring.html is unlinked from the index, its
# api/hiring_stats.py backend no longer exists, and it 404s in production today.
# Keep it that way until it is finished on purpose. Removing it from here is the
# single, deliberate act that publishes it.
EXCLUDED_PAGES = {
    "hiring.html",
}

# --- Guards -----------------------------------------------------------------
# If a reference ever resolves to one of these, the build aborts. These are a
# tripwire, not the mechanism that keeps them out (reachability does that).
DENY_GLOBS = [
    "*.py",
    "*.pyc",
    "*.parquet",
    "*.csv",
    ".DS_Store",
    "*.env",
]
DENY_DIR_NAMES = {
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
DENY_EXACT = {
    "test_server.py",
    "r2-cors.json",
    "vercel.json",
    ".vercelignore",
    ".gitignore",
}

# Cloudflare Pages hard limits.
MAX_FILE_BYTES = 26_214_400  # 25 MiB per file
MAX_FILE_COUNT = 20_000

# --- Reference extraction ---------------------------------------------------
ATTR_RE = re.compile(r"""\b(?:href|src)\s*=\s*(['"])(?P<url>[^'"]+)\1""", re.IGNORECASE)
FETCH_RE = re.compile(r"""\bfetch\s*\(\s*(['"])(?P<url>[^'"]+)\1""")
DYNAMIC_IMPORT_RE = re.compile(r"""\bimport\s*\(\s*(['"])(?P<url>[^'"]+)\1""")
# A module specifier never contains whitespace. Requiring that is what keeps this
# from matching ordinary English inside a JS string — e.g.
#   'Salary from ' + fmt(min) + ' to ' + fmt(max) + ''
# whose `from ' + fmt(min) + '` reads exactly like an import if you allow spaces.
IMPORT_FROM_RE = re.compile(r"""\bfrom\s*(['"])(?P<url>[^'"\s]+)\1""")
CSS_URL_RE = re.compile(r"""url\(\s*(['"]?)(?P<url>[^'")]+)\1\s*\)""")

HTML_PATTERNS = [ATTR_RE, FETCH_RE, DYNAMIC_IMPORT_RE, IMPORT_FROM_RE]
JS_PATTERNS = [FETCH_RE, DYNAMIC_IMPORT_RE, IMPORT_FROM_RE]
CSS_PATTERNS = [CSS_URL_RE]

PARSERS = {
    ".html": HTML_PATTERNS,
    ".htm": HTML_PATTERNS,
    ".js": JS_PATTERNS,
    ".mjs": JS_PATTERNS,
    ".css": CSS_PATTERNS,
}

SKIP_PREFIXES = ("http://", "https://", "//", "data:", "mailto:", "tel:", "javascript:", "#")


class BuildError(Exception):
    """Something is wrong enough that publishing would be unsafe."""


def is_denied(rel: str) -> str | None:
    """Return a reason string if this repo-relative path must never ship."""
    parts = Path(rel).parts
    for part in parts[:-1]:
        if part in DENY_DIR_NAMES:
            return f"lives under forbidden directory {part!r}"
    name = parts[-1]
    if name in DENY_EXACT:
        return f"{name!r} is on the never-publish list"
    for pattern in DENY_GLOBS:
        if fnmatch.fnmatch(name, pattern):
            return f"matches forbidden pattern {pattern!r}"
    return None


def extract_refs(path: Path) -> list[str]:
    """Local (non-remote) references from an HTML/JS/CSS file, in source order."""
    patterns = PARSERS.get(path.suffix.lower())
    if patterns is None:
        return []
    text = path.read_text(encoding="utf-8", errors="replace")
    refs: list[str] = []
    for pattern in patterns:
        for match in pattern.finditer(text):
            url = match.group("url").strip()
            if not url or url.lower().startswith(SKIP_PREFIXES):
                continue
            if "${" in url:  # template literal — resolved at runtime, not a file
                continue
            url = url.split("#", 1)[0].split("?", 1)[0]
            if not url or url in refs:
                continue
            refs.append(url)
    return refs


def resolve(ref: str, source_rel: str, web_dir: Path) -> str | None:
    """Resolve a reference to a web/-relative path, or None if it is not a file."""
    if ref.startswith("/"):
        candidate = (web_dir / ref.lstrip("/")).resolve()
    else:
        candidate = (web_dir / source_rel).parent.joinpath(ref).resolve()
    try:
        rel = candidate.relative_to(web_dir.resolve())
    except ValueError:
        raise BuildError(
            f"{source_rel} references {ref!r}, which escapes web/ "
            f"(resolves to {candidate}). Refusing to build."
        )
    return rel.as_posix()


def check_entry_pages(web_dir: Path) -> None:
    """Every root-level .html must be deliberately published or deliberately not."""
    known = set(PAGES) | EXCLUDED_PAGES
    found = {p.name for p in web_dir.glob("*.html")}
    unknown = sorted(found - known)
    if unknown:
        raise BuildError(
            "Unclassified page(s) in web/: "
            + ", ".join(unknown)
            + "\nAdd each to PAGES (to publish) or EXCLUDED_PAGES (to keep private) "
              "in scripts/build_pages_dist.py. Refusing to guess."
        )
    missing = [p for p in PAGES if not (web_dir / p).is_file()]
    if missing:
        raise BuildError("PAGES lists file(s) that do not exist: " + ", ".join(missing))


def collect(web_dir: Path) -> tuple[list[str], list[tuple[str, str]]]:
    """Breadth-first walk from the entry pages. Returns (files, missing_refs)."""
    queue = list(PAGES)
    seen: set[str] = set(queue)
    files: list[str] = []
    missing: list[tuple[str, str]] = []

    while queue:
        rel = queue.pop(0)
        src = web_dir / rel

        if rel in EXCLUDED_PAGES:
            raise BuildError(f"{rel} is on EXCLUDED_PAGES but something references it.")
        reason = is_denied(rel)
        if reason is not None:
            raise BuildError(f"Reference resolved to {rel} — {reason}. Refusing to build.")
        if not src.is_file():
            missing.append((rel, "referenced file does not exist"))
            continue

        files.append(rel)
        for ref in extract_refs(src):
            target = resolve(ref, rel, web_dir)
            if target is None or target in seen:
                continue
            seen.add(target)
            queue.append(target)

    return sorted(files), missing


def human(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024.0
    return f"{n} B"


def build(web_dir: Path, dist_dir: Path, quiet: bool = False) -> list[str]:
    check_entry_pages(web_dir)
    files, missing = collect(web_dir)

    if missing:
        details = "\n".join(f"  {rel}: {why}" for rel, why in missing)
        raise BuildError(f"Referenced files are missing:\n{details}")

    # Idempotent: nothing survives from a previous run.
    if dist_dir.exists():
        shutil.rmtree(dist_dir)
    dist_dir.mkdir(parents=True)

    total = 0
    oversized: list[tuple[str, int]] = []
    for rel in files:
        src = web_dir / rel
        dst = dist_dir / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        size = dst.stat().st_size
        total += size
        if size > MAX_FILE_BYTES:
            oversized.append((rel, size))
        if not quiet:
            print(f"  {rel:<32} {human(size):>10}")

    if oversized:
        raise BuildError(
            "File(s) exceed Cloudflare Pages' 25 MiB per-file limit:\n"
            + "\n".join(f"  {rel} ({human(size)})" for rel, size in oversized)
        )
    if len(files) > MAX_FILE_COUNT:
        raise BuildError(
            f"{len(files)} files exceeds Cloudflare Pages' {MAX_FILE_COUNT}-file limit."
        )

    if not quiet:
        print(f"\n{len(files)} files, {human(total)} total -> {dist_dir}")
        if EXCLUDED_PAGES:
            print("Deliberately NOT published: " + ", ".join(sorted(EXCLUDED_PAGES)))
    return files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--web", type=Path, default=WEB_DIR, help="site source directory")
    parser.add_argument("--dist", type=Path, default=None, help="output directory")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    web_dir = args.web.resolve()
    dist_dir = (args.dist or web_dir / "dist").resolve()

    if dist_dir == web_dir or web_dir in dist_dir.parents and dist_dir.name != "dist":
        print(f"Refusing to use {dist_dir} as the output directory.", file=sys.stderr)
        return 2

    if not args.quiet:
        print(f"Building {dist_dir} from {web_dir}\n")
    try:
        build(web_dir, dist_dir, quiet=args.quiet)
    except BuildError as exc:
        print(f"\nBUILD FAILED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
