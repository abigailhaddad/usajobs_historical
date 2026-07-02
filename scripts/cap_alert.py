#!/usr/bin/env python3
"""
Tripwire for pagination caps.

Landing *exactly* on a known page-size (500) or max-results ceiling (10,000)
is the classic signature of pagination silently stopping — the API had more
rows but we took only the first page/batch. Any collection that produces one
of these exact counts appends a loud line to logs/CAP_HIT_WARNING.txt, which
the daily and repoll workflows turn into a GitHub issue.

False positives are possible (a query legitimately having exactly 500 rows),
but we would rather investigate a harmless coincidence than ship truncated
numbers.
"""

import os
from datetime import datetime

# Exact counts that mean "you probably hit a cap, not the real end".
CAP_VALUES = {500, 10000}

_MARKER = os.path.join(os.path.dirname(__file__), '..', 'logs', 'CAP_HIT_WARNING.txt')


def check_cap(count, context):
    """If `count` is exactly a known cap value, record it and return True.

    `context` is a short human string describing what was being fetched
    (e.g. "current jobs total" or "repoll date 2025-03-14").
    """
    try:
        count = int(count)
    except (TypeError, ValueError):
        return False

    if count in CAP_VALUES:
        os.makedirs(os.path.dirname(_MARKER), exist_ok=True)
        with open(_MARKER, 'a') as f:
            f.write(f"{datetime.now().isoformat()} | {context} | count == {count} "
                    f"(exact page/max-results boundary — possible silent truncation)\n")
        print(f"🚨 CAP TRIPWIRE: {context} returned exactly {count} — "
              f"logged to CAP_HIT_WARNING.txt for the workflow to alert on.")
        return True
    return False
