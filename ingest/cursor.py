"""Which date ranges to fetch next. The cursor keeps, per (endpoint, area), the last day
fetched after it was over (Central European time). Ranges are at most a calendar month:
one request each, about 650 for the whole backfill."""

from __future__ import annotations

import calendar
import json
from datetime import date, timedelta

DEFAULT_SINCE = date(2023, 1, 1)
DAY = timedelta(days=1)


def load(text: bytes | str | None) -> dict[str, date]:
    if not text:
        return {}
    return {k: date.fromisoformat(v) for k, v in json.loads(text).items()}


def dump(cursor: dict[str, date]) -> bytes:
    data = {k: v.isoformat() for k, v in sorted(cursor.items())}
    return (json.dumps(data, indent=1) + "\n").encode()


def ranges(last: date | None, today: date, since: date | None = None) -> list[tuple[date, date]]:
    """From where we are through today; yesterday is always fetched again (late corrections)."""
    if since is not None:
        start = since
    elif last is None:
        start = DEFAULT_SINCE
    else:
        start = min(last + DAY, today - DAY)
    out = []
    while start <= today:
        month_end = date(start.year, start.month, calendar.monthrange(start.year, start.month)[1])
        end = min(month_end, today)
        out.append((start, end))
        start = end + DAY
    return out


def advance(cursor: dict[str, date], key: str, start: date, end: date, today: date) -> None:
    """Record the last finished day a fetched range covered; never move back, and never jump
    over days not yet fetched (a `--since` later than the cursor must not leave a gap)."""
    done = min(end, today - DAY)
    if key in cursor:
        contiguous = start <= cursor[key] + DAY
    else:
        contiguous = start <= DEFAULT_SINCE
    if contiguous and done >= start and (key not in cursor or done > cursor[key]):
        cursor[key] = done
