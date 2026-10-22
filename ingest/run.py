"""One ingest run: every (endpoint, area) from its cursor through today, one file per
range, the cursor saved after every file, then the pipeline started. A failed range stops
that area for this run (no gap past it); the time budget leaves the rest for the next run."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date

from databricks.sdk.errors import DatabricksError

from pipeline.model import COUNTRIES, ZONE_OF

from . import cursor as cur
from .energy_charts import FetchError

STATE = "_state/cursor.json"
KEYS = [k for c in COUNTRIES for k in (("power", c), ("price", ZONE_OF[c]))]


@dataclass
class Summary:
    files: int = 0
    errors: list[str] = field(default_factory=list)
    skipped: int = 0  # ranges left for the next run


def path(endpoint: str, area: str, start: date, end: date) -> str:
    return f"{endpoint}/{area}/{start.isoformat()}_{end.isoformat()}.json"


def run(
    store,
    api,
    *,
    today: date,
    since: date | None = None,
    budget_s: float,
    clock=time.monotonic,
    refresh: Callable[[], None] | None = None,
) -> Summary:
    cursor = cur.load(store.get(STATE))
    queues = {k: cur.ranges(cursor.get("/".join(k)), today, since) for k in KEYS}
    depth = max((len(q) for q in queues.values()), default=0)
    order = [(k, queues[k][i]) for i in range(depth) for k in KEYS if i < len(queues[k])]

    summary = Summary()
    failed: set[tuple[str, str]] = set()
    started = clock()
    for (endpoint, area), (start, end) in order:
        if (endpoint, area) in failed or clock() - started > budget_s:
            summary.skipped += 1
            continue
        try:
            body = api.fetch(endpoint, area, start, end)
        except FetchError as e:
            summary.errors.append(str(e))
            failed.add((endpoint, area))
            continue
        try:
            store.put(path(endpoint, area, start, end), body)
        except DatabricksError as e:  # a volume hiccup costs this file, not the files after it
            summary.errors.append(f"{endpoint} {area} {start}..{end}: store: {e}")
            failed.add((endpoint, area))
            continue
        cur.advance(cursor, f"{endpoint}/{area}", start, end, today)
        summary.files += 1
        try:
            store.put(STATE, cur.dump(cursor))
        except DatabricksError as e:  # the next cursor write catches up; carry on
            summary.errors.append(f"cursor: store: {e}")
    if refresh is not None and summary.files:
        refresh()
    return summary
