"""The gold tables through the SQL warehouse. Results are cached for 10 minutes (the data
changes every 6 hours); any failure becomes Unavailable, which the UI turns into a message."""

from __future__ import annotations

import logging
import os
import time
from datetime import date

import pandas as pd

CATALOG = os.environ.get("ENERGY_CATALOG") or "workspace"
# Errors that waiting will not fix: no credentials, a missing table or grant (README: one-time
# steps). Matched upper-cased against the error text.
SETUP_ERRORS = (
    "CANNOT CONFIGURE DEFAULT CREDENTIALS",
    "DATABRICKS_WAREHOUSE_ID",
    "TABLE_OR_VIEW_NOT_FOUND",
    "SCHEMA_NOT_FOUND",
    "CATALOG_NOT_FOUND",
    "INSUFFICIENT_PERMISSIONS",
    "PERMISSION_DENIED",
)
log = logging.getLogger(__name__)

DAY = """SELECT local_hour, renewable_share, eur_mwh, rank_green, rank_cheap
FROM {c}.gold.green_hours
WHERE country = :country AND local_date = :day
ORDER BY local_hour"""

FRESHNESS = """SELECT endpoint, area, latest_ts,
  round((unix_timestamp(current_timestamp()) - unix_timestamp(latest_ts)) / 3600, 1) AS age_hours
FROM {c}.gold.freshness
ORDER BY endpoint, area"""

EXPECTATIONS = """SELECT e.dataset, e.name AS rule,
  sum(e.passed_records) AS passed, sum(e.failed_records) AS failed
FROM {c}.gold.pipeline_events
LATERAL VIEW explode(from_json(details:flow_progress.data_quality.expectations,
  'array<struct<name: string, dataset: string, passed_records: bigint, failed_records: bigint>>')) x AS e
WHERE event_type = 'flow_progress' AND timestamp >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY e.dataset, e.name
ORDER BY e.dataset, e.name"""

UPDATES = """SELECT timestamp, details:update_progress.state AS state
FROM {c}.gold.pipeline_events
WHERE event_type = 'update_progress'
  AND details:update_progress.state IN ('COMPLETED', 'FAILED', 'CANCELED')
ORDER BY timestamp DESC
LIMIT 10"""


class Unavailable(Exception):
    """The query did not answer. setup=True: a missing table or permission, which waiting
    will not fix; otherwise the warehouse is starting, stopped, or the query failed."""

    def __init__(self, message: str, *, setup: bool = False) -> None:
        super().__init__(message)
        self.setup = setup


def connect():
    """A connection as the app's own identity; Databricks Apps provides its credentials."""
    from databricks import sql
    from databricks.sdk.core import Config

    cfg = Config()
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}",
        credentials_provider=lambda: cfg.authenticate,
    )


class Queries:
    def __init__(self, connect=connect, *, clock=time.monotonic, catalog=CATALOG, ttl=600.0):
        self._connect = connect
        self._clock = clock
        self._catalog = catalog
        self._ttl = ttl
        self._cache: dict[tuple, tuple[float, pd.DataFrame]] = {}

    def _run(self, template: str, params: dict | None = None) -> pd.DataFrame:
        sql = template.format(c=self._catalog)
        key = (sql, tuple(sorted((params or {}).items())))
        hit = self._cache.get(key)
        if hit and self._clock() - hit[0] < self._ttl:
            return hit[1]
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
        except Exception as e:  # noqa: BLE001 — the visitor gets a message, the log the cause
            log.exception("query failed")
            setup = any(code in str(e).upper() for code in SETUP_ERRORS)
            raise Unavailable(str(e), setup=setup) from e
        df = pd.DataFrame([tuple(r) for r in rows], columns=cols)
        self._cache[key] = (self._clock(), df)
        return df

    def day(self, country: str, day: date) -> pd.DataFrame:
        return self._run(DAY, {"country": country, "day": day.isoformat()})

    def freshness(self) -> pd.DataFrame:
        return self._run(FRESHNESS)

    def expectations(self) -> pd.DataFrame:
        return self._run(EXPECTATIONS)

    def updates(self) -> pd.DataFrame:
        return self._run(UPDATES)
