"""Energy-Charts API v2 client: one request per (endpoint, area, date range), politely.
The published limit is 2 requests per minute per endpoint: we wait 31 s between requests
to the same endpoint, honour Retry-After on 429, and retry server errors with backoff."""

from __future__ import annotations

import json
import time
from datetime import date

import httpx

BASE = "https://api.energy-charts.info/v2"
USER_AGENT = "RationisLabsEnergyLakehouse/1.0 (+https://github.com/rationislabs/databricks-data-pipeline-app)"
MIN_INTERVAL = 31.0
BACKOFF = (10.0, 30.0, 90.0)
MAX_429 = 5
ENDPOINTS = {"power": ("public_power", "country"), "price": ("price", "bzn")}


class FetchError(Exception):
    """A range could not be fetched; the run records it and moves on."""


def client() -> httpx.Client:
    return httpx.Client(base_url=BASE, headers={"User-Agent": USER_AGENT}, timeout=60)


def retry_after(r: httpx.Response) -> float:
    try:
        return max(float(r.headers.get("retry-after", "")), 1.0)
    except ValueError:
        return 60.0


def check(body: bytes, what: str) -> None:
    try:
        data = json.loads(body)
    except ValueError as e:
        raise FetchError(f"{what}: not JSON") from e
    if not isinstance(data, dict) or not isinstance(data.get("data"), list):
        raise FetchError(f"{what}: no data list")


class EnergyCharts:
    def __init__(self, http: httpx.Client, *, clock=time.monotonic, sleep=time.sleep) -> None:
        self._http = http
        self._clock = clock
        self._sleep = sleep
        self._last: dict[str, float] = {}

    def fetch(self, endpoint: str, area: str, start: date, end: date) -> bytes:
        path, key = ENDPOINTS[endpoint]
        params = {key: area, "start": start.isoformat(), "end": end.isoformat()}
        what = f"{endpoint} {area} {start}..{end}"
        retries = throttled = 0
        while True:
            self._wait(endpoint)
            try:
                r = self._http.get(f"/{path}", params=params)
                problem = f"HTTP {r.status_code}" if r.status_code >= 500 else None
            except httpx.TransportError as e:
                r, problem = None, repr(e)
            self._last[endpoint] = self._clock()
            if r is not None and r.status_code == 429 and throttled < MAX_429:
                throttled += 1
                self._sleep(retry_after(r))
                continue
            if problem and retries < len(BACKOFF):
                self._sleep(BACKOFF[retries])
                retries += 1
                continue
            if problem:
                raise FetchError(f"{what}: {problem}")
            if r.status_code != 200:
                raise FetchError(f"{what}: HTTP {r.status_code}")
            check(r.content, what)
            return r.content

    def _wait(self, endpoint: str) -> None:
        last = self._last.get(endpoint)
        if last is not None and (wait := last + MIN_INTERVAL - self._clock()) > 0:
            self._sleep(wait)
