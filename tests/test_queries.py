from datetime import date

import pytest
import queries


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.description = [("local_hour",), ("renewable_share",)]

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))
        if self.conn.error:
            raise self.conn.error

    def fetchall(self):
        return [(12, 97.4), (13, 95.0)]

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    def __init__(self, error=None):
        self.executed, self.error, self.opened = [], error, 0

    def __call__(self):
        self.opened += 1
        return self

    def cursor(self):
        return FakeCursor(self)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_day_query_params_and_frame(clock):
    conn = FakeConn()
    q = queries.Queries(conn, clock=clock, catalog="cat")
    df = q.day("at", date(2026, 10, 7))
    assert list(df.columns) == ["local_hour", "renewable_share"] and len(df) == 2
    sql, params = conn.executed[0]
    assert "cat.gold.green_hours" in sql and params == {"country": "at", "day": "2026-10-07"}


def test_results_cached_for_ten_minutes(clock):
    conn = FakeConn()
    q = queries.Queries(conn, clock=clock)
    q.day("at", date(2026, 10, 7))
    q.day("at", date(2026, 10, 7))
    assert conn.opened == 1
    clock.sleep(601)
    q.day("at", date(2026, 10, 7))
    assert conn.opened == 2


def test_errors_become_unavailable_and_are_not_cached(clock):
    conn = FakeConn(error=RuntimeError("warehouse starting"))
    q = queries.Queries(conn, clock=clock)
    with pytest.raises(queries.Unavailable):
        q.freshness()
    conn.error = None
    assert len(q.freshness()) == 2


@pytest.mark.parametrize("name", ["freshness", "expectations", "updates"])
def test_health_queries_use_gold(clock, name):
    conn = FakeConn()
    getattr(queries.Queries(conn, clock=clock, catalog="cat"), name)()
    sql, _ = conn.executed[0]
    assert "cat.gold." in sql


def test_setup_errors_are_marked_and_logged(clock, caplog):
    conn = FakeConn(error=RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] cat.gold.freshness"))
    q = queries.Queries(conn, clock=clock)
    with pytest.raises(queries.Unavailable) as e:
        q.freshness()
    assert e.value.setup is True
    assert "TABLE_OR_VIEW_NOT_FOUND" in caplog.text
    conn.error = RuntimeError("connection timed out")
    with pytest.raises(queries.Unavailable) as e:
        q.updates()
    assert e.value.setup is False


@pytest.mark.parametrize(
    "error",
    [
        ValueError("default auth: cannot configure default credentials, please check ..."),
        KeyError("DATABRICKS_WAREHOUSE_ID"),
    ],
)
def test_missing_credentials_are_setup_not_a_slow_warehouse(clock, error):
    def connect():
        raise error

    with pytest.raises(queries.Unavailable) as e:
        queries.Queries(connect, clock=clock).freshness()
    assert e.value.setup is True
