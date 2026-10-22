from datetime import date

import httpx
import pytest
import respx

from ingest import energy_charts as ec
from tests.conftest import EC

D1, D2 = date(2026, 10, 7), date(2026, 10, 7)
POWER = "https://api.energy-charts.info/v2/public_power"
PRICE = "https://api.energy-charts.info/v2/price"


@pytest.fixture
def api(clock):
    with respx.mock(assert_all_called=False) as mock, ec.client() as http:
        yield ec.EnergyCharts(http, clock=clock, sleep=clock.sleep), mock


def test_fetch_power_returns_body_and_sends_params(api):
    charts, mock = api
    body = (EC / "power_at_2026-10-07.json").read_bytes()
    route = mock.get(POWER).respond(200, content=body)
    assert charts.fetch("power", "at", D1, D2) == body
    req = route.calls[0].request
    assert dict(req.url.params) == {"country": "at", "start": "2026-10-07", "end": "2026-10-07"}
    assert (
        req.headers["user-agent"]
        == "RationisLabsEnergyLakehouse/1.0 (+https://github.com/rationislabs/databricks-data-pipeline-app)"
    )


def test_fetch_price_uses_bzn(api):
    charts, mock = api
    route = mock.get(PRICE).respond(200, content=(EC / "price_AT_2026-10-07.json").read_bytes())
    charts.fetch("price", "DE-LU", D1, date(2026, 10, 31))
    assert dict(route.calls[0].request.url.params) == {
        "bzn": "DE-LU",
        "start": "2026-10-07",
        "end": "2026-10-31",
    }


def test_31_seconds_between_requests_per_endpoint(api, clock):
    charts, mock = api
    mock.get(POWER).respond(200, json={"data": []})
    mock.get(PRICE).respond(200, json={"data": []})
    charts.fetch("power", "at", D1, D2)
    charts.fetch("price", "AT", D1, D2)  # another endpoint: no wait
    charts.fetch("power", "de", D1, D2)  # same endpoint: waits the rest of 31 s
    assert clock.slept == [31.0]


def test_429_waits_retry_after(api, clock):
    charts, mock = api
    mock.get(POWER).mock(
        side_effect=[
            httpx.Response(429, headers={"retry-after": "40"}),
            httpx.Response(200, json={"data": []}),
        ]
    )
    charts.fetch("power", "at", D1, D2)
    assert clock.slept == [40.0]


def test_429_without_usable_header_waits_a_minute(api, clock):
    charts, mock = api
    mock.get(POWER).mock(
        side_effect=[
            httpx.Response(429, headers={"retry-after": "soon"}),
            httpx.Response(200, json={"data": []}),
        ]
    )
    charts.fetch("power", "at", D1, D2)
    assert clock.slept == [60.0]


def test_too_many_429_gives_up(api):
    charts, mock = api
    mock.get(POWER).respond(429, headers={"retry-after": "40"})
    with pytest.raises(ec.FetchError, match="HTTP 429"):
        charts.fetch("power", "at", D1, D2)


def test_server_errors_back_off_then_succeed(api, clock):
    charts, mock = api
    mock.get(POWER).mock(
        side_effect=[
            httpx.Response(503),
            httpx.Response(502),
            httpx.Response(200, json={"data": []}),
        ]
    )
    charts.fetch("power", "at", D1, D2)
    # 10 s backoff, then the rest of the 31 s spacing; 30 s backoff, then the rest again
    assert clock.slept == [10.0, 21.0, 30.0, 1.0]


def test_network_errors_give_up_after_three_retries(api):
    charts, mock = api
    route = mock.get(POWER).mock(side_effect=httpx.ConnectError("down"))
    with pytest.raises(ec.FetchError, match="power at 2026-10-07..2026-10-07"):
        charts.fetch("power", "at", D1, D2)
    assert route.call_count == 4


@pytest.mark.parametrize(
    "response",
    [httpx.Response(404), httpx.Response(200, text="<html>"), httpx.Response(200, json={"x": 1})],
)
def test_unusable_answers_raise(api, response):
    charts, mock = api
    mock.get(POWER).mock(return_value=response)
    with pytest.raises(ec.FetchError):
        charts.fetch("power", "at", D1, D2)


@pytest.mark.parametrize(
    "header, wait", [("3600", 300.0), ("nan", 60.0), ("inf", 60.0), ("-5", 1.0), ("0.5", 1.0)]
)
def test_retry_after_is_finite_and_capped(header, wait):
    from ingest.energy_charts import retry_after

    assert retry_after(httpx.Response(429, headers={"retry-after": header})) == wait


def test_retry_after_as_an_http_date():
    from datetime import UTC, datetime, timedelta
    from email.utils import format_datetime

    from ingest.energy_charts import retry_after

    when = format_datetime(datetime.now(UTC) + timedelta(seconds=30), usegmt=True)
    assert 25 <= retry_after(httpx.Response(429, headers={"retry-after": when})) <= 31
