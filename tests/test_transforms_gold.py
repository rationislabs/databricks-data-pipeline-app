from datetime import datetime

import pytest
import transforms

from tests.conftest import EC, bronze


@pytest.fixture(scope="module")
def silver(spark):
    power = transforms.silver_power(
        bronze(
            spark,
            EC / "power_at_2026-10-07.json",
            EC / "power_ch_2026-10-07.json",
            EC / "power_at_2026-03.json",
        )
    ).filter("value IS NOT NULL")
    price = transforms.silver_price(
        bronze(spark, EC / "price_AT_2026-10-07.json", EC / "price_CH_2026-10-07.json")
    )
    return power.cache(), price.cache()


def test_mix_daily_mwh(silver):
    power, _ = silver
    row = (
        transforms.mix_daily(power)
        .filter("country = 'at' AND local_date = '2026-10-07' AND series = 'solar'")
        .first()
    )
    assert row.mwh == pytest.approx(22119.0)
    assert transforms.mix_daily(power).filter("series = 'load'").count() == 0  # generation only


def test_mix_hourly_local_hours_on_dst_day(silver):
    power, _ = silver
    hours = transforms.mix_hourly(power).filter("country = 'at' AND local_date = '2026-03-29'")
    assert hours.select("local_hour").distinct().count() == 23


def test_price_vs_renewables_hourly_and_ch(silver):
    power, price = silver
    pvr = transforms.price_vs_renewables(power, price).filter(
        "local_date = '2026-10-07' AND local_hour = 12"
    )
    at = pvr.filter("country = 'at'").first()
    assert at.zone == "AT" and at.hour_utc == datetime(2026, 10, 7, 10, 0)
    assert at.renewable_share == pytest.approx(97.4) and at.eur_mwh == pytest.approx(108.8725)
    ch = pvr.filter("country = 'ch'").first()
    assert ch.renewable_share == pytest.approx(49.8) and ch.eur_mwh == pytest.approx(168.21)


def test_price_missing_stays_null(silver):
    power, price = silver
    march = transforms.price_vs_renewables(power, price).filter("local_date = '2026-03-10'")
    assert march.count() == 24 and march.filter("eur_mwh IS NULL").count() == 24


def test_green_hours_ranks(silver):
    power, price = silver
    g = transforms.green_hours(transforms.price_vs_renewables(power, price)).filter(
        "country = 'at' AND local_date = '2026-10-07'"
    )
    assert g.count() == 24
    assert g.filter("rank_green = 1").first().local_hour == 12
    cheapest = g.orderBy("rank_cheap").first()
    assert cheapest.eur_mwh == min(r.eur_mwh for r in g.collect())


def test_freshness(silver):
    power, price = silver
    rows = {(r.endpoint, r.area): r.latest_ts for r in transforms.freshness(power, price).collect()}
    assert rows == {
        ("power", "at"): datetime(2026, 10, 7, 21, 45),
        ("power", "ch"): datetime(2026, 10, 7, 21, 0),
        ("price", "AT"): datetime(2026, 10, 7, 21, 45),
        ("price", "CH"): datetime(2026, 10, 7, 21, 0),
    }
