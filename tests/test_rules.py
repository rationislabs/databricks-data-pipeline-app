from datetime import datetime

import pytest
import rules
import transforms

from tests.conftest import EC, bronze

POWER_COLS = "country string, timezone string, minutes int, ts_utc timestamp, local_date date, series string, kind string, value double"
PRICE_COLS = (
    "zone string, timezone string, minutes int, ts_utc timestamp, local_date date, eur_mwh double"
)
T = datetime(2026, 10, 7, 10, 15)
GOOD_POWER = ("at", "Europe/Vienna", 15, T, T.date(), "solar", "generation", 100.0)
GOOD_PRICE = ("AT", "Europe/Vienna", 15, T, T.date(), 100.0)


def fails(spark, cols, row, expr) -> bool:
    return (
        spark.createDataFrame([row], cols).filter(f"NOT ({expr}) OR ({expr}) IS NULL").count() == 1
    )


def with_(row, i, v):
    return tuple(v if j == i else x for j, x in enumerate(row))


@pytest.mark.parametrize("rules_", [rules.POWER_DROP, rules.POWER_WARN])
def test_good_power_row_passes_every_rule(spark, rules_):
    for expr in rules_.values():
        assert not fails(spark, POWER_COLS, GOOD_POWER, expr), expr


@pytest.mark.parametrize(
    "name,bad",
    [
        ("ts_present", with_(GOOD_POWER, 3, None)),
        ("on_grid", with_(GOOD_POWER, 3, datetime(2026, 10, 7, 10, 7))),
        ("on_grid", with_(GOOD_POWER, 2, 30)),
        ("known_series", with_(GOOD_POWER, 6, None)),
        ("known_country", with_(GOOD_POWER, 0, "fr")),
        ("value_present", with_(GOOD_POWER, 7, None)),
        ("value_present", with_(GOOD_POWER, 7, float("nan"))),
    ],
)
def test_each_power_drop_rule_catches_its_fault(spark, name, bad):
    assert fails(spark, POWER_COLS, bad, rules.POWER_DROP[name])


def test_hourly_rows_on_the_hour_pass_the_grid(spark):
    hourly = with_(with_(GOOD_POWER, 2, 60), 3, datetime(2026, 10, 7, 10, 0))
    assert not fails(spark, POWER_COLS, hourly, rules.POWER_DROP["on_grid"])
    assert fails(spark, POWER_COLS, with_(hourly, 3, T), rules.POWER_DROP["on_grid"])


def test_power_warnings(spark):
    assert fails(
        spark, POWER_COLS, with_(GOOD_POWER, 7, -5.0), rules.POWER_WARN["generation_not_negative"]
    )
    consumption = with_(with_(GOOD_POWER, 6, "consumption"), 7, -1068.0)
    assert not fails(spark, POWER_COLS, consumption, rules.POWER_WARN["generation_not_negative"])
    share = with_(with_(GOOD_POWER, 6, "share"), 7, 120.0)
    assert fails(spark, POWER_COLS, share, rules.POWER_WARN["share_in_range"])


def test_price_rules(spark):
    for expr in {**rules.PRICE_DROP, **rules.PRICE_WARN}.values():
        assert not fails(spark, PRICE_COLS, GOOD_PRICE, expr), expr
    assert fails(spark, PRICE_COLS, with_(GOOD_PRICE, 0, "FR"), rules.PRICE_DROP["known_zone"])
    assert fails(spark, PRICE_COLS, with_(GOOD_PRICE, 5, None), rules.PRICE_DROP["price_present"])
    assert fails(
        spark, PRICE_COLS, with_(GOOD_PRICE, 5, 9999.0), rules.PRICE_WARN["price_in_range"]
    )


def test_recorded_march_drops_only_its_18_null_values(spark):
    p = transforms.silver_power(bronze(spark, EC / "power_at_2026-03.json"))
    dropped = {
        name: p.filter(f"NOT ({e}) OR ({e}) IS NULL").count()
        for name, e in rules.POWER_DROP.items()
    }
    assert dropped == {
        "ts_present": 0,
        "on_grid": 0,
        "known_series": 0,
        "known_country": 0,
        "value_present": 18,
    }


def test_warnings_ignore_missing_values(spark):
    # Today's future slots arrive as nulls: the drop rule removes them; warnings must not
    # count them a second time as implausible values.
    for kind in ("generation", "share"):
        row = with_(with_(GOOD_POWER, 6, kind), 7, None)
        for expr in rules.POWER_WARN.values():
            assert not fails(spark, POWER_COLS, row, expr), expr
    for expr in rules.PRICE_WARN.values():
        assert not fails(spark, PRICE_COLS, with_(GOOD_PRICE, 5, None), expr), expr
