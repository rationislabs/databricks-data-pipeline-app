import json
import os
from datetime import date, datetime

import transforms

from tests.conftest import EC, bronze

DAY_AT = EC / "power_at_2026-10-07.json"
DAY_CH = EC / "power_ch_2026-10-07.json"
MARCH_AT = EC / "power_at_2026-03.json"


def test_power_rows_kinds_and_utc(spark):
    p = transforms.silver_power(bronze(spark, DAY_AT, DAY_CH))
    assert p.count() == 96 * 16 + 24 * 12
    assert p.filter("kind IS NULL").count() == 0
    row = p.filter("country = 'at' AND series = 'solar' AND ts_utc = '2026-10-07 10:00:00'").first()
    assert row.value == 3320.0 and row.kind == "generation" and row.minutes == 15
    assert row.local_date == date(2026, 10, 7) and row.timezone == "Europe/Vienna"
    first = p.filter("country = 'at'").agg({"ts_utc": "min"}).first()[0]
    assert first == datetime(2026, 10, 6, 22, 0)  # local midnight, UTC+2
    assert p.filter("country = 'ch'").select("minutes").distinct().first()[0] == 60


def test_dst_day_has_23_hours(spark):
    p = transforms.silver_power(bronze(spark, MARCH_AT))
    day = p.filter("local_date = '2026-03-29' AND series = 'solar'")
    assert day.count() == 92
    assert p.filter("local_date = '2026-03-01' AND series = 'solar'").count() == 96


def test_price_rows(spark):
    q = transforms.silver_price(
        bronze(spark, EC / "price_AT_2026-10-07.json", EC / "price_CH_2026-10-07.json")
    )
    assert q.count() == 96 + 24
    noon = q.filter("zone = 'AT' AND ts_utc = '2026-10-07 10:00:00'").first()
    assert noon.eur_mwh == 106.17 and noon.minutes == 15
    assert q.filter("zone = 'CH'").select("minutes").distinct().first()[0] == 60


def _rewrite(src, dst, mtime, change=None):
    data = json.loads(src.read_text())
    if change:
        change(data)
    dst.write_text(json.dumps(data))
    os.utime(dst, (mtime, mtime))
    return dst


def test_latest_file_wins(spark, tmp_path):
    def bump(data):
        for point in data["data"]:
            point["values"]["solar"] = 1.0

    old = _rewrite(DAY_AT, tmp_path / "month.json", 1_000_000)
    new = _rewrite(DAY_AT, tmp_path / "day.json", 2_000_000, bump)
    p = transforms.silver_power(bronze(spark, old, new))
    assert p.count() == 96 * 16  # one row per (country, series, timestamp)
    solar = {r.value for r in p.filter("series = 'solar'").collect()}
    assert solar == {1.0}


def test_empty_and_broken_payloads(spark, tmp_path):
    empty = json.loads(DAY_AT.read_text())
    empty["data"] = []
    (tmp_path / "empty.json").write_text(json.dumps(empty))
    (tmp_path / "broken.json").write_text("<html>maintenance</html>")
    p = transforms.silver_power(bronze(spark, tmp_path / "empty.json", tmp_path / "broken.json"))
    assert p.count() == 0


def test_unknown_series_gets_no_kind(spark, tmp_path):
    data = json.loads(DAY_AT.read_text())
    for point in data["data"]:
        point["values"]["fusion"] = 5.0
    (tmp_path / "new.json").write_text(json.dumps(data))
    p = transforms.silver_power(bronze(spark, tmp_path / "new.json"))
    assert p.filter("series = 'fusion' AND kind IS NULL").count() == 96


def test_malformed_timestamp_is_null_even_under_ansi(spark, tmp_path):
    # Spark 4 (Databricks) runs ANSI mode: to_timestamp would fail the whole update.
    data = json.loads(DAY_AT.read_text())
    data["data"][0]["timestamp"] = "garbage"
    (tmp_path / "bad.json").write_text(json.dumps(data))
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        p = transforms.silver_power(bronze(spark, tmp_path / "bad.json"))
        assert p.filter("ts_utc IS NULL").count() == 16
    finally:
        spark.conf.set("spark.sql.ansi.enabled", "false")
