"""Everything the pipeline computes, as plain PySpark functions over DataFrames, so it runs
on a local Spark session in the tests. pipeline.py only wires these to Databricks."""

from __future__ import annotations

from model import KIND, PRICE_SERIES, RENEWABLE_SHARE, ZONE_OF
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

_DATA = "data array<struct<timestamp: string, values: map<string, double>>>"
POWER_SCHEMA = f"country string, timezone string, interval_minutes int, {_DATA}"
PRICE_SCHEMA = f"bidding_zone string, timezone string, interval_minutes int, {_DATA}"


def _lookup(mapping: dict[str, str], col: str) -> Column:
    return F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])[F.col(col)]


def bronze_columns(files: DataFrame) -> DataFrame:
    """One row per file version: the raw JSON text, its path and modification time.
    `files` is a whole-text read (Auto Loader `text` + `wholetext`, or spark.read.text)."""
    return files.select(
        F.col("value").alias("payload"),
        F.col("_metadata.file_path").alias("file_path"),
        F.col("_metadata.file_modification_time").alias("file_mtime"),
    )


def _values(bronze: DataFrame, schema: str, area: str) -> DataFrame:
    """(area, timezone, minutes, ts_local, series, value, file_mtime), one row per value."""
    j = bronze.select(F.from_json("payload", schema).alias("j"), "file_mtime")
    points = j.select(
        F.col(f"j.{area}").alias("area"),
        F.col("j.timezone").alias("timezone"),
        F.col("j.interval_minutes").alias("minutes"),
        F.explode("j.data").alias("d"),
        "file_mtime",
    )
    return points.select(
        "area",
        "timezone",
        "minutes",
        F.col("d.timestamp").alias("ts_local"),
        F.explode("d.values").alias("series", "value"),
        "file_mtime",
    )


def _times(df: DataFrame) -> DataFrame:
    """ts_utc from the offset in each timestamp; local_date in the area's own time zone."""
    ts = F.try_to_timestamp(F.col("ts_local"))  # null, not an error, under ANSI (Spark 4)
    return df.withColumn("ts_utc", ts).withColumn(
        "local_date", F.to_date(F.from_utc_timestamp(ts, F.col("timezone")))
    )


def latest(df: DataFrame, keys: list[str]) -> DataFrame:
    """One row per key: the one from the most recently written file wins."""
    w = Window.partitionBy(*keys).orderBy(F.col("file_mtime").desc())
    return df.withColumn("_n", F.row_number().over(w)).filter("_n = 1").drop("_n", "file_mtime")


def silver_power(bronze: DataFrame) -> DataFrame:
    rows = _times(_values(bronze, POWER_SCHEMA, "country")).withColumnRenamed("area", "country")
    rows = rows.withColumn("kind", _lookup(KIND, "series"))  # null for an unknown series
    return latest(rows, ["country", "series", "ts_utc"]).select(
        "country", "timezone", "minutes", "ts_utc", "local_date", "series", "kind", "value"
    )


def silver_price(bronze: DataFrame) -> DataFrame:
    rows = _times(_values(bronze, PRICE_SCHEMA, "bidding_zone")).withColumnRenamed("area", "zone")
    rows = rows.filter(F.col("series") == PRICE_SERIES).withColumnRenamed("value", "eur_mwh")
    return latest(rows, ["zone", "ts_utc"]).select(
        "zone", "timezone", "minutes", "ts_utc", "local_date", "eur_mwh"
    )


def _local(df: DataFrame, ts: str = "hour_utc") -> DataFrame:
    local = F.from_utc_timestamp(F.col(ts), F.col("timezone"))
    return df.withColumn("local_date", F.to_date(local)).withColumn("local_hour", F.hour(local))


def mix_hourly(power: DataFrame) -> DataFrame:
    gen = power.filter(F.col("kind") == "generation")
    hourly = gen.groupBy(
        "country", "timezone", F.date_trunc("hour", "ts_utc").alias("hour_utc"), "series"
    ).agg(F.avg("value").alias("mw"))
    return _local(hourly)


def mix_daily(power: DataFrame) -> DataFrame:
    """Energy per source and local day: MWh = Σ MW × interval hours."""
    gen = power.filter(F.col("kind") == "generation")
    return gen.groupBy("country", "local_date", "series").agg(
        F.sum(F.col("value") * F.col("minutes") / 60).alias("mwh")
    )


def price_vs_renewables(power: DataFrame, price: DataFrame) -> DataFrame:
    """Hourly renewable share per country with its zone's hourly price (null if missing)."""
    share = (
        power.filter(F.col("series") == RENEWABLE_SHARE)
        .groupBy("country", "timezone", F.date_trunc("hour", "ts_utc").alias("hour_utc"))
        .agg(F.avg("value").alias("renewable_share"))
        .withColumn("zone", _lookup(ZONE_OF, "country"))
    )
    hourly_price = price.groupBy("zone", F.date_trunc("hour", "ts_utc").alias("hour_utc")).agg(
        F.avg("eur_mwh").alias("eur_mwh")
    )
    return _local(share.join(hourly_price, ["zone", "hour_utc"], "left")).select(
        "country", "zone", "hour_utc", "local_date", "local_hour", "renewable_share", "eur_mwh"
    )


def green_hours(pvr: DataFrame) -> DataFrame:
    """Each hour of a local day ranked by renewable share (1 = greenest) and price (1 = cheapest)."""
    day = Window.partitionBy("country", "local_date")
    return pvr.withColumn(
        "rank_green", F.rank().over(day.orderBy(F.col("renewable_share").desc_nulls_last()))
    ).withColumn("rank_cheap", F.rank().over(day.orderBy(F.col("eur_mwh").asc_nulls_last())))


def freshness(power: DataFrame, price: DataFrame) -> DataFrame:
    """The newest timestamp per (endpoint, area). Age is computed when queried."""
    p = power.groupBy(F.col("country").alias("area")).agg(F.max("ts_utc").alias("latest_ts"))
    q = price.groupBy(F.col("zone").alias("area")).agg(F.max("ts_utc").alias("latest_ts"))
    return (
        p.withColumn("endpoint", F.lit("power"))
        .unionByName(q.withColumn("endpoint", F.lit("price")))
        .select("endpoint", "area", "latest_ts")
    )
