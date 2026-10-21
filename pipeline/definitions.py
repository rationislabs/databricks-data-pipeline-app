"""The Lakeflow Declarative Pipeline: wires transforms.py and rules.py to Databricks.
It runs only on Databricks (the `dlt` module and `spark` exist there); everything it
computes is tested locally through those two modules."""

import os
import sys

if "__file__" in globals():  # the bundle's root_path normally covers this
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import dlt  # noqa: E402
import rules  # noqa: E402
import transforms  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

CATALOG = spark.conf.get("energy.catalog")  # noqa: F821 — provided by the pipeline runtime
LANDING = spark.conf.get("energy.landing")  # noqa: F821


def _landing(sub: str):
    """New and re-fetched (overwritten) files of one endpoint, one row per file version."""
    files = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "text")
        .option("wholetext", "true")
        .option("cloudFiles.allowOverwrites", "true")
        .load(f"{LANDING}/{sub}/")
    )
    return transforms.bronze_columns(files).withColumn("ingested_at", F.current_timestamp())


def _read(table: str):
    return spark.read.table(f"{CATALOG}.{table}")  # noqa: F821


@dlt.table(
    name=f"{CATALOG}.bronze.power_raw", comment="Energy-Charts public_power files, as fetched."
)
def power_raw():
    return _landing("power")


@dlt.table(
    name=f"{CATALOG}.bronze.price_raw", comment="Energy-Charts day-ahead price files, as fetched."
)
def price_raw():
    return _landing("price")


@dlt.table(name=f"{CATALOG}.silver.power_15min", comment="One row per area, series and interval.")
@dlt.expect_all_or_drop(rules.POWER_DROP)
@dlt.expect_all(rules.POWER_WARN)
def power_15min():
    return transforms.silver_power(_read("bronze.power_raw"))


@dlt.table(name=f"{CATALOG}.silver.price_15min", comment="One row per price zone and interval.")
@dlt.expect_all_or_drop(rules.PRICE_DROP)
@dlt.expect_all(rules.PRICE_WARN)
def price_15min():
    return transforms.silver_price(_read("bronze.price_raw"))


@dlt.table(name=f"{CATALOG}.gold.mix_hourly", comment="Average MW per generation source and hour.")
def mix_hourly():
    return transforms.mix_hourly(_read("silver.power_15min"))


@dlt.table(name=f"{CATALOG}.gold.mix_daily", comment="MWh per generation source and local day.")
def mix_daily():
    return transforms.mix_daily(_read("silver.power_15min"))


@dlt.table(name=f"{CATALOG}.gold.price_vs_renewables", comment="Hourly renewable share and price.")
def price_vs_renewables():
    return transforms.price_vs_renewables(_read("silver.power_15min"), _read("silver.price_15min"))


@dlt.table(
    name=f"{CATALOG}.gold.green_hours", comment="Hours of each day ranked by share and price."
)
def green_hours():
    return transforms.green_hours(_read("gold.price_vs_renewables"))


@dlt.table(name=f"{CATALOG}.gold.freshness", comment="Newest timestamp per endpoint and area.")
def freshness():
    return transforms.freshness(_read("silver.power_15min"), _read("silver.price_15min"))
