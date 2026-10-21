import os
import time
from pathlib import Path

os.environ["TZ"] = "UTC"  # collected timestamps are naive; make them mean UTC
time.tzset()

import pytest  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"
EC = FIXTURES / "energy_charts"


class Clock:
    """A fake monotonic clock: sleeping advances it and is recorded, never real."""

    def __init__(self) -> None:
        self.t = 1000.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.t += seconds


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def bronze(spark, *paths):
    """Files read the way Auto Loader reads them (whole text), as bronze rows."""
    import transforms

    files = spark.read.option("wholetext", "true").text([str(p) for p in paths])
    return transforms.bronze_columns(files)
