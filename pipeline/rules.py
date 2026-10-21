"""Data-quality rules for silver, as SQL expressions. The pipeline passes them to
expect_all_or_drop / expect_all; the tests run the same strings with DataFrame.filter.
A row passes a rule when its expression is true."""

from model import COUNTRIES, ZONES


def _in(values) -> str:
    return ", ".join(f"'{v}'" for v in values)


_ON_GRID = "minutes IN (15, 60) AND minute(ts_utc) % minutes = 0 AND second(ts_utc) = 0"

POWER_DROP = {
    "ts_present": "ts_utc IS NOT NULL",
    "on_grid": _ON_GRID,
    "known_series": "kind IS NOT NULL",
    "known_country": f"country IN ({_in(COUNTRIES)})",
    "value_present": "value IS NOT NULL AND NOT isnan(value)",
}
# Warnings skip missing values: value_present / price_present already drop those rows.
POWER_WARN = {
    "generation_not_negative": "value IS NULL OR kind <> 'generation' OR value >= -1",
    "share_in_range": "value IS NULL OR kind <> 'share' OR value BETWEEN 0 AND 100",
}
PRICE_DROP = {
    "ts_present": "ts_utc IS NOT NULL",
    "on_grid": _ON_GRID,
    "known_zone": f"zone IN ({_in(ZONES)})",
    "price_present": "eur_mwh IS NOT NULL AND NOT isnan(eur_mwh)",
}
PRICE_WARN = {"price_in_range": "eur_mwh IS NULL OR eur_mwh BETWEEN -500 AND 4000"}
