"""What the data means: the areas, their price zones, and the kind of every Energy-Charts
series. No imports, so ingest, the pipeline and the tests can all use it."""

COUNTRIES = ("at", "de", "ch", "cz", "hu", "si", "it")
ZONE_OF = {
    "at": "AT",
    "de": "DE-LU",
    "ch": "CH",
    "cz": "CZ",
    "hu": "HU",
    "si": "SI",
    "it": "IT-North",  # stands in for all of Italy
}
ZONES = tuple(ZONE_OF.values())
GENERATION = (
    "battery",
    "biomass",
    "fossil_brown_coal_lignite",
    "fossil_coal_derived_gas",
    "fossil_gas",
    "fossil_hard_coal",
    "fossil_oil",
    "geothermal",
    "hydro_pumped_storage",
    "hydro_run_of_river",
    "hydro_water_reservoir",
    "nuclear",
    "other_renewables",
    "others",
    "solar",
    "waste",
    "wind_offshore",
    "wind_onshore",
)
KIND = {
    **dict.fromkeys(GENERATION, "generation"),
    "battery_consumption": "consumption",
    "hydro_pumped_storage_consumption": "consumption",
    "cross_border_electricity_trading": "trade",
    "load": "load",
    "residual_load": "load",
    "renewable_share_of_generation": "share",
    "renewable_share_of_load": "share",
}
RENEWABLE_SHARE = "renewable_share_of_load"  # the one definition used everywhere
PRICE_SERIES = "day_ahead_price"


def catalog_name(name: str) -> str:
    """The catalog the pipeline writes to. Every table name is built from it, so only
    letters, digits and _, not starting with a digit (the rule ingest and the app apply)."""
    if not (name.isascii() and name.replace("_", "a").isalnum() and not name[:1].isdigit()):
        raise ValueError(f"energy.catalog must be an identifier, not {name!r}")
    return name
