import json

import model

from tests.conftest import EC


def test_countries_and_zones_pair_up():
    assert model.COUNTRIES == ("at", "de", "ch", "cz", "hu", "si", "it")
    assert tuple(model.ZONE_OF) == model.COUNTRIES
    assert model.ZONES == ("AT", "DE-LU", "CH", "CZ", "HU", "SI", "IT-North")


def test_every_recorded_series_has_a_kind():
    seen = set()
    for f in EC.glob("power_*.json"):
        seen |= {s["id"] for s in json.loads(f.read_text())["series"]}
    assert seen <= set(model.KIND)
    assert model.KIND["solar"] == "generation"
    assert model.KIND["hydro_pumped_storage_consumption"] == "consumption"
    assert model.KIND["cross_border_electricity_trading"] == "trade"
    assert model.KIND[model.RENEWABLE_SHARE] == "share"


def test_kinds_are_the_five():
    assert set(model.KIND.values()) == {"generation", "consumption", "trade", "load", "share"}


def test_the_catalog_name_must_be_an_identifier():
    # Every table name in the pipeline is built from it.
    assert model.catalog_name("workspace") == "workspace"
    assert model.catalog_name("_energy_2") == "_energy_2"
    for bad in ("", "1st", "a.b", "a`b", "a-b", "katälog", "a b"):
        try:
            model.catalog_name(bad)
        except ValueError:
            continue
        raise AssertionError(f"accepted {bad!r}")
