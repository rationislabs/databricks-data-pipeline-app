import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
GOLD = {"mix_hourly", "mix_daily", "price_vs_renewables", "green_hours", "freshness"}


def dash():
    return json.loads((ROOT / "dashboards" / "energy.lvdash.json").read_text())


def test_three_pages():
    assert [p["displayName"] for p in dash()["pages"]] == ["Mix", "Prices", "Compare"]


def test_datasets_read_gold_only():
    for ds in dash()["datasets"]:
        sql = " ".join(ds["queryLines"])
        tables = set(re.findall(r"FROM\s+([a-z_]+)", sql, re.I))
        assert tables and tables <= GOLD, (ds["name"], tables)


def test_widgets_reference_known_datasets_and_fields():
    d = dash()
    names = {ds["name"] for ds in d["datasets"]}
    for page in d["pages"]:
        for item in page["layout"]:
            w = item["widget"]
            if "textbox_spec" in w:
                continue
            fields = {}
            for q in w["queries"]:
                assert q["query"]["datasetName"] in names
                fields[q["name"]] = {f["name"] for f in q["query"]["fields"]}
            enc = w["spec"]["encodings"]
            used = [
                e
                for e in (enc.get("fields") or enc.values())
                if isinstance(e, dict) and "fieldName" in e
            ]
            all_fields = set().union(*fields.values())
            for e in used:
                assert e["fieldName"] in all_fields, (w["name"], e["fieldName"])


def test_attribution_on_every_page():
    for page in dash()["pages"]:
        texts = [i["widget"].get("textbox_spec", "") for i in page["layout"]]
        assert any(
            "Energy-Charts (Fraunhofer ISE), CC BY 4.0" in t and "SMARD.de" in t for t in texts
        )


def test_bundle_resource():
    r = yaml.safe_load((ROOT / "resources" / "dashboard.yml").read_text())["resources"][
        "dashboards"
    ]["energy_dashboard"]
    assert (
        ROOT / "resources" / r["file_path"]
    ).resolve() == ROOT / "dashboards" / "energy.lvdash.json"
    assert r["warehouse_id"] == "${var.warehouse_id}"
    assert r["dataset_catalog"] == "${var.catalog}" and r["dataset_schema"] == "gold"


def test_compare_bars_are_grouped_not_stacked():
    page = next(p for p in dash()["pages"] if p["displayName"] == "Compare")
    bar = next(
        i["widget"]
        for i in page["layout"]
        if i["widget"].get("spec", {}).get("widgetType") == "bar"
    )
    assert bar["spec"]["mark"] == {"layout": "group"}
