import subprocess
from datetime import date
from pathlib import Path

import main
import pandas as pd
import yaml
from queries import Unavailable

ROOT = Path(__file__).resolve().parent.parent
DAY = pd.DataFrame(
    {
        "local_hour": [12, 13],
        "renewable_share": [97.4, 90.0],
        "eur_mwh": [108.9, 95.0],
        "rank_green": [1, 2],
        "rank_cheap": [2, 1],
    }
)


class FakeQueries:
    def __init__(self, day=DAY, down=False):
        self._day, self.down, self.calls = day, down, []

    def _maybe_down(self):
        if self.down:
            raise Unavailable("starting")

    def day(self, country, day):
        self.calls.append((country, day))
        self._maybe_down()
        return self._day

    def freshness(self):
        self._maybe_down()
        return pd.DataFrame(
            {"endpoint": ["power"], "area": ["at"], "latest_ts": ["x"], "age_hours": [2.0]}
        )

    def expectations(self):
        return pd.DataFrame(
            {"dataset": ["power_15min"], "rule": ["value_present"], "passed": [10], "failed": [1]}
        )

    def updates(self):
        return pd.DataFrame({"timestamp": ["t"], "state": ["COMPLETED"]})


def test_parse_day():
    assert main.parse_day("2026-10-07") == date(2026, 10, 7)
    assert main.parse_day("2026-10-07 00:00:00") == date(2026, 10, 7)
    assert main.parse_day("yesterday") is None and main.parse_day(None) is None


def test_explore_happy_path_and_compare():
    q = FakeQueries()
    steps = list(main.explore(q, "at", "2026-10-07", "cz"))
    assert steps[0] == (main.WAITING, None)
    text, fig = steps[-1]
    assert "Austria" in text and "Czechia" in text and fig is not None
    assert q.calls == [("at", date(2026, 10, 7)), ("cz", date(2026, 10, 7))]


def test_explore_messages():
    assert list(main.explore(FakeQueries(), "at", "soon", "")) == [
        ("Pick a date as YYYY-MM-DD.", None)
    ]
    assert list(main.explore(FakeQueries(down=True), "at", "2026-10-07", ""))[-1] == (
        main.DOWN,
        None,
    )
    text, fig = list(main.explore(FakeQueries(day=DAY.iloc[0:0]), "at", "2026-10-07", ""))[-1]
    assert text == "No data for Austria on 2026-10-07." and fig is None


def test_compare_with_itself_is_ignored():
    q = FakeQueries()
    list(main.explore(q, "at", "2026-10-07", "at"))
    assert len(q.calls) == 1


def test_health():
    steps = list(main.health(FakeQueries()))
    assert steps[0][0] == main.WAITING
    status, fresh, rules_, updates = steps[-1]
    assert (
        status == ""
        and fresh["status"].tolist() == ["ok"]
        and len(rules_) == 1
        and len(updates) == 1
    )
    assert list(main.health(FakeQueries(down=True)))[-1] == (main.DOWN, None, None, None)


def test_blocks_build_with_two_tabs():
    demo = main.build(FakeQueries())
    labels = [c.label for c in demo.blocks.values() if c.__class__.__name__ == "Tab"]
    assert labels == ["Explorer", "Health"]


def test_app_yaml_and_bundle_resource_agree():
    app_yaml = yaml.safe_load((ROOT / "app" / "app.yaml").read_text())
    assert app_yaml["command"] == ["python", "main.py"]
    env = {e["name"]: e for e in app_yaml["env"]}
    resource = yaml.safe_load((ROOT / "resources" / "app.yml").read_text())["resources"]["apps"][
        "explorer"
    ]
    assert (
        env["DATABRICKS_WAREHOUSE_ID"]["valueFrom"]
        == resource["resources"][0]["name"]
        == "sql-warehouse"
    )
    assert (ROOT / "resources" / resource["source_code_path"]).resolve() == ROOT / "app"
    assert resource["name"] == "energy-explorer"


def test_requirements_match_the_lock():
    fresh = subprocess.run(
        [
            "uv",
            "export",
            "--frozen",
            "--no-default-groups",
            "--group",
            "app",
            "--no-hashes",
            "--no-emit-project",
            "--no-header",
        ],
        capture_output=True,
        text=True,
        check=True,
        cwd=ROOT,
    ).stdout
    assert (ROOT / "app" / "requirements.txt").read_text() == fresh


class SetupBroken(FakeQueries):
    def day(self, country, day):
        raise Unavailable("[INSUFFICIENT_PERMISSIONS] gold", setup=True)

    def freshness(self):
        raise Unavailable("[INSUFFICIENT_PERMISSIONS] gold", setup=True)


def test_setup_errors_get_their_own_message():
    assert list(main.explore(SetupBroken(), "at", "2026-10-07", ""))[-1] == (main.SETUP, None)
    assert list(main.health(SetupBroken()))[-1] == (main.SETUP, None, None, None)
