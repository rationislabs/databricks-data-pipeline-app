from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from databricks.sdk.errors import NotFound

from ingest import __main__ as cli
from ingest import cursor as cur
from ingest import run, volume
from ingest.energy_charts import FetchError

TODAY = date(2026, 10, 8)


class FakeApi:
    def __init__(self, clock=None, fail=(), cost=0.0):
        self.calls, self.fail, self.clock, self.cost = [], set(fail), clock, cost

    def fetch(self, endpoint, area, start, end):
        self.calls.append((endpoint, area, start, end))
        if self.clock is not None:
            self.clock.sleep(self.cost)
        if (endpoint, area) in self.fail:
            raise FetchError(f"{endpoint} {area} {start}..{end}: HTTP 500")
        return f'{{"data": [], "area": "{area}", "start": "{start}"}}'.encode()


def caught_up(store, day=TODAY - timedelta(days=1)):
    store.put(run.STATE, cur.dump({f"{e}/{a}": day for e, a in run.KEYS}))


def test_keys_alternate_endpoints():
    assert run.KEYS[:4] == [("power", "at"), ("price", "AT"), ("power", "de"), ("price", "DE-LU")]
    assert len(run.KEYS) == 14


def test_caught_up_run_fetches_yesterday_and_today_for_every_area(tmp_path):
    store = volume.LocalDir(tmp_path)
    caught_up(store)
    api = FakeApi()
    refreshed = []
    s = run.run(store, api, today=TODAY, budget_s=3600, refresh=lambda: refreshed.append(1))
    assert (s.files, s.errors, s.skipped) == (14, [], 0)
    assert api.calls[0] == ("power", "at", date(2026, 10, 7), TODAY)
    assert (tmp_path / "price/IT-North/2026-10-07_2026-10-08.json").exists()
    assert refreshed == [1]
    assert cur.load(store.get(run.STATE))["power/si"] == date(2026, 10, 7)


def test_backfill_interleaves_by_range(tmp_path):
    store = volume.LocalDir(tmp_path)
    api = FakeApi()
    run.run(store, api, today=date(2023, 2, 2), budget_s=10**6)
    firsts = api.calls[:14]
    assert {c[2] for c in firsts} == {date(2023, 1, 1)}
    assert {c[2] for c in api.calls[14:28]} == {date(2023, 2, 1)}


def test_failure_stops_that_area_only(tmp_path):
    store = volume.LocalDir(tmp_path)
    api = FakeApi(fail={("price", "CH")})
    s = run.run(store, api, today=date(2023, 2, 2), budget_s=10**6)
    assert len(s.errors) == 1 and "price CH" in s.errors[0]
    assert s.skipped == 1  # CH's February range waits for the next run
    c = cur.load(store.get(run.STATE))
    assert "price/CH" not in c and c["power/ch"] == date(2023, 2, 1)


def test_budget_then_resume(tmp_path, clock):
    store = volume.LocalDir(tmp_path)
    api = FakeApi(clock=clock, cost=31.0)
    first = run.run(store, api, today=date(2023, 3, 2), budget_s=31 * 20, clock=clock)
    assert first.files == 21 and first.skipped == 42 - 21
    second = run.run(store, FakeApi(), today=date(2023, 3, 2), budget_s=10**6)
    assert second.files == 21  # 7 areas still need Feb + Mar, 7 only Mar: 7 * 2 + 7 * 1
    c = cur.load(store.get(run.STATE))
    assert set(c.values()) == {date(2023, 3, 1)}  # every area complete, no gap


def test_nothing_fetched_means_no_refresh(tmp_path):
    store = volume.LocalDir(tmp_path)
    caught_up(store)
    refreshed = []
    s = run.run(
        store,
        FakeApi(fail=set(run.KEYS)),
        today=TODAY,
        budget_s=3600,
        refresh=lambda: refreshed.append(1),
    )
    assert s.files == 0 and len(s.errors) == 14 and refreshed == []


class FakeFiles:
    def __init__(self):
        self.store = {}

    def upload(self, file_path, contents, *, overwrite=None):
        assert overwrite is True
        self.store[file_path] = contents.read()

    def download(self, file_path):
        if file_path not in self.store:
            raise NotFound(f"{file_path} not found")
        return SimpleNamespace(contents=SimpleNamespace(read=lambda: self.store[file_path]))


def test_volume_puts_under_root_and_gets_none_when_missing():
    files = FakeFiles()
    v = volume.Volume(files, "/Volumes/workspace/bronze/landing")
    v.put("power/at/x.json", b"{}")
    assert files.store == {"/Volumes/workspace/bronze/landing/power/at/x.json": b"{}"}
    assert v.get("power/at/x.json") == b"{}"
    assert v.get("_state/cursor.json") is None


def test_clean_env_drops_empty_databricks_vars():
    env = {"DATABRICKS_TOKEN": "", "DATABRICKS_HOST": "https://x", "OTHER": ""}
    cli.clean_env(env)
    assert env == {"DATABRICKS_HOST": "https://x", "OTHER": ""}


def fake_workspace(state):
    run_ = SimpleNamespace(state=SimpleNamespace(result_state=SimpleNamespace(value=state)))
    jobs = SimpleNamespace(
        list=lambda name: [SimpleNamespace(job_id=7)] if name == "energy-refresh" else [],
        run_now_and_wait=lambda job_id, timeout: run_,
    )
    return SimpleNamespace(jobs=jobs)


def test_refresh_success_and_failure():
    cli.refresh(fake_workspace("SUCCESS"))
    with pytest.raises(RuntimeError, match="FAILED"):
        cli.refresh(fake_workspace("FAILED"))
    with pytest.raises(RuntimeError, match="deploy the bundle"):
        cli.refresh(fake_workspace("SUCCESS"), job_name="missing")


def test_main_local_mode(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(cli.energy_charts, "EnergyCharts", lambda http: FakeApi())
    monkeypatch.setattr(cli, "today_cet", lambda: date(2023, 1, 3))
    assert cli.main(["--local", str(tmp_path)]) == 0
    assert (tmp_path / "power/at/2023-01-01_2023-01-03.json").exists()
    assert "14 files" in capsys.readouterr().out


def test_the_volume_catalog_is_validated(monkeypatch):
    monkeypatch.setenv("ENERGY_CATALOG", "energy_eu")
    assert cli.catalog() == "energy_eu"
    monkeypatch.delenv("ENERGY_CATALOG")
    assert cli.catalog() == "workspace"
    monkeypatch.setenv("ENERGY_CATALOG", "../x")
    with pytest.raises(SystemExit):
        cli.catalog()


def test_a_store_error_costs_one_file_not_the_run(tmp_path):
    from databricks.sdk.errors import DatabricksError

    class Flaky(volume.LocalDir):
        def put(self, rel, body):
            if rel.startswith("price/AT/"):
                raise DatabricksError("volume hiccup")
            super().put(rel, body)

    store = Flaky(tmp_path)
    caught_up(store)
    s = run.run(store, FakeApi(), today=TODAY, budget_s=3600)
    assert s.files == 13 and any("volume hiccup" in e for e in s.errors)
    assert not (tmp_path / "price/AT").exists()
    assert (tmp_path / "price/IT-North/2026-10-07_2026-10-08.json").exists()  # later files land


def test_the_default_budget_leaves_room_for_the_refresh_wait():
    import yaml

    wf = yaml.safe_load((Path(__file__).parent.parent / ".github/workflows/ingest.yml").read_text())
    job_minutes = wf["jobs"]["ingest"]["timeout-minutes"]
    assert cli.DEFAULT_BUDGET_MINUTES + cli.REFRESH_TIMEOUT.total_seconds() / 60 < job_minutes


def test_a_store_error_on_the_cursor_write_does_not_stop_the_run(tmp_path):
    from databricks.sdk.errors import DatabricksError

    class Flaky(volume.LocalDir):
        fails = 0  # armed after the setup below has written its cursor

        def put(self, rel, body):
            if rel == run.STATE and Flaky.fails:
                Flaky.fails -= 1
                raise DatabricksError("volume hiccup")
            super().put(rel, body)

    store = Flaky(tmp_path)
    caught_up(store)
    Flaky.fails = 1
    s = run.run(store, FakeApi(), today=TODAY, budget_s=3600)
    assert s.files == 14 and any("volume hiccup" in e for e in s.errors)
