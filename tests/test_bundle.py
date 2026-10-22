import ast
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
TABLES = [
    "bronze.power_raw",
    "bronze.price_raw",
    "silver.power_15min",
    "silver.price_15min",
    "gold.mix_hourly",
    "gold.mix_daily",
    "gold.price_vs_renewables",
    "gold.green_hours",
    "gold.freshness",
]


def load(rel):
    return yaml.safe_load((ROOT / rel).read_text())


def resources():
    out = {}
    for f in sorted((ROOT / "resources").glob("*.yml")):
        for kind, items in load(f"resources/{f.name}")["resources"].items():
            out.setdefault(kind, {}).update(items)
    return out


def test_bundle_basics():
    b = load("databricks.yml")
    assert b["bundle"]["name"] == "energy-lakehouse"
    assert b["include"] == ["resources/*.yml"]
    assert b["variables"]["catalog"]["default"] == "workspace"
    assert b["variables"]["warehouse_id"]["lookup"] == {"warehouse": "Serverless Starter Warehouse"}
    assert b["targets"]["prod"]["default"] is True


def test_schemas_and_volume():
    r = resources()
    assert set(r["schemas"]) == {"bronze", "silver", "gold"}
    vol = r["volumes"]["landing"]
    assert vol["name"] == "landing" and vol["volume_type"] == "MANAGED"


def test_pipeline_points_at_real_files_and_publishes_events():
    p = resources()["pipelines"]["energy"]
    assert p["serverless"] is True and p["continuous"] is False
    lib = (ROOT / "resources" / p["libraries"][0]["file"]["path"]).resolve()
    assert lib == ROOT / "pipeline" / "definitions.py" and lib.exists()
    assert (ROOT / "resources" / p["root_path"]).resolve() == ROOT / "pipeline"
    assert p["configuration"]["energy.landing"] == "/Volumes/${var.catalog}/bronze/landing"
    assert p["event_log"]["name"] == "pipeline_events"


def test_job_runs_the_pipeline():
    job = resources()["jobs"]["refresh"]
    assert job["name"] == "energy-refresh" and job["max_concurrent_runs"] == 1
    assert job["tasks"][0]["pipeline_task"]["pipeline_id"] == "${resources.pipelines.energy.id}"


def test_pipeline_defines_every_table_with_its_rules():
    src = (ROOT / "pipeline" / "definitions.py").read_text()
    ast.parse(src)
    for t in TABLES:
        assert f'.{t}"' in src, t
    for r in ("rules.POWER_DROP", "rules.POWER_WARN", "rules.PRICE_DROP", "rules.PRICE_WARN"):
        assert r in src
    assert '"cloudFiles.allowOverwrites", "true"' in src


def test_workflows_are_gated_and_pinned():
    for name in ("ingest", "deploy"):
        wf = load(f".github/workflows/{name}.yml")
        job = next(iter(wf["jobs"].values()))
        assert "vars.DATABRICKS_HOST != ''" in job["if"]
        for step in job["steps"]:
            if "uses" in step:
                assert "@" in step["uses"] and len(step["uses"].split("@")[1]) == 40
    ingest = load(".github/workflows/ingest.yml")
    assert ingest[True]["schedule"][0]["cron"] == "23 */6 * * *"  # PyYAML reads `on:` as True


def test_deploy_never_runs_fork_code():
    wf = load(".github/workflows/deploy.yml")
    cond = wf["jobs"]["deploy"]["if"]
    assert "github.event.workflow_run.event == 'push'" in cond
    assert "github.event.workflow_run.head_repository.full_name == github.repository" in cond


def test_deploy_and_ingest_run_only_from_main_with_the_prod_environment():
    # A dispatch from a feature branch must not reach the service principal's secrets.
    deploy = load(".github/workflows/deploy.yml")["jobs"]["deploy"]
    assert "github.ref == 'refs/heads/main'" in deploy["if"]
    ingest = load(".github/workflows/ingest.yml")["jobs"]["ingest"]
    assert "github.ref == 'refs/heads/main'" in ingest["if"]
    assert deploy["environment"] == "prod"
    # validate reads the same secrets: from the environment, on main only.
    validate = load(".github/workflows/ci.yml")["jobs"]["validate"]
    assert "github.ref == 'refs/heads/main'" in validate["if"]
    # Only Deploy deploys: the others use prod's secrets without adding a deployment record
    # every run.
    for job in (ingest, validate):
        assert job["environment"] == {"name": "prod", "deployment": False}, job["environment"]


def test_resource_keys_are_unique_across_types():
    """`databricks bundle validate` rejects one key used by two resource types."""
    keys = [
        key
        for f in sorted((ROOT / "resources").glob("*.yml"))
        for items in load(f"resources/{f.name}")["resources"].values()
        for key in items
    ]
    assert len(keys) == len(set(keys)), sorted(keys)


def test_the_app_reads_the_catalog_the_deploy_uses():
    # app.yaml holds the default; Deploy rewrites it from the same variable it gives the bundle.
    app_env = {e["name"]: e.get("value") for e in load("app/app.yaml")["env"]}
    assert app_env["ENERGY_CATALOG"] == load("databricks.yml")["variables"]["catalog"]["default"]
    steps = load(".github/workflows/deploy.yml")["jobs"]["deploy"]["steps"]
    script = "\n".join(s.get("run", "") for s in steps)
    assert "app/app.yaml" in script and "ENERGY_CATALOG" in script


def test_ci_deploys_with_the_same_bundle_engine_as_local_deploys():
    # Two engines keep separate state: resources would be created twice or conflict.
    env = {}
    for step in load(".github/workflows/deploy.yml")["jobs"]["deploy"]["steps"]:
        env.update(step.get("env", {}))
    assert env.get("DATABRICKS_BUNDLE_ENGINE") == "direct"
