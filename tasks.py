"""Developer tasks. List them with `uv run inv --list`."""

from __future__ import annotations

from pathlib import Path

from invoke import task

ROOT = Path(__file__).parent


@task
def test(c):
    c.run("uv run pytest", pty=True)


@task
def lint(c):
    c.run("uv run ruff check . && uv run ruff format --check .")


@task
def ingest(c, since="", local=True):
    """Try the ingest into data/ (no Databricks). --no-local uploads for real (needs auth)."""
    flags = (f" --since {since}" if since else "") + (" --local data" if local else "")
    c.run(f"uv run python -m ingest{flags}", pty=True)


@task
def deploy(c):
    """Deploy the bundle with the Databricks CLI (needs `databricks auth login` first).
    The direct engine: older CLIs default to Terraform, newer ones have only direct."""
    c.run("databricks bundle deploy", env={"DATABRICKS_BUNDLE_ENGINE": "direct"}, pty=True)


@task
def requirements(c):
    """Regenerate app/requirements.txt from uv.lock (run after changing app dependencies)."""
    c.run(
        "uv export --frozen --no-default-groups --group app --no-hashes --no-emit-project "
        "--no-header > app/requirements.txt"
    )


@task
def app(c):
    """Run the app locally on http://127.0.0.1:8000 against the workspace in your env
    (DATABRICKS_HOST + auth, DATABRICKS_WAREHOUSE_ID)."""
    c.run("cd app && uv run python main.py", pty=True)
