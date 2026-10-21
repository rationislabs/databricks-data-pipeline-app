"""python -m ingest [--since YYYY-MM-DD] [--budget-minutes N] [--local DIR] [--no-refresh]

Without --local: uploads to /Volumes/$ENERGY_CATALOG/bronze/landing and starts the
`energy-refresh` job (Databricks unified auth from the environment)."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from . import energy_charts, volume
from . import run as ingest_run


def today_cet() -> date:
    return datetime.now(ZoneInfo("Europe/Vienna")).date()


def clean_env(env=os.environ) -> None:
    """GitHub passes unset secrets as empty strings; empty DATABRICKS_* vars confuse auth."""
    for k in [k for k, v in env.items() if k.startswith("DATABRICKS_") and not v]:
        del env[k]


def refresh(w, job_name: str = "energy-refresh", timeout=timedelta(hours=2)) -> None:
    jobs = list(w.jobs.list(name=job_name))
    if not jobs:
        raise RuntimeError(f"job {job_name!r} not found: deploy the bundle first")
    result = w.jobs.run_now_and_wait(jobs[0].job_id, timeout=timeout)
    state = getattr(result.state.result_state, "value", None)
    if state != "SUCCESS":
        raise RuntimeError(f"{job_name} ended {state}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m ingest")
    p.add_argument("--since", type=date.fromisoformat, help="re-fetch from this day")
    p.add_argument("--budget-minutes", type=float, default=330)
    p.add_argument("--local", type=Path, help="write to this folder instead of the volume")
    p.add_argument("--no-refresh", action="store_true")
    args = p.parse_args(argv)

    if args.local:
        store, do_refresh = volume.LocalDir(args.local), None
    else:
        clean_env()
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        catalog = os.environ.get("ENERGY_CATALOG") or "workspace"
        store = volume.Volume(w.files, f"/Volumes/{catalog}/bronze/landing")
        do_refresh = None if args.no_refresh else (lambda: refresh(w))

    with energy_charts.client() as http:
        summary = ingest_run.run(
            store,
            energy_charts.EnergyCharts(http),
            today=today_cet(),
            since=args.since,
            budget_s=args.budget_minutes * 60,
            refresh=do_refresh,
        )
    print(f"{summary.files} files uploaded, {summary.skipped} ranges left for the next run")
    for e in summary.errors:
        print(f"error: {e}", file=sys.stderr)
    return 1 if summary.errors else 0


if __name__ == "__main__":
    sys.exit(main())
