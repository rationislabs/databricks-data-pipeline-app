# databricks-data-pipeline-app

European electricity on Databricks: the generation mix, renewable share and day-ahead prices
of Austria and its six neighbours, every 15 minutes since 2023. A Lakeflow Declarative
Pipeline refines the data from bronze to gold with data-quality expectations, an AI/BI
dashboard gives the overview, and a Gradio Databricks App answers "when is power greenest
and cheapest today?". One Asset Bundle deploys all of it. A Rationis Labs portfolio project.

```
GitHub Actions cron (every 6 h)            Databricks Free Edition (Asset Bundle)
  ingest ─ Energy-Charts v2 ─► JSON ─► UC volume  /Volumes/<catalog>/bronze/landing/{endpoint}/{area}/{start}_{end}.json
  (31 s between requests)                      │
  then: job energy-refresh                     ▼  pipeline "energy" (triggered, serverless)
                                          bronze  power_raw, price_raw         Auto Loader, every file version
                                          silver  power_15min, price_15min     typed, newest file wins, expectations
                                          gold    mix_hourly, mix_daily, price_vs_renewables, green_hours, freshness
                                               │                        │
                                     AI/BI dashboard            Gradio app (SQL warehouse)
                                     Mix · Prices · Compare     Explorer · Health
```

## Data

| Endpoint | Areas | Resolution |
|---|---|---|
| `/v2/public_power` — net generation per source, load, renewable share | AT, DE, CH, CZ, HU, SI, IT | 15 min (CH, SI: 60 min) |
| `/v2/price` — day-ahead price | AT, DE-LU, CH, CZ, HU, SI, IT-North | 15 min (CH: 60 min) |

Data: Energy-Charts (Fraunhofer ISE), CC BY 4.0; prices: Bundesnetzagentur | SMARD.de,
CC BY 4.0. Italy's price is the IT-North zone. The renewable share is Energy-Charts' own
`renewable_share_of_load`, the one definition used everywhere.

The ingest asks politely: User-Agent `RationisLabsEnergyLakehouse/1.0 (+https://github.com/rationislabs/databricks-data-pipeline-app)`,
31 seconds between requests to the same endpoint (the published limit is 2 per minute),
`Retry-After` honoured on HTTP 429, and at most one calendar month per request. The first
runs are the backfill since 2023-01-01 (≈ 650 requests, ≈ 3 hours over one or two runs);
after that each run fetches yesterday and today.

## Why it is built this way: Databricks Free Edition

- **Serverless only, with a daily compute quota.** Exceed it and compute is off for the rest
  of the day. The pipeline therefore runs *triggered* after each ingest, never continuously,
  and a failed run is simply caught up by the next one.
- **Outbound internet is limited to an unpublished list of trusted domains.** So Databricks
  never calls the API: GitHub Actions fetches the data and drops files into a Unity Catalog
  volume, where Auto Loader picks them up — the usual "external producer, lakehouse ingests
  incrementally" pattern.
- **Apps are private and run at most 24 hours after a start.** Every viewer needs a Databricks
  identity with CAN USE; there is no public URL. Start the app before a demo (below); the
  screenshots here show it.
- One SQL warehouse (2X-Small, "Serverless Starter Warehouse"), 5 concurrent job tasks,
  1 active pipeline per type, no commercial use.

## Data quality

Silver applies expectations (SQL in `pipeline/rules.py`, tested locally with the same strings):

| Rule | Action |
|---|---|
| timestamp present and on its 15- or 60-minute grid; known series, country, zone; value present | drop the row |
| generation ≥ −1 MW; shares 0–100 %; price −500…4000 €/MWh | keep, count a warning |

Nothing ever fails the pipeline: one bad value must not stop seven countries. The app's
Health tab shows rows dropped and warned per rule, freshness per area, and the last runs.

## Run it locally

Requirements: [uv](https://docs.astral.sh/uv/) and Java 17 or newer (for the local Spark tests).

```bash
uv sync
uv run inv test                       # offline: API fixtures, local Spark, fake warehouse
uv run inv ingest --since 2026-10-01  # fetch into data/ without Databricks
```

| Task | What it does |
|---|---|
| `uv run inv test` / `lint` | tests / ruff |
| `uv run inv ingest [--since D] [--no-local]` | fetch into `data/`, or (`--no-local`) upload and refresh for real |
| `uv run inv deploy` | `databricks bundle deploy` |
| `uv run inv app` | run the app locally against your workspace |
| `uv run inv requirements` | regenerate `app/requirements.txt` from `uv.lock` |

Everything except `test`, `lint`, `requirements` and a local `ingest` talks to your workspace
and reads it from the environment: `DATABRICKS_HOST` plus `DATABRICKS_TOKEN` (or
`DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`); `inv app` also needs
`DATABRICKS_WAREHOUSE_ID` (the warehouse's page: Connection details → HTTP path, the last part).

## One-time setup (free plans only)

1. Create a Databricks Free Edition account for Rationis Labs.
2. Create a service principal with an OAuth secret (Settings → Identity and access), or a
   personal access token if Free Edition does not offer service principals.
3. In the GitHub repo, in this order: an environment `prod` (Settings → Environments) limited
   to the `main` branch, holding the secrets `DATABRICKS_CLIENT_ID` and
   `DATABRICKS_CLIENT_SECRET`, or `DATABRICKS_TOKEN` (no repository-level copies: any branch
   could read those); then the variable `DATABRICKS_HOST` (the workspace URL). Optional
   variable `ENERGY_CATALOG` if the workspace catalog is not called `workspace` (Deploy writes
   it into the app's `app/app.yaml` too). Ingest and deploy stay off until
   `DATABRICKS_HOST` is set. Give `prod` no required reviewers or wait timer: the scheduled
   ingest and the `validate` check on every push to `main` use it too, and would wait.
4. Deploy: push to `main` (or run the Deploy workflow, or `uv run inv deploy`).
5. Let the app read gold (replace the principal with the app's service principal, shown on the
   app's page):

   ```sql
   GRANT USE CATALOG ON CATALOG workspace TO `<app service principal>`;
   GRANT USE SCHEMA, SELECT ON SCHEMA workspace.gold TO `<app service principal>`;
   ```

6. Run the Ingest workflow once by hand (or wait for the cron); it backfills since 2023 over
   one or two runs and refreshes the pipeline after each.
7. Before a demo: `databricks bundle run explorer` starts the app and deploys its current code
   (starting it from the UI alone can leave it without a deployment). Give viewers CAN USE
   and take the screenshots for this README.

## Known issues to fix

- An error that never goes away on an old date range would block that country on every run.
- The Health tab adds up 7 days of runs, so its counts are too high, and it doesn't say whether
  a rule drops or warns.
- The Explorer's default date stays at the day the app was started.
- Price freshness can show a negative age.
- Silver reprocesses all of bronze on every run; Databricks' incremental change feed
  (AUTO CDC) would avoid that.
- The bundle grants the app nothing, so the README's GRANT step is required.
- GitHub Actions minutes for a private repo may come close to the free 2,000 per month.
- A failed refresh hides that run's error list.
- The Mix chart sums all countries until you pick one.
- Local runs against Databricks leave a temp copy of the data on disk.

Contact: [github.com/rationislabs](https://github.com/rationislabs)
