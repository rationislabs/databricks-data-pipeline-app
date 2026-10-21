"""The Databricks App: the greenest and cheapest hours of a day (Explorer) and the
pipeline's health. Databricks Apps runs `python main.py` (app.yaml)."""

from __future__ import annotations

import os
from datetime import date, datetime
from functools import partial
from zoneinfo import ZoneInfo

import gradio as gr
import views
from queries import Queries, Unavailable

WAITING = "Warming up the SQL warehouse…"
DOWN = "The SQL warehouse didn't answer (it may still be starting). Try again in a moment."
SETUP = (
    "The app can't read the gold tables: no Databricks credentials (locally: DATABRICKS_HOST, "
    "DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID), the tables don't exist yet, or the app has no "
    "permission. Run the pipeline once and apply the README's GRANT step."
)


def today() -> str:
    return datetime.now(ZoneInfo("Europe/Vienna")).date().isoformat()


def parse_day(text) -> date | None:
    try:
        return date.fromisoformat(str(text or "")[:10])
    except ValueError:
        return None


def explore(q, country: str, day_text, compare: str):
    day = parse_day(day_text)
    if day is None:
        yield "Pick a date as YYYY-MM-DD.", None
        return
    yield WAITING, None
    try:
        frames = {country: q.day(country, day)}
        if compare and compare != country:
            frames[compare] = q.day(compare, day)
    except Unavailable as e:
        yield (SETUP if e.setup else DOWN), None
        return
    text = "\n\n".join(views.summary(c, day, df) for c, df in frames.items())
    shown = {views.COUNTRIES[c]: df for c, df in frames.items() if not df.empty}
    yield text, (views.day_figure(shown) if shown else None)


def health(q):
    yield WAITING, None, None, None
    try:
        fresh, rules, updates = q.freshness(), q.expectations(), q.updates()
    except Unavailable as e:
        yield (SETUP if e.setup else DOWN), None, None, None
        return
    yield "", views.freshness_table(fresh), rules, updates


def build(q=None) -> gr.Blocks:
    q = q or Queries()
    choices = [(name, code) for code, name in views.COUNTRIES.items()]
    with gr.Blocks(title="European electricity") as demo:
        gr.Markdown(
            "# European electricity\nWhen is power greenest and cheapest? Austria and its "
            f"neighbours, every 15 minutes since 2023.  \n_{views.ATTRIBUTION}_"
        )
        with gr.Tab("Explorer"):
            with gr.Row():
                country = gr.Dropdown(choices, value="at", label="Country")
                day = gr.DateTime(value=today(), include_time=False, type="string", label="Date")
                compare = gr.Dropdown([("None", "")] + choices, value="", label="Compare with")
                explore_btn = gr.Button("Explore", variant="primary")
            text = gr.Markdown()
            plot = gr.Plot()
            explore_btn.click(partial(explore, q), [country, day, compare], [text, plot])
        with gr.Tab("Health"):
            load = gr.Button("Load")
            status = gr.Markdown()
            fresh = gr.Dataframe(label=f"Freshness (stale after {views.STALE_HOURS} h)")
            rules = gr.Dataframe(label="Data-quality rules, last 7 days")
            updates = gr.Dataframe(label="Last pipeline runs")
            load.click(partial(health, q), None, [status, fresh, rules, updates])
    return demo


if __name__ == "__main__":
    port = int(os.environ.get("DATABRICKS_APP_PORT", "8000"))
    build().launch(server_name="0.0.0.0", server_port=port)
