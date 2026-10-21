"""Pure presentation: query results → text, figures and tables. No Databricks, no Gradio."""

from __future__ import annotations

from datetime import date

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

COUNTRIES = {
    "at": "Austria",
    "de": "Germany",
    "ch": "Switzerland",
    "cz": "Czechia",
    "hu": "Hungary",
    "si": "Slovenia",
    "it": "Italy",
}
STALE_HOURS = 36
ATTRIBUTION = (
    "Data: Energy-Charts (Fraunhofer ISE), CC BY 4.0; "
    "prices: Bundesnetzagentur | SMARD.de, CC BY 4.0"
)


def hour_label(h: int) -> str:
    return f"{h:02d}:00–{(h + 1) % 24:02d}:00"


def _top(df: pd.DataFrame, by: str, n: int = 3) -> pd.DataFrame:
    """The n best rows: by "green" (highest share) or "cheap" (lowest price). Rows, not hours:
    on the 25-hour day local hour 2 happens twice."""
    value, rank = ("renewable_share", "rank_green") if by == "green" else ("eur_mwh", "rank_cheap")
    return df[df[value].notna()].sort_values([rank, "local_hour"]).head(n)


def best(df: pd.DataFrame, by: str, n: int = 3) -> list[int]:
    return [int(h) for h in _top(df, by, n)["local_hour"]]


def summary(country: str, day: date, df: pd.DataFrame) -> str:
    name = COUNTRIES[country]
    if df.empty:
        return f"No data for {name} on {day}."
    green = ", ".join(
        f"{hour_label(int(r.local_hour))} ({r.renewable_share:.0f} %)"
        for r in _top(df, "green").itertuples()
    )
    cheap_rows = _top(df, "cheap")
    cheap = (
        ", ".join(
            f"{hour_label(int(r.local_hour))} ({r.eur_mwh:.1f} €/MWh)"
            for r in cheap_rows.itertuples()
        )
        if len(cheap_rows)
        else "No prices for this day."
    )
    return f"**{name}, {day}**  \nGreenest: {green}  \nCheapest: {cheap}"


def day_figure(frames: dict[str, pd.DataFrame]) -> go.Figure:
    """Renewable share and price in two stacked panels sharing the hour axis."""
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        subplot_titles=("Renewable share of load (%)", "Day-ahead price (€/MWh)"),
    )
    for name, df in frames.items():
        fig.add_trace(
            go.Scatter(
                x=df["local_hour"], y=df["renewable_share"], name=name, mode="lines+markers"
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df["local_hour"],
                y=df["eur_mwh"],
                name=name,
                mode="lines+markers",
                showlegend=False,
            ),
            row=2,
            col=1,
        )
    fig.update_xaxes(title_text="Local hour", row=2, col=1, dtick=2)
    fig.update_layout(height=520, margin={"t": 40, "b": 40}, legend={"orientation": "h"})
    return fig


def freshness_table(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(status=["stale" if a > STALE_HOURS else "ok" for a in df["age_hours"]])
