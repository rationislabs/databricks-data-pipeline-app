from datetime import date

import model
import pandas as pd
import views

DAY = pd.DataFrame(
    {
        "local_hour": [11, 12, 13, 14],
        "renewable_share": [80.0, 97.4, 90.0, 60.0],
        "eur_mwh": [120.0, 108.9, 95.0, None],
        "rank_green": [3, 1, 2, 4],
        "rank_cheap": [3, 2, 1, 4],
    }
)


def test_countries_match_the_pipeline():
    assert tuple(views.COUNTRIES) == model.COUNTRIES


def test_hour_label():
    assert views.hour_label(9) == "09:00–10:00" and views.hour_label(23) == "23:00–00:00"


def test_best_hours():
    assert views.best(DAY, "green") == [12, 13, 11]
    assert views.best(DAY, "cheap") == [13, 12, 11]  # hour 14 has no price


def test_summary_text():
    text = views.summary("at", date(2026, 10, 7), DAY)
    assert "Austria, 2026-10-07" in text
    assert "12:00–13:00 (97 %)" in text and "13:00–14:00 (95.0 €/MWh)" in text


def test_summary_without_data_or_prices():
    assert (
        views.summary("ch", date(2026, 10, 7), DAY.iloc[0:0])
        == "No data for Switzerland on 2026-10-07."
    )
    no_price = DAY.assign(eur_mwh=None)
    assert "No prices" in views.summary("at", date(2026, 10, 7), no_price)


def test_day_figure_two_panels_no_second_axis():
    fig = views.day_figure({"Austria": DAY, "Czechia": DAY})
    assert len(fig.data) == 4
    assert {t.yaxis for t in fig.data} == {"y", "y2"}
    assert "yaxis2" in fig.layout and fig.layout.yaxis2.overlaying is None  # stacked, not overlaid


def test_freshness_table_marks_stale():
    df = pd.DataFrame(
        {
            "endpoint": ["power", "price"],
            "area": ["at", "AT"],
            "latest_ts": ["x", "y"],
            "age_hours": [5.0, 40.0],
        }
    )
    assert views.freshness_table(df)["status"].tolist() == ["ok", "stale"]


def test_summary_on_the_25_hour_day():
    # 2026-10-25: local hour 2 happens twice. Must not crash, and both rows can rank.
    day = pd.DataFrame(
        {
            "local_hour": [1, 2, 2, 3],
            "renewable_share": [70.0, 80.0, 81.0, 60.0],
            "eur_mwh": [90.0, 80.0, 79.0, 100.0],
            "rank_green": [3, 2, 1, 4],
            "rank_cheap": [3, 2, 1, 4],
        }
    )
    text = views.summary("at", date(2026, 10, 25), day)
    assert "02:00–03:00 (81 %), 02:00–03:00 (80 %), 01:00–02:00 (70 %)" in text
    assert "02:00–03:00 (79.0 €/MWh)" in text
