from datetime import date

from ingest import cursor as cur


def test_no_cursor_starts_at_default_in_month_chunks():
    assert cur.ranges(None, date(2023, 3, 10)) == [
        (date(2023, 1, 1), date(2023, 1, 31)),
        (date(2023, 2, 1), date(2023, 2, 28)),
        (date(2023, 3, 1), date(2023, 3, 10)),
    ]


def test_caught_up_fetches_yesterday_and_today():
    today = date(2026, 10, 8)
    assert cur.ranges(date(2026, 10, 7), today) == [(date(2026, 10, 7), today)]
    assert cur.ranges(date(2026, 10, 6), today) == [(date(2026, 10, 7), today)]


def test_gap_across_month_end_splits():
    assert cur.ranges(date(2026, 9, 27), date(2026, 10, 2)) == [
        (date(2026, 9, 28), date(2026, 9, 30)),
        (date(2026, 10, 1), date(2026, 10, 2)),
    ]


def test_since_overrides_the_cursor():
    assert cur.ranges(date(2026, 10, 7), date(2026, 10, 8), since=date(2026, 10, 1)) == [
        (date(2026, 10, 1), date(2026, 10, 8))
    ]


def test_advance_only_counts_finished_days_and_never_goes_back():
    today = date(2026, 10, 8)
    c = {"power/at": date(2026, 10, 6)}
    cur.advance(c, "power/at", date(2026, 10, 7), today, today)
    assert c == {"power/at": date(2026, 10, 7)}  # today is not finished
    cur.advance(c, "power/at", date(2026, 9, 1), date(2026, 9, 30), today)
    assert c == {"power/at": date(2026, 10, 7)}
    cur.advance(c, "price/AT", today, today, today)  # only today: nothing finished
    assert "price/AT" not in c


def test_load_and_dump():
    assert cur.load(None) == {} and cur.load(b"") == {}
    c = {"price/AT": date(2026, 10, 7), "power/at": date(2026, 10, 6)}
    assert cur.load(cur.dump(c)) == c
    assert cur.dump(c).decode().index("power/at") < cur.dump(c).decode().index("price/AT")


def test_since_never_leaves_a_gap():
    today = date(2026, 10, 8)
    c = {}
    cur.advance(c, "power/at", date(2026, 10, 1), date(2026, 10, 8), today)
    assert c == {}  # nothing before Oct 1 was ever fetched: no cursor yet
    c = {"power/at": date(2025, 1, 31)}
    cur.advance(c, "power/at", date(2026, 10, 1), date(2026, 10, 8), today)
    assert c == {"power/at": date(2025, 1, 31)}  # Feb 2025..Sep 2026 still missing
    c = {"power/at": date(2026, 10, 5)}
    cur.advance(c, "power/at", date(2026, 10, 1), date(2026, 10, 8), today)
    assert c == {"power/at": date(2026, 10, 7)}  # overlapping re-fetch: fine
