"""Tiny synthetic checks for ``basecast_pipelines.models.peak_excess`` (no database)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

from basecast_pipelines.models import peak_excess as px

pytest.importorskip("numpy")


def test_flat_block_recovers_flat_and_proportional_parts():
    # Baseline day: max 100, min 60. Add a 10 MW flat block and 10% proportional growth.
    flat, g = px.flat_block(d_max=100 * 0.1 + 10, d_min=60 * 0.1 + 10, max0=100, min0=60)
    assert flat == pytest.approx(10)
    assert g == pytest.approx(0.1)
    # Pure flat block: max and min move together, g = 0.
    flat, g = px.flat_block(d_max=50, d_min=50, max0=100, min0=60)
    assert (flat, g) == (pytest.approx(50), pytest.approx(0))
    with pytest.raises(ValueError):
        px.flat_block(1, 1, 5, 5)


def test_excess_vs_prebreak_finds_a_step_after_the_training_years():
    years = list(range(2003, 2027))
    temps = [30 + (y % 3) * 0.5 for y in years]
    peak = [1000 + 10 * (y - 2014) + 5 * t + (300 if y >= 2022 else 0) for y, t in zip(years, temps, strict=True)]
    panel = pl.DataFrame({"weather_zone": "X", "year": years, "peak_mw": peak, "t": temps})
    out = px.excess_vs_prebreak(panel, "peak_mw", "t", train_until=2019)
    by_year = dict(zip(out["year"].to_list(), out["excess_mw"].to_list(), strict=True))
    assert by_year[2015] == pytest.approx(0, abs=1e-6)
    assert by_year[2021] == pytest.approx(0, abs=1e-6)
    assert by_year[2024] == pytest.approx(300, abs=1e-6)
    # Any target column works (not only peak_mw), and first_year trims the training window.
    out2 = px.excess_vs_prebreak(panel.rename({"peak_mw": "dmin_mean"}), "dmin_mean", "t", first_year=2010)
    assert out2["year"].min() == 2010
    assert out2.filter(pl.col("year") == 2026)["excess_mw"].item() == pytest.approx(300, abs=1e-6)


def test_coincident_zone_loads_reads_zones_at_the_system_peak_hour():
    t0 = datetime(2025, 7, 1, 20, tzinfo=UTC)
    rows = []
    for i, (a, b) in enumerate([(10, 50), (40, 30), (20, 55)]):  # system: 60, 70, 75 -> hour 2 is the peak
        ts = t0 + timedelta(hours=i)
        rows += [{"ts_utc": ts, "operating_date": date(2025, 7, 1), "weather_zone": "A", "mw": float(a)},
                 {"ts_utc": ts, "operating_date": date(2025, 7, 1), "weather_zone": "B", "mw": float(b)},
                 {"ts_utc": ts, "operating_date": date(2025, 7, 1), "weather_zone": "ERCOT", "mw": float(a + b)}]
    out = px.coincident_zone_loads(pl.DataFrame(rows))
    got = dict(zip(out["weather_zone"].to_list(), out["coincident_mw"].to_list(), strict=True))
    # Zone B's own peak (55) is at the system peak hour, zone A's own peak (40) is not: A contributes 20.
    assert got == {"A": 20.0, "B": 55.0, "ERCOT": 75.0}


def test_summer_shape_uses_the_same_window_every_year():
    days = [date(2025, 5, 31), date(2025, 6, 1), date(2025, 9, 20), date(2025, 9, 21)]
    daily = pl.DataFrame({"weather_zone": "A", "date": days, "dmin": [1.0, 60.0, 80.0, 999.0],
                          "dmax": [2.0, 100.0, 120.0, 999.0], "dmean": [1.5, 80.0, 100.0, 999.0]})
    out = px.summer_shape(daily).row(0, named=True)
    assert out["days"] == 2  # May 31 and Sep 21 are outside June 1 - Sep 20
    assert out["dmin_mean"] == pytest.approx(70)
    assert out["peak_mw"] == pytest.approx(120)
    assert out["load_factor"] == pytest.approx(90 / 120)
    assert out["min_max_ratio"] == pytest.approx(70 / 110)


def test_step_scan_places_a_level_shift():
    days = [date(2022, 1, 1) + timedelta(days=i) for i in range(120)]
    series = pl.DataFrame({"date": days, "v": [100.0 if i < 60 else 300.0 for i in range(120)]})
    out = px.step_scan(series, "v", window=28)
    best = out.sort(pl.col("step").abs(), descending=True).row(0, named=True)
    assert best["date"] == date(2022, 3, 2)  # day 60, the first day of the new level
    assert best["step"] == pytest.approx(200)


def _cv(rows: list[dict]) -> pl.DataFrame:
    base = {"document": "deck.pdf", "page": 6, "category_type": "month", "status_bucket": "observed_energized",
            "source_file": "raw/x"}
    return pl.DataFrame([{**base, **r} for r in rows]).with_columns(
        pl.col("report_date").cast(pl.Date), pl.col("as_of").cast(pl.Date))


def test_energized_by_month_splits_simultaneous_and_drops_misdated_axes():
    obs = "Loads Approved to Energize – Observations"
    rows = [
        # A deck of 2025-06 with the past months, both bases.
        {"report_date": date(2025, 6, 1), "as_of": date(2025, 6, 1), "chart_title": obs, "category": "2025-05",
         "status_label": "Observed Simultaneous Peak Load", "value_mw": 3400.0},
        {"report_date": date(2025, 6, 1), "as_of": date(2025, 6, 1), "chart_title": obs, "category": "2025-05",
         "status_label": "Observed Non-Simultaneous Peak Load", "value_mw": 3800.0},
        # A later deck whose axis was misread two years early: must not overwrite 2023-07 or appear at all.
        {"report_date": date(2026, 5, 21), "as_of": date(2026, 5, 21), "chart_title": obs, "category": "2023-07",
         "status_label": "Observed Simultaneous Peak Load", "value_mw": 3528.0},
        # A later, correctly dated reading of 2025-05 wins (latest deck).
        {"report_date": date(2026, 1, 1), "as_of": date(2026, 1, 1), "chart_title": obs, "category": "2025-05",
         "status_label": "Observed Simultaneous Peak Load", "value_mw": 3441.0},
    ]
    out = px.energized_by_month(_cv(rows))
    assert out["month"].to_list() == ["2025-05"]
    row = out.row(0, named=True)
    assert row["simultaneous_mw"] == 3441.0
    assert row["non_simultaneous_mw"] == 3800.0
    assert px.value_asof(out, "2025-08", "simultaneous_mw") == (3441.0, "2025-05")
    assert px.value_asof(out, "2024-01", "simultaneous_mw") == (None, None)


def test_zone_share_daily_and_monthly_yoy():
    d = [date(2024, 7, 1), date(2025, 7, 1)]
    daily = pl.DataFrame({
        "weather_zone": ["A", "A", "B", "B", "ERCOT", "ERCOT"], "date": d * 3,
        "dmin": [10.0, 30.0, 20.0, 20.0, 30.0, 50.0], "dmax": [20.0, 40.0, 40.0, 40.0, 60.0, 80.0],
        "dmean": [15.0, 35.0, 30.0, 30.0, 45.0, 65.0],
    })
    shares = px.zone_share_daily(daily)
    assert shares.filter((pl.col("weather_zone") == "A") & (pl.col("date") == d[0]))["share"].item() == \
        pytest.approx(15 / 45)
    yoy = px.monthly_min_yoy(daily).filter((pl.col("weather_zone") == "A") & (pl.col("month") == date(2025, 7, 1)))
    assert yoy["dmin_yoy"].item() == pytest.approx(20)
    assert yoy["dmax_yoy"].item() == pytest.approx(20)
