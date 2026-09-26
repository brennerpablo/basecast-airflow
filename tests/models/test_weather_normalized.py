"""Tiny synthetic checks for ``basecast_pipelines.models.weather_normalized`` (no database)."""

from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from basecast_pipelines.models import weather_normalized as wn


def _hourly_load(start_local: date, days: int, mw: float, zone: str = "COAST") -> pl.DataFrame:
    """Hour-ending rows for ``days`` Chicago operating days: ``ts_utc`` is the interval end."""
    tz = ZoneInfo("America/Chicago")
    start = datetime(start_local.year, start_local.month, start_local.day, tzinfo=tz).astimezone(UTC)
    end_day = start_local + timedelta(days=days)
    end = datetime(end_day.year, end_day.month, end_day.day, tzinfo=tz).astimezone(UTC)
    rows, t = [], start
    while t < end:
        begin_local = t.astimezone(tz)
        rows.append({"ts_utc": t + timedelta(hours=1), "operating_date": begin_local.date(),
                     "hour_ending": begin_local.hour + 1, "dst_flag": False, "weather_zone": zone, "mw": mw})
        t += timedelta(hours=1)
    return pl.DataFrame(rows).with_columns(pl.col("ts_utc").dt.convert_time_zone("UTC"))


def _weather_daily(dates: list[date], t: list[float], zone: str = "COAST") -> pl.DataFrame:
    return pl.DataFrame({"weather_zone": zone, "date": dates, "t_mean": t, "t_max": [x + 6 for x in t],
                         "td_mean": [x - 8 for x in t], "hi_max": [x + 6 for x in t], "n_hours": 24},
                        schema_overrides={"n_hours": pl.UInt32})


def test_nerc_holidays_move_sunday_not_saturday():
    hol = wn.nerc_holidays([2020, 2021, 2022, 2025])
    assert hol[date(2022, 12, 26)] == "christmas"  # Christmas 2022 fell on a Sunday
    assert hol[date(2021, 7, 5)] == "independence_day"  # July 4, 2021 was a Sunday
    assert date(2020, 7, 3) not in hol and hol[date(2020, 7, 4)] == "independence_day"  # Saturday stays
    assert hol[date(2025, 5, 26)] == "memorial_day"
    assert hol[date(2025, 9, 1)] == "labor_day"
    assert hol[date(2025, 11, 27)] == "thanksgiving"


def test_day_types():
    dates = [date(2025, 11, 28), date(2025, 12, 24), date(2025, 12, 28), date(2025, 12, 25), date(2025, 6, 10)]
    assert wn.day_types(dates) == ["bridge", "bridge", "yearend", "holiday", "normal"]


def test_window_for_slides_inward_at_the_ends():
    assert wn.window_for(2003, 2003, 2026) == (2003, 2005)
    assert wn.window_for(2010, 2003, 2026) == (2009, 2011)
    assert wn.window_for(2026, 2003, 2026) == (2024, 2026)
    assert wn.window_for(2004, 2003, 2004) == (2003, 2004)


def test_daily_panel_on_spring_forward_day():
    # 2025-03-09 has 23 operating hours: energy 23 × MW, the daily mean unchanged.
    load = _hourly_load(date(2025, 3, 8), 3, 100.0)
    dates = [date(2025, 3, 7) + timedelta(days=i) for i in range(4)]
    feats = wn.weather_daily_features(_weather_daily(dates, [10.0, 11.0, 12.0, 13.0]))
    panel = wn.daily_panel(load, feats)
    row = panel.filter(pl.col("date") == date(2025, 3, 9)).row(0, named=True)
    assert row["n_hours"] == 23
    assert row["energy_mwh"] == pytest.approx(2300.0)
    assert row["dmean"] == pytest.approx(100.0)
    assert row["t_lag"] == pytest.approx(11.0)  # yesterday's mean temperature
    assert panel["date"].min() == date(2025, 3, 8)  # Mar 7 has no load; every kept day has a lag
    assert not panel["excluded"].any()


def test_uri_window_is_excluded():
    load = _hourly_load(date(2021, 2, 13), 3, 50.0)
    dates = [date(2021, 2, 12) + timedelta(days=i) for i in range(4)]
    panel = wn.daily_panel(load, wn.weather_daily_features(_weather_daily(dates, [0.0, -5.0, -10.0, -8.0])))
    assert dict(zip(panel["date"].to_list(), panel["excluded"].to_list(), strict=True)) == {
        date(2021, 2, 13): False, date(2021, 2, 14): True, date(2021, 2, 15): True}


def _synthetic_panel(cool: float = 30.0, heat: float = 20.0, seed: int = 1) -> pl.DataFrame:
    """Three years of daily data with a known response: ``cool`` MW/°C above 24 °C, ``heat`` MW/°C below 10 °C
    (both kinks sit on knots of the default spline),
    a 100 MW/yr trend, a weekend dip and noise."""
    np = pytest.importorskip("numpy")
    rng = np.random.default_rng(seed)
    dates = [date(2018, 1, 1) + timedelta(days=i) for i in range(3 * 365)]
    doy = np.array([d.timetuple().tm_yday for d in dates])
    t = 18 - 10 * np.cos(2 * math.pi * (doy - 15) / 365.25) + rng.normal(0, 3, len(dates))
    lag = np.concatenate([[t[0]], t[:-1]])
    years = np.arange(len(dates)) / 365.25
    weekend = np.array([d.weekday() >= 5 for d in dates])
    y = (1000 + 100 * years + cool * np.clip(t - 24, 0, None) + heat * np.clip(10 - t, 0, None) - 80 * weekend
         + rng.normal(0, 5, len(dates)))
    types = wn.day_types(dates)
    return pl.DataFrame({"weather_zone": "Z", "date": dates, "dmean": y, "dmax": y * 1.3, "n_hours": 24,
                         "t_mean": t, "t_lag": lag, "td_mean": t - 8, "t_max": t + 6, "day_type": types,
                         "excluded": False})


def test_fit_recovers_the_weather_response():
    panel = _synthetic_panel()
    fit = wn.fit_daily(panel, "dmean", 2018, 2020, spec=wn.Spec(lag=False, humid=False, fourier=0), zone="Z")
    cool, se = fit.slope(29.0)
    assert cool == pytest.approx(30.0, abs=1.5)
    assert se > 0
    assert fit.slope(7.0)[0] == pytest.approx(-20.0, abs=1.5)
    assert fit.slope(18.0)[0] == pytest.approx(0.0, abs=1.5)
    assert fit.sigma == pytest.approx(5.0, rel=0.2)


def test_normalizing_to_the_same_weather_returns_the_actual():
    panel = _synthetic_panel()
    feats = panel.select("weather_zone", "date", "t_mean", "t_max", "td_mean", "t_lag")
    fit = wn.fit_daily(panel, "dmean", 2018, 2020, spec=wn.Spec(fourier=0), zone="Z")
    frame = panel.filter(pl.col("date").dt.year() == 2019)
    out = wn.normalize_year(fit, frame, wn.normal_weather(feats, (2019, 2019)))
    assert out["n_draws"].to_list() == [1] * frame.height
    assert (out["normalized"] - out["dmean"]).abs().max() == pytest.approx(0.0, abs=1e-6)


def test_excluded_days_normalize_the_fitted_day_not_the_shed_load():
    panel = _synthetic_panel()
    feats = panel.select("weather_zone", "date", "t_mean", "t_max", "td_mean", "t_lag")
    fit = wn.fit_daily(panel, "dmean", 2018, 2020, spec=wn.Spec(fourier=0), zone="Z")
    shed = date(2019, 2, 16)
    frame = panel.filter(pl.col("date").dt.year() == 2019).with_columns(
        pl.when(pl.col("date") == shed).then(pl.col("dmean") * 0.4).otherwise(pl.col("dmean")).alias("dmean"),
        (pl.col("date") == shed).alias("excluded"),
    )
    out = wn.normalize_year(fit, frame, wn.normal_weather(feats, (2019, 2019)))
    day = out.filter(pl.col("date") == shed).row(0, named=True)
    assert day["normalized"] == pytest.approx(day["fitted"], abs=1e-6)
    kept = out.filter(pl.col("date") != shed)
    assert (kept["normalized"] - kept["dmean"]).abs().max() == pytest.approx(0.0, abs=1e-6)


def test_normal_weather_is_the_mean_response_over_normal_years():
    panel = _synthetic_panel()
    feats = panel.select("weather_zone", "date", "t_mean", "t_max", "td_mean", "t_lag")
    fit = wn.fit_daily(panel, "dmean", 2018, 2020, spec=wn.Spec(lag=False, humid=False, fourier=0), zone="Z")
    frame = panel.filter(pl.col("date") == date(2020, 7, 15))
    out, draws = wn.normalize_year(fit, frame, wn.normal_weather(feats, (2018, 2019)), keep_draws=True)
    assert sorted(draws["weather_year"].to_list()) == [2018, 2019]
    by_year = panel.filter(pl.col("date").is_in([date(2018, 7, 15), date(2019, 7, 15)]))
    expected = fit.weather_effect(by_year).mean()
    assert out["w_normal"].item() == pytest.approx(float(expected))
    assert out["normalized"].item() == pytest.approx(
        frame["dmean"].item() - float(fit.weather_effect(frame)[0]) + float(expected))


def test_weather_draws_read_feb_28_for_feb_29():
    feats = pl.DataFrame({"weather_zone": "Z", "date": [date(2019, 2, 28), date(2019, 3, 1)], "t_mean": [5.0, 6.0],
                          "t_max": [9.0, 9.0], "td_mean": [0.0, 0.0], "t_lag": [4.0, 5.0]})
    frame = pl.DataFrame({"weather_zone": "Z", "date": [date(2020, 2, 29)]})
    draws = wn.weather_draws(frame, wn.normal_weather(feats, (2019, 2019)))
    assert draws["t_mean"].to_list() == [5.0]


def test_normal_season_peak_swaps_only_the_weather():
    days = pl.DataFrame({"weather_zone": "Z", "date": [date(2024, 7, 1), date(2024, 7, 2)],
                         "dmax": [100.0, 110.0], "w_actual": [10.0, 30.0]})
    draws = pl.DataFrame({"date": [date(2024, 7, 1), date(2024, 7, 2)] * 2,
                          "weather_year": [2003, 2003, 2004, 2004], "w_draw": [40.0, 0.0, 10.0, 30.0],
                          "weather_zone": "Z"})
    season = wn.normal_season_peak(days, draws)
    # residual+rest: 90 and 80; 2003 → max(130, 80) = 130; 2004 → max(100, 110) = 110
    assert dict(zip(season["weather_year"].to_list(), season["season_peak"].to_list(), strict=True)) == {
        2003: 130.0, 2004: 110.0}
    q = wn.peak_quantiles(season).row(0, named=True)
    assert q["p50"] == pytest.approx(120.0) and q["weather_years"] == 2


def test_monthly_average_mw_and_partial_month():
    days = pl.DataFrame({"weather_zone": "Z", "date": [date(2026, 9, 1), date(2026, 9, 2)], "dmean": [100.0, 200.0],
                         "normalized": [110.0, 190.0], "n_hours": [24, 24], "t_mean": [25.0, 27.0]})
    peaks = days.with_columns(pl.col("dmean").alias("dmax"), (pl.col("normalized") + 5).alias("normalized"))
    row = wn.monthly(days, peaks).row(0, named=True)
    assert row["energy_gwh"] == pytest.approx(7.2)
    assert row["avg_mw"] == pytest.approx(150.0) and row["avg_norm_mw"] == pytest.approx(150.0)
    assert row["complete"] is False
    assert row["peak_mw"] == 200.0 and row["peak_norm_mw"] == 195.0


def test_holdout_relevel_removes_the_mean_error():
    panel = _synthetic_panel()
    errs = wn.holdout(panel, "Z", "dmean", wn.Spec(lag=False, humid=False, fourier=0), train=(2018, 2019),
                      test=(2020,))
    assert (errs["actual"] - errs["pred_relevel"]).mean() == pytest.approx(0.0, abs=1e-6)
    table = wn.holdout_mape(errs).row(0, named=True)
    assert table["mape_relevel"] < table["mape_noweather"]
    assert wn.cagr(100.0, 121.0, 2) == pytest.approx(0.1)
