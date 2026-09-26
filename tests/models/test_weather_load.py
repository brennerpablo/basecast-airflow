"""Tiny synthetic checks for ``basecast_pipelines.models.weather_load`` (no database)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from basecast_pipelines.models import weather_load as wl


def _hours(start: datetime, end: datetime) -> list[datetime]:
    out, t = [], start
    while t < end:
        out.append(t)
        t += timedelta(hours=1)
    return out


def _load_rows(start_utc: datetime, end_utc: datetime, zones: dict[str, float]) -> pl.DataFrame:
    """Hour-ending rows: ``ts_utc`` is the interval end, ``operating_date`` the Chicago date of its start."""
    rows = []
    for end in _hours(start_utc, end_utc):
        start_local = (end - timedelta(hours=1)).astimezone(ZoneInfo(wl.LOCAL_TZ))
        for zone, mw in zones.items():
            rows.append({"ts_utc": end, "operating_date": start_local.date(), "hour_ending": start_local.hour + 1,
                         "dst_flag": False, "weather_zone": zone, "mw": mw})
    return pl.DataFrame(rows).with_columns(pl.col("ts_utc").dt.convert_time_zone("UTC"))


def test_weather_daily_uses_chicago_days_on_dst_changes():
    # Fall back 2025-11-02: 25 local hours; spring forward 2025-03-09: 23.
    stamps = _hours(datetime(2025, 3, 8, 0, tzinfo=UTC), datetime(2025, 3, 11, 0, tzinfo=UTC)) + _hours(
        datetime(2025, 11, 1, 0, tzinfo=UTC), datetime(2025, 11, 4, 6, tzinfo=UTC)
    )
    weather = pl.DataFrame(
        {"weather_zone": "COAST", "ts_utc": stamps, "temperature_c": 20.0, "dew_point_c": 10.0}
    ).with_columns(pl.col("ts_utc").dt.convert_time_zone("UTC"))
    daily = wl.weather_daily(weather)
    hours = dict(zip(daily["date"].to_list(), daily["n_hours"].to_list(), strict=True))
    assert hours[date(2025, 11, 2)] == 25
    assert hours[date(2025, 3, 9)] == 23
    assert hours[date(2025, 11, 3)] == 24
    # The first UTC hours belong to the previous Chicago day.
    assert hours[date(2025, 10, 31)] == 5


def test_load_alignment_checks_on_a_fall_back_day():
    # Operating day 2025-11-02 runs from 05:00 UTC to 06:00 UTC the next day: 25 hour-ending rows.
    load = _load_rows(datetime(2025, 11, 2, 6, tzinfo=UTC), datetime(2025, 11, 3, 7, tzinfo=UTC),
                      {"COAST": 100.0, "NCENT": 50.0, "ERCOT": 150.0})
    checks = wl.load_alignment_checks(load)
    assert checks["rows_start_date_not_operating_date"] == 0
    assert checks["hours_per_day"] == {25: 1}
    assert checks["gaps"].height == 0
    assert checks["zone_sum_minus_total_max_mw"] == 0


def test_summer_peaks_follow_the_operating_date():
    # HE 24 of May 31 ends at 05:00 UTC on June 1 but belongs to May: its 999 MW must not count.
    load = pl.DataFrame({
        "ts_utc": [datetime(2024, 6, 1, 5, tzinfo=UTC), datetime(2024, 6, 1, 22, tzinfo=UTC),
                   datetime(2024, 10, 1, 5, tzinfo=UTC), datetime(2024, 10, 1, 22, tzinfo=UTC)],
        "operating_date": [date(2024, 5, 31), date(2024, 6, 1), date(2024, 9, 30), date(2024, 10, 1)],
        "hour_ending": [24, 18, 24, 18],
        "dst_flag": False,
        "weather_zone": "FWEST",
        "mw": [999.0, 10.0, 20.0, 888.0],
    }).with_columns(pl.col("ts_utc").dt.convert_time_zone("UTC"))
    peaks = wl.summer_peaks(load)
    assert peaks.select("year", "peak_mw", "n_hours").row(0) == (2024, 20.0, 2)
    assert peaks["peak_local"].item().hour == 0  # HE 24 of Sep 30 ends at local midnight


def test_summer_weather_features_take_the_hottest_full_window():
    days = [date(2024, 5, 29) + timedelta(days=i) for i in range(8)]  # May 29 .. Jun 5
    t = [40.0, 40.0, 40.0, 10.0, 10.0, 10.0, 30.0, 30.0]
    daily = pl.DataFrame({"weather_zone": "WEST", "date": days, "t_mean": t, "t_max": t, "td_mean": t,
                          "hi_max": t, "n_hours": 24})
    feats = wl.summer_weather_features(daily)
    # Jun 1 averages May 30–Jun 1 (40, 40, 10) = 30; nothing later in June beats it.
    assert feats.row(0, named=True)["t_mean_3d"] == pytest.approx(30.0)
    assert feats.row(0, named=True)["weather_days"] == 5


def test_heat_index_matches_nws_ballpark():
    frame = pl.DataFrame({"t": [35.0, 20.0], "td": [25.0, 10.0]})
    hi = frame.select(wl.heat_index_c(pl.col("t"), pl.col("td")).alias("hi"))["hi"].to_list()
    assert 42 < hi[0] < 45  # NWS chart: 95 °F at ~56% RH ≈ 110 °F
    assert abs(hi[1] - 20.0) < 1.5


def test_zone_and_system_weights():
    load = _load_rows(datetime(2024, 7, 1, 6, tzinfo=UTC), datetime(2024, 7, 1, 8, tzinfo=UTC),
                      {"COAST": 300.0, "NCENT": 100.0, "ERCOT": 400.0})
    weights = wl.zone_weights(load, [2024])
    assert weights == pytest.approx({"COAST": 0.75, "NCENT": 0.25})
    daily = pl.DataFrame({"weather_zone": ["COAST", "NCENT"], "date": [date(2024, 7, 1)] * 2,
                          "t_mean": [30.0, 34.0], "t_max": [30.0, 34.0], "td_mean": [20.0, 20.0],
                          "hi_max": [30.0, 34.0], "n_hours": [24, 24]})
    system = wl.system_weather_daily(daily, weights)
    assert system["t_mean"].item() == pytest.approx(31.0)
    assert system["weather_zone"].item() == wl.TOTAL


def _panel(zone: str, years: range, trend: float, temp_coef: float, noise: list[float] | None = None):
    temps = [30 + ((y * 7) % 5) * 0.5 for y in years]  # deterministic, not collinear with the year
    noise = noise or [0.0] * len(years)
    peaks = [1000 + trend * (y - 2014) + temp_coef * t + e for y, t, e in zip(years, temps, noise, strict=True)]
    return pl.DataFrame({"weather_zone": zone, "year": list(years), "peak_mw": peaks, "t_mean_3d": temps})


def test_fit_peak_recovers_known_coefficients():
    pytest.importorskip("numpy")
    panel = _panel("SOUTH", range(2003, 2026), trend=50.0, temp_coef=200.0)
    fit = wl.fit_peak(panel, "t_mean_3d", zone="SOUTH")
    assert fit.coef_of("year") == pytest.approx(50.0)
    assert fit.coef_of("t_mean_3d") == pytest.approx(200.0)
    assert fit.r2 == pytest.approx(1.0)
    assert fit.resid_sd_mw == pytest.approx(0.0, abs=1e-6)
    assert fit.predict([2030], [31.0])[0] == pytest.approx(1000 + 50 * 16 + 200 * 31)


def test_holdout_scores_the_test_years_only():
    pytest.importorskip("numpy")
    clean = _panel("NCENT", range(2003, 2026), trend=50.0, temp_coef=200.0)
    shocked = clean.with_columns(
        pl.when(pl.col("year") >= 2023).then(pl.col("peak_mw") * 1.1).otherwise(pl.col("peak_mw")).alias("peak_mw")
    ).with_columns(pl.lit("FWEST").alias("weather_zone"))
    errors = wl.holdout(pl.concat([clean, shocked]), "t_mean_3d")
    assert sorted(set(errors["year"].to_list())) == [2023, 2024, 2025]
    scores = dict(zip(*wl.mape(errors).select("weather_zone", "mape_pct").to_dict(as_series=False).values(),
                      strict=True))
    assert scores["NCENT"] == pytest.approx(0.0, abs=1e-6)
    assert scores["FWEST"] == pytest.approx(100 * (1 - 1 / 1.1))  # predicted the unshocked value


def test_sup_chow_finds_a_trend_break():
    np = pytest.importorskip("numpy")
    rng = np.random.default_rng(1)
    years = list(range(2003, 2026))
    base = _panel("FWEST", range(2003, 2026), trend=20.0, temp_coef=10.0,
                  noise=rng.normal(0, 5, len(years)).tolist())
    kinked = base.with_columns(
        (pl.col("peak_mw") + 300.0 * (pl.col("year") - 2016).clip(0, None)).alias("peak_mw")
    )
    fit = wl.fit_peak(kinked, "t_mean_3d")
    X = fit.design(years, kinked["t_mean_3d"].to_list())
    result = wl.sup_chow(years, X, kinked["peak_mw"].to_numpy(), n_boot=200, seed=3)
    assert 2015 <= result["break_year"] <= 2019
    assert result["p_boot"] < 0.01
