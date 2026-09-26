"""Tiny synthetic checks for ``basecast_pipelines.models.four_cp`` (no database)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from basecast_pipelines.models import four_cp as fc

CHI = ZoneInfo(fc.LOCAL_TZ)


def _monthly_system(rows: list[tuple[date, float, datetime]]) -> pl.DataFrame:
    """``ercot_monthly_peaks``-shaped system 15-minute peaks: (month, MW, local interval end)."""
    return pl.DataFrame(
        {
            "month": [r[0] for r in rows],
            "region_type": "ercot",
            "region_id": "ERCOT",
            "metric": "peak_15min_mw",
            "value": [r[1] for r in rows],
            "interval_minutes": 15,
            "peak_local": [r[2] for r in rows],
            "peak_ts_utc": [r[2].replace(tzinfo=CHI).astimezone(UTC) for r in rows],
            "final_settlement": True,
        }
    ).with_columns(pl.col("peak_ts_utc").dt.convert_time_zone("UTC"))


def _hourly_load(days: dict[date, list[float]], zone: str = "ERCOT") -> pl.DataFrame:
    """Hour-ending rows for whole local days (23/24/25 values on DST days): ``ts_utc`` = interval end."""
    rows = []
    for day, values in days.items():
        start = datetime(day.year, day.month, day.day, tzinfo=CHI).astimezone(UTC)
        for i, mw in enumerate(values):
            end = start + timedelta(hours=i + 1)
            rows.append({"ts_utc": end, "operating_date": day, "hour_ending": i + 1, "weather_zone": zone, "mw": mw})
    return pl.DataFrame(rows).with_columns(pl.col("ts_utc").dt.convert_time_zone("UTC"))


def test_cp_intervals_minutes_hour_ending_and_day():
    monthly = _monthly_system([
        (date(2024, 6, 1), 80_000.0, datetime(2024, 6, 30, 17, 45)),
        (date(2024, 7, 1), 81_000.0, datetime(2024, 7, 1, 17, 0)),
        (date(2024, 5, 1), 70_000.0, datetime(2024, 5, 20, 16, 0)),  # not a CP month
    ])
    cps = fc.cp_intervals(monthly)
    assert cps["month"].to_list() == [6, 7]
    assert cps["end_min"].to_list() == [1065, 1020]
    assert cps["hour_ending"].to_list() == [18, 17]  # 17:30–17:45 lies in HE 18; 16:45–17:00 in HE 17
    assert cps["cp_date"].to_list() == [date(2024, 6, 30), date(2024, 7, 1)]
    assert cps["weekday"].to_list() == [7, 1]  # Sunday, Monday


def test_in_window_needs_the_whole_interval():
    cps = pl.DataFrame({"end_min": [1020, 960, 975, 1080], "interval_minutes": [15, 15, 15, 60]})
    # 2 h window 16:00–18:00: 16:45–17:00 in; 15:45–16:00 out; 16:00–16:15 in; hour 17:00–18:00 in
    assert cps.select(fc.in_window(960, 120)).to_series().to_list() == [True, False, True, True]
    assert fc.window_label(945, 120) == "15:45–17:45"
    cov = fc.window_coverage(cps, 60, [960])
    assert cov["covered"].item() == 2  # 16:45–17:00 and 16:00–16:15


def test_daily_peaks_on_a_dst_day():
    # 2024-11-03 (fall back) has 25 hours; the peak is the 25th hour ending.
    fall = [50.0] * 24 + [60.0]
    spring = [40.0] * 22 + [45.0]  # 2024-03-10 has 23 hours
    load = _hourly_load({date(2024, 11, 3): fall, date(2024, 3, 10): spring})
    daily = fc.daily_peaks(load, months=(3, 11))
    assert daily["n_hours"].to_list() == [23, 25]
    assert daily["peak_he"].to_list() == [23, 25]
    assert daily["peak_mw"].to_list() == [45.0, 60.0]


def test_hourly_cps_and_calendar_fill():
    days = {date(2026, 9, 1): [10.0] * 16 + [90.0] + [10.0] * 7, date(2026, 9, 2): [10.0] * 24}
    hourly = fc.hourly_cps(_hourly_load(days))
    assert hourly.select("year", "month", "cp_mw", "hour_ending", "end_min").row(0) == (2026, 9, 90.0, 17, 1020)
    assert hourly["end_local"].item() == datetime(2026, 9, 1, 17, 0)
    fifteen = fc.cp_intervals(_monthly_system([(date(2026, 8, 1), 91.0, datetime(2026, 8, 24, 17, 0))]))
    cal = fc.cp_calendar(fifteen, hourly)
    assert cal["source"].to_list() == ["15min", "hourly"]
    assert cal["interval_minutes"].to_list() == [15, 60]


def _daily(values: dict[date, float]) -> pl.DataFrame:
    return pl.DataFrame({"operating_date": list(values), "peak_mw": list(values.values())}).with_columns(
        pl.col("operating_date").dt.year().cast(pl.Int32).alias("year"),
        pl.col("operating_date").dt.month().cast(pl.Int32).alias("month"),
    )


def test_threshold_dispatch_uses_month_to_date_max():
    d = _daily({date(2024, 6, 29): 100.0, date(2024, 6, 30): 90.0, date(2024, 7, 1): 50.0, date(2024, 7, 2): 49.0,
                date(2024, 7, 3): 60.0})
    ref = fc.with_reference(d)
    assert ref["reference_mw"].to_list() == [None, 100.0, None, 50.0, 50.0]
    disp = fc.threshold_dispatch(ref, "peak_mw", 0.97, months=(6, 7))
    # June 29 and July 1 are month starts (dispatch); July 2 is 98% of 50 → dispatch; June 30 is 90% → not
    assert disp["dispatch"].to_list() == [True, False, True, True, True]
    trail = fc.with_reference(d, lookback_days=7)
    assert trail["reference_mw"].to_list()[2] == 100.0  # July 1 falls back on the last 7 days


def test_top_n_and_evaluate_dispatch():
    d = _daily({date(2024, 7, 1): 50.0, date(2024, 7, 2): 70.0, date(2024, 7, 3): 60.0})
    disp = fc.top_n_dispatch(d, "peak_mw", 1, months=(7,))
    assert disp["dispatch"].to_list() == [False, True, False]
    cps = pl.DataFrame({"year": [2024], "month": [7], "cp_date": [date(2024, 7, 2)], "end_min": [1020],
                        "interval_minutes": [15]})
    hit = fc.evaluate_dispatch(disp, cps, 960, 60)
    assert hit.select("n_cp", "caught", "day_caught", "dispatch_days", "all_caught").row(0) == (1, 1, 1, 1, True)
    miss = fc.evaluate_dispatch(disp, cps, 1020, 60)  # window 17:00–18:00 misses the 16:45–17:00 interval
    assert miss.select("caught", "day_caught", "all_caught").row(0) == (0, 1, False)
    s = fc.summarize_dispatch(hit)
    assert s["all4_rate"] == 1.0 and s["dispatch_days"] == 1


def test_noisy_forecast_is_seeded_and_exact_at_zero():
    pytest.importorskip("numpy")
    d = _daily({date(2024, 7, 1): 50.0, date(2024, 7, 2): 70.0})
    assert fc.noisy_forecast(d, 0.0, seed=1)["forecast"].to_list() == [50.0, 70.0]
    a = fc.noisy_forecast(d, 0.05, seed=3)["forecast"].to_list()
    assert a == fc.noisy_forecast(d, 0.05, seed=3)["forecast"].to_list()


def test_weather_forecast_recovers_a_noise_free_level():
    pytest.importorskip("numpy")
    import math

    days = [date(y, m, dd) for y in (2021, 2022, 2023) for m in (6, 7) for dd in range(1, 29)]
    t = {dd: 30 + (i * 7 % 11) for i, dd in enumerate(days)}
    peak = {dd: math.exp(10 + 0.02 * t[dd] + 0.1 * (dd.year - 2021)) for dd in days}
    daily = _daily(peak)
    weather = pl.DataFrame({"date": days, "t_max": [float(t[dd]) for dd in days],
                            "t_mean": [float(t[dd]) - 8 for dd in days]})
    out = fc.weather_forecast(daily, weather, [2023], train_years=2, level_days=3)
    got = out.filter((pl.col("year") == 2023) & pl.col("forecast_wx").is_not_null())
    assert got.height > 40
    rel = (got["forecast_wx"] / got["peak_mw"] - 1).abs().max()
    assert rel < 1e-6


def test_zone_coincidence_ratios():
    rows = []
    for m, (cp_a, ncp_a, cp_b, ncp_b) in {6: (60.0, 80.0, 40.0, 40.0), 7: (80.0, 100.0, 20.0, 50.0)}.items():
        for zone, cp, ncp in (("A", cp_a, ncp_a), ("B", cp_b, ncp_b)):
            month = date(2024, m, 1)
            rows += [
                (month, zone, "coincident_peak_15min_mw", cp, datetime(2024, m, 10, 17, 0)),
                (month, zone, "noncoincident_peak_15min_mw", ncp, datetime(2024, m, 11, 16, 30)),
                (month, zone, "energy_mwh", 1000.0, None),
            ]
    monthly = pl.DataFrame(rows, schema=["month", "region_id", "metric", "value", "peak_local"], orient="row")
    monthly = monthly.with_columns(pl.lit("weather_zone").alias("region_type"))
    z = fc.zone_coincidence(monthly).sort("region_id")
    a, b = z.row(0, named=True), z.row(1, named=True)
    assert a["cp_avg_mw"] == 70.0 and a["ncp_summer_mw"] == 100.0 and a["cf_summer"] == pytest.approx(0.7)
    assert a["cf_month"] == pytest.approx((60 / 80 + 80 / 100) / 2)
    assert a["share_4cp"] == pytest.approx(70 / 100) and b["share_4cp"] == pytest.approx(30 / 100)
    assert a["share_energy"] == pytest.approx(0.5)
    assert a["ncp_end_hour"] == pytest.approx(16.5)
    assert a["n_months"] == 2


def test_hourly_from_15min_keys_by_utc_hour_end_on_fall_back():
    # 2024-11-03: the repeated 01:00 hour (CDT then CST) is two distinct UTC hours.
    start = datetime(2024, 11, 3, 5, 0, tzinfo=UTC)  # 00:00 CDT
    stamps = [start + timedelta(minutes=15 * i) for i in range(12)]  # 3 UTC hours
    fuel = pl.DataFrame({"interval_start_utc": stamps, "generation_mwh": [1.0] * 12}).with_columns(
        pl.col("interval_start_utc").dt.convert_time_zone("UTC")
    )
    h = fc.hourly_from_15min(fuel, "generation_mwh")
    assert h["n_intervals"].to_list() == [4, 4, 4]
    assert h["generation_mwh"].to_list() == [4.0, 4.0, 4.0]
    assert h["ts_utc"].to_list()[0] == datetime(2024, 11, 3, 6, 0, tzinfo=UTC)
    local = h["ts_utc"].dt.convert_time_zone(fc.LOCAL_TZ).dt.hour().to_list()
    assert local == [1, 1, 2]  # hour ending 01:00 CDT, 01:00 CST (repeated), 02:00 CST


def test_net_load_and_monthly_argmax():
    day = date(2025, 7, 1)
    load = _hourly_load({day: [100.0] * 17 + [150.0, 120.0, 110.0, 100.0, 90.0, 80.0, 70.0]})
    start = datetime(2025, 7, 1, tzinfo=CHI).astimezone(UTC)
    stamps = [start + timedelta(minutes=15 * i) for i in range(96)]
    solar = [10.0 if 40 <= i < 72 else 0.0 for i in range(96)]  # 10 MWh per 15 min, 10:00–18:00 local
    fuel = pl.DataFrame({
        "interval_start_utc": stamps * 2,
        "fuel": ["wind"] * 96 + ["solar"] * 96,
        "generation_mwh": [0.0] * 96 + solar,
    }).with_columns(pl.col("interval_start_utc").dt.convert_time_zone("UTC"))
    net = fc.net_load_hourly(load, fuel)
    assert net.height == 24
    he18 = net.filter(pl.col("hour_ending") == 18)
    assert he18["solar_mw"].item() == 40.0 and he18["net_mw"].item() == 110.0
    top = fc.monthly_argmax(net, "net_mw")
    assert top.select("net_mw_max", "net_mw_he").row(0) == (120.0, 19)  # the load peak (HE 18) is solar-covered
    ranked = fc.rank_within_month(net, "net_mw")
    assert ranked.filter(pl.col("hour_ending") == 19)["net_mw_rank"].item() == 1
