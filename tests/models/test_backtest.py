"""Backtest of the official peak forecasts (``basecast_pipelines/models/backtest.py``) on tiny synthetic rows."""

from __future__ import annotations

from datetime import date, datetime

import polars as pl
import pytest

from basecast_pipelines.models import backtest as bt


def _monthly(rows: list[tuple]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={"report_year": pl.Int16, "month": pl.Date, "region_type": pl.String, "metric": pl.String,
                "value": pl.Float64, "peak_local": pl.Datetime("us"), "final_settlement": pl.Boolean},
        orient="row",
    ).with_columns(pl.col("peak_local").dt.replace_time_zone("UTC").alias("peak_ts_utc"))


def _forecasts(rows: list[dict]) -> pl.DataFrame:
    defaults = {"vintage": None, "vintage_date": None, "product": None, "target_year": None, "target_month": None,
                "season": "summer", "region_type": "ercot", "metric": "peak_mw", "scenario": None, "value": None}
    return pl.DataFrame(
        [defaults | r for r in rows],
        schema={"vintage": pl.String, "vintage_date": pl.Date, "product": pl.String, "target_year": pl.Int64,
                "target_month": pl.Int64, "season": pl.String, "region_type": pl.String, "metric": pl.String,
                "scenario": pl.String, "value": pl.Float64},
    )


@pytest.mark.parametrize(
    ("vintage_date", "target_year", "expected"),
    [
        (date(2016, 12, 12), 2017, 1),  # December LTLF: next summer is the first
        (date(2025, 4, 8), 2025, 1),  # published before June: that summer is still ahead
        (date(2025, 4, 8), 2026, 2),
        (date(2024, 7, 18), 2024, 0),  # published inside the summer: partly observed
        (date(2025, 6, 1), 2026, 1),
    ],
)
def test_summers_ahead_and_its_expression_agree(vintage_date, target_year, expected):
    assert bt.summers_ahead(vintage_date, target_year) == expected
    frame = pl.DataFrame({"vintage_date": [vintage_date], "target_year": [target_year]})
    assert frame.select(bt.horizon_expr()).item() == expected


def test_summer_peaks_takes_the_largest_june_to_september_month():
    monthly = _monthly([
        (2025, date(2025, 1, 1), "ercot", "peak_hourly_mw", 95.0, datetime(2025, 1, 20, 8), True),  # winter
        (2025, date(2025, 7, 1), "ercot", "peak_hourly_mw", 80.0, datetime(2025, 7, 30, 18), True),
        (2025, date(2025, 8, 1), "ercot", "peak_hourly_mw", 84.0, datetime(2025, 8, 18, 18), True),
        (2025, date(2025, 6, 1), "ercot", "peak_hourly_mw", 77.0, datetime(2025, 6, 19, 17), True),
        (2025, date(2025, 9, 1), "ercot", "peak_hourly_mw", 79.0, datetime(2025, 9, 4, 18), True),
        (2025, date(2025, 8, 1), "ercot", "peak_15min_mw", 84.3, datetime(2025, 8, 18, 17), True),
        (2026, date(2026, 6, 1), "ercot", "peak_hourly_mw", 82.0, datetime(2026, 6, 18, 17), True),
        (2026, date(2026, 7, 1), "ercot", "peak_hourly_mw", 91.0, datetime(2026, 7, 22, 18), False),
        (2026, date(2026, 7, 1), "load_zone", "peak_hourly_mw", 99.0, datetime(2026, 7, 22, 18), False),
    ])
    peaks = bt.summer_peaks(monthly)
    assert peaks["year"].to_list() == [2025, 2026]
    y25, y26 = peaks.rows(named=True)
    assert (y25["actual_mw"], y25["peak_local"], y25["complete"], y25["all_final"]) == (
        84.0, datetime(2025, 8, 18, 18), True, True)
    assert (y26["actual_mw"], y26["months_published"], y26["complete"], y26["all_final"]) == (91.0, 2, False, False)
    assert bt.summer_peaks(monthly, metric="peak_15min_mw")["actual_mw"].to_list() == [84.3]


def test_hourly_summer_peaks_reads_the_ercot_row_and_data_as_of():
    hourly = pl.DataFrame({
        "ts_utc": [datetime(2026, 7, 22, 23), datetime(2026, 7, 22, 23), datetime(2026, 9, 26, 5)],
        "hour_ending_local": [datetime(2026, 7, 22, 18), datetime(2026, 7, 22, 18), datetime(2026, 9, 26, 0)],
        "weather_zone": ["ERCOT", "COAST", "ERCOT"],
        "mw": [91.1, 25.0, 60.0],
        "source": ["a", "a", "b"],
    })
    row = bt.hourly_summer_peaks(hourly).row(0, named=True)
    assert (row["year"], row["actual_mw"], row["data_as_of"]) == (2026, 91.1, datetime(2026, 9, 26, 0))


def test_base_series_picks_one_comparable_value_per_vintage_and_target():
    forecasts = _forecasts([
        # LTLF 2025: ERCOT-adjusted wins over TSP-provided; months fill the years the summer table lacks.
        {"vintage": "LTLF 2025", "vintage_date": date(2025, 4, 8), "product": "LTLF", "target_year": 2026,
         "scenario": "ercot_adjusted", "value": 94.0},
        {"vintage": "LTLF 2025", "vintage_date": date(2025, 4, 8), "product": "LTLF", "target_year": 2026,
         "scenario": "tsp_provided", "value": 109.0},
        {"vintage": "LTLF 2025", "vintage_date": date(2025, 4, 8), "product": "LTLF", "target_year": 2032,
         "season": "month", "target_month": 8, "scenario": "ercot_adjusted", "value": 150.0},
        {"vintage": "LTLF 2025", "vintage_date": date(2025, 4, 8), "product": "LTLF", "target_year": 2032,
         "season": "month", "target_month": 1, "scenario": "ercot_adjusted", "value": 160.0},  # winter, ignored
        # LTLF 2022: no summer base, only P50 and months; P50 wins.
        {"vintage": "LTLF 2022", "vintage_date": date(2022, 2, 24), "product": "LTLF", "target_year": 2023,
         "scenario": "base_p50", "value": 79.3},
        {"vintage": "LTLF 2022", "vintage_date": date(2022, 2, 24), "product": "LTLF", "target_year": 2023,
         "season": "month", "target_month": 8, "scenario": "base", "value": 79.1},
        # Zone sums and zones never enter.
        {"vintage": "LTLF 2022", "vintage_date": date(2022, 2, 24), "product": "LTLF", "target_year": 2023,
         "metric": "ncp_zone_sum_mw", "scenario": "base", "value": 85.0},
        # CDR: the revision of the same month wins; scenario rows and firm peak are ignored.
        {"vintage": "CDR May 2025", "vintage_date": date(2025, 5, 1), "product": "CDR", "target_year": 2026,
         "value": 96.0},
        {"vintage": "CDR May 2025 Revised", "vintage_date": date(2025, 5, 1), "product": "CDR", "target_year": 2026,
         "value": 95.4},
        {"vintage": "CDR May 2025", "vintage_date": date(2025, 5, 1), "product": "CDR", "target_year": 2026,
         "scenario": "peak_net_load_hour", "value": 90.0},
        {"vintage": "CDR May 2025", "vintage_date": date(2025, 5, 1), "product": "CDR", "target_year": 2026,
         "metric": "firm_peak_mw", "value": 93.0},
        # The manual 2026 preliminary figure and its range.
        {"vintage": "2026 preliminary LTLF", "vintage_date": date(2026, 4, 15), "product": "LTLF",
         "target_year": 2026, "region_type": "system", "metric": "peak_demand", "scenario": "base", "value": 112.0},
        {"vintage": "2026 preliminary LTLF", "vintage_date": date(2026, 4, 15), "product": "LTLF",
         "target_year": 2026, "region_type": "system", "metric": "peak_demand", "scenario": "range_low",
         "value": 90.5},
    ])
    base = bt.base_series(forecasts)
    got = {(r["product"], r["vintage"], r["target_year"]): r["forecast_mw"] for r in base.iter_rows(named=True)}
    assert got == {
        ("CDR", "CDR May 2025 Revised", 2026): 95.4,
        ("LTLF", "LTLF 2022", 2023): 79.3,
        ("LTLF", "LTLF 2025", 2026): 94.0,
        ("LTLF", "LTLF 2025", 2032): 150.0,
        (bt.PRELIM, "2026 preliminary LTLF", 2026): 112.0,
    }


def test_match_forecasts_error_is_forecast_minus_actual():
    base = pl.DataFrame({
        "product": ["LTLF", "LTLF", "LTLF"],
        "vintage": ["LTLF 2025", "LTLF 2025", "LTLF 2025"],
        "vintage_date": [date(2025, 4, 8)] * 3,
        "target_year": [2025, 2026, 2030],  # 2030 has no actual yet
        "forecast_mw": [88.0, 99.0, 150.0],
        "series": ["s"] * 3,
    })
    actuals = pl.DataFrame({"year": [2025, 2026], "actual_mw": [80.0, 90.0], "complete": [True, False],
                            "peak_local": [datetime(2025, 8, 18, 18), datetime(2026, 7, 22, 18)]})
    errors = bt.match_forecasts(base, actuals)
    assert errors["target_year"].to_list() == [2025, 2026]
    assert errors["horizon"].to_list() == [1, 2]
    assert errors["error_mw"].to_list() == [8.0, 9.0]
    assert errors["error_pct"].to_list() == pytest.approx([10.0, 10.0])
    assert errors["actual_complete"].to_list() == [True, False]


def test_sign_verdict_needs_the_same_direction_at_every_horizon():
    errors = pl.DataFrame({
        "product": ["A"] * 6 + ["B"] * 6,
        "horizon": [1, 1, 1, 2, 2, 2] * 2,
        "error_pct": [2.0, 3.0, -1.0, 4.0, 1.0, 5.0,  # A: over at both horizons
                      2.0, 3.0, 1.0, -4.0, -1.0, -5.0],  # B: over, then under
    })
    summary = bt.horizon_summary(errors)
    a1 = summary.filter((pl.col("product") == "A") & (pl.col("horizon") == 1)).row(0, named=True)
    assert (a1["n"], a1["median_pct"], a1["min_pct"], a1["max_pct"]) == (3, 2.0, -1.0, 3.0)
    assert a1["share_over"] == pytest.approx(2 / 3)
    verdict = bt.sign_verdict(summary, horizons=(1, 2))
    assert dict(verdict.select("product", "verdict").iter_rows()) == {"A": "consistent over", "B": "mixed"}
    strict = bt.sign_verdict(summary, horizons=(1, 2), min_share=0.9)
    assert dict(strict.select("product", "verdict").iter_rows()) == {"A": "mixed", "B": "mixed"}
