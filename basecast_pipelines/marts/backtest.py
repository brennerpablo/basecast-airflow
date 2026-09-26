"""Backtest marts (A-M6): the observed summer peaks the forecasts are scored against.

``mart_actual_summer_peaks``: one row per summer (June-September) from ``ercot_monthly_peaks``, the hourly peak
(the forecasts' definition, hour ending) and the 15-minute one (interval ending, as X15 confirmed), as in Q1 §2.
A summer counts as final only when all four months are published with final settlements.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from basecast_pipelines.marts.core import Mart, MartContext, value_check

GOLDEN_AS_OF = date(2026, 9, 26)  # the docs' run date (BUILD_A A-M6)

MONTHLY_PEAKS_SQL = (
    "select report_year, month, region_type, metric, value, peak_local, peak_ts_utc, final_settlement "
    "from ercot_monthly_peaks where region_type = 'ercot' and metric in ('peak_hourly_mw', 'peak_15min_mw')"
)


def actual_summer_peaks(ctx: MartContext) -> pl.DataFrame:
    from basecast_pipelines.models import backtest as bt

    monthly = ctx.read_sql(MONTHLY_PEAKS_SQL)
    hourly = bt.summer_peaks(monthly, metric="peak_hourly_mw")
    quarter = bt.summer_peaks(monthly, metric="peak_15min_mw").select(
        "year", pl.col("actual_mw").alias("peak_15min_mw"), pl.col("peak_ts_utc").alias("peak_15min_ts_utc"),
        pl.col("peak_local").alias("_q_local"),
    )
    hour = pl.col("peak_local").dt.hour()
    return (
        hourly.join(quarter, on="year", how="left")
        .select(
            "year",
            pl.col("actual_mw").alias("hourly_peak_mw"),
            "peak_ts_utc",
            pl.col("peak_local").dt.date().alias("peak_date_local"),
            pl.when(hour == 0).then(24).otherwise(hour).cast(pl.Int32).alias("hour_ending_local"),
            "peak_15min_mw",
            "peak_15min_ts_utc",
            pl.col("_q_local").dt.strftime("%H:%M").alias("interval_end_local_15min"),
            (pl.col("complete") & pl.col("all_final")).alias("final"),
            "months_published",
            pl.col("last_month").dt.month_end().alias("data_as_of"),
        )
        .sort("year")
    )


def _year(frame: pl.DataFrame, year: int, col: str):
    return frame.filter(pl.col("year") == year)[col].item()


ACTUAL_SUMMER_PEAKS = Mart(
    name="mart_actual_summer_peaks",
    build=actual_summer_peaks,
    key=("year",),
    inputs=("ercot_monthly_peaks",),
    description="ERCOT's summer (Jun-Sep) peak per year: hourly (hour ending) and 15-minute (interval ending), "
    "with the settlement status; the actuals the forecasts and the backtest are scored against (Q1 §2).",
    caveats=("preliminary_actuals",),
    checks=(
        value_check("one row per year", lambda f: f["year"].is_duplicated().any(), False),
        value_check("2026 hourly peak MW", lambda f: _year(f, 2026, "hourly_peak_mw"), 91_134, as_of=GOLDEN_AS_OF,
                    tol=1),
        value_check("2026 peak at HE 18", lambda f: _year(f, 2026, "hour_ending_local"), 18, as_of=GOLDEN_AS_OF),
        value_check("2026 peak date", lambda f: _year(f, 2026, "peak_date_local"), date(2026, 7, 22),
                    as_of=GOLDEN_AS_OF),
        value_check("2026 not final", lambda f: _year(f, 2026, "final"), False, as_of=GOLDEN_AS_OF),
        value_check("2023 hourly peak MW", lambda f: _year(f, 2023, "hourly_peak_mw"), 85_508, as_of=GOLDEN_AS_OF,
                    tol=1),
    ),
)

MARTS = (ACTUAL_SUMMER_PEAKS,)
