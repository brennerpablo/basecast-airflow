"""Backtest of ERCOT's official summer peak forecasts (LTLF, CDR, manual figures) against the actual peak.

Pure functions on DataFrames plus two thin loaders. The actual peak is the ERCOT system peak of June–September
from ``ercot_monthly_peaks`` (Demand and Energy report); ``peak_hourly_mw`` (hour ending) is the default
because the LTLF and the CDR forecast the hourly coincident system peak. Each forecast vintage contributes one
base series (``base_series``), matched to the actual by target year with a horizon in summers ahead
(``summers_ahead``). Errors are ``forecast − actual``, in percent of the actual: positive means the official
forecast was too high. The forecasts are normal-weather (P50) values, so each error mixes forecast bias with
that summer's weather.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from basecast_pipelines.models.db import read_sql

SUMMER_MONTHS = (6, 7, 8, 9)
PRELIM = "LTLF-prelim"

# LTLF rows by preference for one (vintage, target year): the adopted ERCOT-adjusted summer peak (2025), the
# normal-weather summer coincident peak, its P50 twin, then the max of the monthly peaks, then the annual peak.
_LTLF_PICKS = (
    ("summer", "ercot_adjusted", "summer peak_mw ercot_adjusted"),
    ("summer", "base", "summer peak_mw base"),
    ("summer", "base_p50", "summer peak_mw base_p50"),
    ("month", "ercot_adjusted", "max Jun-Sep monthly peak_mw ercot_adjusted"),
    ("month", "base", "max Jun-Sep monthly peak_mw base"),
    ("annual", "base", "annual peak_mw base"),
)

_BASE_COLUMNS = ["product", "vintage", "vintage_date", "target_year", "forecast_mw", "series"]


def summer_peaks(monthly: pl.DataFrame, *, metric: str = "peak_hourly_mw",
                 months: tuple[int, ...] = SUMMER_MONTHS) -> pl.DataFrame:
    """Actual ERCOT summer peak per year from ``ercot_monthly_peaks`` rows.

    One row per year: the value, local time and UTC instant of the largest monthly peak in ``months``, how many
    of those months are published (``complete`` when all are), the last month present and whether every month
    carries final settlements.
    """
    rows = monthly.filter(
        (pl.col("region_type") == "ercot")
        & (pl.col("metric") == metric)
        & pl.col("month").dt.month().is_in(list(months))
    )
    return (
        rows.sort("value", descending=True)
        .group_by("report_year", maintain_order=True)
        .agg(
            pl.col("value").first().alias("actual_mw"),
            pl.col("peak_local").first(),
            pl.col("peak_ts_utc").first(),
            pl.col("month").first().alias("peak_month"),
            pl.col("month").n_unique().alias("months_published"),
            pl.col("month").max().alias("last_month"),
            pl.col("final_settlement").all().alias("all_final"),
        )
        .with_columns(
            pl.col("report_year").cast(pl.Int64).alias("year"),
            pl.lit(metric).alias("metric"),
            (pl.col("months_published") == len(months)).alias("complete"),
        )
        .drop("report_year")
        .sort("year")
        .select("year", "metric", "actual_mw", "peak_local", "peak_ts_utc", "peak_month", "months_published",
                "last_month", "complete", "all_final")
    )


def hourly_summer_peaks(hourly: pl.DataFrame, *, months: tuple[int, ...] = SUMMER_MONTHS) -> pl.DataFrame:
    """ERCOT summer peak per year from ``ercot_load_hourly_wz`` (``weather_zone == "ERCOT"`` rows).

    A cross-check of ``summer_peaks`` and the only view of the months the Demand and Energy workbook has not
    published yet; ``data_as_of`` is the last hour ending present.
    """
    rows = hourly.filter(
        (pl.col("weather_zone") == "ERCOT") & pl.col("hour_ending_local").dt.month().is_in(list(months))
    )
    return (
        rows.sort("mw", descending=True)
        .group_by(pl.col("hour_ending_local").dt.year().cast(pl.Int64).alias("year"), maintain_order=True)
        .agg(
            pl.col("mw").first().alias("actual_mw"),
            pl.col("hour_ending_local").first().alias("peak_hour_ending_local"),
            pl.col("ts_utc").first().alias("peak_ts_utc"),
            pl.col("source").first().alias("peak_source"),
            pl.col("hour_ending_local").max().alias("data_as_of"),
        )
        .sort("year")
    )


def summers_ahead(vintage_date: date, target_year: int) -> int:
    """Horizon in summers: 1 is the first summer (June 1 on) that starts after the vintage was published.

    A vintage dated before June 1 of year Y sees summer Y as its first; one dated June 1 or later already sits
    inside summer Y, so summer Y is 0 (partly observed) and Y+1 is 1.
    """
    first = vintage_date.year if vintage_date < date(vintage_date.year, 6, 1) else vintage_date.year + 1
    return target_year - first + 1


def horizon_expr(vintage_date: str = "vintage_date", target_year: str = "target_year") -> pl.Expr:
    """``summers_ahead`` as a polars expression over a date column and a year column."""
    vd = pl.col(vintage_date)
    first = vd.dt.year().cast(pl.Int64) + (vd.dt.month() >= 6).cast(pl.Int64)
    return pl.col(target_year).cast(pl.Int64) - first + 1


def _pick_ltlf(ltlf: pl.DataFrame) -> pl.DataFrame:
    parts = []
    for rank, (season, scenario, label) in enumerate(_LTLF_PICKS):
        rows = ltlf.filter((pl.col("season") == season) & (pl.col("scenario") == scenario))
        if season == "month":
            rows = rows.filter(pl.col("target_month").is_in(list(SUMMER_MONTHS)))
        if rows.is_empty():
            continue
        parts.append(
            rows.group_by("vintage", "target_year")
            .agg(pl.col("value").max().alias("forecast_mw"), pl.col("vintage_date").min())
            .with_columns(pl.lit(rank).alias("rank"), pl.lit(label).alias("series"))
        )
    if not parts:
        return pl.DataFrame(schema={c: pl.String for c in _BASE_COLUMNS})
    return (
        pl.concat(parts, how="vertical_relaxed")
        .sort("rank")
        .group_by("vintage", "target_year", maintain_order=True)
        .first()
        .with_columns(pl.lit("LTLF").alias("product"))
        .select(_BASE_COLUMNS)
    )


def _pick_cdr(cdr: pl.DataFrame) -> pl.DataFrame:
    rows = cdr.filter(
        (pl.col("season") == "summer") & (pl.col("metric") == "peak_mw") & (pl.col("region_type") == "ercot")
        & pl.col("scenario").is_null()
    )
    # A report re-published in the same month ("CDR May 2025" and "CDR May 2025 Revised") counts once: the revision.
    return (
        rows.with_columns(pl.col("vintage").str.contains("(?i)revised").alias("_revised"))
        .sort("_revised", "vintage")
        .group_by("vintage_date", "target_year", maintain_order=True)
        .last()
        .with_columns(pl.lit("CDR").alias("product"), pl.col("value").alias("forecast_mw"),
                      pl.lit("summer peak_mw (normal weather)").alias("series"))
        .select(_BASE_COLUMNS)
    )


def base_series(forecasts: pl.DataFrame) -> pl.DataFrame:
    """One forecast per (product, vintage, target year): the series comparable to the actual system peak.

    - ``CDR``: the summer ``peak_mw`` line at the ERCOT level with no scenario (the report's normal-weather summer
      peak demand; ``firm_peak_mw`` nets out load resources and ``peak_before_ee_mw`` adds back efficiency, so
      neither matches metered load). Same-month re-publications count once.
    - ``LTLF``: ERCOT-level ``peak_mw`` by the preference in ``_LTLF_PICKS`` (non-coincident zone sums excluded).
    - ``LTLF-prelim``: the manual figure of the 2026 preliminary LTLF (``peak_demand``, summer, ``base``).
    """
    ltlf = forecasts.filter(
        (pl.col("product") == "LTLF") & (pl.col("metric") == "peak_mw") & (pl.col("region_type") == "ercot")
    )
    prelim = (
        forecasts.filter(
            (pl.col("product") == "LTLF") & (pl.col("metric") == "peak_demand") & (pl.col("season") == "summer")
            & (pl.col("scenario") == "base")
        )
        .with_columns(pl.lit(PRELIM).alias("product"), pl.col("value").alias("forecast_mw"),
                      pl.lit("manual peak_demand summer base").alias("series"))
        .select(_BASE_COLUMNS)
    )
    parts = [_pick_cdr(forecasts.filter(pl.col("product") == "CDR")), _pick_ltlf(ltlf), prelim]
    parts = [p.with_columns(pl.col("target_year").cast(pl.Int64), pl.col("forecast_mw").cast(pl.Float64),
                            pl.col("vintage_date").cast(pl.Date))
             for p in parts if not p.is_empty()]
    return pl.concat(parts).sort("product", "vintage_date", "vintage", "target_year")


def match_forecasts(base: pl.DataFrame, actuals: pl.DataFrame) -> pl.DataFrame:
    """Join each base forecast to the actual summer peak of its target year; error = forecast − actual."""
    joined = base.join(
        actuals.select("year", "actual_mw", "complete", "peak_local"),
        left_on="target_year", right_on="year", how="inner",
    )
    return (
        joined.with_columns(
            horizon_expr().alias("horizon"),
            (pl.col("forecast_mw") - pl.col("actual_mw")).alias("error_mw"),
        )
        .with_columns((pl.col("error_mw") / pl.col("actual_mw") * 100).alias("error_pct"))
        .rename({"complete": "actual_complete", "peak_local": "actual_peak_local"})
        .sort("product", "vintage_date", "target_year")
    )


def horizon_summary(errors: pl.DataFrame) -> pl.DataFrame:
    """Error in percent by product and horizon: n, mean, median, min, max and the share of over-forecasts."""
    return (
        errors.group_by("product", "horizon")
        .agg(
            pl.len().alias("n"),
            pl.col("error_pct").mean().alias("mean_pct"),
            pl.col("error_pct").median().alias("median_pct"),
            pl.col("error_pct").min().alias("min_pct"),
            pl.col("error_pct").max().alias("max_pct"),
            (pl.col("error_pct") > 0).mean().alias("share_over"),
        )
        .sort("product", "horizon")
    )


def sign_verdict(summary: pl.DataFrame, *, horizons: tuple[int, ...] = (1, 2, 3, 4, 5), min_n: int = 3,
                 min_share: float = 2 / 3) -> pl.DataFrame:
    """Direction of the error per product and horizon, and whether it is the same at every horizon.

    A horizon is ``over`` when at least ``min_share`` of its vintages over-forecast, ``under`` when at least
    ``min_share`` under-forecast, ``mixed`` otherwise; horizons with fewer than ``min_n`` vintages are skipped.
    A product is ``consistent`` when every kept horizon has the same non-mixed direction.
    """
    kept = summary.filter(pl.col("horizon").is_in(list(horizons)) & (pl.col("n") >= min_n)).with_columns(
        pl.when(pl.col("share_over") >= min_share).then(pl.lit("over"))
        .when(pl.col("share_over") <= 1 - min_share).then(pl.lit("under"))
        .otherwise(pl.lit("mixed"))
        .alias("direction")
    )
    return (
        kept.group_by("product")
        .agg(
            pl.col("horizon").sort().alias("horizons"),
            pl.col("direction").sort_by("horizon").alias("directions"),
            pl.col("direction").n_unique().alias("_distinct"),
            pl.col("direction").first().alias("_first"),
        )
        .with_columns(
            pl.when((pl.col("_distinct") == 1) & (pl.col("_first") != "mixed"))
            .then(pl.concat_str(pl.lit("consistent "), pl.col("_first")))
            .otherwise(pl.lit("mixed"))
            .alias("verdict")
        )
        .drop("_distinct", "_first")
        .sort("product")
    )


def actual_summer_peaks(metric: str = "peak_hourly_mw") -> pl.DataFrame:
    """Load ``ercot_monthly_peaks`` (ERCOT system rows) and return ``summer_peaks``."""
    monthly = read_sql(
        "select report_year, month, region_type, metric, value, peak_local, peak_ts_utc, final_settlement "
        "from ercot_monthly_peaks where region_type = 'ercot' and metric = %(metric)s",
        {"metric": metric},
    )
    return summer_peaks(monthly, metric=metric)


def load_official_forecasts() -> pl.DataFrame:
    """The ``official_forecasts`` rows ``base_series`` can pick from (ERCOT-level peaks and the manual figures)."""
    return read_sql(
        "select vintage, vintage_date, publisher, product, target_year, target_month, season, region_type, "
        "region_id, metric, scenario, value, unit, row_label, source_file from official_forecasts "
        "where (metric = 'peak_mw' and region_type = 'ercot') or metric = 'peak_demand'"
    )
