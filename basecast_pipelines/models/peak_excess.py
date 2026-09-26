"""Exploration X1: is the recent summer-peak excess over weather + trend the large loads showing up?

Builds on Q7 (``weather_load.py``: peak ~ year + weather per zone) and Q5 (``large_load.py``: the ERCOT large-load
decks). The "organic" reference is the pre-break model: fit on summers <= 2019, extrapolated with each later
summer's actual weather. Everything that summer carries above it is the *excess*.

Three views of the excess:

- **Non-coincident, per zone:** each zone's own summer peak minus its pre-break prediction.
- **Coincident, additive:** each zone's MW *at the ERCOT system peak hour*, fit on the same regressors (year and
  the ERCOT weather feature) for every zone. OLS is linear in the target, so the zones' predictions add up to
  the ERCOT prediction and the zone excesses add up to the ERCOT excess.
- **Shape (weather-light):** the summer mean of the daily minimum and of the daily maximum. A block of flat load
  (data centers, crypto, oil & gas electrification running 24 h) lifts both by the same MW; organic growth scales
  the daily curve, lifting the maximum more than the minimum. :func:`flat_block` solves the two equations.

All functions are pure on DataFrames; the only loaders are the ones in ``weather_load`` and ``large_load``.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import date

import polars as pl

from basecast_pipelines.models import large_load as ll
from basecast_pipelines.models import weather_load as wl

PRE_BREAK_UNTIL = 2019
SHAPE_MONTHS = (6, 7, 8, 9)
SHAPE_LAST_DAY = 20  # September days kept for the shape stats: 2026's weather ends on Sep 20

_SIMULTANEOUS = re.compile(r"(?i)^observed simultaneous")
_NON_SIMULTANEOUS = re.compile(r"(?i)non-simultaneous")


# --- daily and summer shape ----------------------------------------------------------------------------------


def daily_load_stats(load: pl.DataFrame) -> pl.DataFrame:
    """Hourly zone load -> one row per zone and operating day: min, max and mean MW, and the hour count."""
    return (
        load.group_by("weather_zone", pl.col("operating_date").alias("date"))
        .agg(
            pl.col("mw").min().alias("dmin"),
            pl.col("mw").max().alias("dmax"),
            pl.col("mw").mean().alias("dmean"),
            pl.len().alias("n_hours"),
        )
        .sort("weather_zone", "date")
    )


def _in_shape_window(months: Sequence[int] = SHAPE_MONTHS, last_sep_day: int = SHAPE_LAST_DAY) -> pl.Expr:
    m = pl.col("date").dt.month()
    return m.is_in(months) & ~((m == 9) & (pl.col("date").dt.day() > last_sep_day))


def summer_shape(daily: pl.DataFrame, months: Sequence[int] = SHAPE_MONTHS,
                 last_sep_day: int = SHAPE_LAST_DAY) -> pl.DataFrame:
    """Per zone and year, over June 1 - September ``last_sep_day`` (same window every year, so a partial
    September cannot bias one year): mean daily minimum, mean daily maximum, mean load, highest daily max, the
    load factor (mean / peak) and the minimum-to-maximum ratio of the average day."""
    return (
        daily.filter(_in_shape_window(months, last_sep_day))
        .group_by("weather_zone", pl.col("date").dt.year().alias("year"))
        .agg(
            pl.col("dmin").mean().alias("dmin_mean"),
            pl.col("dmax").mean().alias("dmax_mean"),
            pl.col("dmean").mean().alias("mean_mw"),
            pl.col("dmax").max().alias("peak_mw"),
            pl.len().alias("days"),
        )
        .with_columns(
            (pl.col("mean_mw") / pl.col("peak_mw")).alias("load_factor"),
            (pl.col("dmin_mean") / pl.col("dmax_mean")).alias("min_max_ratio"),
        )
        .sort("weather_zone", "year")
    )


def summer_mean_weather(weather_daily: pl.DataFrame, months: Sequence[int] = SHAPE_MONTHS,
                        last_sep_day: int = SHAPE_LAST_DAY) -> pl.DataFrame:
    """Per zone and year, the mean daily mean temperature over the shape window (``t_summer``)."""
    return (
        weather_daily.filter(_in_shape_window(months, last_sep_day))
        .group_by("weather_zone", pl.col("date").dt.year().alias("year"))
        .agg(pl.col("t_mean").mean().alias("t_summer"))
        .sort("weather_zone", "year")
    )


def coincident_zone_loads(load: pl.DataFrame, months: Sequence[int] = wl.SUMMER_MONTHS) -> pl.DataFrame:
    """Per year, each zone's MW in the hour of the ERCOT summer system peak (``coincident_mw``)."""
    summer = load.filter(pl.col("operating_date").dt.month().is_in(months)).with_columns(
        pl.col("operating_date").dt.year().alias("year")
    )
    peak_hours = (
        summer.filter(pl.col("weather_zone") == wl.TOTAL)
        .group_by("year")
        .agg(pl.col("ts_utc").sort_by("mw").last().alias("ts_utc"))
    )
    return (
        summer.join(peak_hours, on=["year", "ts_utc"], how="semi")
        .select("year", "weather_zone", "ts_utc", pl.col("mw").alias("coincident_mw"))
        .sort("year", "weather_zone")
    )


# --- excess over the pre-break model -------------------------------------------------------------------------


def excess_vs_prebreak(
    panel: pl.DataFrame,
    target: str,
    feature: str,
    *,
    train_until: int = PRE_BREAK_UNTIL,
    first_year: int | None = None,
) -> pl.DataFrame:
    """Per zone: fit ``target ~ 1 + (year - 2014) + feature`` on years <= ``train_until`` (Q7's ``fit_peak``) and
    predict every year with its actual ``feature``. ``excess_mw = actual - predicted``.

    ``panel`` has ``weather_zone``, ``year``, ``target`` and ``feature``. Also returns the training residual SD, so
    a reader can tell an excess from noise."""
    rows = []
    for zone in panel["weather_zone"].unique(maintain_order=True).to_list():
        sub = (
            panel.filter(pl.col("weather_zone") == zone)
            .select("year", pl.col(target).alias("peak_mw"), feature)
            .drop_nulls(["peak_mw", feature])
            .sort("year")
        )
        if first_year is not None:
            sub = sub.filter(pl.col("year") >= first_year)
        fit = wl.fit_peak(sub.filter(pl.col("year") <= train_until), feature, zone=zone)
        pred = fit.predict(sub["year"].to_list(), sub[feature].to_list())
        for year, actual, p in zip(sub["year"].to_list(), sub["peak_mw"].to_list(), pred.tolist(), strict=True):
            rows.append({
                "weather_zone": zone, "year": year, "actual_mw": actual, "predicted_mw": p,
                "excess_mw": actual - p, "excess_pct": 100 * (actual / p - 1),
                "train_resid_sd_mw": fit.resid_sd_mw, "trend_mw_yr": fit.coef_of("year"),
            })
    return pl.DataFrame(rows).sort("weather_zone", "year")


def flat_block(d_max: float, d_min: float, max0: float, min0: float) -> tuple[float, float]:
    """Split a change in the average day into a flat block and a proportional scaling.

    Model: new_max = max0 * (1 + g) + F and new_min = min0 * (1 + g) + F, i.e. ``d_max = g * max0 + F`` and
    ``d_min = g * min0 + F``. Returns ``(F, g)``: F in MW (flat, 24 h) and g the proportional growth of the
    baseline curve. Needs ``max0 != min0``."""
    if max0 == min0:
        raise ValueError("baseline max and min are equal: the split is undetermined")
    g = (d_max - d_min) / (max0 - min0)
    flat = d_min - g * min0
    return flat, g


def flat_block_table(shape_excess: pl.DataFrame, baseline: pl.DataFrame) -> pl.DataFrame:
    """Apply :func:`flat_block` per zone and year.

    ``shape_excess``: ``weather_zone``, ``year``, ``dmax_excess`` and ``dmin_excess`` (MW above the pre-break
    prediction). ``baseline``: ``weather_zone``, ``dmax0``, ``dmin0`` (the predicted average day that year)."""
    df = shape_excess.join(baseline, on=["weather_zone", "year"], how="inner")
    out = []
    for row in df.iter_rows(named=True):
        flat, g = flat_block(row["dmax_excess"], row["dmin_excess"], row["dmax0"], row["dmin0"])
        out.append({"weather_zone": row["weather_zone"], "year": row["year"], "flat_mw": flat,
                    "proportional_pct": 100 * g, "proportional_at_max_mw": g * row["dmax0"]})
    return pl.DataFrame(out).sort("weather_zone", "year")


# --- timing: when did a step start? --------------------------------------------------------------------------


def monthly_min_yoy(daily: pl.DataFrame) -> pl.DataFrame:
    """Per zone and month: mean daily minimum and mean daily maximum, and their change vs. the same month a year
    earlier (the seasonal cycle cancels, so a step shows up as twelve months of positive YoY)."""
    monthly = (
        daily.group_by("weather_zone", pl.col("date").dt.truncate("1mo").alias("month"))
        .agg(pl.col("dmin").mean().alias("dmin_mean"), pl.col("dmax").mean().alias("dmax_mean"),
             pl.len().alias("days"))
        .sort("weather_zone", "month")
    )
    prev = monthly.select(
        "weather_zone", pl.col("month").dt.offset_by("1y").alias("month"),
        pl.col("dmin_mean").alias("dmin_prev"), pl.col("dmax_mean").alias("dmax_prev"),
    )
    return monthly.join(prev, on=["weather_zone", "month"], how="left").with_columns(
        (pl.col("dmin_mean") - pl.col("dmin_prev")).alias("dmin_yoy"),
        (pl.col("dmax_mean") - pl.col("dmax_prev")).alias("dmax_yoy"),
    )


def step_scan(series: pl.DataFrame, value: str, *, window: int = 28, start: date | None = None,
              end: date | None = None) -> pl.DataFrame:
    """For each day, mean of ``value`` over the next ``window`` days minus the previous ``window`` days
    (``date`` sorted, one row per day). The day with the largest absolute step is where a level shift sits."""
    s = series.sort("date")
    vals = s[value]
    before = vals.rolling_mean(window_size=window, min_samples=window).shift(1)
    after = vals.reverse().rolling_mean(window_size=window, min_samples=window).reverse()
    out = s.select("date").with_columns((after - before).alias("step"))
    if start is not None:
        out = out.filter(pl.col("date") >= start)
    if end is not None:
        out = out.filter(pl.col("date") <= end)
    return out.drop_nulls("step")


# --- large-load decks ----------------------------------------------------------------------------------------


MAX_CHART_SPAN_MONTHS = 16


def drop_misdated_months(cv: pl.DataFrame, max_span_months: int = MAX_CHART_SPAN_MONTHS) -> pl.DataFrame:
    """Chart values with ``vintage`` and ``series_kind`` added, minus the monthly values of charts whose axis was
    misread. Delegates to ``large_load.drop_misdated_months`` (the fix at the source): a chart is misdated when
    its *last* month sits more than ``max_span_months`` before its deck, so old anchor bars in good charts are
    kept. X1 keeps its looser 16-month tolerance (Q5 uses 3); on the real decks both drop exactly the May 2026
    deck's four monthly charts. Non-month categories are kept."""
    return ll.drop_misdated_months(cv, max_lag_months=max_span_months)


def a2e_by_month_checked(cv: pl.DataFrame, max_span_months: int = MAX_CHART_SPAN_MONTHS) -> pl.DataFrame:
    """Q5's approved-to-energize stock by month (``large_load.a2e_by_month``, which now drops misdated axes
    itself) with X1's tolerance."""
    return ll.a2e_by_month(cv, max_lag_months=max_span_months)


def energized_by_month(cv: pl.DataFrame, max_span_months: int = MAX_CHART_SPAN_MONTHS) -> pl.DataFrame:
    """Observed peak consumption of approved large loads by month, latest deck's reading, from the
    "Loads Approved to Energize – Observations" charts (Gemini-read, ``verified=false``).

    ``simultaneous_mw`` = "Observed Simultaneous Peak Load" (the loads' combined peak in the month);
    ``non_simultaneous_mw`` = the sum of each load's own monthly peak (always >= simultaneous).

    The charts cover the past 11-15 months, so a month more than ``max_span_months`` before the deck is a
    mis-dated axis (the May 2026 deck's months were read as 2023-07..2024-05 but carry the 2025-07..2026-03
    values) and is dropped."""
    rows = (
        drop_misdated_months(cv, max_span_months)
        .filter((pl.col("series_kind") == "energized_by_month") & pl.col("category").str.contains(r"^\d{4}-\d{2}$")
                & (pl.col("status_bucket") == "observed_energized"))
        .with_columns(
            pl.when(pl.col("status_label").str.contains(_NON_SIMULTANEOUS.pattern)).then(pl.lit("non_simultaneous_mw"))
            .when(pl.col("status_label").str.contains(_SIMULTANEOUS.pattern)).then(pl.lit("simultaneous_mw"))
            .alias("basis")
        )
        .drop_nulls("basis")
        .sort("vintage")
    )
    latest = rows.group_by("category", "basis").agg(
        pl.col("value_mw").last().alias("mw"), pl.col("vintage").last().alias("read_from_vintage"),
        pl.col("value_mw").n_unique().alias("n_readings"),
    )
    wide = latest.pivot(on="basis", index="category", values="mw")
    vint = latest.group_by("category").agg(pl.col("read_from_vintage").max())
    for col in ("simultaneous_mw", "non_simultaneous_mw"):
        if col not in wide.columns:
            wide = wide.with_columns(pl.lit(None, dtype=pl.Float64).alias(col))
    return (
        wide.join(vint, on="category").rename({"category": "month"})
        .select("month", "simultaneous_mw", "non_simultaneous_mw", "read_from_vintage").sort("month")
    )


def approved_by_load_zone(cv: pl.DataFrame) -> pl.DataFrame:
    """"Approved to Energize by Load Zone" (LZ_WEST vs. Other) per deck: observed non-simultaneous peak,
    approved-not-operational and their total, MW. One row per vintage and zone (first document of the vintage)."""
    rows = (
        ll.with_vintage(cv)
        .filter(pl.col("chart_title").str.contains(r"(?i)approved to energize by load zone")
                & pl.col("category").is_in(["LZ_WEST", "Other"]))
    )
    first_doc = rows.group_by("vintage").agg(pl.col("document").min().alias("document"))
    rows = rows.join(first_doc, on=["vintage", "document"], how="semi")
    obs = pl.col("status_bucket") == "observed_energized"
    rem = pl.col("status_bucket") == "approved_to_energize_not_operational"
    return (
        rows.group_by("vintage", "category")
        .agg(pl.col("value_mw").filter(obs).sum().alias("observed_nonsim_mw"),
             pl.col("value_mw").filter(rem).sum().alias("not_operational_mw"))
        .with_columns((pl.col("observed_nonsim_mw") + pl.col("not_operational_mw")).alias("approved_mw"))
        .rename({"category": "load_zone"})
        .sort("vintage", "load_zone")
    )


def value_asof(monthly: pl.DataFrame, month: str, column: str) -> tuple[float | None, str | None]:
    """Latest non-null ``column`` at or before ``month`` ("YYYY-MM") and the month it comes from."""
    hit = monthly.filter((pl.col("month") <= month) & pl.col(column).is_not_null()).sort("month")
    if hit.is_empty():
        return None, None
    return hit[column][-1], hit["month"][-1]


def zone_share_daily(daily: pl.DataFrame, total: str = wl.TOTAL) -> pl.DataFrame:
    """Each zone's share of the ``total`` daily mean load (a reassignment of load between zones moves shares
    in opposite directions on the same day, while weather moves every zone together)."""
    tot = daily.filter(pl.col("weather_zone") == total).select("date", pl.col("dmean").alias("_tot"))
    return (
        daily.filter(pl.col("weather_zone") != total)
        .join(tot, on="date")
        .with_columns((pl.col("dmean") / pl.col("_tot")).alias("share"))
        .drop("_tot")
        .sort("weather_zone", "date")
    )
