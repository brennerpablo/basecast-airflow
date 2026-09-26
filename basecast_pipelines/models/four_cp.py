"""ERCOT 4CP: the four summer coincident peaks, day-ahead dispatch rules that try to catch them, and each zone's
load at those intervals. Exploration X3 (``docs/analysis/x3_four_cp.md``).

4CP, as used here: a distribution provider's wholesale transmission charge is allocated by its average load in
the 15-minute interval of ERCOT's system peak in each of June, July, August and September (PUCT Substantive Rule
25.192; the rule text and the one-year lag between the 4CP summer and the billing year are not verified in this
repo). A battery fleet that discharges in those four intervals lowers the co-op's 4CP load.

Time conventions:

- ``ercot_monthly_peaks.peak_local`` is the published local wall-clock **end** of the peak interval (15-minute
  interval ending, or hour ending) and ``peak_ts_utc`` the same instant in UTC (``parsers/ercot/demand_energy.py``).
  Coincident zone rows carry the ERCOT 15-minute peak's time.
- ``ercot_load_hourly_wz.ts_utc`` is the end of the hour-ending interval and ``operating_date`` the ERCOT day.
- ``end_min`` = minutes after local midnight at the interval end (15 … 1440); an interval ending 17:00 covers
  16:45–17:00 and lies in hour ending 17. A discharge window (``start_min``, ``length_min``) covers the local
  wall clock (start, start + length]; hours ending 17–18 are (960, 120), i.e. 16:00–18:00.
- The fuel mix and price series are indexed by UTC interval start; hours are aggregated by their UTC hour end,
  which lines up with ``ercot_load_hourly_wz.ts_utc`` on every day, DST days included.

Loaders are thin (one read-only query each); everything else is a pure function on DataFrames. Numpy is imported
lazily by the helpers that need it.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

import polars as pl

LOCAL_TZ = "America/Chicago"
TOTAL = "ERCOT"
CP_MONTHS = (6, 7, 8, 9)


# --- loaders (read-only) -------------------------------------------------------------------------------------


def load_monthly_peaks() -> pl.DataFrame:
    """``ercot_monthly_peaks`` rows of June–September: system peaks, zone coincident and non-coincident peaks,
    zone energy."""
    from basecast_pipelines.models.db import read_sql

    return read_sql(
        "SELECT month, region_type, region_id, metric, value, interval_minutes, peak_local, peak_ts_utc, "
        "final_settlement FROM ercot_monthly_peaks WHERE EXTRACT(MONTH FROM month) BETWEEN 6 AND 9 "
        "AND value IS NOT NULL"
    )


# --- 4CP intervals -------------------------------------------------------------------------------------------


def _end_min(local: pl.Expr) -> pl.Expr:
    """Minutes after local midnight at an interval end; midnight is 1440 of the previous day."""
    m = local.dt.hour().cast(pl.Int32) * 60 + local.dt.minute().cast(pl.Int32)
    return pl.when(m == 0).then(1440).otherwise(m)


def cp_intervals(monthly: pl.DataFrame, months: Sequence[int] = CP_MONTHS) -> pl.DataFrame:
    """ERCOT's 15-minute system peak per month in ``months`` (``peak_15min_mw``): one row per year and month with
    the MW, the local interval end, its minute of the day, the day it falls in, weekday (1 = Monday) and the
    hour ending that contains it."""
    df = monthly.filter(
        (pl.col("region_type") == "ercot")
        & (pl.col("metric") == "peak_15min_mw")
        & pl.col("month").dt.month().is_in(months)
    )
    minutes = pl.col("interval_minutes").fill_null(15).cast(pl.Int32)
    return (
        df.select(
            pl.col("month").dt.year().cast(pl.Int32).alias("year"),
            pl.col("month").dt.month().cast(pl.Int32).alias("month"),
            pl.col("value").alias("cp_mw"),
            pl.col("peak_local").alias("end_local"),
            pl.col("peak_ts_utc").alias("end_utc"),
            minutes.alias("interval_minutes"),
            _end_min(pl.col("peak_local")).alias("end_min"),
            (pl.col("peak_local") - pl.duration(minutes=minutes)).dt.date().alias("cp_date"),
            "final_settlement",
        )
        .with_columns(
            pl.col("cp_date").dt.weekday().alias("weekday"),
            ((pl.col("end_min") + 59) // 60).alias("hour_ending"),
            pl.lit("15min").alias("source"),
        )
        .sort("year", "month")
    )


def hourly_cps(load: pl.DataFrame, zone: str = TOTAL, months: Sequence[int] = CP_MONTHS) -> pl.DataFrame:
    """The highest hour of each month in ``months`` from the hourly table, in the same shape as
    :func:`cp_intervals` (``interval_minutes`` = 60). Used where the 15-minute value is not published yet."""
    return (
        load.filter((pl.col("weather_zone") == zone) & pl.col("operating_date").dt.month().is_in(months))
        .group_by(
            pl.col("operating_date").dt.year().cast(pl.Int32).alias("year"),
            pl.col("operating_date").dt.month().cast(pl.Int32).alias("month"),
        )
        .agg(
            pl.col("mw").max().alias("cp_mw"),
            pl.col("ts_utc").sort_by("mw").last().alias("end_utc"),
            pl.col("operating_date").sort_by("mw").last().alias("cp_date"),
            pl.col("hour_ending").sort_by("mw").last().cast(pl.Int32).alias("hour_ending"),
        )
        .with_columns(
            pl.col("end_utc").dt.convert_time_zone(LOCAL_TZ).dt.replace_time_zone(None).alias("end_local"),
            pl.lit(60, pl.Int32).alias("interval_minutes"),
            (pl.col("hour_ending") * 60).alias("end_min"),
            pl.lit(None, pl.Boolean).alias("final_settlement"),
            pl.col("cp_date").dt.weekday().alias("weekday"),
            pl.lit("hourly").alias("source"),
        )
        .select(
            "year", "month", "cp_mw", "end_local", "end_utc", "interval_minutes", "end_min", "cp_date",
            "final_settlement", "weekday", "hour_ending", "source",
        )
        .sort("year", "month")
    )


def cp_calendar(fifteen: pl.DataFrame, hourly: pl.DataFrame) -> pl.DataFrame:
    """15-minute CPs where published, hourly CPs for the (year, month) pairs the 15-minute table lacks."""
    missing = hourly.join(fifteen.select("year", "month"), on=["year", "month"], how="anti")
    return pl.concat([fifteen, missing.select(fifteen.columns)], how="vertical_relaxed").sort("year", "month")


def daily_peaks(load: pl.DataFrame, zone: str = TOTAL, months: Sequence[int] = (5, *CP_MONTHS)) -> pl.DataFrame:
    """One row per operating day: the day's highest hourly MW and its hour ending. May is included by default so
    the dispatch rules have a look-back for early June."""
    return (
        load.filter((pl.col("weather_zone") == zone) & pl.col("operating_date").dt.month().is_in(months))
        .group_by("operating_date")
        .agg(
            pl.col("mw").max().alias("peak_mw"),
            pl.col("hour_ending").sort_by("mw").last().cast(pl.Int32).alias("peak_he"),
            pl.len().alias("n_hours"),
        )
        .with_columns(
            pl.col("operating_date").dt.year().cast(pl.Int32).alias("year"),
            pl.col("operating_date").dt.month().cast(pl.Int32).alias("month"),
        )
        .sort("operating_date")
    )


# --- discharge windows ---------------------------------------------------------------------------------------


def in_window(start_min: int, length_min: int, end_min: str = "end_min", minutes: str = "interval_minutes") -> pl.Expr:
    """True when the whole interval lies inside the discharge window (local wall clock, minutes after midnight)
    (``start_min``, ``start_min + length_min``]. HE 17–18 is ``in_window(960, 120)``."""
    start = pl.col(end_min) - pl.col(minutes)
    return (start >= start_min) & (pl.col(end_min) <= start_min + length_min)


def window_label(start_min: int, length_min: int) -> str:
    """``16:30–17:30`` for ``(990, 60)``."""
    def hhmm(m: int) -> str:
        return f"{m // 60:02d}:{m % 60:02d}"

    return f"{hhmm(start_min)}–{hhmm(start_min + length_min)}"


def window_coverage(cps: pl.DataFrame, length_min: int, starts: Iterable[int] = range(12 * 60, 20 * 60, 15)) -> pl.DataFrame:
    """For each window start (minutes after local midnight), how many of the CP intervals in ``cps`` the window
    of ``length_min`` covers."""
    rows = []
    for start in starts:
        covered = cps.select(in_window(start, length_min).sum()).item()
        rows.append({"length_min": length_min, "start_min": start, "window": window_label(start, length_min),
                     "covered": covered, "n": cps.height, "share": covered / cps.height if cps.height else None})
    return pl.DataFrame(rows)


# --- day-ahead dispatch rules --------------------------------------------------------------------------------


def with_reference(daily: pl.DataFrame, lookback_days: int | None = None) -> pl.DataFrame:
    """Adds ``reference_mw``: the highest actual daily peak of the month through the previous day (known the
    evening before, after that day's peak). On the month's first day there is none, so the rule dispatches;
    with ``lookback_days`` the previous ``lookback_days`` calendar days (across the month boundary) stand in."""
    d = daily.sort("operating_date").with_columns(
        pl.col("peak_mw").cum_max().shift(1).over("year", "month").alias("reference_mw")
    )
    if lookback_days is None:
        return d
    trail = pl.col("peak_mw").rolling_max_by("operating_date", window_size=f"{lookback_days}d", closed="left")
    return d.with_columns(pl.coalesce("reference_mw", trail).alias("reference_mw"))


def threshold_dispatch(daily: pl.DataFrame, forecast: str, x: float, months: Sequence[int] = CP_MONTHS) -> pl.DataFrame:
    """Rule (a): dispatch on day d when the forecast of d's peak is ≥ ``x`` × ``reference_mw`` (see
    :func:`with_reference`, which must have run). Returns ``operating_date``, ``year``, ``month``, ``dispatch``
    for the days of ``months``."""
    return daily.filter(pl.col("month").is_in(months)).select(
        "operating_date", "year", "month",
        (pl.col(forecast) >= x * pl.col("reference_mw")).fill_null(True).alias("dispatch"),
    )


def top_n_dispatch(daily: pl.DataFrame, score: str, n: int, months: Sequence[int] = CP_MONTHS) -> pl.DataFrame:
    """Rule (b): dispatch on the ``n`` days of each month with the highest ``score`` (e.g. the forecast peak or the
    forecast temperature). It needs the whole month's forecast up front, so it is a hindsight benchmark, not a
    rule a desk could run day by day."""
    return (
        daily.filter(pl.col("month").is_in(months))
        .with_columns(pl.col(score).rank("ordinal", descending=True).over("year", "month").alias("_rank"))
        .select("operating_date", "year", "month", (pl.col("_rank") <= n).alias("dispatch"))
    )


def evaluate_dispatch(dispatch: pl.DataFrame, cps: pl.DataFrame, start_min: int, length_min: int) -> pl.DataFrame:
    """Per year: dispatch days, the CP months whose day was dispatched and whose interval the window covers, and
    whether all of that year's CP months were caught (``all_caught``; ``n_cp`` says how many months it had)."""
    days = dispatch.group_by("year").agg(pl.col("dispatch").sum().cast(pl.Int32).alias("dispatch_days"))
    hits = (
        cps.join(
            dispatch.select(pl.col("operating_date").alias("cp_date"), "dispatch"), on="cp_date", how="left"
        )
        .with_columns((pl.col("dispatch").fill_null(False) & in_window(start_min, length_min)).alias("caught"))
        .group_by("year")
        .agg(pl.len().alias("n_cp"), pl.col("caught").sum().cast(pl.Int32).alias("caught"),
             pl.col("dispatch").fill_null(False).sum().cast(pl.Int32).alias("day_caught"))
    )
    return (
        hits.join(days, on="year", how="left")
        .with_columns((pl.col("caught") == pl.col("n_cp")).alias("all_caught"))
        .sort("year")
    )


def summarize_dispatch(per_year: pl.DataFrame) -> dict[str, float]:
    """Mean dispatch days per summer, share of summers with every CP caught, share of CP months caught, and the
    same share counting only the day (window ignored)."""
    return {
        "years": per_year.height,
        "dispatch_days": per_year["dispatch_days"].mean(),
        "all4_rate": per_year["all_caught"].mean(),
        "month_rate": per_year["caught"].sum() / per_year["n_cp"].sum(),
        "day_rate": per_year["day_caught"].sum() / per_year["n_cp"].sum(),
    }


# --- forecasts of the daily peak -----------------------------------------------------------------------------


def noisy_forecast(daily: pl.DataFrame, sigma: float, seed: int, name: str = "forecast") -> pl.DataFrame:
    """The actual daily peak times ``exp(sigma · z)``, z standard normal (a stand-in for a day-ahead load forecast
    whose error has log-SD ``sigma``; ``sigma`` = 0 is perfect foresight)."""
    import numpy as np

    z = np.random.default_rng(seed).standard_normal(daily.height)
    return daily.with_columns((pl.col("peak_mw") * pl.Series(np.exp(sigma * z))).alias(name))


def _wx_design(frame: pl.DataFrame):
    import numpy as np

    t = frame["t_max"].to_numpy()
    return np.column_stack([
        t, t**2, frame["t_mean_lag1"].to_numpy(), frame["weekend"].cast(pl.Float64).to_numpy(),
    ])


def weather_forecast(
    daily: pl.DataFrame, weather: pl.DataFrame, years: Iterable[int], *, train_years: int = 3, level_days: int = 7,
    name: str = "forecast_wx",
) -> pl.DataFrame:
    """Day-ahead peak from weather, fit on earlier summers only.

    ``weather`` holds one row per local ``date`` with the system ``t_max`` and ``t_mean`` (ERA5 observed, so the
    forecast is optimistic: it assumes a perfect temperature forecast). For each target year Y the model
    ``log(peak) ~ year effects + t_max + t_max² + t_mean(d−1) + weekend`` is fit on the May–September days of the
    ``train_years`` summers before Y; the level of Y comes from the mean residual of the ``level_days`` previous
    days of Y (actual peaks, known by the evening before). Returns ``daily`` with ``name`` added (null where the
    inputs are missing)."""
    import numpy as np

    wx = weather.sort("date").with_columns(pl.col("t_mean").shift(1).alias("t_mean_lag1"))
    d = (
        daily.join(wx.select("date", "t_max", "t_mean_lag1"), left_on="operating_date", right_on="date", how="left")
        .with_columns(
            (pl.col("operating_date").dt.weekday() >= 6).alias("weekend"),
            pl.col("peak_mw").log().alias("_y"),
        )
        .sort("operating_date")
    )
    ok = pl.col("t_max").is_not_null() & pl.col("t_mean_lag1").is_not_null()
    out = []
    for year in sorted(set(years)):
        train = d.filter(ok & pl.col("year").is_between(year - train_years, year - 1))
        target = d.filter(pl.col("year") == year)
        if train.height == 0 or target.height == 0:
            continue
        fe_years = sorted(train["year"].unique().to_list())
        fe = np.column_stack([(train["year"].to_numpy() == y).astype(float) for y in fe_years])
        beta, *_ = np.linalg.lstsq(np.column_stack([fe, _wx_design(train)]), train["_y"].to_numpy(), rcond=None)
        slope = beta[len(fe_years):]
        tgt = target.with_columns(pl.col("t_max").fill_null(float("nan")), pl.col("t_mean_lag1").fill_null(float("nan")))
        base = _wx_design(tgt) @ slope
        resid = pl.Series(tgt["_y"].to_numpy() - base)
        level = resid.rolling_mean(window_size=level_days, min_samples=1).shift(1)
        pred = np.exp(base + level.to_numpy())
        out.append(target.select("operating_date").with_columns(pl.Series(name, pred).fill_nan(None)))
    fc = pl.concat(out) if out else pl.DataFrame({"operating_date": [], name: []})
    return daily.join(fc, on="operating_date", how="left")


# --- zones at the 4CP intervals ------------------------------------------------------------------------------


def zone_coincidence(monthly: pl.DataFrame, region_type: str = "weather_zone",
                     months: Sequence[int] = CP_MONTHS) -> pl.DataFrame:
    """Per zone and year, from the Demand and Energy report:

    - ``cp_avg_mw``: mean zone load at the ERCOT 15-minute peak of each month (the zone's 4CP load);
    - ``ncp_summer_mw``: the zone's own highest 15-minute load over the months;
    - ``cf_summer`` = ``cp_avg_mw`` / ``ncp_summer_mw`` (coincidence factor vs its own summer peak);
    - ``cf_month`` = mean over months of CP / that month's own peak;
    - ``share_4cp`` (of the sum of zones' 4CP load) and ``share_energy`` (of the months' energy);
    - ``ncp_end_hour``: mean local clock hour at which the zone's monthly peaks end (decimal hours);
    - ``n_months``: how many months the year has."""
    z = monthly.filter((pl.col("region_type") == region_type) & pl.col("month").dt.month().is_in(months))
    wide = (
        z.select(
            "region_id", pl.col("month").dt.year().cast(pl.Int32).alias("year"), pl.col("month").alias("m"),
            "metric", "value",
        )
        .pivot(on="metric", index=["region_id", "year", "m"], values="value")
    )
    ncp_time = z.filter(pl.col("metric") == "noncoincident_peak_15min_mw").select(
        "region_id", pl.col("month").alias("m"), (_end_min(pl.col("peak_local")) / 60).alias("_ncp_hour"),
    )
    wide = wide.join(ncp_time, on=["region_id", "m"], how="left")
    per = (
        wide.group_by("region_id", "year")
        .agg(
            pl.col("coincident_peak_15min_mw").mean().alias("cp_avg_mw"),
            pl.col("noncoincident_peak_15min_mw").max().alias("ncp_summer_mw"),
            (pl.col("coincident_peak_15min_mw") / pl.col("noncoincident_peak_15min_mw")).mean().alias("cf_month"),
            pl.col("energy_mwh").sum().alias("energy_mwh"),
            pl.col("_ncp_hour").mean().alias("ncp_end_hour"),
            pl.len().alias("n_months"),
        )
        .with_columns(
            (pl.col("cp_avg_mw") / pl.col("ncp_summer_mw")).alias("cf_summer"),
            (pl.col("cp_avg_mw") / pl.col("cp_avg_mw").sum().over("year")).alias("share_4cp"),
            (pl.col("energy_mwh") / pl.col("energy_mwh").sum().over("year")).alias("share_energy"),
        )
    )
    return per.sort("region_id", "year")


# --- net load and prices -------------------------------------------------------------------------------------


def hourly_from_15min(frame: pl.DataFrame, value: str, start: str = "interval_start_utc") -> pl.DataFrame:
    """Sum four 15-minute values into the hour they belong to, keyed by the hour's UTC **end** (``ts_utc``), the
    same key as ``ercot_load_hourly_wz``. MWh per 15 minutes summed over an hour is the hour's average MW."""
    return (
        frame.group_by((pl.col(start).dt.truncate("1h") + pl.duration(hours=1)).alias("ts_utc"))
        .agg(pl.col(value).sum(), pl.len().alias("n_intervals"))
        .sort("ts_utc")
    )


def net_load_hourly(load: pl.DataFrame, fuel: pl.DataFrame, zone: str = TOTAL) -> pl.DataFrame:
    """ERCOT hourly load minus hourly wind and solar generation (fuel mix, MWh per 15 minutes): ``ts_utc``,
    ``operating_date``, ``hour_ending``, ``load_mw``, ``wind_mw``, ``solar_mw``, ``net_mw``. Hours without four
    intervals of both fuels are dropped."""
    parts = []
    for f in ("wind", "solar"):
        h = hourly_from_15min(fuel.filter(pl.col("fuel") == f), "generation_mwh")
        parts.append(h.filter(pl.col("n_intervals") == 4).select("ts_utc", pl.col("generation_mwh").alias(f"{f}_mw")))
    ren = parts[0].join(parts[1], on="ts_utc", how="inner")
    return (
        load.filter(pl.col("weather_zone") == zone)
        .select("ts_utc", "operating_date", "hour_ending", pl.col("mw").alias("load_mw"))
        .join(ren, on="ts_utc", how="inner")
        .with_columns((pl.col("load_mw") - pl.col("wind_mw") - pl.col("solar_mw")).alias("net_mw"))
        .sort("ts_utc")
    )


def monthly_argmax(hourly: pl.DataFrame, value: str, months: Sequence[int] = CP_MONTHS) -> pl.DataFrame:
    """Per year and month in ``months``: the max of ``value`` and the operating date and hour ending it falls in."""
    return (
        hourly.filter(pl.col("operating_date").dt.month().is_in(months))
        .group_by(
            pl.col("operating_date").dt.year().cast(pl.Int32).alias("year"),
            pl.col("operating_date").dt.month().cast(pl.Int32).alias("month"),
        )
        .agg(
            pl.col(value).max().alias(f"{value}_max"),
            pl.col("operating_date").sort_by(value).last().alias(f"{value}_date"),
            pl.col("hour_ending").sort_by(value).last().cast(pl.Int32).alias(f"{value}_he"),
        )
        .sort("year", "month")
    )


def rank_within_month(hourly: pl.DataFrame, value: str, months: Sequence[int] = CP_MONTHS) -> pl.DataFrame:
    """Adds ``<value>_rank``: 1 for the month's highest hour of ``value``."""
    return hourly.filter(pl.col("operating_date").dt.month().is_in(months)).with_columns(
        pl.col(value).rank("ordinal", descending=True)
        .over(pl.col("operating_date").dt.year(), pl.col("operating_date").dt.month())
        .alias(f"{value}_rank")
    )
