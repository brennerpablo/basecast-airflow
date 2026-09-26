"""Load vs. weather: the weather-normalized ("organic") summer peak by ERCOT weather zone.

Phase 0 question Q7 (``docs/PHASE0_ANALYSIS.md``): do weather and a linear trend explain each zone's summer peak?
The model is ``peak_mw ~ year + weather`` per zone, one row per summer (June–September).

Time conventions (``docs/decisions.md``):

- ``ercot_load_hourly_wz.ts_utc`` is the **end** of the hour-ending interval; ``operating_date`` is the ERCOT
  operating day (America/Chicago), so a load day is simply its ``operating_date`` (HE 24 ends at the next
  local midnight but belongs to the day it closes).
- ``weather_hourly_wz.ts_utc`` is Open-Meteo ERA5 in GMT, one instantaneous value per hour (instantaneous
  reading of ``temperature_2m``: Open-Meteo's documentation, not verified here); a weather day is the
  America/Chicago date of the instant, so DST days hold 23 or 25 instants.

The loaders are thin (one read-only query each); everything else is a pure function on DataFrames.
Numpy is imported lazily by the fitting helpers so the pure aggregations run without it.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

import polars as pl

LOCAL_TZ = "America/Chicago"
TOTAL = "ERCOT"
WEATHER_ZONES = ("COAST", "EAST", "FWEST", "NCENT", "NORTH", "SCENT", "SOUTH", "WEST")
SUMMER_MONTHS = (6, 7, 8, 9)
SUMMER_HOURS = 122 * 24  # June–September never crosses a DST change
FEATURES = ("t_mean_3d", "t_max_3d", "td_mean_3d", "hi_max_3d")


# --- loaders (read-only) -------------------------------------------------------------------------------------


def load_hourly_load() -> pl.DataFrame:
    """``ercot_load_hourly_wz``: the eight weather zones plus the ``ERCOT`` total, hourly MW."""
    from basecast_pipelines.models.db import read_sql

    return read_sql(
        "SELECT ts_utc, operating_date, hour_ending, dst_flag, weather_zone, mw FROM ercot_load_hourly_wz"
    )


def load_hourly_weather() -> pl.DataFrame:
    """``weather_hourly_wz``: ERA5 temperature and dew point per weather zone, hourly, UTC."""
    from basecast_pipelines.models.db import read_sql

    return read_sql("SELECT weather_zone, ts_utc, temperature_c, dew_point_c, n_points FROM weather_hourly_wz")


def load_official_hourly_peaks() -> pl.DataFrame:
    """ERCOT's published monthly system peak (hourly integrated MW) from ``ercot_monthly_peaks``."""
    from basecast_pipelines.models.db import read_sql

    return read_sql(
        "SELECT month, value AS peak_mw, peak_ts_utc FROM ercot_monthly_peaks "
        "WHERE region_type = 'ercot' AND region_id = 'ERCOT' AND metric = 'peak_hourly_mw'"
    )


# --- time alignment ------------------------------------------------------------------------------------------


def load_alignment_checks(load: pl.DataFrame) -> dict[str, Any]:
    """Checks on the hourly load: ``ts_utc`` is the interval end, days have 23/24/25 hours, zones sum to the total.

    Returns counts, not assertions, so the analysis can report them."""
    total = load.filter(pl.col("weather_zone") == TOTAL)
    start_local_date = (pl.col("ts_utc") - pl.duration(hours=1)).dt.convert_time_zone(LOCAL_TZ).dt.date()
    date_mismatch = load.filter(start_local_date != pl.col("operating_date")).height
    hours = total.group_by("operating_date").agg(pl.len().alias("n")).group_by("n").agg(pl.len().alias("days"))
    steps = total.sort("ts_utc").select(pl.col("ts_utc").diff().alias("step"), "ts_utc", "operating_date")
    gaps = steps.filter(pl.col("step").is_not_null() & (pl.col("step") != pl.duration(hours=1)))
    wide = load.pivot(on="weather_zone", index=["ts_utc", "operating_date"], values="mw")
    zones = [z for z in WEATHER_ZONES if z in wide.columns]
    diff = wide.select(
        "operating_date", (pl.sum_horizontal(zones) - pl.col(TOTAL)).abs().alias("abs_diff"), pl.col(TOTAL)
    )
    summer = diff.filter(pl.col("operating_date").dt.month().is_in(SUMMER_MONTHS))
    return {
        "rows_start_date_not_operating_date": date_mismatch,
        "hours_per_day": dict(zip(hours["n"].to_list(), hours["days"].to_list(), strict=True)),
        "gaps": gaps.select("ts_utc", "operating_date", "step"),
        "zone_sum_minus_total_max_mw": diff["abs_diff"].max(),
        "zone_sum_minus_total_mean_mw": diff["abs_diff"].mean(),
        "hours_off_by_over_1mw": diff.filter(pl.col("abs_diff") > 1).height,
        "summer_zone_sum_minus_total_max_mw": summer["abs_diff"].max(),
        "summer_hours_off_by_over_1mw": summer.filter(pl.col("abs_diff") > 1).height,
    }


def diurnal_peak_hours(load: pl.DataFrame, weather: pl.DataFrame, month: int = 7) -> pl.DataFrame:
    """Per zone, the local clock hour of the mean diurnal temperature maximum and the hour ending of the mean
    load maximum in ``month``: a timezone slip in either series would move one against the other."""
    temp = (
        weather.with_columns(pl.col("ts_utc").dt.convert_time_zone(LOCAL_TZ).alias("_local"))
        .filter(pl.col("_local").dt.month() == month)
        .group_by("weather_zone", pl.col("_local").dt.hour().alias("hour"))
        .agg(pl.col("temperature_c").mean().alias("t"))
        .group_by("weather_zone")
        .agg(pl.col("hour").sort_by("t").last().alias("hottest_local_hour"))
    )
    peak = (
        load.filter(pl.col("operating_date").dt.month() == month)
        .group_by("weather_zone", "hour_ending")
        .agg(pl.col("mw").mean().alias("mw"))
        .group_by("weather_zone")
        .agg(pl.col("hour_ending").sort_by("mw").last().alias("peak_load_hour_ending"))
    )
    return peak.join(temp, on="weather_zone", how="left").sort("weather_zone")


# --- daily and summer aggregation ----------------------------------------------------------------------------


def heat_index_c(t_c: pl.Expr, td_c: pl.Expr) -> pl.Expr:
    """NWS heat index (Rothfusz regression with its two adjustments, Steadman's simple form below ~80 °F), °C.

    Relative humidity comes from temperature and dew point with the Magnus formula."""

    def es(x: pl.Expr) -> pl.Expr:
        return 6.112 * (17.62 * x / (243.12 + x)).exp()

    rh = (100 * es(td_c) / es(t_c)).clip(0, 100)
    t = t_c * 9 / 5 + 32
    simple = 0.5 * (t + 61.0 + (t - 68.0) * 1.2 + rh * 0.094)
    full = (
        -42.379 + 2.04901523 * t + 10.14333127 * rh - 0.22475541 * t * rh - 0.00683783 * t * t
        - 0.05481717 * rh * rh + 0.00122874 * t * t * rh + 0.00085282 * t * rh * rh - 0.00000199 * t * t * rh * rh
    )
    dry = ((13 - rh) / 4) * ((17 - (t - 95).abs()) / 17).clip(0, None).sqrt()
    humid = ((rh - 85) / 10) * ((87 - t) / 5)
    full = (
        pl.when((rh < 13) & (t > 80) & (t < 112)).then(full - dry)
        .when((rh > 85) & (t > 80) & (t < 87)).then(full + humid)
        .otherwise(full)
    )
    hi_f = pl.when((simple + t) / 2 >= 80).then(full).otherwise(simple)
    return (hi_f - 32) * 5 / 9


def weather_daily(weather: pl.DataFrame) -> pl.DataFrame:
    """Hourly zone weather → one row per zone and America/Chicago day: mean and max temperature, mean dew point,
    max heat index, and the number of hourly values (23/24/25 on complete days)."""
    return (
        weather.with_columns(
            pl.col("ts_utc").dt.convert_time_zone(LOCAL_TZ).dt.date().alias("date"),
            heat_index_c(pl.col("temperature_c"), pl.col("dew_point_c")).alias("_hi"),
        )
        .group_by("weather_zone", "date")
        .agg(
            pl.col("temperature_c").mean().alias("t_mean"),
            pl.col("temperature_c").max().alias("t_max"),
            pl.col("dew_point_c").mean().alias("td_mean"),
            pl.col("_hi").max().alias("hi_max"),
            pl.col("temperature_c").count().alias("n_hours"),
        )
        .sort("weather_zone", "date")
    )


def zone_weights(load: pl.DataFrame, years: Iterable[int], months: Sequence[int] = SUMMER_MONTHS) -> dict[str, float]:
    """Each weather zone's share of summer energy over ``years`` (weights for a system-wide weather series)."""
    years = list(years)
    energy = (
        load.filter(
            pl.col("weather_zone").is_in(WEATHER_ZONES)
            & pl.col("operating_date").dt.year().is_in(years)
            & pl.col("operating_date").dt.month().is_in(months)
        )
        .group_by("weather_zone")
        .agg(pl.col("mw").sum())
    )
    total = energy["mw"].sum()
    return {row["weather_zone"]: row["mw"] / total for row in energy.iter_rows(named=True)}


def system_weather_daily(daily: pl.DataFrame, weights: dict[str, float], name: str = TOTAL) -> pl.DataFrame:
    """Weighted mean of the zones' daily weather (weights renormalized over the zones present that day)."""
    w = pl.DataFrame({"weather_zone": list(weights), "_w": list(weights.values())})
    cols = ["t_mean", "t_max", "td_mean", "hi_max"]
    return (
        daily.join(w, on="weather_zone", how="inner")
        .group_by("date")
        .agg(
            *[((pl.col(c) * pl.col("_w")).sum() / pl.col("_w").filter(pl.col(c).is_not_null()).sum()).alias(c)
              for c in cols],
            pl.col("n_hours").min(),
        )
        .with_columns(pl.lit(name).alias("weather_zone"))
        .select("weather_zone", "date", *cols, "n_hours")
        .sort("date")
    )


def summer_weather_features(
    daily: pl.DataFrame, window_days: int = 3, months: Sequence[int] = SUMMER_MONTHS
) -> pl.DataFrame:
    """Per zone and summer: the maximum over ``months`` of the trailing ``window_days``-day mean of each daily
    variable (``t_mean_3d`` = hottest 3-day stretch of daily mean temperature, and so on). The window runs on the
    whole daily series, so a June 1 value averages May 30–June 1; it needs every day of the window."""
    window = f"{window_days}d"
    rolled = daily.sort("weather_zone", "date").with_columns(
        pl.col(src).rolling_mean_by("date", window_size=window, min_samples=window_days).over("weather_zone")
        .alias(dst)
        for src, dst in zip(("t_mean", "t_max", "td_mean", "hi_max"), FEATURES, strict=True)
    )
    return (
        rolled.filter(pl.col("date").dt.month().is_in(months))
        .group_by("weather_zone", pl.col("date").dt.year().alias("year"))
        .agg(*[pl.col(f).max() for f in FEATURES], pl.len().alias("weather_days"))
        .sort("weather_zone", "year")
    )


def summer_peaks(load: pl.DataFrame, months: Sequence[int] = SUMMER_MONTHS) -> pl.DataFrame:
    """Per zone (and ``ERCOT``) and year: the highest hourly MW among the hours of operating days in ``months``.

    Zone peaks are each zone's own (non-coincident) peak; ``ERCOT`` is the system peak. ``n_hours`` says how
    complete the summer is (2,928 for a full June–September)."""
    return (
        load.filter(pl.col("operating_date").dt.month().is_in(months))
        .group_by("weather_zone", pl.col("operating_date").dt.year().alias("year"))
        .agg(
            pl.col("mw").max().alias("peak_mw"),
            pl.col("ts_utc").sort_by("mw").last().alias("peak_ts_utc"),
            pl.len().alias("n_hours"),
        )
        .with_columns(pl.col("peak_ts_utc").dt.convert_time_zone(LOCAL_TZ).alias("peak_local"))
        .sort("weather_zone", "year")
    )


# --- fitting -------------------------------------------------------------------------------------------------


@dataclass
class PeakFit:
    """OLS fit of ``peak ~ 1 + year [+ max(0, year - knot)] + feature`` (on ``log(peak)`` when ``log``)."""

    zone: str
    feature: str
    names: list[str]
    coef: list[float]
    se: list[float]
    r2: float
    resid_sd_mw: float
    n: int
    years: list[int]
    log: bool = False
    knot: int | None = None
    residuals_mw: list[float] = field(default_factory=list)

    def design(self, year: Sequence[float], x: Sequence[float]):
        return _design(year, x, self.knot)

    def predict(self, year: Sequence[float], x: Sequence[float]):
        import numpy as np

        yhat = self.design(year, x) @ np.asarray(self.coef)
        return np.exp(yhat) if self.log else yhat

    def coef_of(self, name: str) -> float:
        return self.coef[self.names.index(name)]


def _design(year: Sequence[float], x: Sequence[float], knot: int | None = None):
    import numpy as np

    year = np.asarray(year, dtype=float)
    cols = [np.ones_like(year), year - 2014.0]
    if knot is not None:
        cols.append(np.clip(year - knot, 0, None))
    cols.append(np.asarray(x, dtype=float))
    return np.column_stack(cols)


def fit_peak(
    frame: pl.DataFrame, feature: str, *, zone: str = "", log: bool = False, knot: int | None = None
) -> PeakFit:
    """OLS of ``peak_mw`` on the year (centered on 2014) and one weather feature; ``frame`` has ``year``,
    ``peak_mw`` and ``feature``. ``r2`` is on the fitted scale; ``resid_sd_mw`` is always in MW (df-corrected)."""
    import numpy as np

    frame = frame.drop_nulls(["peak_mw", feature]).sort("year")
    year = frame["year"].to_numpy().astype(float)
    y = frame["peak_mw"].to_numpy().astype(float)
    x = frame[feature].to_numpy().astype(float)
    X = _design(year, x, knot)
    target = np.log(y) if log else y
    beta, *_ = np.linalg.lstsq(X, target, rcond=None)
    fitted = X @ beta
    resid = target - fitted
    n, k = X.shape
    dof = max(n - k, 1)
    sigma2 = float(resid @ resid) / dof
    cov = sigma2 * np.linalg.pinv(X.T @ X)
    tss = float(((target - target.mean()) ** 2).sum())
    r2 = 1 - float(resid @ resid) / tss if tss > 0 else float("nan")
    resid_mw = y - (np.exp(fitted) if log else fitted)
    names = ["intercept", "year"] + (["year_after_knot"] if knot is not None else []) + [feature]
    return PeakFit(
        zone=zone, feature=feature, names=names, coef=beta.tolist(), se=np.sqrt(np.diag(cov)).tolist(), r2=r2,
        resid_sd_mw=math.sqrt(float(resid_mw @ resid_mw) / dof), n=n, years=[int(v) for v in year], log=log,
        knot=knot, residuals_mw=resid_mw.tolist(),
    )


def best_knot(frame: pl.DataFrame, feature: str, *, min_segment: int = 5) -> int:
    """The knot year (start of the second trend segment) with the smallest SSE, searched on ``frame`` only."""
    years = sorted(frame["year"].to_list())
    candidates = years[min_segment:-min_segment + 1] if len(years) > 2 * min_segment else years[1:-1]
    fits = [(fit_peak(frame, feature, knot=k).resid_sd_mw, k) for k in candidates]
    return min(fits)[1]


def holdout(
    panel: pl.DataFrame,
    feature: str,
    *,
    train_until: int = 2022,
    test_years: Sequence[int] = (2023, 2024, 2025),
    log: bool = False,
    piecewise: bool = False,
) -> pl.DataFrame:
    """Fit each zone on years ≤ ``train_until`` and predict ``test_years`` with their actual weather.

    ``panel`` has ``weather_zone``, ``year``, ``peak_mw`` and ``feature``. With ``piecewise`` the knot is chosen
    on the training years only. Returns one row per zone and test year with the absolute percentage error."""
    rows = []
    for zone in panel["weather_zone"].unique(maintain_order=True).to_list():
        sub = panel.filter(pl.col("weather_zone") == zone)
        train = sub.filter(pl.col("year") <= train_until)
        test = sub.filter(pl.col("year").is_in(list(test_years))).sort("year")
        knot = best_knot(train, feature) if piecewise else None
        fit = fit_peak(train, feature, zone=zone, log=log, knot=knot)
        pred = fit.predict(test["year"].to_list(), test[feature].to_list())
        for year, actual, p in zip(test["year"].to_list(), test["peak_mw"].to_list(), pred.tolist(), strict=True):
            rows.append({"weather_zone": zone, "year": year, "actual_mw": actual, "predicted_mw": p,
                         "error_pct": 100 * (p - actual) / actual, "ape": 100 * abs(p - actual) / actual})
    return pl.DataFrame(rows)


def mape(errors: pl.DataFrame) -> pl.DataFrame:
    """Mean absolute percentage error (and mean signed error) per zone from :func:`holdout` rows."""
    return (
        errors.group_by("weather_zone", maintain_order=True)
        .agg(pl.col("ape").mean().alias("mape_pct"), pl.col("error_pct").mean().alias("bias_pct"))
    )


def rolling_origin(panel: pl.DataFrame, feature: str, first_test: int, last_test: int) -> pl.DataFrame:
    """One-year-ahead errors: for each year Y in [first_test, last_test], fit on years < Y and predict Y."""
    frames = [
        holdout(panel, feature, train_until=year - 1, test_years=(year,)) for year in range(first_test, last_test + 1)
    ]
    return pl.concat(frames)


# --- structural breaks ---------------------------------------------------------------------------------------


def _sse(X, y) -> float:
    import numpy as np

    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    r = y - X @ beta
    return float(r @ r)


def chow_scan(year: Sequence[int], X, y, *, min_segment: int = 5) -> list[tuple[int, float]]:
    """Chow F statistic for a break in every coefficient at each candidate year (first year of the second
    regime), keeping at least ``min_segment`` years on each side."""
    import numpy as np

    year = np.asarray(year)
    X = np.asarray(X, dtype=float)
    y = np.asarray(y, dtype=float)
    n, k = X.shape
    sse_full = _sse(X, y)
    out = []
    for b in year[min_segment:n - min_segment + 1]:
        left = year < b
        sse_split = _sse(X[left], y[left]) + _sse(X[~left], y[~left])
        f = ((sse_full - sse_split) / k) / (sse_split / (n - 2 * k))
        out.append((int(b), float(f)))
    return out


def sup_chow(
    year: Sequence[int], X, y, *, min_segment: int = 5, n_boot: int = 2000, seed: int = 0
) -> dict[str, Any]:
    """The largest Chow F over candidate break years (Quandt's sup-F), with a parametric-bootstrap p-value that
    accounts for the search: simulate the no-break model with Gaussian errors and rescan each draw."""
    import numpy as np

    X = np.asarray(X, dtype=float)
    y = np.asarray(y, dtype=float)
    scan = chow_scan(year, X, y, min_segment=min_segment)
    best_year, best_f = max(scan, key=lambda t: t[1])
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ beta
    sigma = math.sqrt(float(resid @ resid) / (len(y) - X.shape[1]))
    rng = np.random.default_rng(seed)
    fitted = X @ beta
    exceed = 0
    for _ in range(n_boot):
        sim = fitted + rng.normal(0.0, sigma, size=len(y))
        if max(f for _, f in chow_scan(year, X, sim, min_segment=min_segment)) >= best_f:
            exceed += 1
    return {"break_year": best_year, "sup_f": best_f, "p_boot": (exceed + 1) / (n_boot + 1), "scan": scan}
