"""Exploration X12: a weather-normalized daily load series by ERCOT weather zone.

For each zone (and the ``ERCOT`` total) a daily model separates the weather response from growth:

    y(d) = level(d) + season(d) + day type(d) + f(weather(d)) + e(d)

- ``y`` is the daily mean MW (``dmean``, daily energy / hours, so DST days of 23 or 25 hours compare) or the daily
  peak MW (``dmax``), from ``peak_excess.daily_load_stats`` (operating days, America/Chicago).
- ``level`` is continuous and piecewise linear in time with a knot every January 1 (``level_knots_per_year=2``
  adds July 1): it follows growth and structural steps slowly, so the weather slope is identified from
  day-to-day and within-season variation, not from the trend.
- ``season`` is ``fourier`` harmonics of the day of the year (daylight, school and farm calendars).
- ``day type`` is day of week plus NERC holidays, bridge days and the year-end week (:func:`day_types`).
- ``f(weather)`` is a linear spline on the daily mean temperature (knots ``t_knots``, heating on the left,
  cooling on the right), with optional hinges on yesterday's temperature (building inertia), dew point above
  ``humid_base`` and daily maximum temperature above ``tmax_base``. Every weather column is named ``w:*``.

Weather normalization keeps each day's residual and swaps only the weather term:
``normalized(d) = y(d) - f(weather(d)) + mean_w f(weather_w(d))``, where ``weather_w(d)`` is the same calendar day
in each normal year ``w`` (default 2003–2022, the first 20 years of the ERA5 series in the DB). The summer peak
under normal weather is the distribution over ``w`` of the season's maximum with the weather swapped.

Models are fit in rolling three-year windows (:func:`window_for`): each year is normalized with the weather
response estimated around it, which is also how the stability of the cooling slope is read.

Pure functions on DataFrames; numpy is imported lazily so the calendar and aggregation helpers run without it.
The only I/O is ``weather_load``'s loaders.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, timedelta
from typing import Any

import polars as pl

from basecast_pipelines.models import peak_excess as pe
from basecast_pipelines.models import weather_load as wl

NORMAL_YEARS = (2003, 2022)
T_KNOTS = (5.0, 10.0, 15.0, 20.0, 24.0, 27.0, 30.0)
LAG_CDD_BASE = 22.0
LAG_HDD_BASE = 15.0
HUMID_BASE = 16.0
TMAX_BASE = 35.0
# Winter Storm Uri: ERCOT shed firm load in mid-February 2021, so load was not demand on these days. The window
# is kept out of every fit (dates from memory, not verified against an ERCOT source).
EXCLUDED_DAYS = (date(2021, 2, 14), date(2021, 2, 20))
DAY_TYPES = ("holiday", "bridge", "yearend")
DOW_NAMES = ("mon", "tue", "wed", "thu", "fri", "sat")  # Sunday is the baseline


@dataclass(frozen=True)
class Spec:
    """Which regressors enter the daily model."""

    t_knots: tuple[float, ...] = T_KNOTS
    lag: bool = True
    humid: bool = True
    tmax: bool = False
    weather: bool = True
    fourier: int = 2
    level_knots_per_year: int = 1
    trim_sigma: float | None = 4.0


# --- calendar ------------------------------------------------------------------------------------------------


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """The ``n``-th ``weekday`` (Mon=0) of the month; ``n = -1`` is the last one."""
    if n > 0:
        first = date(year, month, 1)
        return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (n - 1))
    nxt = date(year + (month == 12), month % 12 + 1, 1)
    last = nxt - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def nerc_holidays(years: Iterable[int]) -> dict[date, str]:
    """The six NERC off-peak holidays (New Year's Day, Memorial Day, Independence Day, Labor Day, Thanksgiving,
    Christmas); a fixed-date holiday on a Sunday is observed on Monday, one on a Saturday is not moved (NERC rule
    as commonly published; not verified against NERC here)."""
    out: dict[date, str] = {}
    for y in years:
        for name, d in (("new_year", date(y, 1, 1)), ("independence_day", date(y, 7, 4)),
                        ("christmas", date(y, 12, 25))):
            out[d + timedelta(days=1) if d.weekday() == 6 else d] = name
        out[_nth_weekday(y, 5, 0, -1)] = "memorial_day"
        out[_nth_weekday(y, 9, 0, 1)] = "labor_day"
        out[_nth_weekday(y, 11, 3, 4)] = "thanksgiving"
    return out


def day_types(dates: Sequence[date]) -> list[str]:
    """``holiday`` (NERC), ``bridge`` (Friday after Thanksgiving, Dec 24, Dec 26, Dec 31), ``yearend``
    (Dec 27–30) or ``normal``."""
    years = sorted({d.year for d in dates})
    hol = nerc_holidays(range(years[0], years[-1] + 1)) if years else {}
    bridges = {h + timedelta(days=1) for h, n in hol.items() if n == "thanksgiving"}
    out = []
    for d in dates:
        if d in hol:
            out.append("holiday")
        elif d in bridges or (d.month == 12 and d.day in (24, 26, 31)):
            out.append("bridge")
        elif d.month == 12 and 27 <= d.day <= 30:
            out.append("yearend")
        else:
            out.append("normal")
    return out


def window_for(year: int, first: int, last: int, width: int = 3) -> tuple[int, int]:
    """The ``width``-year fitting window centered on ``year``, slid inward at the ends of [first, last]."""
    half = width // 2
    start = min(max(year - half, first), max(last - width + 1, first))
    return start, min(start + width - 1, last)


# --- daily panel ---------------------------------------------------------------------------------------------


def weather_daily_features(weather_daily: pl.DataFrame) -> pl.DataFrame:
    """Daily weather (``weather_load.weather_daily`` or ``system_weather_daily`` rows) plus ``t_lag``, the
    previous calendar day's mean temperature of the same zone (null when that day is missing)."""
    prev = weather_daily.select(
        "weather_zone", (pl.col("date") + pl.duration(days=1)).alias("date"), pl.col("t_mean").alias("t_lag")
    )
    return (
        weather_daily.select("weather_zone", "date", "t_mean", "t_max", "td_mean", "n_hours")
        .join(prev, on=["weather_zone", "date"], how="left")
        .sort("weather_zone", "date")
    )


def daily_panel(load: pl.DataFrame, weather_features: pl.DataFrame, min_weather_hours: int = 23) -> pl.DataFrame:
    """Hourly load + daily weather features → one row per zone and operating day with ``dmean`` (daily mean MW),
    ``dmax``, ``dmin``, ``energy_mwh``, the weather, ``day_type`` and ``excluded`` (the Uri window). Days with
    incomplete weather (fewer than ``min_weather_hours`` values) or no lag are dropped."""
    daily = pe.daily_load_stats(load).with_columns((pl.col("dmean") * pl.col("n_hours")).alias("energy_mwh"))
    panel = (
        daily.join(weather_features.rename({"n_hours": "weather_hours"}), on=["weather_zone", "date"], how="inner")
        .filter((pl.col("weather_hours") >= min_weather_hours) & pl.col("t_lag").is_not_null())
        .sort("weather_zone", "date")
    )
    dates = panel["date"].unique().sort().to_list()
    types = pl.DataFrame({"date": dates, "day_type": day_types(dates)}, schema={"date": pl.Date, "day_type": pl.Utf8})
    return panel.join(types, on="date", how="left").with_columns(
        pl.col("date").is_between(*EXCLUDED_DAYS).alias("excluded")
    )


# --- design --------------------------------------------------------------------------------------------------


def _years_since(dates: Sequence[date], t0: date):
    import numpy as np

    return np.array([(d - t0).days / 365.25 for d in dates], dtype=float)


def weather_block(frame: pl.DataFrame, spec: Spec):
    """The weather columns (all named ``w:*``) for rows with ``t_mean``, ``t_lag``, ``td_mean``, ``t_max``."""
    import numpy as np

    if not spec.weather:
        return np.zeros((frame.height, 0)), []
    t = frame["t_mean"].to_numpy().astype(float)
    cols, names = [t], ["w:t"]
    for k in spec.t_knots:
        cols.append(np.clip(t - k, 0, None))
        names.append(f"w:t>{k:g}")
    if spec.lag:
        lag = frame["t_lag"].to_numpy().astype(float)
        cols += [np.clip(lag - LAG_CDD_BASE, 0, None), np.clip(LAG_HDD_BASE - lag, 0, None)]
        names += [f"w:lag>{LAG_CDD_BASE:g}", f"w:lag<{LAG_HDD_BASE:g}"]
    if spec.humid:
        cols.append(np.clip(frame["td_mean"].to_numpy().astype(float) - HUMID_BASE, 0, None))
        names.append(f"w:td>{HUMID_BASE:g}")
    if spec.tmax:
        cols.append(np.clip(frame["t_max"].to_numpy().astype(float) - TMAX_BASE, 0, None))
        names.append(f"w:tmax>{TMAX_BASE:g}")
    return np.column_stack(cols), names


def calendar_block(frame: pl.DataFrame, spec: Spec, t0: date, level_knots: Sequence[date], t_end: date | None = None):
    """Intercept, piecewise-linear level (flat after ``t_end`` when given), Fourier season, day of week and day
    types."""
    import numpy as np

    dates = frame["date"].to_list()
    n = len(dates)
    x = _years_since(dates, t0)
    if t_end is not None:
        x = np.minimum(x, _years_since([t_end], t0)[0])
    cols, names = [np.ones(n), x], ["intercept", "level"]
    for k in level_knots:
        cols.append(np.clip(x - _years_since([k], t0)[0], 0, None))
        names.append(f"level>{k.isoformat()}")
    doy = np.array([d.timetuple().tm_yday for d in dates], dtype=float) / 365.25
    for h in range(1, spec.fourier + 1):
        cols += [np.sin(2 * math.pi * h * doy), np.cos(2 * math.pi * h * doy)]
        names += [f"sin{h}", f"cos{h}"]
    dow = np.array([d.weekday() for d in dates])
    for i, name in enumerate(DOW_NAMES):
        cols.append((dow == i).astype(float))
        names.append(name)
    types = frame["day_type"].to_numpy() if "day_type" in frame.columns else np.array(day_types(dates))
    for name in DAY_TYPES:
        cols.append((types == name).astype(float))
        names.append(name)
    return np.column_stack(cols), names


def level_knots_for(start: int, end: int, per_year: int = 1) -> list[date]:
    knots = [date(y, 1, 1) for y in range(start + 1, end + 1)]
    if per_year == 2:
        knots += [date(y, 7, 1) for y in range(start, end + 1)]
    return sorted(knots)


# --- fitting -------------------------------------------------------------------------------------------------


def hac_cov(X, resid, lags: int = 7):
    """Newey–West (Bartlett) covariance of OLS coefficients; rows must be in time order."""
    import numpy as np

    xtx_inv = np.linalg.pinv(X.T @ X)
    u = X * resid[:, None]
    s = u.T @ u
    for lag in range(1, min(lags, len(resid) - 1) + 1):
        g = u[lag:].T @ u[:-lag]
        s += (1 - lag / (lags + 1)) * (g + g.T)
    return xtx_inv @ s @ xtx_inv


@dataclass
class DailyFit:
    """One zone's daily model on one window. ``cov`` is Newey–West (7 lags)."""

    zone: str
    target: str
    start: int
    end: int
    spec: Spec
    t0: date
    t_end: date
    level_knots: list[date]
    names: list[str]
    coef: Any
    cov: Any
    sigma: float
    r2: float
    n: int
    n_trimmed: int
    trimmed_dates: list[date] = field(default_factory=list)

    @property
    def weather_idx(self) -> list[int]:
        return [i for i, n in enumerate(self.names) if n.startswith("w:")]

    def design(self, frame: pl.DataFrame, flat_after_end: bool = True):
        import numpy as np

        cal, _ = calendar_block(frame, self.spec, self.t0, self.level_knots, self.t_end if flat_after_end else None)
        w, _ = weather_block(frame, self.spec)
        return np.column_stack([cal, w])

    def predict(self, frame: pl.DataFrame, flat_after_end: bool = True):
        return self.design(frame, flat_after_end) @ self.coef

    def weather_effect(self, frame: pl.DataFrame):
        """The weather term alone (``w:*`` columns times their coefficients), MW."""
        import numpy as np

        w, _ = weather_block(frame, self.spec)
        if not w.shape[1]:
            return np.zeros(frame.height)
        return w @ self.coef[self.weather_idx]

    def slope(self, t: float, *, lag: bool = True) -> tuple[float, float]:
        """MW per °C at daily mean temperature ``t`` when today's (and, with ``lag``, yesterday's) temperature rise
        together, dew point held; returns (slope, Newey–West SE)."""
        import numpy as np

        g = np.zeros(len(self.names))
        for i, name in enumerate(self.names):
            if name == "w:t":
                g[i] = 1.0
            elif name.startswith("w:t>") and t > float(name[4:]):
                g[i] = 1.0
            elif lag and name.startswith("w:lag>") and t > float(name[6:]):
                g[i] = 1.0
            elif lag and name.startswith("w:lag<") and t < float(name[6:]):
                g[i] = -1.0
        return float(g @ self.coef), float(math.sqrt(max(g @ self.cov @ g, 0.0)))


def fit_daily(panel: pl.DataFrame, target: str, start: int, end: int, spec: Spec = Spec(), zone: str = "") -> DailyFit:
    """OLS of ``target`` on the design over the days of years [start, end] (excluded days left out). With
    ``spec.trim_sigma``, days whose residual is beyond that many robust SDs (1.4826 × MAD) are dropped and the model
    refit once (hurricane outages, data glitches)."""
    import numpy as np

    frame = panel.filter(pl.col("date").dt.year().is_between(start, end) & ~pl.col("excluded")).sort("date")
    if zone:
        frame = frame.filter(pl.col("weather_zone") == zone)
    t0 = date(start, 1, 1)
    knots = level_knots_for(start, end, spec.level_knots_per_year)
    t_end = frame["date"].max()
    cal, cnames = calendar_block(frame, spec, t0, knots)
    w, wnames = weather_block(frame, spec)
    X = np.column_stack([cal, w])
    y = frame[target].to_numpy().astype(float)
    keep = np.ones(len(y), dtype=bool)
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    trimmed: list[date] = []
    if spec.trim_sigma:
        r = y - X @ beta
        robust = 1.4826 * float(np.median(np.abs(r - np.median(r))))
        keep = np.abs(r) <= spec.trim_sigma * robust
        if not keep.all():
            trimmed = [d for d, k in zip(frame["date"].to_list(), keep, strict=True) if not k]
            beta, *_ = np.linalg.lstsq(X[keep], y[keep], rcond=None)
    Xk, yk = X[keep], y[keep]
    resid = yk - Xk @ beta
    n, k = Xk.shape
    sigma = math.sqrt(float(resid @ resid) / max(n - k, 1))
    tss = float(((yk - yk.mean()) ** 2).sum())
    return DailyFit(
        zone=zone, target=target, start=start, end=end, spec=spec, t0=t0, t_end=t_end, level_knots=knots,
        names=cnames + wnames, coef=beta, cov=hac_cov(Xk, resid), sigma=sigma,
        r2=1 - float(resid @ resid) / tss if tss > 0 else float("nan"), n=n, n_trimmed=int((~keep).sum()),
        trimmed_dates=trimmed,
    )


# --- normal weather ------------------------------------------------------------------------------------------


def normal_weather(weather_features: pl.DataFrame, years: tuple[int, int] = NORMAL_YEARS) -> pl.DataFrame:
    """The weather of every calendar day in each normal year: ``weather_zone``, ``month``, ``day``,
    ``weather_year`` and the features. February 29 is dropped (targets on Feb 29 read Feb 28)."""
    return (
        weather_features.filter(pl.col("date").dt.year().is_between(*years))
        .with_columns(pl.col("date").dt.month().alias("month"), pl.col("date").dt.day().alias("day"),
                      pl.col("date").dt.year().alias("weather_year"))
        .filter(~((pl.col("month") == 2) & (pl.col("day") == 29)))
        .select("weather_zone", "month", "day", "weather_year", "t_mean", "t_lag", "td_mean", "t_max")
    )


def weather_draws(frame: pl.DataFrame, normals: pl.DataFrame) -> pl.DataFrame:
    """``frame``'s days (with ``weather_zone`` and ``date``) crossed with the same calendar day of every normal
    year: one row per (date, weather_year) carrying that year's weather."""
    keys = frame.select("weather_zone", "date").with_columns(
        pl.col("date").dt.month().alias("month"),
        pl.when((pl.col("date").dt.month() == 2) & (pl.col("date").dt.day() == 29)).then(28)
        .otherwise(pl.col("date").dt.day()).alias("day"),
    )
    return keys.join(normals, on=["weather_zone", "month", "day"], how="inner").sort("date", "weather_year")


def normalize_year(fit: DailyFit, frame: pl.DataFrame, normals: pl.DataFrame, *, keep_draws: bool = False):
    """Normalize ``frame``'s days (one zone) with ``fit``: returns the day rows with ``fitted``, ``w_actual``,
    ``w_normal`` (mean over normal years) and ``normalized``; with ``keep_draws`` also the per-draw weather
    effect (``date``, ``weather_year``, ``w_draw``)."""
    frame = frame.sort("date")
    draws = weather_draws(frame, normals)
    draw_eff = draws.select("date", "weather_year").with_columns(
        pl.Series("w_draw", fit.weather_effect(draws))
    )
    mean_eff = draw_eff.group_by("date").agg(pl.col("w_draw").mean().alias("w_normal"), pl.len().alias("n_draws"))
    out = (
        frame.with_columns(pl.Series("fitted", fit.predict(frame, flat_after_end=False)),
                           pl.Series("w_actual", fit.weather_effect(frame)))
        .join(mean_eff, on="date", how="left")
        .with_columns((pl.col(fit.target) - pl.col("w_actual") + pl.col("w_normal")).alias("normalized"),
                      pl.lit(f"{fit.start}-{fit.end}").alias("window"))
    )
    return (out, draw_eff) if keep_draws else out


def normalize_zone(
    panel: pl.DataFrame,
    zone: str,
    target: str,
    normals: pl.DataFrame,
    spec: Spec = Spec(),
    *,
    years: Sequence[int] | None = None,
    width: int = 3,
    draw_months: Sequence[int] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame, dict[tuple[int, int], DailyFit]]:
    """Normalize every year of one zone with the rolling window around it. Returns the daily rows, the per-draw
    effects on ``draw_months`` days (for the summer-peak distribution; empty when None) and the fits by window."""
    sub = panel.filter(pl.col("weather_zone") == zone)
    all_years = sorted(sub["date"].dt.year().unique().to_list())
    first, last = all_years[0], all_years[-1]
    fits: dict[tuple[int, int], DailyFit] = {}
    days, draws = [], []
    for year in years or all_years:
        win = window_for(year, first, last, width)
        if win not in fits:
            fits[win] = fit_daily(sub, target, *win, spec=spec, zone=zone)
        frame = sub.filter(pl.col("date").dt.year() == year)
        out, eff = normalize_year(fits[win], frame, normals, keep_draws=True)
        days.append(out)
        if draw_months:
            keep = frame.filter(pl.col("date").dt.month().is_in(list(draw_months))).select("date")
            draws.append(eff.join(keep, on="date", how="semi").with_columns(pl.lit(zone).alias("weather_zone")))
    draw_frame = pl.concat(draws) if draws else pl.DataFrame()
    return pl.concat(days), draw_frame, fits


def normal_season_peak(days: pl.DataFrame, draws: pl.DataFrame, target: str = "dmax") -> pl.DataFrame:
    """Per zone, year and normal weather year: the season's highest ``target - w_actual + w_draw`` (each day keeps
    its residual and day type, only the weather is swapped). ``days`` and ``draws`` restricted to the season."""
    return (
        draws.join(days.select("weather_zone", "date", target, "w_actual"), on=["weather_zone", "date"], how="inner")
        .with_columns((pl.col(target) - pl.col("w_actual") + pl.col("w_draw")).alias("swapped"))
        .group_by("weather_zone", pl.col("date").dt.year().alias("year"), "weather_year")
        .agg(pl.col("swapped").max().alias("season_peak"), pl.len().alias("days"))
        .sort("weather_zone", "year", "weather_year")
    )


def peak_quantiles(season: pl.DataFrame) -> pl.DataFrame:
    """P10 / P50 / P90 and mean over normal weather years of :func:`normal_season_peak`."""
    return (
        season.group_by("weather_zone", "year")
        .agg(pl.col("season_peak").quantile(0.1, "linear").alias("p10"),
             pl.col("season_peak").median().alias("p50"),
             pl.col("season_peak").quantile(0.9, "linear").alias("p90"),
             pl.col("season_peak").mean().alias("mean"),
             pl.len().alias("weather_years"))
        .sort("weather_zone", "year")
    )


# --- aggregation ---------------------------------------------------------------------------------------------


def monthly(energy_days: pl.DataFrame, peak_days: pl.DataFrame | None = None) -> pl.DataFrame:
    """Daily normalized rows → per zone and month: actual and normalized energy (GWh, daily mean × hours), the
    same as average MW (``avg_mw``, ``avg_norm_mw``, comparable across partial months), ``complete``, and with ``peak_days`` the highest actual daily peak and the highest weather-adjusted daily peak (MW)."""
    e = (
        energy_days.group_by("weather_zone", pl.col("date").dt.truncate("1mo").alias("month"))
        .agg((pl.col("dmean") * pl.col("n_hours")).sum().truediv(1000).alias("energy_gwh"),
             (pl.col("normalized") * pl.col("n_hours")).sum().truediv(1000).alias("energy_norm_gwh"),
             pl.len().alias("days"), pl.col("n_hours").sum().alias("hours"), pl.col("t_mean").mean().alias("t_mean"))
        .with_columns((pl.col("energy_gwh") * 1000 / pl.col("hours")).alias("avg_mw"),
                      (pl.col("energy_norm_gwh") * 1000 / pl.col("hours")).alias("avg_norm_mw"),
                      (pl.col("days") == pl.col("month").dt.month_end().dt.day()).alias("complete"))
    )
    if peak_days is not None:
        p = peak_days.group_by("weather_zone", pl.col("date").dt.truncate("1mo").alias("month")).agg(
            pl.col("dmax").max().alias("peak_mw"), pl.col("normalized").max().alias("peak_norm_mw")
        )
        e = e.join(p, on=["weather_zone", "month"], how="left")
    return e.sort("weather_zone", "month")


def annual(month_rows: pl.DataFrame) -> pl.DataFrame:
    """Monthly rows → per zone and year: energy actual and normalized (GWh) and the months present."""
    return (
        month_rows.group_by("weather_zone", pl.col("month").dt.year().alias("year"))
        .agg(pl.col("energy_gwh").sum(), pl.col("energy_norm_gwh").sum(), pl.len().alias("months"),
             pl.col("days").sum())
        .sort("weather_zone", "year")
    )


def cagr(start_value: float, end_value: float, years: float) -> float:
    return (end_value / start_value) ** (1 / years) - 1


def acf(x: Sequence[float], lags: Sequence[int]) -> dict[int, float]:
    """Sample autocorrelation of ``x`` at ``lags``."""
    import numpy as np

    x = np.asarray(x, dtype=float)
    x = x - x.mean()
    denom = float(x @ x)
    return {lag: float(x[lag:] @ x[:-lag]) / denom for lag in lags}


# --- validation ----------------------------------------------------------------------------------------------


def holdout(panel: pl.DataFrame, zone: str, target: str, spec: Spec, *, train: tuple[int, int],
            test: Sequence[int]) -> pl.DataFrame:
    """Fit on ``train`` years, predict the ``test`` days with their actual weather.

    ``pred_flat`` holds the level at the end of training (a forecast); ``pred_relevel`` adds each test year's mean
    error (the level known, the weather response and day types tested); ``pred_noweather`` is the same re-leveled
    prediction from a model without weather (the benchmark weather has to beat). Excluded days are left out."""
    fit = fit_daily(panel, target, *train, spec=spec, zone=zone)
    base = fit_daily(panel, target, *train, spec=replace(spec, weather=False), zone=zone)
    frame = panel.filter((pl.col("weather_zone") == zone) & pl.col("date").dt.year().is_in(list(test))
                         & ~pl.col("excluded")).sort("date")
    out = frame.select("weather_zone", "date", pl.col(target).alias("actual")).with_columns(
        pl.Series("pred_flat", fit.predict(frame)), pl.Series("pred_base", base.predict(frame))
    )
    shift = (pl.col("actual") - pl.col("pred_flat")).mean().over(pl.col("date").dt.year())
    shift_b = (pl.col("actual") - pl.col("pred_base")).mean().over(pl.col("date").dt.year())
    return out.with_columns((pl.col("pred_flat") + shift).alias("pred_relevel"),
                            (pl.col("pred_base") + shift_b).alias("pred_noweather")).drop("pred_base")


def holdout_mape(errors: pl.DataFrame) -> pl.DataFrame:
    """MAPE (%) per zone of the three predictions of :func:`holdout`, and the median APE of the re-leveled one."""
    def ape(col: str) -> pl.Expr:
        return (100 * (pl.col(col) - pl.col("actual")).abs() / pl.col("actual"))

    return (
        errors.group_by("weather_zone", maintain_order=True)
        .agg(ape("pred_flat").mean().alias("mape_flat"), ape("pred_relevel").mean().alias("mape_relevel"),
             ape("pred_noweather").mean().alias("mape_noweather"),
             ape("pred_relevel").median().alias("mdape_relevel"),
             (100 * (pl.col("pred_flat") - pl.col("actual")).mean() / pl.col("actual").mean()).alias("bias_flat"),
             pl.len().alias("days"))
    )


# --- loaders (thin) ------------------------------------------------------------------------------------------


def load_panel(years_for_weights: Iterable[int] = range(2003, 2023)) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Read load and weather (``weather_load`` loaders) and build the daily panel for the eight zones and
    ``ERCOT`` (weather weighted by each zone's all-month energy share over ``years_for_weights``). Returns
    (panel, weather_features)."""
    load = wl.load_hourly_load()
    weather = wl.load_hourly_weather()
    return build_panel(load, weather, years_for_weights)


def build_panel(load: pl.DataFrame, weather: pl.DataFrame,
                years_for_weights: Iterable[int] = range(2003, 2023)) -> tuple[pl.DataFrame, pl.DataFrame]:
    daily = wl.weather_daily(weather)
    weights = wl.zone_weights(load, years_for_weights, months=range(1, 13))
    system = wl.system_weather_daily(daily, weights)
    feats = weather_daily_features(pl.concat([daily, system], how="diagonal_relaxed"))
    return daily_panel(load, feats), feats
