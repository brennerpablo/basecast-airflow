"""Exploration X7: a prototype of the summer peak forecast (P10/P50/P90) and its pseudo-out-of-sample backtest.

``peak(Y) = organic(Y) + LL(Y) + U``, built from the pieces of Q5, Q7 and X1 (their modules are imported, not
changed):

- **organic**: Q7's ``peak ~ 1 + (year - 2014) + t_mean_3d``, fit on the pre-break summers (<= 2019, X1). Drawn with
  parameter uncertainty (OLS covariance), a weather value resampled from the training summers and a Gaussian
  residual with the training SD.
- **LL** (large load at the summer peak): ``factor x A2E(summer Y)``, the approved-to-energize stock at the end of
  July of ``Y`` times the observed-simultaneous-peak / A2E factor (X1: 0.51). The A2E path is a line through the
  vintage's own stock and the projected December stocks ``A2E(Dec Y) = base + r x max(promised(Y) - base, 0)``
  (Q5's incremental realization ratio ``r``, one draw per simulation for every year).
- **U** (unattributed flat load): the post-break excess over the organic model that the decks' observed large loads
  do not explain, resampled from the summers known at the as-of date and held flat.

Every input can be cut **as of a date** (decks by ``report_date``, realized stocks by the date the deck that
reports them was published), which is what the pseudo-out-of-sample backtest needs. The three layers are drawn
independently (a prototype simplification). Pure functions on DataFrames and numpy; no loaders of its own.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from basecast_pipelines.models import large_load as ll
from basecast_pipelines.models import peak_excess as px

if TYPE_CHECKING:
    import numpy as np

PRE_BREAK_UNTIL = 2019
POST_BREAK_FROM = 2022  # first summer counted in U (X1: 2022 is the first summer > 3 SD above the pre-break model)
SUMMER_POINT = 6.5  # month offset of the summer peak within the year: end of July (peaks fell Jul 20 - Aug 20)
MIN_HORIZON_MONTHS = 6  # Q5: ratios from decks made weeks before year end say nothing about new MW
QUANTILES = (0.1, 0.5, 0.9)


# --- month arithmetic ----------------------------------------------------------------------------------------


def month_index(value: str | date) -> float:
    """``"YYYY-MM"`` or a date -> running month count (Jan 2000 = 24000); a date adds its day as a fraction."""
    if isinstance(value, date):
        return value.year * 12 + value.month - 1 + (value.day - 1) / 31
    return int(value[:4]) * 12 + int(value[5:7]) - 1


def dec_index(year: int) -> float:
    """Month index of the end of December of ``year`` (the realized stock's date)."""
    return year * 12 + 11


def summer_index(year: int) -> float:
    return year * 12 + SUMMER_POINT


def interpolate(points: Sequence[tuple[float, float]], at: float) -> float | None:
    """Piecewise-linear value at ``at`` through ``(index, value)`` points; carried flat after the last point,
    ``None`` before the first."""
    pts = sorted(points)
    if not pts or at < pts[0][0]:
        return None
    for (x0, y0), (x1, y1) in zip(pts, pts[1:], strict=False):
        if x0 <= at <= x1:
            return y0 if x1 == x0 else y0 + (y1 - y0) * (at - x0) / (x1 - x0)
    return pts[-1][1]


# --- decks as of a date ---------------------------------------------------------------------------------------


def published_by(frame: pl.DataFrame, as_of: date, column: str = "report_date") -> pl.DataFrame:
    """Rows of decks published on or before ``as_of`` (``report_date`` is the deck's posting date)."""
    return frame.filter(pl.col(column) <= as_of)


def realized_year_end(vintages: pl.DataFrame) -> pl.DataFrame:
    """Approved-to-energize stock at the end of each year, and when it became known.

    ``vintages``: one row per deck (``vintage``, ``report_date``, ``a2e_stock_mw``). The stock at Dec ``Y`` is the
    stock printed by the first deck whose data date is after Dec 31 of ``Y`` (Q5 reads 6,297 / 8,786 MW for 2024 /
    2025 off the status series; the first January decks print 6,306 / 8,786). ``known_from`` is that deck's
    ``report_date``."""
    decks = vintages.drop_nulls("a2e_stock_mw").sort("vintage")
    if decks.is_empty():
        return pl.DataFrame(schema={"target_year": pl.Int64, "realized_a2e_mw": pl.Float64, "known_from": pl.Date,
                                    "read_from_vintage": pl.Date})
    rows = []
    for year in range(decks["vintage"].min().year - 1, decks["vintage"].max().year):
        after = decks.filter(pl.col("vintage") > date(year, 12, 31))
        if after.is_empty():
            continue
        first = after.row(0, named=True)
        rows.append({"target_year": year, "realized_a2e_mw": first["a2e_stock_mw"],
                     "known_from": first["report_date"], "read_from_vintage": first["vintage"]})
    return pl.DataFrame(rows, schema={"target_year": pl.Int64, "realized_a2e_mw": pl.Float64,
                                      "known_from": pl.Date, "read_from_vintage": pl.Date})


def incremental_ratios(
    bars: pl.DataFrame,
    realized: pl.DataFrame,
    as_of: date,
    *,
    min_horizon_months: int = MIN_HORIZON_MONTHS,
    firm: bool = False,
) -> pl.DataFrame:
    """Q5's incremental realization ratio, using only what was published by ``as_of``.

    ``bars``: one row per vintage x year (``vintage``, ``report_date``, ``year``, ``total_mw``,
    ``no_studies_submitted``, ``base_a2e_mw``). ``realized``: :func:`realized_year_end`. A (vintage, target year)
    pair counts when the deck was published by ``as_of``, the realized stock was known by ``as_of``, the deck
    predates the end of the target year by at least ``min_horizon_months`` and it promised new MW.
    ``ratio = (realized - base) / (promised - base)``; ``firm`` drops "No Studies Submitted" from the promise."""
    known = realized.filter(pl.col("known_from") <= as_of)
    promised = (pl.col("total_mw") - pl.col("no_studies_submitted").fill_null(0)) if firm else pl.col("total_mw")
    horizon = (pl.date(pl.col("year"), 12, 31) - pl.col("vintage")).dt.total_days() / 30.44
    return (
        bars.filter(pl.col("report_date") <= as_of)
        .join(known, left_on="year", right_on="target_year", how="inner")
        .with_columns(promised.alias("promised_mw"), horizon.round(0).cast(pl.Int32).alias("horizon_months"))
        .filter((pl.col("horizon_months") >= min_horizon_months) & pl.col("base_a2e_mw").is_not_null()
                & (pl.col("promised_mw") > pl.col("base_a2e_mw")))
        .with_columns(((pl.col("realized_a2e_mw") - pl.col("base_a2e_mw"))
                       / (pl.col("promised_mw") - pl.col("base_a2e_mw"))).alias("ratio"))
        .select("vintage", "report_date", pl.col("year").alias("target_year"), "horizon_months", "promised_mw",
                "base_a2e_mw", "realized_a2e_mw", "known_from", "ratio")
        .sort("target_year", "vintage")
    )


def a2e_points(cv: pl.DataFrame, vintages: pl.DataFrame, bars: pl.DataFrame, as_of: date) -> pl.DataFrame:
    """The approved-to-energize stock by month as known on ``as_of``: ``month_idx``, ``a2e_mw``, ``source``.

    Priority for a month: the status-by-month series (latest reading among decks published by ``as_of``,
    misdated axes dropped, Q5's ``a2e_by_month``), then a deck's own stock at its data date, then, only before
    the first of those, the in-service bars of past years (MW approved and in service by Dec ``Y``)."""
    cv_d = published_by(cv, as_of)
    status = ll.a2e_by_month(cv_d) if not cv_d.is_empty() else pl.DataFrame()
    pts: dict[float, tuple[float, str]] = {}
    if not status.is_empty():
        for row in status.drop_nulls("a2e_mw").iter_rows(named=True):
            pts[month_index(row["month"])] = (row["a2e_mw"], "status_by_month")
    for row in published_by(vintages, as_of).drop_nulls("a2e_stock_mw").iter_rows(named=True):
        idx = float(row["vintage"].year * 12 + row["vintage"].month - 1)
        pts.setdefault(idx, (row["a2e_stock_mw"], "deck_stock"))
    first = min(pts) if pts else float("inf")
    past = (published_by(bars, as_of).filter(pl.col("year") < pl.col("vintage").dt.year()).drop_nulls("a2e")
            .sort("vintage"))
    for row in past.iter_rows(named=True):
        idx = dec_index(row["year"])
        if idx < first:
            pts.setdefault(idx, (row["a2e"], "in_service_bar"))
    return pl.DataFrame(
        [{"month_idx": k, "a2e_mw": v, "source": s} for k, (v, s) in sorted(pts.items())],
        schema={"month_idx": pl.Float64, "a2e_mw": pl.Float64, "source": pl.String},
    )


def observed_factor(
    cv: pl.DataFrame, points: pl.DataFrame, peak_months: Mapping[int, str], as_of: date
) -> pl.DataFrame:
    """X1's factor per past summer: the decks' observed simultaneous peak of approved large loads in the summer's
    peak month ÷ the approved stock that month, both as published by ``as_of``. Summers without an observation
    (the series starts in April 2023) are left out."""
    cv_d = published_by(cv, as_of)
    energized = px.energized_by_month(cv_d) if not cv_d.is_empty() else pl.DataFrame()
    pts = list(zip(points["month_idx"].to_list(), points["a2e_mw"].to_list(), strict=True))
    rows = []
    for year, month in sorted(peak_months.items()):
        if energized.is_empty() or month > energized["month"].max() or month < energized["month"].min():
            continue
        sim, used = px.value_asof(energized, month, "simultaneous_mw")
        a2e = interpolate(pts, month_index(month))
        if sim is None or not a2e:
            continue
        rows.append({"year": year, "peak_month": month, "simultaneous_mw": sim, "sim_month": used, "a2e_mw": a2e,
                     "factor": sim / a2e})
    return pl.DataFrame(rows, schema={"year": pl.Int64, "peak_month": pl.String, "simultaneous_mw": pl.Float64,
                                      "sim_month": pl.String, "a2e_mw": pl.Float64, "factor": pl.Float64})


def unattributed(
    excess: Mapping[int, float], points: pl.DataFrame, peak_months: Mapping[int, str], factor: float,
    *, first_year: int = POST_BREAK_FROM,
) -> pl.DataFrame:
    """U per past summer: the excess over the organic model minus ``factor x A2E`` in the summer's peak month."""
    pts = list(zip(points["month_idx"].to_list(), points["a2e_mw"].to_list(), strict=True))
    rows = []
    for year in sorted(excess):
        if year < first_year or year not in peak_months:
            continue
        a2e = interpolate(pts, month_index(peak_months[year]))
        if a2e is None:
            continue
        rows.append({"year": year, "excess_mw": excess[year], "a2e_mw": a2e, "ll_mw": factor * a2e,
                     "u_mw": excess[year] - factor * a2e})
    return pl.DataFrame(rows, schema={"year": pl.Int64, "excess_mw": pl.Float64, "a2e_mw": pl.Float64,
                                      "ll_mw": pl.Float64, "u_mw": pl.Float64})


# --- projection ----------------------------------------------------------------------------------------------


def promised_by_year(bars: pl.DataFrame, years: Iterable[int], *, firm: bool = False) -> dict[int, float]:
    """One vintage's in-service bars -> MW promised in service by Dec of each year. Bars are cumulative, so a year
    past the last bar gets the last bar (the whole queue)."""
    b = bars.sort("year")
    col = (pl.col("total_mw") - pl.col("no_studies_submitted").fill_null(0)) if firm else pl.col("total_mw")
    vals = dict(zip(b["year"].to_list(), b.select(col)["total_mw"].to_list(), strict=True))
    last_year = max(vals)
    return {y: vals.get(y, vals[last_year] if y > last_year else 0.0) for y in years}


def a2e_path(base: float, vintage: date, promised: Mapping[int, float], ratio: float,
             years: Iterable[int]) -> dict[int, float]:
    """A2E at the summer point of each year: a line through (vintage, base) and the projected December stocks
    ``base + ratio x max(promised(Y) - base, 0)`` for every year from the vintage's year on."""
    years = sorted(years)
    knots = [(month_index(vintage), base)]
    for y in range(vintage.year, max(years) + 1):
        knots.append((dec_index(y), base + ratio * max(promised.get(y, 0.0) - base, 0.0)))
    return {y: interpolate(knots, summer_index(y)) for y in years}


# --- organic model with uncertainty --------------------------------------------------------------------------


@dataclass
class OrganicFit:
    """OLS of ``peak ~ 1 + (year - 2014) + x`` with the coefficient covariance, for simulation."""

    beta: list[float]
    cov: list[list[float]]
    sigma: float
    x_train: list[float]
    years: list[int]

    def mean(self, year: float, x: float) -> float:
        return self.beta[0] + self.beta[1] * (year - 2014.0) + self.beta[2] * x

    @property
    def x_p50(self) -> float:
        import numpy as np

        return float(np.median(self.x_train))


def fit_organic(frame: pl.DataFrame, feature: str, *, first: int | None = None,
                until: int = PRE_BREAK_UNTIL) -> OrganicFit:
    """``frame``: ``year``, ``peak_mw`` and ``feature``. Same model and centering as ``weather_load.fit_peak``."""
    import numpy as np

    sub = frame.filter(pl.col("year") <= until)
    if first is not None:
        sub = sub.filter(pl.col("year") >= first)
    sub = sub.drop_nulls(["peak_mw", feature]).sort("year")
    year = sub["year"].to_numpy().astype(float)
    x = sub[feature].to_numpy().astype(float)
    y = sub["peak_mw"].to_numpy().astype(float)
    X = np.column_stack([np.ones_like(year), year - 2014.0, x])
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ beta
    dof = max(len(y) - X.shape[1], 1)
    sigma2 = float(resid @ resid) / dof
    cov = sigma2 * np.linalg.pinv(X.T @ X)
    return OrganicFit(beta=beta.tolist(), cov=cov.tolist(), sigma=sigma2 ** 0.5, x_train=x.tolist(),
                      years=[int(v) for v in year])


def organic_draws(fit: OrganicFit, years: Sequence[int], n: int, rng, *,
                  resid_sd: float | None = None) -> "np.ndarray":
    """``n x len(years)`` draws with coefficients ~ MVN(beta, cov) (one per draw, so the trend's extrapolation
    error grows with the year).

    Without ``resid_sd``: weather resampled from the training summers (independently per year) plus a residual
    ~ N(0, sigma). With ``resid_sd``: weather at its training median and a residual ~ N(0, resid_sd), where
    ``resid_sd`` is an out-of-sample error SD that already carries weather and noise (the default of the script:
    the one-year-ahead rolling-origin RMSE over pre-break summers)."""
    import numpy as np

    b = rng.multivariate_normal(np.asarray(fit.beta), np.asarray(fit.cov), size=n)
    yrs = np.asarray(years, dtype=float)
    if resid_sd is None:
        x = rng.choice(np.asarray(fit.x_train), size=(n, len(yrs)))
        eps = rng.normal(0.0, fit.sigma, size=(n, len(yrs)))
    else:
        x = np.full((n, len(yrs)), fit.x_p50)
        eps = rng.normal(0.0, resid_sd, size=(n, len(yrs)))
    return b[:, [0]] + b[:, [1]] * (yrs - 2014.0) + b[:, [2]] * x + eps


def rolling_origin_errors(frame: pl.DataFrame, feature: str, first_test: int, last_test: int) -> pl.DataFrame:
    """One-year-ahead errors of the organic model at median weather: fit on summers < Y, predict Y at the training
    median of ``feature`` (a normal-weather forecast, like the official ones), for Y in [first_test, last_test]."""
    rows = []
    for y in range(first_test, last_test + 1):
        f = fit_organic(frame, feature, until=y - 1)
        actual = frame.filter(pl.col("year") == y)["peak_mw"].item()
        pred = f.mean(y, f.x_p50)
        rows.append({"year": y, "actual_mw": actual, "predicted_mw": pred, "error_mw": pred - actual,
                     "error_pct": 100 * (pred / actual - 1)})
    return pl.DataFrame(rows)


def large_load_draws(base: float, vintage: date, promised: Mapping[int, float], ratios: Sequence[float],
                     factor: float, years: Sequence[int], n: int, rng) -> "np.ndarray":
    """``n x len(years)`` draws of ``factor x A2E(summer Y)``, one realization ratio per draw (resampled from the
    ratios known at the as-of date) applied to every year."""
    import numpy as np

    r = rng.choice(np.asarray(ratios, dtype=float), size=n)
    uniq = {float(v): a2e_path(base, vintage, promised, float(v), years) for v in np.unique(r)}
    out = np.empty((n, len(years)))
    for i, v in enumerate(r):
        path = uniq[float(v)]
        out[i] = [factor * path[y] for y in years]
    return out


def simulate(
    fit: OrganicFit, years: Sequence[int], *, base: float | None, vintage: date | None,
    promised: Mapping[int, float] | None, ratios: Sequence[float] | None, factor: float | None,
    u_values: Sequence[float] | None, resid_sd: float | None = None, n: int = 10_000, seed: int = 7,
) -> dict[str, "np.ndarray"]:
    """Independent draws of the three layers and their sum. A layer whose inputs are ``None`` is zero."""
    import numpy as np

    rng = np.random.default_rng(seed)
    organic = organic_draws(fit, years, n, rng, resid_sd=resid_sd)
    if base is None or not ratios or factor is None:
        large = np.zeros_like(organic)
    else:
        large = large_load_draws(base, vintage, promised, ratios, factor, years, n, rng)
    u = (rng.choice(np.asarray(u_values, dtype=float), size=(n, 1)) * np.ones((1, len(years)))
         if u_values else np.zeros_like(organic))
    return {"organic": organic, "large_load": large, "unattributed": u, "total": organic + large + u}


def summarize(draws: Mapping[str, "np.ndarray"], years: Sequence[int],
              quantiles: Sequence[float] = QUANTILES) -> pl.DataFrame:
    """One row per layer and year: P10 / P50 / P90 (MW)."""
    import numpy as np

    rows = []
    for layer, arr in draws.items():
        q = np.quantile(arr, quantiles, axis=0)
        for j, y in enumerate(years):
            rows.append({"layer": layer, "year": int(y), **{f"p{round(100 * p)}_mw": float(q[i, j])
                                                          for i, p in enumerate(quantiles)}})
    return pl.DataFrame(rows)


def score(forecast: pl.DataFrame, actual: Mapping[int, float]) -> pl.DataFrame:
    """Error of the P50 and whether the actual fell inside P10-P90: ``forecast`` has ``year``, ``p10_mw``,
    ``p50_mw``, ``p90_mw`` (the ``total`` layer)."""
    return (
        forecast.filter(pl.col("year").is_in(list(actual)))
        .with_columns(pl.col("year").replace_strict(dict(actual), return_dtype=pl.Float64).alias("actual_mw"))
        .with_columns(
            (100 * (pl.col("p50_mw") / pl.col("actual_mw") - 1)).alias("error_pct"),
            pl.col("actual_mw").is_between(pl.col("p10_mw"), pl.col("p90_mw")).alias("in_p10_p90"),
        )
    )


def official_asof(base: pl.DataFrame, as_of: date, *, products: Sequence[str] = ("LTLF", "CDR", "LTLF-prelim"),
                  after_days: int = 0) -> pl.DataFrame:
    """For each product, the latest official vintage published on or before ``as_of`` (``backtest.base_series``
    rows); with ``after_days`` > 0 also the first one published up to that many days after (flagged)."""
    out = []
    for product in products:
        rows = base.filter(pl.col("product") == product)
        before = rows.filter(pl.col("vintage_date") <= as_of)
        if not before.is_empty():
            last = before["vintage_date"].max()
            out.append(before.filter(pl.col("vintage_date") == last).with_columns(pl.lit(False).alias("after_as_of")))
        if after_days:
            later = rows.filter((pl.col("vintage_date") > as_of)
                                & ((pl.col("vintage_date") - pl.lit(as_of)).dt.total_days() <= after_days))
            if not later.is_empty():
                first = later["vintage_date"].min()
                out.append(later.filter(pl.col("vintage_date") == first).with_columns(pl.lit(True).alias("after_as_of")))
    return pl.concat(out) if out else base.head(0).with_columns(pl.lit(False).alias("after_as_of"))
