"""Backtest marts (A-M6): the observed summer peaks, and every forecast scored against them.

``mart_actual_summer_peaks``: one row per summer (June-September) from ``ercot_monthly_peaks``, the hourly peak
(the forecasts' definition, hour ending) and the 15-minute one (interval ending, as X15 confirmed), as in Q1 §2.
A summer is ``final`` when its four months are published and either the workbook flags them as updated with final
settlements (``settlement_flagged``) or it is from a year before ``as_of``'s: the D&E workbooks carry that flag only
from 2015 on, so older summers, long settled, would otherwise read as preliminary.

``mart_peak_backtest``: X7's pseudo-out-of-sample backtest (``analysis/x7_peak_forecast.py`` §1-3 and §7, ported
here, not imported from the forecast marts): at each of X7's eight as-of dates the method is rebuilt from what was
published by then (decks by ``report_date``, realized stocks once known, factor and U from past summers only) with
the latest deck of that date (``variant = deck_latest``), and scored on every later summer through the last one
observed. Each (as_of, target_year) cell has one ``basecast`` row, the ``basecast_organic_only`` ablation and the
official vintages of the same date (``LTLF``, ``CDR``, ``LTLF-prelim``; Q1's ``base_series``); the API pairs them
on (as_of, target_year). ``era`` splits the dates at 2024-07-31, the first after LTLF 2024 took the TSPs' large
loads in.

``mart_official_forecast_errors``: Q1's matrix, every official vintage × target year against the actual.

``mart_queue_backtest``: X2's backtest of the adjusted generation queue (§3), primary variant only
(``queue.model``, MW-weighted), statewide and by fuel, with the county Spearman values of each date.

``mart_backtest_fan``: the marks of the latest backtested summer's fan: ERCOT's preliminary figure and range (the
manual figures), every official vintage targeting it, the actual and the backtest's cells for that summer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import polars as pl

from basecast_pipelines.config import PROJECT_ROOT
from basecast_pipelines.marts import config as marts_config
from basecast_pipelines.marts import queue as marts_queue
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
            (pl.col("complete") & (pl.col("all_final") | (pl.col("year") < ctx.as_of.year))).alias("final"),
            pl.col("all_final").alias("settlement_flagged"),
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
    version=2,
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
        value_check("every complete summer before 2025 final", lambda f: f.filter(
            pl.col("year") < 2025, pl.col("months_published") == 4, ~pl.col("final")).height, 0, as_of=GOLDEN_AS_OF),
    ),
)

# --- shared inputs -------------------------------------------------------------------------------------------


def summer_actuals(ctx: MartContext) -> pl.DataFrame:
    """``mart_actual_summer_peaks``' frame, once per run: every forecast here is scored on ``hourly_peak_mw`` (the
    value Q1 and X7 read from ``backtest.actual_summer_peaks``), with its ``final`` flag."""
    return ctx.cached("backtest.summer_actuals", lambda: actual_summer_peaks(ctx))


def official_forecasts(ctx: MartContext) -> pl.DataFrame:
    """The ``official_forecasts`` rows Q1's ``base_series`` picks from (ERCOT peaks and the manual figures)."""
    from basecast_pipelines.models import backtest as bt

    return ctx.cached("backtest.official_forecasts", bt.load_official_forecasts)


def official_base(ctx: MartContext) -> pl.DataFrame:
    """Q1's ``base_series``: one forecast per (product, vintage, target year)."""
    from basecast_pipelines.models import backtest as bt

    return ctx.cached("backtest.official_base", lambda: bt.base_series(official_forecasts(ctx)))


def manual_verified(ctx: MartContext) -> dict[str, bool]:
    """``manual_official_figures.verified`` by figure id (the ``row_label`` of the manual rows in
    ``official_forecasts``)."""
    return ctx.cached("backtest.manual_verified", lambda: dict(
        ctx.read_sql("select figure_id, verified from manual_official_figures").iter_rows()))


def last_summer(as_of: date) -> int:
    """The last summer observed on ``as_of`` (X7: a summer counts from September on; 2026 peaked on Jul 22)."""
    return as_of.year if as_of.month >= 9 else as_of.year - 1


def add_months(d: date, n: int) -> date:
    """First of the month ``n`` months after ``d``'s month (X2's ``add_month``)."""
    m = d.month - 1 + n
    return date(d.year + m // 12, m % 12 + 1, 1)


# --- mart_peak_backtest (X7 §3) --------------------------------------------------------------------------------

# the month end after each official publication since 2023: LTLF 2023 + CDR May 2023, CDR Dec 2023, CDR May 2024,
# LTLF 2024 (Jul 18), CDR Dec 2024, LTLF 2025 + CDR May 2025, CDR Dec 2025, 2026 preliminary LTLF (Apr 15)
PEAK_AS_OF_DATES = (date(2023, 5, 31), date(2023, 12, 31), date(2024, 5, 31), date(2024, 7, 31), date(2024, 12, 31),
                    date(2025, 5, 31), date(2025, 12, 31), date(2026, 5, 31))
TSP_ERA_START = date(2024, 7, 31)  # first as-of date after LTLF 2024, the first vintage built on TSP large loads
ERAS = (
    {"era": "before_tsp_loads", "label": "Before TSP large loads",
     "description": "As-of dates before 2024-07-31: ERCOT's latest forecasts (LTLF 2023, CDR through May 2024) did "
                    "not yet take the TSPs' large-load submissions in."},
    {"era": "with_tsp_loads", "label": "With TSP large loads",
     "description": "As-of dates from 2024-07-31 on: LTLF 2024 (published 2024-07-18) and later forecasts carry the "
                    "TSPs' large-load submissions."},
)
FIRST_SUMMER, FEATURE = 2003, "t_mean_3d"  # X7: Q7's organic model and weather feature
ORGANIC_BAND_FROM = 2011  # the organic band's SD: RMSE of the one-year-ahead errors, 2011 through the fit end
N_DRAWS = 10_000
VARIANT = "deck_latest"  # X7's "method": the latest deck published by each as-of date
BASECAST, ORGANIC_ONLY = "basecast", "basecast_organic_only"

CELL_SCHEMA = {
    "as_of": pl.Date, "target_year": pl.Int64, "horizon": pl.Int64, "era": pl.String, "actual_mw": pl.Float64,
    "actual_final": pl.Boolean, "source": pl.String, "product": pl.String, "vintage": pl.String,
    "vintage_date": pl.Date, "variant": pl.String, "p10_mw": pl.Float64, "p50_mw": pl.Float64, "p90_mw": pl.Float64,
    "error_pct": pl.Float64, "in_band": pl.Boolean, "organic_p50": pl.Float64, "ll_p50": pl.Float64,
    "u_p50": pl.Float64, "ll_realized": pl.Float64, "u_realized": pl.Float64, "leak_note": pl.String,
    "verified": pl.Boolean, "deck_vintage": pl.Date, "factor": pl.Float64, "series": pl.String,
    "vintage_horizon": pl.Int64,
}


def era_of(as_of: date) -> str:
    return "with_tsp_loads" if as_of >= TSP_ERA_START else "before_tsp_loads"


def cell_row(**values: Any) -> dict[str, Any]:
    """One backtest row with every :data:`CELL_SCHEMA` column (null unless given)."""
    unknown = set(values) - set(CELL_SCHEMA)
    if unknown:
        raise KeyError(f"not backtest columns: {sorted(unknown)}")
    return {c: values.get(c) for c in CELL_SCHEMA}


@dataclass(frozen=True)
class X7Data:
    """What X7's method reads, loaded once (``analysis/x7_peak_forecast.py`` §1-3)."""

    fit: Any  # peak_forecast.OrganicFit, summers FIRST_SUMMER..organic_fit_end
    resid_sd: float  # organic band SD (rolling-origin RMSE)
    excess: dict[int, float]  # summer peak over the organic model at actual weather, summers after the fit
    peak_months: dict[int, str]  # summer -> "YYYY-MM" of its ERCOT peak
    cv: pl.DataFrame  # large_load_chart_values
    vintages: pl.DataFrame  # one picked deck per month (Q5)
    bars: pl.DataFrame  # the picked decks' in-service bars
    realized: pl.DataFrame  # realized December A2E stocks and when they were known


def x7_data(ctx: MartContext) -> X7Data:
    return ctx.cached("backtest.x7_data", lambda: _load_x7(ctx))


def _load_x7(ctx: MartContext) -> X7Data:
    import numpy as np

    from basecast_pipelines.models import large_load as ll
    from basecast_pipelines.models import peak_forecast as pf
    from basecast_pipelines.models import weather_load as wl

    fit_end = marts_config.value(ctx.config, "forecast.organic_fit_end")
    load = wl.load_hourly_load()
    weather = wl.load_hourly_weather()
    cv = ll.load_chart_values()
    headlines = ll.load_headlines()
    manifests = ll.load_manifests(PROJECT_ROOT / "data")

    # 1. Panel: summer peaks + weather (ERCOT weather weighted by 2003-2019 summer energy, as in X1)
    daily_w = wl.weather_daily(weather)
    weights = wl.zone_weights(load, range(FIRST_SUMMER, pf.PRE_BREAK_UNTIL + 1))
    daily_all = pl.concat([daily_w.drop("n_hours"), wl.system_weather_daily(daily_w, weights).drop("n_hours")])
    features = wl.summer_weather_features(daily_all)
    peaks = wl.summer_peaks(load)
    panel = (peaks.select("weather_zone", "year", "peak_mw", "peak_local")
             .join(features, on=["weather_zone", "year"], how="inner").filter(pl.col("year") >= FIRST_SUMMER))
    ercot = panel.filter(pl.col("weather_zone") == wl.TOTAL).sort("year")
    peak_months = {r["year"]: r["peak_local"].strftime("%Y-%m") for r in ercot.iter_rows(named=True)}
    x_actual = dict(zip(ercot["year"].to_list(), ercot[FEATURE].to_list(), strict=True))
    load_peak = dict(zip(ercot["year"].to_list(), ercot["peak_mw"].to_list(), strict=True))
    fit = pf.fit_organic(ercot, FEATURE, until=fit_end)
    excess = {y: load_peak[y] - fit.mean(y, x_actual[y]) for y in load_peak if y > fit_end}

    # 2. Organic band: one-year-ahead rolling-origin errors at median weather over the pre-break summers
    roll = pf.rolling_origin_errors(ercot, FEATURE, ORGANIC_BAND_FROM, fit_end)
    resid_sd = float(np.sqrt((roll["error_mw"] ** 2).mean()))

    # 3. Decks: in-service bars per picked vintage, realized year-end stock and when it was known
    wide = ll.in_service_wide(cv)
    vintages = ll.pick_vintages(ll.check_headlines(ll.chart_checks(wide), headlines), manifests)
    bars = wide.join(vintages.select("vintage", "document", "page", "chart_title",
                                     pl.col("a2e_stock_mw").alias("base_a2e_mw")),
                     on=["vintage", "document", "page", "chart_title"], how="inner")
    return X7Data(fit, resid_sd, excess, peak_months, cv, vintages, bars, pf.realized_year_end(vintages))


def x7_inputs_asof(x: X7Data, as_of: date) -> dict[str, Any]:
    """Every input of the method as it could be known on ``as_of`` (X7's ``inputs_asof``, latest deck)."""
    from basecast_pipelines.models import peak_forecast as pf

    last = last_summer(as_of)
    points = pf.a2e_points(x.cv, x.vintages, x.bars, as_of)
    months = {y: m for y, m in x.peak_months.items() if y <= last}
    fac = pf.observed_factor(x.cv, points, months, as_of)
    ratios = pf.incremental_ratios(x.bars, x.realized, as_of)
    leaks = []
    if fac.is_empty():  # not estimable yet: take the first estimate that exists and flag it
        def months_before(d: date) -> dict[int, str]:
            return {y: m for y, m in x.peak_months.items() if y <= d.year - 1}

        first = min(d for d in x.vintages["report_date"].to_list() if not pf.observed_factor(
            x.cv, pf.a2e_points(x.cv, x.vintages, x.bars, d), months_before(d), d).is_empty())
        fac = pf.observed_factor(x.cv, pf.a2e_points(x.cv, x.vintages, x.bars, first), months_before(first), first)
        leaks.append(f"factor from decks published {first}")
    if ratios.is_empty():
        first = x.realized.filter(pl.col("known_from") > as_of)["known_from"].min()
        ratios = pf.incremental_ratios(x.bars, x.realized, first)
        leaks.append(f"ratios known from {first}")
    factor = float(fac["factor"].mean())
    u = pf.unattributed({y: v for y, v in x.excess.items() if y <= last}, points, months, factor)
    v = pf.published_by(x.vintages, as_of).drop_nulls("a2e_stock_mw").sort("vintage").row(-1, named=True)
    vbars = x.bars.filter((pl.col("vintage") == v["vintage"]) & (pl.col("document") == v["document"]))
    return {"as_of": as_of, "points": points, "factor": factor, "ratios": ratios, "u": u, "vintage": v["vintage"],
            "base": v["a2e_stock_mw"], "bars": vbars, "leaks": leaks}


def x7_run(x: X7Data, inp: dict[str, Any], years: list[int], *, with_ll: bool = True,
           with_u: bool = True) -> pl.DataFrame:
    """P10/P50/P90 per layer and year (X7's ``run``): 10,000 independent draws, seed 7."""
    from basecast_pipelines.models import peak_forecast as pf

    promised = pf.promised_by_year(inp["bars"], range(inp["vintage"].year, max(years) + 1))
    draws = pf.simulate(
        x.fit, years, base=inp["base"] if with_ll else None, vintage=inp["vintage"], promised=promised,
        ratios=inp["ratios"]["ratio"].to_list(), factor=inp["factor"],
        u_values=inp["u"]["u_mw"].to_list() if with_u else None, resid_sd=x.resid_sd, n=N_DRAWS,
    )
    return pf.summarize(draws, years)


def official_cell_rows(official: pl.DataFrame, as_of: date, actual: dict[int, float], final: dict[int, bool],
                       verified: dict[tuple[str, str, int], bool]) -> list[dict[str, Any]]:
    """The official vintages of one as-of date (``peak_forecast.official_asof`` rows) as backtest rows:
    ``p50_mw`` only, ``horizon`` from the as-of date (the cell's), ``vintage_horizon`` from the vintage's own date.
    ``verified``: (product, vintage, target year) -> False for a figure not checked at its source."""
    from basecast_pipelines.models import backtest as bt

    rows = []
    for r in official.iter_rows(named=True):
        y = r["target_year"]
        rows.append(cell_row(
            as_of=as_of, target_year=y, horizon=bt.summers_ahead(as_of, y), era=era_of(as_of),
            actual_mw=actual[y], actual_final=final[y], source=r["product"], product=r["product"],
            vintage=r["vintage"], vintage_date=r["vintage_date"], p50_mw=r["forecast_mw"],
            error_pct=100 * (r["forecast_mw"] / actual[y] - 1),
            verified=verified.get((r["product"], r["vintage"], y), True), series=r["series"],
            vintage_horizon=bt.summers_ahead(r["vintage_date"], y),
        ))
    return rows


def peak_backtest_cells(ctx: MartContext) -> pl.DataFrame:
    return ctx.cached("backtest.peak_cells", lambda: _build_peak_cells(ctx))


def _build_peak_cells(ctx: MartContext) -> pl.DataFrame:
    from basecast_pipelines.models import backtest as bt
    from basecast_pipelines.models import peak_forecast as pf

    x = x7_data(ctx)
    actuals = summer_actuals(ctx)
    actual = dict(zip(actuals["year"].to_list(), actuals["hourly_peak_mw"].to_list(), strict=True))
    final = dict(zip(actuals["year"].to_list(), actuals["final"].to_list(), strict=True))
    official = official_base(ctx)
    verified = prelim_verified(official_forecasts(ctx), manual_verified(ctx))
    last_actual = min(last_summer(ctx.as_of), max(actual))

    # What each layer turned out to be: today's factor x today's reading of A2E at that summer's peak month
    now = x7_inputs_asof(x, ctx.as_of)
    pts_now = list(zip(now["points"]["month_idx"].to_list(), now["points"]["a2e_mw"].to_list(), strict=True))

    def realized_ll(year: int) -> float:
        return now["factor"] * pf.interpolate(pts_now, pf.month_end_index(x.peak_months[year]))

    rows = []
    for as_of in PEAK_AS_OF_DATES:
        if as_of > ctx.as_of:
            continue
        years = [y for y in range(last_summer(as_of) + 1, last_actual + 1) if bt.summers_ahead(as_of, y) >= 1]
        if not years:
            continue
        cell = {"as_of": as_of, "era": era_of(as_of)}
        target = {y: actual[y] for y in years}
        inp = x7_inputs_asof(x, as_of)
        summ = x7_run(x, inp, years)
        for r in pf.score(summ.filter(pl.col("layer") == "total"), target).iter_rows(named=True):
            y = r["year"]
            layers = summ.filter(pl.col("year") == y)
            p50 = dict(zip(layers["layer"].to_list(), layers["p50_mw"].to_list(), strict=True))
            rows.append(cell_row(
                **cell, target_year=y, horizon=bt.summers_ahead(as_of, y), actual_mw=r["actual_mw"],
                actual_final=final[y], source=BASECAST, variant=VARIANT, p10_mw=r["p10_mw"], p50_mw=r["p50_mw"],
                p90_mw=r["p90_mw"], error_pct=r["error_pct"], in_band=r["in_p10_p90"], organic_p50=p50["organic"],
                ll_p50=p50["large_load"], u_p50=p50["unattributed"], ll_realized=realized_ll(y),
                u_realized=r["actual_mw"] - p50["organic"] - realized_ll(y),
                leak_note="; ".join(inp["leaks"]) or None, verified=False, deck_vintage=inp["vintage"],
                factor=inp["factor"],
            ))
        # the ablation: the organic layer alone (no deck value feeds it)
        s_org = x7_run(x, inp, years, with_ll=False, with_u=False)
        for r in pf.score(s_org.filter(pl.col("layer") == "total"), target).iter_rows(named=True):
            y = r["year"]
            rows.append(cell_row(
                **cell, target_year=y, horizon=bt.summers_ahead(as_of, y), actual_mw=r["actual_mw"],
                actual_final=final[y], source=ORGANIC_ONLY, p10_mw=r["p10_mw"], p50_mw=r["p50_mw"],
                p90_mw=r["p90_mw"], error_pct=r["error_pct"], in_band=r["in_p10_p90"], organic_p50=r["p50_mw"],
                verified=True,
            ))
        off = pf.official_asof(official, as_of).filter(pl.col("target_year").is_in(years))
        rows += official_cell_rows(off, as_of, actual, final, verified)
    return pl.DataFrame(rows, schema=CELL_SCHEMA).sort("as_of", "target_year", "source", "vintage", nulls_last=True)


def prelim_verified(raw: pl.DataFrame, figures: dict[str, bool]) -> dict[tuple[str, str, int], bool]:
    """(``LTLF-prelim``, vintage, target year) -> the manual figure's ``verified`` for the preliminary LTLF's summer
    base figures (the rows ``base_series`` turns into ``LTLF-prelim``)."""
    from basecast_pipelines.models import backtest as bt

    rows = raw.filter((pl.col("metric") == "peak_demand") & (pl.col("season") == "summer")
                      & (pl.col("scenario") == "base"))
    return {(bt.PRELIM, r["vintage"], r["target_year"]): figures.get(r["row_label"], False)
            for r in rows.iter_rows(named=True)}


def paired(cells: pl.DataFrame, official: str) -> pl.DataFrame:
    """basecast and one official source on the same (as_of, target_year): ``error_pct`` and ``official_pct``, as
    the API's comparison (and X7's paired table) pairs them."""
    ours = cells.filter(pl.col("source") == BASECAST).select("as_of", "target_year", "error_pct")
    theirs = cells.filter(pl.col("source") == official).select(
        "as_of", "target_year", pl.col("error_pct").alias("official_pct"))
    return ours.join(theirs, on=["as_of", "target_year"], how="inner")


def mape(values: pl.Series) -> float | None:
    return values.abs().mean()


def _source(cells: pl.DataFrame, source: str) -> pl.DataFrame:
    return cells.filter(pl.col("source") == source)


def _cell(cells: pl.DataFrame, as_of: date, year: int, col: str):
    return cells.filter((pl.col("source") == BASECAST) & (pl.col("as_of") == as_of)
                        & (pl.col("target_year") == year))[col].item()


def _as_x7(config) -> bool:
    """X7's switch: the organic model fit through 2019 (R5)."""
    return marts_config.value(config, "forecast.organic_fit_end") == 2019


def _x7_golden(name, get, expected, **kw):
    return value_check(name, get, expected, as_of=GOLDEN_AS_OF, applies=_as_x7, **kw)


PCT = 0.005  # the docs give percentages to two decimals
PEAK_BACKTEST_CHECKS = (
    value_check("one basecast row per cell", lambda f: _source(f, BASECAST).select("as_of", "target_year")
                .is_duplicated().any(), False),
    value_check("unique key", lambda f: f.select("as_of", "target_year", "source", "vintage").is_duplicated().any(),
                False),
    value_check("every row scored", lambda f: f["error_pct"].null_count() + f["actual_mw"].null_count(), 0),
    value_check("basecast rows not verified (machine-read decks)", lambda f: _source(f, BASECAST)["verified"].any(),
                False),
    _x7_golden("18 basecast cells", lambda f: _source(f, BASECAST).height, 18),
    _x7_golden("18 LTLF-paired cells", lambda f: paired(f, "LTLF").height, 18),
    _x7_golden("basecast MAPE on LTLF cells %", lambda f: mape(paired(f, "LTLF")["error_pct"]), 3.30, tol=PCT),
    _x7_golden("LTLF MAPE %", lambda f: mape(paired(f, "LTLF")["official_pct"]), 5.06, tol=PCT),
    _x7_golden("15 CDR-paired cells", lambda f: paired(f, "CDR").height, 15),
    _x7_golden("basecast MAPE on CDR cells %", lambda f: mape(paired(f, "CDR")["error_pct"]), 3.24, tol=PCT),
    _x7_golden("CDR MAPE %", lambda f: mape(paired(f, "CDR")["official_pct"]), 4.81, tol=PCT),
    _x7_golden("organic-only MAPE %", lambda f: mape(_source(f, ORGANIC_ONLY)["error_pct"]), 10.46, tol=PCT),
    _x7_golden("P10-P90 coverage (cells of 18)", lambda f: _source(f, BASECAST)["in_band"].sum(), 10),
    _x7_golden("2026-05-31 cell P50 MW", lambda f: _cell(f, date(2026, 5, 31), 2026, "p50_mw"), 89_037, tol=1),
    _x7_golden("2026-05-31 cell P10 MW", lambda f: _cell(f, date(2026, 5, 31), 2026, "p10_mw"), 86_035, tol=1),
    _x7_golden("2026-05-31 cell P90 MW", lambda f: _cell(f, date(2026, 5, 31), 2026, "p90_mw"), 91_937, tol=1),
)

PEAK_BACKTEST = Mart(
    name="mart_peak_backtest",
    build=peak_backtest_cells,
    key=("as_of", "target_year", "source", "vintage"),
    inputs=("ercot_load_hourly_wz", "weather_hourly_wz", "large_load_chart_values", "large_load_headlines",
            "ercot_monthly_peaks", "official_forecasts", "manual_official_figures"),
    description="X7's pseudo-out-of-sample backtest: at 8 past dates, the peak method rebuilt from what was "
    "published then (P10/P50/P90 and its layers), its organic-only ablation and ERCOT's vintages of the same date, "
    "each scored on the later summers' actual peak; split by era (before / with TSP large loads).",
    caveats=("machine_read_unverified", "band_uncalibrated", "preliminary_actuals"),
    checks=PEAK_BACKTEST_CHECKS,
    meta=lambda frame, ctx: {"eras": list(ERAS)},
)


# --- mart_official_forecast_errors (Q1 §3) ---------------------------------------------------------------------


def official_forecast_errors(ctx: MartContext) -> pl.DataFrame:
    """Q1's ``match_forecasts(base_series(...), summer_peaks(hourly))``: every official vintage × target year."""
    from basecast_pipelines.models import backtest as bt

    actual = bt.summer_peaks(ctx.read_sql(MONTHLY_PEAKS_SQL), metric="peak_hourly_mw")
    errors = bt.match_forecasts(official_base(ctx), actual)
    return errors.select(
        "product", "vintage", "vintage_date", "target_year", pl.col("horizon").cast(pl.Int64), "series",
        "forecast_mw", "actual_mw", "actual_complete",
        pl.col("actual_peak_local").dt.strftime("%Y-%m-%d %H:%M").alias("actual_peak_local"), "error_mw", "error_pct",
    ).sort("product", "vintage_date", "vintage", "target_year")


OFFICIAL_FORECAST_ERRORS = Mart(
    name="mart_official_forecast_errors",
    build=official_forecast_errors,
    key=("product", "vintage", "target_year"),
    inputs=("official_forecasts", "ercot_monthly_peaks"),
    description="Every official summer-peak forecast (LTLF, CDR and the 2026 preliminary LTLF; one base series per "
    "vintage) against the actual hourly summer peak: error in MW and % by horizon (Q1).",
    caveats=("preliminary_actuals",),
    checks=(
        value_check("unique key", lambda f: f.select("product", "vintage", "target_year").is_duplicated().any(),
                    False),
        value_check("354 rows", lambda f: f.height, 354, as_of=GOLDEN_AS_OF),
    ),
)


# --- mart_queue_backtest (X2 §3) --------------------------------------------------------------------------------

QUEUE_REPORT_MONTHS = (date(2022, 6, 1), date(2023, 1, 1), date(2024, 8, 1))  # X2's backtest queues
QUEUE_WINDOW_MONTHS = 24
STRATUM_NAMES = {"gas+other": "gas_other"}  # the contract's stratum codes (get-data schemas/queue.py)
QUEUE_STRATA = ("solar", "storage", "wind", "gas_other")


def queue_backtest_rows(scored: pl.DataFrame, report_month: date) -> pl.DataFrame:
    """One report month's scored queue (``mw_w`` predicted, ``actual`` and ``projected`` MW in the window) ->
    statewide (``all``) and per-stratum totals, error = predicted ÷ actual − 1 (%)."""
    frame = scored.with_columns(pl.col("stratum").replace(STRATUM_NAMES))
    parts = [frame.with_columns(pl.lit("all").alias("stratum")), frame]
    return (
        pl.concat(parts)
        .group_by("stratum")
        .agg(pl.len().cast(pl.Int64).alias("projects"), pl.col("capacity_mw").sum().alias("raw_mw"),
             pl.col("mw_w").sum().alias("pred_mw"), pl.col("actual").sum().alias("actual_mw"),
             pl.col("projected").sum().alias("developer_projected_mw"))
        .with_columns(pl.lit(report_month).alias("report_month"),
                      (100 * (pl.col("pred_mw") / pl.col("actual_mw") - 1)).alias("error_pct"))
        .select("report_month", "stratum", "projects", "raw_mw", "pred_mw", "actual_mw", "developer_projected_mw",
                "error_pct")
    )


def county_rho(scored: pl.DataFrame) -> dict[str, float]:
    """X2's map test: Spearman across counties (unmatched projects form their own group, as in X2) of the
    predicted, raw and developer-projected MW against the MW that reached COD."""
    from scipy.stats import spearmanr

    c = scored.group_by("county_fips").agg(pred=pl.col("mw_w").sum(), actual=pl.col("actual").sum(),
                                           raw=pl.col("capacity_mw").sum(), projected=pl.col("projected").sum())
    a = c["actual"].to_numpy()
    return {f"county_rho_{name}": float(spearmanr(c[col].to_numpy(), a).statistic)
            for name, col in (("adj", "pred"), ("raw", "raw"), ("developer", "projected"))}


def queue_backtest(ctx: MartContext) -> pl.DataFrame:
    from basecast_pipelines.models import queue_adjusted as qa
    from basecast_pipelines.models import survival as sv

    model = marts_config.value(ctx.config, "queue.model")
    events_all = marts_queue.adjusted_queue(ctx).events  # raises for a queue.model other than entry_ia_sm
    cohort = events_all.filter(pl.col("first_seen_month") >= pl.lit(sv.COHORT_START))
    geo = qa.load_geography()
    parts = []
    for rm in QUEUE_REPORT_MONTHS:
        as_of = add_months(rm, 1)
        until = add_months(as_of, QUEUE_WINDOW_MONTHS)
        if until > ctx.as_of:  # the window has not closed yet
            continue
        q = qa.build_queue(qa.load_snapshot(rm), events_all, as_of, qa.STAGE_SETS[model]).filter(
            pl.col("capacity_mw") > 0)
        curves = qa.fit_stage_curves(qa.truncate_at(cohort, as_of), qa.fit_stages(model), weight=marts_queue.WEIGHT)
        scored = qa.score(q, curves, {"w": qa.months_between(until, as_of)}).with_columns(
            actual=qa.actual_cod_mw(until),
            projected=pl.when(pl.col("projected_cod") < pl.lit(until)).then(pl.col("capacity_mw")).otherwise(0.0),
        ).with_columns(county_key=qa.norm_county("county")).join(
            geo.select("county_key", "county_fips"), on="county_key", how="left")
        parts.append(queue_backtest_rows(scored, rm).with_columns(
            pl.lit(QUEUE_WINDOW_MONTHS).alias("window_months"), pl.lit(until).alias("window_end"),
            pl.lit(f"{model}|mw").alias("model_variant"), **{k: pl.lit(v) for k, v in county_rho(scored).items()}))
    return pl.concat(parts).select(
        "report_month", "stratum", "window_months", "window_end", "projects", "raw_mw", "pred_mw", "actual_mw",
        "developer_projected_mw", "error_pct", "model_variant", "county_rho_adj", "county_rho_raw",
        "county_rho_developer",
    ).sort("report_month", "stratum")


def _queue_value(month: date, col: str, stratum: str = "all"):
    return lambda f: f.filter((pl.col("report_month") == month) & (pl.col("stratum") == stratum))[col].item()


def _as_x2(config) -> bool:
    return marts_config.value(config, "queue.model") == "entry_ia_sm"


def _x2_golden(name, get, expected, **kw):
    return value_check(name, get, expected, as_of=GOLDEN_AS_OF, applies=_as_x2, **kw)


QUEUE_BACKTEST = Mart(
    name="mart_queue_backtest",
    build=queue_backtest,
    key=("report_month", "stratum"),
    inputs=("gis_project_events", "gis_snapshots", "tx_counties", "county_weather_zone"),
    description="The adjusted generation queue re-run on past GIS reports (X2 §3): MW predicted to reach COD in the "
    "next 24 months vs what did, the raw queue and the developers' own dates, statewide and by fuel, with the "
    "county Spearman of each ranking.",
    checks=(
        value_check("unique key", lambda f: f.select("report_month", "stratum").is_duplicated().any(), False),
        value_check("strata per date", lambda f: f.group_by("report_month").agg(pl.col("stratum").sort())
                    ["stratum"].to_list() == [sorted(("all", *QUEUE_STRATA))] * f["report_month"].n_unique(), True),
        _x2_golden("3 report months", lambda f: f["report_month"].n_unique(), 3),
        *(_x2_golden(f"{m:%Y-%m} statewide error %", _queue_value(m, "error_pct"), e, tol=0.05)
          for m, e in zip(QUEUE_REPORT_MONTHS, (-13.2, 9.3, 1.4), strict=True)),
        *(_x2_golden(f"{m:%Y-%m} county Spearman {k}", _queue_value(m, f"county_rho_{k}"), e, tol=0.005)
          for k, values in (("adj", (0.66, 0.64, 0.64)), ("raw", (0.43, 0.44, 0.45)))
          for m, e in zip(QUEUE_REPORT_MONTHS, values, strict=True)),
    ),
)


# --- mart_backtest_fan ------------------------------------------------------------------------------------------

FAN_KINDS = ("official_preliminary", "official_range", "official", "actual", "model")
FAN_SCHEMA = {
    "target_year": pl.Int64, "kind": pl.String, "label": pl.String, "product": pl.String, "vintage": pl.String,
    "vintage_date": pl.Date, "value_mw": pl.Float64, "low_mw": pl.Float64, "high_mw": pl.Float64,
    "source": pl.String, "method": pl.String, "final": pl.Boolean, "verified": pl.Boolean,
}
RANGE_SCENARIOS = ("range_low", "range_high")


def _fan_row(**values: Any) -> dict[str, Any]:
    return {c: values.get(c) for c in FAN_SCHEMA}


def _official_label(product: str, vintage: str, series: str) -> str:
    if product == "LTLF" and "ercot_adjusted" in series:
        return f"{vintage}, ERCOT-adjusted"
    return vintage


def fan_rows(year: int, raw: pl.DataFrame, base: pl.DataFrame, figures: dict[str, bool],
             actual: dict[str, Any] | None, cells: pl.DataFrame) -> pl.DataFrame:
    """The marks of summer ``year``'s fan.

    ``raw``: ``official_forecasts`` rows (the manual preliminary figure and range are ``peak_demand`` summer rows,
    scenarios ``base`` / ``range_low`` / ``range_high``; LTLF's ``tsp_provided`` line is shown next to its adjusted
    base). ``base``: Q1's ``base_series``. ``figures``: manual figure id -> verified. ``actual``: the summer's
    ``mart_actual_summer_peaks`` row. ``cells``: ``mart_peak_backtest`` (its ``basecast`` cells for ``year``)."""
    from basecast_pipelines.models import backtest as bt

    rows = []
    manual = raw.filter((pl.col("metric") == "peak_demand") & (pl.col("season") == "summer")
                        & (pl.col("target_year") == year))
    for r in manual.filter(pl.col("scenario") == "base").sort("vintage_date").iter_rows(named=True):
        rows.append(_fan_row(kind="official_preliminary", label=r["vintage"], product=bt.PRELIM,
                             vintage=r["vintage"], vintage_date=r["vintage_date"], value_mw=r["value"],
                             source="official_forecasts", method="manual", final=True,
                             verified=figures.get(r["row_label"], False)))
    ranges = manual.filter(pl.col("scenario").is_in(RANGE_SCENARIOS))
    for (vintage, vdate), g in ranges.group_by("vintage", "vintage_date", maintain_order=True):
        ends = dict(zip(g["scenario"].to_list(), g["value"].to_list(), strict=True))
        rows.append(_fan_row(kind="official_range", label=f"ERCOT's projected range ({vintage})", vintage=vintage,
                             vintage_date=vdate, low_mw=ends.get("range_low"), high_mw=ends.get("range_high"),
                             source="official_forecasts", method="manual", final=True,
                             verified=all(figures.get(label, False) for label in g["row_label"].to_list())))
    tsp = raw.filter((pl.col("product") == "LTLF") & (pl.col("metric") == "peak_mw")
                     & (pl.col("region_type") == "ercot") & (pl.col("season") == "summer")
                     & (pl.col("scenario") == "tsp_provided") & (pl.col("target_year") == year)).select(
        pl.lit("LTLF").alias("product"), "vintage", "vintage_date", pl.col("value").alias("forecast_mw"),
        pl.lit("summer peak_mw tsp_provided").alias("series"))
    official = pl.concat([
        base.filter(pl.col("product").is_in(["LTLF", "CDR"]) & (pl.col("target_year") == year))
        .select("product", "vintage", "vintage_date", "forecast_mw", "series"),
        tsp.with_columns(pl.col("vintage_date").cast(pl.Date)),
    ], how="vertical_relaxed").sort("vintage_date", "product", "series")
    for r in official.iter_rows(named=True):
        label = (f"{r['vintage']}, TSP-provided" if "tsp_provided" in r["series"]
                 else _official_label(r["product"], r["vintage"], r["series"]))
        rows.append(_fan_row(kind="official", label=label, product=r["product"], vintage=r["vintage"],
                             vintage_date=r["vintage_date"], value_mw=r["forecast_mw"], source="official_forecasts",
                             method="file", final=True, verified=True))
    if actual is not None:
        rows.append(_fan_row(kind="actual", label="Actual summer peak" + ("" if actual["final"] else " (preliminary)"),
                             value_mw=actual["hourly_peak_mw"], source="mart_actual_summer_peaks", method="file",
                             final=actual["final"], verified=True))
    model = cells.filter((pl.col("source") == BASECAST) & (pl.col("target_year") == year)).sort("as_of")
    for r in model.iter_rows(named=True):
        rows.append(_fan_row(kind="model", label=f"basecast as of {r['as_of']}", vintage=r["as_of"].isoformat(),
                             vintage_date=r["as_of"], value_mw=r["p50_mw"], low_mw=r["p10_mw"], high_mw=r["p90_mw"],
                             source="mart_peak_backtest", method="model", final=True, verified=r["verified"]))
    return pl.DataFrame([{**r, "target_year": year} for r in rows], schema=FAN_SCHEMA)


def backtest_fan(ctx: MartContext) -> pl.DataFrame:
    cells = peak_backtest_cells(ctx)
    year = int(cells["target_year"].max())
    actuals = summer_actuals(ctx).filter(pl.col("year") == year)
    return fan_rows(year, official_forecasts(ctx), official_base(ctx), manual_verified(ctx),
                    actuals.row(0, named=True) if actuals.height else None, cells)


def _fan(kind: str, col: str):
    return lambda f: f.filter(pl.col("kind") == kind)[col].item()


BACKTEST_FAN = Mart(
    name="mart_backtest_fan",
    build=backtest_fan,
    key=("target_year", "kind", "label"),
    inputs=("official_forecasts", "manual_official_figures", "mart_actual_summer_peaks", "mart_peak_backtest"),
    description="The latest backtested summer's fan: ERCOT's preliminary forecast and its own projected range "
    "(manual figures), every LTLF/CDR vintage targeting that summer, the actual peak and the backtest's P50 "
    "[P10-P90] at each as-of date.",
    caveats=("machine_read_unverified", "band_uncalibrated", "preliminary_actuals"),
    checks=(
        value_check("unique key", lambda f: f.select("target_year", "kind", "label").is_duplicated().any(), False),
        value_check("known kinds", lambda f: set(f["kind"]) <= set(FAN_KINDS), True),
        value_check("a range has low/high and no value", lambda f: f.filter(
            (pl.col("kind") == "official_range")
            & (pl.col("value_mw").is_not_null() | pl.col("low_mw").is_null() | pl.col("high_mw").is_null())).height,
            0),
        value_check("one actual", lambda f: f.filter(pl.col("kind") == "actual").height, 1),
        value_check("fan of 2026", lambda f: f["target_year"].unique().to_list(), [2026], as_of=GOLDEN_AS_OF),
        value_check("preliminary 112,000 MW", _fan("official_preliminary", "value_mw"), 112_000, as_of=GOLDEN_AS_OF),
        value_check("range low 90,500 MW", _fan("official_range", "low_mw"), 90_500, as_of=GOLDEN_AS_OF),
        value_check("range high 98,000 MW", _fan("official_range", "high_mw"), 98_000, as_of=GOLDEN_AS_OF),
        value_check("actual 91,134 MW", _fan("actual", "value_mw"), 91_134, as_of=GOLDEN_AS_OF, tol=1),
        value_check("actual not final", _fan("actual", "final"), False, as_of=GOLDEN_AS_OF),
        value_check("8 model marks", lambda f: f.filter(pl.col("kind") == "model").height, 8, as_of=GOLDEN_AS_OF,
                    applies=_as_x7),
    ),
)

MARTS = (ACTUAL_SUMMER_PEAKS, PEAK_BACKTEST, OFFICIAL_FORECAST_ERRORS, QUEUE_BACKTEST, BACKTEST_FAN)
