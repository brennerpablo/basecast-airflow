"""Forecast marts (A-M5, P0): the summer peak in three layers, ERCOT's official lines and the large-load flow.

- ``mart_peak_forecast``: the chain of ``analysis/x7_peak_forecast.py`` §1–§5 (organic fit ≤ 2019 + large load from
  the decks + the flat unattributed layer, simulated with ``seed = 7``) for the ERCOT system, three variants (R13):
  the last deck before Batch Zero (default), the latest deck and X7's approvals-pace sanity check (P50 only). The
  eight weather zones, for the default variant only, follow ``analysis/x11_large_load_geography.py`` §2–§7 (R16):
  large load up to the deck's approved stock by the stock split, above it by the Batch Zero base + studied split, the
  unattributed layer by the residual excess split; their band is X11's allocation range, not a P10–P90.
- ``mart_official_peak_lines``: ERCOT's latest LTLF (ERCOT-adjusted and TSP-provided, ERCOT and the weather zones it
  publishes) and latest CDR (ERCOT only: it has no zone rows) by target year, from ``official_forecasts``.
- ``mart_large_load_realization`` / ``_in_service`` / ``_monthly``: the chain of ``analysis/q5_large_load.py`` §3–§6
  (one deck per month since May 2023, promised vs approved, the realization ratios) and X1's observed peaks
  (``peak_excess.energized_by_month``), with X11's LZ_WEST / Other split of the approved stock.

Every call gets the run's ``as_of``: decks count when published by then (``report_date``), and the forecast starts at
the first summer after it. The simulation always runs X7's five summers so the draws are X7's own; the rows stop at
``forecast.last_year``. Every deck value is machine-read (``verified = false``). Switches the code cannot honour
(``large_load.mode: scenarios``, ``zone_allocation: x7_fixed``, a zone split for ``approvals_pace``) raise.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Any

import polars as pl

from basecast_pipelines.config import PROJECT_ROOT
from basecast_pipelines.marts import config as marts_config
from basecast_pipelines.marts.core import Check, CheckResult, Mart, MartContext, value_check
from basecast_pipelines.models.weather_load import WEATHER_ZONES as ZONES

if TYPE_CHECKING:
    import numpy as np

GOLDEN_AS_OF = date(2026, 9, 26)  # X7 / X11 / Q5's run date (X7 and X11 re-run after the X16 fixes, decisions.md)

# --- X7 -------------------------------------------------------------------------------------------------------
FIRST, FEATURE = 2003, "t_mean_3d"  # panel start and Q7's weather feature
N_DRAWS, SEED = 10_000, 7
SIM_SUMMERS = 5  # X7 simulates five summers (2027-2031): the draws depend on it, so it stays; rows stop at last_year
ROLL_FIRST = 2011  # organic band: RMSE of the one-year-ahead errors over 2011..2019 (X7 §2)
BATCH_ZERO = date(2026, 4, 1)  # first deck with the >400 GW queue: 2026-04-24
PACE_YEARS = 3  # approvals pace: the last three Decembers (Dec 2022 -> Dec 2025 at the golden as_of)
QUANTILES = (0.1, 0.5, 0.9)

# --- X11 (zone allocation) --------------------------------------------------------------------------------------
EXCESS_FROM = 2023  # zone excess and allocated large load averaged over 2023..last summer (X1, X7, X11)
BZ_DOC = "14-Batch-Zero-Update.pdf"  # "Base & studied load by region, MW" (Board 2026-09-14), p. 7
MAY_DOC = "May-21-LLWG-Report.pptx"  # LLIS by weather zone (p. 17) and by load zone (p. 15): f for NORTH in LZ_WEST
JUN_DOC = "June-19-LLWG-Report.pptx"  # Batch Zero tracking by weather zone (p. 17)
QSA_DOC = "8-Interconnection-and-Grid-Analysis-Update.pdf"  # Q4 2026 QSA large loads by weather zone (p. 7)
TSP_RFI_YEAR = 2030  # the TSP RFI candidate split (58777 item 38)

# --- Q5 ---------------------------------------------------------------------------------------------------------
REALIZATION_FROM = 2024  # Q5's first target year
MIN_HORIZON_MONTHS = 6  # the ratio band uses horizons of 6 months or more
IN_SERVICE_STATUSES = {  # in_service_wide column -> contract status (the A2E buckets are summed in ``a2e``)
    "a2e": "approved_to_energize",
    "planning_studies_approved": "planning_studies_approved",
    "under_ercot_review": "under_ercot_review",
    "no_studies_submitted": "no_studies_submitted",
}

# X6 note 7: the January and February 2026 TAC decks print "January 2025" / "February 2025" for the observed peaks of
# January and February 2026 (non-simultaneous 3,977 / 3,998 MW, simultaneous 3,765 / 3,846 MW), so
# ``large_load_headlines`` stores ``as_of_date`` a year early. The parser fix (BUILD_A A-M5) is not done; until it is,
# this mart re-dates exactly these rows, keyed by (report_date, as_of_date) of the observed-peak metrics. Evidence:
# the chart series (a later deck's reading) has 3,765 MW simultaneous for January 2026, the headline's own value.
# Every other row the parser flags ``as_of_suspect`` is dropped, not re-dated: the March 2026 TAC PDF's "March 2025"
# (3,883 / 3,801 MW) matches neither March 2025 (3,315 / 3,240) nor March 2026 (4,008 / 3,701), so a date would be
# a guess.
HEADLINE_REDATES = {
    (date(2026, 1, 21), date(2025, 1, 1)): date(2026, 1, 1),
    (date(2026, 2, 19), date(2025, 2, 1)): date(2026, 2, 1),
}
OBSERVED_METRICS = {"observed_peak_simultaneous_mw": "simultaneous",
                    "observed_peak_non_simultaneous_mw": "nonsimultaneous"}

# --- labels -----------------------------------------------------------------------------------------------------
VARIANTS = {  # variant -> (label, has_band)
    "deck_pre_batch_zero": ("Before Batch Zero", True),
    "deck_latest": ("Latest deck", True),
    "approvals_pace": ("Approvals pace", False),
}
RATIO_DEFINITION = (
    "Incremental realization ratio: (approved-to-energize stock in December of the target year − the deck's own "
    "approved stock) ÷ (MW the deck promised in service by that December − its approved stock), over every (deck, "
    "target year) pair published and realized by the as-of date with a horizon of 6 months or more (Q5, X7). One "
    "ratio is drawn per simulation and applied to every year."
)
BAND_BASIS = {
    "rolling_rmse": "Organic: coefficient uncertainty plus a residual with the RMSE of the one-year-ahead errors "
    "at median weather over 2011-2019 (X7 §2).",
    "ratio_draws": "Large load: one realization ratio resampled per draw from the known (deck, year) ratios.",
    "u_draws": "Unattributed: resampled from the flat excess of the summers since 2022.",
    "independent_layers": "Total: the three layers drawn independently and summed (no correlation).",
    "in_sample_weather": "Zone organic: coefficient uncertainty, weather resampled from 2003-2019 and the in-sample "
    "residual SD (X7 §1): narrower than the ERCOT organic band.",
    "allocation_variants": "Zone large load, unattributed and total: low and high P50 when the stock split or the "
    "pipeline split is swapped for each candidate split (X11 §2, §4); not a P10-P90.",
}


# --- switches ---------------------------------------------------------------------------------------------------


def _check_switches(config: Mapping[str, Any]) -> None:
    from basecast_pipelines.models import peak_forecast as pf

    if marts_config.value(config, "large_load.mode") != "estimated":
        raise NotImplementedError("large_load.mode: scenarios (R3's fallback) is not built")
    if marts_config.value(config, "large_load.zone_allocation") != "x11":
        raise NotImplementedError("large_load.zone_allocation: x7_fixed is not built (R16 default: x11)")
    if marts_config.value(config, "forecast.unattributed_layer") != "separate":
        raise NotImplementedError("forecast.unattributed_layer: only 'separate' is built (R9)")
    if int(marts_config.value(config, "forecast.organic_fit_end")) != pf.PRE_BREAK_UNTIL:
        raise NotImplementedError(f"forecast.organic_fit_end: only {pf.PRE_BREAK_UNTIL} is built (R5)")
    if marts_config.value(config, "forecast.default_variant") == "approvals_pace":
        raise NotImplementedError("forecast.default_variant: approvals_pace has no draws to split by zone")


def last_summer(as_of: date) -> int:
    """The latest summer known on ``as_of``: its own year from September on (X7: 2026 peaked Jul 22)."""
    return as_of.year if as_of.month >= 9 else as_of.year - 1


# --- shared inputs (one read per run) ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Decks:
    """The large-load decks published by ``as_of`` and what X7 and Q5 derive from them."""

    cv: pl.DataFrame  # large_load_chart_values
    headlines: pl.DataFrame  # large_load_headlines
    wide: pl.DataFrame  # ll.in_service_wide: one row per in-service chart x year
    vintages: pl.DataFrame  # ll.pick_vintages: one deck per calendar month
    bars: pl.DataFrame  # the picked decks' in-service bars with their own approved stock (``base_a2e_mw``)
    realized: pl.DataFrame  # pf.realized_year_end: the December stocks and when they became known


def deck_inputs(ctx: MartContext) -> Decks:
    return ctx.cached("forecast.decks", lambda: _decks(ctx))


def _decks(ctx: MartContext) -> Decks:
    from basecast_pipelines.models import large_load as ll
    from basecast_pipelines.models import peak_forecast as pf

    cv = pf.published_by(ll.load_chart_values(), ctx.as_of)
    headlines = pf.published_by(ll.load_headlines(), ctx.as_of)
    # doc_type (status deck vs board reprint) steers pick_vintages; it lives in the lake's raw manifests, as in X7/Q5
    manifests = ll.load_manifests(PROJECT_ROOT / "data")
    if manifests.is_empty():
        raise FileNotFoundError(f"no manifests under data/raw/source={ll.SOURCE_ID}: pick_vintages needs doc_type")
    wide = ll.in_service_wide(cv)
    vintages = ll.pick_vintages(ll.check_headlines(ll.chart_checks(wide), headlines), manifests)
    bars = wide.join(
        vintages.select("vintage", "document", "page", "chart_title", pl.col("a2e_stock_mw").alias("base_a2e_mw")),
        on=["vintage", "document", "page", "chart_title"], how="inner",
    )
    return Decks(cv, headlines, wide, vintages, bars, pf.realized_year_end(vintages))


@dataclass(frozen=True)
class Organic:
    """X7 §1–§2 and the zone panel of X7 §6 / X11 §6."""

    ercot: pl.DataFrame  # ERCOT summers: year, peak_mw, peak_local, t_mean_3d
    peak_months: dict[int, str]  # year -> "YYYY-MM" of the ERCOT summer peak
    fit: Any  # pf.OrganicFit on the pre-break summers
    excess: dict[int, float]  # post-break summers: actual - organic at actual weather
    resid_sd: float  # RMSE of the one-year-ahead errors 2011-2019
    cpanel: pl.DataFrame  # zone MW at the ERCOT peak hour + the ERCOT weather feature
    zone_fits: dict[str, Any]  # pf.OrganicFit per zone (coincident contribution)


def organic_inputs(ctx: MartContext) -> Organic:
    return ctx.cached("forecast.organic", _organic)


def _organic() -> Organic:
    import numpy as np

    from basecast_pipelines.models import peak_excess as px
    from basecast_pipelines.models import peak_forecast as pf
    from basecast_pipelines.models import weather_load as wl

    load = wl.load_hourly_load()
    daily_w = wl.weather_daily(wl.load_hourly_weather())
    weights = wl.zone_weights(load, range(FIRST, pf.PRE_BREAK_UNTIL + 1))
    daily_all = pl.concat([daily_w.drop("n_hours"), wl.system_weather_daily(daily_w, weights).drop("n_hours")])
    features = wl.summer_weather_features(daily_all)
    panel = (wl.summer_peaks(load).select("weather_zone", "year", "peak_mw", "peak_local")
             .join(features, on=["weather_zone", "year"], how="inner").filter(pl.col("year") >= FIRST))
    ercot = panel.filter(pl.col("weather_zone") == wl.TOTAL).sort("year")
    peak_months = {r["year"]: r["peak_local"].strftime("%Y-%m") for r in ercot.iter_rows(named=True)}
    x_actual = dict(zip(ercot["year"].to_list(), ercot[FEATURE].to_list(), strict=True))
    load_peak = dict(zip(ercot["year"].to_list(), ercot["peak_mw"].to_list(), strict=True))
    fit = pf.fit_organic(ercot, FEATURE)
    excess = {y: load_peak[y] - fit.mean(y, x_actual[y]) for y in load_peak if y > pf.PRE_BREAK_UNTIL}
    roll = pf.rolling_origin_errors(ercot, FEATURE, ROLL_FIRST, pf.PRE_BREAK_UNTIL)
    resid_sd = float(np.sqrt((roll["error_mw"] ** 2).mean()))
    cpanel = (px.coincident_zone_loads(load).join(ercot.select("year", FEATURE), on="year")
              .filter(pl.col("year") >= FIRST))
    zone_fits = {z: pf.fit_organic(cpanel.filter(pl.col("weather_zone") == z).rename({"coincident_mw": "peak_mw"}),
                                   FEATURE) for z in ZONES}
    return Organic(ercot, peak_months, fit, excess, resid_sd, cpanel, zone_fits)


def variant_inputs(decks: Decks, org: Organic, as_of: date, *, vintage_before: date | None = None) -> dict[str, Any]:
    """X7's ``inputs_asof``: every input as it could be known on ``as_of``; ``vintage_before`` picks the latest deck
    published before that date (the pre-Batch-Zero variant). Without X7's backtest fallback: a run date with no
    estimable factor or ratio raises."""
    from basecast_pipelines.models import peak_forecast as pf

    last = last_summer(as_of)
    points = pf.a2e_points(decks.cv, decks.vintages, decks.bars, as_of)
    months = {y: m for y, m in org.peak_months.items() if y <= last}
    fac = pf.observed_factor(decks.cv, points, months, as_of)
    ratios = pf.incremental_ratios(decks.bars, decks.realized, as_of)
    if fac.is_empty() or ratios.is_empty():
        raise ValueError(f"no observed factor or realization ratio is estimable as of {as_of}")
    factor = float(fac["factor"].mean())
    u = pf.unattributed({y: v for y, v in org.excess.items() if y <= last}, points, months, factor)
    decks_by = pf.published_by(decks.vintages, as_of).drop_nulls("a2e_stock_mw")
    if vintage_before is not None:
        decks_by = decks_by.filter(pl.col("vintage") < vintage_before)
    v = decks_by.sort("vintage").row(-1, named=True)
    vbars = decks.bars.filter((pl.col("vintage") == v["vintage"]) & (pl.col("document") == v["document"]))
    return {"points": points, "factor": factor, "factor_rows": fac, "ratios": ratios, "u": u,
            "vintage": v["vintage"], "report_date": v["report_date"], "base": v["a2e_stock_mw"], "bars": vbars}


def simulate_variant(org: Organic, inp: Mapping[str, Any], years: Sequence[int]) -> dict[str, np.ndarray]:
    """X7's ``run``: the layer draws of one deck variant (seed 7)."""
    from basecast_pipelines.models import peak_forecast as pf

    promised = pf.promised_by_year(inp["bars"], range(inp["vintage"].year, max(years) + 1))
    return pf.simulate(org.fit, years, base=inp["base"], vintage=inp["vintage"], promised=promised,
                       ratios=inp["ratios"]["ratio"].to_list(), factor=inp["factor"],
                       u_values=inp["u"]["u_mw"].to_list(), resid_sd=org.resid_sd, n=N_DRAWS, seed=SEED)


# --- pure helpers (tested in tests/marts/test_forecast.py) ------------------------------------------------------


def forecast_years(as_of: date, last_year: int) -> tuple[list[int], list[int]]:
    """``(simulated, published)`` target years: X7's five summers from the first one after ``as_of`` (or through
    ``last_year`` if later), and the ones the mart keeps (through ``last_year``)."""
    first = last_summer(as_of) + 1
    simulated = list(range(first, max(first + SIM_SUMMERS, last_year + 1)))
    return simulated, [y for y in simulated if y <= last_year]


def pace_a2e(a2e_start: float, start_idx: float, pace_mw_yr: float, years: Sequence[int]) -> dict[int, float]:
    """X7's approvals-pace sanity check: the approved stock at each summer point if approvals keep ``pace_mw_yr``
    from ``a2e_start`` at month index ``start_idx`` (``peak_forecast.month_index`` units)."""
    from basecast_pipelines.models import peak_forecast as pf

    return {y: a2e_start + pace_mw_yr * (pf.summer_index(y) - start_idx) / 12 for y in years}


def lz_split_table(headline: pl.DataFrame, chart: pl.DataFrame) -> pl.DataFrame:
    """X11 §2: LZ_WEST vs Other of the approved stock by deck (``report_date``, ``lz_west``, ``other``, ``src``):
    the regex-read headlines first, the Gemini-read charts only for decks after the last headline; plus
    ``lz_west_share``."""
    chart_after = chart
    if not headline.is_empty():
        chart_after = chart.filter(~pl.col("report_date").is_in(headline["report_date"].implode())
                                   & (pl.col("report_date") > headline["report_date"].max()))
    cols = ["report_date", "lz_west", "other", "src"]
    return (pl.concat([headline.select(cols), chart_after.select(cols)], how="vertical_relaxed")
            .with_columns((pl.col("lz_west") / (pl.col("lz_west") + pl.col("other"))).alias("lz_west_share"))
            .sort("report_date"))


def lz_share_at(lzs: pl.DataFrame, d: date) -> float:
    """LZ_WEST share of the approved stock in the latest deck at or before ``d`` (the first deck if none), X11."""
    before = lzs.filter(pl.col("report_date") <= d)
    return float((before if not before.is_empty() else lzs.head(1))["lz_west_share"][-1])


def quantile_row(arr: np.ndarray) -> tuple[float, float, float]:
    import numpy as np

    q = np.quantile(arr, QUANTILES)
    return float(q[0]), float(q[1]), float(q[2])


def zone_layer_rows(
    org: Mapping[str, np.ndarray], ll_draws: np.ndarray, u_draws: np.ndarray, *, stock_ll: float,
    stock: Mapping[str, float], pipeline: Mapping[str, float], unattributed: Mapping[str, float],
    variants: Sequence[tuple[Mapping[str, float], Mapping[str, float]]], years: Sequence[int],
    keep: Sequence[int],
) -> list[dict[str, Any]]:
    """X11 §7 per zone and kept year: the organic layer with its P10-P90 (zone draws), and large load, unattributed
    and total at the central splits with the allocation range (min / max P50 over ``variants``, each a
    ``(stock split, pipeline split)`` pair) in ``p10_mw`` / ``p90_mw``. ``org``: zone -> ``n x years`` draws;
    ``ll_draws``, ``u_draws``: the statewide layers. ``share_of_ll_u``: the zone's mean LL + U over the statewide
    mean that year."""
    import numpy as np

    from basecast_pipelines.models import large_load_geo as geo

    central = geo.split_large_load(ll_draws, stock_ll, stock, pipeline)
    swapped = [geo.split_large_load(ll_draws, stock_ll, s, p) for s, p in variants]
    statewide = (ll_draws + u_draws).mean(axis=0)
    rows = []
    for z in ZONES:
        u_z = unattributed.get(z, 0.0) * u_draws
        for j, y in enumerate(years):
            if y not in keep:
                continue
            ll_p50 = [float(np.median(v[z][:, j])) for v in swapped]
            tot_p50 = [float(np.median(org[z][:, j] + v[z][:, j] + u_z[:, j])) for v in swapped]
            u_p50 = float(np.median(u_z[:, j]))
            share = float((central[z][:, j] + u_z[:, j]).mean() / statewide[j]) if statewide[j] else None
            base = {"region_id": z, "target_year": int(y), "share_of_ll_u": share}
            rows += [
                {**base, "layer": "organic", **dict(zip(("p10_mw", "p50_mw", "p90_mw"), quantile_row(org[z][:, j]))),
                 "band_kind": "p10_p90", "band_basis": "in_sample_weather"},
                {**base, "layer": "large_load", "p10_mw": min(ll_p50), "p50_mw": float(np.median(central[z][:, j])),
                 "p90_mw": max(ll_p50), "band_kind": "allocation_range", "band_basis": "allocation_variants"},
                {**base, "layer": "unattributed", "p10_mw": u_p50, "p50_mw": u_p50, "p90_mw": u_p50,
                 "band_kind": "allocation_range", "band_basis": "allocation_variants"},
                {**base, "layer": "total", "p10_mw": min(tot_p50),
                 "p50_mw": float(np.median(org[z][:, j] + central[z][:, j] + u_z[:, j])), "p90_mw": max(tot_p50),
                 "band_kind": "allocation_range", "band_basis": "allocation_variants"},
            ]
    return rows


def redate_headlines(headlines: pl.DataFrame) -> pl.DataFrame:
    """The observed-peak headline sentences with the known misdating fixed (``HEADLINE_REDATES``, X6 note 7) and
    every other ``as_of_suspect`` row dropped; ``redated`` marks the fixed rows."""
    fixes = pl.DataFrame(
        [{"report_date": r, "as_of_date": a, "_fixed": to} for (r, a), to in HEADLINE_REDATES.items()],
        schema={"report_date": pl.Date, "as_of_date": pl.Date, "_fixed": pl.Date},
    )
    rows = headlines.filter(pl.col("metric").is_in(list(OBSERVED_METRICS)) & pl.col("as_of_date").is_not_null())
    out = rows.join(fixes, on=["report_date", "as_of_date"], how="left").with_columns(
        pl.col("_fixed").is_not_null().alias("redated"),
        pl.coalesce("_fixed", "as_of_date").alias("as_of_date"),
    )
    return out.filter(pl.col("redated") | ~pl.col("as_of_suspect").fill_null(False)).drop("_fixed")


def headline_observed_by_month(headlines: pl.DataFrame) -> pl.DataFrame:
    """Observed monthly peaks stated in the decks' sentences (regex-read), after :func:`redate_headlines`: one row
    per month with the latest deck's ``headline_simultaneous_mw`` / ``headline_nonsimultaneous_mw``."""
    rows = (redate_headlines(headlines).filter(pl.col("peak_basis") == "monthly")
            .with_columns(pl.col("as_of_date").dt.truncate("1mo").alias("month"),
                          pl.col("metric").replace_strict(OBSERVED_METRICS).alias("basis"),
                          pl.coalesce("member", "file_name").alias("_doc"))
            .sort("report_date", "_doc"))
    latest = rows.group_by("month", "basis").agg(pl.col("value").last(), pl.col("report_date").last(),
                                                 pl.col("redated").last())
    wide = latest.pivot(on="basis", index="month", values="value")
    for basis in OBSERVED_METRICS.values():
        if basis not in wide.columns:
            wide = wide.with_columns(pl.lit(None, dtype=pl.Float64).alias(basis))
    meta = latest.group_by("month").agg(pl.col("report_date").max().alias("headline_report_date"),
                                        pl.col("redated").any().alias("headline_redated"))
    return (wide.join(meta, on="month")
            .select("month", pl.col("simultaneous").alias("headline_simultaneous_mw"),
                    pl.col("nonsimultaneous").alias("headline_nonsimultaneous_mw"), "headline_report_date",
                    "headline_redated")
            .sort("month"))


def month_spine(first: date, last: date) -> pl.DataFrame:
    """Every first-of-month from ``first`` to ``last`` (a month without a reading stays a row of nulls)."""
    return pl.DataFrame({"month": pl.date_range(first.replace(day=1), last.replace(day=1), "1mo", eager=True)})


def in_service_long(bars: pl.DataFrame) -> pl.DataFrame:
    """In-service bars (``in_service_wide`` rows of the picked decks) -> one row per deck, year and status with MW
    (cumulative, by the end of the year) and the bar's printed total; a segment the chart does not print is left out,
    not set to 0."""
    return (
        bars.unpivot(index=["vintage", "report_date", "document", "page", "year", "total_mw"],
                     on=list(IN_SERVICE_STATUSES), variable_name="status", value_name="mw")
        .drop_nulls("mw")
        .select(pl.col("vintage").alias("deck_vintage"), "report_date", pl.col("year").cast(pl.Int32).alias(
            "in_service_year"), pl.col("status").replace_strict(IN_SERVICE_STATUSES), "mw", "total_mw", "document",
            "page", pl.lit(False).alias("verified"))
        .sort("deck_vintage", "in_service_year", "status")
    )


def official_lines(frame: pl.DataFrame, as_of: date, years: Sequence[int]) -> pl.DataFrame:
    """ERCOT's latest official summer peak forecasts published by ``as_of``: per LTLF series (``ercot_adjusted``,
    ``tsp_provided``) the latest vintage, with every region it has (ERCOT and weather zones); the latest CDR at the
    ERCOT level (a same-date revision wins, as in ``backtest``). ``frame``: ``official_forecasts`` rows (summer
    ``peak_mw``)."""
    rows = frame.filter((pl.col("vintage_date") <= as_of) & pl.col("target_year").is_in(list(years)))
    parts = []
    for series in ("ercot_adjusted", "tsp_provided"):
        s = rows.filter((pl.col("product") == "LTLF") & (pl.col("scenario") == series))
        ercot = s.filter(pl.col("region_type") == "ercot")
        if ercot.is_empty():
            continue
        vintage = ercot.sort("vintage_date").row(-1, named=True)["vintage"]
        label = "ERCOT-adjusted" if series == "ercot_adjusted" else "TSP-provided"
        parts.append(s.filter(pl.col("vintage") == vintage).with_columns(
            pl.lit(series).alias("series"), (pl.col("vintage") + pl.lit(f" ({label})")).alias("label")))
    cdr = rows.filter((pl.col("product") == "CDR") & (pl.col("region_type") == "ercot") & pl.col("scenario").is_null())
    if not cdr.is_empty():
        last = cdr["vintage_date"].max()
        parts.append(
            cdr.filter(pl.col("vintage_date") == last)
            .with_columns(pl.col("vintage").str.contains("(?i)revised").alias("_revised"))
            .sort("_revised", "vintage").group_by("region_id", "target_year", maintain_order=True).last()
            .drop("_revised").with_columns(pl.lit("cdr").alias("series"), pl.col("vintage").alias("label")))
    if not parts:
        raise ValueError(f"no LTLF or CDR summer peak published by {as_of}")
    out = pl.concat([p.select("product", "vintage", "vintage_date", "series", "label", "region_type", "region_id",
                              "target_year", "value", "source_file") for p in parts], how="vertical_relaxed")
    return (
        out.with_columns(
            pl.col("vintage").str.strip_prefix(pl.col("product") + pl.lit(" ")),
            pl.when(pl.col("region_type") == "ercot").then(pl.lit("ERCOT")).otherwise(pl.col("region_id"))
            .alias("region_id"),
            pl.col("target_year").cast(pl.Int32),
            pl.col("value").cast(pl.Float64).alias("mw"),
        )
        .drop("value")
        .sort("product", "series", "region_id", "target_year")
    )


# --- mart_peak_forecast -----------------------------------------------------------------------------------------

PEAK_SCHEMA = {
    "run_id": pl.String, "region_type": pl.String, "region_id": pl.String, "target_year": pl.Int32,
    "variant": pl.String, "layer": pl.String, "p10_mw": pl.Float64, "p50_mw": pl.Float64, "p90_mw": pl.Float64,
    "band_kind": pl.String, "band_basis": pl.String, "deck_vintage": pl.Date, "deck_report_date": pl.Date,
    "factor": pl.Float64, "ratio_p10": pl.Float64, "ratio_p50": pl.Float64, "ratio_p90": pl.Float64,
    "n_ratios": pl.Int32, "approved_stock_mw": pl.Float64, "pace_mw_per_year": pl.Float64,
    "share_of_ll_u": pl.Float64, "stock_share": pl.Float64, "pipeline_share": pl.Float64, "u_share": pl.Float64,
    "zone_approved_stock_mw": pl.Float64, "x7_fixed_p50_mw": pl.Float64, "is_default": pl.Boolean,
    "verified": pl.Boolean,
}
LAYER_BASIS = {"organic": "rolling_rmse", "large_load": "ratio_draws", "unattributed": "u_draws",
               "total": "independent_layers"}


def _inputs_columns(inp: Mapping[str, Any]) -> dict[str, Any]:
    import numpy as np

    q = np.quantile(inp["ratios"]["ratio"].to_numpy(), QUANTILES)
    return {"deck_vintage": inp["vintage"], "deck_report_date": inp["report_date"], "factor": inp["factor"],
            "ratio_p10": float(q[0]), "ratio_p50": float(q[1]), "ratio_p90": float(q[2]),
            "n_ratios": inp["ratios"].height, "approved_stock_mw": float(inp["base"])}


def build_peak_forecast(ctx: MartContext) -> pl.DataFrame:
    import numpy as np

    from basecast_pipelines.models import large_load as ll
    from basecast_pipelines.models import peak_forecast as pf

    _check_switches(ctx.config)
    default = marts_config.value(ctx.config, "forecast.default_variant")
    sim_years, keep = forecast_years(ctx.as_of, int(marts_config.value(ctx.config, "forecast.last_year")))
    decks, org = deck_inputs(ctx), organic_inputs(ctx)
    run_id = f"peak_forecast.{ctx.as_of.isoformat()}"
    inputs = {"deck_latest": variant_inputs(decks, org, ctx.as_of),
              "deck_pre_batch_zero": variant_inputs(decks, org, ctx.as_of, vintage_before=BATCH_ZERO)}
    draws = {v: simulate_variant(org, inp, sim_years) for v, inp in inputs.items()}

    rows: list[dict[str, Any]] = []
    for variant, inp in inputs.items():
        cols = _inputs_columns(inp)
        for r in pf.summarize(draws[variant], sim_years).filter(pl.col("year").is_in(keep)).iter_rows(named=True):
            rows.append({"region_type": "ercot", "region_id": "ERCOT", "target_year": r["year"], "variant": variant,
                         "layer": r["layer"], "p10_mw": r["p10_mw"], "p50_mw": r["p50_mw"], "p90_mw": r["p90_mw"],
                         "band_kind": "p10_p90", "band_basis": LAYER_BASIS[r["layer"]], **cols,
                         "verified": r["layer"] == "organic"})

    # Approvals pace (X7 §5, a sanity check, P50 only): the stock keeps the pace of the last three Decembers from the
    # latest monthly reading; organic P50 of the default deck run (the organic draws are the same in every variant)
    # and the median U.
    now = inputs["deck_latest"]
    dec = dict(zip(*decks.realized.filter(pl.col("known_from") <= ctx.as_of)
                   .select("target_year", "realized_a2e_mw").to_dict(as_series=False).values(), strict=True))
    last_dec = max(dec)
    pace = (dec[last_dec] - dec[last_dec - PACE_YEARS]) / PACE_YEARS
    start_month = ll.a2e_by_month(decks.cv).drop_nulls("a2e_mw")["month"].max()
    start_idx = pf.month_end_index(start_month)
    a2e_start = pf.interpolate(list(zip(now["points"]["month_idx"].to_list(), now["points"]["a2e_mw"].to_list(),
                                        strict=True)), start_idx)
    org_p50 = {y: float(np.quantile(draws[default]["organic"][:, j], 0.5)) for j, y in enumerate(sim_years)}
    u_p50 = float(np.median(now["u"]["u_mw"].to_numpy()))
    pace_cols = {"deck_vintage": now["vintage"], "deck_report_date": now["report_date"], "factor": now["factor"],
                 "approved_stock_mw": a2e_start, "pace_mw_per_year": pace}
    for y, a2e in pace_a2e(a2e_start, start_idx, pace, keep).items():
        layers = {"organic": org_p50[y], "large_load": a2e * now["factor"], "unattributed": u_p50}
        layers["total"] = sum(layers.values())
        rows += [{"region_type": "ercot", "region_id": "ERCOT", "target_year": y, "variant": "approvals_pace",
                  "layer": layer, "p50_mw": v, **pace_cols, "verified": layer == "organic"}
                 for layer, v in layers.items()]

    # Zones (X11): the default variant's draws split by zone
    split = zone_split(ctx, decks, org, inputs[default])
    d = draws[default]
    zone_org = {z: pf.organic_draws(org.zone_fits[z], sim_years, N_DRAWS, np.random.default_rng(sum(map(ord, z))))
                for z in ZONES}
    variants = ([(s, split["pipeline"]) for s in split["stock_candidates"].values()]
                + [(split["stock"], p) for p in split["pipeline_candidates"].values()])
    cols = _inputs_columns(inputs[default])
    extra = d["large_load"] + d["unattributed"]
    fixed_p50 = {(z, y): float(np.median(zone_org[z][:, j] + split["x7_fixed"][z] * extra[:, j]))
                 for z in ZONES for j, y in enumerate(sim_years)}
    for r in zone_layer_rows(zone_org, d["large_load"], d["unattributed"], stock_ll=inputs[default]["factor"]
                             * inputs[default]["base"], stock=split["stock"], pipeline=split["pipeline"],
                             unattributed=split["unattributed"], variants=variants, years=sim_years, keep=keep):
        z = r["region_id"]
        rows.append({**r, "region_type": "weather_zone", "variant": default, **cols,
                     "stock_share": split["stock"][z], "pipeline_share": split["pipeline"][z],
                     "u_share": split["unattributed"][z],
                     "zone_approved_stock_mw": split["stock"][z] * cols["approved_stock_mw"],
                     "x7_fixed_p50_mw": fixed_p50[(z, r["target_year"])] if r["layer"] == "total" else None,
                     "verified": r["layer"] == "organic"})

    return (pl.DataFrame(rows, schema={k: v for k, v in PEAK_SCHEMA.items() if k not in ("run_id", "is_default")})
            .with_columns(pl.lit(run_id).alias("run_id"), (pl.col("variant") == default).alias("is_default"))
            .select(list(PEAK_SCHEMA))
            .sort("variant", "region_type", "region_id", "layer", "target_year"))


def zone_split(ctx: MartContext, decks: Decks, org: Organic, inp: Mapping[str, Any]) -> dict[str, Any]:
    """X11 §2–§6: the stock split (Batch Zero base load raked to the deck's LZ_WEST share, f for NORTH solved from
    the May 2026 deck), the pipeline split (Batch Zero base + studied) with their candidate splits, and the
    unattributed split (mean 2023..last-summer zone excess minus the allocated large load); plus X7's single fixed
    share of LL + U (the mean excess itself, ``large_load.zone_allocation: x7_fixed``), kept for comparison."""
    import numpy as np

    from basecast_pipelines.models import data_centers as dc
    from basecast_pipelines.models import large_load_geo as geo
    from basecast_pipelines.models import peak_forecast as pf

    cv = decks.cv
    bz_base = geo.chart_zone_mw(cv, document=BZ_DOC, page=7, label="^base load$")
    bz_studied = geo.chart_zone_mw(cv, document=BZ_DOC, page=7, label="^studied load$")
    if not bz_base or not bz_studied:
        raise ValueError(f"the zone allocation needs {BZ_DOC} p. 7 (Batch Zero base and studied load by zone)")
    bz_both = {z: bz_base.get(z, 0) + bz_studied.get(z, 0) for z in ZONES}
    bz_track = geo.chart_zone_mw(cv, document=JUN_DOC, page=17)
    llis_total = geo.chart_zone_mw(cv, document=MAY_DOC, page=17, label="^total")
    llis_studied = geo.subtract(llis_total, geo.chart_zone_mw(cv, document=MAY_DOC, page=17, label=r"^\(6\)"))
    qsa = geo.chart_zone_mw(cv, document=QSA_DOC, page=7, label="^large load$")
    lz_may = cv.filter((pl.col("document") == MAY_DOC) & (pl.col("page") == 15) & (pl.col("status_label") == "Total"))
    lz_may = dict(zip(lz_may["category"].to_list(), lz_may["value_mw"].to_list(), strict=True))
    f_north = geo.lz_west_north_fraction(llis_studied, lz_may["LZ_WEST"])

    lzs = lz_split(decks)
    s_base = geo.shares(bz_base)
    lz_now = lz_share_at(lzs, ctx.as_of)
    stock_cands = {
        "raked_f_est": geo.rake_to_group(s_base, geo.lz_west_membership(f_north), lz_now),
        "raked_f1_x1": geo.rake_to_group(s_base, geo.lz_west_membership(1.0), lz_now),
        "raked_f0": geo.rake_to_group(s_base, geo.lz_west_membership(0.0), lz_now),
        "bz_base_unraked": s_base,
    }

    tsp_rows = geo.load_tsp_requests().filter(~pl.col("is_total"))
    czl = geo.county_zone_long(geo.load_county_weather_zone())
    ov = geo.load_overlap_puct().join(dc.load_population(), on="county_fips", how="left").with_columns(
        (pl.col("population").fill_null(0) * pl.col("county_share")).alias("pop_w"))

    def tsp_split(weight: str) -> dict[str, float]:
        a = geo.allocate_tsp_requests(tsp_rows.filter(pl.col("year") == TSP_RFI_YEAR),
                                      geo.tsp_zone_shares(ov, czl, weight=weight))
        return geo.shares(dict(zip(a["weather_zone"].to_list(), a["mw"].to_list(), strict=True)))

    pipe_cands = {
        "bz_base_plus_studied_2032": geo.shares(bz_both),
        "bz_studied_2032": geo.shares(bz_studied),
        "bz_tracking_jun2026": geo.shares(bz_track),
        "llis_studied_may2026": geo.shares(llis_studied),
        f"tsp_rfi_{TSP_RFI_YEAR}_area": tsp_split("overlap_km2"),
        f"tsp_rfi_{TSP_RFI_YEAR}_pop": tsp_split("pop_w"),
        "qsa_q4_2026": geo.shares(qsa),
    }

    # U split (X11 §6): mean zone excess over the pre-break fit minus the large load the stock split puts there
    years = range(EXCESS_FROM, last_summer(ctx.as_of) + 1)
    sub = org.cpanel.filter(pl.col("year").is_in(list(years)))
    zex = {z: [r["coincident_mw"] - org.zone_fits[z].mean(r["year"], r[FEATURE])
               for r in sub.filter(pl.col("weather_zone") == z).iter_rows(named=True)] for z in ZONES}
    pts = list(zip(inp["points"]["month_idx"].to_list(), inp["points"]["a2e_mw"].to_list(), strict=True))
    zone_ll: dict[str, list[float]] = {z: [] for z in ZONES}
    for y in years:
        m = org.peak_months[y]
        a2e_y = pf.interpolate(pts, pf.month_end_index(m))
        s_y = geo.rake_to_group(s_base, geo.lz_west_membership(f_north),
                                lz_share_at(lzs, date(int(m[:4]), int(m[5:]), 28)))
        for z in ZONES:
            zone_ll[z].append(inp["factor"] * a2e_y * s_y[z])
    mean_ex = {z: float(np.mean(zex[z])) for z in ZONES}
    s_u, _ = geo.residual_shares(mean_ex, {z: float(np.mean(zone_ll[z])) for z in ZONES})
    return {"stock": stock_cands["raked_f_est"], "stock_candidates": stock_cands,
            "pipeline": pipe_cands["bz_base_plus_studied_2032"], "pipeline_candidates": pipe_cands,
            "unattributed": s_u, "x7_fixed": geo.shares(mean_ex), "f_north": f_north, "lz_west_share": lz_now}


def lz_split(decks: Decks) -> pl.DataFrame:
    """X11's approved stock by load zone per deck (headlines first, charts after)."""
    from basecast_pipelines.models import peak_excess as px

    hl_lz = decks.headlines.filter(pl.col("dimension") == "load_zone")
    headline = (hl_lz.unique(["report_date", "category"]).group_by("report_date")
                .agg(pl.col("value").filter(pl.col("category") == "LZ_WEST").first().alias("lz_west"),
                     pl.col("value").filter(pl.col("category") == "Other").first().alias("other"))
                .with_columns(pl.lit("headline").alias("src")))
    chart = (px.approved_by_load_zone(decks.cv).group_by("vintage")
             .agg(pl.col("approved_mw").filter(pl.col("load_zone") == "LZ_WEST").first().alias("lz_west"),
                  pl.col("approved_mw").filter(pl.col("load_zone") == "Other").first().alias("other"))
             .rename({"vintage": "report_date"}).with_columns(pl.lit("chart").alias("src")))
    return lz_split_table(headline, chart)


def _peak(frame: pl.DataFrame, *, variant: str, year: int, col: str = "p50_mw", layer: str = "total",
          region: str = "ERCOT"):
    return frame.filter((pl.col("variant") == variant) & (pl.col("target_year") == year)
                        & (pl.col("layer") == layer) & (pl.col("region_id") == region))[col].item()


def _input(frame: pl.DataFrame, variant: str, col: str):
    return frame.filter((pl.col("variant") == variant) & (pl.col("region_id") == "ERCOT"))[col].unique().item()


def _zone_sums(frame: pl.DataFrame) -> pl.DataFrame:
    """Per year: the ERCOT total P50 of the zones' variant, the zone total P50s' sum (X11) and X7's fixed-share sum."""
    tot = frame.filter(pl.col("layer") == "total")
    zones = tot.filter(pl.col("region_type") == "weather_zone")
    default = zones["variant"].unique().item()
    ercot = tot.filter((pl.col("region_id") == "ERCOT") & (pl.col("variant") == default)).select(
        "target_year", pl.col("p50_mw").alias("ercot_mw"))
    sums = zones.group_by("target_year").agg(pl.col("p50_mw").sum().alias("x11_mw"),
                                             pl.col("x7_fixed_p50_mw").sum().alias("x7_fixed_mw"))
    return ercot.join(sums, on="target_year").sort("target_year")


def _gap_check(num: str, den: str, pct: float):
    """Every year's ``num`` within ``pct`` % of ``den`` (columns of :func:`_zone_sums`); the actual lists each year."""

    def fn(frame: pl.DataFrame) -> CheckResult:
        s = _zone_sums(frame).with_columns((100 * (pl.col(num) / pl.col(den) - 1)).abs().alias("gap_pct"))
        actual = {r["target_year"]: {den: round(r[den]), num: round(r[num]), "gap_pct": round(r["gap_pct"], 3)}
                  for r in s.iter_rows(named=True)}
        return CheckResult(bool((s["gap_pct"] <= pct).all()), f"every year within {pct}%", actual)

    return fn


def _zone_sum(col: str, year: int):
    return lambda f: _zone_sums(f).filter(pl.col("target_year") == year)[col].item()


PRE, LATEST, PACE = "deck_pre_batch_zero", "deck_latest", "approvals_pace"


def _golden_peak(name: str, variant: str, year: int, expected: float, col: str = "p50_mw", *, layer: str = "total",
                 region: str = "ERCOT", tol: float = 1.0) -> Check:
    return value_check(name, lambda f: _peak(f, variant=variant, year=year, col=col, layer=layer, region=region),
                       expected, as_of=GOLDEN_AS_OF, tol=tol, applies=_as_x7)


def _as_x7(config: Mapping[str, Any]) -> bool:
    """X7 / X11's numbers assume the March 2026 deck as the default (zones) and the last year 2030 or later."""
    return (marts_config.value(config, "forecast.default_variant") == PRE
            and int(marts_config.value(config, "forecast.last_year")) >= 2030)


PEAK_CHECKS = (
    value_check("one row per region, year, variant and layer",
                lambda f: f.select("region_id", "target_year", "variant", "layer").is_duplicated().any(), False),
    value_check("zones only for the default variant",
                lambda f: f.filter(pl.col("region_type") == "weather_zone")["is_default"].all(), True),
    value_check("approvals pace has no band",
                lambda f: f.filter(pl.col("variant") == PACE)["p10_mw"].null_count()
                == f.filter(pl.col("variant") == PACE).height, True),
    # X11's zones do not add up to the ERCOT P50 within 0.1% (92,720 vs 92,819 MW in 2027, X11 §4's own table): the
    # docs' checks are X7's (its fixed shares) and X11's (its zones vs X7's), below.
    Check("X7 fixed-share zone P50s add up to the ERCOT P50 within 0.1% (X7 §2)",
          _gap_check("x7_fixed_mw", "ercot_mw", 0.1)),
    Check("X11 zone sum moves under 0.1% from X7's fixed shares (X11 §4)", _gap_check("x11_mw", "x7_fixed_mw", 0.1)),
    # X7 re-run after the X16 fixes (decisions.md, 2026-09-26: "These are the A-M5/A-M6 golden checks")
    _golden_peak("Mar 2026 deck 2027 total P50", PRE, 2027, 92_819),
    _golden_peak("Mar 2026 deck 2027 total P10", PRE, 2027, 89_282, "p10_mw"),
    _golden_peak("Mar 2026 deck 2027 total P90", PRE, 2027, 96_418, "p90_mw"),
    _golden_peak("Mar 2026 deck 2030 total P50", PRE, 2030, 111_323),
    _golden_peak("Mar 2026 deck 2030 total P10", PRE, 2030, 104_031, "p10_mw"),
    _golden_peak("Mar 2026 deck 2030 total P90", PRE, 2030, 123_286, "p90_mw"),
    _golden_peak("Jun 2026 deck 2027 total P50", LATEST, 2027, 101_800),
    _golden_peak("Jun 2026 deck 2030 total P50", LATEST, 2030, 132_799),
    _golden_peak("approvals pace 2030 total P50", PACE, 2030, 96_318),
    # X7 §1 inputs table
    value_check("factor 0.512", lambda f: _input(f, PRE, "factor"), 0.512, as_of=GOLDEN_AS_OF, tol=0.0005),
    value_check("ratio P10 0.131", lambda f: _input(f, PRE, "ratio_p10"), 0.131, as_of=GOLDEN_AS_OF, tol=0.0005),
    value_check("ratio P50 0.187", lambda f: _input(f, PRE, "ratio_p50"), 0.187, as_of=GOLDEN_AS_OF, tol=0.0005),
    value_check("ratio P90 0.289", lambda f: _input(f, PRE, "ratio_p90"), 0.289, as_of=GOLDEN_AS_OF, tol=0.0005),
    value_check("38 (deck, year) ratios", lambda f: _input(f, PRE, "n_ratios"), 38, as_of=GOLDEN_AS_OF),
    value_check("pre-Batch-Zero deck 2026-03-13", lambda f: _input(f, PRE, "deck_vintage"), date(2026, 3, 13),
                as_of=GOLDEN_AS_OF),
    value_check("pre-Batch-Zero deck stock 9,020 MW", lambda f: _input(f, PRE, "approved_stock_mw"), 9_020,
                as_of=GOLDEN_AS_OF),
    value_check("latest deck 2026-06-19", lambda f: _input(f, LATEST, "deck_vintage"), date(2026, 6, 19),
                as_of=GOLDEN_AS_OF),
    value_check("latest deck stock 8,900 MW", lambda f: _input(f, LATEST, "approved_stock_mw"), 8_900,
                as_of=GOLDEN_AS_OF),
    # X7 §2 zone table (organic, same draws in X11) and X11 §4 (Mar 2026 deck, coincident contribution, MW)
    _golden_peak("COAST 2027 organic P50 (X7)", PRE, 2027, 22_202, layer="organic", region="COAST"),
    _golden_peak("NCENT 2027 total P50 (X11)", PRE, 2027, 28_874, region="NCENT"),
    _golden_peak("NCENT 2030 total P50 (X11)", PRE, 2030, 32_885, region="NCENT"),
    _golden_peak("NCENT 2030 allocation low (X11)", PRE, 2030, 32_746, "p10_mw", region="NCENT"),
    _golden_peak("NCENT 2030 allocation high (X11)", PRE, 2030, 39_915, "p90_mw", region="NCENT"),
    _golden_peak("FWEST 2030 total P50 (X11)", PRE, 2030, 9_397, region="FWEST"),
    _golden_peak("FWEST 2030 allocation high (X11)", PRE, 2030, 13_862, "p90_mw", region="FWEST"),
    _golden_peak("WEST 2030 total P50 (X11)", PRE, 2030, 5_254, region="WEST"),
    _golden_peak("NCENT 2027 X7 fixed-share P50", PRE, 2027, 28_299, "x7_fixed_p50_mw", region="NCENT"),
    _golden_peak("FWEST 2030 X7 fixed-share P50", PRE, 2030, 13_755, "x7_fixed_p50_mw", region="FWEST"),
    value_check("X11 zone sum 2027 92,720 MW", _zone_sum("x11_mw", 2027), 92_720, as_of=GOLDEN_AS_OF, tol=1,
                applies=_as_x7),
    value_check("X11 zone sum 2030 111,390 MW", _zone_sum("x11_mw", 2030), 111_390, as_of=GOLDEN_AS_OF, tol=1,
                applies=_as_x7),
    value_check("X7 fixed-share zone sum 2027 92,730 MW", _zone_sum("x7_fixed_mw", 2027), 92_730,
                as_of=GOLDEN_AS_OF, tol=1, applies=_as_x7),
    value_check("X7 fixed-share zone sum 2030 111,288 MW", _zone_sum("x7_fixed_mw", 2030), 111_288,
                as_of=GOLDEN_AS_OF, tol=1, applies=_as_x7),
)


def peak_meta(frame: pl.DataFrame, ctx: MartContext) -> dict[str, Any]:
    default = marts_config.value(ctx.config, "forecast.default_variant")
    return {
        "default_variant": default,
        "variants": [{"variant": v, "label": label, "has_band": band} for v, (label, band) in VARIANTS.items()],
        "ratio_definition": RATIO_DEFINITION,
        "band_basis": BAND_BASIS,
        "simulation": {"draws": N_DRAWS, "seed": SEED, "summers": forecast_years(
            ctx.as_of, int(marts_config.value(ctx.config, "forecast.last_year")))[0]},
        "zone_allocation": marts_config.value(ctx.config, "large_load.zone_allocation"),
    }


PEAK_FORECAST = Mart(
    name="mart_peak_forecast",
    build=build_peak_forecast,
    key=("region_type", "region_id", "target_year", "variant", "layer"),
    inputs=("ercot_load_hourly_wz", "weather_hourly_wz", "large_load_chart_values", "large_load_headlines",
            "puct_tsp_large_load_requests", "county_weather_zone", "county_utility_overlap_puct",
            "census_population_county"),
    description="Summer peak forecast by target year: organic, large load and unattributed layers with P10/P50/P90, "
    "for ERCOT (three large-load variants) and the weather zones' coincident contributions (default variant, X11 "
    "allocation); plus the inputs of the large-load layer (X7 §4, X11 §4).",
    caveats=("band_uncalibrated", "machine_read_unverified", "allocated_statewide", "policy_pause_2026",
             "beyond_backtested_window"),
    checks=PEAK_CHECKS,
    meta=peak_meta,
)


# --- mart_official_peak_lines -----------------------------------------------------------------------------------

OFFICIAL_SQL = (
    "select product, vintage, vintage_date, target_year, region_type, region_id, scenario, value, source_file "
    "from official_forecasts where metric = 'peak_mw' and season = 'summer' and ("
    "(product = 'LTLF' and scenario in ('ercot_adjusted', 'tsp_provided') and region_type in ('ercot', 'weather_zone'))"
    " or (product = 'CDR' and region_type = 'ercot' and scenario is null))"
)


def build_official_peak_lines(ctx: MartContext) -> pl.DataFrame:
    _, keep = forecast_years(ctx.as_of, int(marts_config.value(ctx.config, "forecast.last_year")))
    return official_lines(ctx.read_sql(OFFICIAL_SQL), ctx.as_of, keep)


def _line(frame: pl.DataFrame, series: str, year: int, region: str = "ERCOT"):
    return frame.filter((pl.col("series") == series) & (pl.col("target_year") == year)
                        & (pl.col("region_id") == region))["mw"].item()


def _zones_add_up(frame: pl.DataFrame) -> CheckResult:
    """LTLF's weather-zone lines add up to its ERCOT line (so they are coincident contributions, like ours)."""
    ltlf = frame.filter(pl.col("product") == "LTLF")
    ercot = ltlf.filter(pl.col("region_id") == "ERCOT").select("series", "target_year", "mw")
    zones = (ltlf.filter(pl.col("region_type") == "weather_zone").group_by("series", "target_year")
             .agg(pl.col("mw").sum().alias("zones_mw")))
    j = ercot.join(zones, on=["series", "target_year"])
    worst = float((100 * (j["zones_mw"] / j["mw"] - 1)).abs().max()) if j.height else None
    return CheckResult(worst is not None and worst <= 0.01, "within 0.01%", worst)


OFFICIAL_CHECKS = (
    value_check("one row per series, region and year",
                lambda f: f.select("series", "region_id", "target_year").is_duplicated().any(), False),
    value_check("CDR only at the ERCOT level", lambda f: f.filter(pl.col("series") == "cdr")["region_id"].unique()
                .to_list(), ["ERCOT"]),
    Check("LTLF zone lines add up to the ERCOT line", _zones_add_up),
    # X7 §2 "Next to ERCOT's latest official forecasts" (GW, one decimal)
    value_check("LTLF vintage 2025", lambda f: f.filter(pl.col("product") == "LTLF")["vintage"].unique().to_list(),
                ["2025"], as_of=GOLDEN_AS_OF),
    value_check("CDR vintage Dec 2025", lambda f: f.filter(pl.col("product") == "CDR")["vintage"].unique().to_list(),
                ["Dec 2025"], as_of=GOLDEN_AS_OF),
    value_check("LTLF 2025 adjusted 2027", lambda f: _line(f, "ercot_adjusted", 2027), 104_300, as_of=GOLDEN_AS_OF,
                tol=50),
    value_check("LTLF 2025 adjusted 2030", lambda f: _line(f, "ercot_adjusted", 2030), 138_900, as_of=GOLDEN_AS_OF,
                tol=50),
    value_check("LTLF 2025 TSP-provided 2027", lambda f: _line(f, "tsp_provided", 2027), 138_200,
                as_of=GOLDEN_AS_OF, tol=50),
    value_check("LTLF 2025 TSP-provided 2030", lambda f: _line(f, "tsp_provided", 2030), 208_000,
                as_of=GOLDEN_AS_OF, tol=50),
    value_check("CDR Dec 2025 2027", lambda f: _line(f, "cdr", 2027), 105_400, as_of=GOLDEN_AS_OF, tol=50),
    value_check("CDR Dec 2025 2030", lambda f: _line(f, "cdr", 2030), 141_700, as_of=GOLDEN_AS_OF, tol=50),
)

OFFICIAL_PEAK_LINES = Mart(
    name="mart_official_peak_lines",
    build=build_official_peak_lines,
    key=("product", "vintage", "series", "region_id", "target_year"),
    inputs=("official_forecasts",),
    description="ERCOT's latest official summer peak forecasts by target year, the comparison lines of the peak "
    "forecast: LTLF ERCOT-adjusted and TSP-provided (ERCOT and weather zones) and the CDR (ERCOT only).",
    checks=OFFICIAL_CHECKS,
)


# --- large loads ------------------------------------------------------------------------------------------------


def build_large_load_realization(ctx: MartContext) -> pl.DataFrame:
    """Q5 §5–§6: realized stocks at year end (status series; a year still running is ``realized_partial``, Q5's
    "2026 partial") and the ratios per deck and target year; ``known_from`` is X7's date the December stock was first
    published; ``min_horizon_met``: horizon of 6 months or more, the pairs Q5's band summarizes. The forecast's own
    ratio band is X7's (``mart_peak_forecast``: December stocks as the next deck prints them, known by ``as_of``)."""
    from basecast_pipelines.models import large_load as ll

    decks = deck_inputs(ctx)
    monthly = ll.a2e_by_month(decks.cv)
    realized_rows = []
    for year in range(REALIZATION_FROM, ctx.as_of.year + 1):
        a2e_mw, month = ll.stock_at_year_end(monthly, year)
        if a2e_mw is None:
            continue
        en_mw, _ = ll.energized_at_year_end(decks.wide, year)
        realized_as_of = date(year + 1, 1, 1) if month == f"{year}-12" else date.fromisoformat(f"{month}-28")
        realized_rows.append({"target_year": year, "realized_a2e_mw": a2e_mw, "realized_month": month,
                              "realized_energized_mw": en_mw, "realized_as_of": realized_as_of})
    realized = pl.DataFrame(realized_rows, schema={"target_year": pl.Int64, "realized_a2e_mw": pl.Float64,
                                                   "realized_month": pl.String, "realized_energized_mw": pl.Float64,
                                                   "realized_as_of": pl.Date})
    bars_v = decks.wide.join(
        decks.vintages.select("vintage", "document", "page", "chart_title", "has_segments",
                              pl.col("a2e_stock_mw").alias("base_a2e_mw")),
        on=["vintage", "document", "page", "chart_title"], how="inner",
    ).filter(pl.col("vintage") >= ll.PHASE0_SINCE)
    known = decks.realized.select("target_year", "known_from").filter(pl.col("known_from") <= ctx.as_of)
    return (
        ll.realization_ratios(bars_v, realized)
        .join(known, on="target_year", how="left")
        .select(
            pl.col("vintage").alias("deck_vintage"), "report_date", pl.col("target_year").cast(pl.Int32),
            "horizon_months", "promised_mw", "promised_firm_mw", "base_a2e_mw", "realized_a2e_mw",
            "realized_energized_mw", pl.col("realized_month").str.to_date("%Y-%m"),
            (pl.col("realized_month") != pl.col("target_year").cast(pl.String) + "-12").alias("realized_partial"),
            "known_from", "gross_a2e", "gross_a2e_firm", "incremental_a2e", "incremental_a2e_firm", "gross_energized",
            (pl.col("horizon_months") >= MIN_HORIZON_MONTHS).alias("min_horizon_met"), "document", "page",
            pl.lit(False).alias("verified"),
        )
        .sort("deck_vintage", "target_year")
    )


def _ratio_median(frame: pl.DataFrame, year: int, col: str) -> float:
    return frame.filter((pl.col("target_year") == year) & pl.col("min_horizon_met"))[col].drop_nulls().median()


def _realized(frame: pl.DataFrame, year: int):
    return frame.filter(pl.col("target_year") == year)["realized_a2e_mw"].unique().item()


REALIZATION_CHECKS = (
    value_check("one row per deck and target year",
                lambda f: f.select("deck_vintage", "target_year").is_duplicated().any(), False),
    # Q5 §3 (marts-proposal §3.2: 63 rows, q5_ratios.csv)
    value_check("63 deck x year rows", lambda f: f.height, 63, as_of=GOLDEN_AS_OF),
    value_check("realized Dec 2024 6,297 MW", lambda f: _realized(f, 2024), 6_297, as_of=GOLDEN_AS_OF),
    value_check("realized Dec 2025 8,786 MW", lambda f: _realized(f, 2025), 8_786, as_of=GOLDEN_AS_OF),
    value_check("10 decks in the 2024 band", lambda f: f.filter((pl.col("target_year") == 2024)
                                                                & pl.col("min_horizon_met")).height, 10,
                as_of=GOLDEN_AS_OF),
    value_check("18 decks in the 2025 band", lambda f: f.filter((pl.col("target_year") == 2025)
                                                                & pl.col("min_horizon_met")).height, 18,
                as_of=GOLDEN_AS_OF),
    value_check("incremental median 2024 = 0.15", lambda f: _ratio_median(f, 2024, "incremental_a2e"), 0.15,
                as_of=GOLDEN_AS_OF, tol=0.005),
    value_check("incremental median 2025 = 0.20", lambda f: _ratio_median(f, 2025, "incremental_a2e"), 0.20,
                as_of=GOLDEN_AS_OF, tol=0.005),
    value_check("gross median 2025 = 0.35", lambda f: _ratio_median(f, 2025, "gross_a2e"), 0.35,
                as_of=GOLDEN_AS_OF, tol=0.005),
)

LARGE_LOAD_REALIZATION = Mart(
    name="mart_large_load_realization",
    build=build_large_load_realization,
    key=("deck_vintage", "target_year"),
    inputs=("large_load_chart_values", "large_load_headlines"),
    description="Large-load decks since May 2023 (one per month): MW promised in service by each target year vs the "
    "approved-to-energize stock at that year's end, and the gross and incremental realization ratios (Q5).",
    caveats=("machine_read_unverified", "policy_pause_2026"),
    checks=REALIZATION_CHECKS,
    meta=lambda frame, ctx: {"ratio_definition": RATIO_DEFINITION, "min_horizon_months": MIN_HORIZON_MONTHS},
)


def build_large_load_in_service(ctx: MartContext) -> pl.DataFrame:
    """Q5 §1/§3: the picked decks' in-service bars since May 2023 by status."""
    from basecast_pipelines.models import large_load as ll

    decks = deck_inputs(ctx)
    picked = decks.vintages.filter(pl.col("vintage") >= ll.PHASE0_SINCE).select("vintage", "document", "page",
                                                                               "chart_title")
    bars = decks.wide.join(picked, on=["vintage", "document", "page", "chart_title"], how="inner")
    return in_service_long(bars)


def _bar_total(frame: pl.DataFrame, vintage: date, year: int) -> float:
    return frame.filter((pl.col("deck_vintage") == vintage) & (pl.col("in_service_year") == year))[
        "total_mw"].unique().item()


IN_SERVICE_CHECKS = (
    value_check("one row per deck, year and status",
                lambda f: f.select("deck_vintage", "in_service_year", "status").is_duplicated().any(), False),
    value_check("28 decks since May 2023 (Q5)", lambda f: f["deck_vintage"].n_unique(), 28, as_of=GOLDEN_AS_OF),
    # X7 §1 inputs table (GW, one decimal)
    value_check("Mar 2026 deck, 2027 bar 66.7 GW", lambda f: _bar_total(f, date(2026, 3, 13), 2027), 66_700,
                as_of=GOLDEN_AS_OF, tol=50),
    value_check("Mar 2026 deck, 2030 bar 238.6 GW", lambda f: _bar_total(f, date(2026, 3, 13), 2030), 238_600,
                as_of=GOLDEN_AS_OF, tol=50),
    value_check("Jun 2026 deck, 2027 bar 201.0 GW", lambda f: _bar_total(f, date(2026, 6, 19), 2027), 201_000,
                as_of=GOLDEN_AS_OF, tol=50),
)

LARGE_LOAD_IN_SERVICE = Mart(
    name="mart_large_load_in_service",
    build=build_large_load_in_service,
    key=("deck_vintage", "in_service_year", "status"),
    inputs=("large_load_chart_values", "large_load_headlines"),
    description="What each large-load deck since May 2023 promised: MW in service by the end of each year "
    "(cumulative), by status (Q5's in-service series).",
    caveats=("machine_read_unverified",),
    checks=IN_SERVICE_CHECKS,
)


def build_large_load_monthly(ctx: MartContext) -> pl.DataFrame:
    """Month by month: the approved-to-energize stock (Q5, misdated axes dropped), the observed peaks of the approved
    loads (X1, charts), the LZ_WEST / Other split (X11: headlines, then charts) and the observed peaks the decks'
    sentences state (after the known re-dating, ``HEADLINE_REDATES``)."""
    from basecast_pipelines.models import large_load as ll
    from basecast_pipelines.models import peak_excess as px

    decks = deck_inputs(ctx)
    month = pl.col("month").str.to_date("%Y-%m")
    a2e = ll.a2e_by_month(decks.cv).select(month, "a2e_mw", "read_from_vintage", "document", "page")
    observed = px.energized_by_month(decks.cv).select(
        month, pl.col("simultaneous_mw").alias("observed_simultaneous_mw"),
        pl.col("non_simultaneous_mw").alias("observed_nonsimultaneous_mw"),
        pl.col("read_from_vintage").alias("observed_read_from_vintage"))
    lz = (lz_split(decks).with_columns(pl.col("report_date").dt.truncate("1mo").alias("month"))
          .sort("report_date").group_by("month").last()
          .select("month", pl.col("lz_west").alias("a2e_lz_west_mw"), pl.col("other").alias("a2e_other_mw"),
                  pl.col("src").alias("lz_source")))
    headline = headline_observed_by_month(decks.headlines)
    months = pl.concat([a2e["month"], observed["month"], lz["month"], headline["month"]])
    return (
        month_spine(months.min(), months.max())
        .join(a2e, on="month", how="left").join(observed, on="month", how="left")
        .join(lz, on="month", how="left").join(headline, on="month", how="left")
        .with_columns(pl.lit(False).alias("verified"))
        .sort("month")
    )


def _month(frame: pl.DataFrame, month: date, col: str):
    return frame.filter(pl.col("month") == month)[col].item()


MONTHLY_CHECKS = (
    value_check("one row per month", lambda f: f["month"].is_duplicated().any(), False),
    # Q5 §3 and marts-proposal §3.4 (37 approved-stock months, 32 observed-peak months)
    value_check("37 months with an approved stock", lambda f: f["a2e_mw"].drop_nulls().len(), 37, as_of=GOLDEN_AS_OF),
    value_check("32 months with an observed peak", lambda f: f["observed_simultaneous_mw"].drop_nulls().len(), 32,
                as_of=GOLDEN_AS_OF),
    value_check("approved stock Dec 2025 8,786 MW", lambda f: _month(f, date(2025, 12, 1), "a2e_mw"), 8_786,
                as_of=GOLDEN_AS_OF),
    value_check("approved stock Dec 2023 4,479 MW (axis fix)", lambda f: _month(f, date(2023, 12, 1), "a2e_mw"),
                4_479, as_of=GOLDEN_AS_OF),
    value_check("no reading 2023-11, 2024-02, 2024-04", lambda f: f.filter(pl.col("month").is_in(
        [date(2023, 11, 1), date(2024, 2, 1), date(2024, 4, 1)]))["a2e_mw"].null_count(), 3, as_of=GOLDEN_AS_OF),
    # X6 note 7: after the re-dating, January 2025 keeps its own deck's sentence and January 2026 gets 3,977 MW
    value_check("headline Jan 2025 non-simultaneous 3,211 MW",
                lambda f: _month(f, date(2025, 1, 1), "headline_nonsimultaneous_mw"), 3_211, as_of=GOLDEN_AS_OF),
    value_check("headline Jan 2026 non-simultaneous 3,977 MW (re-dated)",
                lambda f: _month(f, date(2026, 1, 1), "headline_nonsimultaneous_mw"), 3_977, as_of=GOLDEN_AS_OF),
    value_check("LZ split Jun 2026 adds up to 8,926 MW (X11)", lambda f: _month(f, date(2026, 6, 1), "a2e_lz_west_mw")
                + _month(f, date(2026, 6, 1), "a2e_other_mw"), 8_926, as_of=GOLDEN_AS_OF, tol=1),
)

LARGE_LOAD_MONTHLY = Mart(
    name="mart_large_load_monthly",
    build=build_large_load_monthly,
    key=("month",),
    inputs=("large_load_chart_values", "large_load_headlines"),
    description="Large loads month by month: the approved-to-energize stock, the observed peak of the approved loads "
    "(simultaneous and non-simultaneous), its LZ_WEST / Other split and the peaks stated in the decks' sentences; "
    "null = no reading that month (Q5, X1, X11).",
    caveats=("machine_read_unverified", "policy_pause_2026"),
    checks=MONTHLY_CHECKS,
    meta=lambda frame, ctx: {"headline_redates": [
        {"report_date": r.isoformat(), "printed_month": a.isoformat()[:7], "month": to.isoformat()[:7]}
        for (r, a), to in HEADLINE_REDATES.items()]},
)

MARTS = (PEAK_FORECAST, OFFICIAL_PEAK_LINES, LARGE_LOAD_REALIZATION, LARGE_LOAD_IN_SERVICE, LARGE_LOAD_MONTHLY)
