"""Explorer marts (A-M4): the generation queue raw vs adjusted, the new data-center sites and the county
acquisition priority.

- ``mart_queue_project_scores`` and ``mart_queue_adjusted_county``: the adjusted queue of ``marts/queue.py`` (X2
  "Pipeline", shared with the account marts), one row per active project of the latest GIS report and its county ×
  stratum sums plus stratum ``all``, as ``analysis/x2_adjusted_queue.py`` §Q2 writes them, in the contract's names
  (marts-proposal §2.1–2.2). ``large_gas_mw_2028`` (R10) is the adjusted MW of gas/other projects of ≥ 500 MW:
  marts-proposal §2.1 marks that definition *speculative*, and the 500 MW cut is X2's review item 2; not reviewed.
- ``mart_data_center_sites_new``: ``analysis/q4_data_centers.py`` §1–3 (sites with a first permit since 2025, the
  county's territory mix, the ISO class and both metro rules), with the R7 switches ``dc_sites.metro_rule`` (the
  ``metro`` column) and ``dc_sites.include_naics_only``. Sites in counties whose dominant ISO class has no ERCOT are
  kept, with ``in_ercot = false``, so an ERCOT count filters on it.
- ``mart_county_acquisition``: ``analysis/x14_acquisition_zones.py`` (X14 §6). X14 reads its zone inputs from
  ``analysis/out/``; here each is recomputed by the chain that wrote it: X1's peak excess
  (``peak_excess.excess_vs_prebreak``), X12's normalized summer peak (the variant chosen on 2023 validation, then
  ``weather_normalized.normalize_zone`` → ``peak_quantiles``) and the RTM load-zone prices parsed from the lake
  with the ``ercot_spp_hist`` parser. X11 (an alternative signal) and X2 (context, which the API joins) are not
  read. The legend breaks, the weights and the grid tilt go to ``mart_meta`` (``county_acquisition``).

Not built: ``mart_zone_layers`` (P1).
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

import polars as pl

from basecast_pipelines.marts import config as marts_config
from basecast_pipelines.marts import queue as marts_queue
from basecast_pipelines.marts.core import Mart, MartContext, value_check

GOLDEN_AS_OF = date(2026, 9, 26)  # X2, Q4 and X14's run date

# --- the generation queue (X2) ------------------------------------------------------------------------------

ALL = "all"
STRATUM_NAMES = {"gas+other": "gas_other"}  # the contract's stratum names
STAGE_NAMES = {"entry": "entry", "ia_signed": "ia"}  # entry_ia_sm's two stages
GAS = "gas_other"
LARGE_GAS_MIN_MW = 500.0  # R10 (marts-proposal §2.1, speculative; X2 review item 2)
LARGE_GAS_HORIZON = "2028"


def project_rows(scored: pl.DataFrame, stage_dates: pl.DataFrame, report_month: date,
                 horizons: Iterable[str]) -> pl.DataFrame:
    """X2's project scores in the contract's names: ``stratum`` gas_other, ``stage`` entry | ia, ``curve`` own |
    pooled (the pooled curve is the one fit on every stratum), ``p_cod_<h>``, ``elapsed_months`` and
    ``stage_date``; ``large_gas`` marks a gas/other project of ≥ ``LARGE_GAS_MIN_MW``. ``scored``: the
    ``AdjustedQueue.scored`` columns."""
    hs = list(horizons)
    unknown = set(scored["stage"].unique().to_list()) - set(STAGE_NAMES)
    if unknown:
        raise ValueError(f"stages without a contract name: {sorted(unknown)}")
    out = scored.join(stage_dates, on="inr", how="left").select(
        pl.lit(report_month, dtype=pl.Date).alias("as_of_month"),
        "inr", "project_name", "county_fips", "county_name", "weather_zone", "cdr_reporting_zone", "fuel_type",
        pl.col("stratum").replace(STRATUM_NAMES).alias("stratum"),
        pl.col("stage").replace_strict(STAGE_NAMES, return_dtype=pl.Utf8).alias("stage"),
        "stage_date",
        pl.col("elapsed").alias("elapsed_months"),
        "capacity_mw",
        "projected_cod",
        pl.when(pl.col("curve") == pl.col("stratum")).then(pl.lit("own")).otherwise(pl.lit("pooled")).alias("curve"),
        *[pl.col(f"p_{h}").alias(f"p_cod_{h}") for h in hs],
        *[pl.col(f"mw_{h}") for h in hs],
        *[pl.col(f"clamped_{h}") for h in hs],
    )
    return out.with_columns(
        ((pl.col("stratum") == GAS) & (pl.col("capacity_mw") >= LARGE_GAS_MIN_MW)).alias("large_gas")
    ).sort("inr")


def county_rows(projects: pl.DataFrame, horizons: Iterable[str]) -> pl.DataFrame:
    """County × stratum, plus stratum ``all``, from :func:`project_rows`: X2's ``aggregate`` (``projects``,
    ``raw_mw``, ``adj_mw_<h>``, ``ratio_<h>``), ``projects_ia`` / ``raw_mw_ia`` (the IA stage),
    ``large_gas_mw_2028`` (strata ``all`` and gas_other only), the county's modal CDR zone and ``rank_raw`` /
    ``rank_adj`` within the stratum (1 = most MW; adjusted at Dec 2028; ties by FIPS)."""
    from basecast_pipelines.models import queue_adjusted as qa

    hs = list(horizons)
    if LARGE_GAS_HORIZON not in hs:
        raise ValueError(f"large_gas_mw_{LARGE_GAS_HORIZON} needs the {LARGE_GAS_HORIZON} horizon")
    both = pl.concat([projects, projects.with_columns(pl.lit(ALL).alias("stratum"))])
    keys = ["county_fips", "stratum"]
    ia = pl.col("stage") == "ia"
    extra = both.group_by(keys).agg(
        ia.sum().cast(pl.Int64).alias("projects_ia"),
        pl.col("capacity_mw").filter(ia).sum().alias("raw_mw_ia"),
        pl.col(f"mw_{LARGE_GAS_HORIZON}").filter(pl.col("large_gas")).sum().alias("_large_gas"),
    )
    county = projects.group_by("county_fips").agg(
        pl.col("county_name").drop_nulls().first(),
        pl.col("weather_zone").drop_nulls().first(),
        pl.col("cdr_reporting_zone").drop_nulls().mode().sort().first(),
    )
    out = (
        qa.aggregate(both, keys, hs)
        .join(extra, on=keys, how="left")
        .join(county, on="county_fips", how="left")
        .with_columns(
            pl.col("projects").cast(pl.Int64),
            pl.when(pl.col("stratum").is_in([ALL, GAS])).then(pl.col("_large_gas"))
            .alias(f"large_gas_mw_{LARGE_GAS_HORIZON}"),
        )
        .sort("stratum", "county_fips")
    )
    rank = [
        pl.col(c).rank("ordinal", descending=True).over("stratum").cast(pl.Int64).alias(name)
        for c, name in (("raw_mw", "rank_raw"), (f"adj_mw_{LARGE_GAS_HORIZON}", "rank_adj"))
    ]
    return out.with_columns(rank).select(
        "county_fips", "county_name", "weather_zone", "cdr_reporting_zone", "stratum", "projects", "projects_ia",
        "raw_mw", "raw_mw_ia", *[f"adj_mw_{h}" for h in hs], *[f"ratio_{h}" for h in hs], "rank_raw", "rank_adj",
        f"large_gas_mw_{LARGE_GAS_HORIZON}",
    )


def queue_projects(ctx: MartContext) -> pl.DataFrame:
    def build() -> pl.DataFrame:
        q = marts_queue.adjusted_queue(ctx)
        return project_rows(q.scored, marts_queue.stage_dates(ctx), q.report_month, q.horizons)

    return ctx.cached("explorer.queue_projects", build)


def build_project_scores(ctx: MartContext) -> pl.DataFrame:
    return queue_projects(ctx)


def build_queue_county(ctx: MartContext) -> pl.DataFrame:
    q = marts_queue.adjusted_queue(ctx)
    return county_rows(queue_projects(ctx), q.horizons).select(
        pl.lit(q.report_month, dtype=pl.Date).alias("as_of_month"), pl.all()
    )


def _queue_meta(frame: pl.DataFrame, ctx: MartContext) -> dict[str, Any]:
    q = marts_queue.adjusted_queue(ctx)
    return {
        "report_month": q.report_month.isoformat(),
        "queue_as_of": q.as_of.isoformat(),
        "horizons": {h: f"{h}-12-31" for h in q.horizons},
        "horizon_months": q.horizons,
        "model": marts_config.value(ctx.config, "queue.model"),
        "large_gas_min_mw": LARGE_GAS_MIN_MW,
    }


# --- new data-center sites (Q4) -----------------------------------------------------------------------------

METRO_RULES = {"county_list_13": "metro_legacy", "density_100": "metro_density"}


def iso_class(dominant: pl.Expr) -> pl.Expr:
    """``ercot`` (ERCOT only), ``mixed`` (ERCOT and another ISO) or ``outside`` (no ERCOT), from
    ``county_iso_share.dominant_iso_rto`` (e.g. ``ERCOT,SPP``); null without a row."""
    parts = dominant.str.split(",")
    return (
        pl.when(dominant.is_null()).then(pl.lit(None, pl.Utf8))
        .when(~parts.list.contains("ERCOT")).then(pl.lit("outside"))
        .when(parts.list.len() == 1).then(pl.lit("ercot"))
        .otherwise(pl.lit("mixed"))
    )


def site_rows(flagged: pl.DataFrame, counties: pl.DataFrame, *, metro_rule: str,
              include_naics_only: bool) -> pl.DataFrame:
    """Q4's site table in the contract's names. ``flagged``: ``select_new_sites`` → ``classify_sites`` → the
    ``county_iso_share`` join → ``flag_metro`` → ``city_muni_match``; ``counties``: ``county_fips``, ``county``
    (the tx_counties name). ``matched_by`` is ``name`` when the site name matches (else NAICS 518210 only);
    ``in_ercot`` = the county's dominant ISO class includes ERCOT (Q4's ERCOT cut); ``metro`` follows
    ``metro_rule``; NAICS-only sites are dropped unless ``include_naics_only``."""
    if metro_rule not in METRO_RULES:
        raise ValueError(f"metro_rule {metro_rule!r} not in {sorted(METRO_RULES)}")
    out = flagged.join(counties.select("county_fips", "county"), on="county_fips", how="left").select(
        pl.col("ref_num_txt").alias("tceq_rn"),
        pl.col("reg_ent_name").alias("site_name"),
        "county_fips",
        pl.coalesce("county", "county_name").alias("county_name"),
        "city",
        "city_muni",
        pl.col("first_affil_begin_dt").alias("first_permit_date"),
        pl.when(pl.col("matched_by_name")).then(pl.lit("name")).otherwise(pl.lit("naics")).alias("matched_by"),
        "has_undated_affiliation",
        "largest_type",
        pl.col("coop_w").alias("coop_share_w"),
        pl.col("muni_w").alias("muni_share_w"),
        pl.col("iou_w").alias("iou_share_w"),
        "dominant_iso_rto",
        iso_class(pl.col("dominant_iso_rto")).alias("iso_class"),
        pl.col("dominant_iso_rto").str.split(",").list.contains("ERCOT").fill_null(False).alias("in_ercot"),
        "density_per_km2",
        "metro_legacy",
        "metro_density",
        pl.col(METRO_RULES[metro_rule]).alias("metro"),
    )
    if not include_naics_only:
        out = out.filter(pl.col("matched_by") == "name")
    return out.sort("first_permit_date", "tceq_rn")


def build_dc_sites(ctx: MartContext) -> pl.DataFrame:
    from basecast_pipelines.models import data_centers as DC

    overlap = DC.load_overlap()
    counties = DC.load_counties()
    density = DC.county_density(DC.load_population(2025), counties)
    new = DC.select_new_sites(DC.load_sites())
    classified = DC.classify_sites(new, DC.county_territory_types(overlap)).join(
        DC.load_iso_share(), on="county_fips", how="left"
    )
    flagged = DC.city_muni_match(DC.flag_metro(classified, density), overlap)
    return site_rows(flagged, counties, metro_rule=marts_config.value(ctx.config, "dc_sites.metro_rule"),
                     include_naics_only=marts_config.value(ctx.config, "dc_sites.include_naics_only"))


def _dc_meta(frame: pl.DataFrame, ctx: MartContext) -> dict[str, Any]:
    from basecast_pipelines.models import data_centers as DC

    return {
        "since": DC.SINCE.isoformat(),
        "metro_rule": marts_config.value(ctx.config, "dc_sites.metro_rule"),
        "include_naics_only": marts_config.value(ctx.config, "dc_sites.include_naics_only"),
        "density_threshold_per_km2": DC.DENSITY_THRESHOLD_PER_KM2,
        "latest_permit_date": frame["first_permit_date"].max(),
    }


# --- X14's zone inputs: X1 excess, X12 normalized peak, RTM load-zone spread ----------------------------------

X1_FIRST_YEAR = 2003
X1_FEATURE = "t_mean_3d"
X12_TARGET = "dmax"
X12_TRAIN, X12_TEST = (2020, 2022), (2023,)  # X12 cell 1: the variant is chosen on a 2023 validation year
SPP_SOURCE = "ercot_spp_hist"
RTM_FILE = re.compile(r"RTMLZHBSPP_(\d{4})", re.IGNORECASE)


def _x12_variants() -> dict[str, Any]:
    """X12 cell 1's candidate specs, in its order."""
    from basecast_pipelines.models import weather_normalized as wn

    return {
        "t spline": wn.Spec(lag=False, humid=False),
        "+ lag": wn.Spec(lag=True, humid=False),
        "+ lag + dew point": wn.Spec(lag=True, humid=True),
        "+ lag + dew point + tmax": wn.Spec(lag=True, humid=True, tmax=True),
        "+ lag + dew point, level knots 6 mo": wn.Spec(level_knots_per_year=2),
        "+ lag + dew point, fourier 3": wn.Spec(fourier=3),
    }


def _hourly(ctx: MartContext) -> tuple[pl.DataFrame, pl.DataFrame]:
    """``ercot_load_hourly_wz`` and ``weather_hourly_wz``, read once per run."""
    from basecast_pipelines.models import weather_load as wl

    return (ctx.cached("weather_load.hourly_load", wl.load_hourly_load),
            ctx.cached("weather_load.hourly_weather", wl.load_hourly_weather))


def peak_excess(ctx: MartContext) -> pl.DataFrame:
    """X1 §1a: each zone's (and ERCOT's) summer peak over the pre-break (≤ 2019) weather + trend fit, per year."""

    def build() -> pl.DataFrame:
        from basecast_pipelines.models import peak_excess as px
        from basecast_pipelines.models import weather_load as wl

        load, weather = _hourly(ctx)
        daily_w = wl.weather_daily(weather)
        weights = wl.zone_weights(load, range(X1_FIRST_YEAR, px.PRE_BREAK_UNTIL + 1))
        daily_all = pl.concat([daily_w.drop("n_hours"), wl.system_weather_daily(daily_w, weights).drop("n_hours")])
        features = wl.summer_weather_features(daily_all)
        panel = (
            wl.summer_peaks(load).select("weather_zone", "year", "peak_mw")
            .join(features, on=["weather_zone", "year"], how="inner")
            .filter(pl.col("year") >= X1_FIRST_YEAR)
        )
        return px.excess_vs_prebreak(panel, "peak_mw", X1_FEATURE)

    return ctx.cached("x1.excess_peak", build)


def normalized_summer_peak(ctx: MartContext) -> tuple[pl.DataFrame, str]:
    """X12 §7: per zone (and ERCOT) and summer, P10 / P50 / P90 of the season's daily-peak maximum under the 20
    normal weather years, with X12's variant choice for the daily peak (the lowest re-leveled MAPE of the nine
    series, trained 2020–2022, validated on 2023). Returns the quantiles and the chosen variant's name."""

    def build() -> tuple[pl.DataFrame, str]:
        from basecast_pipelines.models import weather_load as wl
        from basecast_pipelines.models import weather_normalized as wn

        load, weather = _hourly(ctx)
        panel, feats = wn.build_panel(load, weather)
        normals = wn.normal_weather(feats, wn.NORMAL_YEARS)
        series = (*wl.WEATHER_ZONES, wl.TOTAL)
        variants = _x12_variants()
        mape = []
        for name, spec in variants.items():
            errs = [wn.holdout(panel, z, X12_TARGET, spec, train=X12_TRAIN, test=X12_TEST) for z in series]
            mape.append((float(wn.holdout_mape(pl.concat(errs))["mape_relevel"].mean()), name))
        chosen = min(mape)[1]
        days, draws = [], []
        for z in series:
            d, w, _ = wn.normalize_zone(panel, z, X12_TARGET, normals, variants[chosen],
                                        draw_months=wl.SUMMER_MONTHS)
            days.append(d)
            if w.height:
                draws.append(w)
        summer = pl.concat(days).filter(pl.col("date").dt.month().is_in(wl.SUMMER_MONTHS))
        return wn.peak_quantiles(wn.normal_season_peak(summer, pl.concat(draws))), chosen

    return ctx.cached("x12.summer_peak", build)


def newest_by_year(files: Sequence[Any], years: Iterable[int], pattern: re.Pattern = RTM_FILE) -> dict[int, Any]:
    """The newest raw file of each year's workbook (``files`` oldest first, as ``list_raw_files`` returns them;
    the year is ``pattern``'s first group in the file name). Raises when a year has none."""
    newest: dict[int, Any] = {}
    for f in files:
        m = pattern.search(f.name)
        if m and int(m.group(1)) in set(years):
            newest[int(m.group(1))] = f
    missing = sorted(set(years) - set(newest))
    if missing:
        raise FileNotFoundError(f"no {pattern.pattern} file in the lake for {missing}")
    return dict(sorted(newest.items()))


def rtm_load_zone_prices(ctx: MartContext, years: Iterable[int]) -> pl.DataFrame:
    """RTM 15-minute load-zone prices (the four ``LZ`` points of ``acquisition_zones.LOAD_ZONES``) for ``years``,
    parsed from the lake's newest ``RTMLZHBSPP_<year>`` workbook of each year, as X14 does."""
    years = tuple(years)

    def build() -> pl.DataFrame:
        from basecast_pipelines.common.storage import storage_from_uri
        from basecast_pipelines.config import load_settings
        from basecast_pipelines.models import acquisition_zones as Z
        from basecast_pipelines.parsers.ercot.spp_hist import parse_rtm
        from basecast_pipelines.processing.core import list_raw_files

        storage = storage_from_uri(load_settings().storage_root)
        frames = []
        for f in newest_by_year(list_raw_files(storage, SPP_SOURCE), years).values():
            df = parse_rtm(f)
            if df is None:
                raise ValueError(f"{f.key}: not an RTM workbook")
            frames.append(
                df.filter(pl.col("settlement_point").is_in(Z.LOAD_ZONES) & (pl.col("settlement_point_type") == "LZ"))
                .select("settlement_point", pl.col("delivery_date").alias("day"), "interval_start_utc",
                        "price_usd_mwh")
            )
        return pl.concat(frames).unique(["settlement_point", "interval_start_utc"], keep="last")

    return ctx.cached(f"x14.rtm_lz.{'-'.join(map(str, years))}", build)


def mean_excess(excess: pl.DataFrame, years: Iterable[int], zones: Iterable[str]) -> dict[str, float]:
    """Mean ``excess_pct`` over ``years`` per zone (X14's ``ll_pressure``)."""
    return dict(
        excess.filter(pl.col("year").is_in(list(years)) & pl.col("weather_zone").is_in(list(zones)))
        .group_by("weather_zone")
        .agg(pl.col("excess_pct").mean())
        .sort("weather_zone")
        .iter_rows()
    )


def peak_cagr(peaks: pl.DataFrame, start: int, end: int, zones: Iterable[str]) -> dict[str, float]:
    """CAGR of the normalized P50 summer peak from ``start`` to ``end`` per zone (X14's ``zone_peak_cagr``); a
    zone without a positive P50 in both years raises (X14 fails there too)."""
    from basecast_pipelines.models import acquisition_zones as Z

    p50 = {(r["weather_zone"], r["year"]): r["p50"] for r in peaks.iter_rows(named=True)}
    out = {z: Z.cagr(p50[(z, start)], p50[(z, end)], end - start) for z in zones}
    bad = sorted(z for z, v in out.items() if v is None)
    if bad:
        raise ValueError(f"no summer-peak CAGR {start}→{end} for {bad}")
    return {z: float(v) for z, v in out.items() if v is not None}


# --- county acquisition priority (X14) ----------------------------------------------------------------------

POP_START, POP_END = 2020, 2025  # Census PEP vintage 2025
PERMIT_YEARS = [2023, 2024, 2025]
HOUSING_VINTAGE = 2024  # ACS 5-year B25032
DC_SINCE = date(2025, 1, 1)
PEAK_START, PEAK_END = 2021, 2026  # X12 normalized summer peak P50, CAGR window
EXCESS_YEARS = [2023, 2024, 2025, 2026]  # X1 excess over the pre-break fit, mean of excess_pct
PRICE_YEARS = [2023, 2024, 2025]  # RTM load-zone prices, full years

SIGNALS = {  # signal -> (label, block, unit); the weights come from acquisition_zones.MARKET / GRID
    "addr_sf_homes": ("Owner-occupied single-family homes × addressable (ERCOT) share", "market", "homes"),
    "pop_growth": ("Population growth 2020→2025", "market", "share"),
    "permits_per_1k": ("Permits 2023–2025 per 1,000 residents", "market", "units/1k"),
    "owner_sf_share": ("Owner-occupied single-family share", "market", "share"),
    "zone_peak_cagr": ("Weather-normalized zone summer peak CAGR 2021→2026", "grid", "per year"),
    "ll_pressure": ("Zone summer-peak excess over the pre-2020 fit, mean 2023–2026", "grid", "%"),
    "lz_spread": ("Load-zone daily 2-hour RTM price spread, mean 2023–2025", "grid", "USD/MWh"),
    "dc_sites": ("New data-center sites since 2025", "grid", "sites"),
}
ACQUISITION_COLUMNS = (
    "county_fips", "county_name", "weather_zone", "rank", "priority", "priority_class", "market_score", "grid_score",
    "grid_factor", "channel", "partner_type", "retail_share", "coop_share", "muni_share", "outside_share",
    "partner_share", "addressable_share", "n_partners", "top_partner_share", "retail_priority", "retail_rank",
    "partner_priority", "partner_rank", "drivers", "drags",
)
CONTEXT_COLUMNS = ("owner_sf_homes", "population", "density_per_km2")


@dataclass(frozen=True)
class Acquisition:
    scored: pl.DataFrame  # one row per ERCOT county (ACQUISITION_COLUMNS, the signals, pct_*, context)
    breaks: list[float]  # the legend's quintile breaks
    zone_inputs: dict[str, dict[str, float]]  # signal -> weather zone -> value
    load_zone_spread: dict[str, float]
    x12_variant: str


def score_signals(signals: pl.DataFrame) -> tuple[pl.DataFrame, list[float]]:
    """X14's score on its signal frame: ``score_counties`` → ``channel_priority`` → ``drivers`` →
    ``legend_classes``. ``drivers`` / ``drags`` become lists (empty when none); returns the frame and the
    breaks."""
    from basecast_pipelines.models import acquisition_zones as Z

    scored = Z.channel_priority(Z.score_counties(signals))
    scored = scored.join(Z.drivers(scored), on="county_fips", how="left")
    breaks, classes = Z.legend_classes(scored["priority"])
    empty = pl.lit([], dtype=pl.List(pl.Utf8))
    scored = scored.with_columns(
        classes.alias("priority_class"),
        *[pl.col(c).str.split(", ").fill_null(empty).alias(c) for c in ("drivers", "drags")],
    )
    return scored, breaks


def acquisition(ctx: MartContext) -> Acquisition:
    return ctx.cached("explorer.acquisition", lambda: _acquisition(ctx))


def _acquisition(ctx: MartContext) -> Acquisition:
    from basecast_pipelines.models import accounts as A
    from basecast_pipelines.models import acquisition_zones as Z
    from basecast_pipelines.models import data_centers as DC
    from basecast_pipelines.models import large_load_geo as LLG

    counties = Z.load_ercot_counties()
    ercot = counties.filter(pl.col("in_ercot").fill_null(False))
    channels = Z.channel_split(Z.load_overlap())

    # Homeowner market, one "account" per county (the accounts.py builders over county_links)
    links = Z.county_links(counties.select("county_fips", "land_km2"))
    population = A.load_population()
    pop = A.population_growth(population, links, start=POP_START, end=POP_END)
    permits = A.load_permits_annual(PERMIT_YEARS).group_by("county_fips").agg(
        pl.col("units_total").sum().alias("units")
    )
    market = (
        pop.join(A.permits_per_1k(permits, pop, links), on="account_id", how="full", coalesce=True)
        .join(A.owner_single_family(A.load_housing(HOUSING_VINTAGE), links), on="account_id", how="full",
              coalesce=True)
        .join(A.data_center_sites(A.load_dc_sites(), links, since=DC_SINCE), on="account_id", how="full",
              coalesce=True)
        .rename({"account_id": "county_fips"})
    )

    # Grid value: zone signals carried to counties by their weather-zone mix
    czl = LLG.county_zone_long(ercot)
    peaks, x12_variant = normalized_summer_peak(ctx)
    zone_cagr = peak_cagr(peaks, PEAK_START, PEAK_END, Z.ZONES)
    excess = mean_excess(peak_excess(ctx), EXCESS_YEARS, Z.ZONES)
    spread = Z.daily_spread(rtm_load_zone_prices(ctx, PRICE_YEARS))
    lz_mean = dict(spread.group_by("settlement_point").agg(pl.col("spread_usd_mwh").mean()).sort("settlement_point")
                   .iter_rows())
    lz_wz = Z.weather_zone_values(lz_mean)
    grid = (
        Z.zone_to_county(zone_cagr, czl, "zone_peak_cagr")
        .join(Z.zone_to_county(excess, czl, "ll_pressure"), on="county_fips", how="full", coalesce=True)
        .join(Z.zone_to_county(lz_wz, czl, "lz_spread"), on="county_fips", how="full", coalesce=True)
    )

    # Density, for the tooltip (Q4's rule, ≥ 100/km² = metro)
    density = DC.county_density(
        population.filter(pl.col("year") == POP_END).select("county_fips", pl.col("value").alias("population")),
        counties.select("county_fips", (pl.col("land_km2") * 1e6).alias("aland_m2")),
    ).select("county_fips", "density_per_km2")

    signals = (
        ercot.select("county_fips", "county_name", "weather_zone")
        .join(channels, on="county_fips", how="left")
        .join(market, on="county_fips", how="left")
        .join(grid, on="county_fips", how="left")
        .join(density, on="county_fips", how="left")
        .with_columns(
            (pl.col("owner_sf_homes") * pl.col("addressable_share").fill_null(0)).alias("addr_sf_homes"),
            pl.col("dc_sites").fill_null(0.0),
        )
    )
    scored, breaks = score_signals(signals)
    names = list(Z.WEIGHTS)
    frame = scored.select(
        *ACQUISITION_COLUMNS, *names, *[f"pct_{c}" for c in names], *CONTEXT_COLUMNS
    ).with_columns(pl.col("n_partners").cast(pl.Int64), pl.col("priority_class").cast(pl.Int64)).sort("county_fips")
    zone_inputs = {"zone_peak_cagr": zone_cagr, "ll_pressure": excess, "lz_spread": lz_wz}
    return Acquisition(frame, breaks, zone_inputs, lz_mean, x12_variant)


def build_acquisition(ctx: MartContext) -> pl.DataFrame:
    return acquisition(ctx).scored


def _acquisition_meta(frame: pl.DataFrame, ctx: MartContext) -> dict[str, Any]:
    from basecast_pipelines.models import acquisition_zones as Z

    a = acquisition(ctx)
    weights = {**Z.MARKET, **Z.GRID}
    return {
        "legend_breaks": a.breaks,
        "grid_tilt": Z.GRID_TILT,
        "signals": [{"signal": s, "label": lab, "block": block, "unit": unit, "weight": weights[s]}
                    for s, (lab, block, unit) in SIGNALS.items()],
        "channel_min_share": Z.CHANNEL_MIN_SHARE,
        "dominant_share": Z.DOMINANT_SHARE,
        "zone_inputs": a.zone_inputs,
        "load_zone_spread_usd_mwh": a.load_zone_spread,
        "weather_zone_to_load_zone": Z.WZ_TO_LZ,
        "x12_variant": a.x12_variant,
    }


# --- checks --------------------------------------------------------------------------------------------------


def _golden(name, get, expected, **kw):
    return value_check(name, get, expected, as_of=GOLDEN_AS_OF, **kw)


def _all(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.filter(pl.col("stratum") == ALL)


def _strata_add_up(frame: pl.DataFrame) -> bool:
    parts = frame.filter(pl.col("stratum") != ALL).group_by("county_fips").agg(
        pl.col("projects").sum().alias("_n"), pl.col("raw_mw").sum().alias("_mw"))
    j = _all(frame).join(parts, on="county_fips", how="full")
    return bool(((j["projects"] == j["_n"]) & ((j["raw_mw"] - j["_mw"]).abs() < 1e-6)).all())


def _naics_default(config) -> bool:
    """Q4's counts include the NAICS-only matches."""
    return bool(marts_config.value(config, "dc_sites.include_naics_only"))


def _q4(name, get, expected, **kw):
    return value_check(name, get, expected, as_of=GOLDEN_AS_OF, applies=_naics_default, **kw)


def _ercot(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.filter(pl.col("in_ercot"))


def _county(frame: pl.DataFrame, name: str, col: str):
    return frame.filter(pl.col("county_name") == name)[col].item()


def _first(frame: pl.DataFrame, rank_col: str) -> str:
    return frame.filter(pl.col(rank_col) == 1)["county_name"].item()


def _channels(channel: str):
    return lambda f: f.filter(pl.col("channel") == channel).height


PROJECT_CHECKS = (
    value_check("unique inr", lambda f: f["inr"].is_duplicated().any(), False),
    value_check("stages entry | ia", lambda f: set(f["stage"].unique().to_list()) <= {"entry", "ia"}, True),
    value_check("every project has a county", lambda f: f["county_fips"].null_count(), 0),
    value_check("probabilities in [0, 1]", lambda f: bool(f.select(
        pl.all_horizontal(pl.col("^p_cod_.*$").is_between(0, 1)).all()).item()), True),
    _golden("1,810 active projects", lambda f: f.height, 1810),
)

COUNTY_CHECKS = (
    value_check("unique county × stratum", lambda f: f.select("county_fips", "stratum").is_duplicated().any(), False),
    value_check("strata add up to all", _strata_add_up, True),
    _golden("1,810 projects (all)", lambda f: _all(f)["projects"].sum(), 1810),
    _golden("438,262 MW raw (all)", lambda f: _all(f)["raw_mw"].sum(), 438_262, tol=1),
    _golden("38,689 MW adjusted, Dec 2027 (all)", lambda f: _all(f)["adj_mw_2027"].sum(), 38_689, tol=1),
    _golden("70,394 MW adjusted, Dec 2028 (all)", lambda f: _all(f)["adj_mw_2028"].sum(), 70_394, tol=1),
    _golden("194 counties with an active project", lambda f: _all(f).height, 194),
)

DC_CHECKS = (
    value_check("unique tceq_rn", lambda f: f["tceq_rn"].is_duplicated().any(), False),
    _q4("38 new sites", lambda f: f.height, 38),
    _q4("in 27 counties", lambda f: f["county_fips"].n_unique(), 27),
    _q4("31 sites in ERCOT counties", lambda f: _ercot(f).height, 31),
    _q4("in 22 ERCOT counties", lambda f: _ercot(f)["county_fips"].n_unique(), 22),
    _q4("15 name matches (23 NAICS only)", lambda f: f.filter(pl.col("matched_by") == "name").height, 15),
    _q4("co-op share 46.6% (county_share-weighted)", lambda f: f["coop_share_w"].fill_null(0).mean(), 0.466,
        tol=0.0005),
    _q4("35 outside the 13-county metro list", lambda f: f.filter(~pl.col("metro_legacy")).height, 35),
    _q4("28 outside metro by density", lambda f: f.filter(~pl.col("metro_density")).height, 28),
)

ACQUISITION_CHECKS = (
    value_check("unique county_fips", lambda f: f["county_fips"].is_duplicated().any(), False),
    value_check("every county has a channel", lambda f: f["channel"].null_count(), 0),
    value_check("every county has a priority", lambda f: f["priority"].null_count(), 0),
    _golden("204 ERCOT counties", lambda f: f.height, 204),
    _golden("161 partnership", _channels("partnership"), 161),
    _golden("39 retail-direct", _channels("retail_direct"), 39),
    _golden("4 mixed", _channels("mixed"), 4),
    _golden("#1 overall is Comal", lambda f: _first(f, "rank"), "Comal"),
    _golden("Comal priority 0.76", lambda f: _county(f, "Comal", "priority"), 0.76, tol=0.005),
    _golden("#2 overall is Williamson (0.73)", lambda f: f.filter(pl.col("rank") == 2)["county_name"].item(),
            "Williamson"),
    _golden("retail-direct #1 is Williamson", lambda f: _first(f, "retail_rank"), "Williamson"),
    _golden("partnership #1 is Comal", lambda f: _first(f, "partner_rank"), "Comal"),
    _golden("legend break 1 (0.258)", lambda f: f["priority"].quantile(0.2, "linear"), 0.258, tol=0.0005),
    _golden("legend break 4 (0.530)", lambda f: f["priority"].quantile(0.8, "linear"), 0.530, tol=0.0005),
    _golden("NORTH peak CAGR 12.0% (the highest)", lambda f: f["zone_peak_cagr"].max(), 0.120, tol=0.0005),
    _golden("NORTH excess 157.9% (the highest)", lambda f: f["ll_pressure"].max(), 157.9, tol=0.05),
    _golden("LZ_WEST spread $170.2 (the highest)", lambda f: f["lz_spread"].max(), 170.2, tol=0.05),
)


# --- marts ---------------------------------------------------------------------------------------------------

QUEUE_PROJECT_SCORES = Mart(
    name="mart_queue_project_scores",
    build=build_project_scores,
    key=("as_of_month", "inr"),
    inputs=("gis_project_events", "gis_snapshots", "tx_counties", "county_weather_zone"),
    description="Every active project of the latest GIS report with its chance of reaching COD by Dec 2027 and Dec "
    "2028 (stage, months at the stage, MW-weighted curves, semi-Markov entry stage) and its expected MW (X2).",
    caveats=("beyond_backtested_window",),
    checks=PROJECT_CHECKS,
    meta=_queue_meta,
)

QUEUE_ADJUSTED_COUNTY = Mart(
    name="mart_queue_adjusted_county",
    build=build_queue_county,
    key=("as_of_month", "county_fips", "stratum"),
    inputs=("mart_queue_project_scores",),
    description="The generation queue by county and stratum (plus all): raw MW vs expected MW reaching COD by Dec "
    "2027 / Dec 2028, IA-stage share, ranks and large new gas (X2).",
    caveats=("beyond_backtested_window",),
    checks=COUNTY_CHECKS,
    meta=_queue_meta,
)

DATA_CENTER_SITES_NEW = Mart(
    name="mart_data_center_sites_new",
    build=build_dc_sites,
    key=("tceq_rn",),
    inputs=("tceq_data_center_sites", "county_utility_overlap_puct", "county_iso_share", "tx_counties",
            "census_population_county"),
    description="TCEQ data-center sites with a first permit since 2025, with their county's territory mix, ISO "
    "class and metro status (Q4); by county, not by point.",
    caveats=("by_county_not_point",),
    checks=DC_CHECKS,
    meta=_dc_meta,
)

COUNTY_ACQUISITION = Mart(
    name="mart_county_acquisition",
    build=build_acquisition,
    key=("county_fips",),
    inputs=("tx_counties", "county_weather_zone", "county_utility_overlap_puct", "census_population_county",
            "census_permits_county", "census_housing_county", "tceq_data_center_sites", "ercot_load_hourly_wz",
            "weather_hourly_wz", "prices_rtm_hub_lz"),
    description="Acquisition priority of the 204 ERCOT counties: homeowner market × grid factor, the channel split "
    "(retail-direct, partnership, mixed) by land area, each channel's list rank, drivers and the 8 signals (X14).",
    caveats=("weights_pending_review", "by_area_not_homes"),
    checks=ACQUISITION_CHECKS,
    meta=_acquisition_meta,
)

MARTS = (QUEUE_PROJECT_SCORES, QUEUE_ADJUSTED_COUNTY, DATA_CENTER_SITES_NEW, COUNTY_ACQUISITION)
