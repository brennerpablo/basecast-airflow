"""Zone layer marts (A-M4, P1): where flat load arrived and where the large loads are, by weather zone, plus the
counties with large load. get-data's contract splits BUILD_A's ``mart_zone_layers`` in two (``GET /geo/zones``).

- ``mart_zone_layers`` (weather zone × measure), from two analyses:

  - ``analysis/x1_peak_excess.py`` §1b: ``excess_share``, each zone's share of the ERCOT coincident excess over the
    pre-break (≤ 2019) weather + trend fit, mean 2023–2026 (not clipped: a zone below its fit has a negative share).
    The coincident panel is ``marts/forecast.py``'s ``organic_inputs().cpanel`` (X1's own construction).
  - X1 §2a: ``min_max_ratio_2019`` / ``min_max_ratio_2026``, the average summer day's minimum ÷ maximum load
    (``peak_excess.summer_shape``, Jun 1 – Sep 20 every year).
  - ``analysis/x11_large_load_geography.py``, through ``marts/forecast.py``'s ``zone_split`` (the split the zone
    forecast uses): ``a2e_stock``, the latest deck's approved stock by the raked Batch Zero base split (band = min /
    max over f ∈ {0, f, 1} and the unraked split, X11 §2a); ``pipeline_2032``, the Batch Zero base + studied load in
    2032 as the deck prints it by zone (X11 §2b, no band); ``u_share``, the unattributed layer's residual split (X11
    §3; X11 gives it no band, so ``low`` / ``high`` stay null).

- ``mart_county_large_load`` (ERCOT county): X11 §5. The approved stock of each zone spread over its counties by
  their share of the zone's data-center signals (TCEQ sites + Comptroller agreements + Comptroller registrations
  with a county); a county without a signal gets 0. Plus the counties the Batch Zero deck names (p. 7, top base and
  top base + studied), with the deck's MW, which the map draws as observed points. The pipeline is never spread by
  permits (X11 §5: Cameron, Jones and Childress hold 26 GW with no signal).

Every X11 value comes from machine-read decks (``verified = false``); X1's come from ERCOT's load archive.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any

import polars as pl

from basecast_pipelines.marts import forecast as F
from basecast_pipelines.marts.core import Check, CheckResult, Mart, MartContext, value_check
from basecast_pipelines.models.weather_load import WEATHER_ZONES as ZONES

GOLDEN_AS_OF = date(2026, 9, 26)  # X1 / X11's run date (X11 refreshed after the X16 #3 fix)

EXCESS_YEARS = (2023, 2024, 2025, 2026)  # X1 §1: the share row of the coincident table
RATIO_YEARS = (2019, 2026)  # X1 §2: the average day's min/max ratio before the break and now
STOCK_CENTRAL = "raked_f_est"  # zone_split's central stock split
SITES_SINCE = date(2025, 1, 1)  # X11 §5's recent-site count (context only, not a weight)
LOAD_CACHE = "weather_load.hourly_load"  # the key explorer.py's hourly read uses

# measure -> (label, unit, method, source)
MEASURES: dict[str, tuple[str, str, str, str]] = {
    "excess_share": ("Share of the summer-peak excess over weather + trend, at the ERCOT peak hour, mean 2023–2026",
                     "share", "observed", "X1 §1"),
    "min_max_ratio_2019": ("Average summer day's minimum ÷ maximum load, 2019 (Jun 1–Sep 20)", "ratio", "observed",
                           "X1 §2"),
    "min_max_ratio_2026": ("Average summer day's minimum ÷ maximum load, 2026 (Jun 1–Sep 20)", "ratio", "observed",
                           "X1 §2"),
    "a2e_stock": ("Approved-to-energize large load, allocated by zone", "MW", "allocated", "X11 §2a"),
    "pipeline_2032": ("Large-load pipeline in 2032 (Batch Zero base + studied), by zone", "MW", "observed by zone",
                      "X11 §2b"),
    "u_share": ("Share of the unattributed flat load (zone excess left after the allocated large load)", "share",
                "allocated", "X11 §3"),
}
ZONE_SCHEMA = {
    "weather_zone": pl.String, "measure": pl.String, "label": pl.String, "unit": pl.String, "method": pl.String,
    "central": pl.Float64, "low": pl.Float64, "high": pl.Float64, "verified": pl.Boolean, "source": pl.String,
    "lz_west_fraction": pl.Float64,
}
SIGNAL_COLUMNS = ("tceq_sites", "tceq_sites_since_2025", "cpa_dc_agreements", "cpa_dc_registrations")
COUNTY_COLUMNS = ("county_fips", "county_name", "weather_zone", "allocated_a2e_mw", "named_by_ercot",
                  "observed_base_mw", "observed_base_studied_mw", "n_signals", *SIGNAL_COLUMNS, "verified")


# --- pure helpers (tested in tests/marts/test_zones.py) ----------------------------------------------------------


def excess_shares(excess: pl.DataFrame, years: Sequence[int],
                  zones: Sequence[str] = ZONES) -> tuple[dict[str, float], dict[str, float]]:
    """X1 §1b: each zone's mean coincident excess over ``years`` (MW) and its share of the zones' sum, not clipped.
    ``excess``: ``peak_excess.excess_vs_prebreak`` on the coincident panel (``weather_zone``, ``year``,
    ``excess_mw``)."""
    rows = excess.filter(pl.col("year").is_in(list(years)) & pl.col("weather_zone").is_in(list(zones)))
    mean = dict(rows.group_by("weather_zone").agg(pl.col("excess_mw").mean()).iter_rows())
    missing = sorted(set(zones) - set(mean))
    if missing:
        raise ValueError(f"no coincident excess in {list(years)} for {missing}")
    total = sum(mean.values())
    if total == 0:
        raise ValueError("the zones' coincident excess adds up to 0: no shares")
    return {z: float(mean[z]) for z in zones}, {z: float(mean[z] / total) for z in zones}


def min_max_ratios(shape: pl.DataFrame, year: int, zones: Sequence[str] = ZONES) -> dict[str, float]:
    """X1 §2a: the average summer day's min/max load ratio per zone in ``year`` (``peak_excess.summer_shape``)."""
    rows = shape.filter((pl.col("year") == year) & pl.col("weather_zone").is_in(list(zones)))
    out = dict(rows.select("weather_zone", "min_max_ratio").iter_rows())
    missing = sorted(set(zones) - set(out))
    if missing:
        raise ValueError(f"no summer shape in {year} for {missing}")
    return {z: float(out[z]) for z in zones}


def measure_rows(measure: str, central: Mapping[str, float], *, low: Mapping[str, float] | None = None,
                 high: Mapping[str, float] | None = None, verified: bool,
                 extra: Mapping[str, Mapping[str, Any]] | None = None) -> list[dict[str, Any]]:
    """One row per zone of ``measure`` with its label, unit, method and source from ``MEASURES``; ``low`` / ``high``
    None = no range; ``extra``: column -> zone -> value."""
    label, unit, method, source = MEASURES[measure]
    return [
        {"weather_zone": z, "measure": measure, "label": label, "unit": unit, "method": method,
         "central": float(central[z]), "low": None if low is None else float(low[z]),
         "high": None if high is None else float(high[z]), "verified": verified, "source": source,
         **{col: values.get(z) for col, values in (extra or {}).items()}}
        for z in ZONES
    ]


def zone_frame(rows: Sequence[Mapping[str, Any]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=ZONE_SCHEMA).sort("measure", "weather_zone")


def approved_stock(lzs: pl.DataFrame) -> dict[str, Any]:
    """X11 §2a's stock to allocate: the latest deck's approved stock (LZ_WEST + Other) and its LZ_WEST share.
    ``lzs``: ``forecast.lz_split``'s table (sorted by ``report_date``)."""
    if lzs.is_empty():
        raise ValueError("no approved stock by load zone in the decks")
    last = lzs.row(-1, named=True)
    return {"report_date": last["report_date"], "mw": float(last["lz_west"] + last["other"]),
            "lz_west_mw": float(last["lz_west"]), "other_mw": float(last["other"]),
            "lz_west_share": float(last["lz_west_share"]), "src": last["src"]}


def county_signals(sites: pl.DataFrame, agreements: pl.DataFrame, registrations: pl.DataFrame, *,
                   since: date = SITES_SINCE) -> pl.DataFrame:
    """X11 §5: data-center signals per county: TCEQ sites (and those first permitted since ``since``), Comptroller
    agreements naming a data center and Comptroller registrations with a county; ``n_signals`` = sites + agreements
    + registrations (every signal weighs 1, whatever its date)."""
    zero = pl.lit(0, dtype=pl.Int64)
    one = pl.lit(1, dtype=pl.Int64)
    parts = [
        sites.select("county_fips", one.alias("tceq_sites"),
                     (pl.col("first_affil_begin_dt") >= since).fill_null(False).cast(pl.Int64)
                     .alias("tceq_sites_since_2025"), zero.alias("cpa_dc_agreements"),
                     zero.alias("cpa_dc_registrations")),
        agreements.select("county_fips", zero.alias("tceq_sites"), zero.alias("tceq_sites_since_2025"),
                          one.alias("cpa_dc_agreements"), zero.alias("cpa_dc_registrations")),
        registrations.drop_nulls("county_fips").select("county_fips", zero.alias("tceq_sites"),
                                                       zero.alias("tceq_sites_since_2025"),
                                                       zero.alias("cpa_dc_agreements"),
                                                       one.alias("cpa_dc_registrations")),
    ]
    return (
        pl.concat(parts, how="vertical_relaxed")
        .group_by("county_fips").agg(pl.all().sum())
        .with_columns((pl.col("tceq_sites") + pl.col("cpa_dc_agreements") + pl.col("cpa_dc_registrations"))
                      .alias("n_signals"))
        .sort("county_fips")
    )


def named_counties(cv: pl.DataFrame, document: str = F.BZ_DOC) -> pl.DataFrame:
    """The counties the Batch Zero deck names ("Top base load counties", "Top base + studied load counties"):
    ``county_name``, ``observed_base_mw``, ``observed_base_studied_mw`` (null when the county is only in the other
    list)."""
    rows = (
        cv.filter((pl.col("document") == document) & pl.col("chart_title").str.contains("(?i)counties")
                  & pl.col("value_mw").is_not_null())
        .with_columns(pl.col("category").str.strip_chars(),
                      pl.when(pl.col("status_label").str.contains("(?i)studied"))
                      .then(pl.lit("observed_base_studied_mw")).otherwise(pl.lit("observed_base_mw")).alias("_col"))
    )
    if rows.is_empty():
        raise ValueError(f"no named counties in {document}")
    wide = rows.group_by("category", "_col").agg(pl.col("value_mw").first()).pivot(
        on="_col", index="category", values="value_mw")
    for c in ("observed_base_mw", "observed_base_studied_mw"):
        if c not in wide.columns:
            wide = wide.with_columns(pl.lit(None, dtype=pl.Float64).alias(c))
    return wide.select(pl.col("category").alias("county_name"), "observed_base_mw",
                       "observed_base_studied_mw").sort("county_name")


def county_rows(counties: pl.DataFrame, signals: pl.DataFrame, allocated: pl.DataFrame,
                named: pl.DataFrame) -> pl.DataFrame:
    """One row per county of ``counties`` (``county_fips``, ``county_name``, ``weather_zone``): its signals, the
    approved stock allocated to it (``allocated``: ``large_load_geo.county_pressure`` with ``a2e_stock_mw``; 0
    without a signal, as X11 fills) and, where the deck names it (matched by name), the deck's MW. A named county
    that matches no row raises."""
    unmatched = sorted(set(named["county_name"].to_list()) - set(counties["county_name"].to_list()))
    if unmatched:
        raise ValueError(f"named counties not in the county list: {unmatched}")
    out = (
        counties.select("county_fips", "county_name", "weather_zone")
        .join(signals.select("county_fips", "n_signals", *SIGNAL_COLUMNS), on="county_fips", how="left")
        .join(allocated.select("county_fips", pl.col("a2e_stock_mw_alloc").alias("allocated_a2e_mw")),
              on="county_fips", how="left")
        .join(named.with_columns(pl.lit(True).alias("named_by_ercot")), on="county_name", how="left")
        .with_columns(
            pl.col("allocated_a2e_mw").fill_null(0.0),
            pl.col("named_by_ercot").fill_null(False),
            *[pl.col(c).fill_null(0).cast(pl.Int64) for c in ("n_signals", *SIGNAL_COLUMNS)],
            pl.lit(False).alias("verified"),
        )
    )
    return out.select(COUNTY_COLUMNS).sort("county_fips")


# --- shared inputs (one read per run) ----------------------------------------------------------------------------


@dataclass(frozen=True)
class ZoneInputs:
    excess_mw: dict[str, float]  # zone -> mean coincident excess over EXCESS_YEARS, MW
    excess_share: dict[str, float]
    ercot_excess: dict[int, float]  # year -> ERCOT coincident excess, MW (EXCESS_YEARS)
    ratios: dict[int, dict[str, float]]  # year -> zone and ERCOT -> average day's min/max ratio
    load_through: date
    split: Mapping[str, Any]  # forecast.zone_split
    stock: dict[str, Any]  # approved_stock
    bz_base: dict[str, float]  # Batch Zero base load in 2032 by zone, MW
    bz_studied: dict[str, float]
    bz_report_date: date | None


def zone_inputs(ctx: MartContext) -> ZoneInputs:
    return ctx.cached("zones.inputs", lambda: _zone_inputs(ctx))


def _zone_inputs(ctx: MartContext) -> ZoneInputs:
    from basecast_pipelines.models import large_load_geo as geo
    from basecast_pipelines.models import peak_excess as px
    from basecast_pipelines.models import weather_load as wl

    decks, org = F.deck_inputs(ctx), F.organic_inputs(ctx)

    # X1 §1b: coincident excess over the pre-break fit (zones add up to ERCOT)
    excess = px.excess_vs_prebreak(org.cpanel, "coincident_mw", F.FEATURE)
    excess_mw, share = excess_shares(excess, EXCESS_YEARS)
    ercot = excess.filter((pl.col("weather_zone") == wl.TOTAL) & pl.col("year").is_in(list(EXCESS_YEARS)))
    ercot_excess = {int(y): float(v) for y, v in ercot.select("year", "excess_mw").iter_rows()}

    # X1 §2a: the average summer day's min/max ratio
    load = ctx.cached(LOAD_CACHE, wl.load_hourly_load)
    shape = px.summer_shape(px.daily_load_stats(load))
    ratios = {y: min_max_ratios(shape, y, (*ZONES, wl.TOTAL)) for y in RATIO_YEARS}

    # X11 §2–§3: forecast.zone_split, with the U split of X11 (factor and A2E points do not depend on the deck)
    split = F.zone_split(ctx, decks, org, F.variant_inputs(decks, org, ctx.as_of))
    stock = approved_stock(F.lz_split(decks))
    bz_base = geo.chart_zone_mw(decks.cv, document=F.BZ_DOC, page=7, label="^base load$")
    bz_studied = geo.chart_zone_mw(decks.cv, document=F.BZ_DOC, page=7, label="^studied load$")
    bz_dates = decks.cv.filter(pl.col("document") == F.BZ_DOC)["report_date"]
    return ZoneInputs(excess_mw, share, ercot_excess, ratios, load["operating_date"].max(), split, stock, bz_base,
                      bz_studied, bz_dates.max() if bz_dates.len() else None)


# --- mart_zone_layers --------------------------------------------------------------------------------------------


def build_zone_layers(ctx: MartContext) -> pl.DataFrame:
    from basecast_pipelines.models import large_load_geo as geo

    x = zone_inputs(ctx)
    spread = {r["weather_zone"]: r for r in geo.share_spread(x.split["stock_candidates"], STOCK_CENTRAL)
              .iter_rows(named=True)}
    a2e = x.stock["mw"]
    membership = geo.lz_west_membership(x.split["f_north"])
    rows = [
        *measure_rows("excess_share", x.excess_share, verified=True),
        *[r for y in RATIO_YEARS for r in measure_rows(f"min_max_ratio_{y}", x.ratios[y], verified=True)],
        *measure_rows("a2e_stock", {z: spread[z]["central"] * a2e for z in ZONES},
                      low={z: spread[z]["low"] * a2e for z in ZONES},
                      high={z: spread[z]["high"] * a2e for z in ZONES}, verified=False,
                      extra={"lz_west_fraction": membership}),
        *measure_rows("pipeline_2032", {z: x.bz_base.get(z, 0.0) + x.bz_studied.get(z, 0.0) for z in ZONES},
                      verified=False),
        *measure_rows("u_share", x.split["unattributed"], verified=False),
    ]
    return zone_frame(rows)


def _zone_meta(frame: pl.DataFrame, ctx: MartContext) -> dict[str, Any]:
    x = zone_inputs(ctx)
    return {
        "measures": {m: {"label": lab, "unit": unit, "method": method, "source": src}
                     for m, (lab, unit, method, src) in MEASURES.items()},
        "excess_years": list(EXCESS_YEARS),
        "excess_mw_mean": x.excess_mw,
        "ercot_coincident_excess_mw": x.ercot_excess,
        "ercot_min_max_ratio": {y: x.ratios[y]["ERCOT"] for y in RATIO_YEARS},
        "shape_window": "Jun 1 - Sep 20",
        "load_through": x.load_through,
        "approved_stock": x.stock,
        "f_north": x.split["f_north"],
        "stock_candidates": list(x.split["stock_candidates"]),
        "stock_central": STOCK_CENTRAL,
        "pipeline": {"document": F.BZ_DOC, "page": 7, "report_date": x.bz_report_date,
                     "base_mw": sum(x.bz_base.values()), "studied_mw": sum(x.bz_studied.values())},
        "u_years": [F.EXCESS_FROM, F.last_summer(ctx.as_of)],
    }


# --- mart_county_large_load --------------------------------------------------------------------------------------


def build_county_large_load(ctx: MartContext) -> pl.DataFrame:
    from basecast_pipelines.models import data_centers as dc
    from basecast_pipelines.models import large_load_geo as geo

    x = zone_inputs(ctx)
    counties = geo.load_county_weather_zone()
    signals = county_signals(dc.load_sites(), geo.load_dc_agreements(), geo.load_cpa_data_centers())
    zone_mw = {z: x.split["stock"][z] * x.stock["mw"] for z in ZONES}
    allocated = geo.county_pressure({"a2e_stock_mw": zone_mw}, signals, geo.county_zone_long(counties))
    return county_rows(counties, signals, allocated, named_counties(F.deck_inputs(ctx).cv))


def _county_meta(frame: pl.DataFrame, ctx: MartContext) -> dict[str, Any]:
    x = zone_inputs(ctx)
    return {
        "approved_stock": x.stock,
        "signals": "TCEQ data-center sites + Comptroller agreements naming a data center + Comptroller data-center "
        "registrations with a county, each weighing 1",
        "named_source": {"document": F.BZ_DOC, "page": 7, "report_date": x.bz_report_date},
        "pipeline_spread": False,
    }


# --- checks ------------------------------------------------------------------------------------------------------


def _z(measure: str, zone: str, col: str = "central"):
    return lambda f: f.filter((pl.col("measure") == measure) & (pl.col("weather_zone") == zone))[col].item()


def _golden(name, get, expected, **kw) -> Check:
    return value_check(name, get, expected, as_of=GOLDEN_AS_OF, **kw)


def _measure_sum(measure: str, col: str = "central"):
    return lambda f: f.filter(pl.col("measure") == measure)[col].sum()


def _band_holds_central(frame: pl.DataFrame) -> CheckResult:
    banded = frame.filter(pl.col("low").is_not_null())
    bad = banded.filter((pl.col("low") > pl.col("central") + 1e-9) | (pl.col("central") > pl.col("high") + 1e-9))
    return CheckResult(bad.is_empty(), "low <= central <= high", bad.select("measure", "weather_zone").rows())


def _pipeline_share(zone: str):
    return lambda f: _z("pipeline_2032", zone)(f) / _measure_sum("pipeline_2032")(f)


X1_EXCESS_SHARE = {"COAST": 0.172, "EAST": 0.061, "FWEST": 0.307, "NCENT": 0.086, "NORTH": 0.131, "SCENT": 0.255,
                   "SOUTH": -0.007, "WEST": -0.004}  # X1 §1, "Share, mean 2023–26"
# X1 "Useful for the core features", Explorer map: (zone, year) -> (value, tol). SOUTH 2026 is 0.6947 (X1's script prints
# 0.695; the doc said 0.70 by rounding twice and now says 0.69, Pablo's call on 2026-09-26).
X1_RATIOS = {("WEST", 2019): (0.63, 0.005), ("WEST", 2026): (0.72, 0.005), ("FWEST", 2019): (0.84, 0.005),
             ("FWEST", 2026): (0.90, 0.005), ("SOUTH", 2019): (0.61, 0.005), ("SOUTH", 2026): (0.6947, 0.0005)}
X11_STOCK = {  # X11 §2a, MW (Jun 2026, 8,926 MW): central [low–high]
    "NORTH": (2_463, 1_063, 2_502), "FWEST": (1_734, 1_142, 2_962), "NCENT": (1_356, 1_088, 1_958),
    "WEST": (1_222, 805, 2_088), "COAST": (1_022, 820, 1_476), "SCENT": (628, 504, 908), "SOUTH": (397, 319, 574),
    "EAST": (103, 83, 149),
}
X11_U_SHARE = {"FWEST": 0.336, "SCENT": 0.330, "COAST": 0.183, "EAST": 0.082, "NCENT": 0.036, "NORTH": 0.033,
               "SOUTH": 0.0, "WEST": 0.0}  # X11 §3
X11_PIPELINE_SHARE = {"NCENT": 0.211, "NORTH": 0.201, "SOUTH": 0.129, "WEST": 0.117, "SCENT": 0.112, "FWEST": 0.099,
                      "COAST": 0.091, "EAST": 0.040}  # X11 §2b, central column

ZONE_CHECKS = (
    value_check("one row per zone and measure", lambda f: f.select("weather_zone", "measure").is_duplicated().any(),
                False),
    value_check("eight zones × six measures", lambda f: f.height, len(ZONES) * len(MEASURES)),
    value_check("excess shares add up to 1", _measure_sum("excess_share"), 1.0, tol=1e-9),
    value_check("unattributed shares add up to 1", _measure_sum("u_share"), 1.0, tol=1e-9),
    Check("allocation band holds the central value", _band_holds_central),
    value_check("X11 measures are not verified", lambda f: f.filter(pl.col("measure").is_in(
        ["a2e_stock", "pipeline_2032", "u_share"]))["verified"].any(), False),
    *[_golden(f"{z} excess share {100 * v:.1f}% (X1)", _z("excess_share", z), v, tol=0.0005)
      for z, v in X1_EXCESS_SHARE.items()],
    *[_golden(f"{z} min/max ratio {y} {v:g} (X1)", _z(f"min_max_ratio_{y}", z), v, tol=tol)
      for (z, y), (v, tol) in X1_RATIOS.items()],
    _golden("approved stock 8,926 MW (X11)", _measure_sum("a2e_stock"), 8_926, tol=1),
    _golden("f = 0.72 for NORTH in LZ_WEST (X11)", _z("a2e_stock", "NORTH", "lz_west_fraction"), 0.72, tol=0.005),
    *[_golden(f"{z} approved stock {col} {v:,} MW (X11)", _z("a2e_stock", z, col), v, tol=1)
      for z, (c, lo, hi) in X11_STOCK.items() for col, v in (("central", c), ("low", lo), ("high", hi))],
    _golden("Batch Zero base + studied 191.8 GW (X11)", _measure_sum("pipeline_2032"), 191_800, tol=50),
    *[_golden(f"{z} pipeline share {100 * v:.1f}% (X11)", _pipeline_share(z), v, tol=0.0005)
      for z, v in X11_PIPELINE_SHARE.items()],
    *[_golden(f"{z} unattributed share {100 * v:.1f}% (X11)", _z("u_share", z), v, tol=0.0005)
      for z, v in X11_U_SHARE.items()],
)


def _county(name: str, col: str):
    return lambda f: f.filter(pl.col("county_name") == name)[col].item()


X11_TOP_COUNTIES = {"Wilbarger": 1_112, "Harris": 825, "Pecos": 601, "Dickens": 556, "Haskell": 516, "Ward": 451,
                    "Bexar": 366, "Taylor": 364, "Medina": 364, "Dallas": 360}  # X11 §5, allocated approved stock
X11_NAMED = {  # X11 §5 validation table: (signals, observed base, observed base + studied), MW
    "Harris": (4, 5_672, 6_899), "Dallas": (10, 5_713, 7_637), "Haskell": (2, 2_689, None), "Ellis": (5, 2_330, None),
    "Brazoria": (0, 2_357, None), "Cameron": (0, None, 12_235), "Jones": (0, None, 7_000),
    "Childress": (0, None, 6_750),
}
NO_SIGNAL_NAMED = ("Brazoria", "Cameron", "Jones", "Childress")  # X11 §5: named, no permit signal


def _top(n: int):
    return lambda f: f.sort("allocated_a2e_mw", descending=True).head(n)["county_name"].to_list()


COUNTY_CHECKS = (
    value_check("one row per county", lambda f: f["county_fips"].is_duplicated().any(), False),
    value_check("no county is verified", lambda f: f["verified"].any(), False),
    value_check("allocation only where there is a signal",
                lambda f: f.filter((pl.col("n_signals") == 0) & (pl.col("allocated_a2e_mw") > 0)).height, 0),
    value_check("observed MW only on named counties",
                lambda f: f.filter(~pl.col("named_by_ercot") & (pl.col("observed_base_mw").is_not_null()
                                                               | pl.col("observed_base_studied_mw").is_not_null()))
                .height, 0),
    _golden("204 ERCOT counties", lambda f: f.height, 204),
    _golden("the 10 callouts name 8 counties", lambda f: sorted(f.filter(pl.col("named_by_ercot"))["county_name"]),
            sorted(X11_NAMED)),
    _golden("35 counties with a data-center signal (X11)", lambda f: f.filter(pl.col("n_signals") > 0).height, 35),
    _golden("all 8,926 MW of approved stock land in a county (X11)", lambda f: f["allocated_a2e_mw"].sum(), 8_926,
            tol=1),
    _golden("top 10 by allocated stock (X11)", _top(len(X11_TOP_COUNTIES)), list(X11_TOP_COUNTIES)),
    *[_golden(f"{c} allocated {v:,} MW (X11)", _county(c, "allocated_a2e_mw"), v, tol=1)
      for c, v in X11_TOP_COUNTIES.items()],
    *[_golden(f"{c} {col.removeprefix('observed_')} {v:,} (X11)", _county(c, col), v)
      for c, values in X11_NAMED.items()
      for col, v in zip(("n_signals", "observed_base_mw", "observed_base_studied_mw"), values, strict=True)
      if v is not None],
    *[_golden(f"{c} named with no signal: 0 MW allocated (X11)", _county(c, "allocated_a2e_mw"), 0.0)
      for c in NO_SIGNAL_NAMED],
    _golden("Cameron + Jones + Childress 26 GW with no signal (X11)", lambda f: f.filter(
        pl.col("county_name").is_in(["Cameron", "Jones", "Childress"]))["observed_base_studied_mw"].sum(), 26_000,
        tol=500),
)


# --- marts -------------------------------------------------------------------------------------------------------

ZONE_LAYERS = Mart(
    name="mart_zone_layers",
    build=build_zone_layers,
    key=("weather_zone", "measure"),
    inputs=("ercot_load_hourly_wz", "weather_hourly_wz", "large_load_chart_values", "large_load_headlines"),
    description="Grid layers by weather zone: where flat load arrived (X1: share of the coincident summer-peak excess "
    "2023–2026, the average summer day's min/max load ratio in 2019 and 2026) and where the large loads are (X11: "
    "approved stock allocated with its band, Batch Zero pipeline in 2032 as ERCOT prints it by zone, the "
    "unattributed layer's split).",
    caveats=("allocated_statewide", "machine_read_unverified"),
    checks=ZONE_CHECKS,
    meta=_zone_meta,
)

COUNTY_LARGE_LOAD = Mart(
    name="mart_county_large_load",
    build=build_county_large_load,
    key=("county_fips",),
    inputs=("large_load_chart_values", "large_load_headlines", "county_weather_zone", "tceq_data_center_sites",
            "cpa_local_dev_agreements", "cpa_data_centers"),
    description="Large load by ERCOT county: the approved stock allocated by data-center signals (0 without one) and "
    "the counties ERCOT's Batch Zero deck names with its base and base + studied MW (X11 §5); the pipeline is never "
    "spread by permits.",
    caveats=("allocated_statewide", "machine_read_unverified", "by_county_not_point"),
    checks=COUNTY_CHECKS,
    meta=_county_meta,
)

MARTS = (ZONE_LAYERS, COUNTY_LARGE_LOAD)
