"""Exploration X11: where the large loads are. A weather-zone allocation of the approved-to-energize stock and of
the MW promised by in-service year, for the peak forecast (X7) and the Explorer map.

The status decks give large load by geography only in a few charts (read by Gemini, ``verified=false``):

- "Approved to Energize by Load Zone": the approved stock split **LZ_WEST vs. Other** (monthly decks, 2024-2026);
- "Large Load Project Distribution by Load Zone": the queue split LZ_NORTH / LZ_WEST / Other (Oct 2025 - Jun 2026);
- "LLIS Projects by Weather Zone" (May 2026) and "Batch Zero Tracking by Weather Zone" (Jun 2026): the whole queue by
  **weather zone**, with the "not included unless studies are submitted" part split out in May;
- "Base & studied load by region" (Batch Zero update, Sep 2026): the Batch Zero base load (energized, QSA, Permian
  plan, advancing, committed and net-metering loads, MW in 2032) and studied load by **weather zone**;
- "Q4 2026 QSA Incoming Generation & Load Capacity - Weather Zone" (Jun 2026): large loads entering the Quarterly
  Stability Assessment, the step 5-8 months before energization.

Plus the PUCT TSP RFI (project 58777 item 38: MW requested by TSP and year, 2026-2032), which this module maps to
weather zones through each TSP's certificated service area (``county_utility_overlap_puct``) and the county ->
weather zone shares (``county_weather_zone``).

Everything here is an **allocation**, not an observation of where load sits. Pure functions on DataFrames and dicts
(tested in ``tests/models/test_large_load_geo.py``); the ``load_*`` functions are thin read-only queries.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

import polars as pl

from basecast_pipelines.models.db import read_sql

if TYPE_CHECKING:
    import numpy as np

ZONES = ("COAST", "EAST", "FWEST", "NCENT", "NORTH", "SCENT", "SOUTH", "WEST")

# Deck spellings of ERCOT's weather zones. "Not Specified" and totals map to None (dropped from shares).
_DECK_ZONE = {
    "coast": "COAST", "east": "EAST", "far west": "FWEST", "farwest": "FWEST", "fwest": "FWEST",
    "north": "NORTH", "north central": "NCENT", "ncent": "NCENT", "south": "SOUTH", "south central": "SCENT",
    "scent": "SCENT", "west": "WEST",
}

# ERCOT load zone LZ_WEST vs. weather zones. X1 assumed LZ_WEST = FWEST + WEST + NORTH (not verified): the NORTH
# weather zone straddles AEP Texas North (LZ_WEST) and Oncor / co-op areas in LZ_NORTH, so NORTH enters with a
# fraction ``f_north`` that ``lz_west_north_fraction`` estimates from two charts of the same deck.
LZ_WEST_CORE = ("FWEST", "WEST")

# TSP names in the PUCT RFI -> certificated retail service areas in ``county_utility_overlap_puct``. Only wires
# utilities with a CCN area are mapped. Generation-and-transmission co-ops (Brazos, Golden Spread, Rayburn, STEC),
# LCRA TSC and transmission-only TSPs (WETT, Lone Star) have no retail area there, so they stay unmapped.
TSP_UTILITIES: dict[str, tuple[str, ...]] = {
    "Oncor": ("Oncor Electric Delivery Company LLC",),
    "CenterPoint": ("CenterPoint Energy Houston Electric, LLC",),
    "AEP": ("AEP Texas Inc.",),
    "TNMP": ("Texas-New Mexico Power Company",),
    "CPS": ("CPS Energy",),
}


def zone_code(name: str | None) -> str | None:
    """Deck label ("Far West", "North Central", "FWEST"…) -> weather-zone code, or None for anything else."""
    if name is None:
        return None
    key = re.sub(r"\s+", " ", name.strip().lower().replace("_", " "))
    return _DECK_ZONE.get(key)


# --- share vectors -------------------------------------------------------------------------------------------


def shares(values: Mapping[str, float], zones: Sequence[str] = ZONES) -> dict[str, float]:
    """MW by zone -> shares over ``zones`` (missing zones 0, negatives clipped to 0). Empty or all-zero -> all 0."""
    pos = {z: max(float(values.get(z, 0.0) or 0.0), 0.0) for z in zones}
    tot = sum(pos.values())
    return {z: (v / tot if tot > 0 else 0.0) for z, v in pos.items()}


def chart_zone_mw(cv: pl.DataFrame, *, document: str, page: int, label: str | None = None,
                  exclude_label: str | None = None) -> dict[str, float]:
    """One chart's MW by weather zone: rows of ``document`` / ``page`` whose category is a weather zone, summed by
    zone; ``label`` / ``exclude_label`` are case-insensitive regexes on ``status_label``."""
    rows = cv.filter((pl.col("document") == document) & (pl.col("page") == page) & pl.col("value_mw").is_not_null())
    if label is not None:
        rows = rows.filter(pl.col("status_label").str.contains(f"(?i){label}"))
    if exclude_label is not None:
        rows = rows.filter(~pl.col("status_label").str.contains(f"(?i){exclude_label}"))
    out: dict[str, float] = {}
    for cat, mw in zip(rows["category"].to_list(), rows["value_mw"].to_list(), strict=True):
        z = zone_code(cat)
        if z is not None:
            out[z] = out.get(z, 0.0) + mw
    return out


def subtract(a: Mapping[str, float], b: Mapping[str, float]) -> dict[str, float]:
    """``a - b`` zone by zone (e.g. a queue total minus its no-studies part)."""
    return {z: a.get(z, 0.0) - b.get(z, 0.0) for z in set(a) | set(b)}


def lz_west_north_fraction(zone_mw: Mapping[str, float], lz_west_mw: float) -> float | None:
    """Fraction of the NORTH weather zone that sits in load zone LZ_WEST, solved from one population seen both ways:
    ``lz_west_mw = FWEST + WEST + f x NORTH``. Clipped to [0, 1]; None when NORTH is empty."""
    north = zone_mw.get("NORTH", 0.0)
    if north <= 0:
        return None
    core = sum(zone_mw.get(z, 0.0) for z in LZ_WEST_CORE)
    return min(max((lz_west_mw - core) / north, 0.0), 1.0)


def lz_west_membership(f_north: float) -> dict[str, float]:
    """Fraction of each weather zone inside LZ_WEST: FWEST and WEST whole, NORTH ``f_north``, the rest 0."""
    return {z: (1.0 if z in LZ_WEST_CORE else f_north if z == "NORTH" else 0.0) for z in ZONES}


def rake_to_group(share: Mapping[str, float], membership: Mapping[str, float], group_share: float) -> dict[str, float]:
    """Rescale a zone split so the group (zones weighted by ``membership`` in [0, 1]) holds ``group_share`` and the
    rest ``1 - group_share``, keeping the proportions inside each side. A zone split across both sides gets each
    side's factor on its own part."""
    inside = sum(share.get(z, 0.0) * membership.get(z, 0.0) for z in ZONES)
    outside = sum(share.get(z, 0.0) * (1 - membership.get(z, 0.0)) for z in ZONES)
    if inside <= 0 or outside <= 0:
        raise ValueError("both sides of the group need a positive share to rake")
    a, b = group_share / inside, (1 - group_share) / outside
    return {z: share.get(z, 0.0) * (a * membership.get(z, 0.0) + b * (1 - membership.get(z, 0.0))) for z in ZONES}


def share_spread(candidates: Mapping[str, Mapping[str, float]], central: str) -> pl.DataFrame:
    """One row per zone: the ``central`` share, and min / max over every candidate split (the allocation band)."""
    rows = []
    for z in ZONES:
        vals = [c.get(z, 0.0) for c in candidates.values()]
        rows.append({"weather_zone": z, "central": candidates[central].get(z, 0.0), "low": min(vals),
                     "high": max(vals), **{name: c.get(z, 0.0) for name, c in candidates.items()}})
    return pl.DataFrame(rows)


# --- TSP service areas -> weather zones ----------------------------------------------------------------------


def county_zone_long(county_zone: pl.DataFrame) -> pl.DataFrame:
    """``county_weather_zone`` (one ``share_<zone>`` column per zone) -> long ``county_fips, weather_zone, share``,
    ERCOT zones only (``share_outside`` dropped)."""
    cols = {f"share_{z.lower()}": z for z in ZONES}
    present = [c for c in cols if c in county_zone.columns]
    return (
        county_zone.select("county_fips", *present)
        .unpivot(index="county_fips", on=present, variable_name="col", value_name="share")
        .with_columns(pl.col("col").replace_strict(cols).alias("weather_zone"),
                      pl.col("share").cast(pl.Float64).fill_null(0.0))
        .filter(pl.col("share") > 0)
        .select("county_fips", "weather_zone", "share")
    )


def tsp_zone_shares(overlap: pl.DataFrame, czl: pl.DataFrame, tsp_utilities: Mapping[str, Sequence[str]] = TSP_UTILITIES,
                    weight: str = "overlap_km2") -> pl.DataFrame:
    """Each TSP's split across weather zones: its utilities' county overlaps (``weight``, e.g. km² of the service area
    in the county, or a population-weighted version) times the county's zone shares, normalized per TSP.
    Returns ``tsp, weather_zone, share``."""
    rows = []
    for tsp, names in tsp_utilities.items():
        sub = overlap.filter(pl.col("utility_name").is_in(list(names))).select("county_fips", pl.col(weight).alias("w"))
        if sub.is_empty():
            continue
        z = (sub.group_by("county_fips").agg(pl.col("w").sum()).join(czl, on="county_fips", how="inner")
             .group_by("weather_zone").agg((pl.col("w") * pl.col("share")).sum().alias("mw")))
        s = shares(dict(zip(z["weather_zone"].to_list(), z["mw"].to_list(), strict=True)))
        rows += [{"tsp": tsp, "weather_zone": k, "share": v} for k, v in s.items()]
    return pl.DataFrame(rows, schema={"tsp": pl.String, "weather_zone": pl.String, "share": pl.Float64})


def allocate_tsp_requests(requests: pl.DataFrame, tsp_shares: pl.DataFrame,
                          fallback: Mapping[str, float] | None = None) -> pl.DataFrame:
    """TSP RFI rows (``name``, ``year``, ``mw``; totals already dropped) -> MW by year and weather zone. TSPs without a
    service-area split go to ``fallback`` shares (flagged ``mapped=False``) or are dropped when it is None."""
    mapped = set(tsp_shares["tsp"].to_list())
    out = []
    for r in requests.iter_rows(named=True):
        if r["name"] in mapped:
            s = tsp_shares.filter(pl.col("tsp") == r["name"])
            out += [{"year": r["year"], "weather_zone": z, "mw": r["mw"] * v, "mapped": True}
                    for z, v in zip(s["weather_zone"].to_list(), s["share"].to_list(), strict=True)]
        elif fallback is not None:
            out += [{"year": r["year"], "weather_zone": z, "mw": r["mw"] * v, "mapped": False} for z, v in fallback.items()]
    return (pl.DataFrame(out, schema={"year": pl.Int64, "weather_zone": pl.String, "mw": pl.Float64, "mapped": pl.Boolean})
            .group_by("year", "weather_zone", "mapped").agg(pl.col("mw").sum()).sort("year", "weather_zone"))


# --- forecast layers by zone ---------------------------------------------------------------------------------


def split_large_load(ll_draws: "np.ndarray", stock_ll: float, stock_share: Mapping[str, float],
                     increment_share: Mapping[str, float]) -> dict[str, "np.ndarray"]:
    """X7's statewide large-load draws (``n x years``, MW at the peak) -> per zone. The part up to ``stock_ll``
    (factor x the deck's approved stock) follows ``stock_share``; anything above it (new approvals) follows
    ``increment_share``. A draw below the stock is split by ``stock_share`` alone."""
    import numpy as np

    stock = np.minimum(ll_draws, stock_ll)
    inc = np.maximum(ll_draws - stock_ll, 0.0)
    return {z: stock * stock_share.get(z, 0.0) + inc * increment_share.get(z, 0.0) for z in ZONES}


def residual_shares(zone_excess: Mapping[str, float], zone_ll: Mapping[str, float]) -> tuple[dict[str, float], dict[str, float]]:
    """The unattributed layer's split: each zone's coincident excess minus its allocated large load (mean over the
    same summers), clipped at 0 and normalized. Also returns the raw residuals (negative = the allocation puts more
    large load in the zone than its excess shows)."""
    resid = {z: zone_excess.get(z, 0.0) - zone_ll.get(z, 0.0) for z in ZONES}
    return shares(resid), resid


# --- county pressure for the Explorer ------------------------------------------------------------------------


def county_pressure(zone_mw: Mapping[str, Mapping[str, float]], signals: pl.DataFrame, czl: pl.DataFrame) -> pl.DataFrame:
    """Spread zone-level allocated MW to counties by their share of the zone's large-load signals.

    ``zone_mw`` maps a measure name (e.g. ``stock_mw``) to MW by zone; ``signals`` has ``county_fips`` and
    ``n_signals`` (e.g. TCEQ data-center sites + Comptroller data-center agreements). A county in several zones
    carries ``n_signals x share`` into each. Zones with no signal leave their MW unallocated (not in the output).
    Returns one row per county with ``<measure>_alloc`` columns."""
    sig = (signals.filter(pl.col("n_signals") > 0).join(czl, on="county_fips", how="inner")
           .with_columns((pl.col("n_signals") * pl.col("share")).alias("w")))
    zt = sig.group_by("weather_zone").agg(pl.col("w").sum().alias("wz"))
    sig = sig.join(zt, on="weather_zone").with_columns((pl.col("w") / pl.col("wz")).alias("within"))
    exprs = []
    for name, mw in zone_mw.items():
        sig = sig.with_columns((pl.col("within") * pl.col("weather_zone").replace_strict(
            {z: float(mw.get(z, 0.0)) for z in ZONES}, default=0.0, return_dtype=pl.Float64)).alias(f"_{name}"))
        exprs.append(pl.col(f"_{name}").sum().alias(f"{name}_alloc"))
    return (sig.group_by("county_fips").agg(*exprs, pl.col("weather_zone").sort_by("w").last().alias("main_zone"))
            .sort("county_fips"))


def compare_with_excess(alloc: Mapping[str, float], excess: Mapping[str, float]) -> pl.DataFrame:
    """Zone table: allocated large load vs. an excess measure (MW), their difference and ranks, for the X1 check."""
    rows = [{"weather_zone": z, "allocated_mw": alloc.get(z, 0.0), "excess_mw": excess.get(z, 0.0),
             "excess_minus_allocated_mw": excess.get(z, 0.0) - alloc.get(z, 0.0)} for z in ZONES]
    return pl.DataFrame(rows).with_columns(
        pl.col("allocated_mw").rank(descending=True).alias("rank_allocated"),
        pl.col("excess_mw").rank(descending=True).alias("rank_excess"),
    )


def spearman(a: Mapping[str, float], b: Mapping[str, float], zones: Sequence[str] = ZONES) -> float:
    """Spearman rank correlation of two zone vectors (average ranks for ties)."""
    x = pl.Series([a.get(z, 0.0) for z in zones]).rank()
    y = pl.Series([b.get(z, 0.0) for z in zones]).rank()
    return float(pl.DataFrame({"x": x, "y": y}).select(pl.corr("x", "y")).item())


# --- thin loaders (read-only) -------------------------------------------------------------------------------


def load_tsp_requests() -> pl.DataFrame:
    """PUCT 58777 item 38, by TSP, from the PPTX (the PDF carries the same numbers)."""
    return read_sql(
        """
        SELECT name, is_total, year, mw, filed_date, member, page
        FROM puct_tsp_large_load_requests
        WHERE breakdown = 'tsp' AND member ILIKE %(m)s
        """,
        {"m": "%.pptx"},
    )


def load_county_weather_zone() -> pl.DataFrame:
    return read_sql("SELECT * FROM county_weather_zone WHERE in_ercot")


def load_overlap_puct() -> pl.DataFrame:
    return read_sql(
        """
        SELECT county_fips, utility_name, utility_type, overlap_km2, county_share, territory_share
        FROM county_utility_overlap_puct WHERE in_ercot
        """
    )


def load_dc_agreements() -> pl.DataFrame:
    """Comptroller local development agreements (ch312 abatements, ch380) that name a data center (NAICS 5182 or the
    words in the activity or summary), with a county."""
    return read_sql(
        """
        SELECT program, agreement_id, county_fips, local_government_name, executed_date, effective_date
        FROM cpa_local_dev_agreements
        WHERE county_fips IS NOT NULL
          AND (naics_code LIKE '5182%%' OR business_activity ILIKE '%%data cent%%' OR summary ILIKE '%%data cent%%')
        """
    )


def load_cpa_data_centers() -> pl.DataFrame:
    """Comptroller qualifying data centers (sales-tax exemption); most rows have no county."""
    return read_sql("SELECT program, data_center_name, effective_date, county_fips FROM cpa_data_centers")
