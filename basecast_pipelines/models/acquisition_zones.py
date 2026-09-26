"""Priority acquisition zones by county for the Explorer map (exploration X14).

The kickoff lists "priority acquisition zones" in the Explorer with no definition. Here a zone is a **county**
(the map's grain), and its priority is a 0-1 score built from **territory signals only**: nothing here reads
an account score, an account rank or where Base's partners are, so the county map cannot leak the partner
validation (``tests/test_partner_lock.py``).

Base acquires homes through two channels, and the county's PUCT territory mix says which one applies:

- **retail-direct**: the county's land in an investor-owned wires utility (TDSP) inside ERCOT. That is the
  competitive retail area, where a homeowner can pick Base as retailer.
- **partnership**: the county's land in an ERCOT co-op or muni, which has no retail choice. Base needs the
  utility, and those utilities are the /accounts list.
- **outside**: land in a territory that is not in ERCOT (SPP, MISO, WECC); neither channel applies.

The shares come from ``county_utility_overlap_puct`` (``county_share`` summed per type and normalized by the
county's own sum, because dual-certified territories overlap), the same rule as
``data_centers.county_territory_types``. Area shares under-read dense city territories (a muni covers a small
part of its county's land but many of its homes): see ``docs/analysis/x14_acquisition_zones.md``.

**Score.** Each signal becomes its percentile rank among the ERCOT counties (``triggers.percentile_ranks``,
ties averaged). The market block (homes, growth, permits, owner share) and the grid block (zone peak growth,
flat large-load pressure, load-zone price spread, new data-center sites) are weighted means of their ranks
(a county missing a signal has its weights renormalized). The priority is the market score, tilted by the grid
score (``GRID_TILT``); the channel enters through the size signal (homes × addressable share) and decides
which channel lists a county joins; see :func:`score_counties` and :func:`channel_priority`.
The weights are a **proposal**, not a fit: there is no outcome to fit them to.

The logic is pure functions on DataFrames (tested in ``tests/models/test_acquisition_zones.py``); the
``load_*`` functions are thin read-only queries through ``models.db``. The county signals reuse the account
builders of ``accounts.py`` with one "account" per county (``county_links``).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import polars as pl

from basecast_pipelines.models import triggers as T
from basecast_pipelines.models.db import read_sql

ZONES = ("COAST", "EAST", "FWEST", "NCENT", "NORTH", "SCENT", "SOUTH", "WEST")

# Proposed weights (each block sums to 1). Market: the homes Base can sell a battery to (``addr_sf_homes`` =
# owner-occupied single-family homes × the county's addressable share, so the channel enters through the size
# signal). Grid: where a battery's energy and capacity are worth most. priority = market_score ×
# (1 − GRID_TILT + GRID_TILT × grid_score): grid value tilts the market score (it can cut it by up to
# GRID_TILT) but never lifts a county with no homes. X14 §3 shows why not an additive mean (zone-level grid
# signals then lift near-empty Permian counties into the retail list) and why the addressable share is not a
# multiplier on the whole priority (area shares drop Montgomery County to #125).
MARKET = {"addr_sf_homes": 0.35, "pop_growth": 0.30, "permits_per_1k": 0.15, "owner_sf_share": 0.20}
GRID = {"zone_peak_cagr": 0.30, "ll_pressure": 0.20, "lz_spread": 0.25, "dc_sites": 0.25}
GRID_TILT = 0.4
WEIGHTS = {**MARKET, **GRID}

CHANNELS = ("retail", "coop", "muni", "outside")
DOMINANT_SHARE = 0.5  # a county's channel label needs at least this normalized share
PARTNER_MIN_SHARE = 0.01  # a co-op/muni reaches a county's account list above 1% of the county's land
CHANNEL_MIN_SHARE = 0.25  # a county enters a channel's top list when the channel covers ≥ 25% of its land

# Weather zone -> ERCOT load zone, a proxy: no table maps counties to load zones (X11 §1). NORTH splits by
# X11's f = 0.72 (LZ_WEST = FWEST + WEST + f x NORTH). The NOIE load zones (LZ_AEN, LZ_CPS, LZ_LCRA,
# LZ_RAYBN) are left out: SCENT/EAST co-op and muni load is priced at its own zone. Not verified.
WZ_TO_LZ: dict[str, dict[str, float]] = {
    "COAST": {"LZ_HOUSTON": 1.0},
    "EAST": {"LZ_NORTH": 1.0},
    "NCENT": {"LZ_NORTH": 1.0},
    "NORTH": {"LZ_WEST": 0.72, "LZ_NORTH": 0.28},
    "FWEST": {"LZ_WEST": 1.0},
    "WEST": {"LZ_WEST": 1.0},
    "SCENT": {"LZ_SOUTH": 1.0},
    "SOUTH": {"LZ_SOUTH": 1.0},
}
LOAD_ZONES = ("LZ_HOUSTON", "LZ_NORTH", "LZ_SOUTH", "LZ_WEST")


# --- channel ---------------------------------------------------------------------------------------------


def channel_of(utility_type: pl.Expr, in_ercot: pl.Expr) -> pl.Expr:
    """``retail`` (IOU in ERCOT), ``coop``/``muni`` (in ERCOT) or ``outside`` (any territory not in ERCOT)."""
    return (
        pl.when(~in_ercot.fill_null(False))
        .then(pl.lit("outside"))
        .when(utility_type == "iou")
        .then(pl.lit("retail"))
        .when(utility_type.is_in(["coop", "muni"]))
        .then(utility_type)
        .otherwise(pl.lit(None, pl.Utf8))
    )


def channel_split(overlap: pl.DataFrame, *, dominant: float = DOMINANT_SHARE) -> pl.DataFrame:
    """One row per county: the normalized share of each channel, the addressable share and the label.

    ``overlap``: ``county_fips``, ``ccn_no``, ``utility_type`` (iou | coop | muni), ``in_ercot``,
    ``county_share``. ``<channel>_share`` = the channel's summed ``county_share`` ÷ the county's sum (so they
    add to 1); ``<channel>_raw`` keeps the raw sum (clipped to 1 it bounds the land the channel covers).
    ``partner_share`` = coop + muni; ``addressable_share`` = 1 − outside. ``channel`` is ``retail_direct``
    or ``partnership`` when that side holds ≥ ``dominant``, else ``mixed``; ``partner_type`` is the larger of
    coop and muni. ``n_partners`` counts the distinct ERCOT co-op/muni CCNs above ``PARTNER_MIN_SHARE`` and
    ``top_partner_share`` is the largest one's share of the partnership land (1 = one deal covers it).
    """
    typed = overlap.with_columns(channel_of(pl.col("utility_type"), pl.col("in_ercot")).alias("_ch")).filter(
        pl.col("_ch").is_not_null()
    )
    wide = typed.group_by("county_fips").agg(
        *[pl.col("county_share").filter(pl.col("_ch") == c).sum().alias(f"{c}_raw") for c in CHANNELS],
        pl.col("county_share").sum().alias("share_sum"),
    )
    partners = (
        typed.filter(pl.col("_ch").is_in(["coop", "muni"]) & (pl.col("county_share") >= PARTNER_MIN_SHARE))
        .group_by("county_fips", "ccn_no")
        .agg(pl.col("county_share").sum())
        .group_by("county_fips")
        .agg(
            pl.col("ccn_no").n_unique().alias("n_partners"),
            (pl.col("county_share").max() / pl.col("county_share").sum()).alias("top_partner_share"),
        )
    )
    norm = [
        pl.when(pl.col("share_sum") > 0).then(pl.col(f"{c}_raw") / pl.col("share_sum")).alias(f"{c}_share")
        for c in CHANNELS
    ]
    out = wide.with_columns(norm).with_columns(
        (pl.col("coop_share") + pl.col("muni_share")).alias("partner_share"),
        (1 - pl.col("outside_share")).alias("addressable_share"),
    )
    label = (
        pl.when(pl.col("retail_share") >= dominant)
        .then(pl.lit("retail_direct"))
        .when(pl.col("partner_share") >= dominant)
        .then(pl.lit("partnership"))
        .otherwise(pl.lit("mixed"))
    )
    out = out.with_columns(
        label.alias("channel"),
        pl.when(pl.col("partner_share") > 0)
        .then(pl.when(pl.col("coop_share") >= pl.col("muni_share")).then(pl.lit("coop")).otherwise(pl.lit("muni")))
        .alias("partner_type"),
    )
    out = out.join(partners, on="county_fips", how="left").with_columns(pl.col("n_partners").fill_null(0))
    return out.select(
        "county_fips",
        *[f"{c}_share" for c in CHANNELS],
        "partner_share",
        "addressable_share",
        "channel",
        "partner_type",
        "n_partners",
        "top_partner_share",
        *[f"{c}_raw" for c in CHANNELS],
        "share_sum",
    ).sort("county_fips")


def county_accounts(overlap: pl.DataFrame, *, min_share: float = PARTNER_MIN_SHARE) -> pl.DataFrame:
    """The reverse of X9's ``account_counties``: for each county, the ERCOT co-ops and munis whose territory
    covers ≥ ``min_share`` of its land, largest first. This is the click-through from a partnership county to
    /accounts; it lists every utility, with no partner flag."""
    return (
        overlap.filter(
            pl.col("utility_type").is_in(["coop", "muni"]) & pl.col("in_ercot").fill_null(False)
            & (pl.col("county_share") >= min_share)
        )
        .group_by("county_fips", "ccn_no", "utility_type")
        .agg(pl.col("county_share").sum())
        .sort(["county_fips", "county_share", "ccn_no"], descending=[False, True, False])
        .rename({"ccn_no": "account_id"})
    )


# --- county signals ----------------------------------------------------------------------------------------


def county_links(counties: pl.DataFrame) -> pl.DataFrame:
    """One "account" per county, so the ``accounts.py`` builders run at county grain unchanged.

    ``counties``: ``county_fips``, ``land_km2``. Returns ``account_id`` (= county_fips), ``county_fips``,
    ``county_share`` = 1 and ``overlap_km2`` = the county's land.
    """
    return counties.select(
        pl.col("county_fips").alias("account_id"),
        "county_fips",
        pl.lit(1.0).alias("county_share"),
        pl.col("land_km2").cast(pl.Float64).alias("overlap_km2"),
    )


def zone_to_county(zone_values: Mapping[str, float], czl: pl.DataFrame, name: str) -> pl.DataFrame:
    """Share-weighted mean of a weather-zone value over each county's zone mix.

    ``czl``: long ``county_fips``, ``weather_zone``, ``share`` (``large_load_geo.county_zone_long``). Zones
    with no value are left out and the remaining shares renormalized; a county with none gets no row.
    """
    vals = pl.DataFrame(
        {"weather_zone": list(zone_values), "_v": [float(v) for v in zone_values.values()]},
        schema={"weather_zone": pl.Utf8, "_v": pl.Float64},
    )
    return (
        czl.join(vals, on="weather_zone", how="inner")
        .group_by("county_fips")
        .agg(((pl.col("_v") * pl.col("share")).sum() / pl.col("share").sum()).alias(name))
        .sort("county_fips")
    )


def weather_zone_values(lz_values: Mapping[str, float], mapping: Mapping[str, Mapping[str, float]] = WZ_TO_LZ) -> dict[str, float]:
    """Load-zone values carried to weather zones through ``mapping`` (weights renormalized over the load
    zones that have a value)."""
    out = {}
    for wz, parts in mapping.items():
        have = {lz: w for lz, w in parts.items() if lz in lz_values}
        if have:
            out[wz] = sum(lz_values[lz] * w for lz, w in have.items()) / sum(have.values())
    return out


def cagr(start: float, end: float, years: int) -> float | None:
    return (end / start) ** (1 / years) - 1 if start and start > 0 and end and end > 0 and years > 0 else None


def daily_spread(prices: pl.DataFrame, *, hours: float = 2.0, interval_minutes: int = 15) -> pl.DataFrame:
    """Per settlement point and local day: mean of the day's top ``hours`` of intervals minus the mean of its
    bottom ``hours`` ($/MWh), a 2-hour battery's round-trip spread before losses.

    ``prices``: ``settlement_point``, ``day`` (local operating date), ``price_usd_mwh``. Days with fewer
    intervals than twice the window are dropped.
    """
    k = int(round(hours * 60 / interval_minutes))
    return (
        prices.group_by("settlement_point", "day")
        .agg(
            pl.col("price_usd_mwh").sort(descending=True).head(k).mean().alias("_top"),
            pl.col("price_usd_mwh").sort().head(k).mean().alias("_bottom"),
            pl.len().alias("_n"),
        )
        .filter(pl.col("_n") >= 2 * k)
        .select("settlement_point", "day", (pl.col("_top") - pl.col("_bottom")).alias("spread_usd_mwh"))
        .sort("settlement_point", "day")
    )


# --- score -------------------------------------------------------------------------------------------------


def _wmean(weights: Mapping[str, float]) -> pl.Expr:
    """Weighted mean of ``pct_<signal>``, renormalized over the signals a county has (null if none)."""
    num = sum(pl.col(f"pct_{c}").fill_null(0) * w for c, w in weights.items())
    den = sum(pl.col(f"pct_{c}").is_not_null().cast(pl.Float64) * w for c, w in weights.items())
    return pl.when(den > 0).then(num / den)


def score_counties(
    signals: pl.DataFrame,
    market: Mapping[str, float] = MARKET,
    grid: Mapping[str, float] = GRID,
    *,
    tilt: float = GRID_TILT,
    gate: str | None = None,
    combine: str = "tilt",
) -> pl.DataFrame:
    """Percentile ranks, block scores and the county priority.

    ``signals``: ``county_fips``, every signal of ``market`` and ``grid`` (higher = better) and the ``gate``
    column. Adds ``pct_<signal>`` (``triggers.percentile_ranks`` over the rows given), ``market_score`` and
    ``grid_score`` (each block's weighted mean, renormalized per county), ``grid_factor`` and ``priority``:

    - ``combine="tilt"`` (the proposal): ``grid_factor = 1 − tilt + tilt × grid_score`` and
      ``priority = market_score × grid_factor × gate``. Grid value can cut a county's market score by up to
      ``tilt``, never lift a county with no homes.
    - ``combine="additive"``: ``priority = ((1 − tilt) × market_score + tilt × grid_score) × gate``, the
      Q3-style weighted mean, kept for the comparison.

    A block with no weights counts as neutral (score 0.5). The gate is clipped to [0, 1]; ``gate=None`` is 1.
    ``rank``: 1 = best, ties by county_fips.
    """
    names = [*market, *grid]
    frame = signals.rename({"county_fips": "account_id"})
    pct = T.percentile_ranks(frame, {c: "higher" for c in names}).rename({"account_id": "county_fips"})
    out = signals.join(pct, on="county_fips", how="left")
    m = _wmean(market).fill_null(0.5) if market else pl.lit(0.5)
    g = _wmean(grid).fill_null(0.5) if grid else pl.lit(0.5)
    out = out.with_columns(m.alias("market_score"), g.alias("grid_score"))
    gate_x = pl.col(gate).fill_null(0.0).clip(0.0, 1.0) if gate else pl.lit(1.0)
    if combine == "tilt":
        out = out.with_columns((1 - tilt + tilt * pl.col("grid_score")).alias("grid_factor")).with_columns(
            (pl.col("market_score") * pl.col("grid_factor") * gate_x).alias("priority")
        )
    elif combine == "additive":
        out = out.with_columns(pl.lit(None, pl.Float64).alias("grid_factor")).with_columns(
            (((1 - tilt) * pl.col("market_score") + tilt * pl.col("grid_score")) * gate_x).alias("priority")
        )
    else:
        raise ValueError(f"combine must be 'tilt' or 'additive', not {combine!r}")
    out = out.sort(["priority", "county_fips"], descending=[True, False], nulls_last=True)
    return out.with_columns(pl.int_range(1, out.height + 1).alias("rank"))


def effective_weights(market: Mapping[str, float] = MARKET, grid: Mapping[str, float] = GRID,
                      tilt: float = GRID_TILT) -> dict[str, float]:
    """How much one percentile point of each signal moves the priority at the middle of the range: market
    weights as they are, grid weights × ``tilt`` × 0.5 / 0.8 (the market score and grid factor at their
    medians ~0.5 and ~0.8). Used only to order the ``drivers``."""
    return {**dict(market), **{c: w * tilt * 0.5 / 0.8 for c, w in grid.items()}}


def drivers(scored: pl.DataFrame, weights: Mapping[str, float] | None = None, *, k: int = 2) -> pl.DataFrame:
    """The ``k`` signals that lift each county most: largest ``weight × (pct − 0.5)`` above zero, as
    ``drivers`` (comma-separated), and the ``k`` most negative as ``drags``. Default weights:
    :func:`effective_weights`."""
    weights = dict(weights or effective_weights())
    long = scored.select("county_fips", *[f"pct_{c}" for c in weights]).unpivot(
        index="county_fips", variable_name="signal", value_name="pct"
    ).with_columns(pl.col("signal").str.strip_prefix("pct_"))
    long = long.with_columns(
        (pl.col("signal").replace_strict(weights, return_dtype=pl.Float64) * (pl.col("pct") - 0.5)).alias("lift")
    ).drop_nulls("lift")
    up = (
        long.filter(pl.col("lift") > 0)
        .sort(["county_fips", "lift"], descending=[False, True])
        .group_by("county_fips", maintain_order=True)
        .agg(pl.col("signal").head(k).str.join(", ").alias("drivers"))
    )
    down = (
        long.filter(pl.col("lift") < 0)
        .sort(["county_fips", "lift"])
        .group_by("county_fips", maintain_order=True)
        .agg(pl.col("signal").head(k).str.join(", ").alias("drags"))
    )
    return scored.select("county_fips").join(up, on="county_fips", how="left").join(down, on="county_fips", how="left")


def channel_priority(scored: pl.DataFrame, *, min_share: float = CHANNEL_MIN_SHARE, mode: str = "eligible") -> pl.DataFrame:
    """Each channel's list: ``retail_priority`` / ``partner_priority`` and ``retail_rank`` / ``partner_rank``
    (1 = best, null when the county is not in the list).

    - ``mode="eligible"`` (the proposal): a county is in a channel's list when the channel covers at least
      ``min_share`` of its land, ranked by its priority. Area shares under-read the channel that serves the
      dense part of a county (the IOU in a DFW suburb), so the share only decides membership.
    - ``mode="share"``: every county, ranked by priority × the channel's share.
    """
    if mode == "eligible":
        exprs = [
            pl.when(pl.col(f"{ch}_share").fill_null(0) >= min_share).then(pl.col("priority")).alias(f"{ch}_priority")
            for ch in ("retail", "partner")
        ]
    elif mode == "share":
        exprs = [(pl.col("priority") * pl.col(f"{ch}_share").fill_null(0)).alias(f"{ch}_priority") for ch in ("retail", "partner")]
    else:
        raise ValueError(f"mode must be 'eligible' or 'share', not {mode!r}")
    out = scored.with_columns(exprs)
    return out.with_columns(
        pl.col(f"{ch}_priority").rank("ordinal", descending=True).cast(pl.Int64).alias(f"{ch}_rank")
        for ch in ("retail", "partner")
    )


def legend_classes(values: pl.Series, n: int = 5) -> tuple[list[float], pl.Series]:
    """Quantile breaks (``n`` classes, equal counts) and each value's class 1..n (n = highest priority)."""
    qs = [float(values.quantile(i / n, "linear")) for i in range(1, n)]
    cls = values.map_elements(lambda v: 1 + sum(v > q for q in qs), return_dtype=pl.Int64)
    return qs, cls


# --- sensitivity -------------------------------------------------------------------------------------------


def ranks(signals: pl.DataFrame, *, mode: str = "eligible", **kw) -> pl.DataFrame:
    """``county_fips``, ``rank``, ``retail_rank``, ``partner_rank`` for one scoring setup (kw → score_counties,
    ``mode`` → channel_priority)."""
    return channel_priority(score_counties(signals, **kw), mode=mode).select(
        "county_fips", "rank", "retail_rank", "partner_rank"
    )


def compare_rankings(base: pl.DataFrame, alt: pl.DataFrame, *, top: int = 20) -> dict[str, float]:
    """Spearman ρ of ``rank`` and top-``top`` overlap overall and per channel between two :func:`ranks`
    frames, plus the largest single move of ``rank``."""
    j = base.join(alt, on="county_fips", suffix="_alt")
    out: dict[str, float] = {"spearman": j.select(pl.corr("rank", "rank_alt", method="spearman")).item()}
    for col, label in (("rank", "all"), ("retail_rank", "retail"), ("partner_rank", "partner")):
        top0 = set(j.filter(pl.col(col) <= top)["county_fips"])
        top1 = set(j.filter(pl.col(f"{col}_alt") <= top)["county_fips"])
        out[f"{label}_top{top}_kept"] = len(top0 & top1)
    out["max_move"] = int(j.select((pl.col("rank") - pl.col("rank_alt")).abs().max()).item())
    return out


def weight_variants(market: Mapping[str, float] = MARKET, grid: Mapping[str, float] = GRID, tilt: float = GRID_TILT,
                    *, delta: float = 0.05, tilt_delta: float = 0.10) -> list[tuple[str, dict]]:
    """Scoring setups for the sensitivity table: ``triggers.perturbations`` inside each block (± ``delta`` on
    one weight, renormalized, then leave-one-signal-out), the tilt ± ``tilt_delta``, the tilt at 0 (market
    only) and at 1 (grid fully multiplicative)."""
    out: list[tuple[str, dict]] = []
    for block, weights in (("market", market), ("grid", grid)):
        for name, w in T.perturbations(dict(weights), delta=delta):
            kw = {"market": w, "grid": dict(grid), "tilt": tilt} if block == "market" else {
                "market": dict(market), "grid": w, "tilt": tilt}
            out.append((f"{block}: {name}", kw))
    for t in (tilt - tilt_delta, tilt + tilt_delta, 0.0, 1.0):
        out.append((f"tilt {t:.2f}", {"market": dict(market), "grid": dict(grid), "tilt": max(0.0, min(1.0, t))}))
    return out


def rank_stability(signals: pl.DataFrame, *, gate: str | None = None, top: int = 20, **kw) -> pl.DataFrame:
    """One row per :func:`weight_variants` setup: ρ with the base ranking, top-``top`` kept overall and per
    channel, largest single rank move. ``signals`` must also carry the channel shares."""
    setup0 = {k: v for k, v in kw.items() if k in ("market", "grid", "tilt")}
    base = ranks(signals, gate=gate, **setup0)
    rows = [{"variant": name, **compare_rankings(base, ranks(signals, gate=gate, **setup), top=top)}
            for name, setup in weight_variants(**kw)]
    return pl.DataFrame(rows)


# --- thin loaders (read-only) ------------------------------------------------------------------------------


def load_ercot_counties() -> pl.DataFrame:
    """Every Texas county with its land area, ERCOT flag, main weather zone and zone shares."""
    return read_sql(
        """
        select c.county_fips, c.county_name, c.aland_m2 / 1e6 as land_km2, z.in_ercot, z.weather_zone,
               z.share_coast, z.share_east, z.share_fwest, z.share_north, z.share_ncent, z.share_south,
               z.share_scent, z.share_west, z.share_outside
        from tx_counties c left join county_weather_zone z using (county_fips)
        order by c.county_fips
        """
    )


def load_overlap() -> pl.DataFrame:
    """County × PUCT CCN territory rows, every type and ISO (``county_utility_overlap_puct``)."""
    return read_sql(
        """
        select county_fips, ccn_no, utility_type, iso_rto, in_ercot, overlap_km2, county_share
        from county_utility_overlap_puct
        """
    )
