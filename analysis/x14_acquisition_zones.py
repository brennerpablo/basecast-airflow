# %% [markdown]
# # X14 — Priority acquisition zones by county (Explorer)
#
# A first definition of the Explorer's "priority acquisition zones": a 0-1 county priority from territory
# signals only (homeowner market + grid value), gated by the county's addressable share, with the channel
# split (retail-direct in the competitive IOU area vs partnership in co-op/muni territory) from public PUCT
# territory types. Numbers go to `docs/analysis/x14_acquisition_zones.md`.
#
# Validation lock: no account score, account rank or partner information is read here. The competitive area
# comes from PUCT territory types only, not from Base's own service areas.
#
# Inputs from earlier explorations (run them first; their outputs are gitignored in `analysis/out/`):
# X1 (`x1_excess_peak.csv`), X2 (`x2_county_adjusted.csv`), X11 (`x11_zone_allocation.csv`),
# X12 (`x12_summer_peak.csv`). RTM load-zone prices are parsed from the local lake (`ercot_spp_hist`) and
# cached in `analysis/out/x14_rtm_lz.parquet`.
#
# Run: `uv run --group analysis python analysis/x14_acquisition_zones.py`

# %%
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis._common import OUT, ROOT, out_path  # noqa: E402
from basecast_pipelines.models import accounts as A  # noqa: E402
from basecast_pipelines.models import acquisition_zones as Z  # noqa: E402
from basecast_pipelines.models import data_centers as DC  # noqa: E402
from basecast_pipelines.models import large_load_geo as LLG  # noqa: E402

POP_START, POP_END = 2020, 2025  # Census PEP vintage 2025
PERMIT_YEARS = [2023, 2024, 2025]
HOUSING_VINTAGE = 2024  # ACS 5-year B25032
DC_SINCE = date(2025, 1, 1)
PEAK_START, PEAK_END = 2021, 2026  # X12 normalized summer peak P50, CAGR window
EXCESS_YEARS = [2023, 2024, 2025, 2026]  # X1 excess over the pre-break fit, mean of excess_pct
PRICE_YEARS = [2023, 2024, 2025]  # RTM load-zone prices, full years
LTLF_START, LTLF_END = 2025, 2031  # alternative zone growth (Q3's signal)
TOP = 20

for name in ("x1_excess_peak.csv", "x2_county_adjusted.csv", "x11_zone_allocation.csv", "x12_summer_peak.csv"):
    if not (OUT / name).exists():
        raise SystemExit(f"missing analysis/out/{name}: run the X1, X2, X11 and X12 scripts first")

# %% [markdown]
# ## Counties and the channel split

# %%
counties = Z.load_ercot_counties()
ercot = counties.filter(pl.col("in_ercot").fill_null(False))
print("Texas counties", counties.height, "| ERCOT counties (county_weather_zone.in_ercot)", ercot.height)

overlap = Z.load_overlap()
channels = Z.channel_split(overlap)
acct_links = Z.county_accounts(overlap)
ch = ercot.select("county_fips", "county_name").join(channels, on="county_fips", how="left")
print("ERCOT counties without a PUCT territory row:", ch.filter(pl.col("channel").is_null()).height)
print(ch.group_by("channel").agg(pl.len().alias("counties")).sort("channel"))
print(ch.filter(pl.col("channel") == "partnership").group_by("partner_type").agg(pl.len()).sort("partner_type"))
print(
    "mean shares over ERCOT counties:",
    ch.select(pl.col("retail_share", "coop_share", "muni_share", "outside_share").mean()).row(0, named=True),
)
print("addressable share < 0.9:", ch.filter(pl.col("addressable_share") < 0.9).height)

# %% [markdown]
# ## Homeowner market (county grain through the `accounts.py` builders)

# %%
links = Z.county_links(counties.select("county_fips", "land_km2"))
population = A.load_population()
pop = A.population_growth(population, links, start=POP_START, end=POP_END)
permits_raw = A.load_permits_annual(PERMIT_YEARS)
permits = permits_raw.group_by("county_fips").agg(pl.col("units_total").sum().alias("units"))
perm = A.permits_per_1k(permits, pop, links)
housing = A.owner_single_family(A.load_housing(HOUSING_VINTAGE), links)
dc = A.data_center_sites(A.load_dc_sites(), links, since=DC_SINCE)

market = (
    pop.join(perm, on="account_id", how="full", coalesce=True)
    .join(housing, on="account_id", how="full", coalesce=True)
    .join(dc, on="account_id", how="full", coalesce=True)
    .rename({"account_id": "county_fips"})
)

# %% [markdown]
# ## Grid value (zone signals carried to counties by their weather-zone mix)

# %%
czl = LLG.county_zone_long(counties.filter(pl.col("in_ercot").fill_null(False)))

x12 = pl.read_csv(OUT / "x12_summer_peak.csv")
p50 = {
    (r["weather_zone"], r["year"]): r["p50"]
    for r in x12.filter(pl.col("year").is_in([PEAK_START, PEAK_END])).iter_rows(named=True)
}
zone_cagr = {z: Z.cagr(p50[(z, PEAK_START)], p50[(z, PEAK_END)], PEAK_END - PEAK_START) for z in Z.ZONES}
print("X12 normalized summer peak CAGR", PEAK_START, "->", PEAK_END, {z: round(v * 100, 1) for z, v in zone_cagr.items()})

x1 = pl.read_csv(OUT / "x1_excess_peak.csv")
excess = dict(
    x1.filter(pl.col("year").is_in(EXCESS_YEARS) & pl.col("weather_zone").is_in(Z.ZONES))
    .group_by("weather_zone")
    .agg(pl.col("excess_pct").mean())
    .iter_rows()
)
print("X1 mean excess_pct", EXCESS_YEARS, {z: round(excess[z], 1) for z in Z.ZONES})

x11 = pl.read_csv(OUT / "x11_zone_allocation.csv")
stock = dict(x11.select("weather_zone", "a2e_stock_mw").iter_rows())
stock_pressure = {z: stock[z] / p50[(z, PEAK_END)] for z in Z.ZONES}  # alternative: X11 approved stock ÷ 2026 P50 peak
print("X11 approved stock / 2026 P50 peak", {z: round(v, 3) for z, v in stock_pressure.items()})

# %% RTM load-zone prices: a 2-hour battery's daily spread
price_cache = out_path("x14_rtm_lz.parquet")
if not price_cache.exists():
    from basecast_pipelines.parsers.ercot.spp_hist import parse_rtm

    frames = []
    for year in PRICE_YEARS:
        paths = sorted((ROOT / "data" / "raw" / "source=ercot_spp_hist").glob(f"dt=*/*RTMLZHBSPP_{year}.zip"))
        if not paths:
            raise SystemExit(f"no RTMLZHBSPP_{year}.zip in the local lake")
        p = paths[-1]  # newest snapshot of that year's workbook
        f = SimpleNamespace(name=p.name, suffix=".zip", key=str(p), meta={}, url=str(p), read_bytes=p.read_bytes)
        df = parse_rtm(f)
        frames.append(
            df.filter(pl.col("settlement_point").is_in(Z.LOAD_ZONES) & (pl.col("settlement_point_type") == "LZ"))
            .select("settlement_point", pl.col("delivery_date").alias("day"), "interval_start_utc", "price_usd_mwh")
        )
        print("rtm", p.name, df.height)
    pl.concat(frames).unique(["settlement_point", "interval_start_utc"], keep="last").write_parquet(price_cache)
prices = pl.read_parquet(price_cache)
spread = Z.daily_spread(prices)
lz_table = (
    spread.with_columns(pl.col("day").dt.year().alias("year"))
    .group_by("settlement_point", "year")
    .agg(pl.col("spread_usd_mwh").mean().alias("mean_spread"), pl.col("spread_usd_mwh").median().alias("median_spread"))
    .sort("settlement_point", "year")
)
print(lz_table)
lz_mean = dict(spread.group_by("settlement_point").agg(pl.col("spread_usd_mwh").mean()).iter_rows())
lz_median = dict(spread.group_by("settlement_point").agg(pl.col("spread_usd_mwh").median()).iter_rows())
print("mean daily 2h spread 2023-2025, $/MWh", {k: round(v, 1) for k, v in sorted(lz_mean.items())})
print("median daily 2h spread 2023-2025, $/MWh", {k: round(v, 1) for k, v in sorted(lz_median.items())})
lz_wz = Z.weather_zone_values(lz_mean)

grid = (
    Z.zone_to_county(zone_cagr, czl, "zone_peak_cagr")
    .join(Z.zone_to_county(excess, czl, "ll_pressure"), on="county_fips", how="full", coalesce=True)
    .join(Z.zone_to_county(lz_wz, czl, "lz_spread"), on="county_fips", how="full", coalesce=True)
    .join(Z.zone_to_county(stock_pressure, czl, "ll_stock_pressure"), on="county_fips", how="full", coalesce=True)
    .join(Z.zone_to_county(Z.weather_zone_values(lz_median), czl, "lz_spread_median"), on="county_fips", how="full",
          coalesce=True)
)
ltlf = A.zone_peak_growth(A.load_zone_peaks(), A.load_county_zone(), links, start=LTLF_START, end=LTLF_END).rename(
    {"account_id": "county_fips", "zone_peak_cagr": "ltlf_zone_cagr"}
)

# X2 adjusted generation queue: context only (not weighted)
x2 = pl.read_csv(OUT / "x2_county_adjusted.csv", schema_overrides={"county_fips": pl.Utf8}).select(
    "county_fips", pl.col("raw_mw").alias("gen_raw_mw"), pl.col("adj_mw_2028").alias("gen_adj_mw_2028"),
    pl.col("adj_mw_2028_storage").alias("gen_adj_storage_mw_2028"), pl.col("adj_mw_2028_solar").alias("gen_adj_solar_mw_2028"),
)

# density for the "rural" face-validity cut (Q4's rule, ≥ 100/km² = metro)
density = DC.county_density(
    population.filter(pl.col("year") == POP_END).select("county_fips", pl.col("value").alias("population")),
    counties.select("county_fips", (pl.col("land_km2") * 1e6).alias("aland_m2")),
).select("county_fips", "density_per_km2")

# %% [markdown]
# ## Signals, coverage, redundancy

# %%
signals = (
    ercot.select("county_fips", "county_name", "weather_zone")
    .join(channels, on="county_fips", how="left")
    .join(market, on="county_fips", how="left")
    .join(grid, on="county_fips", how="left")
    .join(ltlf, on="county_fips", how="left")
    .join(x2, on="county_fips", how="left")
    .join(density, on="county_fips", how="left")
    .with_columns(
        (pl.col("owner_sf_homes") * pl.col("addressable_share").fill_null(0)).alias("addr_sf_homes"),
        pl.col("dc_sites").fill_null(0.0),
        pl.col("gen_raw_mw", "gen_adj_mw_2028", "gen_adj_storage_mw_2028", "gen_adj_solar_mw_2028").fill_null(0.0),
    )
)
cands = [*Z.WEIGHTS, "population", "permit_units", "owner_sf_homes", "ll_stock_pressure", "ltlf_zone_cagr",
         "gen_adj_mw_2028"]
summary = A.signal_summary(signals.rename({"county_fips": "account_id"}), cands)
print(summary.select("signal", "n", "coverage", "min", "median", "max", "zero_share", "mode_share", "near_constant", "enters"))
pairs = A.redundant_pairs(signals, [*Z.WEIGHTS, "ltlf_zone_cagr", "ll_stock_pressure", "gen_adj_mw_2028"])
print(pairs.filter(pl.col("spearman").abs() >= 0.5).sort(pl.col("spearman").abs(), descending=True))
summary.write_csv(out_path("x14_signal_summary.csv"))
pairs.write_csv(out_path("x14_signal_pairs.csv"))

# %% [markdown]
# ## Score, drivers, channel priority, legend

# %%
scored = Z.channel_priority(Z.score_counties(signals))
scored = scored.join(Z.drivers(scored), on="county_fips", how="left")
breaks, classes = Z.legend_classes(scored["priority"])
scored = scored.with_columns(classes.alias("priority_class"))
print("priority quintile breaks", [round(b, 3) for b in breaks])
print(scored.select(pl.col("priority").min().alias("min"), pl.col("priority").median().alias("median"),
                    pl.col("priority").max().alias("max")))

show = ["rank", "county_name", "weather_zone", "priority", "market_score", "grid_score", "grid_factor", "channel",
        "retail_share", "partner_share", "drivers"]
print(scored.select(show).head(25))

top_retail = scored.sort("retail_rank", nulls_last=True).head(TOP).select(
    "retail_rank", "county_name", "weather_zone", "retail_priority", "priority", "rank", "retail_share",
    "market_score", "grid_score", "owner_sf_homes", "pop_growth", "permits_per_1k", "drivers",
)
top_partner = scored.sort("partner_rank", nulls_last=True).head(TOP).select(
    "partner_rank", "county_name", "weather_zone", "partner_priority", "priority", "rank", "partner_share",
    "partner_type", "n_partners", "top_partner_share", "market_score", "grid_score", "density_per_km2", "drivers",
)
print(top_retail)
print(top_partner)

# %% [markdown]
# ## Face validity
#
# Metro growth rings: counties next to the five metro cores. Claude's list (adjacent counties), not an OMB
# MSA definition.

# %%
CORES = {"Dallas", "Tarrant", "Harris", "Travis", "Bexar"}
RINGS = {
    "DFW": {"Collin", "Denton", "Rockwall", "Kaufman", "Ellis", "Johnson", "Parker", "Wise", "Hunt", "Hood"},
    "Houston": {"Fort Bend", "Montgomery", "Brazoria", "Galveston", "Waller", "Liberty", "Chambers"},
    "Austin": {"Williamson", "Hays", "Bastrop", "Caldwell", "Burnet"},
    "San Antonio": {"Comal", "Guadalupe", "Medina", "Wilson", "Kendall", "Bandera", "Atascosa"},
}
ring_of = {c: m for m, cs in RINGS.items() for c in cs}
tag = pl.col("county_name").replace_strict(ring_of, default=None)
scored = scored.with_columns(
    pl.when(pl.col("county_name").is_in(list(CORES))).then(pl.lit("core")).otherwise(tag).alias("metro_ring")
)
for label, rank_col in (("retail", "retail_rank"), ("partner", "partner_rank"), ("overall", "rank")):
    t = scored.filter(pl.col(rank_col) <= TOP)
    print(label, "top 20: ring", t.filter(pl.col("metro_ring").is_in(list(RINGS))).height,
          "| core", t.filter(pl.col("metro_ring") == "core").height,
          "| elsewhere", t.filter(pl.col("metro_ring").is_null()).height)
    print("   by metro", t.group_by("metro_ring").agg(pl.len()).sort("metro_ring").rows())
ring_ranks = scored.filter(pl.col("metro_ring").is_not_null()).select(
    "metro_ring", "county_name", "rank", "retail_rank", "partner_rank", "channel", "retail_share", "priority"
).sort("rank")
print(ring_ranks)

# Rural partnership counties that rise because of grid value: market-only rank vs the full rank
market_only = Z.score_counties(signals, tilt=0.0).select("county_fips", pl.col("rank").alias("rank_market_only"))
rise = (
    scored.join(market_only, on="county_fips")
    .with_columns((pl.col("rank_market_only") - pl.col("rank")).alias("rank_gain_from_grid"))
    .filter((pl.col("partner_share") >= 0.5) & (pl.col("density_per_km2") < 100))
    .sort("rank_gain_from_grid", descending=True)
)
print(rise.select("county_name", "weather_zone", "rank", "rank_market_only", "rank_gain_from_grid", "partner_share",
                  "grid_score", "market_score", "dc_sites", "drivers").head(15))
rural_partner_top = scored.filter((pl.col("partner_rank") <= TOP) & (pl.col("density_per_km2") < 100))
print("rural (< 100/km²) counties in the partnership top 20:", rural_partner_top.height)

# %% [markdown]
# ## Why a tilt and not an additive mean
#
# The same signals and block weights, combined as a Q3-style weighted mean with the grid block at 0.45.

# %%
additive = Z.channel_priority(Z.score_counties(signals, tilt=0.45, combine="additive"))
add_top = additive.sort("retail_rank", nulls_last=True).head(TOP).select(
    "retail_rank", "county_name", "weather_zone", "owner_sf_homes", "population", pl.col("market_score").round(2),
    pl.col("grid_score").round(2),
)
print(add_top)
print("additive retail top 20: FWEST/WEST counties", add_top.filter(pl.col("weather_zone").is_in(["FWEST", "WEST"])).height,
      "| with < 5,000 owner SF homes", add_top.filter(pl.col("owner_sf_homes") < 5000).height)
tilt_top = scored.filter(pl.col("retail_rank") <= TOP)
print("tilt retail top 20: FWEST/WEST counties", tilt_top.filter(pl.col("weather_zone").is_in(["FWEST", "WEST"])).height,
      "| with < 5,000 owner SF homes", tilt_top.filter(pl.col("owner_sf_homes") < 5000).height)

share_mode = Z.channel_priority(Z.score_counties(signals), mode="share").join(
    signals.select("county_fips", "owner_sf_homes"), on="county_fips", suffix="_s")
sm = share_mode.filter(pl.col("retail_rank") <= TOP)
print("share-weighted retail top 20: < 5,000 owner SF homes", sm.filter(pl.col("owner_sf_homes") < 5000).height,
      "| FWEST/WEST", sm.filter(pl.col("weather_zone").is_in(["FWEST", "WEST"])).height, "|",
      sm.sort("retail_rank", nulls_last=True).select("county_name").to_series().to_list())
print("Collin, Denton retail rank under priority × share:",
      share_mode.filter(pl.col("county_name").is_in(["Collin", "Denton"])).select("county_name", "retail_rank").rows())

# %% [markdown]
# ## Sensitivity

# %%
stab = Z.rank_stability(signals, top=TOP)
print(stab)
shift = stab.filter(pl.col("variant").str.contains(r"[+-]0\.05"))
print("±0.05 inside a block: min ρ", round(shift["spearman"].min(), 4), "| min kept all/retail/partner",
      shift["all_top20_kept"].min(), shift["retail_top20_kept"].min(), shift["partner_top20_kept"].min())
loo = stab.filter(pl.col("variant").str.contains("without"))
print("leave-one-signal-out: min ρ", round(loo["spearman"].min(), 4), "| min kept all/retail/partner",
      loo["all_top20_kept"].min(), loo["retail_top20_kept"].min(), loo["partner_top20_kept"].min())

alts = {
    "zone growth = LTLF 2025 CAGR 2025-31 (Q3)": signals.with_columns(pl.col("ltlf_zone_cagr").alias("zone_peak_cagr")),
    "ll pressure = X11 approved stock / peak": signals.with_columns(pl.col("ll_stock_pressure").alias("ll_pressure")),
    "lz spread = median day": signals.with_columns(pl.col("lz_spread_median").alias("lz_spread")),
    "size = owner_sf_homes (no ERCOT share)": signals.with_columns(pl.col("owner_sf_homes").alias("addr_sf_homes")),
}
base_r = Z.ranks(signals)
alt_rows = []
for name, sig in alts.items():
    alt_rows.append({"variant": name, **Z.compare_rankings(base_r, Z.ranks(sig), top=TOP)})
alt_rows.append({"variant": "addressable share also multiplies the priority",
                 **Z.compare_rankings(base_r, Z.ranks(signals, gate="addressable_share"), top=TOP)})
gated = Z.score_counties(signals, gate="addressable_share")
print("with the gate on the priority: Montgomery rank",
      gated.filter(pl.col("county_name") == "Montgomery")["rank"].item())
alt_rows.append({"variant": "channel lists by priority × share", **Z.compare_rankings(base_r, Z.ranks(signals, mode="share"), top=TOP)})
for t in (0.4, 0.45):
    alt_rows.append({"variant": f"additive mean, grid weight {t}",
                     **Z.compare_rankings(base_r, Z.ranks(signals, tilt=t, combine="additive"), top=TOP)})
alt_tab = pl.DataFrame(alt_rows)
print(alt_tab)
pl.concat([stab, alt_tab], how="diagonal").write_csv(out_path("x14_sensitivity.csv"))

# %% [markdown]
# ## Outputs (the map prototype and the county → accounts link)

# %%
keep = [
    "county_fips", "county_name", "weather_zone", "rank", "priority", "priority_class", "market_score", "grid_score",
    "grid_factor",
    "channel", "partner_type", "retail_share", "coop_share", "muni_share", "outside_share", "partner_share",
    "addressable_share", "n_partners", "top_partner_share", "retail_priority", "retail_rank", "partner_priority",
    "partner_rank", "drivers", "drags", *Z.WEIGHTS, *[f"pct_{c}" for c in Z.WEIGHTS], "owner_sf_homes", "population",
    "gen_raw_mw", "gen_adj_mw_2028", "gen_adj_storage_mw_2028", "density_per_km2", "metro_ring",
]
scored.select(keep).sort("county_fips").write_csv(out_path("x14_county_priority.csv"))
top_retail.write_csv(out_path("x14_top_retail.csv"))
top_partner.write_csv(out_path("x14_top_partner.csv"))
acct_links.write_csv(out_path("x14_county_accounts.csv"))
print("county → accounts rows", acct_links.height, "| partnership counties with ≥ 1 account",
      acct_links.join(scored.filter(pl.col("channel") == "partnership").select("county_fips"), on="county_fips")
      ["county_fips"].n_unique())
print("n_partners distribution (partnership counties)",
      scored.filter(pl.col("channel") == "partnership").group_by("n_partners").agg(pl.len()).sort("n_partners").rows())
print("written:", sorted(p.name for p in OUT.glob("x14_*")))
