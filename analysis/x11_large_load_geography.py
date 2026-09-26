"""X11 — where the large loads are: a weather-zone allocation of the approved stock and of the promised MW, checked
against X1's per-zone excess, re-run through X7's zone forecast, and spread to counties for the Explorer.

Run from the repo root: ``uv run --group analysis python analysis/x11_large_load_geography.py`` (~30 s).
Reads (read-only) ``large_load_chart_values``, ``large_load_headlines``, ``large_load_status``,
``puct_tsp_large_load_requests``, ``county_weather_zone``, ``county_utility_overlap_puct``, ``tceq_data_center_sites``,
``cpa_data_centers``, ``cpa_local_dev_agreements``, ``census_population_county`` and, for the X7 re-run, the load and
weather tables. Logic: ``basecast_pipelines/models/large_load_geo.py`` on top of ``large_load.py``, ``peak_excess.py``,
``peak_forecast.py``, ``data_centers.py`` and ``weather_load.py`` (all unchanged). Numbers go to
``docs/analysis/x11_large_load_geography.md``. Every MW by zone or county here is **allocated, not observed**.
"""

# %% Setup
from __future__ import annotations

from datetime import date

import numpy as np
import polars as pl

from analysis._common import ROOT, out_path
from basecast_pipelines.models import data_centers as dc
from basecast_pipelines.models import large_load as ll
from basecast_pipelines.models import large_load_geo as geo
from basecast_pipelines.models import peak_excess as px
from basecast_pipelines.models import peak_forecast as pf
from basecast_pipelines.models import weather_load as wl

Z = geo.ZONES
TODAY = date(2026, 9, 26)
BATCH_ZERO = date(2026, 4, 1)
FIRST, FEATURE, N = 2003, "t_mean_3d", 10_000
YEARS = [2027, 2028, 2029, 2030, 2031]

cv = ll.load_chart_values()
headlines = ll.load_headlines()
tsp_req = geo.load_tsp_requests()
czw = geo.load_county_weather_zone()
czl = geo.county_zone_long(czw)
overlap = geo.load_overlap_puct()
sites = dc.load_sites()
agreements = geo.load_dc_agreements()
cpa_dc = geo.load_cpa_data_centers()
pop = dc.load_population()
print(cv.shape, tsp_req.shape, czw.shape, overlap.shape, sites.shape, agreements.shape, cpa_dc.shape)


def fmt(d: dict, digits: int = 3) -> dict:
    return {k: round(v, digits) for k, v in d.items()}


# %% 1. Inventory: every source with large load by geography
def chart_summary(pattern: str) -> pl.DataFrame:
    return (cv.filter(pl.col("chart_title").str.contains(f"(?i){pattern}"))
            .group_by("chart_title").agg(pl.col("document").n_unique().alias("docs"), pl.col("report_date").min().alias("first"),
                                         pl.col("report_date").max().alias("last"), pl.len().alias("rows")))


for pat in ("by load zone", "weather zone", "by region", "counties", "TSP|Transmission Servic"):
    print(pat, "\n", chart_summary(pat))
hl_lz = headlines.filter(pl.col("dimension") == "load_zone")
print("headlines by load zone:", hl_lz.height, hl_lz["report_date"].min(), hl_lz["report_date"].max())
tsp_counts = cv.filter((pl.col("category_type") == "tsp")).group_by("unit").agg(pl.len(), pl.col("value_mw").is_not_null().sum())
print("TSP chart rows by unit (MW never given, counts only):", tsp_counts)
print("TCEQ sites:", sites.height, "since 2025:", sites.filter(pl.col("first_affil_begin_dt") >= date(2025, 1, 1)).height,
      "| CPA data centers:", cpa_dc.height, "with county:", cpa_dc["county_fips"].is_not_null().sum(),
      "| CPA data-center agreements with county:", agreements.height)

# %% 2. The weather-zone and load-zone splits in the decks
BZ_DOC, MAY, JUN, QSA_DOC = "14-Batch-Zero-Update.pdf", "May-21-LLWG-Report.pptx", "June-19-LLWG-Report.pptx", \
    "8-Interconnection-and-Grid-Analysis-Update.pdf"
bz_base = geo.chart_zone_mw(cv, document=BZ_DOC, page=7, label="^base load$")
bz_studied = geo.chart_zone_mw(cv, document=BZ_DOC, page=7, label="^studied load$")
bz_both = {z: bz_base.get(z, 0) + bz_studied.get(z, 0) for z in Z}
bz_track = geo.chart_zone_mw(cv, document=JUN, page=17)  # "Not Specified" (9.7 GW) and the total drop out
llis_total = geo.chart_zone_mw(cv, document=MAY, page=17, label="^total")
llis_nostudy = geo.chart_zone_mw(cv, document=MAY, page=17, label=r"^\(6\)")
llis_studied = geo.subtract(llis_total, llis_nostudy)
qsa = geo.chart_zone_mw(cv, document=QSA_DOC, page=7, label="^large load$")
splits = {"bz_base_2032": bz_base, "bz_studied_2032": bz_studied, "bz_base_plus_studied_2032": bz_both,
          "bz_tracking_jun2026": bz_track, "llis_total_may2026": llis_total, "llis_studied_may2026": llis_studied,
          "qsa_q4_2026": qsa}
split_tab = pl.DataFrame([{"split": k, **{z: v.get(z, 0.0) for z in Z}, "total_mw": sum(v.get(z, 0.0) for z in Z)}
                          for k, v in splits.items()])
print(split_tab.with_columns(pl.selectors.float().round(0)))
print("BZ base total", sum(bz_base.values()), "studied", sum(bz_studied.values()))

# Load-zone view of the same May 2026 population: LZ distribution (planning studies approved + under review)
lz_may = cv.filter((pl.col("document") == MAY) & (pl.col("page") == 15) & (pl.col("status_label") == "Total"))
lz_may = dict(zip(lz_may["category"].to_list(), lz_may["value_mw"].to_list(), strict=True))
lz_jun = cv.filter((pl.col("document") == JUN) & (pl.col("page") == 15) & (pl.col("status_label") == "Total"))
lz_jun = dict(zip(lz_jun["category"].to_list(), lz_jun["value_mw"].to_list(), strict=True))
print("LZ distribution May 2026:", lz_may, "sum", sum(lz_may.values()), "| LLIS studied part by zone sum",
      round(sum(llis_studied.values())), "(incl. Not Specified:", round(sum(llis_total.values()) - sum(llis_nostudy.values())), ")")
print("LZ distribution Jun 2026:", lz_jun, "sum", sum(lz_jun.values()), "| BZ tracking sum", round(sum(bz_track.values())))
F_NORTH = geo.lz_west_north_fraction(llis_studied, lz_may["LZ_WEST"])
print(f"f_north (share of NORTH in LZ_WEST) from May 2026: {F_NORTH:.3f}; "
      f"LZ_NORTH implied = NCENT + (1-f) NORTH = {llis_studied['NCENT'] + (1 - F_NORTH) * llis_studied['NORTH']:.0f} "
      f"vs {lz_may['LZ_NORTH']:.0f} (rest from EAST?)")

# Approved stock by load zone over time: headlines (regex) first, chart values (Gemini) after
hl = (hl_lz.unique(["report_date", "category"]).group_by("report_date")
      .agg(pl.col("value").filter(pl.col("category") == "LZ_WEST").first().alias("lz_west"),
           pl.col("value").filter(pl.col("category") == "Other").first().alias("other"))
      .with_columns(pl.lit("headline").alias("src")))
ch = (px.approved_by_load_zone(cv).group_by("vintage")
      .agg(pl.col("approved_mw").filter(pl.col("load_zone") == "LZ_WEST").first().alias("lz_west"),
           pl.col("approved_mw").filter(pl.col("load_zone") == "Other").first().alias("other"))
      .rename({"vintage": "report_date"}).with_columns(pl.lit("chart").alias("src")))
lzs = (pl.concat([hl, ch.filter(~pl.col("report_date").is_in(hl["report_date"].implode())
                                & (pl.col("report_date") > hl["report_date"].max()))])
       .with_columns((pl.col("lz_west") / (pl.col("lz_west") + pl.col("other"))).alias("lz_west_share")).sort("report_date"))
print(lzs.with_columns(pl.selectors.float().round(3)))
lzs.write_csv(out_path("x11_a2e_by_load_zone.csv"))


def lz_share_at(d: date) -> float:
    """LZ_WEST share of the approved stock in the latest deck at or before ``d`` (the first deck if none)."""
    before = lzs.filter(pl.col("report_date") <= d)
    return float((before if not before.is_empty() else lzs.head(1))["lz_west_share"][-1])


# %% 3. (a) Allocation of the approved-to-energize stock
LZ_NOW = lz_share_at(TODAY)
A2E_NOW = float(lzs["lz_west"][-1] + lzs["other"][-1])
s_base = geo.shares(bz_base)
stock_cands = {
    "raked_f_est": geo.rake_to_group(s_base, geo.lz_west_membership(F_NORTH), LZ_NOW),
    "raked_f1_x1": geo.rake_to_group(s_base, geo.lz_west_membership(1.0), LZ_NOW),
    "raked_f0": geo.rake_to_group(s_base, geo.lz_west_membership(0.0), LZ_NOW),
    "bz_base_unraked": s_base,
}
stock_spread = geo.share_spread(stock_cands, "raked_f_est")
stock_tab = stock_spread.with_columns((pl.col(c) * A2E_NOW).alias(f"{c}_mw") for c in ("central", "low", "high"))
print(f"A2E latest {A2E_NOW:.0f} MW, LZ_WEST share {LZ_NOW:.3f}; base-load share of FWEST+WEST+f*NORTH "
      f"= {sum(s_base[z] * geo.lz_west_membership(F_NORTH)[z] for z in Z):.3f}")
print(stock_tab.with_columns(pl.selectors.float().round(3)))
stock_tab.write_csv(out_path("x11_stock_allocation.csv"))
S_STOCK = stock_cands["raked_f_est"]

# %% 4. (b) Allocation of the promised MW: candidate splits of the pipeline + TSP RFI through service areas
tsp_rows = tsp_req.filter(~pl.col("is_total"))
print("TSP RFI 2032 by TSP:", dict(zip(*tsp_rows.filter(pl.col("year") == 2032).select("name", "mw").to_dict(as_series=False).values(), strict=True)))
ov = overlap.join(pop, on="county_fips", how="left").with_columns(
    (pl.col("population").fill_null(0) * pl.col("county_share")).alias("pop_w"))
tsp_area = geo.tsp_zone_shares(ov, czl, weight="overlap_km2")
tsp_pop = geo.tsp_zone_shares(ov, czl, weight="pop_w")
print(tsp_area.pivot(on="weather_zone", index="tsp", values="share").with_columns(pl.selectors.float().round(3)))
print(tsp_pop.pivot(on="weather_zone", index="tsp", values="share").with_columns(pl.selectors.float().round(3)))
mapped_names = set(tsp_area["tsp"].to_list())
share_mapped = (tsp_rows.filter(pl.col("name").is_in(list(mapped_names))).group_by("year").agg(pl.col("mw").sum())
                .join(tsp_rows.group_by("year").agg(pl.col("mw").sum().alias("all")), on="year")
                .with_columns((pl.col("mw") / pl.col("all")).alias("mapped_share")).sort("year"))
print("TSP RFI MW mapped to a service area:", share_mapped)


def tsp_split(tsp_sh: pl.DataFrame, year: int) -> dict[str, float]:
    a = geo.allocate_tsp_requests(tsp_rows.filter(pl.col("year") == year), tsp_sh)
    return geo.shares(dict(zip(a["weather_zone"].to_list(), a["mw"].to_list(), strict=True)))


tsp_by_year = pl.DataFrame([{"year": y, "weighting": w, **tsp_split(sh, y)} for y in range(2026, 2033)
                            for w, sh in (("area", tsp_area), ("population", tsp_pop))])
print("TSP RFI (mapped TSPs only) split by year:\n", tsp_by_year.with_columns(pl.selectors.float().round(3)))
tsp_by_year.write_csv(out_path("x11_tsp_rfi_zone_by_year.csv"))

pipe_cands = {
    "bz_base_plus_studied_2032": geo.shares(bz_both),
    "bz_studied_2032": geo.shares(bz_studied),
    "bz_tracking_jun2026": geo.shares(bz_track),
    "llis_studied_may2026": geo.shares(llis_studied),
    "tsp_rfi_2030_area": tsp_split(tsp_area, 2030),
    "tsp_rfi_2030_pop": tsp_split(tsp_pop, 2030),
    "qsa_q4_2026": geo.shares(qsa),
}
pipe_spread = geo.share_spread(pipe_cands, "bz_base_plus_studied_2032")
print(pipe_spread.with_columns(pl.selectors.float().round(3)))
pipe_spread.write_csv(out_path("x11_pipeline_shares.csv"))
S_PIPE = pipe_cands["bz_base_plus_studied_2032"]

# %% 5. X7 inputs (same code path as analysis/x7_peak_forecast.py; the modules are imported unchanged)
load = wl.load_hourly_load()
weather = wl.load_hourly_weather()
manifests = ll.load_manifests(ROOT / "data")
daily_w = wl.weather_daily(weather)
weights = wl.zone_weights(load, range(FIRST, pf.PRE_BREAK_UNTIL + 1))
daily_all = pl.concat([daily_w.drop("n_hours"), wl.system_weather_daily(daily_w, weights).drop("n_hours")])
features = wl.summer_weather_features(daily_all)
peaks = wl.summer_peaks(load)
panel = (peaks.select("weather_zone", "year", "peak_mw", "peak_local")
         .join(features, on=["weather_zone", "year"], how="inner").filter(pl.col("year") >= FIRST))
ercot = panel.filter(pl.col("weather_zone") == wl.TOTAL).sort("year")
peak_months = {r["year"]: r["peak_local"].strftime("%Y-%m") for r in ercot.iter_rows(named=True)}
x_actual = dict(zip(ercot["year"].to_list(), ercot[FEATURE].to_list(), strict=True))
load_peak = dict(zip(ercot["year"].to_list(), ercot["peak_mw"].to_list(), strict=True))
fit = pf.fit_organic(ercot, FEATURE)
excess = {y: load_peak[y] - fit.mean(y, x_actual[y]) for y in load_peak if y > pf.PRE_BREAK_UNTIL}
roll = pf.rolling_origin_errors(ercot, FEATURE, 2011, pf.PRE_BREAK_UNTIL)
RESID_SD = float(np.sqrt((roll["error_mw"] ** 2).mean()))
wide = ll.in_service_wide(cv)
vintages = ll.pick_vintages(ll.check_headlines(ll.chart_checks(wide), headlines), manifests)
bars = wide.join(vintages.select("vintage", "document", "page", "chart_title", pl.col("a2e_stock_mw").alias("base_a2e_mw")),
                 on=["vintage", "document", "page", "chart_title"], how="inner")
realized = pf.realized_year_end(vintages)
points = pf.a2e_points(cv, vintages, bars, TODAY)
pts = list(zip(points["month_idx"].to_list(), points["a2e_mw"].to_list(), strict=True))
months = {y: m for y, m in peak_months.items() if y <= 2026}
FACTOR = float(pf.observed_factor(cv, points, months, TODAY)["factor"].mean())
u_tab = pf.unattributed(excess, points, months, FACTOR)
ratios = pf.incremental_ratios(bars, realized, TODAY)


def deck_inputs(before: date | None):
    decks = pf.published_by(vintages, TODAY).drop_nulls("a2e_stock_mw")
    if before is not None:
        decks = decks.filter(pl.col("vintage") < before)
    v = decks.sort("vintage").row(-1, named=True)
    vb = bars.filter((pl.col("vintage") == v["vintage"]) & (pl.col("document") == v["document"]))
    return v["vintage"], v["a2e_stock_mw"], pf.promised_by_year(vb, range(v["vintage"].year, max(YEARS) + 1))


VINT, BASE, PROMISED = deck_inputs(BATCH_ZERO)
VINT_J, BASE_J, PROMISED_J = deck_inputs(None)
print(f"factor {FACTOR:.3f}; U median {np.median(u_tab['u_mw']):.0f}; deck {VINT} A2E {BASE:.0f}; latest {VINT_J} A2E {BASE_J:.0f}")
draws = pf.simulate(fit, YEARS, base=BASE, vintage=VINT, promised=PROMISED, ratios=ratios["ratio"].to_list(),
                    factor=FACTOR, u_values=u_tab["u_mw"].to_list(), resid_sd=RESID_SD, n=N)
print(pf.summarize(draws, YEARS).filter(pl.col("layer") == "total").with_columns(pl.selectors.float().round(0)))

# (b) table: promised MW by in-service year (cumulative, all statuses) x the pipeline split, with the band
prom_rows = []
for label, prom in (("mar2026_deck", PROMISED), ("jun2026_deck", PROMISED_J)):
    for y in (2026, 2027, 2028, 2030):
        for r in pipe_spread.iter_rows(named=True):
            prom_rows.append({"deck": label, "in_service_year": y, "promised_mw": prom.get(y), "weather_zone": r["weather_zone"],
                              "p_central_mw": prom.get(y, 0) * r["central"], "p_low_mw": prom.get(y, 0) * r["low"],
                              "p_high_mw": prom.get(y, 0) * r["high"]})
prom_tab = pl.DataFrame(prom_rows)
print(prom_tab.filter(pl.col("deck") == "mar2026_deck").pivot(on="in_service_year", index="weather_zone",
                                                               values="p_central_mw").with_columns(pl.selectors.float().round(0)))
prom_tab.write_csv(out_path("x11_promised_allocation.csv"))

# %% 6. Check against X1: do the zones with approved large load show the excess?
coinc = px.coincident_zone_loads(load)
cpanel = coinc.join(ercot.select("year", FEATURE), on="year").filter(pl.col("year") >= FIRST)
zone_fits = {z: pf.fit_organic(cpanel.filter(pl.col("weather_zone") == z).rename({"coincident_mw": "peak_mw"}), FEATURE)
             for z in Z}
zex = {z: {r["year"]: r["coincident_mw"] - zone_fits[z].mean(r["year"], r[FEATURE])
           for r in cpanel.filter((pl.col("weather_zone") == z) & pl.col("year").is_between(2023, 2026)).iter_rows(named=True)}
       for z in Z}
# the flat part: excess of the summer mean daily minimum over its pre-break model (X1 §2)
daily = px.daily_load_stats(load)
spanel = px.summer_shape(daily).join(px.summer_mean_weather(daily_w), on=["weather_zone", "year"], how="inner").filter(
    pl.col("year") >= FIRST)
ex_min = px.excess_vs_prebreak(spanel, "dmin_mean", "t_summer")
dmin = {(r["weather_zone"], r["year"]): r["excess_mw"] for r in ex_min.iter_rows(named=True)}

check_rows, zone_ll_hist = [], {z: [] for z in Z}
for y in (2023, 2024, 2025, 2026):
    m = peak_months[y]
    a2e_y = pf.interpolate(pts, pf.month_end_index(m))
    lzs_y = lz_share_at(date(int(m[:4]), int(m[5:]), 28))
    s_y = geo.rake_to_group(s_base, geo.lz_west_membership(F_NORTH), lzs_y)
    for z in Z:
        ll_z = FACTOR * a2e_y * s_y[z]
        zone_ll_hist[z].append(ll_z)
        check_rows.append({"year": y, "weather_zone": z, "a2e_mw": a2e_y, "lz_west_share": lzs_y,
                           "allocated_ll_at_peak_mw": ll_z, "allocated_a2e_mw": a2e_y * s_y[z],
                           "coincident_excess_mw": zex[z][y], "dmin_excess_mw": dmin.get((z, y))})
check = pl.DataFrame(check_rows)
check.write_csv(out_path("x11_x1_check.csv"))
print(check.filter(pl.col("year").is_in([2025, 2026])).with_columns(pl.selectors.float().round(0)))
for y in (2025, 2026):
    c = check.filter(pl.col("year") == y)
    a = dict(zip(c["weather_zone"].to_list(), c["allocated_a2e_mw"].to_list(), strict=True))
    e1 = dict(zip(c["weather_zone"].to_list(), c["coincident_excess_mw"].to_list(), strict=True))
    e2 = dict(zip(c["weather_zone"].to_list(), c["dmin_excess_mw"].to_list(), strict=True))
    print(f"{y}: Spearman(allocated A2E, coincident excess) = {geo.spearman(a, e1):.2f}; "
          f"(allocated A2E, dmin excess) = {geo.spearman(a, e2):.2f}")
    print(geo.compare_with_excess({z: FACTOR * a[z] for z in Z}, e1).with_columns(pl.selectors.float().round(0)))
mean_ex = {z: float(np.mean(list(zex[z].values()))) for z in Z}
mean_ll = {z: float(np.mean(zone_ll_hist[z])) for z in Z}
S_U, resid = geo.residual_shares(mean_ex, mean_ll)
X7_SHARE = geo.shares(mean_ex)  # X7's fixed share: mean coincident excess 2023-2026, negatives clipped
print("mean coincident excess 2023-26:", fmt(mean_ex, 0), "\nallocated LL:", fmt(mean_ll, 0), "\nresidual:", fmt(resid, 0))
print("X7 fixed share:", fmt(X7_SHARE), "\nU share (residual):", fmt(S_U))

# %% 7. X7 zone re-run: fixed share of LL + U vs X11 (stock / increment / residual U)
stock_ll = FACTOR * BASE


def zone_forecast(s_stock: dict, s_inc: dict, s_u: dict, *, fixed: dict | None = None) -> pl.DataFrame:
    parts = (geo.split_large_load(draws["large_load"], stock_ll, s_stock, s_inc) if fixed is None else None)
    rows = []
    for z in Z:
        org = pf.organic_draws(zone_fits[z], YEARS, N, np.random.default_rng(sum(map(ord, z))))
        extra = (fixed[z] * (draws["large_load"] + draws["unattributed"]) if fixed is not None
                 else parts[z] + s_u[z] * draws["unattributed"])
        tot = org + extra
        for j, y in enumerate(YEARS):
            q = np.quantile(tot[:, j], [0.1, 0.5, 0.9])
            rows.append({"weather_zone": z, "year": y, "organic_p50_mw": float(np.median(org[:, j])),
                         "ll_u_p50_mw": float(np.median(extra[:, j])), "p10_mw": q[0], "p50_mw": q[1], "p90_mw": q[2]})
    return pl.DataFrame(rows)


fc_fixed = zone_forecast({}, {}, {}, fixed=X7_SHARE)
fc_x11 = zone_forecast(S_STOCK, S_PIPE, S_U)
variants = []  # allocation band: swap one input split at a time
for name, s in stock_cands.items():
    variants.append(zone_forecast(s, S_PIPE, S_U).with_columns(pl.lit(f"stock:{name}").alias("variant")))
for name, s in pipe_cands.items():
    variants.append(zone_forecast(S_STOCK, s, S_U).with_columns(pl.lit(f"pipe:{name}").alias("variant")))
var = pl.concat(variants)
band = var.group_by("weather_zone", "year").agg(pl.col("p50_mw").min().alias("p50_alloc_low"),
                                                pl.col("p50_mw").max().alias("p50_alloc_high"))
cmp = (fc_fixed.select("weather_zone", "year", pl.col("p50_mw").alias("x7_fixed_p50"), pl.col("p10_mw").alias("x7_fixed_p10"),
                       pl.col("p90_mw").alias("x7_fixed_p90"))
       .join(fc_x11.select("weather_zone", "year", "organic_p50_mw", pl.col("ll_u_p50_mw").alias("x11_ll_u_p50"),
                           pl.col("p50_mw").alias("x11_p50"), pl.col("p10_mw").alias("x11_p10"), pl.col("p90_mw").alias("x11_p90")),
             on=["weather_zone", "year"])
       .join(band, on=["weather_zone", "year"]).with_columns((pl.col("x11_p50") - pl.col("x7_fixed_p50")).alias("diff_mw"))
       .sort("year", "weather_zone"))
print(cmp.filter(pl.col("year").is_in([2027, 2030])).with_columns(pl.selectors.float().round(0)))
print("sum of zone P50 (x11) by year:", cmp.group_by("year").agg(pl.col("x11_p50").sum(), pl.col("x7_fixed_p50").sum()).sort("year"))
cmp.write_csv(out_path("x11_zone_forecast_compare.csv"))
var.write_csv(out_path("x11_zone_forecast_variants.csv"))

# %% 8. County-level pressure for the Explorer (allocated, not observed)
sig = pl.concat([
    sites.select("county_fips", pl.lit(1).alias("tceq_sites"),
                 (pl.col("first_affil_begin_dt") >= date(2025, 1, 1)).cast(pl.Int64).alias("tceq_sites_since_2025"),
                 pl.lit(0).alias("cpa_dc_agreements"), pl.lit(0).alias("cpa_dc_registrations")),
    agreements.select("county_fips", pl.lit(0).alias("tceq_sites"), pl.lit(0).alias("tceq_sites_since_2025"),
                      pl.lit(1).alias("cpa_dc_agreements"), pl.lit(0).alias("cpa_dc_registrations")),
    cpa_dc.drop_nulls("county_fips").select("county_fips", pl.lit(0).alias("tceq_sites"), pl.lit(0).alias("tceq_sites_since_2025"),
                                            pl.lit(0).alias("cpa_dc_agreements"), pl.lit(1).alias("cpa_dc_registrations")),
], how="vertical_relaxed").group_by("county_fips").agg(pl.all().sum()).with_columns(
    (pl.col("tceq_sites") + pl.col("cpa_dc_agreements") + pl.col("cpa_dc_registrations")).alias("n_signals"))
ll30 = geo.split_large_load(draws["large_load"], stock_ll, S_STOCK, S_PIPE)
zone_measures = {
    "a2e_stock_mw": {z: S_STOCK[z] * A2E_NOW for z in Z},
    "ll_at_peak_2030_p50_mw": {z: float(np.median(ll30[z][:, YEARS.index(2030)])) for z in Z},
    "bz_base_2032_mw": bz_base,
    "bz_base_plus_studied_2032_mw": bz_both,
}
cp = geo.county_pressure(zone_measures, sig, czl)
names = czw.select("county_fips", "county_name", "weather_zone")
bz_named = cv.filter((pl.col("document") == BZ_DOC) & pl.col("chart_title").str.contains("(?i)counties"))
bz_named = bz_named.pivot(on="status_label", index="category", values="value_mw").rename({"category": "county_name"})
county = (names.join(sig, on="county_fips", how="left").join(cp.drop("main_zone"), on="county_fips", how="left")
          .join(bz_named, on="county_name", how="left").fill_null(0)
          .with_columns(pl.lit("allocated, not observed").alias("alloc_label")).sort("a2e_stock_mw_alloc", descending=True))
print(county.head(20).with_columns(pl.selectors.float().round(0)))
unalloc = {z: zone_measures["a2e_stock_mw"][z] for z in Z if z not in set(cp["main_zone"].to_list())
           and sig.join(czl, on="county_fips").filter(pl.col("weather_zone") == z).is_empty()}
# Validation of the county spread: the Batch Zero deck names its top counties (observed), the spread allocates them
named_chk = (county.filter((pl.col("Base load") > 0) | (pl.col("Base + studied load") > 0))
             .select("county_name", "weather_zone", "n_signals", "Base load", "bz_base_2032_mw_alloc", "Base + studied load",
                     "bz_base_plus_studied_2032_mw_alloc"))
print("named counties, observed vs allocated (MW):\n", named_chk.with_columns(pl.selectors.float().round(0)))
named_chk.write_csv(out_path("x11_named_county_check.csv"))
print("zones with no county signal (MW left at zone level):", fmt(unalloc, 0))
county.write_csv(out_path("x11_county_pressure.csv"))
zone_out = pl.DataFrame([{"weather_zone": z, "stock_share": S_STOCK[z], "pipeline_share": S_PIPE[z], "u_share": S_U[z],
                          "x7_fixed_share": X7_SHARE[z], **{k: v[z] for k, v in zone_measures.items()}} for z in Z])
print(zone_out.with_columns(pl.selectors.float().round(3)))
zone_out.write_csv(out_path("x11_zone_allocation.csv"))
print("tables written to analysis/out/x11_*")
