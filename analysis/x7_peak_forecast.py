"""X7 — prototype of the peak forecast (P10/P50/P90) and a pseudo-out-of-sample backtest against ERCOT's forecasts.

Run from the repo root: ``uv run --group analysis python analysis/x7_peak_forecast.py`` (~30 s).
Reads ``ercot_load_hourly_wz``, ``weather_hourly_wz``, ``large_load_chart_values``, ``large_load_headlines``,
``ercot_monthly_peaks`` and ``official_forecasts`` (read-only). Logic: ``basecast_pipelines/models/peak_forecast.py``
on top of Q7's ``weather_load.py``, Q5's ``large_load.py``, X1's ``peak_excess.py`` and Q1's ``backtest.py``
(all unchanged). Numbers go to ``docs/analysis/x7_peak_forecast.md``.
"""

# %% Setup
from __future__ import annotations

from datetime import date

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import polars as pl

from analysis._common import ROOT, out_path
from basecast_pipelines.models import backtest as bt
from basecast_pipelines.models import large_load as ll
from basecast_pipelines.models import peak_excess as px
from basecast_pipelines.models import peak_forecast as pf
from basecast_pipelines.models import weather_load as wl

FIRST, FEATURE = 2003, "t_mean_3d"
TODAY = date(2026, 9, 26)
FORECAST_YEARS = [2027, 2028, 2029, 2030, 2031]
# the month end after each official publication since 2023: LTLF 2023 + CDR May 2023, CDR Dec 2023, CDR May 2024, LTLF 2024
# (Jul 18), CDR Dec 2024, LTLF 2025 + CDR May 2025, CDR Dec 2025, 2026 preliminary LTLF (Apr 15)
AS_OF_DATES = [date(2023, 5, 31), date(2023, 12, 31), date(2024, 5, 31), date(2024, 7, 31), date(2024, 12, 31),
               date(2025, 5, 31), date(2025, 12, 31), date(2026, 5, 31)]
LAST_ACTUAL = 2026
BATCH_ZERO = date(2026, 4, 1)  # first deck with the >400 GW queue: 2026-04-24
N = 10_000

load = wl.load_hourly_load()
weather = wl.load_hourly_weather()
cv = ll.load_chart_values()
headlines = ll.load_headlines()
manifests = ll.load_manifests(ROOT / "data")
official = bt.base_series(bt.load_official_forecasts())
actual_tbl = bt.actual_summer_peaks()
actual = dict(zip(actual_tbl["year"].to_list(), actual_tbl["actual_mw"].to_list(), strict=True))
print(load.shape, weather.shape, cv.shape, official.shape, "actual 2026:", actual.get(2026))

# %% 1. Panel: summer peaks + weather (ERCOT weather weighted by 2003-2019 summer energy, as in X1)
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
print(f"organic fit <= 2019: beta {np.round(fit.beta, 1)}, sigma {fit.sigma:.0f} MW, "
      f"P50 weather {fit.x_p50:.2f} °C, trend SE {np.sqrt(fit.cov[1][1]):.0f} MW/yr")
excess = {y: load_peak[y] - fit.mean(y, x_actual[y]) for y in load_peak if y > pf.PRE_BREAK_UNTIL}
print("excess over the pre-break organic model (MW):", {y: round(v) for y, v in excess.items()})

# %% 2. Organic band: one-year-ahead rolling-origin errors at median weather over the pre-break summers
# Q7's proposed default (the SD of the one-year-ahead errors) restricted to 2011-2019, so the post-2020 step, which
# the large-load and unattributed layers carry, is not also counted in the organic band.
roll = pf.rolling_origin_errors(ercot, FEATURE, 2011, pf.PRE_BREAK_UNTIL)
RESID_SD = float(np.sqrt((roll["error_mw"] ** 2).mean()))
print(roll.with_columns(pl.selectors.float().round(1)))
print(f"1-yr-ahead, 2011-2019: MAPE {roll['error_pct'].abs().mean():.1f}%, bias {roll['error_pct'].mean():+.1f}%, "
      f"RMSE {RESID_SD:.0f} MW (in-sample residual SD {fit.sigma:.0f} MW)")
cal = []
for y in range(2011, pf.PRE_BREAK_UNTIL + 1):
    f_y = pf.fit_organic(ercot, FEATURE, until=y - 1)
    for label, sd in (("in-sample sigma + weather draws", None), ("rolling RMSE at P50 weather", RESID_SD)):
        d = pf.organic_draws(f_y, [y], N, np.random.default_rng(y), resid_sd=sd)[:, 0]
        p10, p90 = np.quantile(d, [0.1, 0.9])
        cal.append({"band": label, "year": y, "in_band": p10 <= load_peak[y] <= p90,
                    "half_width_pct": 100 * (p90 - p10) / 2 / load_peak[y]})
cal = pl.DataFrame(cal)
print(cal.group_by("band").agg(pl.col("in_band").sum().alias("inside_p10_p90_of_9"),
                               pl.col("half_width_pct").mean().round(2)))

# %% 3. Decks: in-service bars per picked vintage, realized year-end stock and when it was known
wide = ll.in_service_wide(cv)
vintages = ll.pick_vintages(ll.check_headlines(ll.chart_checks(wide), headlines), manifests)
bars = wide.join(vintages.select("vintage", "document", "page", "chart_title",
                                 pl.col("a2e_stock_mw").alias("base_a2e_mw")),
                 on=["vintage", "document", "page", "chart_title"], how="inner")
realized = pf.realized_year_end(vintages)
print(realized)


def inputs_asof(as_of: date, *, vintage_before: date | None = None, firm: bool = False) -> dict:
    """Every input of the method as it could be known on ``as_of``. ``vintage_before`` picks the latest deck
    published before that date instead of the latest one (the pre-Batch-Zero variant)."""
    last_summer = as_of.year if as_of.month >= 9 else as_of.year - 1  # 2026: peak Jul 22, Sep max 4.6 GW lower
    points = pf.a2e_points(cv, vintages, bars, as_of)
    months = {y: m for y, m in peak_months.items() if y <= last_summer}
    fac = pf.observed_factor(cv, points, months, as_of)
    ratios = pf.incremental_ratios(bars, realized, as_of, firm=firm)
    leaks = []
    if fac.is_empty():  # not estimable yet: take the first estimate that exists and flag it
        first = min(d for d in vintages["report_date"].to_list() if not pf.observed_factor(
            cv, pf.a2e_points(cv, vintages, bars, d), {y: m for y, m in peak_months.items() if y <= d.year - 1},
            d).is_empty())
        fac = pf.observed_factor(cv, pf.a2e_points(cv, vintages, bars, first),
                                 {y: m for y, m in peak_months.items() if y <= first.year - 1}, first)
        leaks.append(f"factor from decks published {first}")
    if ratios.is_empty():
        first = realized.filter(pl.col("known_from") > as_of)["known_from"].min()
        ratios = pf.incremental_ratios(bars, realized, first, firm=firm)
        leaks.append(f"ratios known from {first}")
    factor = float(fac["factor"].mean())
    u = pf.unattributed({y: v for y, v in excess.items() if y <= last_summer}, points, months, factor)
    decks = pf.published_by(vintages, as_of).drop_nulls("a2e_stock_mw")
    if vintage_before is not None:
        decks = decks.filter(pl.col("vintage") < vintage_before)
    v = decks.sort("vintage").row(-1, named=True)
    vbars = bars.filter((pl.col("vintage") == v["vintage"]) & (pl.col("document") == v["document"]))
    return {"as_of": as_of, "points": points, "factor_rows": fac, "factor": factor, "ratios": ratios, "u": u,
            "vintage": v["vintage"], "base": v["a2e_stock_mw"], "bars": vbars, "leaks": leaks, "firm": firm}


def run(inp: dict, years: list[int], *, fit_: pf.OrganicFit = fit, with_ll: bool = True, with_u: bool = True):
    promised = pf.promised_by_year(inp["bars"], range(inp["vintage"].year, max(years) + 1), firm=inp["firm"])
    draws = pf.simulate(
        fit_, years, base=inp["base"] if with_ll else None, vintage=inp["vintage"], promised=promised,
        ratios=inp["ratios"]["ratio"].to_list(), factor=inp["factor"],
        u_values=inp["u"]["u_mw"].to_list() if with_u else None, resid_sd=RESID_SD, n=N,
    )
    return draws, pf.summarize(draws, years), promised


# %% 4. Inputs as of today
now = inputs_asof(TODAY)
print("factor rows:\n", now["factor_rows"])
print(f"factor mean {now['factor']:.3f}")
print("U rows:\n", now["u"].with_columns(pl.selectors.float().round(0)))
print("ratios (incremental, horizon >= 6 months):\n", now["ratios"].with_columns(pl.selectors.float().round(3)))
rq = np.quantile(now["ratios"]["ratio"].to_numpy(), [0.1, 0.5, 0.9])
print(f"ratio p10/p50/p90 = {rq.round(3)} over {now['ratios'].height} vintage x year pairs "
      f"(targets {sorted(set(now['ratios']['target_year'].to_list()))})")
now_prebz = inputs_asof(TODAY, vintage_before=BATCH_ZERO)
print("latest deck:", now["vintage"], now["base"], "| pre-Batch-Zero deck:", now_prebz["vintage"], now_prebz["base"])

# %% 5. Forecast 2027-2031: two queue vintages (latest = post Batch Zero; last deck before it)
years_fc = FORECAST_YEARS
draws_bz, fc_bz, prom_bz = run(now, years_fc)
draws_pre, fc_pre, prom_pre = run(now_prebz, years_fc)
print("promised by year, Jun 2026 deck:", {y: round(v) for y, v in prom_bz.items()})
print("promised by year, Mar 2026 deck:", {y: round(v) for y, v in prom_pre.items()})


def wide_fc(fc: pl.DataFrame, label: str) -> pl.DataFrame:
    return (fc.with_columns(pl.lit(label).alias("variant"))
            .select("variant", "layer", "year", "p10_mw", "p50_mw", "p90_mw").sort("layer", "year"))


fc_all = pl.concat([wide_fc(fc_pre, "pre_batch_zero_deck_2026_03"), wide_fc(fc_bz, "latest_deck_2026_06")])
print(fc_all.with_columns(pl.selectors.float().round(0)))
fc_all.write_csv(out_path("x7_forecast_ercot.csv"))

# Sanity check (not a forecast): approvals keep their 2023-2025 pace instead of following the queue
dec = dict(zip(realized["target_year"].to_list(), realized["realized_a2e_mw"].to_list(), strict=True))
pace = (dec[2025] - dec[2022]) / 3
a2e_jun26 = pf.interpolate(list(zip(now["points"]["month_idx"].to_list(), now["points"]["a2e_mw"].to_list(),
                                    strict=True)), pf.month_end_index("2026-06"))
org_p50 = dict(zip(*fc_pre.filter(pl.col("layer") == "organic").select("year", "p50_mw").to_dict(as_series=False).values(),
                   strict=True))
u_p50 = float(np.median(now["u"]["u_mw"].to_numpy()))
pace_rows = [{"year": y, "a2e_summer_mw": a2e_jun26 + pace * (pf.summer_index(y) - pf.month_end_index("2026-06")) / 12,
              } for y in years_fc]
pace_tab = pl.DataFrame(pace_rows).with_columns((pl.col("a2e_summer_mw") * now["factor"]).alias("ll_mw")).with_columns(
    (pl.col("year").replace_strict(org_p50, return_dtype=pl.Float64) + pl.col("ll_mw") + u_p50).alias("total_p50_mw"))
print(f"A2E pace 2022-2025: {pace:.0f} MW/yr; A2E Jun 2026 {a2e_jun26:.0f} MW; U median {u_p50:.0f}")
print(pace_tab.with_columns(pl.selectors.float().round(0)))
a2e_implied = {v: {y: round(fc.filter((pl.col("layer") == "large_load") & (pl.col("year") == y))["p50_mw"].item()
                            / now["factor"]) for y in years_fc} for v, fc in (("pre_bz", fc_pre), ("latest", fc_bz))}
print("A2E at the summer implied by the LL P50 (MW):", a2e_implied)

# Official forecasts for the same years
off_rows = official.filter(pl.col("target_year").is_in(years_fc) & (
    pl.col("vintage").is_in(["LTLF 2025", "CDR Dec 2025", "CDR May 2025 Revised"]) | (pl.col("product") == bt.PRELIM)))
raw = bt.load_official_forecasts()
tsp = (raw.filter((pl.col("vintage") == "LTLF 2025") & (pl.col("scenario") == "tsp_provided")
                  & (pl.col("season") == "summer") & (pl.col("region_type") == "ercot") & (pl.col("metric") == "peak_mw")
                  & pl.col("target_year").is_in(years_fc))
       .select(pl.lit("LTLF 2025 TSP-provided").alias("vintage"), "target_year", pl.col("value").alias("forecast_mw")))
prelim = raw.filter((pl.col("metric") == "peak_demand") & pl.col("target_year").is_in(years_fc)
                    & pl.col("season").is_null()).select(
    pl.lit("2026 preliminary LTLF (season not stated)").alias("vintage"), "target_year",
    pl.col("value").alias("forecast_mw"))
off_cmp = pl.concat([off_rows.select("vintage", "target_year", "forecast_mw"), tsp, prelim]).unique(
    ["vintage", "target_year"]).pivot(on="target_year", index="vintage", values="forecast_mw", sort_columns=True)
print(off_cmp.with_columns(pl.selectors.float().round(0)))
off_cmp.write_csv(out_path("x7_official_2027_2031.csv"))

# %% 6. Zones: coincident contribution to the ERCOT peak (additive), organic per zone + share of LL + U
coinc = px.coincident_zone_loads(load)
ercot_x = ercot.select("year", FEATURE)
cpanel = coinc.join(ercot_x, on="year").filter(pl.col("year") >= FIRST)
zone_fits = {z: pf.fit_organic(cpanel.filter(pl.col("weather_zone") == z).rename({"coincident_mw": "peak_mw"}),
                               FEATURE) for z in wl.WEATHER_ZONES}
zone_excess = []
for z, zf in zone_fits.items():
    sub = cpanel.filter((pl.col("weather_zone") == z) & pl.col("year").is_between(2023, LAST_ACTUAL))
    for r in sub.iter_rows(named=True):
        zone_excess.append({"zone": z, "year": r["year"], "excess": r["coincident_mw"] - zf.mean(r["year"], r[FEATURE])})
shares = (pl.DataFrame(zone_excess).group_by("zone").agg(pl.col("excess").mean())
          .with_columns(pl.col("excess").clip(0, None).alias("pos"))
          .with_columns((pl.col("pos") / pl.col("pos").sum()).alias("share")).sort("zone"))
print(shares.with_columns(pl.selectors.float().round(3)))
share = dict(zip(shares["zone"].to_list(), shares["share"].to_list(), strict=True))
zrows = []
extra = draws_pre["large_load"] + draws_pre["unattributed"]
for z, zf in zone_fits.items():
    org = pf.organic_draws(zf, years_fc, N, np.random.default_rng(sum(map(ord, z))))
    tot = org + share[z] * extra
    for j, y in enumerate(years_fc):
        q_o = np.quantile(org[:, j], [0.1, 0.5, 0.9])
        q_t = np.quantile(tot[:, j], [0.1, 0.5, 0.9])
        zrows.append({"zone": z, "year": y, "share_of_ll_u": share[z], "organic_p50_mw": q_o[1],
                      "p10_mw": q_t[0], "p50_mw": q_t[1], "p90_mw": q_t[2]})
zfc = pl.DataFrame(zrows)
print(zfc.filter(pl.col("year").is_in([2027, 2031])).with_columns(pl.selectors.float().round(0)))
print("sum of zone P50 vs ERCOT P50 (pre-BZ):", zfc.group_by("year").agg(pl.col("p50_mw").sum()).sort("year"),
      fc_pre.filter(pl.col("layer") == "total").select("year", "p50_mw"))
zfc.write_csv(out_path("x7_forecast_zones.csv"))

# %% 7. Pseudo-out-of-sample backtest: rebuild everything as of the day an official vintage was out
# Each as-of date sits just after an official publication, so both sides saw the same calendar. Targets start at
# the first summer after the as-of date (Q1's horizon 1) and end at 2026 (preliminary actual).
bt_rows, off_bt, inputs_log = [], [], []
pts_now = pf.a2e_points(cv, vintages, bars, TODAY)
pts_now_l = list(zip(pts_now["month_idx"].to_list(), pts_now["a2e_mw"].to_list(), strict=True))


def realized_ll(year: int) -> float:
    """What the LL layer should have been: today's factor x today's reading of A2E at that summer's peak month."""
    return now["factor"] * pf.interpolate(pts_now_l, pf.month_end_index(peak_months[year]))


def last_summer_of(as_of: date) -> int:
    return as_of.year if as_of.month >= 9 else as_of.year - 1


def row(as_of, name, r, summ=None):
    out = {"as_of": as_of, "variant": name, "target_year": r["year"], "horizon": bt.summers_ahead(as_of, r["year"]),
           "p10_mw": r["p10_mw"], "p50_mw": r["p50_mw"], "p90_mw": r["p90_mw"], "actual_mw": r["actual_mw"],
           "error_pct": r["error_pct"], "in_p10_p90": r["in_p10_p90"]}
    if summ is not None:
        layers = summ.filter(pl.col("year") == r["year"])
        p50 = dict(zip(layers["layer"].to_list(), layers["p50_mw"].to_list(), strict=True))
        out |= {"organic_p50": p50["organic"], "ll_p50": p50["large_load"], "u_p50": p50["unattributed"],
                "ll_realized": realized_ll(r["year"]),
                "u_realized": r["actual_mw"] - p50["organic"] - realized_ll(r["year"])}
    return out


for as_of in AS_OF_DATES:
    years = [y for y in range(last_summer_of(as_of) + 1, LAST_ACTUAL + 1) if bt.summers_ahead(as_of, y) >= 1]
    if not years:
        continue
    variants = {"method": inputs_asof(as_of)}
    if as_of > BATCH_ZERO:
        variants["method_pre_batch_zero_deck"] = inputs_asof(as_of, vintage_before=BATCH_ZERO)
    for name, inp in variants.items():
        _, summ, prom = run(inp, years)
        for r in pf.score(summ.filter(pl.col("layer") == "total"), {y: actual[y] for y in years}).iter_rows(named=True):
            bt_rows.append(row(as_of, name, r, summ))
        inputs_log.append({"as_of": as_of, "variant": name, "deck": inp["vintage"], "base_a2e_mw": inp["base"],
                           "factor": round(inp["factor"], 3), "factor_years": inp["factor_rows"]["year"].to_list(),
                           "n_ratios": inp["ratios"].height,
                           "ratio_p10_p50_p90": np.quantile(inp["ratios"]["ratio"].to_numpy(), [.1, .5, .9]).round(3).tolist(),
                           "ratio_targets": sorted(set(inp["ratios"]["target_year"].to_list())),
                           "u_values": [round(v) for v in inp["u"]["u_mw"].to_list()],
                           "u_years": inp["u"]["year"].to_list(), "leaks": "; ".join(inp["leaks"]) or "-",
                           "promised": str({y: round(prom[y]) for y in years if y in prom})})
    # baselines: organic only (pre-break), and no hindsight on the break (organic fit on every summer known at the
    # as-of date) + LL, without U (U is defined against the pre-break model)
    base_in = variants["method"]
    _, s_org, _ = run(base_in, years, with_ll=False, with_u=False)
    _, s_nh, _ = run(base_in, years, fit_=pf.fit_organic(ercot, FEATURE, until=last_summer_of(as_of)), with_u=False)
    for name, s in {"organic_only": s_org, "no_hindsight_full_trend_plus_ll": s_nh}.items():
        for r in pf.score(s.filter(pl.col("layer") == "total"), {y: actual[y] for y in years}).iter_rows(named=True):
            bt_rows.append(row(as_of, name, r))
    for r in pf.official_asof(official, as_of).filter(pl.col("target_year").is_in(years)).iter_rows(named=True):
        off_bt.append({"as_of": as_of, "product": r["product"], "vintage": r["vintage"],
                       "vintage_date": r["vintage_date"], "target_year": r["target_year"],
                       "horizon": bt.summers_ahead(r["vintage_date"], r["target_year"]),
                       "forecast_mw": r["forecast_mw"], "actual_mw": actual[r["target_year"]],
                       "error_pct": 100 * (r["forecast_mw"] / actual[r["target_year"]] - 1)})

bt_tab = pl.DataFrame(bt_rows)
off_tab = pl.DataFrame(off_bt)
log = pl.DataFrame(inputs_log)
pl.Config.set_fmt_table_cell_list_len(8)
print(log)
m = bt_tab.filter(pl.col("variant").str.starts_with("method"))
print(m.select("as_of", "variant", "target_year", "horizon", "p10_mw", "p50_mw", "p90_mw", "actual_mw", "error_pct",
               "in_p10_p90", "organic_p50", "ll_p50", "ll_realized", "u_p50", "u_realized")
      .with_columns(pl.selectors.float().round(1)))
print(off_tab.with_columns(pl.selectors.float().round(1)))
bt_tab.write_csv(out_path("x7_backtest.csv"))
off_tab.write_csv(out_path("x7_backtest_official.csv"))
log.with_columns(pl.col(c).cast(pl.List(pl.String)).list.join(", ") for c in (
    "factor_years", "ratio_p10_p50_p90", "ratio_targets", "u_values", "u_years")).write_csv(out_path("x7_backtest_inputs.csv"))

# Paired table: one row per (as_of, target year) with our P50/band and each official product's vintage of that date
ours = bt_tab.filter(pl.col("variant") == "method").select("as_of", "target_year", "horizon", "actual_mw",
                                                            "p10_mw", "p50_mw", "p90_mw", "error_pct", "in_p10_p90")
off_w = off_tab.pivot(on="product", index=["as_of", "target_year"], values="error_pct")
off_v = off_tab.group_by("as_of", "product").agg(pl.col("vintage").first()).pivot(on="product", index="as_of",
                                                                                    values="vintage")
paired = ours.join(off_w, on=["as_of", "target_year"], how="left").sort("as_of", "target_year")
print(paired.with_columns(pl.selectors.float().round(1)))
print(off_v)
paired.write_csv(out_path("x7_backtest_paired.csv"))


def paired_mape(col: str) -> tuple[int, float, float, float]:
    sub = paired.drop_nulls(col)
    return sub.height, sub[col].abs().mean(), sub["error_pct"].abs().mean(), sub[col].mean()


for col in ("LTLF", "CDR", bt.PRELIM):
    if col in paired.columns:
        n, off_mape, our_mape, off_bias = paired_mape(col)
        print(f"same cells as {col}: n={n}, {col} MAPE {off_mape:.2f}% (bias {off_bias:+.2f}%), "
              f"basecast MAPE {our_mape:.2f}%")
summary = (bt_tab.group_by("variant").agg(pl.len().alias("n"), pl.col("error_pct").abs().mean().alias("mape"),
                                          pl.col("error_pct").mean().alias("bias"),
                                          pl.col("in_p10_p90").mean().alias("coverage")).sort("mape"))
print(summary.with_columns(pl.selectors.float().round(2)))
by_h = (bt_tab.filter(pl.col("variant") == "method").group_by("horizon")
        .agg(pl.len().alias("n"), pl.col("error_pct").abs().mean().alias("mape"), pl.col("error_pct").mean().alias("bias"),
             pl.col("in_p10_p90").mean().alias("coverage")).sort("horizon"))
print(by_h.with_columns(pl.selectors.float().round(2)))

# %% 8. Figures
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e5e4df"
OURS, OFF1, OFF2, OFF3 = "#2a78d6", "#eb6834", "#1f9e6e", "#8a5cc2"


def style(ax):
    ax.grid(axis="y", color=GRID, lw=0.8)
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(colors=MUTED, labelsize=8)


fig, ax = plt.subplots(figsize=(10, 5.2))
hist = actual_tbl.filter(pl.col("year").is_between(2015, LAST_ACTUAL))
ax.plot(hist["year"], hist["actual_mw"] / 1000, color=INK, lw=2, marker="o", ms=4, label="Actual summer peak")
t = fc_pre.filter(pl.col("layer") == "total").sort("year")
ax.fill_between(t["year"], t["p10_mw"] / 1000, t["p90_mw"] / 1000, color=OURS, alpha=0.18, lw=0,
                label="basecast P10–P90 (Mar 2026 deck)")
ax.plot(t["year"], t["p50_mw"] / 1000, color=OURS, lw=2, marker="o", ms=4, label="basecast P50 (Mar 2026 deck)")
t2 = fc_bz.filter(pl.col("layer") == "total").sort("year")
ax.plot(t2["year"], t2["p50_mw"] / 1000, color=OURS, lw=2, ls="--", label="basecast P50 (Jun 2026 deck, post Batch Zero)")
for label, color, ls in (("LTLF 2025", OFF1, "-"), ("CDR Dec 2025", OFF3, "-"), ("LTLF 2025 TSP-provided", OFF1, ":")):
    if label in off_cmp["vintage"].to_list():
        row = off_cmp.filter(pl.col("vintage") == label).row(0, named=True)
        ys = [y for y in years_fc if row.get(str(y)) is not None]
        ax.plot(ys, [row[str(y)] / 1000 for y in ys], color=color, lw=2, ls=ls, label=f"ERCOT {label}")
ax.scatter([2026], [112], color=OFF1, marker="*", s=120, zorder=5, label="ERCOT 2026 preliminary (112 GW)")
ax.set_ylabel("GW", color=MUTED)
ax.set_ylim(60, 225)
ax.set_title("ERCOT summer peak: basecast prototype vs ERCOT's official forecasts (large loads machine-read, not verified)",
             loc="left", fontsize=10, color=INK)
ax.legend(fontsize=7.5, frameon=False, loc="upper left")
style(ax)
fig.tight_layout()
fig.savefig(out_path("x7_forecast_vs_official.png"), dpi=140)

fig, axes = plt.subplots(1, len(AS_OF_DATES), figsize=(20, 4.4), sharey=True)
for ax, as_of in zip(axes, AS_OF_DATES, strict=True):
    m = bt_tab.filter((pl.col("as_of") == as_of) & (pl.col("variant") == "method")).sort("target_year")
    ax.fill_between(m["target_year"], m["p10_mw"] / 1000, m["p90_mw"] / 1000, color=OURS, alpha=0.18, lw=0)
    ax.errorbar(m["target_year"], m["p50_mw"] / 1000,
                yerr=[(m["p50_mw"] - m["p10_mw"]) / 1000, (m["p90_mw"] - m["p50_mw"]) / 1000],
                color=OURS, lw=2, marker="o", ms=4, capsize=3, label="basecast P50 / P10–P90")
    ax.plot(m["target_year"], m["actual_mw"] / 1000, color=INK, lw=2, marker="o", ms=4, label="Actual")
    o = off_tab.filter(pl.col("as_of") == as_of)
    for (vint,), g in o.group_by(["vintage"], maintain_order=True):
        g = g.sort("target_year")
        color = OFF1 if g["product"][0] == "LTLF" else OFF3 if g["product"][0] == "CDR" else OFF2
        ax.plot(g["target_year"], g["forecast_mw"] / 1000, color=color, lw=1.6, marker="s", ms=4, label=vint)
    ax.set_title(f"as of {as_of}", loc="left", fontsize=10, color=INK)
    ax.set_xticks(m["target_year"].to_list())
    ax.set_xlim(m["target_year"].min() - 0.5, m["target_year"].max() + 0.5)
    ax.legend(fontsize=7, frameon=False, loc="upper left")
    style(ax)
axes[0].set_ylabel("GW", color=MUTED)
fig.suptitle("Pseudo-out-of-sample backtest: forecasts rebuilt with data published by each date vs ERCOT's vintages "
             "of the same date", x=0.01, ha="left", fontsize=10, color=INK)
fig.tight_layout()
fig.savefig(out_path("x7_backtest.png"), dpi=140)
print("figures and tables written to analysis/out/x7_*")
