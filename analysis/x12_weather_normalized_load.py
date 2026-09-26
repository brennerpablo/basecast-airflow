"""X12 — a weather-normalized daily load series by weather zone (Forecast module).

Run from the repo root: ``uv run --group analysis python analysis/x12_weather_normalized_load.py``.
Reads ``ercot_load_hourly_wz`` and ``weather_hourly_wz`` (read-only, through ``weather_load``'s loaders); the logic
lives in ``basecast_pipelines/models/weather_normalized.py``. Numbers go to
``docs/analysis/x12_weather_normalized_load.md``; tables and figures to ``analysis/out/x12_*``.
"""

# %% Setup
from __future__ import annotations

import time
from dataclasses import replace

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from statsmodels.stats.diagnostic import acorr_ljungbox

from analysis._common import out_path
from basecast_pipelines.models import weather_load as wl
from basecast_pipelines.models import weather_normalized as wn

T0 = time.time()
SERIES = (*wl.WEATHER_ZONES, wl.TOTAL)
SUMMER = wl.SUMMER_MONTHS
SPEC = wn.Spec()  # replaced by the variant chosen in cell 1

load = wl.load_hourly_load()
weather = wl.load_hourly_weather()
panel, feats = wn.build_panel(load, weather)
normals = wn.normal_weather(feats, wn.NORMAL_YEARS)
print(panel.shape, panel["date"].min(), panel["date"].max(), f"{time.time() - T0:.1f}s")
print(panel.group_by("day_type").agg(pl.len()).sort("day_type"))


def mean_mape(frames: list[pl.DataFrame]) -> dict[str, float]:
    m = wn.holdout_mape(pl.concat(frames))
    return {c: float(m[c].mean()) for c in ("mape_flat", "mape_relevel", "mape_noweather")}


# %% 1. Variant selection on a validation year (train 2020–2022, test 2023), before touching 2024–2025
VARIANTS = {
    "t spline": wn.Spec(lag=False, humid=False),
    "+ lag": wn.Spec(lag=True, humid=False),
    "+ lag + dew point": wn.Spec(lag=True, humid=True),
    "+ lag + dew point + tmax": wn.Spec(lag=True, humid=True, tmax=True),
    "+ lag + dew point, level knots 6 mo": wn.Spec(level_knots_per_year=2),
    "+ lag + dew point, fourier 3": wn.Spec(fourier=3),
}
rows = []
for name, spec in VARIANTS.items():
    for target in ("dmean", "dmax"):
        errs = [wn.holdout(panel, z, target, spec, train=(2020, 2022), test=(2023,)) for z in SERIES]
        rows.append({"variant": name, "target": target, **mean_mape(errs)})
variants = pl.DataFrame(rows)
print(variants.pivot(on="target", index="variant", values="mape_relevel"))
chosen = {}
for target in ("dmean", "dmax"):
    best = variants.filter(pl.col("target") == target).sort("mape_relevel").row(0, named=True)
    chosen[target] = VARIANTS[best["variant"]]
    print(target, "->", best["variant"])
variants.write_csv(out_path("x12_variants.csv"))

# %% 2. Hold-out: train 2021–2023, predict every day of 2024–2025 with actual weather
hold = {}
for target in ("dmean", "dmax"):
    hold[target] = pl.concat(
        [wn.holdout(panel, z, target, chosen[target], train=(2021, 2023), test=(2024, 2025)) for z in SERIES]
    )
ho_table = (
    wn.holdout_mape(hold["dmean"]).rename(lambda c: c if c == "weather_zone" else f"e_{c}")
    .join(wn.holdout_mape(hold["dmax"]).rename(lambda c: c if c == "weather_zone" else f"p_{c}"), on="weather_zone")
)
print(ho_table.with_columns(pl.exclude("weather_zone").round(2)))
ho_table.write_csv(out_path("x12_holdout_mape.csv"))
pl.concat([hold["dmean"].with_columns(pl.lit("dmean").alias("target")),
           hold["dmax"].with_columns(pl.lit("dmax").alias("target"))]).write_csv(out_path("x12_holdout_days.csv"))

# Worst hold-out days (re-leveled), to see what the model misses.
worst = (hold["dmean"].with_columns((100 * (pl.col("pred_relevel") / pl.col("actual") - 1)).alias("err_pct"))
         .sort(pl.col("err_pct").abs(), descending=True).head(12))
print(worst)

# Hold-out MAPE by year for the flat-level forecast.
print(hold["dmean"].with_columns((100 * (pl.col("pred_flat") - pl.col("actual")).abs() / pl.col("actual")).alias("ape"))
      .group_by("weather_zone", pl.col("date").dt.year().alias("year")).agg(pl.col("ape").mean().round(2))
      .pivot(on="year", index="weather_zone", values="ape"))

# %% 3. Normalize every year with its rolling three-year window; normal weather = 2003–2022
days, draws, fits = {}, {}, {}
for target in ("dmean", "dmax"):
    d_parts, w_parts = [], []
    for z in SERIES:
        d, w, f = wn.normalize_zone(panel, z, target, normals, chosen[target],
                                    draw_months=SUMMER if target == "dmax" else None)
        d_parts.append(d)
        w_parts.append(w)
        fits[(target, z)] = f
    days[target] = pl.concat(d_parts)
    draws[target] = pl.concat([w for w in w_parts if w.height]) if target == "dmax" else pl.DataFrame()
print({k: v.shape for k, v in days.items()}, f"{time.time() - T0:.1f}s")

trim = pl.DataFrame([{"target": t, "weather_zone": z, "window": f"{k[0]}-{k[1]}", "n": fit.n, "trimmed": fit.n_trimmed,
                      "r2": fit.r2, "sigma": fit.sigma}
                     for (t, z), fs in fits.items() for k, fit in fs.items()])
print(trim.group_by("target", "weather_zone").agg(pl.col("r2").min().alias("r2_min"), pl.col("r2").median().alias("r2_med"),
                                                  pl.col("trimmed").sum(), pl.col("n").sum()).sort("target", "weather_zone"))
trimmed_days = sorted({d for fs in fits.values() for f in fs.values() for d in f.trimmed_dates})
print("trimmed dates (any zone/target):", len(trimmed_days), trimmed_days[:40])

# %% 4. Residual autocorrelation (in-sample rolling fits and hold-out)
rows = []
for target in ("dmean", "dmax"):
    for z in SERIES:
        r = (days[target].filter((pl.col("weather_zone") == z) & ~pl.col("excluded"))
             .sort("date").select((pl.col(target) - pl.col("fitted")).alias("r"))["r"].to_numpy())
        a = wn.acf(r, (1, 2, 7, 365))
        lb = acorr_ljungbox(r, lags=[14], return_df=True)
        h = (hold[target].filter(pl.col("weather_zone") == z).sort("date")
             .select((pl.col("actual") - pl.col("pred_relevel")).alias("r"))["r"].to_numpy())
        rows.append({"target": target, "weather_zone": z, "acf1": a[1], "acf2": a[2], "acf7": a[7], "acf365": a[365],
                     "lb14": float(lb["lb_stat"].iloc[0]), "lb14_p": float(lb["lb_pvalue"].iloc[0]),
                     "holdout_acf1": wn.acf(h, (1,))[1], "dw": float(np.sum(np.diff(r) ** 2) / np.sum(r ** 2))})
autocorr = pl.DataFrame(rows)
print(autocorr.with_columns(pl.exclude("target", "weather_zone").round(3)))
autocorr.write_csv(out_path("x12_residual_acf.csv"))

# %% 5. Stability of the temperature response: non-overlapping 3-year blocks
hist = feats.filter(pl.col("date").dt.year().is_between(*wn.NORMAL_YEARS))
t_hot = dict(hist.filter(pl.col("date").dt.month().is_in((7, 8))).group_by("weather_zone")
             .agg(pl.col("t_mean").median()).iter_rows())
t_cold = dict(hist.filter(pl.col("date").dt.month().is_in((12, 1, 2))).group_by("weather_zone")
              .agg(pl.col("t_mean").quantile(0.1)).iter_rows())
print("hot (Jul–Aug median) °C:", {k: round(v, 1) for k, v in sorted(t_hot.items())})
print("cold (Dec–Feb P10) °C:", {k: round(v, 1) for k, v in sorted(t_cold.items())})
BLOCKS = [(y, y + 2) for y in range(2003, 2025, 3)]
rows = []
for z in SERIES:
    for start, end in BLOCKS:
        sub = panel.filter((pl.col("weather_zone") == z) & pl.col("date").dt.year().is_between(start, end))
        mean_summer = sub.filter(pl.col("date").dt.month().is_in((7, 8)))
        for target in ("dmean", "dmax"):
            fit = wn.fit_daily(panel, target, start, end, spec=chosen[target], zone=z)
            cs, cse = fit.slope(t_hot[z])
            hs, hse = fit.slope(t_cold[z])
            level = float(mean_summer[target].mean())
            rows.append({"weather_zone": z, "block": f"{start}-{end}", "target": target, "cool_mw_per_c": cs,
                         "cool_se": cse, "cool_pct_per_c": 100 * cs / level, "heat_mw_per_c": hs, "heat_se": hse,
                         "summer_mean_mw": level, "t_hot": t_hot[z], "t_cold": t_cold[z]})
slopes = pl.DataFrame(rows)
slopes.write_csv(out_path("x12_slopes.csv"))
for target, col in (("dmean", "cool_mw_per_c"), ("dmean", "cool_pct_per_c"), ("dmax", "cool_mw_per_c"),
                    ("dmean", "heat_mw_per_c")):
    print(target, col)
    print(slopes.filter(pl.col("target") == target).pivot(on="block", index="weather_zone", values=col)
          .with_columns(pl.exclude("weather_zone").round(2 if "pct" in col else 0)))
print("SE of dmean cooling slope, ERCOT:", slopes.filter((pl.col("weather_zone") == "ERCOT") & (pl.col("target") == "dmean"))
      .select("block", pl.col("cool_se").round(0)).to_dicts())

# %% 6. Monthly and annual normalized series; growth raw vs normalized
month = wn.monthly(days["dmean"], days["dmax"])
year = wn.annual(month)
month.write_csv(out_path("x12_monthly.csv"))
year.write_csv(out_path("x12_annual.csv"))
days["dmean"].select("weather_zone", "date", "n_hours", "t_mean", "dmean", "fitted", "w_actual", "w_normal",
                     "normalized", "window", "excluded").join(
    days["dmax"].select("weather_zone", "date", pl.col("dmax"), pl.col("normalized").alias("dmax_normalized")),
    on=["weather_zone", "date"]).write_parquet(out_path("x12_daily.parquet"))

full = year.filter(pl.col("months") == 12)
wide_e = full.pivot(on="weather_zone", index="year", values="energy_gwh").sort("year")
wide_n = full.pivot(on="weather_zone", index="year", values="energy_norm_gwh").sort("year")
adj_pct = full.with_columns((100 * (pl.col("energy_norm_gwh") / pl.col("energy_gwh") - 1)).alias("adj")).pivot(
    on="weather_zone", index="year", values="adj").sort("year")
print("normalized annual energy, TWh"); print(wide_n.with_columns(pl.exclude("year").truediv(1000).round(1)).select("year", *SERIES))
print("weather adjustment, % of actual"); print(adj_pct.with_columns(pl.exclude("year").round(2)).select("year", *SERIES))

growth = []
for z in SERIES:
    r = full.filter(pl.col("weather_zone") == z)
    get = lambda y, c: r.filter(pl.col("year") == y)[c].item()  # noqa: E731
    growth.append({"weather_zone": z,
                   "raw_2019_twh": get(2019, "energy_gwh") / 1000, "raw_2025_twh": get(2025, "energy_gwh") / 1000,
                   "norm_2019_twh": get(2019, "energy_norm_gwh") / 1000, "norm_2025_twh": get(2025, "energy_norm_gwh") / 1000,
                   "raw_cagr_19_25": 100 * wn.cagr(get(2019, "energy_gwh"), get(2025, "energy_gwh"), 6),
                   "norm_cagr_19_25": 100 * wn.cagr(get(2019, "energy_norm_gwh"), get(2025, "energy_norm_gwh"), 6),
                   "raw_cagr_10_19": 100 * wn.cagr(get(2010, "energy_gwh"), get(2019, "energy_gwh"), 9),
                   "norm_cagr_10_19": 100 * wn.cagr(get(2010, "energy_norm_gwh"), get(2019, "energy_norm_gwh"), 9),
                   "norm_cagr_22_25": 100 * wn.cagr(get(2022, "energy_norm_gwh"), get(2025, "energy_norm_gwh"), 3),
                   "raw_cagr_22_25": 100 * wn.cagr(get(2022, "energy_gwh"), get(2025, "energy_gwh"), 3)})
growth = pl.DataFrame(growth)
print(growth.with_columns(pl.exclude("weather_zone").round(2)))
growth.write_csv(out_path("x12_growth.csv"))

# 2026 so far (Jan 1 – Sep 19) vs the same days of 2025.
ytd = (days["dmean"].filter(pl.col("date").dt.year().is_in((2025, 2026))
                            & (pl.col("date").dt.ordinal_day() <= pl.date(2026, 9, 19).dt.ordinal_day()))
       .group_by("weather_zone", pl.col("date").dt.year().alias("year"))
       .agg((pl.col("dmean") * pl.col("n_hours")).sum().alias("e"), (pl.col("normalized") * pl.col("n_hours")).sum().alias("n")))
ytd = ytd.pivot(on="year", index="weather_zone", values=["e", "n"]).with_columns(
    (100 * (pl.col("e_2026") / pl.col("e_2025") - 1)).alias("raw_yoy"),
    (100 * (pl.col("n_2026") / pl.col("n_2025") - 1)).alias("norm_yoy")).select("weather_zone", "raw_yoy", "norm_yoy")
print("2026 YTD (through Sep 19) vs 2025 same days, %"); print(ytd.with_columns(pl.exclude("weather_zone").round(2)).sort("weather_zone"))

# Does normalization remove weather noise? SD of year-over-year change, 2004–2021 (before the step).
noise = (full.sort("weather_zone", "year").with_columns(
    (100 * pl.col("energy_gwh").pct_change()).over("weather_zone").alias("yoy_raw"),
    (100 * pl.col("energy_norm_gwh").pct_change()).over("weather_zone").alias("yoy_norm"))
    .filter(pl.col("year").is_between(2004, 2021)).group_by("weather_zone")
    .agg(pl.col("yoy_raw").std().alias("sd_yoy_raw"), pl.col("yoy_norm").std().alias("sd_yoy_norm")).sort("weather_zone"))
print(noise.with_columns(pl.exclude("weather_zone").round(2)))

# ERCOT fit directly on system-weighted weather vs the sum of the zones' normalized energy.
zsum = full.filter(pl.col("weather_zone") != wl.TOTAL).group_by("year").agg(pl.col("energy_norm_gwh").sum().alias("zones"))
cmp_total = full.filter(pl.col("weather_zone") == wl.TOTAL).join(zsum, on="year").with_columns(
    (100 * (pl.col("zones") / pl.col("energy_norm_gwh") - 1)).alias("zones_vs_direct_pct")).sort("year")
print(cmp_total.select("year", "energy_gwh", "energy_norm_gwh", "zones", pl.col("zones_vs_direct_pct").round(2)))

# Sensitivity: normal = 2006–2025 (the last 20 full years) instead of 2003–2022.
normals_late = wn.normal_weather(feats, (2006, 2025))
sens = []
for z in SERIES:
    for y in (2019, 2025):
        win = wn.window_for(y, 2003, 2026)
        fit = fits[("dmean", z)][win]
        frame = panel.filter((pl.col("weather_zone") == z) & (pl.col("date").dt.year() == y))
        a = wn.normalize_year(fit, frame, normals)
        b = wn.normalize_year(fit, frame, normals_late)
        sens.append({"weather_zone": z, "year": y, "norm_0322": float((a["normalized"] * a["n_hours"]).sum()),
                     "norm_0625": float((b["normalized"] * b["n_hours"]).sum())})
sens = pl.DataFrame(sens).with_columns((100 * (pl.col("norm_0625") / pl.col("norm_0322") - 1)).alias("diff_pct"))
print(sens.pivot(on="year", index="weather_zone", values="diff_pct").with_columns(pl.exclude("weather_zone").round(2)))

# %% 7. The summer peak under normal weather vs actual and Q7
season = wn.normal_season_peak(days["dmax"].filter(pl.col("date").dt.month().is_in(SUMMER)), draws["dmax"])
pq = wn.peak_quantiles(season)
actual_peak = (days["dmax"].filter(pl.col("date").dt.month().is_in(SUMMER))
               .group_by("weather_zone", pl.col("date").dt.year().alias("year")).agg(pl.col("dmax").max().alias("actual")))
# Q7's model: peak ~ year + t_mean_3d, fit 2003–2025, at the year's actual weather and at the 2003–2025 median weather.
wdaily = wl.weather_daily(weather)
q7_w = wl.zone_weights(load, range(2003, 2023))
q7_feat = wl.summer_weather_features(pl.concat([wdaily.drop("n_hours"), wl.system_weather_daily(wdaily, q7_w).drop("n_hours")]))
q7_panel = wl.summer_peaks(load).select("weather_zone", "year", "peak_mw").join(q7_feat, on=["weather_zone", "year"])
q7_rows = []
for z in SERIES:
    sub = q7_panel.filter(pl.col("weather_zone") == z).sort("year")
    fit = wl.fit_peak(sub.filter(pl.col("year").is_between(2003, 2025)), "t_mean_3d", zone=z)
    med = float(sub.filter(pl.col("year").is_between(2003, 2025))["t_mean_3d"].median())
    yrs = sub["year"].to_list()
    q7_rows += [{"weather_zone": z, "year": y, "q7_actual_weather": a, "q7_p50": b}
                for y, a, b in zip(yrs, fit.predict(yrs, sub["t_mean_3d"].to_list()).tolist(),
                                   fit.predict(yrs, [med] * len(yrs)).tolist(), strict=True)]
peak_cmp = (pq.join(actual_peak, on=["weather_zone", "year"]).join(pl.DataFrame(q7_rows), on=["weather_zone", "year"], how="left")
            .with_columns((100 * (pl.col("p50") / pl.col("actual") - 1)).alias("norm_vs_actual_pct")).sort("weather_zone", "year"))
peak_cmp.write_csv(out_path("x12_summer_peak.csv"))
print(peak_cmp.filter(pl.col("weather_zone") == wl.TOTAL).select(
    "year", "actual", "p10", "p50", "p90", "norm_vs_actual_pct", "q7_actual_weather", "q7_p50")
      .with_columns(pl.exclude("year", "norm_vs_actual_pct").round(0), pl.col("norm_vs_actual_pct").round(2)))
print(peak_cmp.filter(pl.col("year").is_in((2019, 2022, 2023, 2024, 2025, 2026))).select(
    "weather_zone", "year", pl.col("actual").round(0), pl.col("p50").round(0), pl.col("norm_vs_actual_pct").round(1),
    pl.col("q7_p50").round(0)).pivot(on="year", index="weather_zone", values=["p50"]))
peak_growth = peak_cmp.filter(pl.col("year").is_in((2019, 2025))).pivot(on="year", index="weather_zone", values=["actual", "p50"]).with_columns(
    (100 * ((pl.col("actual_2025") / pl.col("actual_2019")) ** (1 / 6) - 1)).alias("raw_peak_cagr"),
    (100 * ((pl.col("p50_2025") / pl.col("p50_2019")) ** (1 / 6) - 1)).alias("norm_peak_cagr"))
print(peak_growth.with_columns(pl.exclude("weather_zone").round(1)))

# %% 8. Does the normalized series show the ~2022 step (X1)? Annual YoY z-scores against 2010–2021
yoy = (full.sort("weather_zone", "year").with_columns(
    (100 * pl.col("energy_norm_gwh").pct_change()).over("weather_zone").alias("yoy_norm"),
    (100 * pl.col("energy_gwh").pct_change()).over("weather_zone").alias("yoy_raw")))
ref = yoy.filter(pl.col("year").is_between(2010, 2021)).group_by("weather_zone").agg(
    pl.col("yoy_norm").mean().alias("mu"), pl.col("yoy_norm").std().alias("sd"))
yoy = yoy.join(ref, on="weather_zone").with_columns(((pl.col("yoy_norm") - pl.col("mu")) / pl.col("sd")).alias("z"))
print(yoy.filter(pl.col("year") >= 2019).pivot(on="weather_zone", index="year", values="yoy_norm").select("year", *SERIES)
      .with_columns(pl.exclude("year").round(1)))
print(yoy.filter(pl.col("year") >= 2019).pivot(on="weather_zone", index="year", values="z").select("year", *SERIES)
      .with_columns(pl.exclude("year").round(1)))
print(ref.with_columns(pl.col("mu", "sd").round(2)).sort("weather_zone"))

# Monthly: 12-month YoY of normalized average load, from mid-2020.
# On average MW, so the partial last month (Sep 1–19, 2026) compares with the full month a year before.
m = month.sort("weather_zone", "month").with_columns(
    (100 * (pl.col("avg_norm_mw") / pl.col("avg_norm_mw").shift(12) - 1)).over("weather_zone").alias("yoy"),
    (100 * (pl.col("avg_mw") / pl.col("avg_mw").shift(12) - 1)).over("weather_zone").alias("yoy_raw"))
for z in ("NORTH", "FWEST", "WEST", "SCENT", "COAST"):
    s = m.filter((pl.col("weather_zone") == z) & (pl.col("month") >= pl.date(2020, 7, 1)))
    print(z, [(r["month"].strftime("%Y-%m"), round(r["yoy"], 1), round(r["yoy_raw"], 1)) for r in s.iter_rows(named=True)])

# %% 9. Figures
GRAY, BLUE = "#b4b2a9", "#2a78d6"
fig, axes = plt.subplots(3, 3, figsize=(13, 9), sharex=True)
roll = month.sort("weather_zone", "month").with_columns(
    (pl.col("energy_gwh").rolling_sum(12) / pl.col("days").rolling_sum(12) / 24).over("weather_zone").alias("act_gw"),
    (pl.col("energy_norm_gwh").rolling_sum(12) / pl.col("days").rolling_sum(12) / 24).over("weather_zone").alias("norm_gw"))
for ax, z in zip(axes.flat, SERIES, strict=True):
    s = roll.filter((pl.col("weather_zone") == z) & pl.col("act_gw").is_not_null())
    ax.plot(s["month"].to_list(), s["act_gw"].to_list(), color=GRAY, lw=2, label="actual")
    ax.plot(s["month"].to_list(), s["norm_gw"].to_list(), color=BLUE, lw=2, label="normal weather (2003–2022)")
    ax.set_title(z, fontsize=10, loc="left")
    ax.grid(axis="y", color="#e1e0d9", lw=0.6)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
axes.flat[0].legend(frameon=False, fontsize=8)
fig.suptitle("Average load, trailing 12 months (GW): actual vs weather-normalized", x=0.01, ha="left")
fig.tight_layout()
fig.savefig(out_path("x12_normalized_by_zone.png"), dpi=130)

tot = peak_cmp.filter(pl.col("weather_zone") == wl.TOTAL).sort("year")
fig, ax = plt.subplots(figsize=(10, 4.5))
yrs = tot["year"].to_list()
ax.fill_between(yrs, (tot["p10"] / 1000).to_list(), (tot["p90"] / 1000).to_list(), color=BLUE, alpha=0.15, lw=0,
                label="normal weather P10–P90")
ax.plot(yrs, (tot["p50"] / 1000).to_list(), color=BLUE, lw=2, marker="o", ms=4, label="normal weather P50")
ax.plot(yrs, (tot["actual"] / 1000).to_list(), color="#52514c", lw=2, marker="o", ms=4, label="actual")
ax.plot(yrs, (tot["q7_p50"] / 1000).to_list(), color=GRAY, lw=2, ls="--", label="Q7 trend + median weather")
ax.set_ylabel("GW")
ax.set_title("ERCOT summer peak (daily max, Jun–Sep; 2026 through Sep 19)", loc="left")
ax.grid(axis="y", color="#e1e0d9", lw=0.6)
for side in ("top", "right"):
    ax.spines[side].set_visible(False)
ax.legend(frameon=False, fontsize=8)
fig.tight_layout()
fig.savefig(out_path("x12_ercot_summer_peak.png"), dpi=130)
print(f"done in {time.time() - T0:.1f}s")
