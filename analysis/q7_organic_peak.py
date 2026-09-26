"""Q7 — do weather and a linear trend explain the summer peak by weather zone? (``docs/PHASE0_ANALYSIS.md`` §2)

Run from the repo root: ``uv run --group analysis python analysis/q7_organic_peak.py``.
Reads ``ercot_load_hourly_wz``, ``weather_hourly_wz`` and ``ercot_monthly_peaks`` (read-only); the logic lives in
``basecast_pipelines/models/weather_load.py``. Numbers go to ``docs/analysis/q7_organic_peak.md``.
"""

# %% Setup
from __future__ import annotations

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from statsmodels.stats.diagnostic import breaks_cusumolsresid

from analysis._common import out_path
from basecast_pipelines.models import weather_load as wl

FIRST, LAST, TRAIN_UNTIL = 2003, 2025, 2022
TEST = (2023, 2024, 2025)
DEFAULT = "t_mean_3d"  # max over Jun–Sep of the 3-day mean of the zone's daily mean temperature
SERIES = (*wl.WEATHER_ZONES, wl.TOTAL)

load = wl.load_hourly_load()
weather = wl.load_hourly_weather()
official = wl.load_official_hourly_peaks()
print(load.shape, weather.shape, official.shape)

# %% 1. Time alignment
checks = wl.load_alignment_checks(load)
for key, value in checks.items():
    print(key, value)
print("weather zones:", sorted(weather["weather_zone"].unique().to_list()))
print("load zones:", sorted(load["weather_zone"].unique().to_list()))
print(weather.group_by("weather_zone").agg(
    pl.len(), pl.col("ts_utc").min().alias("first"), pl.col("ts_utc").max().alias("last"),
    pl.col("temperature_c").null_count().alias("t_nulls"), pl.col("dew_point_c").null_count().alias("td_nulls"),
    pl.col("n_points").min().alias("min_points")).sort("weather_zone"))
print(wl.diurnal_peak_hours(load.filter(pl.col("weather_zone") != wl.TOTAL), weather))

daily = wl.weather_daily(weather)
print("weather hours per Chicago day:", daily.group_by("n_hours").agg(pl.len()).sort("n_hours"))

# %% 2. Summer peak series (June–September), zones = non-coincident peaks, ERCOT = system peak
peaks = wl.summer_peaks(load)
print(peaks.filter(pl.col("n_hours") != wl.SUMMER_HOURS))
peak_wide = peaks.filter(pl.col("year").is_between(FIRST, 2026)).pivot(
    on="weather_zone", index="year", values="peak_mw").select("year", *SERIES).sort("year")
print(peak_wide.with_columns(pl.exclude("year").round(0)))

# Cross-check the ERCOT summer peak against ERCOT's published monthly hourly peaks (2008+).
off = (official.filter(pl.col("month").dt.month().is_in(wl.SUMMER_MONTHS))
       .group_by(pl.col("month").dt.year().alias("year")).agg(pl.col("peak_mw").max().alias("official_mw")))
xcheck = (peaks.filter(pl.col("weather_zone") == wl.TOTAL).join(off, on="year")
          .with_columns((100 * (pl.col("peak_mw") / pl.col("official_mw") - 1)).alias("diff_pct")).sort("year"))
print(xcheck.select("year", "peak_mw", "official_mw", "diff_pct", "peak_local"))

# Coincidence: system peak / sum of the zones' own peaks.
ncp_sum = peaks.filter(pl.col("weather_zone") != wl.TOTAL).group_by("year").agg(pl.col("peak_mw").sum().alias("ncp"))
coinc = (peaks.filter(pl.col("weather_zone") == wl.TOTAL).join(ncp_sum, on="year")
         .with_columns((pl.col("peak_mw") / pl.col("ncp")).alias("ratio")).filter(pl.col("year") <= LAST))
print("system / sum of zone peaks:", coinc.select(pl.col("ratio").mean().alias("mean"), pl.col("ratio").min().alias("min"),
                                                    pl.col("ratio").max().alias("max")))

# %% 3. Weather features: zones and a load-weighted ERCOT series (weights = summer energy share, train years)
weights = wl.zone_weights(load, range(FIRST, TRAIN_UNTIL + 1))
print({k: round(v, 3) for k, v in sorted(weights.items())})
daily_all = pl.concat([daily.drop("n_hours"), wl.system_weather_daily(daily, weights).drop("n_hours")])
features = wl.summer_weather_features(daily_all)
panel = (peaks.select("weather_zone", "year", "peak_mw", "n_hours")
         .join(features, on=["weather_zone", "year"], how="inner"))
hist = panel.filter(pl.col("year").is_between(FIRST, LAST))
print(hist.group_by("weather_zone").agg(pl.len(), pl.col("n_hours").min(), pl.col("weather_days").min()))

# %% 4. Fit peak ~ year + temperature per zone, 2003–2025
def fit_table(frame: pl.DataFrame, feature: str, **kw) -> pl.DataFrame:
    rows = []
    for zone in SERIES:
        sub = frame.filter(pl.col("weather_zone") == zone)
        fit = wl.fit_peak(sub, feature, zone=zone, **kw)
        mean_peak = sub["peak_mw"].mean()
        rows.append({
            "zone": zone, "n": fit.n, "mean_peak_mw": mean_peak,
            "trend_mw_yr": fit.coef_of("year"), "trend_se": fit.se[fit.names.index("year")],
            "temp_mw_per_c": fit.coef_of(feature), "temp_se": fit.se[fit.names.index(feature)],
            "r2": fit.r2, "resid_sd_mw": fit.resid_sd_mw, "resid_sd_pct": 100 * fit.resid_sd_mw / mean_peak,
        })
    return pl.DataFrame(rows)


fits = {f: fit_table(hist, f) for f in wl.FEATURES}
print(fits[DEFAULT].with_columns(pl.selectors.float().round(3)))
print(pl.DataFrame({"zone": SERIES, **{f: fits[f]["r2"].round(3) for f in wl.FEATURES}}))

# %% 5. Hold-out: fit ≤ 2022, predict 2023–2025 with the actual weather
def holdout_mape(feature: str, **kw) -> tuple[pl.DataFrame, pl.DataFrame]:
    errors = wl.holdout(hist, feature, train_until=TRAIN_UNTIL, test_years=TEST, **kw)
    return wl.mape(errors), errors


ho_mape, ho_errors = holdout_mape(DEFAULT)
print(ho_errors.with_columns(pl.selectors.float().round(1)))
print(ho_mape.with_columns(pl.selectors.float().round(2)))

variants = {"linear": {}, "log": {"log": True}, "piecewise": {"piecewise": True}}
compare = {}
for f in wl.FEATURES:
    for name, kw in variants.items():
        compare[f"{f}/{name}"] = holdout_mape(f, **kw)[0]["mape_pct"].round(2)
variant_table = pl.DataFrame({"zone": ho_mape["weather_zone"], **compare})
print(variant_table)

# Knots the piecewise variant picks on 2003–2022, and how collinear warming is with the trend.
print(pl.DataFrame([{
    "zone": z,
    "knot_train": wl.best_knot(hist.filter((pl.col("weather_zone") == z) & (pl.col("year") <= TRAIN_UNTIL)), DEFAULT),
    "corr_year_temp": hist.filter(pl.col("weather_zone") == z).select(pl.corr("year", DEFAULT)).item(),
    "temp_sd_c": hist.filter(pl.col("weather_zone") == z)[DEFAULT].std(),
} for z in SERIES]).with_columns(pl.selectors.float().round(2)))

# Trend only (a zero weather column, so lstsq drops it), for scale: how much does weather add?
trend_only = wl.mape(wl.holdout(hist.with_columns(pl.lit(0.0).alias("none")), "none",
                                train_until=TRAIN_UNTIL, test_years=TEST))
print("trend-only hold-out:", trend_only.with_columns(pl.selectors.float().round(2)))

# %% 6. One-year-ahead rolling origin (robustness of the 3-year hold-out)
ro = wl.rolling_origin(hist, DEFAULT, 2013, LAST)
ro_mape = ro.group_by("weather_zone", maintain_order=True).agg(
    pl.col("ape").mean().round(2).alias("mape_1y_2013_2025"), pl.col("ape").max().round(1).alias("max_ape"),
    pl.col("year").sort_by("ape").last().alias("worst_year"))
print(ro_mape)

# %% 7. Structural breaks in the full-sample residuals
breaks = []
for zone in SERIES:
    sub = hist.filter(pl.col("weather_zone") == zone).sort("year")
    fit = wl.fit_peak(sub, DEFAULT, zone=zone)
    X = fit.design(sub["year"].to_list(), sub[DEFAULT].to_list())
    y = sub["peak_mw"].to_numpy()
    chow = wl.sup_chow(sub["year"].to_list(), X, y, n_boot=2000, seed=7)
    cusum_stat, cusum_p, _ = breaks_cusumolsresid(np.asarray(fit.residuals_mw), ddof=X.shape[1])
    resid = np.asarray(fit.residuals_mw)
    first_neg = sub["year"].to_numpy()[np.argmin(np.cumsum(resid))]
    breaks.append({"zone": zone, "chow_break_year": chow["break_year"], "sup_f": round(chow["sup_f"], 2),
                   "p_boot": round(chow["p_boot"], 4), "cusum_stat": round(float(cusum_stat), 3),
                   "cusum_p": round(float(cusum_p), 4), "cusum_min_year": int(first_neg)})
breaks = pl.DataFrame(breaks)
print(breaks)

# Residuals by year (MW) for the table in the doc.
resid_wide = pl.DataFrame({"year": list(range(FIRST, LAST + 1))})
for zone in SERIES:
    sub = hist.filter(pl.col("weather_zone") == zone).sort("year")
    fit = wl.fit_peak(sub, DEFAULT, zone=zone)
    resid_wide = resid_wide.with_columns(pl.Series(zone, np.round(fit.residuals_mw)))
print(resid_wide)

# %% 8. 2026 so far vs. the weather-normalized expectation (model fit on 2003–2025)
y26 = panel.filter(pl.col("year") == 2026)
print("2026 coverage:", y26.select("weather_zone", "n_hours", "weather_days"))
rows = []
for zone in SERIES:
    sub = hist.filter(pl.col("weather_zone") == zone)
    fit = wl.fit_peak(sub, DEFAULT, zone=zone)
    cur = y26.filter(pl.col("weather_zone") == zone)
    q = sub[DEFAULT].quantile(0.1), sub[DEFAULT].quantile(0.5), sub[DEFAULT].quantile(0.9)
    p10, p50, p90 = fit.predict([2026] * 3, list(q)).tolist()
    pred_actual_wx = float(fit.predict([2026], cur[DEFAULT].to_list())[0])
    actual = cur["peak_mw"].item()
    rows.append({"zone": zone, "actual_2026_mw": actual, "feature_2026": cur[DEFAULT].item(),
                 "feature_p50": q[1], "pred_actual_weather_mw": pred_actual_wx,
                 "gap_pct": 100 * (actual / pred_actual_wx - 1),
                 "wx_p10_mw": p10, "wx_p50_mw": p50, "wx_p90_mw": p90,
                 "resid_band_mw": 1.2816 * fit.resid_sd_mw,
                 "peak_local": peaks.filter((pl.col("weather_zone") == zone) & (pl.col("year") == 2026))["peak_local"].item()})
y26_table = pl.DataFrame(rows)
print(y26_table.with_columns(pl.selectors.float().round(1)))

# %% 9. Figure: actual vs. model, hold-out years highlighted (the decision's evidence)
fig, axes = plt.subplots(3, 3, figsize=(13, 10), sharex=True)
ACTUAL, FIT, PRED = "#52514e", "#2a78d6", "#eb6834"
for ax, zone in zip(axes.flat, SERIES, strict=True):
    sub = hist.filter(pl.col("weather_zone") == zone).sort("year")
    train = sub.filter(pl.col("year") <= TRAIN_UNTIL)
    fit = wl.fit_peak(train, DEFAULT, zone=zone)
    years = sub["year"].to_numpy()
    ax.plot(years, sub["peak_mw"] / 1000, color=ACTUAL, lw=2, marker="o", ms=4, label="actual")
    ax.plot(train["year"], fit.predict(train["year"].to_list(), train[DEFAULT].to_list()) / 1000,
            color=FIT, lw=2, label="fit ≤ 2022")
    test = sub.filter(pl.col("year") > TRAIN_UNTIL)
    ax.plot(test["year"], fit.predict(test["year"].to_list(), test[DEFAULT].to_list()) / 1000,
            color=PRED, lw=2, marker="o", ms=5, label="hold-out prediction")
    m = ho_mape.filter(pl.col("weather_zone") == zone)["mape_pct"].item()
    ax.set_title(f"{zone}  (hold-out MAPE {m:.1f}%)", fontsize=10, loc="left")
    ax.grid(axis="y", color="#e5e4df", lw=0.8)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.set_ylabel("GW", fontsize=8)
axes.flat[0].legend(fontsize=8, frameon=False)
fig.suptitle("Summer peak (Jun–Sep) vs. peak ~ year + max 3-day mean temperature", fontsize=12, x=0.01, ha="left")
fig.tight_layout()
fig.savefig(out_path("q7_holdout_by_zone.png"), dpi=130)
print("saved", out_path("q7_holdout_by_zone.png"))
