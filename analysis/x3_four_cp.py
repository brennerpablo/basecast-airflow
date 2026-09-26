# %% [markdown]
# # X3 — 4CP targeting: the offer to co-ops
#
# Exploration X3 after phase 0. Questions and results in `docs/analysis/x3_four_cp.md`; logic in
# `basecast_pipelines/models/four_cp.py` (tests in `tests/models/test_four_cp.py`). Read-only: Postgres through the
# Cloud SQL proxy, plus the local raw lake for the fuel mix and real-time prices (Q4), parsed with the repo's own
# parsers and cached under `analysis/out/` (gitignored).
#
# Run from the repo root: `uv run --group analysis python analysis/x3_four_cp.py`.

# %%
from __future__ import annotations

from types import SimpleNamespace

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from scipy import stats

from analysis._common import ROOT, out_path
from basecast_pipelines.models import four_cp as fc
from basecast_pipelines.models import weather_load as wl

EVAL_YEARS = range(2010, 2026)  # full four-CP summers used for the dispatch rates
MONTH_NAMES = {6: "Jun", 7: "Jul", 8: "Aug", 9: "Sep"}

# %% Load (read-only)
monthly = fc.load_monthly_peaks()
load = wl.load_hourly_load()
weather = wl.load_hourly_weather()
print(monthly.height, load.height, weather.height)
print("load through", load["ts_utc"].max(), "| weather through", weather["ts_utc"].max())

# %% [markdown]
# ## Q1 — the 4CP intervals by year

# %%
cp15 = fc.cp_intervals(monthly)
cph = fc.hourly_cps(load)
cps = fc.cp_calendar(cp15, cph)
cps = cps.filter(pl.col("year") >= 2008)

print("15-minute CPs:", cp15.height, "months", cp15["year"].min(), "→", cp15["year"].max())
print("hourly fill-ins:", cps.filter(pl.col("source") == "hourly").select("year", "month", "cp_mw", "end_local"))

table = cps.with_columns(
    pl.format(
        "{} {} ({} MW)",
        pl.col("end_local").dt.strftime("%m-%d"),
        pl.col("end_local").dt.strftime("%H:%M"),
        (pl.col("cp_mw") / 1).round(0).cast(pl.Int64),
    ).alias("cell")
).pivot(on="month", index="year", values="cell", sort_columns=True)
with pl.Config(tbl_rows=30, fmt_str_lengths=40):
    print(table)

# %% Cross-check: 15-minute CP vs the hourly table's max hour of the month
check = cp15.join(
    cph.select("year", "month", pl.col("cp_mw").alias("h_mw"), pl.col("cp_date").alias("h_date"),
               pl.col("hour_ending").alias("h_he")),
    on=["year", "month"], how="inner",
).with_columns(
    (pl.col("cp_date") == pl.col("h_date")).alias("same_day"),
    (pl.col("hour_ending") == pl.col("h_he")).alias("same_he"),
    ((pl.col("cp_mw") / pl.col("h_mw")) - 1).alias("ratio_minus_1"),
)
print("months compared:", check.height, "| same day:", check["same_day"].sum(), "| same hour ending:",
      check["same_he"].sum())
print("15-min / hourly − 1: min %.4f max %.4f" % (check["ratio_minus_1"].min(), check["ratio_minus_1"].max()))
print(check.filter(~pl.col("same_day")).select("year", "month", "cp_date", "h_date", "cp_mw", "h_mw"))
# the hourly load at the 15-min CP day's HE 17 and HE 18: which hour is higher?
he = load.filter(pl.col("weather_zone") == "ERCOT").select(
    pl.col("operating_date").alias("cp_date"), "hour_ending", "mw"
)
he_cp = cp15.select("year", "month", "cp_date", "end_min").join(he, on="cp_date").filter(
    pl.col("hour_ending").is_in([17, 18])
).pivot(on="hour_ending", index=["year", "month", "end_min"], values="mw")
print("CP days with HE18 > HE17 (hourly):", he_cp.filter(pl.col("18") > pl.col("17")).height, "of", he_cp.height)
print(he_cp.filter(pl.col("18") > pl.col("17")).group_by(pl.col("year") >= 2023).agg(pl.len()))

# %% Timing trend: interval end (local clock) and day of week
cps_t = cps.filter(pl.col("source") == "15min").with_columns((pl.col("end_min") / 60).alias("end_hour"))
fit = stats.linregress(cps_t["year"].to_numpy(), cps_t["end_hour"].to_numpy())
print(f"interval end vs year: slope {fit.slope * 60:.2f} min/yr (SE {fit.stderr * 60:.2f}), p = {fit.pvalue:.4f}, "
      f"n = {cps_t.height}")
eras = cps_t.with_columns(
    pl.when(pl.col("year") <= 2012).then(pl.lit("2008–2012"))
    .when(pl.col("year") <= 2017).then(pl.lit("2013–2017"))
    .when(pl.col("year") <= 2022).then(pl.lit("2018–2022"))
    .otherwise(pl.lit("2023–2026")).alias("era")
)
era_tab = eras.group_by("era").agg(
    pl.len().alias("n"),
    pl.col("end_hour").mean().alias("mean_end_hour"),
    (pl.col("end_min") <= 990).sum().alias("end_le_1630"),
    (pl.col("end_min") == 1005).sum().alias("end_1645"),
    (pl.col("end_min") == 1020).sum().alias("end_1700"),
    (pl.col("end_min") > 1020).sum().alias("end_after_1700"),
    (pl.col("weekday") >= 6).sum().alias("weekend"),
    (pl.col("weekday") == 5).sum().alias("friday"),
).sort("era")
print(era_tab)
wd = cps_t.group_by("weekday").agg(pl.len()).sort("weekday")
print("weekday counts (1 = Mon):", dict(zip(wd["weekday"].to_list(), wd["len"].to_list(), strict=True)))
print("latest CP each year (end time):",
      cps_t.group_by("year").agg(pl.col("end_min").max()).sort("year").with_columns(
          (pl.col("end_min") // 60 * 100 + pl.col("end_min") % 60).alias("hhmm"))["hhmm"].to_list())

# day-of-month position: how many days of the month's top-5 are on a weekend, rank of the CP day by daily peak
daily = fc.daily_peaks(load)
rank = daily.filter(pl.col("month").is_in(fc.CP_MONTHS)).with_columns(
    pl.col("peak_mw").rank("ordinal", descending=True).over("year", "month").alias("rank")
)
cp_rank = cps.join(rank.select(pl.col("operating_date").alias("cp_date"), "rank"), on="cp_date", how="left")
print("CP day's rank among the month's daily hourly peaks:", cp_rank["rank"].value_counts().sort("rank").rows())

# %% Figure: CP interval end time by year
fig, ax = plt.subplots(figsize=(8, 4))
colors = {6: "#4C78A8", 7: "#F58518", 8: "#E45756", 9: "#72B7B2"}
for m in fc.CP_MONTHS:
    sub = cps.filter(pl.col("month") == m)
    ax.scatter(sub["year"] + (m - 7.5) * 0.12, sub["end_min"] / 60, label=MONTH_NAMES[m], color=colors[m], s=28)
yrs = np.arange(2008, 2027)
ax.plot(yrs, fit.intercept + fit.slope * yrs, color="#666", lw=1, ls="--",
        label=f"trend {fit.slope * 60:+.1f} min/yr")
ax.set_yticks([14.5, 15, 15.5, 16, 16.5, 17, 17.5, 18])
ax.set_yticklabels(["14:30", "15:00", "15:30", "16:00", "16:30", "17:00", "17:30", "18:00"])
ax.set_ylabel("CP interval end (CPT)")
ax.set_title("ERCOT 4CP: 15-minute interval end by year (Sep 2026: hourly, provisional)")
ax.legend(ncol=5, fontsize=8, frameon=False)
ax.grid(alpha=0.3)
fig.tight_layout()
fig.savefig(out_path("x3_cp_timing.png"), dpi=140)
plt.close(fig)

# %% [markdown]
# ## Q2 — dispatch days needed to catch all four CPs

# %% Windows: coverage of the CP intervals by fixed discharge windows
cp_eval = cps.filter(pl.col("year").is_between(EVAL_YEARS.start, EVAL_YEARS.stop - 1))
recent = cps.filter(pl.col("year") >= 2021)
for length in (60, 120, 240):
    cov = fc.window_coverage(cp_eval, length).join(
        fc.window_coverage(recent.filter(pl.col("source") == "15min"), length).select(
            "start_min", pl.col("covered").alias("covered_2021_26"), pl.col("n").alias("n_2021_26")),
        on="start_min",
    ).filter(pl.col("covered") > 0)
    print(cov.select("window", "covered", "n", "share", "covered_2021_26", "n_2021_26"))

WINDOWS = {  # (start_min, length_min): the windows reported in the doc
    "1h 16:00–17:00 (HE 17)": (960, 60),
    "1h 16:15–17:15": (975, 60),
    "2h 15:45–17:45": (945, 120),
    "2h 16:00–18:00 (HE 17–18)": (960, 120),
    "4h 15:00–19:00 (HE 16–19)": (900, 240),
    "4h 16:00–20:00 (HE 17–20)": (960, 240),
}
for name, (s, n) in WINDOWS.items():
    c = cp_eval.select(fc.in_window(s, n).sum()).item()
    r = recent.filter(pl.col("source") == "15min").select(fc.in_window(s, n).sum()).item()
    print(f"{name}: {c}/{cp_eval.height} CPs 2010–2025, {r}/{recent.filter(pl.col('source') == '15min').height} 2021–2026")

# %% Forecasts of the daily peak
daily = fc.with_reference(fc.daily_peaks(load))
wx_daily = wl.weather_daily(weather)
weights = wl.zone_weights(load, range(2003, 2023))
sys_wx = wl.system_weather_daily(wx_daily, weights).select("date", "t_max", "t_mean")
daily = fc.weather_forecast(daily, sys_wx, range(2008, 2027))
summer = daily.filter(pl.col("month").is_in(fc.CP_MONTHS) & pl.col("year").is_between(2010, 2025))
err = summer.filter(pl.col("forecast_wx").is_not_null()).select(
    ((pl.col("forecast_wx") / pl.col("peak_mw")).log()).alias("e")
)
wx_sigma = err["e"].std()
print(f"weather model day-ahead error (log SD) 2010–2025 summers: {wx_sigma:.4f}; "
      f"MAPE {(summer['forecast_wx'] / summer['peak_mw'] - 1).abs().mean():.4f}")
print("record days (a new month-to-date max, perfect foresight) per summer:",
      summer.with_columns((pl.col("peak_mw") >= pl.col("reference_mw")).fill_null(True).alias("rec"))
      .group_by("year").agg(pl.col("rec").sum()).sort("year")["rec"].to_list())

# %% Rule (a): threshold on the forecast vs the month-to-date max
XS = [0.90, 0.92, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 1.00]
SIGMAS = [0.0, 0.02, 0.03, 0.05]
DRAWS = 100
rows = []


def _eval(dispatch: pl.DataFrame, label: str, param: float, draw: int | None = None) -> None:
    disp = dispatch.filter(pl.col("year").is_between(EVAL_YEARS.start, EVAL_YEARS.stop - 1))
    for wname, (s, n) in WINDOWS.items():
        res = fc.summarize_dispatch(fc.evaluate_dispatch(disp, cp_eval, s, n))
        rows.append({"forecast": label, "param": param, "draw": draw, "window": wname, **res})


for x in XS:
    _eval(fc.threshold_dispatch(daily, "forecast_wx", x), "weather (ERA5 observed)", x)
for sigma in SIGMAS:
    for draw in range(DRAWS if sigma > 0 else 1):
        noisy = fc.noisy_forecast(daily, sigma, seed=draw)
        for x in XS:
            _eval(fc.threshold_dispatch(noisy, "forecast", x), f"actual × noise σ={sigma:.0%}", x, draw)

# Rule (b): top-N days per month by forecast peak (weather model) and by forecast t_max
daily_t = daily.join(sys_wx.select(pl.col("date").alias("operating_date"), "t_max"), on="operating_date", how="left")
for n in range(1, 11):
    _eval(fc.top_n_dispatch(daily_t, "forecast_wx", n), "top-N by weather-model peak", n)
    _eval(fc.top_n_dispatch(daily_t, "t_max", n), "top-N by hottest t_max", n)
    _eval(fc.top_n_dispatch(daily_t, "peak_mw", n), "top-N by actual peak (hindsight)", n)

res = pl.DataFrame(rows).group_by("forecast", "param", "window").agg(
    pl.col("dispatch_days").mean(), pl.col("all4_rate").mean(), pl.col("month_rate").mean(),
    pl.col("day_rate").mean(),
).sort("forecast", "window", "param")
res.write_csv(out_path("x3_dispatch_rules.csv"))
MAIN = "2h 15:45–17:45"
with pl.Config(tbl_rows=200):
    print(res.filter(pl.col("window") == MAIN).select("forecast", "param", "dispatch_days", "all4_rate", "month_rate",
                                                       "day_rate"))


# %% Smallest dispatch-day budget for a target hit rate, per forecast and window
def budget(target: float) -> pl.DataFrame:
    return (
        res.filter(pl.col("all4_rate") >= target)
        .sort("dispatch_days")
        .group_by("forecast", "window", maintain_order=True)
        .first()
        .select("forecast", "window", "param", "dispatch_days", "all4_rate", "month_rate")
        .sort("window", "forecast")
    )


with pl.Config(tbl_rows=80, fmt_str_lengths=40):
    for t in (0.75, 0.9, 1.0):
        print(f"--- all four caught in ≥ {t:.0%} of 2010–2025 summers")
        print(budget(t))

# %% Per-year detail for the headline rule (weather model, 2h window)
for x in (0.95, 0.97):
    per = fc.evaluate_dispatch(fc.threshold_dispatch(daily, "forecast_wx", x), cps.filter(pl.col("year") >= 2010),
                               *WINDOWS[MAIN])
    print(f"weather model, x = {x}:")
    print(per.select("year", "n_cp", "caught", "day_caught", "dispatch_days", "all_caught"))

# %% Figure: hit rate vs dispatch days per summer
fig, axes = plt.subplots(1, 3, figsize=(13, 4), sharey=True)
panels = ["1h 16:00–17:00 (HE 17)", "2h 15:45–17:45", "4h 15:00–19:00 (HE 16–19)"]
styles = {
    "actual × noise σ=0%": ("#222", "-", "o"),
    "actual × noise σ=3%": ("#4C78A8", "-", "o"),
    "actual × noise σ=5%": ("#9ecae9", "-", "o"),
    "weather (ERA5 observed)": ("#E45756", "-", "s"),
    "top-N by weather-model peak": ("#F58518", ":", "^"),
    "top-N by actual peak (hindsight)": ("#54A24B", ":", "v"),
}
for ax, wname in zip(axes, panels, strict=True):
    for label, (col, ls, mk) in styles.items():
        sub = res.filter((pl.col("forecast") == label) & (pl.col("window") == wname)).sort("dispatch_days")
        ax.plot(sub["dispatch_days"], sub["all4_rate"] * 100, color=col, ls=ls, marker=mk, ms=3, lw=1.2,
                label=label.replace("actual × noise", "perfect × noise"))
    ax.set_title(wname, fontsize=10)
    ax.set_xlabel("dispatch days per summer (Jun–Sep)")
    ax.set_xlim(0, 90)
    ax.grid(alpha=0.3)
axes[0].set_ylabel("summers with all 4 CPs caught (%), 2010–2025")
axes[0].legend(fontsize=7, frameon=False, loc="lower right")
fig.suptitle("Day-ahead 4CP dispatch: hit rate vs. days called (threshold on month-to-date max, or top-N days)",
             fontsize=11)
fig.tight_layout()
fig.savefig(out_path("x3_dispatch_tradeoff.png"), dpi=140)
plt.close(fig)

# %% Has it got harder? Budget by era (weather model, 2h window)
for lo, hi in ((2010, 2017), (2018, 2025)):
    for x in (0.93, 0.95, 0.97, 0.98):
        disp = fc.threshold_dispatch(daily, "forecast_wx", x).filter(pl.col("year").is_between(lo, hi))
        s = fc.summarize_dispatch(fc.evaluate_dispatch(disp, cps.filter(pl.col("year").is_between(lo, hi)),
                                                       *WINDOWS[MAIN]))
        print(f"{lo}–{hi} x={x}: days {s['dispatch_days']:.1f}, all4 {s['all4_rate']:.2f}, months {s['month_rate']:.2f}")
# how close the runner-up days come: the month's 2nd-highest daily peak as % of the CP day's
close = rank.filter(pl.col("year") >= 2010).group_by("year", "month").agg(
    pl.col("peak_mw").sort(descending=True).head(5).alias("top5")
).with_columns(
    (pl.col("top5").list.get(1) / pl.col("top5").list.get(0)).alias("second_pct"),
    (pl.col("top5").list.get(4) / pl.col("top5").list.get(0)).alias("fifth_pct"),
)
print(close.group_by(pl.col("year") >= 2018).agg(pl.col("second_pct").mean(), pl.col("fifth_pct").mean()))
days_within = rank.filter(pl.col("year") >= 2010).with_columns(
    (pl.col("peak_mw") / pl.col("peak_mw").max().over("year", "month")).alias("pct")
).group_by("year").agg((pl.col("pct") >= 0.97).sum().alias("days_ge_97"), (pl.col("pct") >= 0.95).sum().alias("days_ge_95"))
print(days_within.sort("year"))

# %% [markdown]
# ## Q3 — zones at the 4CP intervals

# %%
zc = fc.zone_coincidence(monthly, "weather_zone").filter(pl.col("year") >= 2008)
zc.write_csv(out_path("x3_zone_coincidence.csv"))
with pl.Config(tbl_rows=30):
    print(zc.pivot(on="region_id", index="year", values="cf_summer").sort("year"))
    print(zc.pivot(on="region_id", index="year", values="share_4cp").sort("year"))
era_z = zc.filter(pl.col("n_months") == 4).with_columns(
    pl.when(pl.col("year") <= 2014).then(pl.lit("2010–2014")).when(pl.col("year") >= 2021)
    .then(pl.lit("2021–2025")).otherwise(None).alias("era")
).filter(pl.col("era").is_not_null() & (pl.col("year") >= 2010)).group_by("region_id", "era").agg(
    pl.col("cf_summer").mean(), pl.col("cf_month").mean(), pl.col("share_4cp").mean(), pl.col("share_energy").mean(),
    pl.col("ncp_end_hour").mean(),
).sort("region_id", "era")
print(era_z)
z26 = zc.filter(pl.col("year") == 2026).select("region_id", "cf_summer", "cf_month", "share_4cp", "share_energy",
                                              "ncp_end_hour", "n_months")
print("2026 (Jun–Aug only):", z26)

lz = fc.zone_coincidence(monthly, "load_zone").filter(pl.col("year") >= 2011, ~pl.col("region_id").str.starts_with("DC_"))
lz_era = lz.filter(pl.col("n_months") == 4).with_columns(
    pl.when(pl.col("year") <= 2014).then(pl.lit("2011–2014")).when(pl.col("year") >= 2021)
    .then(pl.lit("2021–2025")).otherwise(None).alias("era")
).filter(pl.col("era").is_not_null()).group_by("region_id", "era").agg(
    pl.col("cf_summer").mean(), pl.col("share_4cp").mean(), pl.col("share_energy").mean(), pl.col("ncp_end_hour").mean()
).sort("region_id", "era")
print(lz_era)

# figure: coincidence factor by zone over time
fig, axes = plt.subplots(1, 2, figsize=(12, 4))
zcol = {"COAST": "#4C78A8", "EAST": "#9ecae9", "FWEST": "#E45756", "NCENT": "#F58518", "NORTH": "#B279A2",
        "SCENT": "#54A24B", "SOUTH": "#72B7B2", "WEST": "#EECA3B"}
for z, col in zcol.items():
    sub = zc.filter((pl.col("region_id") == z) & (pl.col("n_months") == 4)).sort("year")
    lw = 2.2 if z in ("FWEST", "NORTH", "WEST") else 1.1
    axes[0].plot(sub["year"], sub["cf_summer"], color=col, lw=lw, label=z)
    axes[1].plot(sub["year"], sub["share_4cp"] * 100, color=col, lw=lw, label=z)
axes[0].set_title("4CP load ÷ zone's own summer 15-min peak (coincidence factor)", fontsize=10)
axes[1].set_title("Zone share of ERCOT's 4CP load (%)", fontsize=10)
axes[1].set_yscale("log")
for ax in axes:
    ax.grid(alpha=0.3)
axes[0].legend(ncol=4, fontsize=7, frameon=False)
fig.tight_layout()
fig.savefig(out_path("x3_zone_coincidence.png"), dpi=140)
plt.close(fig)

# %% [markdown]
# ## Q4 — net load and real-time prices at the 4CP intervals
#
# Fuel mix (`ercot_fuel_mix`) and RTM hub prices (`ercot_spp_hist`) are not in Postgres; the raw workbooks are in
# the local lake, so they are parsed here with the repo's parsers and cached (wind + solar only; HB_HUBAVG only).

# %%
RAW = ROOT / "data" / "raw"
fuel_cache = out_path("x3_fuel_wind_solar.parquet")
if not fuel_cache.exists():
    from basecast_pipelines.parsers.ercot._market_common import workbooks
    from basecast_pipelines.parsers.ercot.fuel_mix import _workbook

    frames = []
    for p in sorted((RAW / "source=ercot_fuel_mix").glob("dt=*/*")):
        if p.suffix.lower() not in {".zip", ".xlsx", ".xls"}:
            continue
        f = SimpleNamespace(name=p.name, suffix=p.suffix.lower(), key=str(p), read_bytes=p.read_bytes)
        for name, data in workbooks(f):
            year = int("".join(ch for ch in name if ch.isdigit())[:4])
            if year < 2011:
                continue
            wb = _workbook(name, data, "x3")
            frames.append(wb.filter(pl.col("fuel").cast(pl.String).is_in(["wind", "solar"])).select(
                "interval_start_utc", pl.col("fuel").cast(pl.String), "generation_mwh"))
            print("fuel mix", name, wb.height)
    pl.concat(frames).unique(["interval_start_utc", "fuel"], keep="last").sort("interval_start_utc").write_parquet(fuel_cache)
fuel = pl.read_parquet(fuel_cache)
print("fuel mix rows", fuel.height, fuel["interval_start_utc"].min(), "→", fuel["interval_start_utc"].max())

price_cache = out_path("x3_rtm_hubavg.parquet")
if not price_cache.exists():
    from basecast_pipelines.parsers.ercot.spp_hist import parse_rtm

    frames = []
    for p in sorted((RAW / "source=ercot_spp_hist").glob("dt=*/*RTMLZHBSPP_*.zip")):
        f = SimpleNamespace(name=p.name, suffix=".zip", key=str(p), meta={}, url=str(p), read_bytes=p.read_bytes)
        df = parse_rtm(f)
        frames.append(df.filter(pl.col("settlement_point") == "HB_HUBAVG").select("interval_start_utc", "price_usd_mwh"))
        print("rtm", p.name, df.height)
    pl.concat(frames).unique("interval_start_utc", keep="last").sort("interval_start_utc").write_parquet(price_cache)
price = pl.read_parquet(price_cache)
print("price rows", price.height, price["interval_start_utc"].min(), "→", price["interval_start_utc"].max())

# %% Net-load peak vs load peak, per summer month
net = fc.net_load_hourly(load, fuel)
lmax = fc.monthly_argmax(net, "load_mw")
nmax = fc.monthly_argmax(net, "net_mw")
peaks = lmax.join(nmax, on=["year", "month"]).filter(pl.col("year") >= 2011)
by_year = peaks.group_by("year").agg(
    pl.col("load_mw_he").mean().alias("load_peak_he"),
    pl.col("net_mw_he").mean().alias("net_peak_he"),
    (pl.col("net_mw_date") == pl.col("load_mw_date")).sum().alias("same_day"),
    pl.len().alias("months"),
).sort("year")
print(by_year)
ren_share = net.filter(pl.col("operating_date").dt.month().is_in(fc.CP_MONTHS)).group_by(
    pl.col("operating_date").dt.year().alias("year")
).agg(((pl.col("wind_mw") + pl.col("solar_mw")).sum() / pl.col("load_mw").sum()).alias("wind_solar_share"),
      pl.col("solar_mw").max().alias("solar_max_mw")).sort("year")
print(ren_share)

# %% The CP interval in net-load and price terms
fuel15 = fuel.pivot(on="fuel", index="interval_start_utc", values="generation_mwh")
cp_q4 = (
    cps.filter((pl.col("source") == "15min") & (pl.col("year") >= 2011))
    .with_columns((pl.col("end_utc") - pl.duration(minutes=15)).alias("interval_start_utc"))
    .join(fuel15, on="interval_start_utc", how="left")
    .join(price, on="interval_start_utc", how="left")
    .with_columns((pl.col("cp_mw") - 4 * (pl.col("wind") + pl.col("solar"))).alias("cp_net_mw"))
)
# rank of the CP hour in the month by hourly net load, and of the CP interval by RT price
net_rank = fc.rank_within_month(net, "net_mw").select(
    pl.col("ts_utc"), "net_mw", "net_mw_rank"
)
cp_q4 = cp_q4.with_columns(
    (pl.col("interval_start_utc").dt.truncate("1h") + pl.duration(hours=1)).alias("ts_utc")
).join(net_rank, on="ts_utc", how="left")
price_m = price.with_columns(
    pl.col("interval_start_utc").dt.convert_time_zone(fc.LOCAL_TZ).alias("_l")
).filter(pl.col("_l").dt.month().is_in(fc.CP_MONTHS)).with_columns(
    pl.col("price_usd_mwh").rank("min", descending=True).over(pl.col("_l").dt.year(), pl.col("_l").dt.month())
    .alias("price_rank"),
    pl.col("price_usd_mwh").max().over(pl.col("_l").dt.year(), pl.col("_l").dt.month()).alias("price_month_max"),
)
cp_q4 = cp_q4.join(price_m.select("interval_start_utc", "price_rank", "price_month_max"), on="interval_start_utc",
                   how="left")
with pl.Config(tbl_rows=80):
    print(cp_q4.select("year", "month", "end_local", "cp_mw", "cp_net_mw", "net_mw_rank", "price_usd_mwh",
                       "price_rank", "price_month_max"))
print(cp_q4.group_by(pl.col("year") >= 2021).agg(
    pl.col("net_mw_rank").median().alias("median_net_rank"),
    (pl.col("net_mw_rank") <= 10).mean().alias("net_top10"),
    pl.col("price_rank").median().alias("median_price_rank"),
    (pl.col("price_rank") <= 20).mean().alias("price_top20"),
    pl.len(),
))

# where the scarcity sits: hour ending of the month's top-20 RT price intervals, by year
top_px = price_m.filter(pl.col("price_rank") <= 20).with_columns(
    ((pl.col("_l").dt.hour().cast(pl.Int32) * 60 + pl.col("_l").dt.minute().cast(pl.Int32) + 15 + 59) // 60)
    .alias("he")
)
print(top_px.group_by(pl.col("_l").dt.year().alias("year")).agg(
    pl.col("he").median().alias("median_he"),
    (pl.col("he").is_between(17, 18)).mean().alias("share_he17_18"),
    (pl.col("he") >= 19).mean().alias("share_he19plus"),
).sort("year"))

# %% Figure: load peak vs net-load peak hour, and the CP interval's net-load rank
fig, axes = plt.subplots(1, 2, figsize=(12, 4))
axes[0].plot(by_year["year"], by_year["load_peak_he"], marker="o", label="load peak (mean HE, Jun–Sep)")
axes[0].plot(by_year["year"], by_year["net_peak_he"], marker="s", label="net-load peak (load − wind − solar)")
axes[0].set_ylabel("hour ending (CPT)")
axes[0].set_title("Monthly peak hour: load vs net load", fontsize=10)
axes[0].legend(fontsize=8, frameon=False)
axes[0].grid(alpha=0.3)
yr_rank = cp_q4.group_by("year").agg(pl.col("net_mw_rank").median(), pl.col("price_rank").median()).sort("year")
axes[1].semilogy(yr_rank["year"], yr_rank["net_mw_rank"], marker="o", label="net-load rank of the CP hour (of ~720 h)")
axes[1].semilogy(yr_rank["year"], yr_rank["price_rank"], marker="s", label="RT price rank of the CP interval (of ~2,900)")
axes[1].invert_yaxis()
axes[1].set_title("Is the 4CP interval still a scarcity interval? (median over the 4 months)", fontsize=10)
axes[1].legend(fontsize=8, frameon=False)
axes[1].grid(alpha=0.3)
fig.tight_layout()
fig.savefig(out_path("x3_net_load.png"), dpi=140)
plt.close(fig)

# %%
print("done")
