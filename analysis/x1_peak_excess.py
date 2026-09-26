"""X1 — is the recent summer-peak excess over weather + trend the large loads showing up?

Run from the repo root: ``uv run --group analysis python analysis/x1_peak_excess.py``.
Reads ``ercot_load_hourly_wz``, ``weather_hourly_wz``, ``large_load_chart_values``, ``county_weather_zone`` and
``tceq_data_center_sites`` (read-only). Logic: ``basecast_pipelines/models/peak_excess.py`` (on top of Q7's
``weather_load.py`` and Q5's ``large_load.py``). Numbers go to ``docs/analysis/x1_peak_excess.md``.
"""

# %% Setup
from __future__ import annotations

from datetime import date

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import polars as pl

from analysis._common import out_path, read_sql
from basecast_pipelines.models import large_load as ll
from basecast_pipelines.models import peak_excess as px
from basecast_pipelines.models import weather_load as wl

FIRST, TRAIN_UNTIL = 2003, px.PRE_BREAK_UNTIL
FEATURE = "t_mean_3d"
SERIES = (*wl.WEATHER_ZONES, wl.TOTAL)
YEARS = (2020, 2021, 2022, 2023, 2024, 2025, 2026)

load = wl.load_hourly_load()
weather = wl.load_hourly_weather()
cv = ll.load_chart_values()
print(load.shape, weather.shape, cv.shape)
print("last operating day:", load["operating_date"].max(), "| last weather hour:", weather["ts_utc"].max())

# %% 1a. Non-coincident excess per zone over the pre-break model (fit <= 2019, actual weather)
daily_w = wl.weather_daily(weather)
weights = wl.zone_weights(load, range(FIRST, TRAIN_UNTIL + 1))
print("ERCOT weather weights (2003-2019):", {k: round(v, 3) for k, v in sorted(weights.items())})
daily_w_all = pl.concat([daily_w.drop("n_hours"), wl.system_weather_daily(daily_w, weights).drop("n_hours")])
features = wl.summer_weather_features(daily_w_all)
peaks = wl.summer_peaks(load)
panel = peaks.select("weather_zone", "year", "peak_mw").join(features, on=["weather_zone", "year"], how="inner")
panel = panel.filter(pl.col("year") >= FIRST)

ex_peak = px.excess_vs_prebreak(panel, "peak_mw", FEATURE)
ex_peak_wide = ex_peak.filter(pl.col("year").is_in(YEARS)).pivot(
    on="weather_zone", index="year", values="excess_mw").select("year", *SERIES).sort("year")
print("non-coincident peak excess, MW (actual - pre-break prediction):")
print(ex_peak_wide.with_columns(pl.exclude("year").round(0)))
print("excess %:")
print(ex_peak.filter(pl.col("year").is_in(YEARS)).pivot(on="weather_zone", index="year", values="excess_pct")
      .select("year", *SERIES).sort("year").with_columns(pl.exclude("year").round(1)))
print("training residual SD and trend (MW):")
print(ex_peak.group_by("weather_zone").agg(pl.col("train_resid_sd_mw").first().round(0),
                                            pl.col("trend_mw_yr").first().round(1)).sort("weather_zone"))
ercot_pred = ex_peak.filter((pl.col("weather_zone") == wl.TOTAL) & pl.col("year").is_in(YEARS))
print(ercot_pred.select("year", "actual_mw", "predicted_mw", "excess_mw", "excess_pct").with_columns(
    pl.selectors.float().round(1)))

# %% 1b. Coincident, additive decomposition: each zone's MW at the ERCOT peak hour ~ year + ERCOT weather
coinc = px.coincident_zone_loads(load)
ercot_feat = features.filter(pl.col("weather_zone") == wl.TOTAL).select("year", FEATURE)
cpanel = coinc.join(ercot_feat, on="year").filter(pl.col("year") >= FIRST)
ex_coinc = px.excess_vs_prebreak(cpanel, "coincident_mw", FEATURE)
ex_coinc_wide = ex_coinc.filter(pl.col("year").is_in(YEARS)).pivot(
    on="weather_zone", index="year", values="excess_mw").select("year", *SERIES).sort("year")
zone_sum = ex_coinc.filter(pl.col("weather_zone") != wl.TOTAL).group_by("year").agg(
    pl.col("excess_mw").sum().alias("zone_sum"))
print("coincident excess at the ERCOT peak hour, MW (zones add up to ERCOT):")
print(ex_coinc_wide.join(zone_sum, on="year").with_columns(pl.exclude("year").round(0)))
print("peak hours:", coinc.filter(pl.col("weather_zone") == wl.TOTAL).filter(pl.col("year") >= 2020)
      .with_columns(pl.col("ts_utc").dt.convert_time_zone(wl.LOCAL_TZ).alias("local")).select("year", "local"))

# Shares of the ERCOT coincident excess, averaged 2023-2026.
share = (ex_coinc.filter(pl.col("year").is_between(2023, 2026) & (pl.col("weather_zone") != wl.TOTAL))
         .group_by("weather_zone").agg(pl.col("excess_mw").mean().alias("mean_excess_2023_26"))
         .with_columns((100 * pl.col("mean_excess_2023_26") / pl.col("mean_excess_2023_26").sum()).alias("share_pct"))
         .sort("mean_excess_2023_26", descending=True))
print(share.with_columns(pl.selectors.float().round(1)))

# %% 2a. Shape: summer mean daily min / max, load factor (Jun 1 - Sep 20 every year)
daily = px.daily_load_stats(load)
shape = px.summer_shape(daily)
tsum = px.summer_mean_weather(daily_w_all)
spanel = shape.join(tsum, on=["weather_zone", "year"], how="inner").filter(pl.col("year") >= FIRST)
print("days per summer window:", spanel.group_by("year").agg(pl.col("days").min()).sort("year").tail(8))

lf = spanel.filter(pl.col("year").is_in([2010, 2015, 2019, 2022, 2023, 2024, 2025, 2026])).pivot(
    on="weather_zone", index="year", values="load_factor").select("year", *SERIES).sort("year")
print("summer load factor (mean / peak):")
print(lf.with_columns(pl.exclude("year").round(3)))
mm = spanel.filter(pl.col("year").is_in([2010, 2015, 2019, 2022, 2023, 2024, 2025, 2026])).pivot(
    on="weather_zone", index="year", values="min_max_ratio").select("year", *SERIES).sort("year")
print("average-day min / max ratio:")
print(mm.with_columns(pl.exclude("year").round(3)))
dmin_tab = spanel.filter(pl.col("year").is_in([2019, 2022, 2023, 2024, 2025, 2026])).pivot(
    on="weather_zone", index="year", values="dmin_mean").select("year", *SERIES).sort("year")
print("summer mean daily minimum, MW:")
print(dmin_tab.with_columns(pl.exclude("year").round(0)))

# %% 2b. Excess of the daily min and max over their own pre-break models (target ~ year + summer mean temp)
ex_min = px.excess_vs_prebreak(spanel, "dmin_mean", "t_summer")
ex_max = px.excess_vs_prebreak(spanel, "dmax_mean", "t_summer")
both = (ex_max.select("weather_zone", "year", pl.col("excess_mw").alias("dmax_excess"),
                      pl.col("predicted_mw").alias("dmax0"), pl.col("train_resid_sd_mw").alias("dmax_sd"))
        .join(ex_min.select("weather_zone", "year", pl.col("excess_mw").alias("dmin_excess"),
                            pl.col("predicted_mw").alias("dmin0"), pl.col("train_resid_sd_mw").alias("dmin_sd")),
              on=["weather_zone", "year"]))
flat = px.flat_block_table(both.select("weather_zone", "year", "dmax_excess", "dmin_excess"),
                           both.select("weather_zone", "year", "dmax0", "dmin0"))
shape_ex = both.join(flat, on=["weather_zone", "year"]).join(
    ex_peak.select("weather_zone", "year", pl.col("excess_mw").alias("peak_excess")), on=["weather_zone", "year"])
print("average-day excess over pre-break (MW): daily max, daily min, flat block F, proportional g, vs. peak excess")
print(shape_ex.filter(pl.col("year").is_in([2023, 2024, 2025, 2026])).select(
    "weather_zone", "year", "dmax_excess", "dmin_excess", "dmin_sd", "flat_mw", "proportional_pct", "peak_excess")
    .sort("weather_zone", "year").with_columns(pl.selectors.float().round(1)))

ercot_years = (ex_peak.filter(pl.col("weather_zone") == wl.TOTAL).select("year", pl.col("excess_mw").alias("peak_ex"))
               .join(both.filter(pl.col("weather_zone") == wl.TOTAL).select("year", "dmax_excess", "dmin_excess",
                                                                            "dmax0", "dmin0"), on="year")
               .with_columns((pl.col("dmin_excess") / pl.col("dmax_excess")).alias("flatness"),
                             (pl.col("dmin0") / pl.col("dmax0")).alias("baseline_min_max"))
               .filter(pl.col("year") >= 2018).sort("year"))
print("ERCOT excess by year (fit <= 2019): peak, average-day max, average-day min; flatness = dmin/dmax excess")
print(ercot_years.with_columns(pl.selectors.float().round(2)))

# Sensitivity: train on 2010-2019 only (drops NORTH's 2009 level shift from the trend)
sens = px.excess_vs_prebreak(panel, "peak_mw", FEATURE, first_year=2010)
sens_min = px.excess_vs_prebreak(spanel, "dmin_mean", "t_summer", first_year=2010)
print("sensitivity, fit 2010-2019: peak excess and daily-min excess, 2025 and 2026")
print(sens.filter(pl.col("year").is_in([2025, 2026])).select("weather_zone", "year", "excess_mw", "excess_pct")
      .join(sens_min.select("weather_zone", "year", pl.col("excess_mw").alias("dmin_excess")),
            on=["weather_zone", "year"]).sort("weather_zone", "year").with_columns(pl.selectors.float().round(0)))

# %% 2c. Compare with the decks: observed energized large load at the same months
energized = px.energized_by_month(cv)
print("observed energized (approved large loads), by month, latest deck reading:")
print(energized)
a2e = px.a2e_by_month_checked(cv).select("month", "a2e_mw")
a2e_q5 = ll.a2e_by_month(cv).select("month", pl.col("a2e_mw").alias("a2e_q5_mw"))
misdated = a2e_q5.join(a2e, on="month", how="left").filter(
    pl.col("a2e_mw").is_null() | (pl.col("a2e_mw") != pl.col("a2e_q5_mw")))
print("months where Q5's a2e_by_month differs once mis-dated axes are dropped:", misdated)
deck = energized.join(a2e, on="month", how="full", coalesce=True).sort("month")
peak_months = (peaks.filter((pl.col("weather_zone") == wl.TOTAL) & (pl.col("year") >= 2023))
               .select("year", pl.col("peak_local").dt.strftime("%Y-%m").alias("month")))
cmp_rows = []
for row in peak_months.iter_rows(named=True):
    y, m = row["year"], row["month"]
    cmp_rows.append({
        "year": y, "peak_month": m,
        "ercot_peak_excess_mw": ex_peak.filter((pl.col("weather_zone") == wl.TOTAL) & (pl.col("year") == y))[
            "excess_mw"].item(),
        "ercot_min_excess_mw": ex_min.filter((pl.col("weather_zone") == wl.TOTAL) & (pl.col("year") == y))[
            "excess_mw"].item(),
        "ercot_flat_block_mw": flat.filter((pl.col("weather_zone") == wl.TOTAL) & (pl.col("year") == y))[
            "flat_mw"].item(),
        "deck_simultaneous_mw": px.value_asof(energized, m, "simultaneous_mw")[0],
        "deck_non_simultaneous_mw": px.value_asof(energized, m, "non_simultaneous_mw")[0],
        "deck_month_used": px.value_asof(energized, m, "simultaneous_mw")[1],
        "deck_a2e_mw": px.value_asof(a2e, m, "a2e_mw")[0],
        "a2e_month_used": px.value_asof(a2e, m, "a2e_mw")[1],
    })
cmp = pl.DataFrame(cmp_rows)
print(cmp.with_columns(pl.selectors.float().round(0)))

# Summer means of the deck series, to line up with the summer-mean min/max excess.
deck_summer = (deck.with_columns(pl.col("month").str.slice(0, 4).cast(pl.Int32).alias("year"),
                                 pl.col("month").str.slice(5, 2).cast(pl.Int32).alias("m"))
               .filter(pl.col("m").is_between(6, 9))
               .group_by("year").agg(pl.col("simultaneous_mw").mean().alias("sim_summer_mean"),
                                     pl.col("non_simultaneous_mw").mean().alias("nonsim_summer_mean"),
                                     pl.col("a2e_mw").mean().alias("a2e_summer_mean"),
                                     pl.col("simultaneous_mw").count().alias("months_sim"))
               .sort("year"))
print(deck_summer.with_columns(pl.selectors.float().round(0)))

# Earliest months of the deck series (how much large load predates 2020 is not in the decks).
print("first deck months:", deck.head(6))

# %% 2d. By region: LZ_WEST (load zone) vs. FWEST + WEST + NORTH weather zones
lz = px.approved_by_load_zone(cv)
print(lz.filter(pl.col("vintage") >= date(2024, 6, 1)))
west_zones = ["FWEST", "WEST", "NORTH"]
west_ex = (shape_ex.filter(pl.col("weather_zone").is_in(west_zones) & pl.col("year").is_between(2022, 2026))
           .group_by("year").agg(pl.col("peak_excess").sum().alias("west3_peak_excess_noncoinc"),
                                 pl.col("dmin_excess").sum().alias("west3_dmin_excess"),
                                 pl.col("flat_mw").sum().alias("west3_flat"))
           .join(ex_coinc.filter(pl.col("weather_zone").is_in(west_zones)).group_by("year").agg(
               pl.col("excess_mw").sum().alias("west3_coinc_excess")), on="year").sort("year"))
print(west_ex.with_columns(pl.selectors.float().round(0)))

# %% 3a. NORTH: the 2009 drop and the 2022+ jump; WEST 2026; FWEST timing
north = daily.filter(pl.col("weather_zone") == "NORTH").sort("date")
shares = px.zone_share_daily(daily)
scan = px.step_scan(shares.filter(pl.col("weather_zone") == "NORTH"), "share", window=56,
                    start=date(2008, 3, 1), end=date(2009, 12, 31))
top = scan.sort(pl.col("step").abs(), descending=True).head(3)
print("NORTH share of ERCOT daily mean load, largest 56-day steps 2008-03..2009-12:", top)
step_day = top["date"][0]
moves = []
for zone in wl.WEATHER_ZONES:
    s_ = px.step_scan(shares.filter(pl.col("weather_zone") == zone), "share", window=56, start=step_day, end=step_day)
    d_ = px.step_scan(daily.filter(pl.col("weather_zone") == zone), "dmean", window=56, start=step_day, end=step_day)
    moves.append({"zone": zone, "share_step_pp": 100 * s_["step"].item(), "mw_step": d_["step"].item()})
print(f"56-day steps on {step_day} (share in percentage points, MW):",
      pl.DataFrame(moves).with_columns(pl.selectors.float().round(2)))
print("NORTH daily min/max/mean around the step:")
print(north.filter(pl.col("date").is_between(date.fromordinal(step_day.toordinal() - 5),
                                             date.fromordinal(step_day.toordinal() + 5)))
      .select("date", "dmin", "dmax", "dmean").with_columns(pl.selectors.float().round(0)))
yoy_all = px.monthly_min_yoy(daily)
print("NORTH monthly YoY 2008-10..2010-03:")
print(yoy_all.filter((pl.col("weather_zone") == "NORTH") & pl.col("month").is_between(date(2008, 10, 1),
                                                                                        date(2010, 3, 1)))
      .select("month", "dmin_mean", "dmin_yoy", "dmax_mean", "dmax_yoy").with_columns(pl.selectors.float().round(0)))

yoy = px.monthly_min_yoy(daily)
for zone in ("NORTH", "WEST", "FWEST"):
    sub = yoy.filter((pl.col("weather_zone") == zone) & (pl.col("month") >= date(2021, 1, 1)))
    print(zone, "monthly mean daily min / max and YoY change (MW):")
    print(sub.select("month", "dmin_mean", "dmin_yoy", "dmax_mean", "dmax_yoy").with_columns(
        pl.selectors.float().round(0)))

# NORTH 2022 step: daily scan.
scan22 = px.step_scan(north, "dmin", window=28, start=date(2021, 6, 1), end=date(2023, 6, 30))
print("NORTH largest 28-day steps in the daily MIN 2021-06..2023-06:",
      scan22.sort(pl.col("step").abs(), descending=True).head(5))
west = daily.filter(pl.col("weather_zone") == "WEST").sort("date")
scanw = px.step_scan(west, "dmin", window=28, start=date(2025, 1, 1), end=date(2026, 8, 31))
print("WEST largest 28-day steps in the daily MIN 2025-01..2026-08:",
      scanw.sort(pl.col("step").abs(), descending=True).head(8))

# WEST 2026 shape: hourly profile, Jul-Sep 2026 vs. Jul-Sep 2025 (a flat block moves every hour the same)
def profile(zone: str, year: int) -> pl.DataFrame:
    return (load.filter((pl.col("weather_zone") == zone) & (pl.col("operating_date") >= date(year, 7, 1))
                        & (pl.col("operating_date") <= date(year, 9, 20)))
            .group_by("hour_ending").agg(pl.col("mw").mean().alias(str(year))))


for zone in ("WEST", "NORTH", "FWEST"):
    prof = profile(zone, 2025).join(profile(zone, 2026), on="hour_ending").sort("hour_ending").with_columns(
        (pl.col("2026") - pl.col("2025")).alias("diff"))
    d = prof["diff"]
    print(f"{zone} Jul 1-Sep 20 hourly profile 2026 - 2025: min {d.min():.0f}, max {d.max():.0f}, "
          f"mean {d.mean():.0f} MW; HE of max diff {prof['hour_ending'][d.arg_max()]}")

# TCEQ data-center permits by weather zone (a source in the DB; it shows sites, not MW)
cw = read_sql("SELECT county_fips, weather_zone FROM county_weather_zone WHERE in_ercot")
dc = read_sql("SELECT reg_ent_name, county_name, county_fips, first_affil_begin_dt, has_active, has_pending "
              "FROM tceq_data_center_sites")
dcz = dc.join(cw, on="county_fips", how="left")
print(dcz.filter(pl.col("weather_zone").is_in(["NORTH", "WEST"])).sort("weather_zone", "first_affil_begin_dt"))

# %% 4. Forecast split: organic (pre-break) + large-load layer, and what is left
split_rows = []
for y in (2023, 2024, 2025, 2026):
    e = ex_peak.filter((pl.col("weather_zone") == wl.TOTAL) & (pl.col("year") == y)).row(0, named=True)
    c = cmp.filter(pl.col("year") == y).row(0, named=True)
    sim = c["deck_simultaneous_mw"]
    split_rows.append({
        "year": y, "actual_mw": e["actual_mw"], "organic_prebreak_mw": e["predicted_mw"],
        "excess_mw": e["excess_mw"], "deck_sim_mw": sim,
        "residual_after_ll_mw": None if sim is None else e["excess_mw"] - sim,
        "flat_block_mw": c["ercot_flat_block_mw"],
    })
split = pl.DataFrame(split_rows)
print(split.with_columns(pl.selectors.float().round(0)))

# Observed-at-peak factor: deck simultaneous (and non-simultaneous) peak / approved-to-energize stock, peak months.
fac = cmp.filter(pl.col("year") <= 2025).with_columns(
    (pl.col("deck_simultaneous_mw") / pl.col("deck_a2e_mw")).alias("sim_over_a2e"),
    (pl.col("deck_non_simultaneous_mw") / pl.col("deck_a2e_mw")).alias("nonsim_over_a2e"))
print(fac.select("year", "peak_month", "deck_simultaneous_mw", "deck_non_simultaneous_mw", "deck_a2e_mw",
                 "sim_over_a2e", "nonsim_over_a2e").with_columns(pl.selectors.float().round(3)))
sim_factor = fac["sim_over_a2e"].mean()
u_mean = split.filter(pl.col("year") <= 2025)["residual_after_ll_mw"].mean()
row26 = split.filter(pl.col("year") == 2026).row(0, named=True)
a2e_26 = cmp.filter(pl.col("year") == 2026)["deck_a2e_mw"].item()
proposed_26 = row26["organic_prebreak_mw"] + sim_factor * a2e_26 + u_mean
print(f"sim/a2e factor 2023-25 mean {sim_factor:.3f}; unattributed excess U mean 2023-25 {u_mean:.0f} MW; "
      f"2026 = organic {row26['organic_prebreak_mw']:.0f} + LL {sim_factor * a2e_26:.0f} (A2E {a2e_26:.0f}) + "
      f"U {u_mean:.0f} = {proposed_26:.0f} vs actual {row26['actual_mw']:.0f} "
      f"({100 * (proposed_26 / row26['actual_mw'] - 1):+.1f}%)")

# Rolling-origin variant: organic = the Q7 full-sample fit (<= 2025) already absorbs part of it.
full_fit = wl.fit_peak(panel.filter((pl.col("weather_zone") == wl.TOTAL) & (pl.col("year") <= 2025)), FEATURE)
y26 = panel.filter((pl.col("weather_zone") == wl.TOTAL) & (pl.col("year") == 2026))
pre_fit = wl.fit_peak(panel.filter((pl.col("weather_zone") == wl.TOTAL) & (pl.col("year") <= TRAIN_UNTIL)), FEATURE)
print(f"ERCOT trend: fit<=2019 {pre_fit.coef_of('year'):.0f} MW/yr, fit<=2025 {full_fit.coef_of('year'):.0f} MW/yr; "
      f"2026 prediction fit<=2025 {full_fit.predict([2026], y26[FEATURE].to_list())[0]:.0f}, "
      f"fit<=2019 {pre_fit.predict([2026], y26[FEATURE].to_list())[0]:.0f}")
for yr in (2026, 2028, 2030):
    gap = (full_fit.predict([yr], y26[FEATURE].to_list())[0] - pre_fit.predict([yr], y26[FEATURE].to_list())[0])
    print(f"  full-sample minus pre-break organic at {yr} (2026 weather): {gap:.0f} MW = double count if the whole "
          f"large-load layer is stacked on the full-sample model")
q7_plus_ll = full_fit.predict([2026], y26[FEATURE].to_list())[0] + sim_factor * a2e_26
print(f"Q7 full-sample 2026 + LL = {q7_plus_ll:.0f} ({100 * (q7_plus_ll / row26['actual_mw'] - 1):+.1f}%)")

# %% 5. Figures
ACTUAL, PRED, LL, FLAT = "#52514e", "#2a78d6", "#eb6834", "#1f9e6e"
fig, axes = plt.subplots(3, 3, figsize=(13, 10), sharex=True)
for ax, zone in zip(axes.flat, SERIES, strict=True):
    sub = ex_peak.filter(pl.col("weather_zone") == zone).sort("year")
    ax.plot(sub["year"], sub["actual_mw"] / 1000, color=ACTUAL, lw=2, marker="o", ms=3, label="actual peak")
    ax.plot(sub["year"], sub["predicted_mw"] / 1000, color=PRED, lw=2, label="pre-break model (fit ≤ 2019)")
    ax.axvline(TRAIN_UNTIL + 0.5, color="#b8b7b2", lw=1, ls="--")
    last = sub.filter(pl.col("year") == 2026)
    ax.set_title(f"{zone}  2026 excess {last['excess_mw'].item()/1000:+.2f} GW ({last['excess_pct'].item():+.0f}%)",
                 fontsize=10, loc="left")
    ax.grid(axis="y", color="#e5e4df", lw=0.8)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
axes.flat[0].legend(fontsize=8, frameon=False)
fig.supylabel("summer peak, GW")
fig.suptitle("X1 — summer peak vs. the pre-break (≤ 2019) weather + trend model, per weather zone", x=0.01,
             ha="left", fontsize=12)
fig.tight_layout()
fig.savefig(out_path("x1_excess_by_zone.png"), dpi=130)

fig, ax = plt.subplots(figsize=(10, 5))
e = ex_peak.filter((pl.col("weather_zone") == wl.TOTAL) & (pl.col("year") >= 2015)).sort("year")
m = ex_min.filter((pl.col("weather_zone") == wl.TOTAL) & (pl.col("year") >= 2015)).sort("year")
ax.bar(e["year"] - 0.2, e["excess_mw"] / 1000, width=0.4, color=PRED, label="system peak excess")
ax.bar(m["year"] + 0.2, m["excess_mw"] / 1000, width=0.4, color=FLAT, label="summer mean daily-min excess")
ds = deck_summer.filter(pl.col("year") >= 2023)
ax.plot(ds["year"], ds["sim_summer_mean"] / 1000, color=LL, marker="o", lw=2,
        label="deck: observed simultaneous peak of approved large loads (Jun–Sep mean; not verified)")
ax.axhline(0, color="#52514e", lw=0.8)
ax.set_ylabel("GW above the pre-break model")
ax.set_title("ERCOT: excess over weather + trend (fit ≤ 2019) vs. large loads in the ERCOT decks", loc="left",
             fontsize=11)
ax.legend(fontsize=8, frameon=False, loc="upper left")
ax.grid(axis="y", color="#e5e4df", lw=0.8)
for side in ("top", "right"):
    ax.spines[side].set_visible(False)
fig.tight_layout()
fig.savefig(out_path("x1_ercot_excess_vs_decks.png"), dpi=130)

fig, axes = plt.subplots(1, 3, figsize=(14, 4.2), sharey=False)
for ax, zone in zip(axes, ("NORTH", "WEST", "FWEST"), strict=True):
    sub = yoy.filter((pl.col("weather_zone") == zone) & (pl.col("month") >= date(2019, 1, 1))).sort("month")
    ax.plot(sub["month"], sub["dmin_mean"], color=FLAT, lw=1.8, label="monthly mean daily min")
    ax.plot(sub["month"], sub["dmax_mean"], color=ACTUAL, lw=1.2, label="monthly mean daily max")
    ax.set_title(zone, loc="left", fontsize=10)
    ax.grid(axis="y", color="#e5e4df", lw=0.8)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
axes[0].legend(fontsize=8, frameon=False)
axes[0].set_ylabel("MW")
fig.suptitle("Daily min and max by month, 2019–2026: a flat block lifts both lines together", x=0.01, ha="left",
             fontsize=11)
fig.tight_layout()
fig.savefig(out_path("x1_min_max_by_month.png"), dpi=130)

for name, frame in {"x1_excess_peak.csv": ex_peak, "x1_excess_coincident.csv": ex_coinc,
                    "x1_shape_excess.csv": shape_ex, "x1_deck_vs_excess.csv": cmp,
                    "x1_energized_by_month.csv": energized}.items():
    frame.write_csv(out_path(name))
print("figures and tables written to analysis/out/x1_*")
