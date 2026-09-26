"""Q1 of phase 0: how far off are ERCOT's official summer peak forecasts? (``docs/PHASE0_ANALYSIS.md`` §2)

Writes the tables to ``analysis/out/q1_*.csv``, one figure to ``analysis/out/q1_backtest.png``, and prints the
numbers that go to ``docs/analysis/q1_backtest.md``.
"""

# %% Setup
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import polars as pl  # noqa: E402
from _common import out_path, read_sql  # noqa: E402

from basecast_pipelines.models import backtest as bt  # noqa: E402

# %% 1. Metric catalog: distinct (product, metric, scenario, region_type) with counts
catalog = read_sql(
    "select product, metric, scenario, region_type, count(*) as n, count(distinct vintage) as vintages, "
    "min(target_year) as first_target, max(target_year) as last_target "
    "from official_forecasts group by 1, 2, 3, 4 order by 1, 2, 4, 3"
)
catalog.write_csv(out_path("q1_metric_catalog.csv"))
# Weather-year scenarios (one per historical weather year) collapse into one pattern for reading.
compact = (
    catalog.with_columns(
        pl.col("scenario").str.replace(r"weather_year_\d{4}", "weather_year_YYYY").alias("scenario"),
        pl.col("scenario").str.contains(r"weather_year_\d{4}").fill_null(False).alias("_wy"),
    )
    .group_by("product", "metric", "scenario", "region_type", maintain_order=True)
    .agg(pl.col("n").sum(), pl.col("vintages").max(), pl.col("first_target").min(), pl.col("last_target").max(),
         pl.len().alias("variants"))
)
print(f"catalog: {catalog.height} distinct (product, metric, scenario, region_type); {compact.height} compacted")
print(compact)

# %% 2. Actual summer peak (Jun-Sep) per year: hourly (the forecasts' definition) and 15-minute
monthly = read_sql(
    "select report_year, month, region_type, metric, value, peak_local, peak_ts_utc, final_settlement "
    "from ercot_monthly_peaks where region_type = 'ercot' and metric in ('peak_hourly_mw', 'peak_15min_mw')"
)
actual_h = bt.summer_peaks(monthly, metric="peak_hourly_mw")
actual_15 = bt.summer_peaks(monthly, metric="peak_15min_mw")
actual_h.write_csv(out_path("q1_actual_summer_peaks.csv"))
print(actual_h.join(actual_15.select("year", pl.col("actual_mw").alias("actual_15min_mw")), on="year"))

# 2026: the workbook has June-August only; the hourly weather-zone load covers September up to today.
hourly = read_sql(
    "select ts_utc, hour_ending_local, weather_zone, mw, source from ercot_load_hourly_wz "
    "where weather_zone = 'ERCOT' and hour_ending_local >= '2026-06-01'"
)
check_2026 = bt.hourly_summer_peaks(hourly)
print(check_2026)
sept_2026 = hourly.filter(pl.col("hour_ending_local").dt.month() == 9).sort("mw", descending=True).head(1)
print("Sept 2026 max so far:", sept_2026)
source_2026 = read_sql(
    "select distinct source_file, ingested_at from ercot_monthly_peaks "
    "where report_year = 2026 and region_type = 'ercot'"
)
print(source_2026)

# %% 3. Error of each vintage x target year and summary by horizon
forecasts = bt.load_official_forecasts()
base = bt.base_series(forecasts)
errors = bt.match_forecasts(base, actual_h)
errors.write_csv(out_path("q1_errors.csv"))
print(base.group_by("product", "series").agg(pl.len(), pl.col("vintage").n_unique().alias("vintages")))
print(errors.select("product", "vintage", "vintage_date", "target_year", "horizon", "forecast_mw", "actual_mw",
                    "error_pct"))

summary = bt.horizon_summary(errors.filter(pl.col("product") != bt.PRELIM))
summary.write_csv(out_path("q1_horizon_summary.csv"))
print(summary)
for share in (0.5 + 1e-9, 2 / 3):
    print(f"min_share={share:.2f}")
    print(bt.sign_verdict(summary, min_share=share))

# Same summary on the large-load era only (targets 2023+), where the narrative lives.
recent = bt.horizon_summary(errors.filter((pl.col("product") != bt.PRELIM) & (pl.col("target_year") >= 2023)))
print(recent)

# Pooled over horizons 1-5, per product and per target year.
pooled = (
    errors.filter(pl.col("horizon").is_between(1, 5))
    .group_by("product")
    .agg(pl.len().alias("n"), pl.col("error_pct").mean().alias("mean_pct"),
         pl.col("error_pct").median().alias("median_pct"), (pl.col("error_pct") > 0).mean().alias("share_over"))
    .sort("product")
)
print(pooled)
by_target = (
    errors.filter(pl.col("horizon").is_between(1, 5) & (pl.col("product") != bt.PRELIM))
    .group_by("product", "target_year")
    .agg(pl.len().alias("n"), pl.col("error_pct").mean().alias("mean_pct"),
         pl.col("error_pct").min().alias("min_pct"), pl.col("error_pct").max().alias("max_pct"))
    .sort("product", "target_year")
)
print(by_target)

# %% 4. The 2026 preliminary LTLF (~112 GW) against ERCOT's own 90.5-98 GW range and the actual 2026 peak
prelim = forecasts.filter((pl.col("metric") == "peak_demand") & (pl.col("target_year") == 2026))
actual_2026 = actual_h.filter(pl.col("year") == 2026).row(0, named=True)
actual_2026_15 = actual_15.filter(pl.col("year") == 2026).row(0, named=True)
comparison = prelim.select("vintage", "vintage_date", "scenario", "value").with_columns(
    pl.lit(actual_2026["actual_mw"]).alias("actual_hourly_mw"),
    (pl.col("value") - actual_2026["actual_mw"]).alias("diff_mw"),
    ((pl.col("value") - actual_2026["actual_mw"]) / actual_2026["actual_mw"] * 100).alias("diff_pct"),
    (pl.col("value") - actual_2026_15["actual_mw"]).alias("diff_vs_15min_mw"),
)
print(comparison)
other_2026 = errors.filter(pl.col("target_year") == 2026).select(
    "product", "vintage", "vintage_date", "horizon", "forecast_mw", "error_mw", "error_pct"
)
print(other_2026)
# Sensitivity: LTLF 2025 with the TSP-provided large loads instead of ERCOT's adjustment (not a base series).
tsp = forecasts.filter((pl.col("vintage") == "LTLF 2025") & (pl.col("season") == "summer")
                       & (pl.col("scenario") == "tsp_provided"))
tsp_errors = bt.match_forecasts(
    tsp.select(pl.lit("LTLF tsp_provided").alias("product"), "vintage", "vintage_date", "target_year",
               pl.col("value").alias("forecast_mw"), pl.lit("summer peak_mw tsp_provided").alias("series")),
    actual_h,
)
print(tsp_errors.select("vintage", "target_year", "horizon", "forecast_mw", "actual_mw", "error_mw", "error_pct"))

# %% 5. Figure: every LTLF/CDR vintage against the actual summer peak (the backtest screen's main chart)
fig, (ax, ax2) = plt.subplots(1, 2, figsize=(13, 5), gridspec_kw={"width_ratios": [3, 2]})
ltlf = base.filter((pl.col("product") == "LTLF") & pl.col("target_year").is_between(2014, 2030))
vintages = ltlf["vintage"].unique(maintain_order=True).to_list()
cmap = plt.get_cmap("viridis", len(vintages))
for i, v in enumerate(vintages):
    s = ltlf.filter(pl.col("vintage") == v).sort("target_year")
    ax.plot(s["target_year"], s["forecast_mw"] / 1000, color=cmap(i), lw=1.2, label=v)
tsp_line = tsp.filter(pl.col("target_year") <= 2030).sort("target_year")
ax.plot(tsp_line["target_year"], tsp_line["value"] / 1000, color="tab:red", ls="--", lw=1.2,
        label="LTLF 2025 TSP-provided")
act = actual_h.filter(pl.col("year") >= 2014)
ax.plot(act["year"], act["actual_mw"] / 1000, color="black", lw=2.5, marker="o", ms=4, label="Actual (hourly)")
ax.errorbar([2026], [94.25], yerr=[[3.75], [3.75]], fmt="none", ecolor="tab:red", capsize=6, lw=2,
            label="ERCOT range 90.5-98 GW (Apr 2026)")
ax.scatter([2026], [112], color="tab:red", marker="*", s=160, zorder=5, label="2026 preliminary LTLF ~112 GW")
ax.set_xlabel("Target summer")
ax.set_ylabel("Summer peak, GW")
ax.set_title("LTLF vintages vs actual ERCOT summer peak")
ax.set_ylim(60, 145)
ax.legend(fontsize=7, ncol=2, loc="upper left")
ax.grid(alpha=0.3)

for product, color in (("LTLF", "tab:blue"), ("CDR", "tab:orange")):
    s = summary.filter((pl.col("product") == product) & pl.col("horizon").is_between(1, 6))
    offset = -0.15 if product == "LTLF" else 0.15
    ax2.bar(s["horizon"] + offset, s["median_pct"], width=0.3, color=color, label=f"{product} median")
    ax2.vlines(s["horizon"] + offset, s["min_pct"], s["max_pct"], color=color, lw=1)
ax2.axhline(0, color="black", lw=0.8)
ax2.set_xlabel("Horizon (summers ahead)")
ax2.set_ylabel("Error, % of actual (forecast - actual)")
ax2.set_title("Error by horizon (median, min-max)")
ax2.legend(fontsize=8)
ax2.grid(alpha=0.3)
fig.tight_layout()
fig.savefig(out_path("q1_backtest.png"), dpi=130)
print("figure:", out_path("q1_backtest.png"))
