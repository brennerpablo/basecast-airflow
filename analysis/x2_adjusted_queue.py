"""X2: the adjusted generation queue by county (raw MW vs expected MW reaching COD by a date).

Scores every active project in ERCOT's latest GIS report with its stage's conditional CIF of COD (q6's
Aalen–Johansen, MW-weighted, from the stage it has reached and the months it has already spent there), sums
by county / zone / fuel, and backtests the same procedure from past report months. Model code:
``basecast_pipelines/models/queue_adjusted.py`` (on top of ``models/survival.py``, unchanged).

Run from the repo root: ``uv run --group analysis python analysis/x2_adjusted_queue.py``. Numbers go to
``docs/analysis/x2_adjusted_queue.md``; CSVs and figures to ``analysis/out/x2_*``.
"""

# %% setup
from __future__ import annotations

import re
from datetime import date

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from _common import out_path, read_sql
from scipy.stats import spearmanr

from basecast_pipelines.models import queue_adjusted as qa
from basecast_pipelines.models import survival as sv

events_all = sv.load_events(cohort_start=None).with_columns(stratum=qa.stratum())
cohort = events_all.filter(pl.col("first_seen_month") >= pl.lit(sv.COHORT_START))
geo = qa.load_geography()
LATEST = events_all["latest_report_month"].max()
AS_OF = date(LATEST.year + (LATEST.month == 12), LATEST.month % 12 + 1, 1)  # end of the latest report month
HORIZONS = {"2027": date(2028, 1, 1), "2028": date(2029, 1, 1)}  # COD by end of Dec 2027 / Dec 2028
H_MONTHS = {k: qa.months_between(v, AS_OF) for k, v in HORIZONS.items()}
print(f"latest report {LATEST}, as of {AS_OF}; horizons (months): { {k: round(v, 2) for k, v in H_MONTHS.items()} }")
print(f"all INRs {events_all.height}, training cohort (first listed >= {sv.COHORT_START}) {cohort.height}")


def add_month(d: date, n: int) -> date:
    m = d.month - 1 + n
    return date(d.year + m // 12, m % 12 + 1, 1)


def with_geo(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(county_key=qa.norm_county("county")).join(
        geo.select("county_key", "county_fips", "county_name", "weather_zone"), on="county_key", how="left"
    )


# %% Q1. the active queue today, by stage x fuel, and the top 20 counties by raw MW
STAGES_ALL = qa.STAGE_SETS["entry_fis_ia_sync"]
snap = qa.load_snapshot(LATEST)
today = with_geo(qa.build_queue(snap, events_all, AS_OF, STAGES_ALL))
print(f"active projects {today.height}, {today['capacity_mw'].sum():,.0f} MW; "
      f"null capacity {today['capacity_mw'].is_null().sum()}; county match {today['county_fips'].is_not_null().mean():.1%}; "
      f"weather zone null (outside ERCOT zones) {today['weather_zone'].is_null().sum()}")
inactive = snap.filter(pl.col("status") == "inactive")
print(f"inactive (listed, not in the raw queue): {inactive.height} projects, {inactive['capacity_mw'].sum():,.0f} MW")

stage_fuel = (
    today.group_by("stage", "stratum").agg(n=pl.len(), mw=pl.col("capacity_mw").sum())
    .pivot(on="stratum", index="stage", values=["n", "mw"]).fill_null(0)
)
print(stage_fuel.with_columns(pl.col("^mw_.*$").round(0)))
print(today.group_by("stage").agg(n=pl.len(), mw=pl.col("capacity_mw").sum().round(0),
      med_elapsed=pl.col("elapsed").median().round(1)).sort("mw", descending=True))
print(today.group_by("stratum").agg(n=pl.len(), mw=pl.col("capacity_mw").sum().round(0)).sort("mw", descending=True))
top_raw = (
    today.group_by("county_fips", "county_name").agg(n=pl.len(), raw_mw=pl.col("capacity_mw").sum())
    .sort("raw_mw", descending=True)
)
print("counties with an active project:", top_raw.height)
print(top_raw.head(20).with_columns(pl.col("raw_mw").round(0)))

# %% Q3. backtest from past report months: fit on what was known then, predict MW reaching COD in 24 months
BACKTEST_MONTHS = [date(2022, 6, 1), date(2023, 1, 1), date(2024, 8, 1)]
WINDOW = 24
VARIANTS = [(s, w) for s in ("entry_ia", "entry_fis_ia", "entry_fis_ia_sync", "entry_ia_sm", "entry_ia_sync_sm")
            for w in ("capacity_mw", None)]


def variant_name(stages: str, weight: str | None) -> str:
    return f"{stages}|{'mw' if weight else 'count'}"


bt_rows, bt_projects = [], []
for rm in BACKTEST_MONTHS:
    as_of = add_month(rm, 1)
    until = add_month(as_of, WINDOW)
    train = qa.truncate_at(cohort, as_of)
    snap_then = qa.load_snapshot(rm)
    for stages, weight in VARIANTS:
        st = qa.STAGE_SETS[stages]
        q = qa.build_queue(snap_then, events_all, as_of, st).filter(pl.col("capacity_mw") > 0)
        curves = qa.fit_stage_curves(train, qa.fit_stages(stages), weight=weight)
        scored = qa.score(q, curves, {"w": qa.months_between(until, as_of)}).with_columns(
            actual=qa.actual_cod_mw(until),
            projected=pl.when(pl.col("projected_cod") < pl.lit(until)).then(pl.col("capacity_mw")).otherwise(0.0),
        )
        scored = with_geo(scored)
        bt_projects.append(scored.with_columns(report_month=pl.lit(rm), variant=pl.lit(variant_name(stages, weight))))
cols = ["report_month", "variant", "inr", "county_fips", "stratum", "stage", "curve", "elapsed", "capacity_mw",
        "p_w", "mw_w", "clamped_w", "actual", "projected", "cod_date", "projected_cod"]
bt = pl.concat([b.select(cols) for b in bt_projects])
bt.write_csv(out_path("x2_backtest_projects.csv"))

# statewide: predicted vs actual, plus the two baselines (raw queue, developer-projected COD in the window)
state = (
    bt.group_by("report_month", "variant")
    .agg(n=pl.len(), raw=pl.col("capacity_mw").sum(), pred=pl.col("mw_w").sum(), actual=pl.col("actual").sum(),
         projected=pl.col("projected").sum(), cod_before_asof=(pl.col("cod_date") < pl.col("report_month").dt.offset_by("1mo")).sum())
    .with_columns(err=(pl.col("pred") / pl.col("actual") - 1), proj_err=(pl.col("projected") / pl.col("actual") - 1),
                  raw_err=(pl.col("raw") / pl.col("actual") - 1))
    .sort("report_month", "variant")
)
print(state.with_columns(pl.col("raw", "pred", "actual", "projected").round(0), pl.col("^.*err$").round(3)))
summary = state.group_by("variant").agg(
    mean_abs_err=pl.col("err").abs().mean(), mean_err=pl.col("err").mean(), max_abs_err=pl.col("err").abs().max()
).sort("mean_abs_err")
print(summary.with_columns(pl.col("^.*err$").round(3)))

# %% Q3b. by fuel and by stage, and the county-level map test (does adjusted rank counties better than raw?)
by_fuel = (
    bt.group_by("report_month", "variant", "stratum")
    .agg(pred=pl.col("mw_w").sum(), actual=pl.col("actual").sum(), projected=pl.col("projected").sum(),
         raw=pl.col("capacity_mw").sum())
    .with_columns(err=pl.col("pred") / pl.col("actual") - 1)
    .sort("report_month", "variant", "stratum")
)
by_stage = (
    bt.group_by("report_month", "variant", "stage")
    .agg(n=pl.len(), pred=pl.col("mw_w").sum(), actual=pl.col("actual").sum(), clamped=pl.col("clamped_w").sum())
    .with_columns(err=pl.col("pred") / pl.col("actual") - 1)
    .sort("report_month", "variant", "stage")
)


def county_scores(df: pl.DataFrame) -> dict[str, float]:
    c = df.group_by("county_fips").agg(pred=pl.col("mw_w").sum(), actual=pl.col("actual").sum(),
                                       raw=pl.col("capacity_mw").sum(), projected=pl.col("projected").sum())
    a = c["actual"].to_numpy()
    out = {"counties": c.height, "counties_with_cod": int((a > 0).sum())}
    for k in ("pred", "raw", "projected"):
        v = c[k].to_numpy()
        # raw MW is not a forecast of COD MW: scale it to the actual total so WAPE compares shapes only
        scaled = v * a.sum() / v.sum() if v.sum() > 0 else v
        out[f"spearman_{k}"] = float(spearmanr(v, a).statistic)
        out[f"wape_{k}_scaled"] = float(np.abs(scaled - a).sum() / a.sum())
    out["wape_pred"] = float(np.abs(c["pred"].to_numpy() - a).sum() / a.sum())
    return out


county_bt = pl.DataFrame([
    {"report_month": k[0], "variant": k[1], **county_scores(g)} for k, g in bt.group_by("report_month", "variant")
]).sort("report_month", "variant")
print(county_bt.with_columns(pl.col(pl.Float64).round(3)))

# %% Q3c. pick the primary variant: lowest mean absolute statewide error, ties -> the simpler stage set
PRIMARY = summary["variant"][0]
print("primary variant by the backtest:", PRIMARY)
pv = pl.col("variant") == PRIMARY
print(by_fuel.filter(pv).with_columns(pl.col("pred", "actual", "projected", "raw").round(0), pl.col("err").round(3)))
print(by_stage.filter(pl.col("variant").is_in(summary["variant"].head(4).to_list()))
      .with_columns(pl.col("pred", "actual").round(0), pl.col("err").round(3)))
fuel_err = by_fuel.filter(pv).group_by("stratum").agg(
    pred=pl.col("pred").sum(), actual=pl.col("actual").sum(), mean_err=pl.col("err").mean(),
    mean_abs_err=pl.col("err").abs().mean()).with_columns(pooled_err=pl.col("pred") / pl.col("actual") - 1)
print(fuel_err.with_columns(pl.col("pred", "actual").round(0), pl.col("^.*err$").round(3)))

# stochastic-only band (independent Bernoulli per project): too narrow by construction, reported for contrast
for rm in BACKTEST_MONTHS:
    g = bt.filter(pv & (pl.col("report_month") == rm))
    sd = float(np.sqrt((g["capacity_mw"] ** 2 * g["p_w"] * (1 - g["p_w"])).sum()))
    print(f"{rm}: pred {g['mw_w'].sum():,.0f} MW, Bernoulli sd {sd:,.0f} MW, actual {g['actual'].sum():,.0f} MW, "
          f"z = {(g['actual'].sum() - g['mw_w'].sum()) / sd:+.1f}")

# synchronized-stage diagnostic: why does a "synchronized" stage overpredict? (outcome of those projects)
sync_bt = bt.filter((pl.col("variant") == "entry_ia_sync_sm|mw") & (pl.col("stage") == "synchronized"))
print(sync_bt.with_columns(hit=pl.col("actual") > 0).group_by("report_month", "hit").agg(
    n=pl.len(), mw=pl.col("capacity_mw").sum().round(0), med_elapsed=pl.col("elapsed").median().round(1),
    med_p=pl.col("p_w").median().round(2)).sort("report_month", "hit"))
miss = sync_bt.filter(pl.col("actual") == 0).join(events_all.select("inr", "exit_status", "commercial_operation_date"), on="inr")
print("synchronized at the as-of date but no COD within 24 months, by exit status:",
      miss.group_by("report_month", "exit_status").len().sort("report_month", "exit_status").rows())

# %% Q2. today's adjusted queue: conditional expected MW reaching COD by Dec 2027 / Dec 2028
STAGE_SET = PRIMARY.split("|")[0]
WEIGHT = "capacity_mw" if PRIMARY.endswith("|mw") else None
curves_today = qa.fit_stage_curves(qa.truncate_at(cohort, AS_OF), qa.fit_stages(STAGE_SET), weight=WEIGHT)
for (stage, name), c in sorted(curves_today.items()):
    print(f"curve {stage:>9} {name:>9}: n={c.n}, events={c.cod}, supported to {c.end:.1f} mo")
queue = with_geo(qa.build_queue(snap, events_all, AS_OF, qa.STAGE_SETS[STAGE_SET]))
scored = qa.score(queue, curves_today, H_MONTHS)
H = list(H_MONTHS)
tot = scored.select(raw=pl.col("capacity_mw").sum(), **{f"adj_{h}": pl.col(f"mw_{h}").sum() for h in H})
print("statewide:", {k: round(v, 0) for k, v in tot.row(0, named=True).items()},
      {f"ratio_{h}": round(tot[f"adj_{h}"].item() / tot["raw"].item(), 4) for h in H})
print("clamped share:", {h: round(scored[f"clamped_{h}"].mean(), 3) for h in H})
for by in ("stage", "stratum", "cdr_reporting_zone", "weather_zone"):
    print(qa.aggregate(scored, by, H).with_columns(pl.col("^raw_mw|adj_mw_.*$").round(0), pl.col("^ratio_.*$").round(4)))
print(qa.aggregate(scored, ["stage", "stratum"], H).sort("stage", "stratum")
      .with_columns(pl.col("^raw_mw|adj_mw_.*$").round(0), pl.col("^ratio_.*$").round(4)))

county = qa.aggregate(scored, ["county_fips", "county_name", "weather_zone"], H).with_columns(
    rank_raw=pl.col("raw_mw").rank("ordinal", descending=True),
    rank_adj=pl.col("adj_mw_2028").rank("ordinal", descending=True),
)
fuel_cols = scored.pivot(on="stratum", index="county_fips", values="mw_2028", aggregate_function="sum").fill_null(0)
fuel_cols = fuel_cols.rename({c: f"adj_mw_2028_{c}" for c in fuel_cols.columns if c != "county_fips"})
county = county.join(fuel_cols, on="county_fips", how="left")
county.write_csv(out_path("x2_county_adjusted.csv"))
show = ["county_name", "weather_zone", "projects", "raw_mw", "adj_mw_2027", "adj_mw_2028", "ratio_2028", "rank_raw", "rank_adj"]
rnd = [pl.col("raw_mw", "adj_mw_2027", "adj_mw_2028").round(0), pl.col("ratio_2028").round(3)]
print("top 20 by raw MW:\n", county.sort("raw_mw", descending=True).head(20).select(show).with_columns(rnd))
print("top 20 by adjusted MW (Dec 2028):\n", county.sort("adj_mw_2028", descending=True).head(20).select(show).with_columns(rnd))
print("big raw, little adjusted (raw >= 4,000 MW, lowest ratio):\n",
      county.filter(pl.col("raw_mw") >= 4000).sort("ratio_2028").head(10).select(show).with_columns(rnd))
print("punching above the raw queue (adjusted >= 300 MW, highest ratio):\n",
      county.filter(pl.col("adj_mw_2028") >= 300).sort("ratio_2028", descending=True).head(10).select(show).with_columns(rnd))
print("largest rank gains / losses among the top 40 of either list:")
top40 = county.filter((pl.col("rank_raw") <= 40) | (pl.col("rank_adj") <= 40)).with_columns(
    gain=pl.col("rank_raw").cast(pl.Int64) - pl.col("rank_adj").cast(pl.Int64))
print(top40.sort("gain", descending=True).head(8).select(*show, "gain").with_columns(rnd))
print(top40.sort("gain").head(8).select(*show, "gain").with_columns(rnd))
top20_raw = set(county.sort("raw_mw", descending=True).head(20)["county_fips"])
top20_adj = set(county.sort("adj_mw_2028", descending=True).head(20)["county_fips"])
print(f"top-20 overlap raw vs adjusted: {len(top20_raw & top20_adj)} of 20; "
      f"Spearman raw vs adjusted over {county.height} counties: {spearmanr(county['raw_mw'], county['adj_mw_2028']).statistic:.3f}")
share = county.sort("adj_mw_2028", descending=True)
print(f"top 10 counties hold {share['adj_mw_2028'].head(10).sum() / share['adj_mw_2028'].sum():.1%} of adjusted MW "
      f"vs {county.sort('raw_mw', descending=True)['raw_mw'].head(10).sum() / county['raw_mw'].sum():.1%} of raw MW")
proj_cols = ["inr", "project_name", "county_fips", "county_name", "weather_zone", "cdr_reporting_zone", "fuel_type",
             "stratum", "stage", "elapsed", "capacity_mw", "projected_cod", "curve", *[f"p_{h}" for h in H],
             *[f"mw_{h}" for h in H], *[f"clamped_{h}" for h in H]]
scored.select(proj_cols).write_csv(out_path("x2_project_scores.csv"))
qa.aggregate(scored, ["cdr_reporting_zone", "stratum"], H).write_csv(out_path("x2_zone_fuel_adjusted.csv"))
print("largest adjusted projects:\n", scored.sort("mw_2028", descending=True).head(10).select(
    "project_name", "county_name", "stratum", "stage", pl.col("elapsed").round(1), "capacity_mw",
    pl.col("p_2027").round(3), pl.col("p_2028").round(3)))

# sanity: the IA-stage queue then and now, the realized COD rate, and the developers' own projected CODs
ia_hist = bt.filter(pl.col("variant") == PRIMARY).group_by("report_month").agg(
    ia_mw=pl.col("capacity_mw").filter(pl.col("stage") == "ia_signed").sum().round(0),
    ia_n=(pl.col("stage") == "ia_signed").sum(),
    actual_24m=pl.col("actual").sum().round(0)).sort("report_month")
print(ia_hist.with_columns(actual_per_year=(pl.col("actual_24m") / 2).round(0)))
ia_now = scored.filter(pl.col("stage") == "ia_signed")
print(f"today: IA-stage {ia_now.height} projects, {ia_now['capacity_mw'].sum():,.0f} MW; model COD by Dec 2027 "
      f"{tot['adj_2027'].item():,.0f} MW = {tot['adj_2027'].item() / H_MONTHS['2027'] * 12:,.0f} MW/yr")
for h, until in HORIZONS.items():
    dev = scored.filter(pl.col("projected_cod") < pl.lit(until))["capacity_mw"].sum()
    print(f"developer projected COD before {until}: {dev:,.0f} MW (model {tot[f'adj_{h}'].item():,.0f} MW)")
print("IA-stage gas+other today: median MW", ia_now.filter(pl.col("stratum") == "gas+other")["capacity_mw"].median(),
      "| in training (IA landmark, gas+other): median MW",
      qa.stage_frame(qa.truncate_at(cohort, AS_OF), "ia_signed").filter(pl.col("stratum") == "gas+other")["capacity_mw"].median())
gas_hist = cohort.filter((pl.col("stratum") == "gas+other") & pl.col("ia_signed").is_not_null())
print("gas+other with IA in the cohort, by size >= 500 MW: n, COD", gas_hist.group_by(big=pl.col("capacity_mw") >= 500).agg(
    n=pl.len(), cod=(pl.col("exit_status") == "operational").sum(),
    med_months_ia_to_cod=qa.months(pl.col("commercial_operation_date"), pl.col("ia_signed")).median().round(1)).rows())

# %% figures (default dataviz palette: slot 1 blue, slot 2 orange; one axis per chart)
BLUE, ORANGE, INK, MUTED, GRID = "#2a78d6", "#eb6834", "#1f1f1e", "#6b6a64", "#e4e3dc"
plt.rcParams.update({"font.size": 9, "axes.edgecolor": MUTED, "axes.labelcolor": INK, "xtick.color": MUTED,
                     "ytick.color": MUTED, "axes.spines.top": False, "axes.spines.right": False})
prim = by_fuel.filter(pv)
fig, axes = plt.subplots(1, len(BACKTEST_MONTHS), figsize=(12, 3.6), sharey=True)
strata = ["solar", "storage", "wind", "gas+other"]
for ax, rm in zip(axes, BACKTEST_MONTHS, strict=True):
    g = prim.filter(pl.col("report_month") == rm)
    vals = {r["stratum"]: r for r in g.iter_rows(named=True)}
    x = np.arange(len(strata))
    ax.bar(x - 0.2, [vals[s]["actual"] / 1000 for s in strata], 0.38, color=BLUE, label="reached COD (actual)")
    ax.bar(x + 0.2, [vals[s]["pred"] / 1000 for s in strata], 0.38, color=ORANGE, label="adjusted queue (predicted)")
    st = state.filter((pl.col("report_month") == rm) & pv).row(0, named=True)
    ax.set_title(f"{rm:%b %Y} queue ({st['raw'] / 1000:.0f} GW raw), next 24 months\n"
                 f"predicted {st['pred'] / 1000:.1f} vs actual {st['actual'] / 1000:.1f} GW ({st['err']:+.0%})",
                 fontsize=8.5, color=INK)
    ax.set_xticks(x, strata)
    ax.yaxis.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
axes[0].set_ylabel("GW")
axes[0].legend(frameon=False, loc="upper left")
fig.tight_layout()
fig.savefig(out_path("x2_backtest.png"), dpi=150)

fig, ax = plt.subplots(figsize=(6.5, 5))
c = county.filter(pl.col("adj_mw_2028") > 1)
ax.scatter(c["raw_mw"], c["adj_mw_2028"], s=18, color=BLUE, edgecolor="white", linewidth=0.8)
xs = np.array([c["raw_mw"].min(), c["raw_mw"].max()])
r = tot["adj_2028"].item() / tot["raw"].item()
ax.plot(xs, xs * r, color=MUTED, linestyle="--", linewidth=1)
ax.text(xs[0] * 1.3, xs[0] * r * 2.2, f"statewide ratio {r:.0%}", color=MUTED, fontsize=8, rotation=0)
label = pl.concat([c.sort("raw_mw", descending=True).head(2), c.sort("adj_mw_2028", descending=True).head(2),
                   c.filter(pl.col("raw_mw") >= 4000).sort("ratio_2028").head(3)]).unique("county_fips", maintain_order=True)
for i, row in enumerate(label.sort("adj_mw_2028").iter_rows(named=True)):
    ax.annotate(row["county_name"], (row["raw_mw"], row["adj_mw_2028"]), xytext=(-6, 4 if i % 2 else -10),
                textcoords="offset points", fontsize=7.5, color=INK, ha="right")
ax.set_xscale("log")
ax.set_yscale("log")
ax.set_xlabel("raw queue, MW (active projects, Aug 2026 report)")
ax.set_ylabel("adjusted: expected MW at COD by Dec 2028")
ax.set_title("County queues, raw vs adjusted", fontsize=10, color=INK, loc="left")
ax.grid(True, color=GRID, linewidth=0.6)
fig.tight_layout()
fig.savefig(out_path("x2_county_raw_vs_adjusted.png"), dpi=150)

# %% Q4. external check: do GIS COD projects show up as operating in EIA-860M?
eia_last = read_sql("SELECT max(report_month) AS m FROM eia860m_generators")["m"].item()
eia = read_sql(
    "SELECT plant_id_eia, plant_name, generator_id, county, county_fips, balancing_authority_code, energy_source_code, "
    "technology, nameplate_capacity_mw, operating_year, operating_month FROM eia860m_generators "
    "WHERE report_month = %(m)s AND state = 'TX' AND sheet_status = 'operating'", {"m": eia_last})
print(f"EIA-860M {eia_last}: {eia.height} operating TX generators; county_fips null {eia['county_fips'].is_null().sum()}")
print("energy_source_code:", eia.group_by("energy_source_code").len().sort("len", descending=True).head(12).rows())
EIA_FUEL = {"SUN": "solar", "WND": "wind", "MWH": "storage", "NG": "gas"}
eia_plants = (
    eia.with_columns(fuel_group=pl.col("energy_source_code").replace_strict(EIA_FUEL, default="other"))
    .group_by("plant_id_eia", "fuel_group")
    .agg(plant_name=pl.col("plant_name").first(), county_fips=pl.col("county_fips").first(),
         eia_mw=pl.col("nameplate_capacity_mw").sum(),
         eia_first_op=pl.date(pl.col("operating_year").min(), 1, 1),
         eia_op=pl.min_horizontal(pl.date(pl.col("operating_year"), pl.col("operating_month").fill_null(1), 1)).min())
)
STOP = {"SOLAR", "WIND", "STORAGE", "ENERGY", "BESS", "PROJECT", "LLC", "FARM", "POWER", "CENTER", "STATION", "PLANT",
        "PV", "I", "II", "III", "IV", "PHASE", "BATTERY", "GENERATING", "GENERATION", "RANCH", "SLR", "WIND", "ESS",
        "FACILITY", "THE", "OF", "AND", "HYBRID", "PARK", "LP", "PEAKER", "PEAKING", "UNIT", "UNITS", "CC", "CT", "GAS"}


def tokens(name: str | None) -> set[str]:
    return {t for t in re.findall(r"[A-Z]+", (name or "").upper()) if t not in STOP and len(t) > 1}


gis_cod = with_geo(
    events_all.filter((pl.col("exit_status") == "operational") & pl.col("commercial_operation_date").is_not_null()
                      & pl.col("commercial_operation_date").is_between(date(2019, 1, 1), date(2025, 12, 31)))
)
print(f"GIS projects with an ERCOT COD in 2019-2025: {gis_cod.height} ({gis_cod['capacity_mw'].sum():,.0f} MW)")
cand = gis_cod.select("inr", "project_name", "county_fips", "fuel_group", "capacity_mw", "commercial_operation_date").join(
    eia_plants, on=["county_fips", "fuel_group"], how="left")
rows = []
for r in cand.iter_rows(named=True):
    if r["plant_id_eia"] is None:
        continue
    gt, et = tokens(r["project_name"]), tokens(r["plant_name"])
    jac = len(gt & et) / len(gt | et) if gt | et else 0.0
    ratio = r["capacity_mw"] / r["eia_mw"] if r["eia_mw"] else np.inf
    rows.append({**r, "name_jaccard": jac, "shared": bool(gt & et), "cap_ratio": ratio})
pairs = pl.DataFrame(rows)
cap_ok = pl.col("cap_ratio").is_between(0.67, 1.5)
best = (
    pairs.with_columns(
        strength=pl.when(pl.col("shared") & cap_ok).then(pl.lit(2)).when(pl.col("shared")).then(pl.lit(1))
        .when(pl.col("cap_ratio").is_between(0.9, 1.1)).then(pl.lit(1)).otherwise(pl.lit(0)))
    .sort("inr", "strength", "name_jaccard", descending=[False, True, True])
    .unique("inr", keep="first")
)
match = gis_cod.select("inr", "fuel_group").join(best.select("inr", "strength", "plant_name", "project_name", "cap_ratio",
                                                            "eia_op", "commercial_operation_date"), on="inr", how="left")
match = match.with_columns(level=pl.col("strength").fill_null(0).replace_strict(
    {2: "strong (name + county + fuel + capacity)", 1: "weak (name or capacity only)", 0: "none"}))
print(match.group_by("level").len().sort("len", descending=True))
print(match.group_by("fuel_group").agg(n=pl.len(), strong=(pl.col("strength") == 2).mean().round(3),
      any_=(pl.col("strength").fill_null(0) >= 1).mean().round(3)).sort("n", descending=True))
lag = match.filter(pl.col("strength") == 2).with_columns(
    lag_m=qa.months(pl.col("eia_op"), pl.col("commercial_operation_date").dt.truncate("1mo")))
print("EIA first operating month minus ERCOT COD month (strong matches), months:",
      lag.select(med=pl.col("lag_m").median(), p10=pl.col("lag_m").quantile(0.1), p90=pl.col("lag_m").quantile(0.9),
                 within3=(pl.col("lag_m").abs() <= 3).mean()).with_columns(pl.all().round(2)).rows())
print("sample of strong matches:\n", match.filter(pl.col("strength") == 2).sample(8, seed=0).select(
    "project_name", "plant_name", "fuel_group", pl.col("cap_ratio").round(2), "commercial_operation_date", "eia_op"))
print("sample of unmatched:\n", match.filter(pl.col("strength").fill_null(0) == 0).select("inr").join(
    gis_cod, on="inr").head(10).select("project_name", "county_name", "fuel_group", "capacity_mw", "commercial_operation_date"))
match.write_csv(out_path("x2_eia860m_match.csv"))
