"""Q6: can the generation queue support a survival model? (docs/PHASE0_ANALYSIS.md §2 Q6)

Competing risks on ``gis_project_events``: COD vs withdrawal, by landmark (entry, FIS approved, IA signed)
and fuel group. The estimator and the outcome rules live in ``basecast_pipelines/models/survival.py``;
this script only calls them, cross-checks the numpy Aalen–Johansen against lifelines, and prints the
numbers that go to ``docs/analysis/q6_survival.md``.

Run from the repo root: ``uv run --group analysis python analysis/q6_survival.py``.
"""

# %% setup
from __future__ import annotations

import warnings

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from _common import out_path, read_sql
from lifelines import AalenJohansenFitter

from basecast_pipelines.models import survival as sv

events = sv.load_events()
print(f"cohort (first_seen_month >= {sv.COHORT_START}): {events.height} projects, "
      f"latest report {events['latest_report_month'].max()}")

# %% 1. cohort composition
comp = (
    events.pivot(on="exit_status", index="fuel_group", values="inr", aggregate_function="len")
    .fill_null(0)
    .with_columns(total=pl.sum_horizontal(pl.exclude("fuel_group")))
    .sort("total", descending=True)
)
cols = ["fuel_group", *[c for c in ["active", "operational", "cancelled", "inactive", "dropped"] if c in comp.columns], "total"]
print(comp.select(cols))
print("fuel_type -> fuel group:", events.group_by("fuel_type", "fuel_group").len().sort("len", descending=True).rows())
n = events.height
dropped = (events["exit_status"] == "dropped").sum()
inferred = events["exit_inferred"].sum()
print(f"dropped: {dropped} ({dropped / n:.1%}); exit_inferred: {inferred} ({inferred / n:.1%})")
vanished_inactive = events.filter(
    (pl.col("exit_status") == "inactive") & (pl.col("last_seen_month") < pl.col("latest_report_month"))
).height
print(f"inactive but gone before the latest report (treated like dropped): {vanished_inactive}")
mm = events["months_missing"]
print(f"months_missing: 0 for {(mm == 0).sum()} ({(mm == 0).mean():.1%}); 1 for {(mm == 1).sum()}; "
      f">=2 for {(mm >= 2).sum()}; max {mm.max()}")
print("months_missing by exit_status:", events.group_by("exit_status").agg(
    gt0=(pl.col("months_missing") > 0).sum(), n=pl.len()).sort("n").rows())

# %% 2. event definition: COD vs synchronization vs energization
ops = events.filter(pl.col("exit_status") == "operational")


def lag(later: str, earlier: str) -> pl.Expr:
    return (pl.col(later) - pl.col(earlier)).dt.total_days() / sv.DAYS_PER_MONTH


print("operational:", ops.height, "| with COD date:", ops["commercial_operation_date"].is_not_null().sum(),
      "| with sync:", ops["approved_synchronization"].is_not_null().sum(),
      "| with energization:", ops["approved_energization"].is_not_null().sum())
print("months sync -> COD (median, p90):", ops.select(
    lag("commercial_operation_date", "approved_synchronization").median().alias("med"),
    lag("commercial_operation_date", "approved_synchronization").quantile(0.9).alias("p90")).rows())
print("months energization -> COD (median, p90):", ops.select(
    lag("commercial_operation_date", "approved_energization").median().alias("med"),
    lag("commercial_operation_date", "approved_energization").quantile(0.9).alias("p90")).rows())
synced = events.filter(pl.col("approved_synchronization").is_not_null())
print("synchronized projects by exit_status:", synced.group_by("exit_status").len().sort("len").rows())
energized = events.filter(pl.col("approved_energization").is_not_null())
print("energized projects by exit_status:", energized.group_by("exit_status").len().sort("len").rows())
ever_inactive = events.filter(pl.col("inactive_first_month").is_not_null())
print("ever inactive by exit_status:", ever_inactive.group_by("exit_status").len().sort("len").rows())
print("IA signed before FIS approved:", events.filter(pl.col("ia_signed") < pl.col("fis_approved")).height,
      "| IA without FIS approved:", events.filter(pl.col("ia_signed").is_not_null() & pl.col("fis_approved").is_null()).height)

# %% 3. landmark frames and stratum sizes (merge rule applied consistently across landmarks)
frames = {lm: sv.landmark_frame(events, lm) for lm in sv.LANDMARKS}
for lm, f in frames.items():
    trunc = (f["entry"] > 0).sum()
    print(f"{lm}: n={f.height}, COD={int((f['event'] == sv.COD).sum())}, withdrawn={int((f['event'] == sv.WITHDRAWN).sum())}, "
          f"delayed entry={trunc} ({trunc / f.height:.0%}, median {f.filter(pl.col('entry') > 0)['entry'].median():.1f} mo), "
          f"clipped={int(f['clipped'].sum())}")

raw_sizes = pl.concat([
    f.group_by("fuel_group").agg(n=pl.len(), cod=(pl.col("event") == sv.COD).sum()).with_columns(landmark=pl.lit(lm))
    for lm, f in frames.items()
]).pivot(on="landmark", index="fuel_group", values=["n", "cod"]).sort("fuel_group")
print(raw_sizes)

mapping = {g: g for g in events["fuel_group"].unique().to_list()}
while True:
    changed = False
    for f in frames.values():
        counts = {}
        for g, sub in f.with_columns(stratum=pl.col("fuel_group").replace(mapping)).group_by("stratum"):
            counts[g[0]] = (sub.height, int((sub["event"] == sv.COD).sum()))
        step = sv.merge_small_strata(counts)
        if any(k != v for k, v in step.items()):
            mapping = {g: step.get(s, s) for g, s in mapping.items()}
            changed = True
    if not changed:
        break
print("strata after the size rule:", mapping)
frames = {lm: f.with_columns(stratum=pl.col("fuel_group").replace(mapping)) for lm, f in frames.items()}

# %% 4. CIF by landmark x stratum
def table(frames: dict[str, pl.DataFrame], **kw) -> pl.DataFrame:
    parts = []
    for lm, f in frames.items():
        parts.append(sv.cif_table(f, "stratum", **kw).with_columns(landmark=pl.lit(lm)))
        parts.append(sv.cif_table(f, None, **kw).with_columns(landmark=pl.lit(lm)))
    return pl.concat(parts, how="diagonal").select("landmark", pl.exclude("landmark"))


cif = table(frames)
show = ["landmark", "stratum", "n", "cod", "withdrawn", *[f"cif_{h}" for h in sv.HORIZONS], "wd_36", "at_risk_36", "at_risk_48"]
print(cif.select(show).with_columns(pl.col("^cif_.*$", "wd_36").round(3)))

# curves for the app (step functions, one row per event time)
curves = []
for lm, f in frames.items():
    for stratum, g in [("all", f), *[(k[0], g) for k, g in f.group_by("stratum")]]:
        fit = sv.fit_cif(g["time"].to_numpy(), g["event"].to_numpy(), g["entry"].to_numpy())
        curves.append(pl.DataFrame({
            "landmark": lm, "stratum": stratum, "month": fit.times, "at_risk": fit.at_risk,
            "cif_cod": fit.cif[:, fit.causes.index(sv.COD)], "cif_withdrawn": fit.cif[:, fit.causes.index(sv.WITHDRAWN)],
        }))
pl.concat(curves).write_csv(out_path("q6_cif_curves.csv"))

# %% 5. cross-check against lifelines (tied event times are jittered there; seed fixed)
worst = 0.0
for lm, f in frames.items():
    for stratum, g in [("all", f), *[(k[0], g) for k, g in f.group_by("stratum")]]:
        ours = sv.fit_cif(g["time"].to_numpy(), g["event"].to_numpy(), g["entry"].to_numpy()).at(sv.HORIZONS)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            aj = AalenJohansenFitter(calculate_variance=False, seed=0).fit(
                g["time"].to_numpy(), g["event"].to_numpy(), event_of_interest=sv.COD, entry=g["entry"].to_numpy())
        theirs = aj.cumulative_density_.iloc[:, 0]
        ref = np.array([theirs[theirs.index <= h].iloc[-1] for h in sv.HORIZONS])
        worst = max(worst, float(np.abs(ours - ref).max()))
print(f"max |numpy AJ - lifelines AJ| at 12/24/36/48 months over all strata: {worst:.2e}")

# %% 6. ordering sanity at 36 months: IA signed > FIS approved > entry, per stratum
c36 = cif.pivot(on="landmark", index="stratum", values="cif_36")
c36 = c36.with_columns(ordered=(pl.col("ia_signed") > pl.col("fis_approved")) & (pl.col("fis_approved") > pl.col("entry")))
print(c36.with_columns(pl.col("entry", "fis_approved", "ia_signed").round(3)))

# are the landmarks nested? (the check assumes FIS approval comes before the IA)
both = events.filter(pl.col("ia_signed").is_not_null() & pl.col("fis_approved").is_not_null())
print("IA signed before FIS approved, among projects with both:", both.with_columns(
    stratum=pl.col("fuel_group").replace(mapping)).group_by("stratum").agg(
    n=pl.len(), ia_first=(pl.col("ia_signed") < pl.col("fis_approved")).mean().round(3),
    median_fis_minus_ia_months=lag("fis_approved", "ia_signed").median().round(1)).sort("stratum").rows())
for lm in ("fis_approved", "ia_signed"):
    for stratum, g in sorted(frames[lm].group_by("stratum"), key=lambda kv: kv[0][0]):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            aj = AalenJohansenFitter(seed=0).fit(g["time"].to_numpy(), g["event"].to_numpy(),
                                                 event_of_interest=sv.COD, entry=g["entry"].to_numpy())
        lo, hi = aj.confidence_interval_[aj.confidence_interval_.index <= 36].iloc[-1].to_numpy()
        print(f"  95% CI of CIF36, {lm} {stratum[0]}: [{lo:.3f}, {hi:.3f}] (lifelines)")

# %% 7. dropped sensitivity: vanished projects censored instead of competing
frames_c = {lm: sv.landmark_frame(events, lm, vanished="censored").with_columns(stratum=pl.col("fuel_group").replace(mapping))
            for lm in sv.LANDMARKS}
cif_c = table(frames_c)
sens = cif.select("landmark", "stratum", "cif_36").join(
    cif_c.select("landmark", "stratum", pl.col("cif_36").alias("cif_36_censored")), on=["landmark", "stratum"]
).with_columns(diff_pp=(pl.col("cif_36_censored") - pl.col("cif_36")) * 100)
print(sens.with_columns(pl.col("cif_36", "cif_36_censored").round(3), pl.col("diff_pp").round(2)))
print(f"max |diff| at 36 months: {sens['diff_pp'].abs().max():.2f} pp")

# %% 8. MW-weighted CIF (the app's "of the MW in the queue") and the no-truncation check
cif_mw = table(frames, weight="capacity_mw")
print("MW-weighted:", cif_mw.select("landmark", "stratum", "n", *[pl.col(f"cif_{h}").round(3) for h in sv.HORIZONS]))
no_trunc = {lm: f.with_columns(entry=pl.lit(0.0), time=pl.max_horizontal("time", pl.lit(sv.ONE_DAY)))
            for lm, f in frames.items()}
cif_nt = table(no_trunc)
print("entry ignored (no left truncation), CIF36:", cif_nt.filter(pl.col("stratum") == "all").select("landmark", pl.col("cif_36").round(3)).rows())

# %% 9. figure: CIF of COD by stratum, one panel per landmark
PALETTE = {"solar": "#eda100", "storage": "#2a78d6", "wind": "#1baf7a", "gas+other": "#eb6834",
           "gas": "#eb6834", "other": "#4a3aa7", "all": "#3d3d3a"}
curves_df = pl.read_csv(out_path("q6_cif_curves.csv"))
fig, axes = plt.subplots(1, 3, figsize=(13, 4.2), sharey=True)
titles = {"entry": "From queue entry", "fis_approved": "From FIS approved", "ia_signed": "From IA signed"}
for ax, lm in zip(axes, sv.LANDMARKS, strict=True):
    sub = curves_df.filter(pl.col("landmark") == lm)
    for stratum in sorted(sub["stratum"].unique().to_list(), key=lambda s: (s == "all", s)):
        # cut each curve where fewer than 10 projects are still at risk (the tail is noise)
        s = sub.filter(pl.col("stratum") == stratum).filter(pl.col("at_risk") >= 10)
        x = np.concatenate([[0], s["month"].to_numpy()])
        y = np.concatenate([[0], s["cif_cod"].to_numpy()])
        ax.step(x, y, where="post", lw=2 if stratum != "all" else 1.5, color=PALETTE.get(stratum, "#888"),
                ls="--" if stratum == "all" else "-", label=stratum)
        ax.annotate(stratum, (x[-1], y[-1]), xytext=(3, 0), textcoords="offset points", fontsize=8, color="#3d3d3a", va="center")
    ax.axvline(36, color="#c3c2b7", lw=1, zorder=0)
    ax.set_title(titles[lm], fontsize=10, loc="left")
    ax.set_xlabel("months since landmark")
    ax.set_xlim(0, 80)
    ax.grid(axis="y", color="#e5e4df", lw=0.8)
    ax.spines[["top", "right"]].set_visible(False)
axes[0].set_ylabel("share reaching COD (Aalen–Johansen CIF)")
axes[0].legend(frameon=False, fontsize=8, loc="upper left")
fig.suptitle("ERCOT generation queue, cohort first listed since Aug 2018: cumulative incidence of COD (curves stop at < 10 at risk)", fontsize=11, x=0.01, ha="left")
fig.tight_layout()
fig.savefig(out_path("q6_cif_by_landmark.png"), dpi=150)
print("figure:", out_path("q6_cif_by_landmark.png"))

# %% 10. county -> FIPS
counties = read_sql("SELECT county_fips, county_name FROM tx_counties")


def norm(col: str) -> pl.Expr:
    return (pl.col(col).str.to_uppercase().str.replace_all(r"\bCOUNTY\b", "").str.replace_all(r"[^A-Z]", ""))


lookup = counties.with_columns(key=norm("county_name")).select("key", "county_fips")
all_rows = sv.load_events(cohort_start=None)
for label, df in [("cohort", events), ("all INRs", all_rows)]:
    m = df.with_columns(key=norm("county")).join(lookup, on="key", how="left")
    ok = m["county_fips"].is_not_null().sum()
    print(f"county match ({label}): {ok}/{df.height} = {ok / df.height:.2%}; null county: {df['county'].is_null().sum()}")
    print("  unmatched:", m.filter(pl.col("county_fips").is_null()).group_by("county").len().sort("len", descending=True).rows())

# %% 11. plan B ready: cohort rates (share of each first-listed-year cohort by current outcome)
rates = (
    sv.outcomes(events)
    .with_columns(cohort=pl.col("first_seen_month").dt.year(), stratum=pl.col("fuel_group").replace(mapping))
    .group_by("cohort").agg(
        n=pl.len(),
        cod=(pl.col("event") == sv.COD).mean().round(3),
        withdrawn=(pl.col("event") == sv.WITHDRAWN).mean().round(3),
        still_queued=(pl.col("event") == sv.CENSORED).mean().round(3),
    ).sort("cohort")
)
print("cohort rates (all fuels):", rates)

# %% 12. decision inputs
ordered_all = bool(c36["ordered"].all())
sizes_ok = bool(cif.filter(pl.col("stratum") != "all").select((pl.col("n") >= sv.MIN_N) & (pl.col("cod") >= sv.MIN_EVENTS)).to_series().all())
print(f"ordering coherent in every stratum: {ordered_all}; every stratum n>={sv.MIN_N} and COD>={sv.MIN_EVENTS}: {sizes_ok}; "
      f"dropped sensitivity max {sens['diff_pp'].abs().max():.2f} pp (threshold 10)")
