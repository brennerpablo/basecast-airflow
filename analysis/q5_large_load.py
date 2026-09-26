# %% [markdown]
# # Q5 — do the large-load series support a realization ratio?
#
# Spec: `docs/PHASE0_ANALYSIS.md` §2 Q5. Result: `docs/analysis/q5_large_load.md`.
# Run from the repo root: `uv run --group analysis python analysis/q5_large_load.py`.
# Inputs are read-only (`basecast_reader`). Gemini chart values are `verified=false`.

# %%
from __future__ import annotations

from datetime import date

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import polars as pl  # noqa: E402

from analysis._common import ROOT, out_path  # noqa: E402
from basecast_pipelines.models import large_load as ll  # noqa: E402

cv = ll.load_chart_values()
headlines = ll.load_headlines()
status = ll.load_status()
manifests = ll.load_manifests(ROOT / "data")
print(f"chart values {cv.height}, headlines {headlines.height}, native status {status.height}, "
      f"raw files {manifests.height}")

# %% [markdown]
# ## 1. Inventory

# %%
inventory = ll.series_inventory(cv)
inventory.write_csv(out_path("q5_inventory.csv"))
print(inventory.group_by("series_kind").agg(pl.len().alias("charts"), pl.col("document").n_unique().alias("docs"))
      .sort("docs", descending=True))

coverage = ll.deck_coverage(inventory).join(manifests.select("source_file", "doc_type"), on="source_file",
                                            how="left")
print(coverage)
n_docs = coverage.height
n_in_service_docs = coverage["has_in_service"].sum()
n_status_docs = coverage["has_status_by_month"].sum()
n_status_decks = coverage.filter(pl.col("doc_type") == "status_deck")
print(f"documents since {ll.PHASE0_SINCE}: {n_docs}; with in-service series {n_in_service_docs}; "
      f"with status-by-month series {n_status_docs}")
print(f"status decks since {ll.PHASE0_SINCE}: {n_status_decks.height}; with in-service "
      f"{n_status_decks['has_in_service'].sum()}; with status-by-month {n_status_decks['has_status_by_month'].sum()}; "
      f"both {(n_status_decks['has_in_service'] & n_status_decks['has_status_by_month']).sum()}")
missing = n_status_decks.filter(~pl.col("has_in_service") | ~pl.col("has_status_by_month"))
print("status decks missing a series:\n", missing)

# %% [markdown]
# ## 2. Consistency: bars sum vs chart total, chart totals vs the same deck's headline sentences (±5%)

# %%
wide = ll.in_service_wide(cv)
charts = ll.check_headlines(ll.chart_checks(wide), headlines)
charts = charts.join(manifests.select("source_file", "doc_type", "url"), on="source_file", how="left")
charts.write_csv(out_path("q5_chart_checks.csv"))
print(charts.select("vintage", "doc_type", "document", "page", "first_year", "last_year", "bars_checked",
                    "bars_failed", "bars_worst_rel_diff", "monotone", "queue_total_mw", "headline_total_mw",
                    "total_rel_diff", "a2e_stock_mw", "headline_a2e_mw", "a2e_rel_diff", "consistent"))

bars = ll.check_bars_sum(wide).drop_nulls("bars_ok")
print(f"bars-sum check: {bars['bars_ok'].sum()}/{bars.height} chart-years within ±5% "
      f"(worst {bars['bars_rel_diff'].abs().max():.3%})")
print("failing chart-years:\n", bars.filter(~pl.col("bars_ok")).select("vintage", "document", "year", "total",
                                                                      "segment_sum", "bars_rel_diff"))
tot = charts.drop_nulls("total_ok")
a2e = charts.drop_nulls("a2e_ok")
print(f"queue total vs headline: {tot['total_ok'].sum()}/{tot.height} within ±5%")
print(tot.select("vintage", "document", "queue_total_mw", "headline_total_mw", "total_rel_diff"))
print(f"approved stock vs headline: {a2e['a2e_ok'].sum()}/{a2e.height} within ±5%")
print(a2e.filter(~pl.col("a2e_ok")).select("vintage", "document", "a2e_stock_mw", "headline_a2e_mw", "a2e_rel_diff"))

# %% [markdown]
# ## 3. Vintages (one chart per data date) and the decision count

# %%
vintages = ll.pick_vintages(charts, manifests)
since = vintages.filter(pl.col("vintage") >= ll.PHASE0_SINCE)
print(since.select("vintage", "doc_type", "document", "page", "last_year", "has_segments", "headline_checked",
                   "consistent"))
n_vint = since.height
n_consistent = since["consistent"].sum()
n_consistent_hl = since.filter(pl.col("headline_checked") & pl.col("consistent")).height
n_status_vint = since.filter(pl.col("doc_type") == "status_deck").height
print(f"vintages since {ll.PHASE0_SINCE} with the in-service series: {n_vint} ({n_status_vint} from status decks)")
print(f"consistent (bars ≤5%, monotone, headline ≤5% where present): {n_consistent}; "
      f"of which with a same-deck headline checked: {n_consistent_hl}")

# %% [markdown]
# ## 4. Cross-checks Claude can do without Pablo (not a substitute for the spot-check)

# %%
xs = ll.crosscheck_status_vs_headlines(cv, headlines)
print(f"status series (Gemini, last month) vs headline sentence: {xs['agree'].sum()}/{xs.height} within 1%")
print(xs.filter(~pl.col("agree")))
xn = ll.crosscheck_native(cv, status)
print(f"native PPTX months (Oct 2024 deck) vs Gemini's reading of the next deck: {xn['agree'].sum()}/{xn.height} "
      f"within 1%")
print(xn.select("series_kind", "series", "month", "native_mw", "document", "gemini_mw", "rel_diff", "agree"))
dup = ll.duplicate_agreement(wide)
print(f"same vintage read twice: {dup['agree'].sum()}/{dup.height} bar values within 1%")
print(dup.filter(~pl.col("agree")).select("vintage", "document", "document_b", "year", "field", "mw", "mw_b"))
# The March 2026 PDF (data of 13 Mar, 239 GW) and its "Updated_03262026" PPTX (26 Mar, 410 GW) are two different
# snapshots that Gemini dated alike; their disagreement is a real revision, not a misread.
REVISED = "March-TAC-Report-Updated_03262026.pptx"
dup_same = dup.filter((pl.col("document") != REVISED) & (pl.col("document_b") != REVISED))
print(f"same vintage read twice, excluding the March 2026 revision: {dup_same['agree'].sum()}/{dup_same.height}")

# %% [markdown]
# ## 5. Realized stocks at year end

# %%
monthly = ll.a2e_by_month(cv)
print(monthly)
realized_rows = []
for year in (2024, 2025, 2026):
    a2e_mw, month = ll.stock_at_year_end(monthly, year)
    en_mw, en_vintage = ll.energized_at_year_end(wide, year)
    as_of = date(year + 1, 1, 1) if month == f"{year}-12" else date.fromisoformat(f"{month}-28")
    realized_rows.append({"target_year": year, "realized_a2e_mw": a2e_mw, "a2e_month": month,
                          "realized_energized_mw": en_mw, "energized_from_vintage": en_vintage,
                          "realized_as_of": as_of})
realized = pl.DataFrame(realized_rows)
print(realized)

# %% [markdown]
# ## 6. Realization ratio per vintage and target year

# %%
bars_v = wide.join(
    vintages.select("vintage", "document", "page", "chart_title", "has_segments",
                    pl.col("a2e_stock_mw").alias("base_a2e_mw")),
    on=["vintage", "document", "page", "chart_title"],
    how="inner",
).filter(pl.col("vintage") >= ll.PHASE0_SINCE)
ratios = ll.realization_ratios(bars_v, realized)
ratios.write_csv(out_path("q5_ratios.csv"))
print(ratios.select("target_year", "vintage", "horizon_months", "promised_mw", "promised_firm_mw", "base_a2e_mw",
                    "realized_a2e_mw", "gross_a2e", "gross_a2e_firm", "incremental_a2e", "incremental_a2e_firm",
                    "gross_energized"))
summary = ll.summarize_ratios(ratios)
print(summary)
consistent_only = ll.summarize_ratios(
    ratios.join(vintages.filter(pl.col("consistent")).select("vintage"), on="vintage", how="semi")
)
print("consistent vintages only:\n", consistent_only)
# Vintages made weeks before year end promise almost nothing new for that year, so the incremental ratio is
# 0/0-ish there; the range for A3 uses horizons of 6 months or more.
summary_h6 = ll.summarize_ratios(ratios.filter(pl.col("horizon_months") >= 6))
summary_h6.write_csv(out_path("q5_ratio_summary_h6.csv"))
print("horizon >= 6 months:\n", summary_h6)

# %% [markdown]
# ## 7. Figure: promised (per vintage) vs realized, one panel per target year
#
# The one chart that carries the decision and the video line: how far each vintage's promise sits above what
# was approved by the end of the year.

# %%
BLUE, ORANGE, INK, MUTED = "#2a78d6", "#eb6834", "#0b0b0b", "#52514e"
fig, axes = plt.subplots(1, 3, figsize=(13, 4.2), sharey=False)
for ax, year in zip(axes, (2024, 2025, 2026), strict=True):
    r = ratios.filter(pl.col("target_year") == year).sort("vintage")
    ax.plot(r["vintage"], r["promised_mw"] / 1000, color=BLUE, lw=2, marker="o", ms=4,
            label="Promised by vintage (all statuses)")
    ax.plot(r["vintage"], r["promised_firm_mw"] / 1000, color=ORANGE, lw=2, marker="o", ms=4,
            label="Promised, excl. no studies submitted")
    real = realized.filter(pl.col("target_year") == year).row(0, named=True)
    ax.axhline(real["realized_a2e_mw"] / 1000, color=INK, lw=2, ls="--",
               label="Approved to energize at year end")
    suffix = " (partial)" if year == 2026 else ""
    ax.set_title(f"Target year {year}{suffix}", fontsize=11, color=INK, loc="left")
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", color="#e5e5e3", lw=0.8)
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(colors=MUTED, labelsize=8)
    ax.set_ylabel("GW", color=MUTED, fontsize=9)
    ax.set_xlabel("Deck (vintage)", color=MUTED, fontsize=8)
    ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(1, 7)))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
handles, labels = axes[0].get_legend_handles_labels()
fig.legend(handles, labels, frameon=False, fontsize=8, loc="lower center", ncol=3)
fig.suptitle("Large-load queue: MW promised in service by year end vs MW approved to energize "
             "(ERCOT decks, Gemini chart reads, verified=false)", fontsize=10, color=INK, x=0.01, ha="left")
fig.tight_layout(rect=(0, 0.07, 1, 1))
fig.savefig(out_path("q5_promised_vs_realized.png"), dpi=150)
print("saved", out_path("q5_promised_vs_realized.png"))

# %% [markdown]
# ## 8. Spot-check file for Pablo (5 values per series, with the slide link)

# %%
urls = manifests.select("source_file", "url")
is_pick = ll.spread(vintages.filter(pl.col("vintage") >= ll.PHASE0_SINCE).sort("vintage"), 5)
is_rows = []
for v in is_pick.iter_rows(named=True):
    target = min(v["vintage"].year + 1, v["last_year"])
    bar = wide.filter((pl.col("vintage") == v["vintage"]) & (pl.col("document") == v["document"])
                      & (pl.col("page") == v["page"]) & (pl.col("year") == target)).row(0, named=True)
    is_rows.append({
        "series": "In-service (MW by projected in-service year)", "vintage": v["vintage"],
        "document": v["document"], "page": v["page"], "chart_title": v["chart_title"],
        "value": f"{target} bar, Total = {bar['total_mw']:,.0f} MW", "url": v["url"],
    })
st_charts = (
    ll.with_vintage(cv)
    .filter((pl.col("series_kind") == "status_by_month") & (pl.col("status_bucket") == "approved_to_energize"))
    .join(urls, on="source_file", how="left")
    .sort("category")
    .group_by("vintage", "document", "page", "chart_title", "url", maintain_order=True)
    .agg(pl.col("category").last().alias("month"), pl.col("value_mw").last())
    .sort("vintage")
)
st_rows = [
    {"series": "Status by month (approved to energize stock)", "vintage": s["vintage"], "document": s["document"],
     "page": s["page"], "chart_title": s["chart_title"],
     "value": f"{s['month']}, Approved to Energize = {s['value_mw']:,.0f} MW", "url": s["url"]}
    for s in ll.spread(st_charts, 5).iter_rows(named=True)
]
lines = [
    "# Large-load chart values — spot-check (PENDING human check)",
    "",
    "> **Status: PENDING.** Pablo checks each value against the slide and ticks the box. Until then every value in",
    "> `large_load_chart_values` stays `verified=false`. Generated by `analysis/q5_large_load.py` (phase 0, Q5).",
    "",
    "Values were read off chart images by Gemini (`gemini-3.8-flash`, prompt v1). Five values per series, spread",
    "over the vintages since May 2023. Open the link, go to the page, and compare the number printed on the chart.",
    "For a zip, open the named member inside it. Tick **OK** if the slide shows the same number (±1 MW, or the",
    "same rounding the slide prints), otherwise write the slide's number in **Slide says**.",
    "",
    "| # | Series | Vintage | Deck, page | Chart | Gemini value | Slide link | OK | Slide says |",
    "|---|---|---|---|---|---|---|---|---|",
]
for i, r in enumerate(is_rows + st_rows, start=1):
    link = ll.slide_link(r["url"], r["document"], r["page"])
    lines.append(f"| {i} | {r['series']} | {r['vintage']} | {r['document']}, p. {r['page']} | {r['chart_title']} "
                 f"| {r['value']} | {link} | [ ] | |")
lines += [
    "",
    "Automatic cross-checks already run (not a substitute for this check): see `docs/analysis/q5_large_load.md`,",
    "section Consistency.",
    "",
]
(ROOT / "docs" / "large-load-spot-check.md").write_text("\n".join(lines))
print("\n".join(lines))
