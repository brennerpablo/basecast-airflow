# %% [markdown]
# # X4 — EIA-861 short form (861S) for the account universe
#
# Phase 0 found that 46 of the 106 ERCOT co-ops and munis in EIA-861 file only the short form
# (`Short_Form_<year>.xlsx`), which the `eia_861` parser does not load, so the EIA signals cover 57% of the
# universe (27% of munis). This script reads the short form straight from the raw zips (analysis only, no
# pipeline or database change), combines it with the long form in `eia861_sales`, and re-runs the Q3 signal
# rule. Logic: `basecast_pipelines/models/eia861_short_form.py`. Numbers: `docs/analysis/x4_eia861_short_form.md`.
#
# Validation lock: the five partner accounts are held out **before** any number is computed, exactly as
# `analysis/q3_signals.py` does. Every statistic, rank and example below covers the other 107 accounts.
#
# Run: `uv run --group analysis python analysis/x4_eia861_short_form.py`.

# %%
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis._common import ROOT, out_path  # noqa: E402
from basecast_pipelines.models import accounts as A  # noqa: E402
from basecast_pipelines.models import eia861_short_form as S  # noqa: E402
from basecast_pipelines.parsers.eia._common import zip_member  # noqa: E402
from basecast_pipelines.processing.tabular import clean_label, find_header_row, read_grid  # noqa: E402

YEAR, BASE_YEAR = 2024, 2019  # latest final release; base of the 5-year growth (as in Q3)
POP_START, POP_END = 2020, 2025
PERMIT_YEARS = [2023, 2024, 2025]
ZONE_START, ZONE_END = 2025, 2031
DC_SINCE = date(2025, 1, 1)
RAW = ROOT / "data" / "raw"

# %% [markdown]
# ## Universe (Q2) minus the held-out partner accounts (same code as Q3)

# %%
accounts = A.load_accounts()
crosswalk = A.read_crosswalk(ROOT / "config" / "utility_crosswalk.yaml")
universe_all = A.build_universe(accounts, crosswalk, intersect=True)

PARTNER_NAMES = [
    "Bandera Electric Cooperative",
    "Guadalupe Valley Electric Cooperative",
    "CoServ",
    "Farmers Electric Cooperative",
    "Austin Energy",
]
held_names = A.find_names(accounts["name"], PARTNER_NAMES)["found"].drop_nulls().to_list()
held_ids = accounts.filter(pl.col("name").is_in(held_names))["account_id"].to_list()
assert len(held_ids) == len(PARTNER_NAMES), "partner hold-out did not find every name"
universe = universe_all.filter(~pl.col("account_id").is_in(held_ids)).join(
    accounts.select("account_id", "name"), on="account_id", how="left"
)
print(f"universe: {universe_all.height} accounts; held out: {len(held_ids)}; analysed: {universe.height}")
print(universe.group_by("account_type").len().sort("account_type"))
shared = universe.group_by("eia_utility_id").len().filter(pl.col("len") > 1)
print("EIA ids shared by more than one analysed account:", shared.height)
links = A.load_links().filter(pl.col("account_id").is_in(universe["account_id"].to_list()))
uids = universe["eia_utility_id"].to_list()

# %% [markdown]
# ## 1. What the short form carries
#
# Header of every `Short_Form_<year>` workbook in the local lake, read as it is (no assumption on names).

# %%
layout_rows = []
for path in S.raw_zips(RAW):
    data = path.read_bytes()
    member = zip_member(data, S.SHORT_FORM_MEMBER)
    year, early = S.release_of(path.name)
    if member is None:
        layout_rows.append({"data_year": year, "early_release": early, "file": None, "columns": None})
        continue
    grid = read_grid(member[1], 0, n_rows=10)
    header = find_header_row(grid, [r"utility number", r"customers"])
    labels = [clean_label(c) for c in grid.row(header) if c is not None and clean_label(c)]
    layout_rows.append(
        {"data_year": year, "early_release": early, "file": member[0], "columns": " | ".join(labels)}
    )
layout = pl.DataFrame(layout_rows)
short_all = S.load_short_form(RAW, states=None)
short = short_all.filter(pl.col("state") == "TX")
REL = ["data_year", "early_release"]
layout = (
    layout.join(short_all.group_by(REL).agg(pl.len().alias("rows_us")), on=REL, how="left")
    .join(short.group_by(REL).agg(pl.len().alias("rows_tx")), on=REL, how="left")
    .sort(REL)
)
layout.write_csv(out_path("x4_short_form_layout.csv"))
with pl.Config(fmt_str_lengths=400, tbl_width_chars=400):
    print(layout)
print("distinct header layouts:", layout["columns"].drop_nulls().n_unique())
print("TX rows by ownership and year:")
print(
    short.group_by("data_year", "ownership").len().pivot(on="ownership", index="data_year", values="len").sort("data_year")
)
# Raw Texas rows for review, without the held-out accounts' utilities.
held_uids = universe_all.filter(pl.col("account_id").is_in(held_ids))["eia_utility_id"].to_list()
short.filter(~pl.col("utility_id").is_in(held_uids)).write_csv(out_path("x4_short_form_tx.csv"))

# %% [markdown]
# ## Long + short form, one row per utility-year

# %%
long_tot = S.long_form_totals(S.load_long_sales())
short_tot = S.short_form_totals(short)
# Delivery-only customers (EIA part C, retail choice) sit in Delivery_Companies_<year> from the 2020 zip on;
# the long-form rule leaves them out, which breaks the meter count of a utility that moves to retail choice.
delivery = S.load_delivery(RAW)
FIRST_DELIVERY_YEAR = int(delivery["data_year"].min())
combined = S.add_delivery(S.with_price(S.combine_forms(long_tot, short_tot)), delivery, first_year=FIRST_DELIVERY_YEAR)
print("utility-years in both forms (long kept):", combined.filter(pl.col("in_both")).height)
print("first Delivery_Companies year:", FIRST_DELIVERY_YEAR)
# Accounts x years (an EIA id shared by two accounts counts for both).
acct_years = universe.select("account_id", "account_type", pl.col("eia_utility_id").alias("utility_id")).join(
    combined, on="utility_id", how="left"
)

# %% [markdown]
# ### Coverage by year and type (accounts with a positive value; final releases, plus the 2025 early release)

# %%
years = sorted(combined["data_year"].unique().to_list())
cov_rows = []
for (y, er) in combined.select("data_year", "early_release").unique().sort("data_year", "early_release").iter_rows():
    sub = acct_years.filter(pl.col("data_year") == y, pl.col("early_release") == er)
    for t in (*A.ACCOUNT_TYPES, "all"):
        base = universe if t == "all" else universe.filter(pl.col("account_type") == t)
        s = sub if t == "all" else sub.filter(pl.col("account_type") == t)
        n = base.height
        cov_rows.append(
            {
                "data_year": y,
                "early_release": er,
                "type": t,
                "accounts": n,
                "customers": s.filter(pl.col("customers") > 0).height,
                "customers_long": s.filter(pl.col("customers") > 0, pl.col("form") == "long").height,
                "customers_short": s.filter(pl.col("customers") > 0, pl.col("form") == "short").height,
                "sales": s.filter(pl.col("sales_mwh") > 0).height,
                "revenue": s.filter(pl.col("revenue_thousand_usd") > 0).height,
                "price": s.filter(pl.col("price_usd_kwh").is_not_null()).height,
            }
        )
coverage = pl.DataFrame(cov_rows).with_columns((pl.col("customers") / pl.col("accounts")).alias("customers_share"))
coverage.write_csv(out_path("x4_coverage_by_year.csv"))
print(coverage.filter(pl.col("type") == "all"))
print(coverage.filter(pl.col("type") != "all", pl.col("data_year").is_in([2018, 2019, 2020, 2024])))
never = universe.join(
    acct_years.filter(~pl.col("early_release"), pl.col("customers") > 0).select("account_id").unique(),
    on="account_id",
    how="anti",
)
print("analysed accounts with no customers in any final year (either form):", never.height)
print(never.select("account_id", "account_type", "name", "eia_utility_id"))
no_base = universe.join(
    acct_years.filter(pl.col("data_year") == BASE_YEAR, pl.col("customers") > 0).select("account_id"),
    on="account_id",
    how="anti",
)
print(
    f"analysed accounts without customers in {BASE_YEAR}:",
    no_base.select("account_id", "account_type", "name", "eia_utility_id").rows(),
)
print("  their final years with customers:", acct_years.filter(
    pl.col("account_id").is_in(no_base["account_id"].to_list()), ~pl.col("early_release"), pl.col("customers") > 0
).group_by("account_id").agg(pl.col("data_year").sort()).rows())
blank_2024 = acct_years.filter(
    pl.col("data_year") == YEAR, ~pl.col("early_release"), pl.col("customers").is_null() | (pl.col("customers") <= 0)
)
print(f"{YEAR} rows without positive customers:", blank_2024.select("account_id", "utility_id", "form", "customers").rows())

# %% [markdown]
# ## 2. Signals: Q3's public signals plus the combined EIA signals

# %%
sales_long = A.load_eia_sales()
pop_growth = A.population_growth(A.load_population(), links, start=POP_START, end=POP_END)
permits = A.load_permits_annual(PERMIT_YEARS)
permit_units = permits.group_by("county_fips").agg(pl.col("units_total").sum().alias("units"))
signals = A.build_signals(
    universe,
    links,
    eia=A.eia_account_signals(sales_long, year=YEAR, base_year=BASE_YEAR).select(
        "utility_id", pl.col("eia_customers").alias("eia_customers_long"), "eia_res_sales_share"
    ),
    population=pop_growth,
    permits=A.permits_per_1k(permit_units, pop_growth, links),
    housing=A.owner_single_family(A.load_housing(), links),
    dc=A.data_center_sites(A.load_dc_sites(), links, since=DC_SINCE),
    zone=A.zone_peak_growth(A.load_zone_peaks(), A.load_county_zone(), links, start=ZONE_START, end=ZONE_END),
)
eia = S.utility_signals(combined, year=YEAR, base_year=BASE_YEAR, customers="wires_customers")
bundled = S.utility_signals(combined, year=YEAR, base_year=BASE_YEAR).select(
    pl.col("utility_id").alias("eia_utility_id"),
    pl.col("eia_customers").alias("eia_bundled_customers"),
    pl.col("eia_customer_cagr").alias("eia_bundled_customer_cagr"),
)
signals = signals.join(eia.rename({"utility_id": "eia_utility_id"}), on="eia_utility_id", how="left").join(
    bundled, on="eia_utility_id", how="left"
)
same = signals.filter(pl.col("eia_customers_long").is_not_null())
assert (same["eia_customers_long"] == same["eia_bundled_customers"]).all(), "long-form customers differ from Q3's"
diff = signals.filter(
    (pl.col("eia_customers") != pl.col("eia_bundled_customers"))
    | (pl.col("eia_customer_cagr").round(6) != pl.col("eia_bundled_customer_cagr").round(6))
)
print("accounts where delivery-only customers change the signals:")
print(diff.select("account_id", "name", "eia_bundled_customers", "eia_customers", "eia_bundled_customer_cagr",
                  "eia_customer_cagr"))  # fmt: skip
SIGNALS = [
    "eia_customers",
    "eia_sales_mwh",
    "eia_revenue_kusd",
    "eia_res_sales_share",
    "eia_customer_cagr",
    "eia_price",
    "eia_price_cagr",
    "population",
    "pop_growth",
    "permit_units",
    "permits_per_1k",
    "owner_sf_homes",
    "owner_sf_share",
    "dc_sites",
    "zone_peak_cagr",
]
signals.select("account_id", "account_type", "eia_utility_id", "eia_form", "eia_base_form", *SIGNALS).write_csv(
    out_path("x4_signals.csv")
)
summary = A.signal_summary(signals, SIGNALS)
with pl.Config(float_precision=4, tbl_cols=14):
    print(summary)
for t in A.ACCOUNT_TYPES:
    sub = A.signal_summary(signals.filter(pl.col("account_type") == t), SIGNALS)
    print(t, dict(zip(sub["signal"], [round(c, 3) for c in sub["coverage"]], strict=True)))
print("form of the 2024 row:", signals["eia_form"].value_counts().sort("eia_form").rows())
print("form of the 2019 base row (CAGR):", signals["eia_base_form"].value_counts().sort("eia_base_form").rows())
print("accounts the short form adds for eia_customers:",
      signals.filter(pl.col("eia_customers").is_not_null(), pl.col("eia_customers_long").is_null()).height)
summary.write_csv(out_path("x4_signal_summary.csv"))

# %% [markdown]
# ### Distribution by type

# %%
def q(s: pl.Series) -> list:
    s = s.drop_nulls()
    vals = (s.quantile(0.1, "linear"), s.median(), s.quantile(0.9, "linear"))
    return [round(v, 4) if v is not None else None for v in vals]


for c in ("eia_customers", "eia_customer_cagr", "eia_price", "eia_price_cagr", "owner_sf_homes", "pop_growth"):
    print(c, {t: q(signals.filter(pl.col("account_type") == t)[c]) for t in A.ACCOUNT_TYPES})

# %% [markdown]
# ### Redundancy (Spearman, accounts that have both)

# %%
pairs = A.redundant_pairs(signals, SIGNALS)
pairs.write_csv(out_path("x4_signal_pairs.csv"))
new = ["eia_customers", "eia_customer_cagr", "eia_price", "eia_price_cagr"]
with pl.Config(float_precision=3):
    print("pairs with a combined EIA signal, |ρ| ≥ 0.4 or with owner_sf_homes / pop_growth:")
    print(
        pairs.filter(
            pl.col("a").is_in(new) | pl.col("b").is_in(new),
            (pl.col("spearman").abs() >= 0.4)
            | pl.col("a").is_in(["owner_sf_homes", "pop_growth"])
            | pl.col("b").is_in(["owner_sf_homes", "pop_growth"]),
        ).sort(pl.col("spearman").abs(), descending=True)
    )
    print("price signals against every other signal:")
    price = ["eia_price", "eia_price_cagr"]
    print(pairs.filter(pl.col("a").is_in(price) | pl.col("b").is_in(price)).sort(pl.col("spearman").abs(), descending=True))
    for t in A.ACCOUNT_TYPES:
        sub = A.redundant_pairs(
            signals.filter(pl.col("account_type") == t),
            ["eia_customers", "owner_sf_homes", "eia_customer_cagr", "pop_growth"],
        )
        print(t, [(a, b, n, round(r, 3) if r is not None else None) for a, b, n, r, _ in sub.rows()])

# %% [markdown]
# ### Does eia_customers fix the muni size bias?
#
# Percentile rank within the 107 (the score method) under each size signal, by type. The customers per
# apportioned owner-occupied home shows how far area apportionment is from the meters.

# %%
ranked = signals.filter(pl.col("eia_customers").is_not_null()).with_columns(
    S.percentile_rank(pl.col("owner_sf_homes")).alias("rank_owner_sf"),
    S.percentile_rank(pl.col("eia_customers")).alias("rank_eia_customers"),
    (pl.col("eia_customers") / pl.col("owner_sf_homes")).alias("customers_per_sf_home"),
)
bias = ranked.group_by("account_type").agg(
    pl.len().alias("n"),
    pl.col("rank_owner_sf").median().alias("median_rank_owner_sf"),
    pl.col("rank_eia_customers").median().alias("median_rank_eia_customers"),
    (pl.col("rank_eia_customers") - pl.col("rank_owner_sf")).median().alias("median_shift"),
    (pl.col("rank_owner_sf") >= 0.5).sum().alias("top_half_owner_sf"),
    (pl.col("rank_eia_customers") >= 0.5).sum().alias("top_half_eia_customers"),
    pl.col("customers_per_sf_home").median().alias("median_customers_per_sf_home"),
    pl.col("customers_per_sf_home").quantile(0.1, "linear").alias("p10_customers_per_sf_home"),
    pl.col("customers_per_sf_home").quantile(0.9, "linear").alias("p90_customers_per_sf_home"),
).sort("account_type")
with pl.Config(float_precision=3):
    print(bias)
ranked.select(
    "account_id", "account_type", "eia_customers", "owner_sf_homes", "customers_per_sf_home", "rank_owner_sf",
    "rank_eia_customers",
).write_csv(out_path("x4_size_ranks.csv"))
big_moves = ranked.with_columns((pl.col("rank_eia_customers") - pl.col("rank_owner_sf")).alias("shift")).filter(
    pl.col("shift").abs() >= 0.3
)
print("accounts moving ≥ 30 percentile points:", big_moves.group_by("account_type").agg(
    pl.len(), (pl.col("shift") > 0).sum().alias("up")).sort("account_type").rows())

# %% [markdown]
# ## 3. The Q3 rule on the new signals, and the weights proposal

# %%
entering = summary.filter(pl.col("enters"))["signal"].to_list()
print("pass coverage ≥ 80% and not near-constant:", entering)
print("fail:", summary.filter(~pl.col("enters")).select("signal", "coverage", "mode_share").rows())

# Not written to config/account_score.yaml; see docs/analysis/x4_eia861_short_form.md. Follows the rule Q3
# wrote down before this data existed (the `pending` block of the YAML): eia_customers replaces owner_sf_homes
# with the same weight; eia_customer_cagr, once ≥ 80%, takes 0.10 from pop_growth.
PROPOSED = {
    "pop_growth": 0.15,
    "eia_customers": 0.20,
    "eia_customer_cagr": 0.10,
    "permits_per_1k": 0.15,
    "dc_sites": 0.15,
    "zone_peak_cagr": 0.15,
    "owner_sf_share": 0.10,
}
assert abs(sum(PROPOSED.values()) - 1) < 1e-9
assert set(PROPOSED) <= set(entering), f"proposed signals that fail the rule: {set(PROPOSED) - set(entering)}"
both = pairs.filter(pl.col("a").is_in(list(PROPOSED)), pl.col("b").is_in(list(PROPOSED)), pl.col("redundant"))
assert both.is_empty(), both
print("proposed weighted pairs, top |ρ|:",
      [(a, b, round(r, 3)) for a, b, _, r, _ in
       pairs.filter(pl.col("a").is_in(list(PROPOSED)), pl.col("b").is_in(list(PROPOSED)))
       .sort(pl.col("spearman").abs(), descending=True).head(4).rows()])  # fmt: skip

# Score under the current (Q3) and proposed weights, to show how the order moves (partners are not here).
CURRENT = {"pop_growth": 0.25, "owner_sf_homes": 0.20, "permits_per_1k": 0.15, "dc_sites": 0.15,
           "zone_peak_cagr": 0.15, "owner_sf_share": 0.10}  # fmt: skip


def score(df: pl.DataFrame, weights: dict[str, float]) -> pl.Expr:
    ranks = [S.percentile_rank(pl.col(k)) for k in weights]
    num = pl.sum_horizontal([(r * w).fill_null(0) for r, w in zip(ranks, weights.values(), strict=True)])
    den = pl.sum_horizontal(
        [r.is_not_null().cast(pl.Float64) * w for r, w in zip(ranks, weights.values(), strict=True)]
    )
    return num / den


scored = signals.with_columns(score(signals, CURRENT).alias("score_q3"), score(signals, PROPOSED).alias("score_x4"))
scored = scored.with_columns(
    pl.col("score_q3").rank("ordinal", descending=True).alias("pos_q3"),
    pl.col("score_x4").rank("ordinal", descending=True).alias("pos_x4"),
)
rho = scored.select(pl.corr("score_q3", "score_x4", method="spearman")).item()
print("Spearman score_q3 × score_x4:", round(rho, 3))
print("median position by type (1 = top), Q3 vs X4:",
      scored.group_by("account_type").agg(pl.col("pos_q3").median(), pl.col("pos_x4").median())
      .sort("account_type").rows())  # fmt: skip
print("munis in the top 25, Q3 vs X4:",
      scored.filter(pl.col("pos_q3") <= 25, pl.col("account_type") == "muni").height,
      scored.filter(pl.col("pos_x4") <= 25, pl.col("account_type") == "muni").height)
scored.select("account_id", "account_type", "score_q3", "pos_q3", "score_x4", "pos_x4").write_csv(
    out_path("x4_scores.csv")
)

# %% [markdown]
# ## 4. Year-over-year jumps beyond ±50% on the combined series (final releases)

# %%
final_u = combined.filter(~pl.col("early_release"), pl.col("utility_id").is_in(uids))
names = universe.select(pl.col("eia_utility_id").alias("utility_id"), "name", "account_type").unique("utility_id")
jump_frames = []
for v in ("customers", "sales_mwh", "price_usd_kwh"):
    j = A.yoy_jumps(final_u.select("utility_id", "data_year", pl.col(v).alias("value")), "value")
    jump_frames.append(j.with_columns(pl.lit(v).alias("measure")))
jumps = pl.concat(jump_frames).join(names, on="utility_id", how="left")
forms = final_u.select("utility_id", "data_year", "form")
jumps = jumps.join(forms, on=["utility_id", "data_year"], how="left").join(
    forms.rename({"data_year": "prev_year", "form": "prev_form"}), on=["utility_id", "prev_year"], how="left"
).select("measure", "utility_id", "name", "account_type", "prev_year", "data_year", "prev_form", "form", "prev", "value",
          "change")  # fmt: skip
jumps.write_csv(out_path("x4_jumps.csv"))
with pl.Config(float_precision=3, tbl_rows=80):
    print(jumps.sort("utility_id", "measure", "data_year"))
print("utilities with a jump:", jumps["utility_id"].n_unique(), "; by measure:",
      jumps.group_by("measure").agg(pl.col("utility_id").n_unique()).sort("measure").rows())
print("jumps at a form switch (long ↔ short):", jumps.filter(pl.col("form") != pl.col("prev_form")).height)
with pl.Config(tbl_rows=120):
    short_jumps = jumps.filter(pl.col("form") == "short")["utility_id"].unique().to_list()
    print(final_u.filter(pl.col("utility_id").is_in(short_jumps))
          .select("utility_id", "data_year", "form", "customers", "sales_mwh", "revenue_thousand_usd", "price_usd_kwh")
          .pivot(on="utility_id", index="data_year", values="sales_mwh").sort("data_year"))
in_window = jumps.filter(pl.col("data_year") > BASE_YEAR, pl.col("measure") == "customers")
print("customer jumps inside the CAGR window:", in_window.select("utility_id", "name", "data_year", "change").rows())

# %% [markdown]
# ### Lubbock: delivery-only customers (Delivery_Companies, EIA part C)

# %%
delivery = delivery.filter(pl.col("utility_id").is_in(uids))
print("universe utilities in Delivery_Companies:", delivery.select("utility_id", "utility_name").unique().rows())
print(delivery.sort("utility_id", "data_year"))
lub = delivery.filter(pl.col("utility_name").str.contains("(?i)lubbock"))["utility_id"].unique().to_list()
if lub:
    bundled_lub = final_u.filter(pl.col("utility_id").is_in(lub)).select(
        "utility_id", "data_year", "form", pl.col("customers").alias("bundled"), pl.col("sales_mwh").alias("bundled_mwh")
    )
    delivery_lub = delivery.filter(~pl.col("early_release")).select(
        "utility_id", "data_year", pl.col("customers").alias("delivery"), pl.col("sales_mwh").alias("delivery_mwh")
    )
    wires = (
        bundled_lub.join(delivery_lub, on=["utility_id", "data_year"], how="left")
        .with_columns((pl.col("bundled") + pl.col("delivery").fill_null(0)).alias("wires_customers"))
        .sort("data_year")
    )
    print(wires.filter(pl.col("data_year") >= 2019))
    er = combined.filter(pl.col("utility_id").is_in(lub), pl.col("early_release"))
    print("Lubbock 2025 early release (bundled):", er.select("data_year", "form", "customers", "sales_mwh").rows(),
          "; delivery:",
          delivery.filter(pl.col("utility_id").is_in(lub), pl.col("early_release")).select("customers", "sales_mwh").rows())
