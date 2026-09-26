# %% [markdown]
# # X13 — Large-load requests through the G&T co-ops
#
# How much of the PUCT TSP RFI (project 58777 item 38, MW requested by TSP and year, 2026-2032) sits with IOUs,
# public power, G&T co-ops and others; which member co-ops (and LCRA-supplied munis) stand behind each G&T; how
# far member allocations disagree; and the per-account "your G&T reported X GW" fact and its coverage.
#
# Validation lock: the five partner accounts are held out of the account universe **before** anything
# account-level is computed (as in `analysis/q3_signals.py`). TSP-level aggregates over the whole RFI are not
# account-level and stay as filed.
#
# Run: `uv run --group analysis python analysis/x13_gt_large_load.py`. Numbers go to
# `docs/analysis/x13_gt_large_load.md`; tables to `analysis/out/x13_*`.

# %%
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis._common import ROOT, out_path, read_sql
from basecast_pipelines.models import accounts as A
from basecast_pipelines.models import eia861_short_form as S
from basecast_pipelines.models import gt_large_load as G
from basecast_pipelines.models import large_load as ll
from basecast_pipelines.models import triggers as T

DC_SINCE = date(2025, 1, 1)
EIA_YEAR = 2024
NEAR, FAR = 2030, 2032
BZ_DOC = "14-Batch-Zero-Update.pdf"
RAW = ROOT / "data" / "raw"


# %% [markdown]
# ## Universe minus the held-out partner accounts

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
universe = universe_all.filter(~pl.col("account_id").is_in(held_ids))
ids = universe["account_id"].to_list()
N = universe.height
print(f"universe: {universe_all.height}; held out: {len(held_ids)}; analysed: {N}")
names = universe.select("account_id", "name", "account_type")

# %% [markdown]
# ## Q1 — the RFI by entity type

# %%
rfi = G.load_rfi()
tsp = G.rfi_by_tsp(rfi)
FILED = rfi["filed_date"].max()
print("filed:", FILED, "| copies:", rfi["member"].unique().to_list())
print("sum of TSP rows vs filed total:\n", G.check_total(tsp))
seg = G.check_segments(G.load_ercot_members())
print("entity type vs ERCOT membership segment:\n", seg)
seg.write_csv(out_path("x13_segment_check.csv"))

wide = (
    G.classify(tsp)
    .pivot(on="year", index=["name", "entity_type", "group"], values="mw")
    .sort(str(NEAR), descending=True)
)
print(wide)
wide.write_csv(out_path("x13_rfi_by_tsp.csv"))

by_type = G.by_entity_type(tsp)
by_group = G.by_entity_type(tsp, col="group")
type_wide = by_type.pivot(on="year", index=["entity_type", "tsps"], values="mw")
share_wide = by_group.pivot(on="year", index="group", values="share")
print(type_wide)
print(share_wide.with_columns(pl.selectors.float().round(3)))
by_type.write_csv(out_path("x13_rfi_entity_type.csv"))

coop = G.co_op_shares(tsp)
print(coop.with_columns(pl.selectors.float().round(3)))
coop.write_csv(out_path("x13_coop_share.csv"))

growth = G.growth_multiple(tsp, start=2027, end=NEAR)
print("MW 2030 / MW 2027 by TSP:\n", growth.with_columns(pl.col("multiple").round(1)))

# Cumulative GW by group, 2026-2030 and the added MW 2027 -> 2030 (where the growth goes)
grp = G.classify(tsp).group_by("group", "year").agg(pl.col("mw").sum())
added = grp.filter(pl.col("year").is_in([2027, NEAR])).pivot(on="year", index="group", values="mw").with_columns(
    (pl.col(str(NEAR)) - pl.col("2027")).alias("added")
)
added = added.with_columns((pl.col("added") / pl.col("added").sum()).alias("share_of_added"))
print("added 2027 -> 2030 by group:\n", added.with_columns(pl.selectors.float().round(3)))

# Baseline: who sells ERCOT's energy today (EIA-861 2024, long form, BA = ERCO)
sales = G.sales_shares(G.load_ercot_sales(), year=EIA_YEAR)
base = sales.join(by_group.filter(pl.col("year") == NEAR).select("group", pl.col("share").alias("rfi_2030")), on="group",
                  how="full", coalesce=True).with_columns((pl.col("rfi_2030") / pl.col("share")).alias("ratio"))
print(f"ERCOT retail sales {EIA_YEAR} vs RFI {NEAR} by group:\n", base.with_columns(pl.selectors.float().round(3)))
base.write_csv(out_path("x13_sales_vs_rfi.csv"))

# %% [markdown]
# ## Q2 — G&T -> members (non-partner universe only)

# %%
gt_all = G.gt_members(G.load_gt_accounts())
members = gt_all.filter(pl.col("account_id").is_in(ids)).join(names, on="account_id")
in_rfi = members.filter(pl.col("tsp").is_not_null())
print("accounts with a supplier in the RFI:", in_rfi["account_id"].n_unique(), "of", N)
gt_table = (
    in_rfi.group_by("tsp")
    .agg(
        pl.col("account_id").n_unique().alias("members"),
        (pl.col("account_type") == "coop").sum().alias("coops"),
        (pl.col("account_type") == "muni").sum().alias("munis"),
        (pl.col("n_gt") > 1).sum().alias("with_two_suppliers"),
        pl.col("name").sort().str.join("; ").alias("names"),
    )
    .join(tsp.filter(pl.col("year") == NEAR).select(pl.col("name").alias("tsp"), pl.col("mw").alias("mw_2030")), on="tsp")
    .sort("mw_2030", descending=True)
)
print(gt_table.select(pl.exclude("names")))
gt_table.write_csv(out_path("x13_gt_members.csv"))
# ERCOT accounts outside the analysed universe (no EIA match), per supplier, counted only
outside = gt_all.filter(~pl.col("account_id").is_in(ids), ~pl.col("account_id").is_in(held_ids), pl.col("tsp").is_not_null())
print("ERCOT accounts with an RFI supplier but outside the universe (no EIA match):", outside["account_id"].n_unique())
not_rfi = members.filter(pl.col("tsp").is_null()).group_by("gt").agg(pl.col("account_id").n_unique().alias("n"))
print("universe accounts whose supplier is not an RFI TSP:\n", not_rfi)
no_supplier = names.filter(~pl.col("account_id").is_in(members["account_id"].to_list()))
print("universe accounts without a supplier in PUCT's field:", no_supplier.group_by("account_type").len())

# %% [markdown]
# ### Allocation variants (assumptions, not observations)
#
# Denominators are the analysed (non-partner) members of each supplier, so each member's share is an upper
# bound. Weights: equal; territory area (km², all counties); EIA customers (2024, long or short form, X4);
# expected new data-center sites since 2025 (TCEQ sites per county × county_share, Q3's `dc_sites`).

# %%
links = A.load_links().filter(pl.col("account_id").is_in(ids))
area = links.group_by("account_id").agg(pl.col("overlap_km2").sum().alias("area_km2"))
long_tot = S.long_form_totals(S.load_long_sales())
short_tot = S.short_form_totals(S.load_short_form(RAW))
cust = (
    S.combine_forms(long_tot, short_tot)
    .filter(pl.col("data_year") == EIA_YEAR, ~pl.col("early_release"))
    .select(pl.col("utility_id").alias("eia_utility_id"), pl.col("customers"))
    .join(universe.select("account_id", "eia_utility_id"), on="eia_utility_id")
    .group_by("account_id").agg(pl.col("customers").sum())
)
dcs = A.data_center_sites(A.load_dc_sites(), links, since=DC_SINCE).select("account_id", "dc_sites")
w = (
    in_rfi.select("account_id", "name", "account_type", "tsp", "n_gt", "via")
    .join(area, on="account_id", how="left")
    .join(cust, on="account_id", how="left")
    .join(dcs, on="account_id", how="left")
    .with_columns(pl.col("dc_sites").fill_null(0.0), pl.lit(1.0).alias("equal"))
)
gt_mw = tsp.filter(pl.col("year") == NEAR, ~pl.col("is_total")).select(pl.col("name").alias("tsp"), "mw")
methods = {"equal": "equal", "area": "area_km2", "customers": "customers", "dc_sites": "dc_sites"}
alloc = w.select("account_id", "name", "account_type", "tsp", "n_gt")
for m, col in methods.items():
    a = G.allocate(gt_mw, w.select("account_id", "tsp", "n_gt", pl.col(col).alias("weight")))
    alloc = alloc.join(a.select("account_id", "tsp", pl.col("mw").alias(m)), on=["account_id", "tsp"], how="left")
alloc = G.method_spread(alloc.with_columns(pl.col(list(methods)).fill_null(0.0)), list(methods))
alloc = alloc.filter(pl.col("tsp") != "CPS")  # its own TSP: nothing to allocate
alloc.write_csv(out_path("x13_member_allocation.csv"))
print("customers missing:", w.filter(pl.col("customers").is_null()).select("name").to_series().to_list())

for t in ["Brazos", "Golden Spread", "LCRA", "Rayburn", "STEC (South Texas)"]:
    sub = alloc.filter(pl.col("tsp") == t)
    print(f"\n{t}: {sub.height} analysed members; top 5 by each method (MW, 2030):")
    for m in methods:
        top = sub.sort(m, descending=True).head(5)
        print(f"  {m:10s}", "; ".join(f"{n} {v:,.0f}" for n, v in zip(top["name"], top[m], strict=True)))
    varied = [m for m in methods if m != "equal"]  # "equal" is constant apart from two-supplier members
    rho = sub.select(
        *[pl.corr(a, b, method="spearman").alias(f"{a}~{b}") for i, a in enumerate(varied) for b in varied[i + 1:]]
    )
    print("  spearman between methods:", {k: round(v, 2) if v is not None else None for k, v in rho.row(0, named=True).items()})
    top1 = {m: sub.sort(m, descending=True)["name"][0] for m in methods}
    print("  largest member by method:", top1)
    print("  members with 0 MW under dc_sites:", sub.filter(pl.col("dc_sites") == 0).height,
          "| share of G&T MW in the top member under dc_sites:",
          round(sub["dc_sites"].max() / sub["dc_sites"].sum(), 2) if sub["dc_sites"].sum() > 0 else None)
    print("  median max/min ratio over equal/area/customers:",
          round(sub.select((pl.max_horizontal("equal", "area", "customers") /
                            pl.min_horizontal("equal", "area", "customers")).median()).item(), 1))

# %% [markdown]
# ### Where ERCOT itself names large-load counties (Batch Zero, Sep 2026, observed MW by county)
#
# Which analysed accounts cover ≥ 20% of those counties, and whose supplier they are. The TSP of each county's
# loads is not in the data: this only shows that named MW sit in co-op-served counties.

# %%
cv = ll.load_chart_values()
bz = cv.filter((pl.col("document") == BZ_DOC) & pl.col("chart_title").str.contains("(?i)counties"))
bz = bz.pivot(on="status_label", index="category", values="value_mw", aggregate_function="max").rename(
    {"category": "county_name"}
)
geo = read_sql("select county_fips, county_name from tx_counties")
bz = bz.join(geo, on="county_name", how="left")
print(bz)
named_all = bz.join(links.select("account_id", "county_fips", "county_share"), on="county_fips", how="left")
named_cover = named_all.filter(pl.col("county_share") >= 0.20)
named_rows = (
    named_cover.join(names, on="account_id")
    .join(members.select("account_id", "gt", "tsp"), on="account_id", how="left")
    .select("county_name", *[c for c in bz.columns if c not in ("county_name", "county_fips")], "name", "account_type",
            pl.col("county_share").round(2), "gt", "tsp")
    .sort("county_name")
)
print(named_rows)
named_rows.write_csv(out_path("x13_named_counties.csv"))
coop_named = named_all.join(names, on="account_id", how="left").group_by("county_name").agg(
    pl.col("county_share").filter(pl.col("account_type") == "coop").sum().alias("coop_share_of_land"),
    pl.col("county_share").filter(pl.col("account_type") == "muni").sum().alias("muni_share_of_land"),
)
print("share of each named county's land in analysed co-ops / munis (raw county_share sums):\n",
      coop_named.sort("county_name").with_columns(pl.selectors.float().round(2)))

# %% [markdown]
# ## Q3 — the per-account fact and its coverage

# %%
facts = G.exposure_facts(in_rfi, tsp, near=NEAR, far=FAR).join(names, on="account_id")
facts.write_csv(out_path("x13_exposure_facts.csv"))
print(facts.select("name", "tsp", "fact").head(3).rows())
per_acct = facts.group_by("account_id").agg(
    pl.col("mw_near").max().alias("max_mw_near"), pl.col("tsp").sort().str.join("+").alias("tsps")
).join(names, on="account_id")
cov = per_acct.group_by("account_type").agg(
    pl.len().alias("with_fact"), (pl.col("max_mw_near") >= 1000).sum().alias("ge_1gw_2030")
)
print(cov, "of", names.group_by("account_type").len())
dist = per_acct.group_by("tsps").len().sort("len", descending=True)
dist = dist.with_columns((pl.col("len") / N).alias("share_of_107"))
print("distinct fact values across the 107:\n", dist)
print("accounts without the fact:", N - per_acct.height, "| largest group sharing one value:", dist["len"].max(),
      f"({dist['len'].max() / N:.0%})")

rank_file = out_path("x5_ranking.csv")
if rank_file.exists():
    rank = pl.read_csv(rank_file, schema_overrides={"account_id": pl.Utf8}).filter(pl.col("account_id").is_in(ids))
    x = rank.join(per_acct.select("account_id", "tsps", "max_mw_near"), on="account_id", how="left").with_columns(
        pl.col("tsps").fill_null("none"),
        pl.col("n_strong").fill_null(0),
    )
    by_gt = x.group_by("tsps").agg(
        pl.len().alias("n"),
        pl.col("rank").median().alias("median_rank"),
        (pl.col("tier") == "A").sum().alias("tier_a"),
        (pl.col("n_strong") > 0).sum().alias("with_strong"),
        (pl.col("next_action") == "call_now").sum().alias("call_now"),
        pl.col("triggers").fill_null("").str.contains("dc_permit").sum().alias("dc_permit"),
    ).sort("n", descending=True)
    print("X5 ranking by supplier (non-partner accounts):\n", by_gt)
    by_gt.write_csv(out_path("x13_coverage.csv"))
    rho = x.filter(pl.col("max_mw_near").is_not_null()).select(pl.corr("rank", "max_mw_near", method="spearman")).item()
    print("spearman(rank, supplier MW 2030) among accounts with the fact:", round(rho, 2))
    only_context = x.filter(pl.col("max_mw_near") >= 1000, pl.col("n_strong") == 0)
    print("accounts with the ≥ 1 GW fact and no active strong trigger:", only_context.height,
          "| tier A among them:", only_context.filter(pl.col("tier") == "A").height)

    # What if the RFI fact were a strong trigger (filed 2026-04-15, 164 days before as-of)?
    age = (T.AS_OF - FILED).days
    sim = x.with_columns((pl.col("max_mw_near").fill_null(0) >= 1000).alias("_fires")).with_columns(
        (pl.col("n_strong") + pl.col("_fires").cast(pl.Int64)).alias("n_strong_sim"),
        pl.when(pl.col("_fires")).then(pl.min_horizontal(pl.col("strong_age_days").fill_null(age), pl.lit(age)))
        .otherwise(pl.col("strong_age_days")).alias("age_sim"),
    )
    sim = sim.with_columns(
        pl.struct("tier", "n_strong_sim", "age_sim").map_elements(
            lambda r: T.next_action(r["tier"], r["n_strong_sim"], r["age_sim"]), return_dtype=pl.Utf8
        ).alias("next_action_sim")
    )
    moves = sim.filter(pl.col("next_action") != pl.col("next_action_sim")).group_by("next_action", "next_action_sim", "tsps").len()
    print("next-action changes if tsp_large_load were strong:\n", moves.sort("len", descending=True))
    print("call_now:", x.filter(pl.col("next_action") == "call_now").height, "->",
          sim.filter(pl.col("next_action_sim") == "call_now").height)
else:
    print("x5_ranking.csv not found: run analysis/x5_triggers.py first for the cross-tab")
