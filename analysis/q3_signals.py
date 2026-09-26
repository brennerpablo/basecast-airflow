# %% [markdown]
# # Q3 — Are the account signals any good? (`docs/PHASE0_ANALYSIS.md` §2 Q3)
#
# Universe from Q2 (`config/utility_crosswalk.yaml`, auto + accepted review rows). For each candidate signal:
# coverage and distribution, EIA-861 history checks, permit reporting, and pairwise redundancy. Writes the
# proposed `config/account_score.yaml`. Numbers go to `docs/analysis/q3_signals.md`.
#
# Validation lock: the five partner accounts are held out **before** any signal is computed, so no number
# here includes them (the universe below is the Q2 universe minus five accounts).
#
# Run: `uv run --group analysis python analysis/q3_signals.py` (after `q2_universe.py`).

# %%
from __future__ import annotations

import sys
import zipfile
from datetime import date
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis._common import ROOT, out_path  # noqa: E402
from basecast_pipelines.models import accounts as A  # noqa: E402

YEAR, BASE_YEAR = 2024, 2019  # EIA-861: latest final release and the base of the 5-year growth
POP_START, POP_END = 2020, 2025  # Census PEP vintage 2025
PERMIT_YEARS = [2023, 2024, 2025]  # annual BPS files, imputed units
ZONE_START, ZONE_END = 2025, 2031  # LTLF 2025, ERCOT-adjusted summer peak by weather zone
DC_SINCE = date(2025, 1, 1)

# %% [markdown]
# ## Universe (Q2) minus the held-out partner accounts

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
print(f"universe: {universe_all.height} accounts; held out: {len(held_ids)}; analysed: {universe.height}")
print(universe.group_by("account_type").len().sort("account_type"))

links = A.load_links().filter(pl.col("account_id").is_in(universe["account_id"].to_list()))

# %% [markdown]
# ## Signals

# %%
sales = A.load_eia_sales()
pop = A.load_population()
pop_growth = A.population_growth(pop, links, start=POP_START, end=POP_END)
permits = A.load_permits_annual(PERMIT_YEARS)
permit_units = permits.group_by("county_fips").agg(pl.col("units_total").sum().alias("units"))
signals = A.build_signals(
    universe,
    links,
    eia=A.eia_account_signals(sales, year=YEAR, base_year=BASE_YEAR),
    population=pop_growth,
    permits=A.permits_per_1k(permit_units, pop_growth, links),
    housing=A.owner_single_family(A.load_housing(), links),
    dc=A.data_center_sites(A.load_dc_sites(), links, since=DC_SINCE),
    zone=A.zone_peak_growth(
        A.load_zone_peaks(), A.load_county_zone(), links, start=ZONE_START, end=ZONE_END
    ),
)
SIGNALS = [
    "eia_customers",
    "eia_sales_mwh",
    "eia_res_sales_share",
    "eia_customer_cagr",
    "population",
    "pop_growth",
    "permit_units",
    "permits_per_1k",
    "owner_sf_homes",
    "owner_sf_share",
    "dc_sites",
    "zone_peak_cagr",
]
signals.select("account_id", "account_type", *SIGNALS).write_csv(out_path("q3_signals.csv"))

# %% [markdown]
# ### 1. Coverage and distribution

# %%
summary = A.signal_summary(signals, SIGNALS)
with pl.Config(float_precision=4, tbl_cols=14):
    print(summary)
for t in A.ACCOUNT_TYPES:
    sub = A.signal_summary(signals.filter(pl.col("account_type") == t), SIGNALS)
    print(t, dict(zip(sub["signal"], [round(c, 3) for c in sub["coverage"]], strict=True)))

# %% [markdown]
# Projection: the same size signals if `eia861_sales` also carried the 861S short form (totals only), read
# from the raw zip when the local lake has it. Not a table in the database; for the decision only.

# %%
def short_form_totals(year: int) -> pl.DataFrame | None:
    from basecast_pipelines.processing.tabular import read_grid

    hits = sorted((ROOT / "data" / "raw" / "source=eia_861").glob(f"dt=*/f861{year}.zip"))
    if not hits:
        return None
    with zipfile.ZipFile(hits[-1]) as z:
        name = next((n for n in z.namelist() if n.startswith(f"Short_Form_{year}")), None)
        if name is None:
            return None
        grid = read_grid(z.read(name), 0)
    body = grid.slice(1).rename(dict(zip(grid.columns, [str(v) for v in grid.row(0)], strict=True)))
    return body.filter(pl.col("State") == "TX").select(
        pl.col("Utility Number").alias("eia_utility_id"),
        pl.col("Total Customers").cast(pl.Float64, strict=False).alias("sf_customers"),
        pl.col("Total Sales (MWh)").cast(pl.Float64, strict=False).alias("sf_sales_mwh"),
    )


sf = short_form_totals(YEAR)
if sf is not None:
    projected = signals.join(sf, on="eia_utility_id", how="left").with_columns(
        pl.coalesce("eia_customers", "sf_customers").alias("eia_customers"),
        pl.coalesce("eia_sales_mwh", "sf_sales_mwh").alias("eia_sales_mwh"),
    )
    with pl.Config(float_precision=4, tbl_cols=14):
        print(A.signal_summary(projected, ["eia_customers", "eia_sales_mwh"]))

# %% [markdown]
# ### 2. EIA-861 history

# %%
uids = universe["eia_utility_id"].to_list()
totals = A.eia_totals(sales).filter(pl.col("utility_id").is_in(uids))
years = totals.filter(pl.col("customers") > 0).group_by("utility_id").agg(
    pl.len().alias("final_years"), pl.col("data_year").min().alias("first"), pl.col("data_year").max().alias("last")
)
print("final EIA-861 years with customers, per universe utility that has any (2013-2024 possible):")
print(years["final_years"].value_counts().sort("final_years"))
print("last year of the single-year utilities:", years.filter(pl.col("final_years") == 1)["last"].value_counts().rows())
print("universe utilities with no long-form sales in any final year:", len(set(uids) - set(years["utility_id"])))
early = sales.filter(pl.col("early_release"), pl.col("utility_id").is_in(uids), pl.col("sector") == "total")
print("early-release rows (2025, flagged early_release, left out of every signal):",
      early.height, "rows /", early["utility_id"].n_unique(), "utilities")

jumps = pl.concat(
    [
        A.yoy_jumps(totals.select("utility_id", "data_year", pl.col(v).alias("value")), "value").with_columns(
            pl.lit(v).alias("measure")
        )
        for v in ("customers", "sales_mwh")
    ]
)
names = universe.join(accounts.select("account_id", "name"), on="account_id").select(
    pl.col("eia_utility_id").alias("utility_id"), "name"
)
jumps = jumps.join(names, on="utility_id", how="left").select(
    "measure", "utility_id", "name", "prev_year", "data_year", "prev", "value", "change"
)
print(f"year-over-year jumps beyond ±50% ({jumps['utility_id'].n_unique()} utilities):")
with pl.Config(float_precision=3):
    print(jumps.sort("measure", "utility_id", "data_year"))
print("jumps inside the CAGR window", BASE_YEAR, "→", YEAR, ":",
      jumps.filter(pl.col("data_year") > BASE_YEAR, pl.col("measure") == "customers")["utility_id"].n_unique(),
      "utilities")

# %% [markdown]
# ### 3. Building permits: reporting and imputation

# %%
tx_counties = pop.filter(pl.col("year") == POP_END).select("county_fips").unique()
reporting = A.permits_reporting(permits)
universe_counties = links.select("county_fips").unique()
missing_all = tx_counties.join(reporting, on="county_fips", how="anti")
missing_universe = universe_counties.join(reporting, on="county_fips", how="anti")
print(f"Texas counties with no BPS row in {PERMIT_YEARS}: {missing_all.height} of {tx_counties.height}")
print(f"universe counties with no BPS row: {missing_universe.height} of {universe_counties.height}")
tot_units, tot_rep = reporting["units"].sum(), reporting["units_rep"].sum()
print(f"imputed − reported units, Texas {PERMIT_YEARS}: {tot_units - tot_rep:,} of {tot_units:,} "
      f"({1 - tot_rep / tot_units:.1%} imputed)")
share = reporting.filter(pl.col("units") > 0)["imputed_share"]
print("per-county imputed share: p10 / median / p90 / max =",
      [round(share.quantile(q, "linear"), 3) for q in (0.1, 0.5, 0.9)], round(share.max(), 3))
print("counties fully imputed (reported = 0, imputed > 0):",
      reporting.filter(pl.col("units") > 0, pl.col("units_rep") == 0).height)
acct_imputed = (
    links.join(reporting, on="county_fips", how="left")
    .group_by("account_id")
    .agg(
        (pl.col("units") * pl.col("county_share")).sum().alias("u"),
        (pl.col("units_rep") * pl.col("county_share")).sum().alias("r"),
    )
    .filter(pl.col("u") > 0)
    .with_columns((1 - pl.col("r") / pl.col("u")).alias("imputed_share"))
)
print("per-account imputed share of apportioned permits: median / p90 / max =",
      [round(acct_imputed["imputed_share"].quantile(q, "linear"), 3) for q in (0.5, 0.9)],
      round(acct_imputed["imputed_share"].max(), 3))

# %% [markdown]
# ### 4. Redundancy (Spearman on the accounts that have both)

# %%
pairs = A.redundant_pairs(signals, SIGNALS)
with pl.Config(float_precision=3):
    print(pairs.filter(pl.col("redundant")).sort("spearman", descending=True))
    print("other pairs with |ρ| ≥ 0.6:")
    print(pairs.filter(~pl.col("redundant"), pl.col("spearman").abs() >= 0.6).sort("spearman", descending=True))
pairs.write_csv(out_path("q3_signal_pairs.csv"))
if sf is not None:
    proj_pairs = A.redundant_pairs(projected, ["eia_customers", "owner_sf_homes", "population", "eia_sales_mwh"])
    with pl.Config(float_precision=3):
        print("size signals with the short form projected:")
        print(proj_pairs)

# %% [markdown]
# ## Decision

# %%
entering = summary.filter(pl.col("enters"))["signal"].to_list()
print("pass coverage ≥ 80% and not near-constant:", entering)
print("fail:", summary.filter(~pl.col("enters")).select("signal", "coverage", "mode_share").rows())

# %% [markdown]
# Check the proposed `config/account_score.yaml` against the rules: every weighted signal passes, no two
# weighted signals are redundant, weights sum to 1.

# %%
import yaml  # noqa: E402

score_cfg = yaml.safe_load((ROOT / "config" / "account_score.yaml").read_text())
weights = {k: v["weight"] for k, v in score_cfg["signals"].items()}
print("proposed weights:", weights, "sum =", round(sum(weights.values()), 6))
assert abs(sum(weights.values()) - 1) < 1e-9
assert set(weights) <= set(entering), f"weighted signals that fail Q3: {set(weights) - set(entering)}"
both = pairs.filter(pl.col("a").is_in(list(weights)), pl.col("b").is_in(list(weights)), pl.col("redundant"))
assert both.is_empty(), both
print("weighted pairs, max |ρ|:",
      pairs.filter(pl.col("a").is_in(list(weights)), pl.col("b").is_in(list(weights)))
      .sort(pl.col("spearman").abs(), descending=True).head(3).rows())
