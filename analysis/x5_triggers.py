# %% [markdown]
# # X5 — Account triggers ("why now") and a dry run of the account ranking
#
# Defines the dated public events that make *now* the moment to call an account, measures how often each one
# fires, and runs the proposed score (`config/account_score.yaml`, pending Pablo) on the universe with the
# next-action rules. Numbers go to `docs/analysis/x5_triggers.md`; logic in
# `basecast_pipelines/models/triggers.py`.
#
# Validation lock: the five partner accounts are held out **before** anything is computed, exactly as in
# `q3_signals.py`. Every statistic, ranking and example covers the other 107 accounts.
#
# Run: `uv run --group analysis python analysis/x5_triggers.py` (after `q2_universe.py`).

# %%
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis._common import ROOT, out_path, read_sql
from basecast_pipelines.models import accounts as A
from basecast_pipelines.models import data_centers as DC
from basecast_pipelines.models import triggers as T

AS_OF = T.AS_OF
POP_START, POP_END = 2020, 2025
PERMIT_YEARS = [2023, 2024, 2025]
ZONE_START, ZONE_END = 2025, 2031
DC_SINCE = date(2025, 1, 1)
GEN_FUELS = ("storage", "gas")  # dispatchable capacity; solar/wind IAs are reported apart below

# %% [markdown]
# ## Universe (Q2) minus the held-out partner accounts (same approach as Q3)

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

names = universe.join(accounts.select("account_id", "name"), on="account_id").select(
    "account_id", "name", "account_type"
)
links = A.load_links().filter(pl.col("account_id").is_in(ids))

# %% [markdown]
# ## Signals and the proposed score (dry run)

# %%
score_cfg = yaml.safe_load((ROOT / "config" / "account_score.yaml").read_text())
weights = {k: v["weight"] for k, v in score_cfg["signals"].items()}
directions = {k: v["direction"] for k, v in score_cfg["signals"].items()}
assert abs(sum(weights.values()) - 1) < 1e-9

pop_growth = A.population_growth(A.load_population(), links, start=POP_START, end=POP_END)
permits = A.load_permits_annual(PERMIT_YEARS)
permit_units = permits.group_by("county_fips").agg(pl.col("units_total").sum().alias("units"))
signals = A.build_signals(
    universe,
    links,
    population=pop_growth,
    permits=A.permits_per_1k(permit_units, pop_growth, links),
    housing=A.owner_single_family(A.load_housing(), links),
    dc=A.data_center_sites(A.load_dc_sites(), links, since=DC_SINCE),
    zone=A.zone_peak_growth(A.load_zone_peaks(), A.load_county_zone(), links, start=ZONE_START, end=ZONE_END),
)
pct = T.percentile_ranks(signals, directions)
scored = T.score_accounts(pct, weights).join(names, on="account_id")
scored = scored.with_columns(
    pl.col("rank").map_elements(lambda r: T.score_tier(r, N), return_dtype=pl.Utf8).alias("tier")
)
print(scored.group_by("tier").len().sort("tier"))

# %% [markdown]
# ## Trigger events, mapped to accounts

# %%
geo = T.load_geography()
agreements = T.dev_agreement_events(T.load_agreements(), min_value=1_000_000.0)
latest_permit_month = read_sql(
    "select max(period_start) m from census_permits_county where period_type = 'monthly'"
)["m"][0]
print("latest monthly permits:", latest_permit_month)

prices = T.residential_price(T.load_eia_residential())
parts = [
    T.events_by_county(T.dc_permit_events(DC.load_sites()), links),
    T.events_by_county(T.gen_storage_events(T.load_gis_projects(), geo, min_mw=50.0, fuels=GEN_FUELS), links),
    T.events_by_county(T.county_agreements(agreements), links),
    T.city_agreement_accounts(agreements, names),
    T.registration_events(T.load_market_participants(), names),
    T.permit_surge_events(
        T.load_permits_monthly(latest_permit_month.year - 3), links, as_of_month=latest_permit_month
    ),
    T.events_by_county(T.transmission_events(T.load_tpit(), min_kv=138.0), links),
    T.rate_increase_events(prices, universe, year=2025, base_year=2024, threshold=0.10),
    T.tsp_large_load_events(
        pl.concat([T.load_gt(), T.self_tsp_rows(names)]), T.load_rfi(), target_year=2030, min_mw=1000.0
    ),
]
cols = ["account_id", *T.EVENT_COLUMNS, "exposure"]
events = (
    pl.concat([p.select(cols) for p in parts], how="vertical_relaxed")
    .filter(pl.col("account_id").is_in(ids))  # validation lock: held-out accounts never enter
    .unique(subset=["account_id", "trigger", "source_ref"], keep="first")
    .join(names.select("account_id", "name", "account_type"), on="account_id")
)
events.write_csv(out_path("x5_trigger_events.csv"))
print(events.group_by("trigger").len().sort("trigger"))

# %% [markdown]
# ### Coverage: accounts that ever fire, that fire in the last 12 months, recency

# %%
coverage = T.trigger_coverage(events, N)
by_type = (
    T.active_events(events)
    .group_by("trigger", "account_type")
    .agg(pl.col("account_id").n_unique().alias("n"))
    .pivot(on="account_type", index="trigger", values="n")
)
coverage = coverage.join(by_type, on="trigger", how="left").with_columns(pl.col("coop", "muni").fill_null(0))
with pl.Config(float_precision=3):
    print(coverage)
coverage.write_csv(out_path("x5_trigger_coverage.csv"))
active = T.active_events(events)
print("accounts with ≥ 1 active trigger:", active["account_id"].n_unique(), "of", N)
print("accounts with ≥ 1 active STRONG trigger:",
      active.filter(pl.col("trigger").is_in(list(T.STRONG)))["account_id"].n_unique(), "of", N)

# Event dates by quarter for the dated county triggers (recency / cadence)
print(
    T.active_events(events, window_days=3 * 365)
    .with_columns(pl.col("event_date").dt.truncate("1q").alias("q"))
    .group_by("trigger", "q")
    .agg(pl.col("source_ref").n_unique().alias("events"))
    .pivot(on="trigger", index="q", values="events")
    .sort("q")
)

# %% [markdown]
# ### Threshold sensitivity for the county mapping (active accounts by MIN_COUNTY_SHARE)

# %%
county_sources = {
    "dc_permit": T.dc_permit_events(DC.load_sites()),
    "gen_storage_ia": T.gen_storage_events(T.load_gis_projects(), geo, min_mw=50.0, fuels=GEN_FUELS),
    "dev_agreement": T.county_agreements(agreements),
    "new_transmission": T.transmission_events(T.load_tpit(), min_kv=138.0),
}
rows = []
for share in (0.05, 0.10, 0.20, 0.30, 0.50):
    for k, ev in county_sources.items():
        act = T.active_events(T.events_by_county(ev, links, min_share=share))
        rows.append({"trigger": k, "min_share": share, "accounts_12m": act["account_id"].n_unique()})
print(pl.DataFrame(rows).pivot(on="min_share", index="trigger", values="accounts_12m"))

# Noise checks on the two broad triggers
dev_act = T.active_events(agreements.filter(pl.col("county_fips").is_not_null()))
for v in (1e6, 1e7, 5e7):
    sub = dev_act.filter(pl.col("detail").str.extract(r"\$([0-9.]+)M").cast(pl.Float64) * 1e6 >= v)
    print(f"dev agreements active, incentives ≥ ${v/1e6:.0f}M: {sub['source_ref'].n_unique()} "
          f"({sub.filter(pl.col('source_ref').str.starts_with('ch312'))['source_ref'].n_unique()} Ch. 312)")
all_fuels = T.active_events(T.events_by_county(T.gen_storage_events(T.load_gis_projects(), geo, min_mw=50.0), links))
print("gen IA ≥ 50 MW, all fuels: accounts active", all_fuels["account_id"].n_unique(),
      "| storage+gas only:", T.active_events(T.events_by_county(county_sources["gen_storage_ia"], links))["account_id"].n_unique())
city_380 = T.active_events(T.city_agreement_accounts(agreements, names))
print("city Ch. 380 → own muni, active:", city_380["account_id"].n_unique(), "munis,", city_380["source_ref"].n_unique(), "agreements")
tx_act = T.active_events(county_sources["new_transmission"])
print("new TPIT projects ≥ 138 kV in 12 months:", tx_act["source_ref"].n_unique(),
      "| by first-listed snapshot:", tx_act.group_by("event_date").agg(pl.col("source_ref").n_unique()).sort("event_date").rows())

# %% [markdown]
# ### Examples (most recent active events per trigger, held-out accounts excluded)

# %%
with pl.Config(fmt_str_lengths=60, tbl_rows=40):
    print(
        active.sort("event_date", descending=True)
        .group_by("trigger", maintain_order=True)
        .head(3)
        .select("trigger", "event_date", "name", "title", "detail", "exposure")
    )

# %% [markdown]
# ### Candidates not implemented: quick facts

# %%
cpa_dc = read_sql("select count(*) n, count(county_fips) with_county, "
                  "sum((effective_date > %(d)s)::int) last12 from cpa_data_centers",
                  {"d": date(AS_OF.year - 1, AS_OF.month, AS_OF.day)})
print("cpa_data_centers:", cpa_dc.rows())
print("cpa_jeti:", read_sql("select count(*) n, count(county_fips) with_county from cpa_jeti").rows())
filings = read_sql("select filing_party, docket, filed_date from puct_filing_documents where filing_party is not null")
parties = filings["filing_party"].unique().to_list()
hits = []
for aid, nm in names.select("account_id", "name").rows():
    best = max(((A.name_score(nm, p), p) for p in parties), default=(0, None))
    if best[0] >= 95:
        hits.append((aid, nm, best[1]))
print(f"accounts that filed in PUCT 58481/58777/59772 (name score ≥ 95): {len(hits)}", [h[1] for h in hits])
assoc = filings.filter(pl.col("filing_party").str.to_uppercase().str.contains("TEXAS ELECTRIC COOPERATIVES|TPPA"))
print("filings by the associations (TEC, TPPA):", assoc.height, "last", assoc["filed_date"].max())
members = read_sql("select year, segment, member_name from ercot_members where segment in ('cooperative','municipal')")
print("ERCOT members 2026 (co-op + muni segments):", members.filter(pl.col("year") == 2026).height)
qcew = read_sql("select year, count(*) n, sum(disclosed::int) disclosed from bls_qcew_county "
                "where area_type = 'county' and industry_code = '518210' group by 1 order by 1 desc limit 2")
print("QCEW 518210 counties (latest years):", qcew.rows())

# %% [markdown]
# ## Ranking dry run: top 15 and bottom 5 with signal ranks, triggers and next action

# %%
summary = T.account_trigger_summary(events)
ranking = (
    scored.join(summary, on="account_id", how="left")
    .join(signals.select("account_id", *weights), on="account_id")
    .with_columns(pl.col("n_strong", "n_context").fill_null(0))
)
ranking = ranking.with_columns(
    pl.struct("tier", "n_strong", "strong_age_days")
    .map_elements(lambda r: T.next_action(r["tier"], r["n_strong"], r["strong_age_days"]), return_dtype=pl.Utf8)
    .alias("next_action"),
    pl.col("triggers").list.join(", ").fill_null("").alias("triggers"),
)
ranking.write_csv(out_path("x5_ranking.csv"))
show = ["rank", "name", "account_type", "score", *[f"pct_{k}" for k in weights], "triggers", "next_action"]
with pl.Config(float_precision=2, fmt_str_lengths=45, tbl_rows=30):
    print(ranking.sort("rank").head(15).select(show))
    print(ranking.sort("rank").tail(5).select(show))
print(ranking.group_by("tier", "next_action").len().sort("tier", "next_action"))

# %% [markdown]
# ### Face validity: what the top 15 look like

# %%
top = ranking.filter(pl.col("rank") <= 15)
print("top 15 by type:", top.group_by("account_type").len().rows())
print("top 15 median owner_sf_homes:", top["owner_sf_homes"].median(), "| universe median:",
      ranking["owner_sf_homes"].median())
print("smallest top-15 accounts by owner_sf_homes:")
print(top.sort("owner_sf_homes").head(3).select("rank", "name", "owner_sf_homes", "pop_growth", "permits_per_1k"))
pop25 = A.load_population().filter(pl.col("year") == POP_END).select("county_fips", pl.col("value").alias("pop"))
top_counties = (
    links.filter(pl.col("account_id").is_in(top["account_id"].to_list()))
    .join(pop25, on="county_fips")
    .with_columns((pl.col("pop") * pl.col("county_share")).alias("apportioned_pop"))
    .sort("apportioned_pop", descending=True)
    .group_by("account_id", maintain_order=True)
    .agg(
        (pl.col("apportioned_pop").first() / pl.col("apportioned_pop").sum()).alias("top_county_pop_share"),
        pl.col("county_fips").head(2),
    )
    .explode("county_fips")
    .join(geo, on="county_fips")
    .group_by("account_id", "top_county_pop_share")
    .agg(pl.col("county_name").alias("main_counties_by_pop"))
    .join(top.select("account_id", "rank", "name"), on="account_id")
    .sort("rank")
)
with pl.Config(fmt_str_lengths=60, float_precision=2):
    print(top_counties.select("rank", "name", "main_counties_by_pop", "top_county_pop_share"))

# Apportionment artifact check: area-apportioned owner-occupied single-family homes vs EIA-861 2024 meters
# (all customer classes, so the ratio should sit well under 1 for a real territory; > 1 means area
# apportionment gave the account more homes than it has meters).
eia = A.eia_account_signals(A.load_eia_sales(), year=2024, base_year=2019).rename({"utility_id": "eia_utility_id"})
art = (
    ranking.join(universe.select("account_id", "eia_utility_id"), on="account_id")
    .join(eia.select("eia_utility_id", "eia_customers"), on="eia_utility_id", how="left")
    .with_columns((pl.col("owner_sf_homes") / pl.col("eia_customers")).alias("homes_per_meter"))
)
art.select("account_id", "name", "rank", "owner_sf_homes", "eia_customers", "homes_per_meter").write_csv(
    out_path("x5_apportionment_check.csv")
)
print("homes_per_meter, accounts with EIA customers:", art["homes_per_meter"].drop_nulls().len(),
      "| median", round(art["homes_per_meter"].median(), 2), "| > 1:", art.filter(pl.col("homes_per_meter") > 1).height)
with pl.Config(float_precision=2, fmt_str_lengths=45):
    print(art.filter(pl.col("rank") <= 15).sort("rank").select(
        "rank", "name", "owner_sf_homes", "eia_customers", "homes_per_meter"))
    print(art.filter(pl.col("homes_per_meter") > 1).sort("homes_per_meter", descending=True).select(
        "rank", "name", "owner_sf_homes", "eia_customers", "homes_per_meter"))
# Same ranking with the size signal capped at the account's meters (where EIA has them), to see who moves
capped = signals.join(art.select("account_id", "eia_customers"), on="account_id", how="left").with_columns(
    pl.min_horizontal("owner_sf_homes", pl.coalesce("eia_customers", "owner_sf_homes")).alias("owner_sf_homes")
)
alt = T.score_accounts(T.percentile_ranks(capped, directions), weights).select("account_id", pl.col("rank").alias("rank_capped"))
moved = ranking.join(alt, on="account_id").with_columns((pl.col("rank_capped") - pl.col("rank")).alias("move"))
print("Spearman base vs homes capped at meters:", round(moved.select(pl.corr("rank", "rank_capped", method="spearman")).item(), 3),
      "| top 15 kept:", moved.filter(pl.col("rank") <= 15, pl.col("rank_capped") <= 15).height)
with pl.Config(fmt_str_lengths=45):
    print(moved.filter((pl.col("rank") <= 15) | (pl.col("rank_capped") <= 15)).sort("rank_capped").select(
        "name", "rank", "rank_capped", "move"))

# Market-registration matches that are not exact core-name equality (review sample)
reg = events.filter(pl.col("trigger") == "market_registration", ~pl.col("detail").str.ends_with("100.0"))
print("registration matches with score < 100:", reg.select("name", "title").unique().rows()[:10])
print("accounts with any registration ever:", events.filter(pl.col("trigger") == "market_registration")["account_id"].n_unique())
print("munis by tier:", ranking.filter(pl.col("account_type") == "muni").group_by("tier").len().sort("tier").rows())

# %% [markdown]
# ## Sensitivity: ± 0.05 weight perturbations and leave-one-signal-out

# %%
stab = T.rank_stability(pct, weights, delta=0.05, top=15)
with pl.Config(float_precision=3, tbl_rows=20):
    print(stab)
stab.write_csv(out_path("x5_sensitivity.csv"))
print("min Spearman, ± 0.05:", stab.filter(~pl.col("variant").str.starts_with("without"))["spearman"].min())
print("min Spearman, leave-one-out:", stab.filter(pl.col("variant").str.starts_with("without"))["spearman"].min())

# Next-action stability under the same perturbations (share of accounts whose action changes)
base_actions = ranking.select("account_id", "next_action")
changes = []
for name, w in T.perturbations(weights, delta=0.05):
    alt = T.score_accounts(pct, w).join(summary, on="account_id", how="left").with_columns(
        pl.col("n_strong").fill_null(0)
    )
    alt = alt.with_columns(
        pl.struct("rank", "n_strong", "strong_age_days")
        .map_elements(lambda r: T.next_action(T.score_tier(r["rank"], N), r["n_strong"], r["strong_age_days"]),
                      return_dtype=pl.Utf8)
        .alias("alt_action")
    ).join(base_actions, on="account_id")
    changes.append({"variant": name, "changed": alt.filter(pl.col("alt_action") != pl.col("next_action")).height})
print(pl.DataFrame(changes))
