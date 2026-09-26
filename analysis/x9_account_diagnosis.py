# %% [markdown]
# # X9 — Per-account diagnosis (/accounts/[id]) prototype
#
# Assembles, for one account, the header, the score breakdown, the trigger timeline, the territory facts, the
# EIA-861 series and the rule-based next action, every value with its source and as-of date. Renders three
# non-partner accounts, measures field coverage over the 107 and lists the data gaps. Logic:
# `basecast_pipelines/models/diagnosis.py`; numbers and the API proposal: `docs/analysis/x9_account_diagnosis.md`.
#
# Validation lock: the five partner accounts are held out **before** anything is computed, exactly as in
# `q3_signals.py` / `x5_triggers.py`. They are never scored, ranked, rendered or counted in the coverage.
#
# Run: `uv run --group analysis python analysis/x9_account_diagnosis.py` (after `x2_adjusted_queue.py` and
# `x3_four_cp.py`, whose outputs it reads; X5's ranking is only used as a cross-check).

# %%
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis._common import ROOT, OUT, out_path, read_sql  # noqa: E402
from basecast_pipelines.models import accounts as A  # noqa: E402
from basecast_pipelines.models import data_centers as DC  # noqa: E402
from basecast_pipelines.models import diagnosis as D  # noqa: E402
from basecast_pipelines.models import eia861_short_form as S  # noqa: E402
from basecast_pipelines.models import four_cp as FC  # noqa: E402
from basecast_pipelines.models import triggers as T  # noqa: E402

AS_OF = T.AS_OF
POP_START, POP_END = 2020, 2025
PERMIT_YEARS = [2023, 2024, 2025]
ZONE_START, ZONE_END = 2025, 2031
DC_SINCE = date(2025, 1, 1)
GEN_FUELS = ("storage", "gas")
RAW = ROOT / "data" / "raw"
PICKS = {  # X5 dry-run positions: #1 overall, the median co-op, a short-form muni with a strong trigger
    "30123": "#1 of the X5 ranking (muni)",
    "30120": "mid-ranked co-op (rank 54 of 107, the median)",
    "30012": "muni on the EIA-861 short form",
}

# %% [markdown]
# ## Universe (Q2) minus the held-out partner accounts (same code as Q3 / X5)

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
assert not set(PICKS) & set(held_ids)

names = universe.join(accounts.select("account_id", "name", "gt_cooperative"), on="account_id").select(
    "account_id", "name", "account_type", "eia_utility_id", "gt_cooperative"
)
links = A.load_links().filter(pl.col("account_id").is_in(ids))
geo = T.load_geography()
county_zone = A.load_county_zone()

# %% [markdown]
# ## Signals and score (the X5 dry run, `config/account_score.yaml`, pending review)

# %%
score_cfg = yaml.safe_load((ROOT / "config" / "account_score.yaml").read_text())
weights = {k: v["weight"] for k, v in score_cfg["signals"].items()}
directions = {k: v["direction"] for k, v in score_cfg["signals"].items()}
pop_growth = A.population_growth(A.load_population(), links, start=POP_START, end=POP_END)
permits = A.load_permits_annual(PERMIT_YEARS)
permit_units = permits.group_by("county_fips").agg(pl.col("units_total").sum().alias("units"))
zone_peaks = A.load_zone_peaks()
signals = A.build_signals(
    universe,
    links,
    population=pop_growth,
    permits=A.permits_per_1k(permit_units, pop_growth, links),
    housing=A.owner_single_family(A.load_housing(), links),
    dc=A.data_center_sites(A.load_dc_sites(), links, since=DC_SINCE),
    zone=A.zone_peak_growth(zone_peaks, county_zone, links, start=ZONE_START, end=ZONE_END),
)
pct = T.percentile_ranks(signals, directions)
scored = T.score_accounts(pct, weights).with_columns(
    pl.col("rank").map_elements(lambda r: T.score_tier(r, N), return_dtype=pl.Utf8).alias("tier")
)
x5 = OUT / "x5_ranking.csv"
if x5.exists():
    prev = pl.read_csv(x5, schema_overrides={"account_id": pl.Utf8}).select("account_id", pl.col("rank").alias("r5"))
    moved = scored.join(prev, on="account_id").filter(pl.col("rank") != pl.col("r5"))
    print("ranks different from X5's CSV:", moved.height)

# %% [markdown]
# ## Trigger events (every date; same sources and thresholds as X5)

# %%
agreements = T.dev_agreement_events(T.load_agreements(), min_value=1_000_000.0)
latest_permit_month = read_sql(
    "select max(period_start) m from census_permits_county where period_type = 'monthly'"
)["m"][0]
permits_monthly = T.load_permits_monthly(latest_permit_month.year - 3)
eia_res = T.load_eia_residential()
res_price = T.residential_price(eia_res)
parts = [
    T.events_by_county(T.dc_permit_events(DC.load_sites()), links),
    T.events_by_county(T.gen_storage_events(T.load_gis_projects(), geo, min_mw=50.0, fuels=GEN_FUELS), links),
    T.events_by_county(T.county_agreements(agreements), links),
    T.city_agreement_accounts(agreements, names),
    T.registration_events(T.load_market_participants(), names),
    T.permit_surge_events(permits_monthly, links, as_of_month=latest_permit_month),
    T.events_by_county(T.transmission_events(T.load_tpit(), min_kv=138.0), links),
    T.rate_increase_events(res_price, universe, year=2025, base_year=2024, threshold=0.10),
    T.tsp_large_load_events(
        pl.concat([T.load_gt(), T.self_tsp_rows(names)]), T.load_rfi(), target_year=2030, min_mw=1000.0
    ),
]
cols = ["account_id", *T.EVENT_COLUMNS, "exposure"]
events = (
    pl.concat([p.select(cols) for p in parts], how="vertical_relaxed")
    .filter(pl.col("account_id").is_in(ids))  # validation lock
    .unique(subset=["account_id", "trigger", "source_ref"], keep="first")
)
print("events:", events.height, "| accounts:", events["account_id"].n_unique())

# %% [markdown]
# ## EIA-861 long + short form (X4), with delivery-only customers

# %%
long_tot = S.long_form_totals(S.load_long_sales())
short_tot = S.short_form_totals(S.load_short_form(RAW))
delivery = S.load_delivery(RAW)
eia = S.add_delivery(
    S.with_price(S.combine_forms(long_tot, short_tot)), delivery, first_year=int(delivery["data_year"].min())
)
print("EIA utility-years:", eia.height, "| years:", eia["data_year"].min(), "–", eia["data_year"].max())

# %% [markdown]
# ## Adjusted queue (X2 project scores → county × stratum) and zone 4CP (X3's `zone_coincidence`)

# %%
proj = pl.read_csv(OUT / "x2_project_scores.csv", schema_overrides={"county_fips": pl.Utf8})
queue = proj.group_by("county_fips", "stratum").agg(
    pl.len().alias("projects"),
    pl.col("capacity_mw").sum().alias("raw_mw"),
    pl.col("mw_2027").sum().alias("adj_mw_2027"),
    pl.col("mw_2028").sum().alias("adj_mw_2028"),
)
x2c = pl.read_csv(OUT / "x2_county_adjusted.csv", schema_overrides={"county_fips": pl.Utf8})
chk = queue.group_by("county_fips").agg(pl.col("raw_mw", "adj_mw_2028").sum()).join(x2c, on="county_fips")
print("X2 county totals reproduced:",
      chk.filter((pl.col("raw_mw") - pl.col("raw_mw_right")).abs() > 0.01).height == 0,
      chk.filter((pl.col("adj_mw_2028") - pl.col("adj_mw_2028_right")).abs() > 0.01).height == 0)
zone_cp = FC.zone_coincidence(FC.load_monthly_peaks())
print("zones: county_weather_zone", sorted(county_zone["weather_zone"].drop_nulls().unique().to_list()),
      "| LTLF", sorted(zone_peaks["region_id"].unique().to_list()),
      "| D&E", sorted(zone_cp["region_id"].unique().to_list()))

# %% [markdown]
# ## Sources and their as-of dates

# %%
q1 = lambda sql: read_sql(sql).row(0)[0]  # noqa: E731
ccn_date = q1("select max(coalesce(data_source_date::date, layer_edited_on::date)) from puct_ccn_territories")
overlap_built = q1("select max(built_at)::date from county_utility_overlap_puct")
czone_built = q1("select max(built_at)::date from county_weather_zone")
pop_ref = q1("select max(ref_date) from census_population_county where series = 'postcensal_v2025'")
peaks_month = q1("select max(month) from ercot_monthly_peaks where value is not null and region_type = 'weather_zone'")
mp_snap = q1("select max(snapshot_date) from ercot_market_participants")
ltlf_date = q1("select max(vintage_date) from ltlf_forecasts where vintage = 'LTLF 2025'")
gis_month = q1("select max(latest_report_month) from gis_project_events")
dc_latest = q1("select max(first_affil_begin_dt) from tceq_data_center_sites")
print(ccn_date, overlap_built, czone_built, pop_ref, peaks_month, mp_snap, ltlf_date, gis_month, dc_latest)

Src = D.Source
sources = {
    "puct_ccn_territories": Src("puct_ccn_territories", ccn_date, "PUCT CCN layer date"),
    "utility_crosswalk": Src("config/utility_crosswalk.yaml", date(2026, 9, 26), "Q2 crosswalk; 5 rows pending review"),
    "county_utility_overlap_puct": Src("county_utility_overlap_puct", overlap_built),
    "county_weather_zone": Src("county_weather_zone", czone_built),
    "eia_861": Src("eia861_sales + 861S (lake)", None, "861S read from the raw zips, not a table yet (X4)"),
    "ercot_market_participants": Src("ercot_market_participants", mp_snap),
    "census_population_county": Src("census_population_county", pop_ref, "PEP vintage 2025"),
    "census_permits_county": Src("census_permits_county", date(2025, 12, 31), "annual BPS 2023–2025, imputed"),
    "census_housing_county": Src("census_housing_county", date(2024, 12, 31), "ACS 2024 5-year B25032"),
    "tceq_data_center_sites": Src("tceq_data_center_sites", None, f"no snapshot date in the table; latest permit {dc_latest}"),
    "ltlf_forecasts": Src("ltlf_forecasts", ltlf_date, "LTLF 2025, ERCOT-adjusted, summer"),
    "ercot_monthly_peaks": Src("ercot_monthly_peaks", peaks_month, "D&E report; recent months not final-settled"),
    "queue_adjusted": Src("gis_project_events → queue_adjusted (X2)", gis_month,
                          "GIS report month; adjusted = expected MW reaching COD (model)"),
}
meta = {
    "pop_growth": ("Population growth 2020→2025", "share", "census_population_county"),
    "owner_sf_homes": ("Owner-occupied single-family homes", "homes", "census_housing_county"),
    "permits_per_1k": ("Permits 2023–2025 per 1,000 residents", "units/1k", "census_permits_county"),
    "dc_sites": ("New data-center sites since 2025 (expected)", "sites", "tceq_data_center_sites"),
    "zone_peak_cagr": ("Zone summer peak CAGR 2025→2031 (LTLF)", "per year", "ltlf_forecasts"),
    "owner_sf_share": ("Owner-occupied single-family share", "share", "census_housing_county"),
}
inp = D.DiagnosisInputs(
    accounts=names, links=links, geo=geo, county_zone=county_zone, signals=signals, scored=scored,
    weights=weights, signal_meta=meta, events=events, eia=eia, res_price=res_price, dc_sites=DC.load_sites(),
    queue=queue, permits_monthly=permits_monthly, permits_month=latest_permit_month, zone_peaks=zone_peaks,
    zone_cp=zone_cp, sources=sources, as_of=AS_OF, dc_since=DC_SINCE, ltlf_years=(ZONE_START, ZONE_END),
)

# %% [markdown]
# ## Every non-partner account: diagnosis, coverage, gaps

# %%
diags = [D.assemble(a, inp) for a in scored["account_id"].to_list()]
for pid in held_ids:  # the guard: a held-out account cannot be assembled
    try:
        D.assemble(pid, inp)
        raise AssertionError("held-out account assembled")
    except KeyError:
        pass
cov = D.coverage(diags)
with pl.Config(float_precision=3, tbl_rows=60, fmt_str_lengths=70):
    print(cov)
cov.write_csv(out_path("x9_coverage.csv"))

gap_rows = [
    {"account_id": d["account_id"], "account_type": d["header"][1]["value"], **g}
    for d in diags
    for g in D.data_gaps(d, stale_days=730)
]
gaps = pl.DataFrame(gap_rows)
gaps.write_csv(out_path("x9_gaps.csv"))
gap_summary = (
    gaps.group_by("key", "kind")
    .agg(pl.col("account_id").n_unique().alias("accounts"),
         (pl.col("account_type") == "coop").sum().alias("coop_rows"),
         (pl.col("account_type") == "muni").sum().alias("muni_rows"))
    .with_columns((pl.col("accounts") / N).alias("share"))
    .sort("accounts", descending=True)
)
with pl.Config(float_precision=3, tbl_rows=60):
    print(gap_summary)
gap_summary.write_csv(out_path("x9_gap_summary.csv"))

# Structural facts behind the gaps
exposed_any = links.group_by("account_id").agg((pl.col("county_share") >= D.MIN_COUNTY_SHARE).any().alias("x")).join(
    names.select("account_id", "account_type"), on="account_id")
print("accounts with ≥ 1 exposed county:", exposed_any.group_by("account_type").agg(pl.col("x").sum(), pl.len()).rows())
last_form = pl.DataFrame([
    {"account_id": d["account_id"], "type": d["header"][1]["value"],
     "form": next(f["value"] for f in d["header"] if f["key"] == "eia_form"),
     "years": d["eia_series"].filter(~pl.col("early_release")).height,
     "has_er": d["eia_series"]["early_release"].any()}
    for d in diags
])
print("EIA form (latest year) by type:", last_form.group_by("type", "form").len().sort("type", "form").rows())
print("EIA final years in 2013–2024 (median, min):", last_form["years"].median(), last_form["years"].min(),
      "| with 2025 early release:", last_form["has_er"].sum())
# rate_increase reads the long-form residential price only: how many accounts can it ever see, and what would
# an all-class price rule (861S included) add? 2024 final → 2025 early release, ≥ +10% (X5's threshold).
px = pl.DataFrame([
    {"account_id": d["account_id"], "type": d["header"][1]["value"],
     "p24": D.series_value(d["eia_series"], 2024, "price_usd_kwh"),
     "r24": D.series_value(d["eia_series"], 2024, "res_price_usd_kwh"),
     "p25": (lambda e: e["price_usd_kwh"][0] if e.height else None)(d["eia_series"].filter(pl.col("early_release"))),
     "r25": (lambda e: e["res_price_usd_kwh"][0] if e.height else None)(d["eia_series"].filter(pl.col("early_release")))}
    for d in diags
]).with_columns((pl.col("p25") / pl.col("p24") - 1).alias("all_chg"), (pl.col("r25") / pl.col("r24") - 1).alias("res_chg"))
print("residential price in 2024 and 2025 ER (rate_increase can see):", px.filter(pl.col("res_chg").is_not_null()).height,
      "| all-class price both years:", px.filter(pl.col("all_chg").is_not_null()).height)
print("all-class ≥ +10% 2024→2025ER, accounts with no residential price:",
      px.filter(pl.col("all_chg") >= 0.10, pl.col("res_chg").is_null()).select("account_id", "type", "all_chg").rows())
ctx_rule = pl.DataFrame([
    {"type": d["header"][1]["value"],
     "rule": "exposed" if d["territory"]["counties"]["exposed"].any() else "home county"} for d in diags
])
print("context-county rule by type:", ctx_rule.group_by("type", "rule").len().sort("type", "rule").rows())
actions = pl.DataFrame([{"a": d["next_action"]["action"]} for d in diags]).group_by("a").len().sort("a")
print("next actions (should match X5):", actions.rows())
chg = pl.DataFrame([
    {"account_id": d["account_id"], "action": d["next_action"]["action"], "changes_on": d["next_action"]["changes_on"],
     "changes_to": d["next_action"]["changes_to"]} for d in diags
]).with_columns((pl.col("changes_on") - pl.lit(AS_OF)).dt.total_days().alias("days"))
print("call-now accounts whose action lapses within 30 / 90 days without a new event:",
      chg.filter(pl.col("action") == "call_now", pl.col("days") <= 30).height,
      chg.filter(pl.col("action") == "call_now", pl.col("days") <= 90).height, "of",
      chg.filter(pl.col("action") == "call_now").height)

# %% [markdown]
# ## The three rendered diagnoses

# %%
by_id = {d["account_id"]: d for d in diags}
md = []
for aid, why in PICKS.items():
    d = by_id[aid]
    md.append(f"<!-- {why} -->\n" + D.render_markdown(d))
    out_path(f"x9_diagnosis_{aid}.json").write_text(D.dumps(d), encoding="utf-8")
    print(D.render_markdown(d))
    print("gaps:", D.data_gaps(d, stale_days=730))
out_path("x9_diagnoses.md").write_text("\n".join(md), encoding="utf-8")
