"""Account marts (A-M3): the ranked co-ops and munis, their events and counties, and the per-account diagnosis.

Port of ``analysis/x9_account_diagnosis.py`` (a superset of ``x5_triggers.py``): the universe (Q2) minus the
held-out accounts, dropped by name **before** any signal; the signals and the score (``config/account_score.yaml``,
or X4's weights with ``scoring.weights_set: x4``); the nine event sources; the EIA-861 long and short form (X4);
the adjusted queue (X2, ``marts/queue.py``) and the zone 4CP (X3); then ``diagnosis.assemble`` per account. Every
call gets the run's ``as_of``.

Not built yet (P1): the X10 muni extras (``munis.*``), the G&T card (X13) and the 4CP offer (X3 + X15);
``triggers.gen_storage_ia: context`` (R12) is not built either.
"""

from __future__ import annotations

import json
from datetime import date
from typing import Any

import polars as pl
import yaml

from basecast_pipelines.config import PROJECT_ROOT
from basecast_pipelines.marts import config as marts_config
from basecast_pipelines.marts import holdout
from basecast_pipelines.marts import queue as marts_queue
from basecast_pipelines.marts.core import Mart, MartContext, value_check

GOLDEN_AS_OF = date(2026, 9, 26)  # X5 / X9's run date
POP_START, POP_END = 2020, 2025
PERMIT_YEARS = [2023, 2024, 2025]
ZONE_START, ZONE_END = 2025, 2031
DC_SINCE = date(2025, 1, 1)
GEN_FUELS = ("storage", "gas")  # dispatchable capacity (X5)
RAW = PROJECT_ROOT / "data" / "raw"
N_ACCOUNTS = 107  # 112 in the universe minus the five held out
MIN_LISTED_SHARE = 0.01  # mart_account_counties keeps counties the account covers ≥ 1% of (plus its context county)

# X4's proposal (docs/analysis/x4_eia861_short_form.md §3): eia_customers replaces owner_sf_homes, and
# eia_customer_cagr takes 0.10 from pop_growth.
X4_WEIGHTS = {"pop_growth": 0.15, "eia_customers": 0.20, "eia_customer_cagr": 0.10, "permits_per_1k": 0.15,
              "dc_sites": 0.15, "zone_peak_cagr": 0.15, "owner_sf_share": 0.10}
SIGNAL_META = {  # signal -> (label, unit, source key), as in X9
    "pop_growth": ("Population growth 2020→2025", "share", "census_population_county"),
    "owner_sf_homes": ("Owner-occupied single-family homes", "homes", "census_housing_county"),
    "permits_per_1k": ("Permits 2023–2025 per 1,000 residents", "units/1k", "census_permits_county"),
    "dc_sites": ("New data-center sites since 2025 (expected)", "sites", "tceq_data_center_sites"),
    "zone_peak_cagr": ("Zone summer peak CAGR 2025→2031 (LTLF)", "per year", "ltlf_forecasts"),
    "owner_sf_share": ("Owner-occupied single-family share", "share", "census_housing_county"),
    "eia_customers": ("EIA-861 meters 2024 (incl. delivery-only)", "customers", "eia_861"),
    "eia_customer_cagr": ("EIA-861 meter growth 2019→2024", "per year", "eia_861"),
}

# The glossary the app shows for chips, flags and actions (GET /glossary reads it from mart_meta).
TRIGGER_CHIPS = {
    "dc_permit": "Data-center permit", "gen_storage_ia": "Storage/gas IA nearby", "dev_agreement": "Dev agreement",
    "market_registration": "ERCOT registration", "permit_surge": "Permit surge",
    "new_transmission": "New transmission", "rate_increase": "Rate increase",
    "tsp_large_load": "G&T large-load requests",
}
FLAGS = {
    "no_exposed_county": ("No exposed county", "No county where the territory covers ≥ 20% of the land: county "
                          "events (data centers, IAs, permits, transmission) never reach the account; its context is "
                          "the home county."),
    "apportionment_under": ("Homes under-counted", "Area apportionment under-counts the territory (a city inside a "
                            "large county): population, homes and permit counts run low."),
    "apportionment_over": ("Homes over-counted", "Area apportionment gives the territory more homes than it has EIA "
                           "meters: size and growth signals are over-counted."),
    "short_form": ("EIA short form", "Files the EIA-861 short form: totals only, no residential split or "
                   "residential price."),
    "eia_break": ("EIA series break", "The EIA meter count moves 50% or more in one year: a reporting change, so "
                  "read the meter growth with care."),
}
ACTIONS = {
    "call_now": ("Call now", "Tier A with an active strong trigger; or tier B with two, or with one from the last 90 "
                 "days."),
    "nurture": ("Nurture", "Tier A without an active strong trigger; or tier B with one strong trigger older than 90 "
                "days."),
    "watch": ("Watch (alert)", "Tier B without an active strong trigger; or tier C with an active strong trigger: "
              "set an alert."),
    "hold": ("Hold", "Tier C without an active strong trigger."),
}


# --- inputs (shared by the account marts in one run) ---------------------------------------------------------


def _weights(ctx: MartContext) -> tuple[dict[str, float], dict[str, str], str]:
    score_cfg = yaml.safe_load((PROJECT_ROOT / "config" / "account_score.yaml").read_text())
    weights = {k: v["weight"] for k, v in score_cfg["signals"].items()}
    directions = {k: v["direction"] for k, v in score_cfg["signals"].items()}
    weights_set = marts_config.value(ctx.config, "scoring.weights_set")
    if weights_set == "x4":
        return X4_WEIGHTS, {k: directions.get(k, "higher") for k in X4_WEIGHTS}, weights_set
    return weights, directions, weights_set


def diagnosis_inputs(ctx: MartContext):
    """``(DiagnosisInputs, weights_set)``, built once per run."""
    return ctx.cached("accounts.inputs", lambda: _inputs(ctx))


def _inputs(ctx: MartContext):
    from basecast_pipelines.models import accounts as A
    from basecast_pipelines.models import data_centers as DC
    from basecast_pipelines.models import diagnosis as D
    from basecast_pipelines.models import eia861_short_form as S
    from basecast_pipelines.models import four_cp as FC
    from basecast_pipelines.models import triggers as T

    cfg = ctx.config
    if marts_config.value(cfg, "triggers.gen_storage_ia") != "strong":
        raise NotImplementedError("triggers.gen_storage_ia: context (R12) is not built")
    if marts_config.value(cfg, "munis.place_facts") or marts_config.value(cfg, "munis.place_permit_trigger"):
        raise NotImplementedError("munis.* (X10 extras, R15) are P1 and not built")

    # Universe minus the held-out accounts, by name, before anything else (validation lock).
    accounts = A.load_accounts()
    crosswalk = A.read_crosswalk(PROJECT_ROOT / "config" / "utility_crosswalk.yaml")
    universe_all = A.build_universe(accounts, crosswalk, intersect=True)
    universe = holdout.without_held_out(universe_all, accounts, marts_config.held_out_names(cfg))
    ids = universe["account_id"].to_list()
    n = universe.height
    names = universe.join(accounts.select("account_id", "name", "gt_cooperative"), on="account_id").select(
        "account_id", "name", "account_type", "eia_utility_id", "gt_cooperative"
    )
    links = A.load_links().filter(pl.col("account_id").is_in(ids))
    geo = T.load_geography()
    county_zone = A.load_county_zone()

    # EIA-861 long + short form, with delivery-only customers (X4)
    long_tot = S.long_form_totals(S.load_long_sales())
    short_tot = S.short_form_totals(S.load_short_form(RAW))
    delivery = S.load_delivery(RAW)
    eia = S.add_delivery(
        S.with_price(S.combine_forms(long_tot, short_tot)), delivery, first_year=int(delivery["data_year"].min())
    )

    # Signals and score
    weights, directions, weights_set = _weights(ctx)
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
    if weights_set == "x4":
        x4 = S.utility_signals(eia, year=2024, base_year=2019, customers="wires_customers").select(
            pl.col("utility_id").alias("_uid"), "eia_customers", "eia_customer_cagr"
        )
        signals = (
            signals.join(names.select("account_id", pl.col("eia_utility_id").alias("_uid")), on="account_id", how="left")
            .join(x4, on="_uid", how="left")
            .drop("_uid")
        )
    pct = T.percentile_ranks(signals, directions)
    scored = T.score_accounts(pct, weights).with_columns(
        pl.col("rank").map_elements(lambda r: T.score_tier(r, n), return_dtype=pl.Utf8).alias("tier")
    )

    # Trigger events, every date (same sources and thresholds as X5 / X9)
    agreements = T.dev_agreement_events(T.load_agreements(), min_value=1_000_000.0)
    latest_permit_month = ctx.read_sql(
        "select max(period_start) m from census_permits_county where period_type = 'monthly'"
    )["m"][0]
    permits_monthly = T.load_permits_monthly(latest_permit_month.year - 3)
    res_price = T.residential_price(T.load_eia_residential())
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
        .filter(pl.col("account_id").is_in(ids))  # validation lock: held-out accounts never enter
        .unique(subset=["account_id", "trigger", "source_ref"], keep="first")
    )

    # Adjusted queue (X2) and zone 4CP (X3)
    queue = marts_queue.county_stratum(marts_queue.adjusted_queue(ctx))
    zone_cp = FC.zone_coincidence(FC.load_monthly_peaks())

    # Sources and their as-of dates (X9)
    def one(sql: str):
        return ctx.read_sql(sql).row(0)[0]

    dc_latest = one("select max(first_affil_begin_dt) from tceq_data_center_sites")
    Src = D.Source
    sources = {
        "puct_ccn_territories": Src("puct_ccn_territories", one(
            "select max(coalesce(data_source_date::date, layer_edited_on::date)) from puct_ccn_territories"),
            "PUCT CCN layer date"),
        "utility_crosswalk": Src("config/utility_crosswalk.yaml", date(2026, 9, 26),
                                 "Q2 crosswalk; 5 rows pending review"),
        "county_utility_overlap_puct": Src("county_utility_overlap_puct",
                                           one("select max(built_at)::date from county_utility_overlap_puct")),
        "county_weather_zone": Src("county_weather_zone", one("select max(built_at)::date from county_weather_zone")),
        "eia_861": Src("eia861_sales + 861S (lake)", None, "861S read from the raw zips, not a table yet (X4)"),
        "ercot_market_participants": Src("ercot_market_participants",
                                         one("select max(snapshot_date) from ercot_market_participants")),
        "census_population_county": Src("census_population_county", one(
            "select max(ref_date) from census_population_county where series = 'postcensal_v2025'"), "PEP vintage 2025"),
        "census_permits_county": Src("census_permits_county", date(2025, 12, 31), "annual BPS 2023–2025, imputed"),
        "census_housing_county": Src("census_housing_county", date(2024, 12, 31), "ACS 2024 5-year B25032"),
        "tceq_data_center_sites": Src("tceq_data_center_sites", None,
                                      f"no snapshot date in the table; latest permit {dc_latest}"),
        "ltlf_forecasts": Src("ltlf_forecasts", one(
            "select max(vintage_date) from ltlf_forecasts where vintage = 'LTLF 2025'"), "LTLF 2025, ERCOT-adjusted, summer"),
        "ercot_monthly_peaks": Src("ercot_monthly_peaks", one(
            "select max(month) from ercot_monthly_peaks where value is not null and region_type = 'weather_zone'"),
            "D&E report; recent months not final-settled"),
        "queue_adjusted": Src("gis_project_events → queue_adjusted (X2)",
                              one("select max(latest_report_month) from gis_project_events"),
                              "GIS report month; adjusted = expected MW reaching COD (model)"),
    }
    inp = D.DiagnosisInputs(
        accounts=names, links=links, geo=geo, county_zone=county_zone, signals=signals, scored=scored,
        weights=weights, signal_meta={k: SIGNAL_META[k] for k in weights}, events=events, eia=eia,
        res_price=res_price, dc_sites=DC.load_sites(), queue=queue, permits_monthly=permits_monthly,
        permits_month=latest_permit_month, zone_peaks=zone_peaks, zone_cp=zone_cp, sources=sources,
        as_of=ctx.as_of, dc_since=DC_SINCE, ltlf_years=(ZONE_START, ZONE_END),
    )
    return inp, weights_set


def diagnoses(ctx: MartContext) -> dict[str, dict]:
    """``diagnosis.assemble`` for every scored account, in rank order."""

    def build() -> dict[str, dict]:
        from basecast_pipelines.models import diagnosis as D

        inp, _ = diagnosis_inputs(ctx)
        return {a: D.assemble(a, inp) for a in inp.scored.sort("rank")["account_id"].to_list()}

    return ctx.cached("accounts.diagnoses", build)


def _weights_status(ctx: MartContext) -> str:
    status = marts_config.status(ctx.config, "scoring.weights_set")
    return "pending_review" if status == "default_pending_review" else "reviewed"


def _facts(diag: dict, block: str) -> dict[str, Any]:
    facts = diag["header"] if block == "header" else diag["territory"]["facts"]
    return {f["key"]: f["value"] for f in facts}


def flags(diag: dict) -> list[str]:
    """The list flags (``FLAGS``), with the rules of ``diagnosis.data_gaps``."""
    from basecast_pipelines.models import diagnosis as D

    out = []
    counties = diag["territory"]["counties"]
    if counties.height and not counties["exposed"].any():
        out.append("no_exposed_county")
    hpm = _facts(diag, "territory").get("homes_per_meter")
    if hpm is not None and hpm < D.UNDER_COUNT:
        out.append("apportionment_under")
    if hpm is not None and hpm > 1:
        out.append("apportionment_over")
    if _facts(diag, "header").get("eia_form") == "short":
        out.append("short_form")
    if diag["eia_breaks"]:
        out.append("eia_break")
    return out


# --- builds --------------------------------------------------------------------------------------------------


ACCOUNTS_SCHEMA = {
    "account_id": pl.Utf8, "name": pl.Utf8, "account_type": pl.Utf8, "eia_utility_id": pl.Utf8, "gt": pl.Utf8,
    "primary_weather_zone": pl.Utf8, "meters": pl.Float64, "score": pl.Float64, "rank": pl.Int32, "tier": pl.Utf8,
    "signals": pl.Utf8, "next_action": pl.Utf8, "action_changes_on": pl.Date, "action_changes_to": pl.Utf8,
    "n_strong": pl.Int32, "n_context": pl.Int32, "latest_event_date": pl.Date, "top_trigger": pl.Utf8,
    "active_triggers": pl.List(pl.Utf8), "flags": pl.List(pl.Utf8), "simulated": pl.Boolean,
    "weights_set": pl.Utf8, "weights_status": pl.Utf8,
}


def build_accounts(ctx: MartContext) -> pl.DataFrame:
    from basecast_pipelines.models import diagnosis as D

    _, weights_set = diagnosis_inputs(ctx)
    status = _weights_status(ctx)
    rows = []
    for d in diagnoses(ctx).values():
        head, terr, tl, na = _facts(d, "header"), _facts(d, "territory"), d["triggers"], d["next_action"]
        past = tl.filter(pl.col("event_date") <= ctx.as_of)
        lead = na["lead_trigger"]
        top = None if lead is None else {"trigger": lead["trigger"], "title": lead["title"],
                                         "event_date": lead["event_date"], "age_days": (ctx.as_of - lead["event_date"]).days}
        sig = {r["signal"]: {"raw": r["raw"], "pct": r["pct"]} for r in d["score"]["signals"].iter_rows(named=True)}
        rows.append({
            "account_id": d["account_id"], "name": head["name"], "account_type": head["account_type"],
            "eia_utility_id": head["eia_utility_id"], "gt": head["gt"], "primary_weather_zone": terr["weather_zone"],
            "meters": head["customers"], "score": d["score"]["score"], "rank": d["score"]["rank"],
            "tier": d["score"]["tier"], "signals": json.dumps(D.to_jsonable(sig)), "next_action": na["action"],
            "action_changes_on": na["changes_on"], "action_changes_to": na["changes_to"], "n_strong": na["n_strong"],
            "n_context": na["n_context"], "latest_event_date": past["event_date"].max() if past.height else None,
            "top_trigger": None if top is None else json.dumps(D.to_jsonable(top)),
            "active_triggers": sorted(tl.filter(pl.col("active"))["trigger"].unique().to_list()),
            "flags": flags(d), "simulated": False, "weights_set": weights_set, "weights_status": status,
        })
    return pl.DataFrame(rows, schema=ACCOUNTS_SCHEMA).with_columns(
        pl.col("rank").rank("ordinal").over("account_type").cast(pl.Int32).alias("rank_within_type")
    ).sort("rank")


def build_events(ctx: MartContext) -> pl.DataFrame:
    frames = [d["triggers"].with_columns(pl.lit(a).alias("account_id")) for a, d in diagnoses(ctx).items()]
    return pl.concat(frames, how="vertical_relaxed").select("account_id", pl.exclude("account_id"))


def build_counties(ctx: MartContext) -> pl.DataFrame:
    from basecast_pipelines.models import diagnosis as D

    inp, _ = diagnosis_inputs(ctx)
    frames = []
    for a in diagnoses(ctx):
        counties, _ = D.with_context(D.account_counties(inp.links, inp.geo, inp.county_zone, a))
        frames.append(counties.filter((pl.col("county_share") >= MIN_LISTED_SHARE) | pl.col("context"))
                      .with_columns(pl.lit(a).alias("account_id")))
    return pl.concat(frames, how="vertical_relaxed").select("account_id", pl.exclude("account_id"))


def detail_payload(d: dict, ctx: MartContext) -> dict:
    """The X9 §4 detail: the diagnosis with the triggers summarized (active + context summary + history count),
    the context rule, the gaps, the coverage, and ``simulated`` / ``verified`` on every fact."""
    from basecast_pipelines.models import diagnosis as D

    inp, weights_set = diagnosis_inputs(ctx)
    p = D.to_jsonable({k: v for k, v in d.items() if k != "triggers"})
    tl = d["triggers"]
    active = tl.filter(pl.col("active"))
    context = (
        active.filter(pl.col("strength") == "context")
        .group_by("trigger")
        .agg(pl.len().alias("count"), pl.col("event_date").max().alias("latest_date"),
             pl.col("county_name").drop_nulls().unique().sort().alias("counties"))
        .sort("trigger")
    )
    p["triggers"] = {"active": D.to_jsonable(active), "context_summary": D.to_jsonable(context),
                     "history_count": tl.height}
    counties, label = D.with_context(D.account_counties(inp.links, inp.geo, inp.county_zone, d["account_id"]))
    exposed = bool(counties.height and counties["exposed"].any())
    p["territory"]["context_rule"] = "exposed" if exposed else "home_county"
    p["territory"]["context_label"] = label
    p["gaps"] = D.to_jsonable(D.data_gaps(d, as_of=ctx.as_of, stale_days=730))
    p["coverage"] = {"public_data": True, "utility_private_data": False, "fleet_data": False, "resolution": "zone"}
    p["score"]["weights_set"] = weights_set
    p["score"]["weights_status"] = _weights_status(ctx)
    for fact in (*p["header"], *p["territory"]["facts"]):
        fact["simulated"] = False
        fact["verified"] = True
    return p


def build_detail(ctx: MartContext) -> pl.DataFrame:
    rows = [{"account_id": a, "payload": json.dumps(detail_payload(d, ctx), ensure_ascii=False)}
            for a, d in diagnoses(ctx).items()]
    return pl.DataFrame(rows, schema={"account_id": pl.Utf8, "payload": pl.Utf8})


def build_glossary(ctx: MartContext) -> pl.DataFrame:
    from basecast_pipelines.models import triggers as T

    rows = [{"kind": "trigger", "code": k, "label": TRIGGER_CHIPS.get(k, k), "text": v.label, "strength": v.strength}
            for k, v in T.TRIGGERS.items()]
    rows += [{"kind": "flag", "code": k, "label": lab, "text": txt, "strength": None} for k, (lab, txt) in FLAGS.items()]
    rows += [{"kind": "next_action", "code": k, "label": lab, "text": txt, "strength": None}
             for k, (lab, txt) in ACTIONS.items()]
    return pl.DataFrame(rows, schema={"kind": pl.Utf8, "code": pl.Utf8, "label": pl.Utf8, "text": pl.Utf8,
                                      "strength": pl.Utf8})


def _accounts_meta(frame: pl.DataFrame, ctx: MartContext) -> dict[str, Any]:
    inp, _ = diagnosis_inputs(ctx)
    return {"signals": [{"signal": k, "label": SIGNAL_META[k][0], "unit": SIGNAL_META[k][1], "weight": w}
                        for k, w in inp.weights.items()]}


# --- checks --------------------------------------------------------------------------------------------------


def _as_x5(config) -> bool:
    """X5 / X9's switches: Q3 weights, the IA trigger strong, no X10 extras."""
    v = marts_config.value
    return (v(config, "scoring.weights_set") == "q3" and v(config, "triggers.gen_storage_ia") == "strong"
            and not v(config, "munis.place_permit_trigger"))


def _golden(name, get, expected, **kw):
    return value_check(name, get, expected, as_of=GOLDEN_AS_OF, applies=_as_x5, **kw)


def _action_count(action: str):
    return lambda f: f.filter(pl.col("next_action") == action).height


ACCOUNT_CHECKS = (
    value_check("107 accounts (5 held out)", lambda f: f.height, N_ACCOUNTS),
    value_check("no is_base_partner column", lambda f: "is_base_partner" in f.columns, False),
    value_check("unique account_id", lambda f: f["account_id"].is_duplicated().any(), False),
    _golden("accounts with an active strong trigger", lambda f: f.filter(pl.col("n_strong") >= 1).height, 46),
    _golden("call now", _action_count("call_now"), 25),
    _golden("nurture", _action_count("nurture"), 12),
    _golden("watch", _action_count("watch"), 34),
    _golden("hold", _action_count("hold"), 36),
    _golden("#1 is New Braunfels Utilities", lambda f: f.filter(pl.col("rank") == 1)["name"].item(),
            "New Braunfels Utilities"),
    _golden("#1 action changes on 2026-10-22", lambda f: f.filter(pl.col("rank") == 1)["action_changes_on"].item(),
            date(2026, 10, 22)),
)

ACCOUNTS = Mart(
    name="mart_accounts",
    build=build_accounts,
    key=("account_id",),
    inputs=("puct_ccn_territories", "county_utility_overlap_puct", "eia861_sales", "census_population_county",
            "census_permits_county", "census_housing_county", "tceq_data_center_sites", "ltlf_forecasts",
            "county_weather_zone", "cpa_local_dev_agreements", "ercot_market_participants", "gis_project_events",
            "tpit_projects", "puct_tsp_large_load_requests"),
    description="The ranked ERCOT co-ops and munis (validation accounts held out): score, tier, signals, next action "
    "with its lapse date, active triggers and data flags (X5, X9 §4).",
    caveats=("weights_pending_review", "by_county_not_point"),
    checks=ACCOUNT_CHECKS,
    json_columns=("signals", "top_trigger"),
    meta=_accounts_meta,
)

ACCOUNT_EVENTS = Mart(
    name="mart_account_events",
    build=build_events,
    key=("account_id", "trigger", "source_ref"),
    inputs=("mart_accounts",),
    description="Every dated trigger event of every ranked account (ever; active = the last 12 months), with its "
    "strength, mapping (county or name), exposure, source and offer angle (X5).",
    caveats=("by_county_not_point",),
    checks=(
        value_check("unique event per account", lambda f: f.select("account_id", "trigger", "source_ref")
                    .is_duplicated().any(), False),
        value_check("107 accounts at most", lambda f: f["account_id"].n_unique() <= N_ACCOUNTS, True),
    ),
)

ACCOUNT_COUNTIES = Mart(
    name="mart_account_counties",
    build=build_counties,
    key=("account_id", "county_fips"),
    inputs=("county_utility_overlap_puct", "county_weather_zone", "mart_accounts"),
    description="Account × county: overlap area, county share (of the county's land) and territory share, weather "
    "zone, exposed (≥ 20%, county events reach the account) and context; counties ≥ 1% plus the home county.",
    checks=(value_check("every account has a county", lambda f: f["account_id"].n_unique(), N_ACCOUNTS),),
)

ACCOUNT_DETAIL = Mart(
    name="mart_account_detail",
    build=build_detail,
    key=("account_id",),
    inputs=("mart_accounts", "mart_account_events", "mart_account_counties"),
    description="The per-account diagnosis (X9 §4) as one JSON payload: header facts, score breakdown, next action, "
    "active triggers, territory, EIA series, gaps and coverage; every fact with source and as-of.",
    caveats=("weights_pending_review",),
    checks=(value_check("107 payloads", lambda f: f.height, N_ACCOUNTS),),
    json_columns=("payload",),
)

GLOSSARY = Mart(
    name="mart_glossary",
    build=build_glossary,
    key=("kind", "code"),
    inputs=(),
    description="Labels and definitions of the trigger, flag and next-action codes the account screens show.",
    meta=lambda frame, ctx: {"items": frame.to_dicts()},
)

MARTS = (ACCOUNTS, ACCOUNT_EVENTS, ACCOUNT_COUNTIES, ACCOUNT_DETAIL, GLOSSARY)
