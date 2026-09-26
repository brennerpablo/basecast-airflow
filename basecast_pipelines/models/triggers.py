"""Account triggers ("why now") and the rule-based next action (exploration X5, after phase 0).

A **trigger** is a dated public event tied to an account (an ERCOT co-op or muni, ``account_id`` = PUCT
``ccn_no``) that makes *now* the moment for Base's partnerships team to call. Each ``*_events`` function
turns one source into rows of the same shape (``EVENT_COLUMNS``): what happened, when, where, and the
source reference. Events reach accounts two ways:

- **by county**: an event in a county reaches every account whose territory covers at least
  ``MIN_COUNTY_SHARE`` of that county's land (``county_share`` from ``county_utility_overlap_puct``). This is
  the same by-county, not-by-point attribution as Q3/Q4; it almost never reaches a muni (a city covers a
  small share of its county), which is physically right for rural sites.
- **by name**: an event names the account itself (an ERCOT registration, a city's own Ch. 380 agreement,
  the account's G&T in the TSP large-load RFI).

The score is the weighted mean of within-universe percentile ranks of ``config/account_score.yaml``
(:func:`score_accounts`); the next action crosses the score tier with the active triggers
(:func:`next_action`). Everything is pure functions on DataFrames (``tests/models/test_triggers.py``);
the ``load_*`` functions are thin read-only queries through ``models.db``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta

import polars as pl

from basecast_pipelines.models.db import read_sql

AS_OF = date(2026, 9, 26)
WINDOW_DAYS = 365  # "active" = the event happened in the last 12 months
FRESH_DAYS = 90  # a strong trigger this recent lifts a tier-B account to "call now"
MIN_COUNTY_SHARE = 0.20  # the account covers ≥ 20% of the event county's land

EVENT_COLUMNS = ("trigger", "event_date", "county_fips", "title", "detail", "source", "source_ref")


@dataclass(frozen=True)
class TriggerSpec:
    label: str
    strength: str  # "strong" drives the next action; "context" only annotates it
    mapping: str  # "county" or "name"
    offer: str  # the angle of the call


TRIGGERS: dict[str, TriggerSpec] = {
    "dc_permit": TriggerSpec(
        "New data-center air permit in the territory", "strong", "county",
        "capacity: large-load pressure on the co-op's peak and 4CP; offer VPP capacity",
    ),
    "gen_storage_ia": TriggerSpec(
        "Generation or storage project signed its interconnection agreement nearby", "strong", "county",
        "local capacity is being built by others; position distributed storage as the member-owned option",
    ),
    "dev_agreement": TriggerSpec(
        "Local economic-development agreement (Ch. 312 / Ch. 380)", "strong", "county+name",
        "new commercial/industrial load coming; offer peak shaving before it lands",
    ),
    "market_registration": TriggerSpec(
        "Account registered a new ERCOT market role (LSE / QSE / TDSP / RE)", "strong", "name",
        "wholesale set-up is changing; offer a QSE-ready VPP",
    ),
    "permit_surge": TriggerSpec(
        "Residential permits up ≥ 25% (last 12 months vs the 12 before)", "strong", "county",
        "new homes: batteries at construction, builder partnerships",
    ),
    "new_transmission": TriggerSpec(
        "New transmission project listed in ERCOT TPIT touching the territory", "context", "county",
        "grid constraint in the area; storage as a non-wires alternative",
    ),
    "rate_increase": TriggerSpec(
        "Residential average price up ≥ 10% (EIA-861)", "context", "name",
        "bill pressure: demand-charge / 4CP savings for members",
    ),
    "tsp_large_load": TriggerSpec(
        "The account's G&T / TSP reported large-load requests (PUCT 58777 RFI)", "context", "name",
        "wholesale supplier faces large-load growth; capacity costs likely to rise",
    ),
}

STRONG = tuple(k for k, v in TRIGGERS.items() if v.strength == "strong")

# The TSP names of the RFI table (PUCT 58777 item 38) → the G&T names of ``puct_ccn_territories.gt_cooperatives``.
RFI_TSP_FOR_GT = {
    "Brazos Electric Power Coop": "Brazos",
    "Golden Spread Electric Coop": "Golden Spread",
    "LCRA": "LCRA",
    "San Miguel Electric Coop | South Texas Electric Coop": "STEC (South Texas)",
    "Rayburn Electric Coop": "Rayburn",
    "CPS Energy": "CPS",  # an account that is its own TSP (see :func:`self_tsp_rows`)
}
# Accounts that report to the RFI as TSPs themselves (PUCT company name → the ``gt`` key above).
SELF_TSP = {"CPS Energy": "CPS Energy"}


def self_tsp_rows(accounts: pl.DataFrame) -> pl.DataFrame:
    """``account_id``, ``gt`` rows for accounts that are their own TSP in the RFI table."""
    return accounts.filter(pl.col("name").is_in(list(SELF_TSP))).select(
        "account_id", pl.col("name").replace_strict(SELF_TSP).alias("gt")
    )


# --- helpers ------------------------------------------------------------------------------------------------


def county_key(col: str = "county") -> pl.Expr:
    """County name key (upper case, letters only, "County" removed), the same rule as Q6's join."""
    return pl.col(col).str.to_uppercase().str.replace_all(r"\bCOUNTY\b", "").str.replace_all(r"[^A-Z]", "")


def _events(df: pl.DataFrame, extra: tuple[str, ...] = ()) -> pl.DataFrame:
    """Cast to the common event shape (``extra`` columns are kept after it)."""
    return df.select(
        pl.col("trigger").cast(pl.Utf8),
        pl.col("event_date").cast(pl.Date),
        pl.col("county_fips").cast(pl.Utf8),
        pl.col("title").cast(pl.Utf8),
        pl.col("detail").cast(pl.Utf8),
        pl.col("source").cast(pl.Utf8),
        pl.col("source_ref").cast(pl.Utf8),
        *extra,
    )


def empty_account_events() -> pl.DataFrame:
    schema = {"account_id": pl.Utf8, **{c: pl.Utf8 for c in EVENT_COLUMNS}, "exposure": pl.Float64}
    schema["event_date"] = pl.Date
    return pl.DataFrame(schema=schema)


def events_by_county(
    events: pl.DataFrame, links: pl.DataFrame, *, min_share: float = MIN_COUNTY_SHARE
) -> pl.DataFrame:
    """Attach county events to accounts covering ≥ ``min_share`` of the county; ``exposure`` = county_share.

    ``links``: ``account_id``, ``county_fips``, ``county_share``. An event in two counties of the same
    account (a transmission line) is kept once per account, with the larger exposure.
    """
    exposed = links.filter(pl.col("county_share") >= min_share).select(
        "account_id", "county_fips", pl.col("county_share").alias("exposure")
    )
    joined = events.filter(pl.col("county_fips").is_not_null()).join(exposed, on="county_fips", how="inner")
    return (
        joined.sort("exposure", descending=True)
        .unique(subset=["account_id", "trigger", "source_ref"], keep="first", maintain_order=True)
        .select("account_id", *EVENT_COLUMNS, "exposure")
    )


def events_by_name(events: pl.DataFrame, account_col: str = "account_id") -> pl.DataFrame:
    """Events that already carry their account (name mappings): exposure 1."""
    return events.select(pl.col(account_col).alias("account_id"), *EVENT_COLUMNS, pl.lit(1.0).alias("exposure"))


# --- one function per trigger -------------------------------------------------------------------------------


def dc_permit_events(sites: pl.DataFrame) -> pl.DataFrame:
    """TCEQ data-center sites (``tceq_data_center_sites``) dated by their first known permit."""
    return _events(
        sites.filter(pl.col("first_affil_begin_dt").is_not_null()).select(
            pl.lit("dc_permit").alias("trigger"),
            pl.col("first_affil_begin_dt").alias("event_date"),
            "county_fips",
            pl.col("reg_ent_name").alias("title"),
            pl.when(pl.col("matched_by_name"))
            .then(pl.lit("matched by name"))
            .otherwise(pl.lit("matched by NAICS 518210 only"))
            .alias("detail"),
            pl.lit("tceq_data_center_sites").alias("source"),
            pl.col("ref_num_txt").alias("source_ref"),
        )
    )


def gen_storage_events(
    projects: pl.DataFrame, geography: pl.DataFrame, *, min_mw: float = 50.0, fuels: tuple[str, ...] | None = None
) -> pl.DataFrame:
    """GIS queue projects (``gis_project_events``) dated by ``ia_signed``, at least ``min_mw``.

    ``geography``: ``county_fips``, ``county_name`` (``tx_counties``); projects join on :func:`county_key`.
    """
    geo = geography.select("county_fips", county_key("county_name").alias("_k"))
    sel = projects.filter(pl.col("ia_signed").is_not_null(), pl.col("capacity_mw") >= min_mw)
    if fuels is not None:
        sel = sel.filter(pl.col("fuel_type").is_in(list(fuels)))
    sel = sel.with_columns(county_key("county").alias("_k")).join(geo, on="_k", how="left")
    return _events(
        sel.select(
            pl.lit("gen_storage_ia").alias("trigger"),
            pl.col("ia_signed").alias("event_date"),
            "county_fips",
            pl.col("project_name").alias("title"),
            pl.format(
                "{} {} MW, status {}",
                pl.col("fuel_type").fill_null("?"),
                pl.col("capacity_mw").round(0),
                pl.col("last_status").fill_null("?"),
            ).alias("detail"),
            pl.lit("gis_project_events").alias("source"),
            pl.col("inr").alias("source_ref"),
        )
    )


def agreement_date() -> pl.Expr:
    """Ch. 312: ``executed_date`` (else effective); Ch. 380 has no executed date, so ``effective_date``.

    Ch. 380 ``effective_date`` can be years ahead (the incentive's start); :func:`active_events` drops
    future dates, so a deal signed now but effective in 2028 does not fire yet (not verified what the
    Comptroller's effective date means for each reporting city).
    """
    return (
        pl.when(pl.col("program") == "ch312")
        .then(pl.coalesce("executed_date", "effective_date"))
        .otherwise(pl.col("effective_date"))
    )


def dev_agreement_events(agreements: pl.DataFrame, *, min_value: float = 1_000_000.0) -> pl.DataFrame:
    """Texas Comptroller Ch. 312 / Ch. 380 agreements (``cpa_local_dev_agreements``), one event per agreement
    and county. Ch. 380 needs ≥ ``min_value`` in total incentives; Ch. 312 abatements always pass (their
    ``total_incentive_value`` is mostly reported as 0). Cancelled agreements are dropped.

    Mapping (see :func:`county_agreements`, :func:`city_agreement_accounts`): Ch. 312 and county-signed
    Ch. 380 go by county; city-signed Ch. 380 go by name to that city's muni only, because a city's deal
    sits inside the city, which a co-op sharing the county rarely serves.
    """
    value_ok = (pl.col("program") == "ch312") | (pl.col("total_incentive_value") >= min_value)
    sel = agreements.filter(value_ok, pl.col("status").fill_null("") != "Cancelled").with_columns(
        agreement_date().alias("event_date")
    )
    return _events(
        sel.select(
            pl.lit("dev_agreement").alias("trigger"),
            "event_date",
            "county_fips",
            pl.coalesce("recipient_name", pl.lit("(recipient redacted)")).alias("title"),
            pl.format(
                "{} {} by {}, incentives ${}M",
                pl.col("program"),
                pl.col("status").fill_null(""),
                pl.col("local_government_name").fill_null("?"),
                (pl.col("total_incentive_value") / 1e6).round(1),
            ).alias("detail"),
            pl.lit("cpa_local_dev_agreements").alias("source"),
            pl.format("{}:{}", pl.col("program"), pl.col("agreement_id")).alias("source_ref"),
            "local_government_name",
            "local_government_type",
        ),
        extra=("local_government_name", "local_government_type"),
    )


def county_agreements(agreements: pl.DataFrame) -> pl.DataFrame:
    """The agreements that map by county: Ch. 312, and Ch. 380 signed by a county."""
    keep = pl.col("source_ref").str.starts_with("ch312") | (pl.col("local_government_type") == "County")
    return agreements.filter(keep).select(*EVENT_COLUMNS)


def city_agreement_accounts(agreements: pl.DataFrame, accounts: pl.DataFrame) -> pl.DataFrame:
    """Name mapping for munis: agreements signed by a **city** whose name is the muni's core name.

    ``agreements`` is the output of :func:`dev_agreement_events` (with ``local_government_*``);
    ``accounts``: ``account_id``, ``name``, ``account_type``. Exact core-name equality only.
    """
    from basecast_pipelines.models.accounts import core_name

    munis = accounts.filter(pl.col("account_type") == "muni").select(
        "account_id", pl.col("name").map_elements(core_name, return_dtype=pl.Utf8).alias("_core")
    )
    cities = agreements.filter(
        pl.col("source_ref").str.starts_with("ch380"), pl.col("local_government_type") == "City"
    ).with_columns(
        pl.col("local_government_name").map_elements(core_name, return_dtype=pl.Utf8).alias("_core")
    )
    return events_by_name(cities.join(munis, on="_core", how="inner"))


def transmission_events(tpit: pl.DataFrame, *, min_kv: float = 138.0) -> pl.DataFrame:
    """ERCOT TPIT projects (``tpit_projects``) dated by the first snapshot that lists them, one event per
    project and endpoint county; ``kv`` ≥ ``min_kv`` (unknown kV kept out)."""
    dated = tpit.filter(pl.col("snapshot_date").is_not_null(), pl.col("project_id").is_not_null())
    first = dated.sort("snapshot_date").group_by("project_id", maintain_order=True).agg(
        pl.col("snapshot_date").first().alias("event_date"),
        pl.col("project_title").drop_nulls().last(),
        pl.col("tsp").drop_nulls().last(),
        pl.col("kv").drop_nulls().max(),
        pl.col("transmission_status").drop_nulls().last(),
        pl.col("county_start_fips").drop_nulls().last(),
        pl.col("county_end_fips").drop_nulls().last(),
    )
    first = first.filter(pl.col("kv") >= min_kv)
    long = pl.concat(
        [
            first.select(pl.exclude("county_start_fips", "county_end_fips"), pl.col(c).alias("county_fips"))
            for c in ("county_start_fips", "county_end_fips")
        ]
    ).unique(subset=["project_id", "county_fips"])
    return _events(
        long.select(
            pl.lit("new_transmission").alias("trigger"),
            "event_date",
            "county_fips",
            pl.col("project_title").alias("title"),
            pl.format("{} {} kV, {}", pl.col("tsp").fill_null("?"), pl.col("kv"),
                      pl.col("transmission_status").fill_null("?")).alias("detail"),
            pl.lit("tpit_projects").alias("source"),
            pl.col("project_id").alias("source_ref"),
        )
    )


_ROLE = re.compile(r"\s*\((QSE|LSE|RE|TDSP|CRRAH|IMRE|SQ\d*)\)\s*$", re.IGNORECASE)
_CO_OP = re.compile(r"\bco[\s-]+op\b", re.IGNORECASE)


def registration_events(
    participants: pl.DataFrame, accounts: pl.DataFrame, *, min_score: float = 95.0
) -> pl.DataFrame:
    """ERCOT market-participant registrations (``ercot_market_participants``) whose entity is an account.

    Names are compared on their core words (:func:`accounts.core_name`, rapidfuzz ``token_sort_ratio``
    ≥ ``min_score``). "CO OP" is read as "COOP". An entity whose name tells its type (city of / cooperative)
    only matches accounts of that type; one that does not (``CPS ENERGY``) may match either. Dated by
    ``sfa_effective_date``.
    """
    from rapidfuzz import fuzz

    from basecast_pipelines.models.accounts import core_name, infer_type_from_name

    ents = participants.filter(pl.col("sfa_effective_date").is_not_null()).with_columns(
        pl.coalesce("entity_name", pl.col("name").str.replace("(?i)" + _ROLE.pattern, ""))
        .str.replace_all("(?i)" + _CO_OP.pattern, "COOP")
        .alias("_entity")
    )
    pool = [(e, core_name(e), infer_type_from_name(e)) for e in ents["_entity"].unique().to_list()]
    pool = [p for p in pool if p[1]]
    rows = []
    for account_id, name, kind in accounts.select("account_id", "name", "account_type").rows():
        core = core_name(name)
        if not core:
            continue
        for ent, ent_core, ent_type in pool:
            if ent_type is not None and ent_type != kind:
                continue
            score = float(fuzz.token_sort_ratio(core, ent_core))
            if score >= min_score:
                rows.append({"account_id": account_id, "_entity": ent, "match_score": score})
    if not rows:
        return empty_account_events()
    hits = pl.DataFrame(rows).join(ents, on="_entity", how="inner")
    out = hits.select(
        "account_id",
        pl.lit("market_registration").alias("trigger"),
        pl.col("sfa_effective_date").cast(pl.Date).alias("event_date"),
        pl.lit(None, pl.Utf8).alias("county_fips"),
        pl.format("{} registered as {}", pl.col("_entity"), pl.col("market_participant_type")).alias("title"),
        pl.format("name match {}", pl.col("match_score").round(0)).alias("detail"),
        pl.lit("ercot_market_participants").alias("source"),
        pl.format("{}:{}", pl.col("market_participant_type"), pl.col("name")).alias("source_ref"),
        pl.lit(1.0).alias("exposure"),
    )
    return out.unique(subset=["account_id", "source_ref"])


def residential_price(sales: pl.DataFrame) -> pl.DataFrame:
    """Residential average price (US$/kWh) per utility and data year from ``eia861_sales``.

    Sums parts and balancing authorities of the ``residential`` sector (bundled parts only: part C
    delivery-only rows have no energy revenue). Early release rows are kept and flagged.
    """
    res = sales.filter(pl.col("sector") == "residential", pl.col("part").fill_null("") != "C")
    return (
        res.group_by("utility_id", "data_year")
        .agg(
            pl.col("revenue_thousand_usd").sum().alias("rev_k"),
            pl.col("sales_mwh").sum().alias("mwh"),
            pl.col("early_release").any().alias("early_release"),
        )
        .filter(pl.col("mwh") > 0, pl.col("rev_k") > 0)
        .with_columns((pl.col("rev_k") / pl.col("mwh")).alias("usd_per_kwh"))
    )


def rate_increase_events(
    prices: pl.DataFrame, universe: pl.DataFrame, *, year: int, base_year: int, threshold: float = 0.10
) -> pl.DataFrame:
    """Residential price up ≥ ``threshold`` from ``base_year`` to ``year``, per account (via its EIA id).

    Dated at the end of ``year`` (the period, not the release date, which is not in the table).
    """
    wide = (
        prices.filter(pl.col("data_year").is_in([base_year, year]))
        .pivot(on="data_year", index="utility_id", values="usd_per_kwh", aggregate_function="first")
        .rename({str(base_year): "p0", str(year): "p1"}, strict=False)
    )
    if "p0" not in wide.columns or "p1" not in wide.columns:
        return empty_account_events()
    hits = (
        wide.with_columns((pl.col("p1") / pl.col("p0") - 1).alias("change"))
        .filter(pl.col("change") >= threshold)
        .join(universe.select("account_id", pl.col("eia_utility_id").alias("utility_id")), on="utility_id")
    )
    return hits.select(
        "account_id",
        pl.lit("rate_increase").alias("trigger"),
        pl.lit(date(year, 12, 31)).alias("event_date"),
        pl.lit(None, pl.Utf8).alias("county_fips"),
        pl.format("residential price +{}% ({} → {})", (pl.col("change") * 100).round(1),
                  pl.lit(base_year), pl.lit(year)).alias("title"),
        pl.format("{} → {} US$/kWh", pl.col("p0").round(4), pl.col("p1").round(4)).alias("detail"),
        pl.lit("eia861_sales").alias("source"),
        pl.format("{}:{}", pl.col("utility_id"), pl.lit(year)).alias("source_ref"),
        pl.lit(1.0).alias("exposure"),
    )


def permit_surge_events(
    monthly: pl.DataFrame, links: pl.DataFrame, *, as_of_month: date, growth: float = 0.25, min_units: float = 50.0
) -> pl.DataFrame:
    """Apportioned residential permit units, the 12 months up to ``as_of_month`` vs the 12 before.

    ``monthly``: ``county_fips``, ``period_start`` (first of month), ``units_total``. Fires when units grew
    ≥ ``growth`` and the recent window has ≥ ``min_units`` apportioned units. Dated at ``as_of_month``.
    """
    end = as_of_month
    start_recent = date(end.year - 1, end.month, 1) + timedelta(days=32)
    start_recent = date(start_recent.year, start_recent.month, 1)
    start_prior = date(start_recent.year - 1, start_recent.month, 1)
    m = monthly.filter(pl.col("period_start") >= start_prior, pl.col("period_start") <= end).with_columns(
        (pl.col("period_start") >= start_recent).alias("_recent")
    )
    per_county = m.group_by("county_fips").agg(
        pl.col("units_total").filter(pl.col("_recent")).sum().alias("recent"),
        pl.col("units_total").filter(~pl.col("_recent")).sum().alias("prior"),
    )
    acct = (
        links.join(per_county, on="county_fips", how="inner")
        .group_by("account_id")
        .agg((pl.col("recent") * pl.col("county_share")).sum(), (pl.col("prior") * pl.col("county_share")).sum())
        .filter(pl.col("prior") > 0)
        .with_columns((pl.col("recent") / pl.col("prior") - 1).alias("change"))
    )
    hits = acct.filter(pl.col("change") >= growth, pl.col("recent") >= min_units)
    return hits.select(
        "account_id",
        pl.lit("permit_surge").alias("trigger"),
        pl.lit(end).alias("event_date"),
        pl.lit(None, pl.Utf8).alias("county_fips"),
        pl.format("permits +{}% in 12 months", (pl.col("change") * 100).round(0)).alias("title"),
        pl.format("{} → {} apportioned units", pl.col("prior").round(0), pl.col("recent").round(0)).alias("detail"),
        pl.lit("census_permits_county").alias("source"),
        pl.format("{}:{}", pl.col("account_id"), pl.lit(end.isoformat())).alias("source_ref"),
        pl.lit(1.0).alias("exposure"),
    )


def tsp_large_load_events(
    gt: pl.DataFrame, rfi: pl.DataFrame, *, target_year: int = 2030, min_mw: float = 1000.0
) -> pl.DataFrame:
    """Accounts whose G&T is a TSP in the RFI table with ≥ ``min_mw`` requested for ``target_year``.

    ``gt``: ``account_id``, ``gt`` (one row per account and G&T). ``rfi``: the ``tsp`` breakdown of
    ``puct_tsp_large_load_requests`` (``name``, ``year``, ``mw``, ``filed_date``); PPTX and PDF copies
    carry the same numbers, so the max per TSP and year is taken.
    """
    per_tsp = (
        rfi.filter(pl.col("year") == target_year)
        .group_by("name")
        .agg(pl.col("mw").max(), pl.col("filed_date").max(), pl.col("docket").first(), pl.col("item").first())
    )
    mapped = gt.with_columns(pl.col("gt").replace_strict(RFI_TSP_FOR_GT, default=None).alias("name"))
    hits = mapped.join(per_tsp, on="name", how="inner").filter(pl.col("mw") >= min_mw)
    return hits.select(
        "account_id",
        pl.lit("tsp_large_load").alias("trigger"),
        pl.col("filed_date").cast(pl.Date).alias("event_date"),
        pl.lit(None, pl.Utf8).alias("county_fips"),
        pl.format("{} reported {} MW of large-load requests for {}", pl.col("name"), pl.col("mw").round(0),
                  pl.lit(target_year)).alias("title"),
        pl.format("via G&T {}", pl.col("gt")).alias("detail"),
        pl.lit("puct_tsp_large_load_requests").alias("source"),
        pl.format("{}:{}:{}", pl.col("docket"), pl.col("item"), pl.col("name")).alias("source_ref"),
        pl.lit(1.0).alias("exposure"),
    ).unique(subset=["account_id", "source_ref"])


# --- activity, coverage and next action ---------------------------------------------------------------------


def active_events(events: pl.DataFrame, *, as_of: date = AS_OF, window_days: int = WINDOW_DAYS) -> pl.DataFrame:
    """Events in ``(as_of − window_days, as_of]``; future-dated events never fire."""
    return events.filter(
        pl.col("event_date") > as_of - timedelta(days=window_days), pl.col("event_date") <= as_of
    ).with_columns((pl.lit(as_of) - pl.col("event_date")).dt.total_days().alias("age_days"))


def trigger_coverage(
    events: pl.DataFrame, n_accounts: int, *, as_of: date = AS_OF, window_days: int = WINDOW_DAYS
) -> pl.DataFrame:
    """Per trigger: accounts that ever fired (event dated ≤ as_of), accounts active in the window, events,
    latest date and median age of the active events."""
    past = events.filter(pl.col("event_date") <= as_of)
    act = active_events(events, as_of=as_of, window_days=window_days)
    ever = past.group_by("trigger").agg(
        pl.col("account_id").n_unique().alias("accounts_ever"),
        pl.col("source_ref").n_unique().alias("events_ever"),
        pl.col("event_date").max().alias("latest"),
    )
    now = act.group_by("trigger").agg(
        pl.col("account_id").n_unique().alias("accounts_12m"),
        pl.col("source_ref").n_unique().alias("events_12m"),
        pl.col("age_days").median().alias("median_age_days"),
    )
    base = pl.DataFrame({"trigger": list(TRIGGERS)})
    return (
        base.join(ever, on="trigger", how="left")
        .join(now, on="trigger", how="left")
        .with_columns(
            pl.col("accounts_ever", "accounts_12m", "events_ever", "events_12m").fill_null(0),
            (pl.col("accounts_12m").fill_null(0) / n_accounts).alias("share_12m"),
            pl.col("trigger").replace_strict({k: v.strength for k, v in TRIGGERS.items()}).alias("strength"),
        )
    )


def account_trigger_summary(events: pl.DataFrame, *, as_of: date = AS_OF, window_days: int = WINDOW_DAYS) -> pl.DataFrame:
    """One row per account with active triggers: strong/context counts, the freshest strong event, and the
    active trigger names (sorted)."""
    act = active_events(events, as_of=as_of, window_days=window_days).with_columns(
        pl.col("trigger").is_in(list(STRONG)).alias("_strong")
    )
    return act.group_by("account_id").agg(
        pl.col("trigger").filter(pl.col("_strong")).n_unique().alias("n_strong"),
        pl.col("trigger").filter(~pl.col("_strong")).n_unique().alias("n_context"),
        pl.col("age_days").filter(pl.col("_strong")).min().alias("strong_age_days"),
        pl.col("event_date").max().alias("latest_event"),
        pl.col("trigger").unique().sort().alias("triggers"),
    )


def score_tier(rank: int, n: int) -> str:
    """A = top quartile, B = second quartile, C = bottom half (rank 1 = best)."""
    if rank <= round(n * 0.25):
        return "A"
    if rank <= round(n * 0.50):
        return "B"
    return "C"


def next_action(tier: str, n_strong: int, strong_age_days: int | None, *, fresh_days: int = FRESH_DAYS) -> str:
    """Rule-based next action from the score tier and the active strong triggers.

    - ``call_now``: tier A with ≥ 1 strong trigger; tier B with ≥ 2, or with 1 in the last ``fresh_days``.
    - ``nurture``: tier A without a strong trigger; tier B with 1 older strong trigger.
    - ``watch``: tier B without a strong trigger; tier C with any strong trigger (set an alert).
    - ``hold``: tier C without a strong trigger.
    Context triggers never change the action; they feed the talking points.
    """
    fresh = strong_age_days is not None and strong_age_days <= fresh_days
    if tier == "A":
        return "call_now" if n_strong >= 1 else "nurture"
    if tier == "B":
        if n_strong >= 2 or (n_strong == 1 and fresh):
            return "call_now"
        return "nurture" if n_strong == 1 else "watch"
    return "watch" if n_strong >= 1 else "hold"


# --- score (dry run of config/account_score.yaml) -----------------------------------------------------------


def percentile_ranks(signals: pl.DataFrame, directions: dict[str, str]) -> pl.DataFrame:
    """Each signal as its within-universe percentile rank in [0, 1]: (average rank − 1) / (n − 1) over the
    accounts that have it; ``lower`` flips it. Nulls stay null."""
    out = []
    for c, d in directions.items():
        n = pl.col(c).is_not_null().sum()
        r = (pl.col(c).rank("average") - 1) / (n - 1)
        out.append((1 - r if d == "lower" else r).alias(f"pct_{c}"))
    return signals.select("account_id", *out)


def score_accounts(pct: pl.DataFrame, weights: dict[str, float]) -> pl.DataFrame:
    """Weighted mean of the percentile ranks; an account missing a signal has its weights renormalized over
    the signals it has. Adds ``score`` and ``rank`` (1 = best, ties by account_id)."""
    num = sum(pl.col(f"pct_{c}").fill_null(0) * w for c, w in weights.items())
    den = sum(pl.col(f"pct_{c}").is_not_null().cast(pl.Float64) * w for c, w in weights.items())
    scored = pct.with_columns((num / den).alias("score")).sort(["score", "account_id"], descending=[True, False])
    return scored.with_columns(pl.int_range(1, scored.height + 1).alias("rank"))


def perturbations(weights: dict[str, float], *, delta: float = 0.05) -> list[tuple[str, dict[str, float]]]:
    """One-at-a-time ± ``delta`` on each weight (renormalized to sum 1), then leave-one-signal-out."""
    out = []
    for k in weights:
        for sign in (+1, -1):
            w = dict(weights)
            w[k] = max(0.0, w[k] + sign * delta)
            s = sum(w.values())
            out.append((f"{k} {'+' if sign > 0 else '-'}{delta:.2f}", {a: v / s for a, v in w.items()}))
    for k in weights:
        w = {a: v for a, v in weights.items() if a != k}
        s = sum(w.values())
        out.append((f"without {k}", {a: v / s for a, v in w.items()}))
    return out


def rank_stability(pct: pl.DataFrame, weights: dict[str, float], *, delta: float = 0.05, top: int = 15) -> pl.DataFrame:
    """Spearman ρ of each perturbed ranking with the base one, plus how many of the base top ``top`` stay in
    the top ``top`` and the largest single rank move."""
    base = score_accounts(pct, weights).select("account_id", pl.col("rank").alias("r0"))
    top0 = set(base.filter(pl.col("r0") <= top)["account_id"])
    rows = []
    for name, w in perturbations(weights, delta=delta):
        alt = score_accounts(pct, w).select("account_id", pl.col("rank").alias("r1")).join(base, on="account_id")
        rho = alt.select(pl.corr("r0", "r1", method="spearman")).item()
        kept = len(top0 & set(alt.filter(pl.col("r1") <= top)["account_id"]))
        rows.append({
            "variant": name, "spearman": rho, f"top{top}_kept": kept,
            "max_move": int(alt.select((pl.col("r1") - pl.col("r0")).abs().max()).item()),
        })
    return pl.DataFrame(rows)


# --- thin loaders (read-only) -------------------------------------------------------------------------------


def load_gis_projects() -> pl.DataFrame:
    return read_sql(
        """
        select inr, project_name, county, fuel_type, capacity_mw, ia_signed, commercial_operation_date,
               last_status, exit_status
        from gis_project_events
        """
    )


def load_geography() -> pl.DataFrame:
    return read_sql("select county_fips, county_name from tx_counties")


def load_agreements() -> pl.DataFrame:
    return read_sql(
        """
        select program, agreement_id, status, local_government_type, local_government_name, county_fips,
               recipient_name, executed_date, effective_date, total_incentive_value, naics_code
        from cpa_local_dev_agreements
        """
    )


def load_tpit() -> pl.DataFrame:
    return read_sql(
        """
        select snapshot_date, project_id, project_title, tsp, kv, transmission_status, county_start_fips,
               county_end_fips
        from tpit_projects
        """
    )


def load_market_participants() -> pl.DataFrame:
    return read_sql(
        "select name, entity_name, market_participant_type, sfa_effective_date from ercot_market_participants"
    )


def load_permits_monthly(since_year: int) -> pl.DataFrame:
    return read_sql(
        """
        select county_fips, period_start, units_total from census_permits_county
        where period_type = 'monthly' and year >= %(y)s
        """,
        {"y": since_year},
    )


def load_gt() -> pl.DataFrame:
    """Account × G&T rows from ``puct_ccn_territories.gt_cooperative`` (``;``-separated)."""
    df = read_sql(
        """
        select ccn_no as account_id, gt_cooperative from puct_ccn_territories
        where utility_type in ('coop', 'muni') and in_ercot and gt_cooperative is not null
        """
    )
    return df.with_columns(pl.col("gt_cooperative").str.split(";").alias("gt")).explode("gt").select(
        "account_id", pl.col("gt").str.strip_chars()
    )


def load_rfi() -> pl.DataFrame:
    return read_sql(
        """
        select docket, item, filed_date, name, year, mw from puct_tsp_large_load_requests
        where breakdown = 'tsp' and not is_total
        """
    )


def load_eia_residential() -> pl.DataFrame:
    """Residential rows of ``eia861_sales`` with revenue (``accounts.load_eia_sales`` has no revenue)."""
    return read_sql(
        """
        select data_year, early_release, utility_id, part, sector, revenue_thousand_usd, sales_mwh
        from eia861_sales where sector = 'residential'
        """
    )
