"""Exploration X13: large-load requests through the generation-and-transmission (G&T) co-ops.

The PUCT TSP RFI (project 58777 item 38, ``puct_tsp_large_load_requests``) gives MW of large-load requests by
transmission service provider (TSP) and year, 2026-2032, cumulative. X11 mapped the wires IOUs to weather zones
and left the G&T co-ops (Brazos, Golden Spread, Rayburn, STEC), LCRA TSC and the transmission-only TSPs unmapped.
This module:

- classifies each RFI TSP by entity type (:data:`TSP_ENTITIES`, cross-checked against ERCOT's own membership
  segment in ``ercot_members`` by :func:`check_segments`) and sums MW by type and year (:func:`by_entity_type`);
- links each G&T to its member distribution co-ops (and LCRA to the munis it supplies) through PUCT's
  ``puct_ccn_territories.gt_cooperative`` (:func:`gt_members`);
- spreads a G&T's MW over its members under explicit assumptions (:func:`allocate`), only to show how far the
  methods disagree: **the RFI gives no location below the TSP**, so every member number is an assumption;
- builds the per-account "your G&T reported X GW" fact (:func:`exposure_facts`).

The RFI MW are **requests**, not forecasts (Q5: ~0.32-0.35 of promised MW reached approval to energize; X7: ~0.19).
A TSP is the transmission owner at the point of interconnection, not the retail utility, so MW "at a G&T" is not
the same as MW "in co-op retail territory" (an Oncor-connected load can sit in a co-op's certificated area, not
verified how often).

Pure functions on DataFrames (tested in ``tests/models/test_gt_large_load.py``); the ``load_*`` functions are thin
read-only queries through ``models.db``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import polars as pl

from basecast_pipelines.models.db import read_sql

TOTAL_NAME = "Total"
AGGREGATE_NAME = "Aggregate of TSPs with < 3 sites"
YEARS = tuple(range(2026, 2033))


@dataclass(frozen=True)
class TspEntity:
    entity_type: str  # iou | transmission_only | muni | public_power | gt_coop | dist_coop | unknown
    ercot_member: str | None  # regex on ``ercot_members.member_name`` for the segment cross-check
    gt_name: str | None  # the name in ``puct_ccn_territories.gt_cooperative`` (wholesale supplier), if any


# RFI TSP name -> entity. LCRA is classified as public power (a state-created river authority, not a co-op),
# although ERCOT seats it in its "cooperative" membership segment; ``co_op_shares`` reports both readings.
TSP_ENTITIES: dict[str, TspEntity] = {
    "Oncor": TspEntity("iou", r"^Oncor Electric Delivery", None),
    "AEP": TspEntity("iou", r"^(AEP|American Electric Power)", None),
    "TNMP": TspEntity("iou", r"^Texas-New Mexico Power", None),
    "CenterPoint": TspEntity("iou", r"^CenterPoint Energy", None),
    "WETT": TspEntity("transmission_only", r"^Wind Energy Transmission Texas", None),
    "Lone Star": TspEntity("transmission_only", r"^Lone Star Transmission", None),
    "LCRA": TspEntity("public_power", r"^Lower Colorado River Authority", "LCRA"),
    "CPS": TspEntity("muni", r"CPS Energy", None),
    "Brazos": TspEntity("gt_coop", r"^Brazos Electric Power Coop", "Brazos Electric Power Coop"),
    "Golden Spread": TspEntity("gt_coop", r"^Golden Spread Electric Coop", "Golden Spread Electric Coop"),
    "Rayburn": TspEntity("gt_coop", r"^Rayburn Country Electric Coop", "Rayburn Electric Coop"),
    "STEC (South Texas)": TspEntity(
        "gt_coop", r"^South Texas Electric Coop", "San Miguel Electric Coop | South Texas Electric Coop"
    ),
    AGGREGATE_NAME: TspEntity("unknown", None, None),
}

# Entity type -> the group of the headline split, and the ERCOT segment it should sit in (None = no own segment).
ENTITY_GROUP = {
    "iou": "IOU", "transmission_only": "IOU", "muni": "public power", "public_power": "public power",
    "gt_coop": "co-op", "dist_coop": "co-op", "unknown": "unknown",
}
EXPECTED_SEGMENT = {
    "iou": "investor_owned_utility", "transmission_only": "investor_owned_utility", "muni": "municipal",
    "public_power": None, "gt_coop": "cooperative", "dist_coop": "cooperative", "unknown": None,
}
# Accounts that are an RFI TSP themselves (PUCT company name -> RFI name).
SELF_TSP = {"CPS Energy": "CPS"}


def gt_to_tsp(entities: dict[str, TspEntity] = TSP_ENTITIES) -> dict[str, str]:
    """``gt_cooperative`` name -> RFI TSP name."""
    return {e.gt_name: tsp for tsp, e in entities.items() if e.gt_name}


# --- the RFI by TSP and by entity type -------------------------------------------------------------------------


def rfi_by_tsp(rfi: pl.DataFrame) -> pl.DataFrame:
    """RFI rows of the ``tsp`` breakdown (``name``, ``is_total``, ``year``, ``mw``; PPTX and PDF copies) -> one row
    per TSP and year (the max over copies, which carry the same numbers). Keeps the ``Total`` row, flagged."""
    return (
        rfi.group_by("name", "year")
        .agg(pl.col("mw").max(), pl.col("is_total").any())
        .sort("name", "year")
    )


def check_total(tsp: pl.DataFrame) -> pl.DataFrame:
    """Per year: the sum of the TSP rows, the filed total and the difference (MW)."""
    parts = tsp.filter(~pl.col("is_total")).group_by("year").agg(pl.col("mw").sum().alias("sum_tsp"))
    tot = tsp.filter(pl.col("is_total")).select("year", pl.col("mw").alias("total"))
    return parts.join(tot, on="year", how="full", coalesce=True).with_columns(
        (pl.col("sum_tsp") - pl.col("total")).alias("diff")
    ).sort("year")


def classify(tsp: pl.DataFrame, entities: dict[str, TspEntity] = TSP_ENTITIES) -> pl.DataFrame:
    """Add ``entity_type`` and ``group`` to TSP rows (``name``); an unknown name raises."""
    missing = set(tsp.filter(~pl.col("is_total"))["name"].to_list()) - set(entities)
    if missing:
        raise KeyError(f"RFI TSPs without an entity type: {sorted(missing)}")
    types = {k: v.entity_type for k, v in entities.items()}
    return tsp.filter(~pl.col("is_total")).with_columns(
        pl.col("name").replace_strict(types).alias("entity_type")
    ).with_columns(pl.col("entity_type").replace_strict(ENTITY_GROUP).alias("group"))


def by_entity_type(tsp: pl.DataFrame, entities: dict[str, TspEntity] = TSP_ENTITIES, *, col: str = "entity_type") -> pl.DataFrame:
    """MW and share of the year's TSP sum by ``col`` (``entity_type`` or ``group``) and year."""
    c = classify(tsp, entities)
    out = c.group_by("year", col).agg(pl.col("mw").sum(), pl.col("name").sort().str.join(", ").alias("tsps"))
    return out.with_columns((pl.col("mw") / pl.col("mw").sum().over("year")).alias("share")).sort("year", col)


def co_op_shares(tsp: pl.DataFrame, entities: dict[str, TspEntity] = TSP_ENTITIES) -> pl.DataFrame:
    """Per year, the co-op share of requested MW under three readings: G&T co-ops only (``coop``), plus LCRA as ERCOT
    seats it (``coop_with_lcra``), and G&T co-ops over the known TSPs only (``coop_known``, the aggregate of TSPs
    with < 3 sites left out of the denominator)."""
    c = classify(tsp, entities)
    is_coop = pl.col("group") == "co-op"
    return (
        c.group_by("year")
        .agg(
            pl.col("mw").sum().alias("total"),
            pl.col("mw").filter(is_coop).sum().alias("coop_mw"),
            pl.col("mw").filter(pl.col("name") == "LCRA").sum().alias("lcra_mw"),
            pl.col("mw").filter(pl.col("entity_type") == "unknown").sum().alias("unknown_mw"),
        )
        .with_columns(
            (pl.col("coop_mw") / pl.col("total")).alias("coop"),
            ((pl.col("coop_mw") + pl.col("lcra_mw")) / pl.col("total")).alias("coop_with_lcra"),
            (pl.col("coop_mw") / (pl.col("total") - pl.col("unknown_mw"))).alias("coop_known"),
        )
        .sort("year")
    )


def check_segments(members: pl.DataFrame, entities: dict[str, TspEntity] = TSP_ENTITIES) -> pl.DataFrame:
    """Cross-check each TSP's entity type against its ERCOT membership segment (``ercot_members``: ``year``,
    ``segment``, ``member_name``; the latest year a member appears). ``status``: ``agrees``, ``override`` (the type
    has no ERCOT segment of its own, e.g. LCRA as public power), ``differs`` or ``not_found``."""
    rows = []
    for tsp, e in entities.items():
        if e.ercot_member is None:
            continue
        hit = members.filter(pl.col("member_name").str.contains(e.ercot_member)).sort("year", descending=True)
        expected = EXPECTED_SEGMENT[e.entity_type]
        if hit.is_empty():
            rows.append({"tsp": tsp, "entity_type": e.entity_type, "ercot_member": None, "year": None,
                         "segment": None, "expected": expected, "status": "not_found"})
            continue
        r = hit.row(0, named=True)
        status = "override" if expected is None else ("agrees" if r["segment"] == expected else "differs")
        rows.append({"tsp": tsp, "entity_type": e.entity_type, "ercot_member": r["member_name"], "year": r["year"],
                     "segment": r["segment"], "expected": expected, "status": status})
    return pl.DataFrame(rows, schema={"tsp": pl.Utf8, "entity_type": pl.Utf8, "ercot_member": pl.Utf8,
                                      "year": pl.Int64, "segment": pl.Utf8, "expected": pl.Utf8, "status": pl.Utf8})


# --- G&T -> members ---------------------------------------------------------------------------------------------


def gt_members(accounts: pl.DataFrame, entities: dict[str, TspEntity] = TSP_ENTITIES) -> pl.DataFrame:
    """Account x RFI TSP rows. ``accounts``: ``account_id``, ``name``, ``gt_cooperative`` (``;``-separated). A
    member with two suppliers gets one row per supplier and ``n_gt`` = its number of suppliers; accounts that are
    an RFI TSP themselves (:data:`SELF_TSP`) get their own row (``via`` = ``self``). Suppliers not in the RFI
    (e.g. East Texas Electric Coop) keep ``tsp`` null."""
    lookup = gt_to_tsp(entities)
    split = (
        accounts.filter(pl.col("gt_cooperative").is_not_null())
        .with_columns(pl.col("gt_cooperative").str.split(";").alias("gt"))
        .explode("gt", empty_as_null=True)
        .with_columns(pl.col("gt").str.strip_chars())
        .with_columns(pl.len().over("account_id").alias("n_gt"))
        .select("account_id", "gt", "n_gt", pl.col("gt").replace_strict(lookup, default=None).alias("tsp"),
                pl.lit("g&t").alias("via"))
    )
    own = accounts.filter(pl.col("name").is_in(list(SELF_TSP))).select(
        "account_id", pl.col("name").alias("gt"), pl.lit(1, pl.UInt32).alias("n_gt"),
        pl.col("name").replace_strict(SELF_TSP).alias("tsp"), pl.lit("self").alias("via"),
    )
    return pl.concat([split.with_columns(pl.col("n_gt").cast(pl.UInt32)), own], how="vertical")


def allocate(gt_mw: pl.DataFrame, weights: pl.DataFrame) -> pl.DataFrame:
    """Spread each TSP's MW over its members by a weight: ``gt_mw`` (``tsp``, ``mw``), ``weights`` (``account_id``,
    ``tsp``, ``n_gt``, ``weight``). A member with two suppliers brings ``weight / n_gt`` to each (assumption); a
    member with a null or zero weight gets nothing. Returns ``account_id``, ``tsp``, ``share``, ``mw``."""
    w = weights.filter(pl.col("weight").is_not_null(), pl.col("weight") > 0).with_columns(
        (pl.col("weight") / pl.col("n_gt")).alias("_w")
    )
    w = w.with_columns((pl.col("_w") / pl.col("_w").sum().over("tsp")).alias("share"))
    return w.join(gt_mw.select("tsp", "mw"), on="tsp", how="inner").select(
        "account_id", "tsp", "share", (pl.col("share") * pl.col("mw")).alias("mw")
    )


def method_spread(alloc: pl.DataFrame, methods: list[str]) -> pl.DataFrame:
    """Per member and TSP: the min and max allocated MW over ``methods`` (columns of ``alloc``) and their ratio."""
    return alloc.with_columns(
        pl.min_horizontal(*methods).alias("min_mw"), pl.max_horizontal(*methods).alias("max_mw")
    ).with_columns(
        pl.when(pl.col("min_mw") > 0).then(pl.col("max_mw") / pl.col("min_mw")).otherwise(None).alias("max_over_min")
    )


# --- the per-account fact ---------------------------------------------------------------------------------------


def exposure_facts(members: pl.DataFrame, tsp: pl.DataFrame, *, near: int = 2030, far: int = 2032) -> pl.DataFrame:
    """One row per account and RFI supplier: the supplier's requested MW in 2026, ``near`` and ``far``, its share of
    the RFI total in ``near``, the number of accounts sharing the fact (``n_accounts`` in ``members``), and the
    sentence for the account page. ``members``: output of :func:`gt_members` (rows with a ``tsp``)."""
    t = tsp.filter(~pl.col("is_total"))
    total = tsp.filter(pl.col("is_total"), pl.col("year") == near)["mw"]
    total_near = float(total[0]) if total.len() else float(t.filter(pl.col("year") == near)["mw"].sum())
    wide = t.filter(pl.col("year").is_in([2026, near, far])).pivot(on="year", index="name", values="mw")
    wide = wide.rename({"name": "tsp", "2026": "mw_2026", str(near): "mw_near", str(far): "mw_far"})
    m = members.filter(pl.col("tsp").is_not_null())
    shared = m.group_by("tsp").agg(pl.col("account_id").n_unique().alias("n_accounts"))
    return (
        m.join(wide, on="tsp", how="inner")
        .join(shared, on="tsp", how="left")
        .with_columns((pl.col("mw_near") / total_near).alias("share_near"))
        .with_columns(
            pl.format(
                "{} reported {} GW of large-load requests to be in service by {} ({} GW by {}; {}% of the "
                "ERCOT-wide RFI), up from {} GW in 2026. Requests, not forecasts; no location below the TSP.",
                pl.when(pl.col("via") == "self").then(pl.lit("As a TSP, you"))
                .otherwise(pl.format("Your wholesale supplier {}", pl.col("gt"))),
                (pl.col("mw_near") / 1000).round(1), pl.lit(near),
                (pl.col("mw_far") / 1000).round(1), pl.lit(far), (pl.col("share_near") * 100).round(1),
                (pl.col("mw_2026") / 1000).round(1),
            ).alias("fact")
        )
        .sort("account_id", "tsp")
    )


def growth_multiple(tsp: pl.DataFrame, *, start: int = 2027, end: int = 2030) -> pl.DataFrame:
    """Per TSP, MW in ``end`` / MW in ``start`` (how back-loaded its requests are)."""
    t = tsp.filter(~pl.col("is_total"), pl.col("year").is_in([start, end]))
    w = t.pivot(on="year", index="name", values="mw").rename({str(start): "mw_start", str(end): "mw_end"})
    return w.with_columns(
        pl.when(pl.col("mw_start") > 0).then(pl.col("mw_end") / pl.col("mw_start")).otherwise(None).alias("multiple")
    ).sort("multiple", descending=True, nulls_last=True)


def slug(name: str) -> str:
    """File-name-safe slug of a TSP name."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


# EIA-861 ``ownership`` -> the RFI groups, for the baseline "who sells ERCOT's energy today". Retail power marketers
# sell in the competitive (IOU wires) areas; munis and state entities are public power.
OWNERSHIP_GROUP = {
    "Cooperative": "co-op", "Municipal": "public power", "State": "public power", "Political Subdivision": "public power",
    "Retail Power Marketer": "IOU", "Investor Owned": "IOU",
}


def sales_shares(sales: pl.DataFrame, *, year: int) -> pl.DataFrame:
    """Share of ERCOT retail sales by RFI group in ``year`` from ``eia861_sales`` total-sector rows under balancing
    authority ERCO (final release; part C delivery-only rows dropped, their energy is sold by a marketer too).
    Ownership not in :data:`OWNERSHIP_GROUP` (null, behind the meter) is ``unknown``."""
    s = sales.filter(
        pl.col("data_year") == year, ~pl.col("early_release"), pl.col("part").fill_null("") != "C",
        pl.col("sector") == "total", pl.col("ba_code") == "ERCO",
    ).with_columns(pl.col("ownership").replace_strict(OWNERSHIP_GROUP, default="unknown").alias("group"))
    out = s.group_by("group").agg(pl.col("sales_mwh").sum())
    return out.with_columns((pl.col("sales_mwh") / pl.col("sales_mwh").sum()).alias("share")).sort("group")


# --- thin loaders (read-only) -----------------------------------------------------------------------------------


def load_rfi() -> pl.DataFrame:
    return read_sql(
        """
        select name, is_total, year, mw, filed_date, member from puct_tsp_large_load_requests
        where breakdown = 'tsp'
        """
    )


def load_ercot_sales() -> pl.DataFrame:
    return read_sql(
        """
        select data_year, early_release, part, ownership, ba_code, sector, sales_mwh from eia861_sales
        where ba_code = 'ERCO' and sector = 'total'
        """
    )


def load_ercot_members() -> pl.DataFrame:
    return read_sql("select year, segment, member_name from ercot_members")


def load_gt_accounts() -> pl.DataFrame:
    """ERCOT co-ops and munis with their PUCT wholesale supplier(s)."""
    return read_sql(
        """
        select ccn_no as account_id, company_name as name, utility_type as account_type, gt_cooperative
        from puct_ccn_territories
        where utility_type in ('coop', 'muni') and in_ercot
        """
    )
