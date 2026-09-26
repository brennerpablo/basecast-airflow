"""Per-account diagnosis for the commercial-intelligence detail page (``/accounts/[id]``), exploration X9.

One account (an ERCOT co-op or muni, ``account_id`` = PUCT ``ccn_no``) is assembled into the six blocks the
partnerships team reads in 30 seconds: **header** (who they are), **score** (why they rank where they do),
**triggers** (why now), **territory** (what is happening around them), **EIA series** (customers, sales,
revenue and price by year) and **next action** (the rule that fired and the offer angle). Every value carries
its unit, source table and as-of date; a value the sources do not have stays ``None`` and becomes a data gap.

Nothing here computes a score or an event: the universe, signals and score come from
:mod:`basecast_pipelines.models.accounts` and :mod:`basecast_pipelines.models.triggers`, the EIA series from
:mod:`basecast_pipelines.models.eia861_short_form`, the adjusted queue from
:mod:`basecast_pipelines.models.queue_adjusted` (X2 output) and the zone 4CP numbers from
:func:`basecast_pipelines.models.four_cp.zone_coincidence`. The caller builds :class:`DiagnosisInputs` once for
the scored universe; :func:`assemble` then only selects and formats. An account that is not in the scored
universe (the held-out validation accounts) raises instead of being rendered.

Everything is a pure function on DataFrames (``tests/models/test_diagnosis.py``); there are no loaders here.
All sources are public: nothing is simulated.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

import polars as pl

from basecast_pipelines.models import triggers as T

AS_OF = T.AS_OF
MIN_COUNTY_SHARE = T.MIN_COUNTY_SHARE
CP_WINDOW = "15:45–17:45 CPT"  # X3 default 4CP discharge window (in-sample, 63 of 64 CPs 2010–2025)
# A zone whose own summer peaks end outside the 4CP window (before 16:00 or after 18:00, local) needs a different
# discharge for its own peak than for the 4CP: the offer must say which one it serves (X3).
PEAK_WINDOW_HOURS = (16.0, 18.0)
# Q7 hold-out MAPE (%) of the weather + trend zone peak model, zones above 10% (docs/analysis/q7_organic_peak.md).
Q7_FLAGGED_ZONES = {"NORTH": 47.0, "FWEST": 35.3, "SCENT": 10.1}
UNDER_COUNT = 0.25  # apportioned homes per meter below this: a city territory the county area split misses
ACTION_LABEL = {"call_now": "Call now", "nurture": "Nurture", "watch": "Watch (alert)", "hold": "Hold"}


# --- facts ---------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Source:
    """Where a value comes from: the table (or model output) and its as-of date."""

    table: str
    as_of: date | None
    note: str = ""


def fact(key: str, label: str, value: Any, unit: str | None, src: Source, note: str | None = None) -> dict:
    """One displayed value with its provenance. ``value`` None = not available (a data gap)."""
    if isinstance(value, float) and math.isnan(value):
        value = None
    return {
        "key": key,
        "label": label,
        "value": value,
        "unit": unit,
        "source": src.table,
        "as_of": src.as_of,
        "note": note or (src.note or None),
    }


def _row(df: pl.DataFrame, key: str, value: Any) -> dict | None:
    hit = df.filter(pl.col(key) == value)
    return hit.row(0, named=True) if hit.height else None


def _cagr(now: float | None, then: float | None, span: int) -> float | None:
    if now is None or then is None or now <= 0 or then <= 0 or span <= 0:
        return None
    return (now / then) ** (1 / span) - 1


# --- inputs ----------------------------------------------------------------------------------------------------


@dataclass
class DiagnosisInputs:
    """Universe-wide frames, built once by the caller (validation accounts already removed).

    - ``accounts``: ``account_id``, ``name``, ``account_type``, ``eia_utility_id``, ``gt_cooperative``.
    - ``links``: ``account_id``, ``county_fips``, ``overlap_km2``, ``county_share``, ``territory_share``.
    - ``geo``: ``county_fips``, ``county_name``. ``county_zone``: ``county_fips``, ``weather_zone``.
    - ``signals``: raw signals per account (``accounts.build_signals``). ``scored``: ``account_id``,
      ``pct_<signal>``, ``score``, ``rank``, ``tier`` (``triggers.score_accounts`` + ``score_tier``).
    - ``weights``: signal → weight; ``signal_meta``: signal → (label, unit, source key).
    - ``events``: account × event rows (``triggers.EVENT_COLUMNS`` + ``exposure``), every date.
    - ``eia``: one row per EIA utility, year and release (``eia861_short_form.combine_forms`` +
      ``with_price`` [+ ``add_delivery``]); ``res_price``: ``triggers.residential_price`` output.
    - ``dc_sites``: ``tceq_data_center_sites`` rows (``ref_num_txt``, ``reg_ent_name``, ``county_fips``,
      ``first_affil_begin_dt``, ``matched_by_name``).
    - ``queue``: county × stratum rows with ``raw_mw``, ``adj_mw_2027``, ``adj_mw_2028`` (X2).
    - ``permits_monthly``: ``county_fips``, ``period_start``, ``units_total``; ``permits_month``: latest month.
    - ``zone_peaks``: LTLF ``region_id``, ``target_year``, ``value``; ``zone_cp``: ``four_cp.zone_coincidence``.
    - ``sources``: source key → :class:`Source`.
    """

    accounts: pl.DataFrame
    links: pl.DataFrame
    geo: pl.DataFrame
    county_zone: pl.DataFrame
    signals: pl.DataFrame
    scored: pl.DataFrame
    weights: dict[str, float]
    signal_meta: dict[str, tuple[str, str, str]]
    events: pl.DataFrame
    eia: pl.DataFrame
    res_price: pl.DataFrame
    dc_sites: pl.DataFrame
    queue: pl.DataFrame
    permits_monthly: pl.DataFrame
    permits_month: date
    zone_peaks: pl.DataFrame
    zone_cp: pl.DataFrame
    sources: dict[str, Source]
    as_of: date = AS_OF
    dc_since: date = date(2025, 1, 1)
    eia_years: tuple[int, int] = (2013, 2024)
    price_base_year: int = 2019
    ltlf_years: tuple[int, int] = (2025, 2031)
    extra: dict[str, Any] = field(default_factory=dict)

    def src(self, key: str) -> Source:
        return self.sources.get(key, Source(key, None, "as-of not recorded"))


# --- blocks ------------------------------------------------------------------------------------------------------


def account_counties(
    links: pl.DataFrame, geo: pl.DataFrame, county_zone: pl.DataFrame, account_id: str,
    *, min_share: float = MIN_COUNTY_SHARE,
) -> pl.DataFrame:
    """The account's counties by overlap area: ``county_share`` (of the county's land in the territory),
    ``territory_share`` (of the territory in the county), weather zone, and ``exposed`` (county events reach
    the account: ``county_share`` ≥ ``min_share``, the X5 rule)."""
    return (
        links.filter(pl.col("account_id") == account_id)
        .join(geo.select("county_fips", "county_name"), on="county_fips", how="left")
        .join(county_zone.select("county_fips", "weather_zone"), on="county_fips", how="left")
        .with_columns((pl.col("county_share") >= min_share).alias("exposed"))
        .sort("overlap_km2", descending=True)
        .select("county_fips", "county_name", "overlap_km2", "county_share", "territory_share", "weather_zone",
                "exposed")
    )


def with_context(counties: pl.DataFrame) -> tuple[pl.DataFrame, str]:
    """Add ``context``: the counties whose events describe the territory. The exposed counties when there are any;
    otherwise (most munis: a city covers a few % of its county) the home county, the one holding most of the
    territory. Returns the frame and a label saying which rule applied."""
    if counties.height == 0:
        return counties.with_columns(pl.lit(False).alias("context")), "no county"
    if counties["exposed"].any():
        n = int(counties["exposed"].sum())
        return counties.with_columns(pl.col("exposed").alias("context")), f"{n} exposed counties (share ≥ 20%)"
    home = counties.sort("territory_share", descending=True).row(0, named=True)
    label = (f"home county {home['county_name']} (the account covers {home['county_share']:.0%} of it; "
             "no county ≥ 20%)")
    return counties.with_columns((pl.col("county_fips") == home["county_fips"]).alias("context")), label


def zone_mix(counties: pl.DataFrame) -> pl.DataFrame:
    """Weather zones of the territory, as shares of its overlap area (largest first)."""
    z = counties.filter(pl.col("weather_zone").is_not_null())
    if z.height == 0:
        return pl.DataFrame(schema={"weather_zone": pl.Utf8, "area_share": pl.Float64})
    return (
        z.group_by("weather_zone")
        .agg(pl.col("overlap_km2").sum())
        .with_columns((pl.col("overlap_km2") / pl.col("overlap_km2").sum()).alias("area_share"))
        .sort("area_share", descending=True)
        .select("weather_zone", "area_share")
    )


def score_breakdown(
    scored_row: dict, signal_row: dict, weights: dict[str, float], meta: dict[str, tuple[str, str, str]],
    sources: dict[str, Source],
) -> pl.DataFrame:
    """One row per score signal: raw value, percentile rank, weight as configured and as used (renormalized over
    the signals the account has), and its contribution; contributions sum to the score."""
    have = {k: scored_row.get(f"pct_{k}") is not None for k in weights}
    den = sum(w for k, w in weights.items() if have[k])
    rows = []
    for k, w in weights.items():
        label, unit, src_key = meta.get(k, (k, "", k))
        pct = scored_row.get(f"pct_{k}")
        used = w / den if have[k] and den else 0.0
        src = sources.get(src_key, Source(src_key, None))
        rows.append({
            "signal": k, "label": label, "raw": signal_row.get(k), "unit": unit, "pct": pct, "weight": w,
            "weight_used": used, "contribution": (pct or 0.0) * used, "source": src.table, "as_of": src.as_of,
        })
    return pl.DataFrame(rows, schema={
        "signal": pl.Utf8, "label": pl.Utf8, "raw": pl.Float64, "unit": pl.Utf8, "pct": pl.Float64,
        "weight": pl.Float64, "weight_used": pl.Float64, "contribution": pl.Float64, "source": pl.Utf8,
        "as_of": pl.Date,
    }).sort("contribution", descending=True)


def trigger_timeline(
    events: pl.DataFrame, account_id: str, *, as_of: date = AS_OF, window_days: int = T.WINDOW_DAYS,
    counties: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Every event of the account (newest first) with strength, mapping, offer angle, age and ``active``
    (dated in the last ``window_days``; future-dated events are kept but never active)."""
    ev = events.filter(pl.col("account_id") == account_id)
    spec = {k: (v.label, v.strength, v.mapping, v.offer) for k, v in T.TRIGGERS.items()}
    out = ev.with_columns(
        (pl.lit(as_of) - pl.col("event_date")).dt.total_days().alias("age_days"),
        ((pl.col("event_date") > as_of - timedelta(days=window_days)) & (pl.col("event_date") <= as_of)).alias(
            "active"
        ),
        pl.col("trigger").replace_strict({k: v[0] for k, v in spec.items()}, default=None).alias("label"),
        pl.col("trigger").replace_strict({k: v[1] for k, v in spec.items()}, default=None).alias("strength"),
        pl.when(pl.col("county_fips").is_null()).then(pl.lit("name")).otherwise(pl.lit("county")).alias("mapping"),
        pl.col("trigger").replace_strict({k: v[3] for k, v in spec.items()}, default=None).alias("offer"),
    )
    if counties is not None and counties.height:
        out = out.join(counties.select("county_fips", "county_name"), on="county_fips", how="left")
    else:
        out = out.with_columns(pl.lit(None, pl.Utf8).alias("county_name"))
    return out.sort(["event_date", "trigger"], descending=[True, False]).select(
        "event_date", "age_days", "active", "trigger", "label", "strength", "title", "detail", "county_fips",
        "county_name", "exposure", "mapping", "source", "source_ref", "offer",
    )


def rule_text(tier: str, rank: int, n: int, n_strong: int, strong_age_days: int | None, action: str) -> str:
    """The next-action rule that fired, in words (``triggers.next_action``)."""
    if n_strong == 0:
        why = "no active strong trigger"
    else:
        why = f"{n_strong} active strong trigger{'s' if n_strong > 1 else ''} (freshest {strong_age_days} days old)"
    return f"Tier {tier} (rank {rank} of {n}) + {why} → {ACTION_LABEL.get(action, action)}"


def action_on(tier: str, timeline: pl.DataFrame, day: date, *, window_days: int = T.WINDOW_DAYS) -> str:
    """The next action on ``day`` from the events already known (no new events assumed)."""
    strong = timeline.filter(
        pl.col("strength") == "strong",
        pl.col("event_date") > day - timedelta(days=window_days),
        pl.col("event_date") <= day,
    )
    age = (day - strong["event_date"].max()).days if strong.height else None
    return T.next_action(tier, strong["trigger"].n_unique(), age)


def action_changes_on(tier: str, timeline: pl.DataFrame, as_of: date, *, window_days: int = T.WINDOW_DAYS,
                      fresh_days: int = T.FRESH_DAYS) -> tuple[date, str] | None:
    """The first day after ``as_of`` on which the action changes if nothing new happens (an event leaves the
    12-month window or stops being fresh, or a future-dated one starts), and the action from then; ``None``
    when it holds for a year."""
    now = action_on(tier, timeline, as_of, window_days=window_days)
    dates = timeline.filter(pl.col("strength") == "strong")["event_date"].drop_nulls().to_list()
    candidates = sorted({
        d for e in dates
        for d in (e + timedelta(days=window_days), e + timedelta(days=fresh_days + 1), e)
        if as_of < d <= as_of + timedelta(days=window_days)
    })
    for d in candidates:
        later = action_on(tier, timeline, d, window_days=window_days)
        if later != now:
            return d, later
    return None


def next_action_card(tier: str, rank: int, n: int, timeline: pl.DataFrame, four_cp: dict | None = None,
                     *, as_of: date = AS_OF) -> dict:
    """The rule-based next action (``triggers.next_action``) with the rule that fired, the lead trigger (the
    freshest active strong one) and the offer: the lead trigger's angle, then the context triggers' angles and the
    zone's 4CP line as talking points."""
    active = timeline.filter(pl.col("active"))
    strong = active.filter(pl.col("strength") == "strong").sort("event_date", descending=True)
    n_strong = strong["trigger"].n_unique()
    age = int(strong["age_days"].min()) if strong.height else None
    action = T.next_action(tier, n_strong, age)
    lead = strong.row(0, named=True) if strong.height else None
    context = active.filter(pl.col("strength") == "context")
    points = [f"{r['label']}: {r['offer']}" for r in context.unique("trigger", keep="first").sort("trigger").iter_rows(named=True)]
    if four_cp and four_cp.get("line"):
        points.append(four_cp["line"])
    change = action_changes_on(tier, timeline, as_of)
    return {
        "action": action,
        "action_label": ACTION_LABEL.get(action, action),
        "tier": tier,
        "rank": rank,
        "n_accounts": n,
        "n_strong": n_strong,
        "n_context": context["trigger"].n_unique(),
        "rule": rule_text(tier, rank, n, n_strong, age, action),
        "lead_trigger": None if lead is None else {
            "trigger": lead["trigger"], "title": lead["title"], "event_date": lead["event_date"],
            "source": lead["source"], "source_ref": lead["source_ref"],
        },
        "offer": lead["offer"] if lead else "no strong trigger: keep the account warm with the territory facts",
        "talking_points": points,
        "changes_on": None if change is None else change[0],
        "changes_to": None if change is None else change[1],
    }


def eia_series(
    eia: pl.DataFrame, res_price: pl.DataFrame, utility_id: str | None, *, years: tuple[int, int],
) -> pl.DataFrame:
    """Customers, sales, revenue and average price per year for one EIA utility (final releases in ``years``
    plus any later early release), with the form (long / short) and the long-form residential price.

    ``customers`` are bundled-service customers (the long-form rule); ``meters`` adds the delivery-only ones
    (EIA part C, retail choice), so a utility that opens to retail choice keeps its meter count (Lubbock, X4).
    Revenue, sales and price stay bundled-only."""
    cols = {
        "data_year": pl.Int64, "early_release": pl.Boolean, "form": pl.Utf8, "customers": pl.Float64,
        "delivery_customers": pl.Float64, "meters": pl.Float64, "sales_mwh": pl.Float64, "revenue_kusd": pl.Float64,
        "price_usd_kwh": pl.Float64, "res_price_usd_kwh": pl.Float64,
    }
    if utility_id is None:
        return pl.DataFrame(schema=cols)
    e = eia.filter(pl.col("utility_id") == utility_id)
    e = e.filter(
        (~pl.col("early_release") & pl.col("data_year").is_between(*years))
        | (pl.col("early_release") & (pl.col("data_year") > years[1]))
    )
    if "delivery_customers" not in e.columns:
        e = e.with_columns(pl.lit(None, pl.Float64).alias("delivery_customers"))
    rp = res_price.filter(pl.col("utility_id") == utility_id).select(
        "data_year", "early_release", pl.col("usd_per_kwh").alias("res_price_usd_kwh")
    )
    return (
        e.join(rp, on=["data_year", "early_release"], how="left")
        .select(
            pl.col("data_year").cast(pl.Int64), pl.col("early_release"), pl.col("form"),
            pl.col("customers").cast(pl.Float64), pl.col("delivery_customers").cast(pl.Float64),
            (pl.col("customers") + pl.col("delivery_customers").fill_null(0)).cast(pl.Float64).alias("meters"),
            pl.col("sales_mwh").cast(pl.Float64), pl.col("revenue_thousand_usd").cast(pl.Float64).alias("revenue_kusd"),
            pl.col("price_usd_kwh").cast(pl.Float64), pl.col("res_price_usd_kwh").cast(pl.Float64),
        )
        .sort("data_year", "early_release")
    )


def series_value(series: pl.DataFrame, year: int, col: str) -> float | None:
    hit = series.filter(pl.col("data_year") == year, ~pl.col("early_release"))
    return None if hit.height == 0 else hit[col][0]


def yoy_breaks(series: pl.DataFrame, col: str = "meters", threshold: float = 0.5) -> list[tuple[int, float]]:
    """Final-release years whose ``col`` moved more than ``threshold`` against the previous year."""
    s = series.filter(~pl.col("early_release")).sort("data_year").with_columns(
        (pl.col(col) / pl.col(col).shift(1) - 1).alias("_chg"), pl.col("data_year").shift(1).alias("_prev")
    )
    hits = s.filter((pl.col("_chg").abs() > threshold) & (pl.col("data_year") - pl.col("_prev") == 1))
    return [(int(y), float(c)) for y, c in hits.select("data_year", "_chg").rows()]


def data_centers_near(sites: pl.DataFrame, counties: pl.DataFrame, *, since: date) -> pl.DataFrame:
    """TCEQ data-center sites first permitted since ``since`` in any county of the account, with that county's
    share (the score's expected count is the sum of ``county_share``), whether a county trigger can fire
    (``exposed``) and whether the county is a context county (:func:`with_context`)."""
    ctx = counties if "context" in counties.columns else counties.with_columns(pl.col("exposed").alias("context"))
    return (
        sites.filter(pl.col("first_affil_begin_dt") >= since)
        .join(ctx.select("county_fips", "county_name", "county_share", "exposed", "context"), on="county_fips",
              how="inner")
        .sort("first_affil_begin_dt", descending=True)
        .select(
            "first_affil_begin_dt", "reg_ent_name", "ref_num_txt", "county_name", "county_share", "exposed", "context",
            pl.when(pl.col("matched_by_name")).then(pl.lit("name")).otherwise(pl.lit("NAICS 518210 only"))
            .alias("matched_by"),
        )
    )


def queue_near(queue: pl.DataFrame, counties: pl.DataFrame) -> pl.DataFrame:
    """Raw vs adjusted generation queue (X2) by stratum, two ways: summed over the **context** counties
    (:func:`with_context`) and **apportioned** by ``county_share`` over every county of the account. A context
    county without projects counts as 0 (the GIS report covers the whole state)."""
    ctx = counties if "context" in counties.columns else counties.with_columns(pl.col("exposed").alias("context"))
    q = queue.join(ctx.select("county_fips", "county_share", "context"), on="county_fips", how="inner")
    val = ["raw_mw", "adj_mw_2027", "adj_mw_2028"]
    exp = q.filter(pl.col("context")).group_by("stratum").agg(
        pl.col("projects").sum().alias("projects_context"), *[pl.col(c).sum().alias(f"{c}_context") for c in val]
    )
    app = q.group_by("stratum").agg(*[(pl.col(c) * pl.col("county_share")).sum().alias(f"{c}_apportioned") for c in val])
    out = app.join(exp, on="stratum", how="left").fill_null(0)
    if out.height:
        total = out.select(pl.lit("total").alias("stratum"), *[pl.col(c).sum() for c in out.columns if c != "stratum"])
        out = pl.concat([out.sort("stratum"), total], how="vertical_relaxed")
    return out


def permit_trend(monthly: pl.DataFrame, counties: pl.DataFrame, *, as_of_month: date) -> dict:
    """Apportioned residential permit units in the 12 months to ``as_of_month`` vs the 12 before (same windows
    as ``triggers.permit_surge_events``). Counties without permit-issuing places have no rows (null, not 0)."""
    end = as_of_month
    nxt = date(end.year - 1, end.month, 1) + timedelta(days=32)
    start_recent = date(nxt.year, nxt.month, 1)
    start_prior = date(start_recent.year - 1, start_recent.month, 1)
    m = monthly.filter(pl.col("period_start") >= start_prior, pl.col("period_start") <= end)
    j = m.join(counties.select("county_fips", "county_share"), on="county_fips", how="inner")
    if j.height == 0:
        return {"recent": None, "prior": None, "change": None, "window_end": end}
    recent = j.filter(pl.col("period_start") >= start_recent).select(
        (pl.col("units_total") * pl.col("county_share")).sum()).item()
    prior = j.filter(pl.col("period_start") < start_recent).select(
        (pl.col("units_total") * pl.col("county_share")).sum()).item()
    return {"recent": recent, "prior": prior, "change": (recent / prior - 1) if prior else None, "window_end": end}


def zone_outlook(zone_peaks: pl.DataFrame, zone_cp: pl.DataFrame, zone: str | None, *,
                 ltlf_years: tuple[int, int], as_of: date = AS_OF, hour_years: int = 5) -> dict:
    """The primary weather zone's peak outlook and 4CP exposure.

    - LTLF (``ercot_adjusted``) summer peak for the current year and the end year, and its CAGR;
    - the zone's own summer peak (15-min, June–September) in the latest two years, and the current year's actual
      against the LTLF value for that year;
    - 4CP: the zone's mean load at the four ERCOT CPs, coincidence factor, 4CP share, energy share and 4CP
      intensity (share ÷ energy share) for the latest complete summer (4 months), and its own-peak end hour as the
      mean of the last ``hour_years`` complete summers (X3 reads 2021–2025; one summer is noisy).
    """
    out: dict[str, Any] = {"zone": zone}
    if zone is None:
        return out
    zp = zone_peaks.filter(pl.col("region_id") == zone)
    f = dict(zip(zp["target_year"].to_list(), zp["value"].to_list(), strict=True))
    y0, y1 = ltlf_years
    out.update({
        "ltlf_start": f.get(y0), "ltlf_end": f.get(y1), "ltlf_now": f.get(as_of.year),
        "ltlf_cagr": _cagr(f.get(y1), f.get(y0), y1 - y0),
    })
    cp = zone_cp.filter(pl.col("region_id") == zone).sort("year")
    full = cp.filter(pl.col("n_months") == 4)
    now = cp.filter(pl.col("year") == as_of.year)
    if full.height:
        r = full.row(-1, named=True)
        out.update({
            "cp_year": r["year"], "cp_avg_mw": r["cp_avg_mw"], "cf_summer": r["cf_summer"], "share_4cp": r["share_4cp"],
            "share_energy": r["share_energy"],
            "intensity": r["share_4cp"] / r["share_energy"] if r["share_energy"] else None,
            "ncp_summer_mw_full": r["ncp_summer_mw"],
            "ncp_end_hour": full.tail(hour_years)["ncp_end_hour"].mean(),
            "hour_years": f"{full.tail(hour_years)['year'].min()}–{r['year']}",
        })
    if now.height:
        r = now.row(0, named=True)
        out.update({"ncp_now_mw": r["ncp_summer_mw"], "ncp_now_months": r["n_months"]})
        if out.get("ltlf_now"):
            out["now_vs_ltlf"] = r["ncp_summer_mw"] / out["ltlf_now"] - 1
    hour = out.get("ncp_end_hour")
    lo, hi = PEAK_WINDOW_HOURS
    mismatch = None if hour is None else ("early" if hour < lo else "late" if hour > hi else None)
    out["peak_mismatch"] = mismatch
    out["q7_holdout_mape"] = Q7_FLAGGED_ZONES.get(zone)
    if out.get("cp_avg_mw") is not None:
        pitch = f"a {CP_WINDOW} discharge covers the 4CP (X3: 63 of 64 CPs, 2010–2025)"
        if mismatch:
            pitch += (f", but the zone's own peak ends {'before 16:00' if mismatch == 'early' else 'after 18:00'}: "
                      "own-peak and 4CP discharges differ, the offer must say which it serves")
        out["line"] = (f"4CP ({zone}, {out['cp_year']}): zone load at the CPs {out['cf_summer']:.0%} of its own peak, "
                       f"4CP intensity {out['intensity']:.2f}; {pitch}; no $/kW-yr rate in the repo (not verified)")
    return out


# --- assembly --------------------------------------------------------------------------------------------------


def assemble(account_id: str, inp: DiagnosisInputs) -> dict:
    """The full diagnosis of one account. Raises ``KeyError`` for an account outside the scored universe (the
    held-out validation accounts are never in it)."""
    s = _row(inp.scored, "account_id", account_id)
    if s is None:
        raise KeyError(f"account {account_id} is not in the scored universe")
    acct = _row(inp.accounts, "account_id", account_id) or {}
    sig = _row(inp.signals, "account_id", account_id) or {}
    n = inp.scored.height
    src = inp.src
    counties, context_label = with_context(account_counties(inp.links, inp.geo, inp.county_zone, account_id))
    has_context = bool(counties["context"].any()) if counties.height else False
    zones = zone_mix(counties)
    zone = zones["weather_zone"][0] if zones.height else None
    uid = acct.get("eia_utility_id")
    series = eia_series(inp.eia, inp.res_price, uid, years=inp.eia_years)
    y1 = inp.eia_years[1]
    final = series.filter(~pl.col("early_release"))
    last_year = int(final["data_year"].max()) if final.height else None
    eia_src = Source(src("eia_861").table, date(last_year, 12, 31) if last_year else None, src("eia_861").note)
    form = series_value(series, last_year, "form") if last_year else None
    cust = series_value(series, last_year, "meters") if last_year else None
    deliv = series_value(series, last_year, "delivery_customers") if last_year else None
    price = series_value(series, last_year, "price_usd_kwh") if last_year else None
    base = inp.price_base_year
    price_cagr = _cagr(price, series_value(series, base, "price_usd_kwh"), (last_year or base) - base)
    cust_cagr = _cagr(cust, series_value(series, base, "meters"), (last_year or base) - base)
    er = series.filter(pl.col("early_release"))
    regs = inp.events.filter(pl.col("account_id") == account_id, pl.col("trigger") == "market_registration")
    roles = sorted({r.split(":")[0] for r in regs["source_ref"].to_list()})
    ccn = src("puct_ccn_territories")
    header = [
        fact("name", "Name", acct.get("name"), None, ccn),
        fact("account_type", "Type", acct.get("account_type"), None, ccn),
        fact("ccn_no", "PUCT CCN", account_id, None, ccn),
        fact("gt", "G&T / wholesale supplier", acct.get("gt_cooperative"), None, ccn),
        fact("eia_utility_id", "EIA-861 utility id", uid, None, src("utility_crosswalk")),
        fact("counties", "Counties (largest overlap first)",
             ", ".join(counties["county_name"].drop_nulls().head(4).to_list()) or None, None,
             src("county_utility_overlap_puct"), f"{counties.height} counties"),
        fact("territory_km2", "Territory area in ERCOT counties", counties["overlap_km2"].sum() if counties.height else None,
             "km²", src("county_utility_overlap_puct")),
        fact("eia_form", "EIA-861 form (latest year)", form, None, eia_src,
             "short form: totals only, no sector split" if form == "short" else None),
        fact("customers", "Meters (all classes, incl. delivery-only)", cust, "customers", eia_src,
             f"{deliv:,.0f} delivery-only (retail choice); sales and revenue are bundled only" if deliv else None),
        fact("customer_cagr", f"Meter growth {base}→{last_year}", cust_cagr, "per year", eia_src),
        fact("sales_mwh", "Retail sales", series_value(series, last_year, "sales_mwh") if last_year else None, "MWh", eia_src),
        fact("revenue_kusd", "Retail revenue", series_value(series, last_year, "revenue_kusd") if last_year else None,
             "thousand USD", eia_src),
        fact("price", "Average price (all classes)", price, "USD/kWh", eia_src),
        fact("price_cagr", f"Price trend {base}→{last_year}", price_cagr, "per year", eia_src),
        fact("res_price", "Residential price", series_value(series, last_year, "res_price_usd_kwh") if last_year else None,
             "USD/kWh", eia_src, "long form only"),
        fact("price_early", "Average price, early release", er["price_usd_kwh"][0] if er.height else None, "USD/kWh",
             Source(src("eia_861").table, date(int(er["data_year"][0]), 12, 31) if er.height else None,
                    "early release, not final")),
        fact("ercot_roles", "ERCOT market roles registered (ever)", ", ".join(roles) or "none", None,
             src("ercot_market_participants")),
    ]
    score = score_breakdown(s, sig, inp.weights, inp.signal_meta, inp.sources)
    timeline = trigger_timeline(inp.events, account_id, as_of=inp.as_of, counties=counties)
    dcs = data_centers_near(inp.dc_sites, counties, since=inp.dc_since)
    queue = queue_near(inp.queue, counties)
    permits = permit_trend(inp.permits_monthly, counties, as_of_month=inp.permits_month)
    outlook = zone_outlook(inp.zone_peaks, inp.zone_cp, zone, ltlf_years=inp.ltlf_years, as_of=inp.as_of)
    zero = {f"{c}_context": 0.0 for c in ("raw_mw", "adj_mw_2027", "adj_mw_2028")} if has_context else {}
    q_tot = _row(queue, "stratum", "total") or zero
    q_sto = _row(queue, "stratum", "storage") or zero
    homes = sig.get("owner_sf_homes")
    pop_src, per_src, hou_src = src("census_population_county"), src("census_permits_county"), src("census_housing_county")
    ltlf, cp, qsrc = src("ltlf_forecasts"), src("ercot_monthly_peaks"), src("queue_adjusted")
    y0, yl = inp.ltlf_years
    territory = [
        fact("population", "Population (apportioned)", sig.get("population"), "people", pop_src),
        fact("pop_growth", "Population growth 2020→2025", sig.get("pop_growth"), "share", pop_src),
        fact("permits_per_1k", "Residential permits 2023–2025 per 1,000 residents", sig.get("permits_per_1k"),
             "units/1k", per_src),
        fact("permits_12m", "Permit units, last 12 months (apportioned)", permits["recent"], "units",
             Source(per_src.table, permits["window_end"])),
        fact("permits_12m_change", "Permit units vs the 12 months before", permits["change"], "share",
             Source(per_src.table, permits["window_end"])),
        fact("owner_sf_homes", "Owner-occupied single-family homes (apportioned)", homes, "homes", hou_src),
        fact("owner_sf_share", "Owner-occupied single-family share", sig.get("owner_sf_share"), "share", hou_src),
        fact("homes_per_meter", "Apportioned homes per EIA meter", (homes / cust) if homes and cust else None, "ratio",
             hou_src, "> 1 = area apportionment over-counts the territory (X5)"),
        fact("dc_sites_expected", "New data-center sites since 2025 (expected, by county share)", sig.get("dc_sites"),
             "sites", src("tceq_data_center_sites")),
        fact("dc_sites_context", "New data-center sites since 2025 in the context counties",
             dcs.filter(pl.col("context")).height if has_context else None, "sites", src("tceq_data_center_sites"),
             context_label),
        fact("queue_raw_mw", "Generation queue, raw (context counties)", q_tot.get("raw_mw_context"), "MW", qsrc,
             context_label),
        fact("queue_adj_2027", "Adjusted: expected COD by Dec 2027 (context counties)", q_tot.get("adj_mw_2027_context"),
             "MW", qsrc),
        fact("queue_adj_2028", "Adjusted: expected COD by Dec 2028 (context counties)", q_tot.get("adj_mw_2028_context"),
             "MW", qsrc),
        fact("storage_raw_mw", "Storage in the queue, raw (context counties)", q_sto.get("raw_mw_context"), "MW", qsrc),
        fact("storage_adj_2028", "Storage expected by Dec 2028 (context counties)", q_sto.get("adj_mw_2028_context"),
             "MW", qsrc),
        fact("weather_zone", "Primary weather zone (by area)", zone, None, src("county_weather_zone"),
             ", ".join(f"{z} {a:.0%}" for z, a in zones.rows()) if zones.height > 1 else None),
        fact("zone_ltlf_cagr", f"Zone summer peak outlook, LTLF 2025 CAGR {y0}→{yl}", outlook.get("ltlf_cagr"),
             "per year", ltlf),
        fact("zone_ltlf_end", f"Zone summer peak {yl} (LTLF 2025, ERCOT-adjusted)", outlook.get("ltlf_end"), "MW", ltlf),
        fact("zone_peak_now", f"Zone summer peak {inp.as_of.year} (15-min, actual)", outlook.get("ncp_now_mw"), "MW", cp,
             f"{outlook.get('ncp_now_months')} of 4 months; not final-settled" if outlook.get("ncp_now_mw") else None),
        fact("zone_peak_vs_ltlf", f"Zone {inp.as_of.year} actual vs LTLF 2025 for {inp.as_of.year}",
             outlook.get("now_vs_ltlf"), "share", ltlf),
        fact("zone_q7_mape", "Zone peak model hold-out MAPE (Q7, flagged zones only)", outlook.get("q7_holdout_mape"),
             "%", Source("docs/analysis/q7_organic_peak.md", date(2026, 9, 26))),
        fact("zone_4cp_mw", f"Zone load at the 4CP ({outlook.get('cp_year')})", outlook.get("cp_avg_mw"), "MW", cp),
        fact("zone_4cp_cf", "Zone load at the 4CP ÷ its own summer peak", outlook.get("cf_summer"), "ratio", cp),
        fact("zone_4cp_intensity", "4CP intensity (4CP share ÷ energy share)", outlook.get("intensity"), "ratio", cp),
        fact("zone_peak_hour", f"Zone's own monthly peaks end ({outlook.get('hour_years')} mean, local)",
             outlook.get("ncp_end_hour"), "hour", cp,
             "outside 16:00–18:00: own-peak and 4CP discharges differ (X3)" if outlook.get("peak_mismatch") else None),
        fact("account_4cp_mw", "Account load at the 4CP", None, "MW", Source("UtilityDataSource", None),
             "not public: needs the co-op's own meter data"),
    ]
    action = next_action_card(s["tier"], int(s["rank"]), n, timeline, outlook, as_of=inp.as_of)
    return {
        "account_id": account_id,
        "as_of": inp.as_of,
        "simulated": False,
        "header": header,
        "score": {"score": s["score"], "rank": int(s["rank"]), "n_accounts": n, "tier": s["tier"],
                  "method": "weighted mean of within-universe percentile ranks (config/account_score.yaml, pending review)",
                  "signals": score},
        "triggers": timeline,
        "territory": {"facts": territory, "counties": counties, "zones": zones, "data_centers": dcs, "queue": queue,
                      "zone_outlook": outlook},
        "eia_series": series,
        "eia_breaks": yoy_breaks(series),
        "next_action": action,
    }


# --- gaps and coverage -----------------------------------------------------------------------------------------


def all_facts(diag: dict) -> list[dict]:
    return [*diag["header"], *diag["territory"]["facts"]]


OPTIONAL_KEYS = frozenset({"zone_q7_mape"})  # absent by design (only flagged zones carry it)


def data_gaps(diag: dict, *, as_of: date = AS_OF, stale_days: int = 730) -> list[dict]:
    """Missing values, stale as-of dates (older than ``stale_days``) and structural limits of one diagnosis."""
    gaps = []
    for f in all_facts(diag):
        if f["key"] in OPTIONAL_KEYS:
            continue
        if f["value"] is None:
            gaps.append({"key": f["key"], "kind": "missing", "detail": f"{f['label']}: no value ({f['source']})"})
        elif f["as_of"] is not None and (as_of - f["as_of"]).days > stale_days:
            gaps.append({"key": f["key"], "kind": "stale",
                         "detail": f"{f['label']}: as of {f['as_of']} ({(as_of - f['as_of']).days} days)"})
        elif f["as_of"] is None:
            gaps.append({"key": f["key"], "kind": "no_as_of", "detail": f"{f['label']}: source has no as-of date"})
    counties = diag["territory"]["counties"]
    if counties.height and not counties["exposed"].any():
        gaps.append({"key": "exposed_counties", "kind": "structural",
                     "detail": "no county with share ≥ 20%: county triggers (data centers, IAs, permits, "
                               "transmission) can never fire for this account"})
    hpm = next((f["value"] for f in diag["territory"]["facts"] if f["key"] == "homes_per_meter"), None)
    if hpm is not None and hpm > 1:
        gaps.append({"key": "homes_per_meter", "kind": "quality",
                     "detail": f"{hpm:.2f} apportioned homes per meter: size and growth signals over-counted"})
    elif hpm is not None and hpm < UNDER_COUNT:
        gaps.append({"key": "homes_per_meter", "kind": "quality",
                     "detail": f"{hpm:.2f} apportioned homes per meter: area apportionment under-counts a city "
                               "territory (population, homes and permit counts biased low)"})
    for y, c in diag["eia_breaks"]:
        gaps.append({"key": "eia_series", "kind": "quality", "detail": f"customers {c:+.0%} in {y} vs {y - 1}"})
    series = diag["eia_series"]
    if series.height and (series["form"] == "short").any():
        years = series.filter(pl.col("form") == "short")["data_year"].to_list()
        gaps.append({"key": "eia_series", "kind": "coverage",
                     "detail": f"short form in {min(years)}–{max(years)}: no residential split or residential price"})
    return gaps


def coverage(diags: list[dict], keys: list[str] | None = None) -> pl.DataFrame:
    """Share of accounts with each fact present, overall and by account type."""
    rows = []
    for d in diags:
        typ = next((f["value"] for f in d["header"] if f["key"] == "account_type"), None)
        for f in all_facts(d):
            if keys is None or f["key"] in keys:
                rows.append({"key": f["key"], "label": f["label"], "account_type": typ, "present": f["value"] is not None})
    df = pl.DataFrame(rows)
    by = df.group_by("key", "label").agg(pl.col("present").mean().alias("all"), pl.len().alias("n"))
    typ = df.group_by("key", "account_type").agg(pl.col("present").mean().alias("p")).pivot(
        on="account_type", index="key", values="p")
    order = {k: i for i, k in enumerate(dict.fromkeys(r["key"] for r in rows))}
    return by.join(typ, on="key", how="left").sort(pl.col("key").replace_strict(order, return_dtype=pl.Int64))


# --- output ----------------------------------------------------------------------------------------------------


def to_jsonable(obj: Any) -> Any:
    """Diagnosis → JSON-ready values (DataFrames become lists of records, dates ISO strings)."""
    if isinstance(obj, pl.DataFrame):
        return [to_jsonable(r) for r in obj.iter_rows(named=True)]
    if isinstance(obj, dict):
        return {k: to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_jsonable(v) for v in obj]
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, float) and math.isnan(obj):
        return None
    return obj


def dumps(diag: dict) -> str:
    return json.dumps(to_jsonable(diag), indent=2, ensure_ascii=False)


def fmt(value: Any, unit: str | None) -> str:
    """A fact value for display."""
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (int, float)):
        v = float(value)
        if unit in ("share", "per year"):
            return f"{v:+.1%}" if unit == "per year" or v < 0 else f"{v:.1%}"
        if unit == "USD/kWh":
            return f"{v * 100:.2f} ¢/kWh"
        if unit == "thousand USD":
            return f"US$ {v / 1000:,.1f}M"
        if unit == "hour":
            return f"{int(v):02d}:{round((v % 1) * 60):02d}"
        if unit in ("ratio",):
            return f"{v:.2f}"
        if unit == "%":
            return f"{v:.1f}%"
        if abs(v) >= 100:
            return f"{v:,.0f} {unit or ''}".strip()
        return f"{v:,.2f} {unit or ''}".strip()
    return str(value)


def _as_of(d: Any) -> str:
    return d.isoformat() if isinstance(d, date) else "not recorded"


def render_markdown(diag: dict, *, max_events: int = 12) -> str:
    """The diagnosis as the markdown the prototype doc shows (one account)."""
    h = {f["key"]: f for f in diag["header"]}
    sc = diag["score"]
    act = diag["next_action"]
    name = h["name"]["value"]
    lines = [
        f"### {name} ({h['account_type']['value']}, CCN {diag['account_id']})",
        "",
        f"**{act['action_label']}** · score {sc['score']:.2f}, rank {sc['rank']} of {sc['n_accounts']}, tier {sc['tier']} · "
        f"as of {diag['as_of']} · all sources public (nothing simulated)",
        "",
        f"> **Rule:** {act['rule']}. **Offer:** {act['offer']}.",
    ]
    if act["changes_on"]:
        lines.append(f"> **Holds until** {act['changes_on']}, then {ACTION_LABEL[act['changes_to']]} "
                     "(if no new strong event).")
    if act["lead_trigger"]:
        lt = act["lead_trigger"]
        lines.append(f"> **Lead trigger:** {lt['title']} ({lt['event_date']}, `{lt['source']}` {lt['source_ref']}).")
    for p in act["talking_points"]:
        lines.append(f"> - {p}")
    lines += ["", "**Who they are**", "", "| Field | Value | Source | As of | Note |", "|---|---|---|---|---|"]
    for f in diag["header"]:
        if f["key"] in ("name", "account_type", "ccn_no"):
            continue
        lines.append(f"| {f['label']} | {fmt(f['value'], f['unit'])} | `{f['source']}` | {_as_of(f['as_of'])} | {f['note'] or ''} |")
    lines += ["", "**Why they rank here** (contributions sum to the score)", "",
              "| Signal | Raw | Percentile | Weight | Contribution | Source (as of) |", "|---|---|---|---|---|---|"]
    for r in sc["signals"].iter_rows(named=True):
        raw = fmt(r["raw"], r["unit"])
        pct = "—" if r["pct"] is None else f"{r['pct']:.2f}"
        lines.append(f"| {r['label']} | {raw} | {pct} | {r['weight_used']:.2f} | {r['contribution']:.3f} | "
                     f"`{r['source']}` ({_as_of(r['as_of'])}) |")
    tl = diag["triggers"]
    act_tl = tl.filter(pl.col("active"))
    lines += ["", f"**Why now** ({act_tl.height} active events in the last 12 months, {tl.height} ever; newest first)", ""]
    strong = act_tl.filter(pl.col("strength") == "strong")
    context = act_tl.filter(pl.col("strength") != "strong")
    if act_tl.height:
        lines += ["| Date | Age (d) | Trigger | Strength | Event | Where | Source |", "|---|---|---|---|---|---|---|"]
        for r in strong.head(max_events).iter_rows(named=True):
            where = (f"{r['county_name']} ({r['exposure']:.0%})" if r["mapping"] == "county" and r["county_name"]
                     else "by name")
            ev = f"{r['title']} — {r['detail']}".replace("|", "/")
            lines.append(f"| {r['event_date']} | {r['age_days']} | {r['trigger']} | strong | {ev} | {where} | "
                         f"`{r['source']}` {r['source_ref']} |")
        if strong.height > max_events:
            lines.append(f"| … | | | | {strong.height - max_events} more strong events | | |")
        for trig, g in context.group_by("trigger", maintain_order=True):
            g = g.sort("event_date", descending=True)
            r = g.row(0, named=True)
            ev = (f"{g.height} event{'s' if g.height > 1 else ''}; latest: {r['title']} — {r['detail']}").replace("|", "/")
            where = "by name" if r["mapping"] == "name" else ", ".join(sorted(set(g["county_name"].drop_nulls())))
            lines.append(f"| {r['event_date']} | {r['age_days']} | {trig[0]} | context | {ev} | {where} | "
                         f"`{r['source']}` |")
    else:
        lines.append("No active trigger.")
    lines += ["", "**Territory**", "", "| Field | Value | Source | As of | Note |", "|---|---|---|---|---|"]
    for f in diag["territory"]["facts"]:
        lines.append(f"| {f['label']} | {fmt(f['value'], f['unit'])} | `{f['source']}` | {_as_of(f['as_of'])} | {f['note'] or ''} |")
    c = diag["territory"]["counties"]
    lines += ["", "Counties: " + "; ".join(
        f"{r['county_name']} {r['county_share']:.0%} of county / {r['territory_share']:.0%} of territory"
        + (" (exposed)" if r["exposed"] else "") for r in c.head(6).iter_rows(named=True))
        + (f"; +{c.height - 6} more" if c.height > 6 else "")]
    dcs = diag["territory"]["data_centers"]
    if dcs.height:
        lines += ["", "Data-center sites since 2025 in its counties: " + "; ".join(
            f"{r['reg_ent_name']} ({r['county_name']}, {r['first_affil_begin_dt']}, {r['matched_by']}"
            + (", exposed" if r["exposed"] else "") + ")" for r in dcs.head(6).iter_rows(named=True))
            + (f"; +{dcs.height - 6} more" if dcs.height > 6 else "")]
    s = diag["eia_series"]
    lines += ["", "**EIA-861 series** (`eia861_sales` long form + 861S short form from the lake; final releases, "
              "early release marked)", "",
              "| Year | Form | Meters | Sales (GWh) | Revenue (US$M) | Avg price (¢/kWh) | Residential (¢/kWh) |",
              "|---|---|---|---|---|---|---|"]
    for r in s.iter_rows(named=True):
        yr = f"{r['data_year']}{' ER' if r['early_release'] else ''}"
        g = lambda v, k=1.0, p=1: "—" if v is None else f"{v * k:,.{p}f}"  # noqa: E731
        lines.append(f"| {yr} | {r['form']} | {g(r['customers'], p=0)} | {g(r['sales_mwh'], 1e-3)} | "
                     f"{g(r['revenue_kusd'], 1e-3)} | {g(r['price_usd_kwh'], 100, 2)} | {g(r['res_price_usd_kwh'], 100, 2)} |")
    if not s.height:
        lines.append("| — | no EIA-861 rows for this utility | | | | | |")
    return "\n".join(lines) + "\n"
