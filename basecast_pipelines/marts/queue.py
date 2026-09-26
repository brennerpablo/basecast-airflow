"""The adjusted generation queue of today (X2 "Pipeline"), shared by the account and Explorer marts.

``truncate_at`` → ``fit_stage_curves(..., fit_stages(<queue.model>))`` → ``build_queue`` → ``score``, as
``analysis/x2_adjusted_queue.py`` §Q2 runs it. The as-of date is the queue's own: the end of the latest GIS report
month (horizons Dec 2027 / Dec 2028 counted from there), not the mart run's ``as_of``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import polars as pl

from basecast_pipelines.marts import config as marts_config
from basecast_pipelines.marts.core import MartContext

WEIGHT = "capacity_mw"  # X2's primary variant is MW-weighted (entry_ia_sm|mw)
PROJECT_COLUMNS = (
    "inr", "project_name", "county_fips", "county_name", "weather_zone", "cdr_reporting_zone", "fuel_type", "stratum",
    "stage", "elapsed", "capacity_mw", "projected_cod", "curve",
)


@dataclass(frozen=True)
class AdjustedQueue:
    report_month: date  # latest GIS report month
    as_of: date  # end of that month
    horizons: dict[str, float]  # label ("2027") -> months from as_of
    scored: pl.DataFrame  # one row per active project, PROJECT_COLUMNS + p_/mw_/clamped_<label>
    events: pl.DataFrame  # every INR's events (survival.load_events), with stratum


def _next_month(d: date) -> date:
    return date(d.year + (d.month == 12), d.month % 12 + 1, 1)


def adjusted_queue(ctx: MartContext) -> AdjustedQueue:
    return ctx.cached("queue.adjusted", lambda: _build(ctx))


def _build(ctx: MartContext) -> AdjustedQueue:
    from basecast_pipelines.models import queue_adjusted as qa
    from basecast_pipelines.models import survival as sv

    model = marts_config.value(ctx.config, "queue.model")
    if model != "entry_ia_sm":
        raise NotImplementedError(f"queue.model {model!r}: only entry_ia_sm is built (R4 default)")
    years = marts_config.value(ctx.config, "queue.horizons")
    events = sv.load_events(cohort_start=None).with_columns(stratum=qa.stratum())
    cohort = events.filter(pl.col("first_seen_month") >= pl.lit(sv.COHORT_START))
    latest = events["latest_report_month"].max()
    as_of = _next_month(latest)
    horizons = {str(y): qa.months_between(date(y + 1, 1, 1), as_of) for y in years}
    geo = qa.load_geography()
    curves = qa.fit_stage_curves(qa.truncate_at(cohort, as_of), qa.fit_stages(model), weight=WEIGHT)
    queue = qa.build_queue(qa.load_snapshot(latest), events, as_of, qa.STAGE_SETS[model]).with_columns(
        county_key=qa.norm_county("county")
    ).join(geo.select("county_key", "county_fips", "county_name", "weather_zone"), on="county_key", how="left")
    scored = qa.score(queue, curves, horizons)
    cols = [*PROJECT_COLUMNS, *[f"{p}_{h}" for p in ("p", "mw", "clamped") for h in horizons]]
    return AdjustedQueue(latest, as_of, horizons, scored.select(cols), events)


def county_stratum(q: AdjustedQueue) -> pl.DataFrame:
    """County × stratum: ``projects``, ``raw_mw`` and ``adj_mw_<label>`` (the diagnosis' territory queue)."""
    return q.scored.group_by("county_fips", "stratum").agg(
        pl.len().alias("projects"),
        pl.col("capacity_mw").sum().alias("raw_mw"),
        *[pl.col(f"mw_{h}").sum().alias(f"adj_mw_{h}") for h in q.horizons],
    )
