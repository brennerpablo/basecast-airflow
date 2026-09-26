"""Adjusted generation queue: expected MW of the active queue that reaches COD by a date (exploration X2).

Builds on :mod:`basecast_pipelines.models.survival` (phase 0 Q6) without changing it: the outcome rules
(:func:`survival.outcomes`) and the Aalen–Johansen estimator (:func:`survival.fit_cif`) are imported as is.
What this module adds:

- **stages**: the latest milestone an active project has reached, in the order entry < FIS approved < IA
  signed < synchronized (a later stage wins even when its date is earlier: in ERCOT the IA often precedes FIS
  approval). Each stage has its own landmark curve per fuel stratum (solar, storage, wind, gas+other).
- **conditional probability**: a project that has spent ``e`` months at its stage without an event reaches
  COD within ``h`` more months with probability ``(F(e + h) - F(e)) / S(e)``, where ``F`` is the stage's CIF
  of COD and ``S`` the event-free survival (:func:`conditional_cod`). Past the curve's support (fewer than
  ``min_at_risk`` projects at risk) the clock is clamped so the ``h``-month window ends at the last supported
  time, i.e. older projects get the latest observed ``h``-month conditional rate.
- **semi-Markov entry stage** (:func:`pre_ia_frame`, :func:`composed_cod`): a project that has not signed
  its IA can only reach COD through the IA (or, rarely, straight from entry). The marginal entry curve
  ignores that and counts the COD of projects that already signed an IA, which the backtest shows
  overpredicts entry-stage MW 5-11x. The composed probability is: time to IA from entry (competing with COD
  and withdrawal, conditional on no exit by ``e``) convolved with the IA-stage CIF from 0 months.
- **as-of truncation** (:func:`truncate_at`): the fit only sees what was known on a past date (events after it
  are censored, milestones first reported after it are dropped), for the backtest.

Loaders (:func:`load_snapshot`, :func:`load_geography`) are the only functions that touch the database.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date

import numpy as np
import polars as pl

from basecast_pipelines.models import survival as sv

# stage -> milestone date column in gis_project_events / gis_snapshots, in order of progress
STAGE_DATES: dict[str, str] = {
    "entry": "entry_date",
    "fis_approved": "fis_approved",
    "ia_signed": "ia_signed",
    "synchronized": "approved_synchronization",
}
# first report month that shows the milestone (gis_project_events), for "known by the as-of date"
STAGE_FIRST_MONTH: dict[str, str] = {"ia_signed": "ia_first_month", "synchronized": "synchronization_first_month"}
STAGE_SETS: dict[str, tuple[str, ...]] = {
    "entry_ia": ("entry", "ia_signed"),  # the q6 proposal (FIS approved is not nested with the IA)
    "entry_fis_ia": ("entry", "fis_approved", "ia_signed"),
    "entry_fis_ia_sync": ("entry", "fis_approved", "ia_signed", "synchronized"),
    # "_sm": entry-stage projects scored semi-Markov through the IA (see fit_stages)
    "entry_ia_sm": ("entry", "ia_signed"),
    "entry_ia_sync_sm": ("entry", "ia_signed", "synchronized"),
}
# phase 0 Q6: "other" fails the size rule at FIS/IA and is folded into gas
STRATA: dict[str, str] = {"solar": "solar", "storage": "storage", "wind": "wind", "gas": "gas+other", "other": "gas+other"}
MIN_AT_RISK = 10  # q6: curves are cut where fewer than 10 projects remain at risk
POOLED = "all"
PRE_IA = "pre_ia"  # pseudo stage: the sojourn from entry to the IA (semi-Markov entry stage)
IA_EVENT = 3  # cause code of "signed the IA" in the pre-IA sojourn (after COD = 1 and WITHDRAWN = 2)


def fit_stages(stage_set: str) -> tuple[str, ...]:
    """The curves :func:`fit_stage_curves` needs for a ``STAGE_SETS`` entry (``_sm``: entry -> pre-IA)."""
    stages = STAGE_SETS[stage_set]
    if not stage_set.endswith("_sm"):
        return stages
    return tuple(PRE_IA if s == "entry" else s for s in stages)


def stratum(col: str = "fuel_group") -> pl.Expr:
    """Survival stratum of a fuel group: solar, storage, wind or gas+other."""
    return pl.col(col).replace_strict(STRATA, default="gas+other", return_dtype=pl.String)


def months(later: pl.Expr, earlier: pl.Expr) -> pl.Expr:
    """Months between two date expressions (days / 30.4375, as in the survival clock)."""
    return (later - earlier).dt.total_days() / sv.DAYS_PER_MONTH


def months_between(later: date, earlier: date) -> float:
    return (later - earlier).days / sv.DAYS_PER_MONTH


def norm_county(col: str = "county") -> pl.Expr:
    """County name key for the tx_counties join (upper case, letters only, "County" removed; q6 §7)."""
    return pl.col(col).str.to_uppercase().str.replace_all(r"\bCOUNTY\b", "").str.replace_all(r"[^A-Z]", "")


# --- conditional CIF ------------------------------------------------------------------------------------


def survival_at(fit: sv.CifFit, t: Sequence[float] | np.ndarray) -> np.ndarray:
    """Event-free survival (all causes) at ``t``: a right-continuous step function, 1 before the first event."""
    curve = np.concatenate([[1.0], fit.survival])
    return curve[np.searchsorted(fit.times, np.asarray(t, dtype=float), side="right")]


def support_end(fit: sv.CifFit, min_at_risk: float = MIN_AT_RISK) -> float:
    """Last event time with at least ``min_at_risk`` subjects at risk (0 when there is none)."""
    ok = fit.at_risk >= min_at_risk
    return float(fit.times[ok].max()) if ok.any() else 0.0


def conditional_cod(
    fit: sv.CifFit,
    elapsed: Sequence[float] | np.ndarray,
    horizon: float | Sequence[float] | np.ndarray,
    *,
    end: float | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """P(COD in (e, e + h] | no event by e) = (F(e + h) - F(e)) / S(e), for each elapsed time ``e``.

    ``end`` is the curve's supported end (see :func:`support_end`; ``None`` = no clamp). When ``e + h`` runs
    past it the clock is moved back to ``max(end - h, 0)``. Returns ``(probability, clamped)``; a zero
    survival gives probability 0.
    """
    e = np.asarray(elapsed, dtype=float)
    h = np.broadcast_to(np.asarray(horizon, dtype=float), e.shape)
    if (e < 0).any() or (h < 0).any():
        raise ValueError("elapsed and horizon must be >= 0")
    clamped = np.zeros(e.shape, dtype=bool) if end is None else (e + h > end)
    e_eff = e if end is None else np.where(clamped, np.maximum(end - h, 0.0), e)
    f0, f1 = fit.at(e_eff, sv.COD), fit.at(e_eff + h, sv.COD)
    s0 = survival_at(fit, e_eff)
    with np.errstate(divide="ignore", invalid="ignore"):
        p = np.where(s0 > 0, (f1 - f0) / s0, 0.0)
    return np.clip(p, 0.0, 1.0), clamped


def composed_cod(
    pre: sv.CifFit,
    ia: sv.CifFit,
    elapsed: Sequence[float] | np.ndarray,
    horizon: float,
    *,
    end: float | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """P(COD in (e, e + h] | still before the IA at e), semi-Markov through the IA.

    ``pre`` is the pre-IA sojourn fit (causes COD, WITHDRAWN, ``IA_EVENT``; clock from entry), ``ia`` the
    IA-stage fit (clock from the IA). The chance is the sum over IA times ``u`` in ``(e, e + h]`` of
    ``dF_IA(u) * F_ia(e + h - u)`` plus the direct COD increment, all over ``S_pre(e)``. The clock restarts at
    the IA (the months spent before it do not change the IA-stage curve). ``end`` clamps as in
    :func:`conditional_cod`.
    """
    e = np.asarray(elapsed, dtype=float)
    h = float(horizon)
    if (e < 0).any() or h < 0:
        raise ValueError("elapsed and horizon must be >= 0")
    clamped = np.zeros(e.shape, dtype=bool) if end is None else (e + h > end)
    e_eff = e if end is None else np.where(clamped, max(end - h, 0.0), e)
    s0 = survival_at(pre, e_eff)
    direct = pre.at(e_eff + h, sv.COD) - pre.at(e_eff, sv.COD)
    if IA_EVENT in pre.causes and pre.times.size:
        d_ia = np.diff(np.concatenate([[0.0], pre.cif[:, pre.causes.index(IA_EVENT)]]))
        t = pre.times[None, :]
        window = (t > e_eff[:, None]) & (t <= (e_eff + h)[:, None])
        rest = np.clip((e_eff + h)[:, None] - t, 0.0, None)
        g = ia.at(rest.ravel(), sv.COD).reshape(rest.shape)
        via_ia = (window * d_ia[None, :] * g).sum(axis=1)
    else:
        via_ia = np.zeros_like(e_eff)
    with np.errstate(divide="ignore", invalid="ignore"):
        p = np.where(s0 > 0, (direct + via_ia) / s0, 0.0)
    return np.clip(p, 0.0, 1.0), clamped


# --- as-of truncation and stage frames ------------------------------------------------------------------


def truncate_at(events: pl.DataFrame, as_of: date, *, vanished: str = "competing") -> pl.DataFrame:
    """:func:`survival.outcomes` as it could have been known on ``as_of``.

    Keeps projects first listed before ``as_of``; an event after ``as_of`` becomes censored at ``as_of``;
    a stage milestone dated on or after ``as_of``, or first reported after the month before ``as_of``, is
    set to null (the date columns are overwritten, so :func:`stage_frame` sees the truncated history).
    """
    report_month = date(as_of.year, as_of.month, 1)
    last_report = pl.lit(report_month).dt.offset_by("-1mo") if as_of.day == 1 else pl.lit(report_month)
    out = sv.outcomes(events, vanished=vanished).filter(pl.col("first_seen_month") < pl.lit(as_of))
    late = pl.col("event_date") > pl.lit(as_of)
    out = out.with_columns(
        event=pl.when(late).then(pl.lit(sv.CENSORED, dtype=pl.Int8)).otherwise("event"),
        event_date=pl.when(late).then(pl.lit(as_of)).otherwise("event_date"),
    )
    fixes = []
    for stage, col in STAGE_DATES.items():
        if stage == "entry" or col not in out.columns:
            continue
        known = pl.col(col) < pl.lit(as_of)
        if (first := STAGE_FIRST_MONTH.get(stage)) and first in out.columns:
            known &= pl.col(first).is_not_null() & (pl.col(first) <= last_report)
        fixes.append(pl.when(known).then(pl.col(col)).otherwise(None).alias(col))
    return out.with_columns(fixes)


def stage_frame(outcomes: pl.DataFrame, stage: str) -> pl.DataFrame:
    """Landmark frame at ``stage`` from an outcomes frame (same rules as :func:`survival.landmark_frame`).

    Needed because ``survival.landmark_frame`` recomputes outcomes from ``exit_status`` (so it cannot take a
    truncated history) and only knows the three q6 landmarks.
    """
    col = STAGE_DATES[stage]
    frame = (
        outcomes.filter(pl.col(col).is_not_null())
        .with_columns(origin=pl.col(col))
        .with_columns(
            entry=pl.max_horizontal(months(pl.col("first_seen_month"), pl.col("origin")), pl.lit(0.0)),
            raw_time=months(pl.col("event_date"), pl.col("origin")),
        )
    )
    return frame.with_columns(
        clipped=pl.col("raw_time") <= pl.col("entry"),
        time=pl.max_horizontal("raw_time", pl.col("entry") + sv.ONE_DAY),
    ).drop("raw_time")


def pre_ia_frame(outcomes: pl.DataFrame) -> pl.DataFrame:
    """Sojourn before the IA, clock from ``entry_date``: event ``IA_EVENT`` at the IA, else the outcome.

    A project whose IA falls on or before its delayed entry (signed before it was first listed, or before its
    screening start) was never observed without an IA and is left out; other events at or before the delayed
    entry are moved one day after it, as in :func:`survival.landmark_frame`.
    """
    ia = pl.col("ia_signed")
    has_ia = ia.is_not_null() & (ia <= pl.col("event_date"))
    frame = outcomes.with_columns(
        origin=pl.col("entry_date"),
        event=pl.when(has_ia).then(pl.lit(IA_EVENT)).otherwise(pl.col("event")).cast(pl.Int8),
        event_date=pl.when(has_ia).then(ia).otherwise(pl.col("event_date")),
    ).with_columns(
        entry=pl.max_horizontal(months(pl.col("first_seen_month"), pl.col("origin")), pl.lit(0.0)),
        raw_time=months(pl.col("event_date"), pl.col("origin")),
    )
    frame = frame.filter(~((pl.col("event") == IA_EVENT) & (pl.col("raw_time") <= pl.col("entry"))))
    return frame.with_columns(
        clipped=pl.col("raw_time") <= pl.col("entry"),
        time=pl.max_horizontal("raw_time", pl.col("entry") + sv.ONE_DAY),
    ).drop("raw_time")


def current_stage(stages: Sequence[str], as_of: date | None = None) -> pl.Expr:
    """The latest stage in ``stages`` whose milestone is set (and before ``as_of``, when given)."""
    expr = pl.lit(stages[0])
    for stage in stages[1:]:
        cond = pl.col(STAGE_DATES[stage]).is_not_null()
        if as_of is not None:
            cond &= pl.col(STAGE_DATES[stage]) < pl.lit(as_of)
        expr = pl.when(cond).then(pl.lit(stage)).otherwise(expr)
    return expr


def stage_date(stages: Sequence[str]) -> pl.Expr:
    """Date of the stage chosen by :func:`current_stage` (evaluate after adding ``stage``)."""
    expr = pl.col(STAGE_DATES[stages[0]])
    for stage in stages[1:]:
        expr = pl.when(pl.col("stage") == stage).then(pl.col(STAGE_DATES[stage])).otherwise(expr)
    return expr


# --- curves and scoring ---------------------------------------------------------------------------------


@dataclass(frozen=True)
class StageCurve:
    """One stage × stratum curve used to score projects."""

    stage: str
    stratum: str  # the stratum it was fit on (``POOLED`` when the stratum fell back)
    fit: sv.CifFit  # MW-weighted or count fit, the one the probabilities come from
    end: float  # supported end, from the count fit (at risk >= MIN_AT_RISK)
    n: int
    cod: int  # events of interest (IA signed for PRE_IA)


def fit_stage_curves(
    outcomes: pl.DataFrame,
    stages: Sequence[str],
    *,
    weight: str | None = "capacity_mw",
    min_n: int = sv.MIN_N,
    min_events: int = sv.MIN_EVENTS,
    min_at_risk: float = MIN_AT_RISK,
) -> dict[tuple[str, str], StageCurve]:
    """Curves keyed by ``(stage, stratum)`` plus ``(stage, POOLED)``.

    ``outcomes`` needs ``stratum`` (see :func:`stratum`). A stratum with fewer than ``min_n`` projects or
    ``min_events`` COD events at a stage is not fit; :func:`score` then uses the pooled curve. The pseudo
    stage ``PRE_IA`` fits the pre-IA sojourn (:func:`pre_ia_frame`; its size rule counts IA events).
    """
    curves: dict[tuple[str, str], StageCurve] = {}
    for stage in stages:
        frame = pre_ia_frame(outcomes) if stage == PRE_IA else stage_frame(outcomes, stage)
        event_of_interest = IA_EVENT if stage == PRE_IA else sv.COD
        if weight is not None:
            frame = frame.filter(pl.col(weight).is_not_null() & (pl.col(weight) > 0))
        groups = [(POOLED, frame), *[(k[0], g) for k, g in frame.group_by("stratum")]]
        for name, g in groups:
            cod = int((g["event"] == event_of_interest).sum())
            if g.height < min_n or cod < min_events:
                continue
            args = (g["time"].to_numpy(), g["event"].to_numpy(), g["entry"].to_numpy())
            count_fit = sv.fit_cif(*args)
            fit = count_fit if weight is None else sv.fit_cif(*args, g[weight].to_numpy())
            curves[(stage, name)] = StageCurve(stage, name, fit, support_end(count_fit, min_at_risk), g.height, cod)
    return curves


def score(
    queue: pl.DataFrame,
    curves: Mapping[tuple[str, str], StageCurve],
    horizons: Mapping[str, float],
) -> pl.DataFrame:
    """Adds ``p_<label>`` (conditional P(COD within the horizon)), ``mw_<label>`` and ``clamped_<label>``.

    ``queue`` needs ``stage``, ``stratum``, ``elapsed`` (months at the stage on the as-of date) and
    ``capacity_mw``. Adds ``curve`` (the stratum actually used: its own or ``POOLED``). When ``curves`` has
    ``PRE_IA`` curves, entry-stage projects are scored semi-Markov (:func:`composed_cod` with the
    ``ia_signed`` curve of the same stratum, or the pooled one).
    """
    semi_markov = any(k[0] == PRE_IA for k in curves)
    def own(stage: str, name: str) -> bool:
        key = PRE_IA if semi_markov and stage == "entry" else stage
        return (key, name) in curves

    used = [s if own(st, s) else POOLED for st, s in zip(queue["stage"], queue["stratum"], strict=True)]
    out = queue.with_columns(curve=pl.Series(used, dtype=pl.String))
    elapsed = out["elapsed"].to_numpy().clip(0)
    cols: dict[str, np.ndarray] = {}
    for label, h in horizons.items():
        p = np.full(out.height, np.nan)
        clamped = np.zeros(out.height, dtype=bool)
        for (stage, name), idx in _index(out).items():
            if semi_markov and stage == "entry":
                pre = curves.get((PRE_IA, name)) or curves[(PRE_IA, POOLED)]
                ia = curves.get(("ia_signed", name)) or curves[("ia_signed", POOLED)]
                p[idx], clamped[idx] = composed_cod(pre.fit, ia.fit, elapsed[idx], h, end=pre.end)
                continue
            c = curves.get((stage, name))
            if c is None:
                continue
            p[idx], clamped[idx] = conditional_cod(c.fit, elapsed[idx], h, end=c.end)
        cols[f"p_{label}"], cols[f"clamped_{label}"] = p, clamped
    out = out.with_columns(**{k: pl.Series(v) for k, v in cols.items()})
    return out.with_columns(**{f"mw_{label}": pl.col("capacity_mw") * pl.col(f"p_{label}") for label in horizons})


def _index(frame: pl.DataFrame) -> dict[tuple[str, str], np.ndarray]:
    keys = frame.select("stage", "curve").with_row_index("_i")
    return {(k[0], k[1]): g["_i"].to_numpy() for k, g in keys.group_by("stage", "curve")}


def aggregate(scored: pl.DataFrame, by: str | Sequence[str], horizons: Sequence[str]) -> pl.DataFrame:
    """Raw vs adjusted MW per group: ``projects``, ``raw_mw``, ``adj_mw_<label>``, ``ratio_<label>``."""
    keys = [by] if isinstance(by, str) else list(by)
    return (
        scored.group_by(keys)
        .agg(
            projects=pl.len(),
            raw_mw=pl.col("capacity_mw").sum(),
            **{f"adj_mw_{h}": pl.col(f"mw_{h}").sum() for h in horizons},
        )
        .with_columns(**{f"ratio_{h}": pl.col(f"adj_mw_{h}") / pl.col("raw_mw") for h in horizons})
        .sort("raw_mw", descending=True)
    )


def build_queue(
    snapshot: pl.DataFrame,
    events: pl.DataFrame,
    as_of: date,
    stages: Sequence[str],
) -> pl.DataFrame:
    """The active queue of one GIS report month, ready for :func:`score`.

    ``snapshot`` is ``gis_snapshots`` for that month (status active; capacity, county and milestones as
    printed then); ``events`` gives ``first_seen_month``, the fuel group, ``exit_status`` and ``cod_date``
    (the realized COD, for the backtest). Entry is the
    snapshot's screening start, else the first listed month. Milestones dated on or after ``as_of`` are
    ignored. ``elapsed`` = months from the stage date to ``as_of``.
    """
    ev = events.select(
        "inr", "first_seen_month", "fuel_group", "exit_status",
        cod_date=pl.when(pl.col("exit_status") == "operational")
        .then(pl.coalesce("commercial_operation_date", "cod_first_month", "exit_month")),
    )
    q = snapshot.filter(pl.col("status") == "active").join(ev, on="inr", how="left")
    q = q.with_columns(
        fuel_group=pl.coalesce("fuel_group", sv.fuel_group("fuel_type")),
        entry_date=pl.min_horizontal(
            pl.coalesce("screening_study_started", "first_seen_month", "report_month"), pl.lit(as_of)
        ),
    ).with_columns(stratum=stratum())
    q = q.with_columns(stage=current_stage(stages, as_of))
    return q.with_columns(elapsed=months(pl.lit(as_of), stage_date(stages)).clip(lower_bound=0.0))


def actual_cod_mw(until: date) -> pl.Expr:
    """Expression on a :func:`build_queue` frame: the project's MW if it reached COD before ``until``, else 0.

    ``cod_date`` comes from ``gis_project_events`` (ERCOT's COD approval; null unless operational).
    """
    return pl.when(pl.col("cod_date") < pl.lit(until)).then(pl.col("capacity_mw")).otherwise(0.0)


# --- loaders --------------------------------------------------------------------------------------------

SNAPSHOT_COLUMNS = (
    "inr", "report_month", "status", "project_name", "county", "cdr_reporting_zone", "fuel_type",
    "capacity_mw", "projected_cod", "screening_study_started", "fis_approved", "ia_signed",
    "approved_synchronization", "commercial_operation_date",
)


def load_snapshot(report_month: date) -> pl.DataFrame:
    """``gis_snapshots`` rows of one report month."""
    from basecast_pipelines.models.db import read_sql

    return read_sql(
        f"SELECT {', '.join(SNAPSHOT_COLUMNS)} FROM gis_snapshots WHERE report_month = %(m)s ORDER BY inr",
        {"m": report_month},
    )


def load_geography() -> pl.DataFrame:
    """County key -> FIPS, county name and ERCOT weather zone (``county_weather_zone``)."""
    from basecast_pipelines.models.db import read_sql

    df = read_sql(
        "SELECT c.county_fips, c.county_name, w.weather_zone, w.in_ercot "
        "FROM tx_counties c LEFT JOIN county_weather_zone w USING (county_fips)"
    )
    return df.with_columns(county_key=norm_county("county_name"))
