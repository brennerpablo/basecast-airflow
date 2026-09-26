"""Generation-queue survival: how much of the queue reaches commercial operation, and when (phase 0 Q6).

Competing risks over ``gis_project_events`` (one row per INR, built in ``parsers/ercot/gis.py``):

- **event of interest** (``COD``): ERCOT approval for commercial operation (``commercial_operation_date``;
  ``cod_first_month`` or ``exit_month`` for the one operational project inferred from a synchronization
  approval);
- **competing event** (``WITHDRAWN``): cancellation (``cancel_date``); projects that vanished from the
  report without a recorded reason (``dropped``, or last seen as ``inactive`` before the latest report)
  are competing events by default and censored in the sensitivity case (``vanished="censored"``);
- **censored**: projects still listed (active or inactive) in the latest report, at the end of that month.

The clock starts at a landmark (``entry`` = screening start or first listing, ``fis_approved``,
``ia_signed``); only projects that reached the landmark are included. A project is observed from its first
report month on, so ``entry`` in :func:`landmark_frame` is the delayed entry (left truncation) for projects
listed after their landmark date.

:func:`fit_cif` is the Aalen–Johansen estimator in plain numpy (it runs in production without lifelines;
``analysis/q6_survival.py`` cross-checks it against lifelines' ``AalenJohansenFitter``). The loader
(:func:`load_events`) is the only function that touches the database.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date

import numpy as np
import polars as pl

COHORT_START = date(2018, 8, 1)  # first report of the GINR layout; exits are recorded from here on
LANDMARKS: dict[str, str] = {"entry": "entry_date", "fis_approved": "fis_approved", "ia_signed": "ia_signed"}
HORIZONS: tuple[int, ...] = (12, 24, 36, 48)  # months
CENSORED, COD, WITHDRAWN = 0, 1, 2
DAYS_PER_MONTH = 30.4375
ONE_DAY = 1 / DAYS_PER_MONTH
MIN_N, MIN_EVENTS = 30, 10  # stratum size rule (docs/PHASE0_ANALYSIS.md, Q6 step 4)

# fuel_type (derived in the GIS parser) -> fuel group; anything else (oil, coal, nuclear, hydro, biomass,
# other, null) is "other"
FUEL_GROUPS: dict[str, str] = {"solar": "solar", "wind": "wind", "storage": "storage", "gas": "gas"}
# where a stratum that fails the size rule is folded (the handoff's example: gas + other)
MERGE_INTO: dict[str, str] = {"other": "gas", "gas": "other", "wind": "solar"}

EVENT_COLUMNS = (
    "inr", "project_name", "county", "cdr_reporting_zone", "fuel", "technology", "fuel_type", "capacity_mw",
    "size_category", "first_seen_month", "first_status", "last_seen_month", "last_status", "months_seen",
    "months_missing", "latest_report_month", "entry_date", "screening_study_started", "fis_requested",
    "fis_approved", "ia_signed", "approved_energization", "approved_synchronization",
    "commercial_operation_date", "cancel_date", "inactive_date", "cod_first_month", "cancelled_month",
    "inactive_first_month", "last_active_month", "exit_status", "exit_month", "exit_inferred",
)


def fuel_group(col: str = "fuel_type") -> pl.Expr:
    """Fuel group of a project: solar, wind, storage, gas, or other."""
    return pl.col(col).replace_strict(FUEL_GROUPS, default="other", return_dtype=pl.String)


def load_events(cohort_start: date | None = COHORT_START) -> pl.DataFrame:
    """``gis_project_events`` for the cohort first seen on or after ``cohort_start``, plus ``fuel_group``."""
    from basecast_pipelines.models.db import read_sql

    query = f"SELECT {', '.join(EVENT_COLUMNS)} FROM gis_project_events"
    params = None
    if cohort_start is not None:
        query += " WHERE first_seen_month >= %(start)s"
        params = {"start": cohort_start}
    return read_sql(query + " ORDER BY inr", params).with_columns(fuel_group=fuel_group())


# --- outcomes ------------------------------------------------------------------------------------------


def outcomes(events: pl.DataFrame, *, vanished: str = "competing") -> pl.DataFrame:
    """Adds ``event`` (CENSORED / COD / WITHDRAWN), ``event_date`` and ``vanished`` to each project.

    ``vanished`` is ``"competing"`` (a project that left the report without a recorded reason withdrew
    when it disappeared) or ``"censored"`` (its fate is unknown after the last report that listed it).
    """
    if vanished not in ("competing", "censored"):
        raise ValueError(f"vanished must be 'competing' or 'censored', not {vanished!r}")
    status = pl.col("exit_status")
    end_of_last_month = pl.col("last_seen_month").dt.offset_by("1mo")  # a report month covers that month
    gone = (status == "dropped") | (
        (status == "inactive") & (pl.col("last_seen_month") < pl.col("latest_report_month"))
    )
    gone_event = WITHDRAWN if vanished == "competing" else CENSORED
    return events.with_columns(
        vanished=gone,
        event=pl.when(status == "operational").then(pl.lit(COD))
        .when(status == "cancelled").then(pl.lit(WITHDRAWN))
        .when(gone).then(pl.lit(gone_event))
        .otherwise(pl.lit(CENSORED))
        .cast(pl.Int8),
        event_date=pl.when(status == "operational")
        .then(pl.coalesce("commercial_operation_date", "cod_first_month", "exit_month"))
        # a cancellation relisted as active later counts from the last active month on
        .when(status == "cancelled")
        .then(pl.max_horizontal(pl.coalesce("cancel_date", "exit_month"), "last_active_month"))
        .otherwise(end_of_last_month),
    )


def _months(later: pl.Expr, earlier: pl.Expr) -> pl.Expr:
    return (later - earlier).dt.total_days() / DAYS_PER_MONTH


def landmark_frame(events: pl.DataFrame, landmark: str = "entry", *, vanished: str = "competing") -> pl.DataFrame:
    """One row per project that reached ``landmark``, with the clock started there.

    Columns added: ``origin`` (landmark date), ``entry`` (delayed entry in months: the first report month
    that lists the project, 0 when it was listed before the landmark), ``time`` (months to the event or
    censoring) and ``clipped`` (the event fell on or before the delayed entry and was moved one day after
    it; also true when the event date precedes the landmark date).
    """
    if landmark not in LANDMARKS:
        raise ValueError(f"unknown landmark {landmark!r}; expected one of {sorted(LANDMARKS)}")
    frame = (
        outcomes(events, vanished=vanished)
        .filter(pl.col(LANDMARKS[landmark]).is_not_null())
        .with_columns(origin=pl.col(LANDMARKS[landmark]))
        .with_columns(
            entry=pl.max_horizontal(_months(pl.col("first_seen_month"), pl.col("origin")), pl.lit(0.0)),
            raw_time=_months(pl.col("event_date"), pl.col("origin")),
        )
    )
    return frame.with_columns(
        clipped=pl.col("raw_time") <= pl.col("entry"),
        time=pl.max_horizontal("raw_time", pl.col("entry") + ONE_DAY),
    ).drop("raw_time")


# --- Aalen–Johansen ------------------------------------------------------------------------------------


@dataclass(frozen=True)
class CifFit:
    """Aalen–Johansen fit: one row per distinct event time (any cause)."""

    times: np.ndarray  # distinct event times, ascending
    at_risk: np.ndarray  # (weighted) number at risk just before each time
    events: np.ndarray  # (len(times), len(causes)): (weighted) events of each cause at each time
    survival: np.ndarray  # event-free survival (all causes) just after each time
    cif: np.ndarray  # (len(times), len(causes)): cumulative incidence of each cause just after each time
    causes: tuple[int, ...]
    n: int  # subjects

    def at(self, horizons: Sequence[float], cause: int = COD) -> np.ndarray:
        """Cumulative incidence of ``cause`` at each horizon (a right-continuous step function)."""
        h = np.asarray(horizons, dtype=float)
        if cause not in self.causes:
            return np.zeros_like(h)
        curve = np.concatenate([[0.0], self.cif[:, self.causes.index(cause)]])
        return curve[np.searchsorted(self.times, h, side="right")]

    def n_at_risk(self, horizons: Sequence[float]) -> np.ndarray:
        """Number at risk just before the first event time at or after each horizon (0 past the last one)."""
        idx = np.searchsorted(self.times, np.asarray(horizons, dtype=float), side="left")
        padded = np.concatenate([self.at_risk, [0.0]])
        return padded[idx]


def fit_cif(
    time: Sequence[float],
    event: Sequence[int],
    entry: Sequence[float] | None = None,
    weights: Sequence[float] | None = None,
) -> CifFit:
    """Aalen–Johansen cumulative incidence for every cause in ``event`` (0 = censored).

    A subject is at risk on ``(entry, time]``: ``entry`` handles delayed entry (left truncation) and
    defaults to 0. Tied event times are handled exactly (all events at a time share the same risk set;
    censorings at that time are still at risk). ``weights`` (e.g. MW) turn counts into weighted counts.
    """
    t = np.asarray(time, dtype=float)
    e = np.asarray(event, dtype=int)
    s = np.zeros_like(t) if entry is None else np.asarray(entry, dtype=float)
    w = np.ones_like(t) if weights is None else np.asarray(weights, dtype=float)
    if not (t.shape == e.shape == s.shape == w.shape) or t.ndim != 1:
        raise ValueError("time, event, entry and weights must be 1-d arrays of the same length")
    if np.isnan(t).any() or np.isnan(s).any() or np.isnan(w).any():
        raise ValueError("time, entry and weights must not contain NaN")
    if (e < 0).any():
        raise ValueError("event codes must be >= 0 (0 = censored)")
    if (t <= s).any():
        raise ValueError("every subject needs time > entry")
    if (w <= 0).any():
        raise ValueError("weights must be > 0")

    causes = tuple(int(c) for c in np.unique(e[e > 0]))
    times = np.unique(t[e > 0])
    if times.size == 0:
        empty = np.zeros((0, len(causes)))
        return CifFit(times, np.zeros(0), empty, np.zeros(0), empty, causes, int(t.size))

    # at risk just before t_j: entered strictly before t_j minus left strictly before t_j
    entry_order, time_order = np.argsort(s, kind="stable"), np.argsort(t, kind="stable")
    cum_in = np.concatenate([[0.0], np.cumsum(w[entry_order])])
    cum_out = np.concatenate([[0.0], np.cumsum(w[time_order])])
    at_risk = (
        cum_in[np.searchsorted(s[entry_order], times, side="left")]
        - cum_out[np.searchsorted(t[time_order], times, side="left")]
    )

    slot = np.searchsorted(times, t)
    events = np.column_stack([
        np.bincount(slot[e == c], weights=w[e == c], minlength=times.size) for c in causes
    ])
    hazard = events / at_risk[:, None]
    survival = np.cumprod(1.0 - hazard.sum(axis=1))
    before = np.concatenate([[1.0], survival[:-1]])
    cif = np.cumsum(before[:, None] * hazard, axis=0)
    return CifFit(times, at_risk, events, survival, cif, causes, int(t.size))


# --- strata --------------------------------------------------------------------------------------------


def cif_table(
    frame: pl.DataFrame,
    by: str | None = "fuel_group",
    horizons: Sequence[float] = HORIZONS,
    *,
    weight: str | None = None,
) -> pl.DataFrame:
    """CIF of COD (and of withdrawal) at ``horizons`` per stratum of a :func:`landmark_frame`.

    Columns: stratum, n, cod (events), withdrawn (events), censored, ``cif_<h>`` and ``wd_<h>`` per horizon,
    ``at_risk_<h>``. ``weight`` (e.g. ``capacity_mw``) gives a weighted CIF; rows without it are dropped.
    """
    rows = []
    groups = [("all", frame)] if by is None else [(k[0], g) for k, g in frame.group_by(by, maintain_order=True)]
    for stratum, g in sorted(groups, key=lambda kv: str(kv[0])):
        if weight is not None:
            g = g.filter(pl.col(weight).is_not_null() & (pl.col(weight) > 0))
        if g.is_empty():
            continue
        fit = fit_cif(
            g["time"].to_numpy(), g["event"].to_numpy(), g["entry"].to_numpy(),
            None if weight is None else g[weight].to_numpy(),
        )
        cif, wd, risk = fit.at(horizons, COD), fit.at(horizons, WITHDRAWN), fit.n_at_risk(horizons)
        row = {
            "stratum": stratum, "n": g.height,
            "cod": int((g["event"] == COD).sum()),
            "withdrawn": int((g["event"] == WITHDRAWN).sum()),
            "censored": int((g["event"] == CENSORED).sum()),
        }
        row |= {f"cif_{h}": float(v) for h, v in zip(horizons, cif, strict=True)}
        row |= {f"wd_{h}": float(v) for h, v in zip(horizons, wd, strict=True)}
        row |= {f"at_risk_{h}": float(v) for h, v in zip(horizons, risk, strict=True)}
        rows.append(row)
    return pl.DataFrame(rows)


def merge_small_strata(
    counts: Mapping[str, tuple[int, int]],
    *,
    min_n: int = MIN_N,
    min_events: int = MIN_EVENTS,
    into: Mapping[str, str] = MERGE_INTO,
) -> dict[str, str]:
    """Map each stratum to the (possibly merged) stratum it is reported in.

    ``counts`` is stratum -> (n, events of interest). A stratum with ``n < min_n`` or fewer than
    ``min_events`` events is folded into ``into[stratum]`` (the merged name joins both with ``+``, sorted),
    smallest first, until every stratum passes or nothing is left to merge.
    """
    members: dict[str, set[str]] = {k: {k} for k in counts}
    totals = {k: tuple(v) for k, v in counts.items()}

    def failing() -> list[str]:
        bad = [k for k, (n, ev) in totals.items() if n < min_n or ev < min_events]
        return sorted(bad, key=lambda k: totals[k])

    while bad := [k for k in failing() if _target(k, members, into) is not None]:
        src = bad[0]
        dst = _target(src, members, into)
        merged = "+".join(sorted(members[src] | members[dst]))
        members[merged] = members.pop(src) | members.pop(dst)
        n1, e1 = totals.pop(src)
        n2, e2 = totals.pop(dst)
        totals[merged] = (n1 + n2, e1 + e2)
    return {m: name for name, ms in members.items() for m in ms}


def _target(stratum: str, members: Mapping[str, set[str]], into: Mapping[str, str]) -> str | None:
    """The current stratum that holds the merge partner of any member of ``stratum``."""
    for m in sorted(members[stratum]):
        partner = into.get(m)
        if partner is None:
            continue
        for name, ms in members.items():
            if partner in ms and name != stratum:
                return name
    return None
