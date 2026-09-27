"""Forecast marts (A-M5, P1): the weather-normalized load, the queue's stage curves and the dated annotations.

- ``mart_load_normalized_monthly`` / ``mart_load_normalized_annual``: ``analysis/x12_weather_normalized_load.py``
  cells 1, 3, 6, 7 and 8, in the grain of X12 §5. The variant of each target (daily mean, daily peak) is X12's
  choice on a 2023 validation year; every year is normalized with its rolling three-year window and 2003–2022 as
  normal weather. The hourly load and weather and the daily-peak variant and summer-peak quantiles are read through
  ``marts/explorer.py`` (same cache keys, so a run that builds both computes them once). ``yoy_norm_pct`` is the
  calendar-year change for full years and, for the running year, X12's same-days comparison with the year before
  (``yoy_basis``); ``yoy_norm_z`` is X12 §8's z-score against the 2010–2021 normalized YoY.
- ``mart_queue_stage_curves``: X2's curve set of today (``queue.model`` ``entry_ia_sm``, the fits
  ``marts/queue.py`` scores the queue with: MW-weighted, and X2's count-weighted variant), on a monthly grid.
  ``ia``: the IA-stage Aalen–Johansen CIF of COD and withdrawal, the same curves as Q6's IA landmark. ``entry``: the
  semi-Markov entry stage from queue entry (month 0), i.e. the pre-IA sojourn convolved with the IA-stage curve
  (``queue_adjusted.composed_cod``; withdrawal composed the same way), with ``cif_ia`` (the entry → IA step itself).
  This is not Q6's marginal entry curve, which X2 dropped. ``supported`` cuts each curve where fewer than 10
  projects remain at risk (Q6 review item 5), in every fit the point uses.
- ``mart_annotations``: dated events from ``config/annotations.yaml``, each with the source URL written in the repo
  doc named in ``doc_ref`` (``tests/marts/test_load.py`` checks the URL is there); an event without one keeps
  ``source_url`` null and ``verified = false``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import yaml

from basecast_pipelines.config import PROJECT_ROOT
from basecast_pipelines.marts import config as marts_config
from basecast_pipelines.marts import explorer
from basecast_pipelines.marts import queue as marts_queue
from basecast_pipelines.marts.core import Mart, MartContext, value_check
from basecast_pipelines.models.weather_load import SUMMER_MONTHS, TOTAL, WEATHER_ZONES

if TYPE_CHECKING:
    import numpy as np

    from basecast_pipelines.models.survival import CifFit

GOLDEN_AS_OF = date(2026, 9, 26)  # X12 (refreshed after the X16 Uri fix), Q6 and X2's run date

# --- X12: the weather-normalized load -----------------------------------------------------------------------

SERIES = (*WEATHER_ZONES, TOTAL)
TARGETS = ("dmean", "dmax")
YOY_REF = (2010, 2021)  # X12 §8: z-scores against the normalized YoY of 2010-2021
BIAS_YEARS = (2003, 2022)  # X12 §2 bias check: normalized P50 vs the actual summer peak over the normal period
X12_MODEL = "x12-v1"
WEATHER_SOURCE = "ERA5 (Open-Meteo)"
CALENDAR, YTD = "calendar_year", "same_days_ytd"

MONTHLY_COLUMNS = (
    "weather_zone", "month", "days", "complete", "avg_mw", "avg_norm_mw", "energy_gwh", "energy_norm_gwh", "peak_mw",
    "peak_norm_mw", "t_mean_c", "yoy_norm_pct", "yoy_pct",
)
ANNUAL_COLUMNS = (
    "weather_zone", "year", "months", "days", "complete", "complete_through", "energy_gwh", "energy_norm_gwh",
    "yoy_norm_pct", "yoy_pct", "yoy_basis", "yoy_norm_z", "summer_peak_mw", "summer_peak_norm_p10",
    "summer_peak_norm_p50", "summer_peak_norm_p90", "summer_weather_years",
)


def choose_variant(panel: pl.DataFrame, target: str) -> str:
    """X12 cell 1 for one target: the candidate spec with the lowest mean re-leveled MAPE of the nine series,
    trained 2020–2022 and validated on 2023 (ties by name, as ``explorer.normalized_summer_peak`` breaks them)."""
    from basecast_pipelines.models import weather_normalized as wn

    mape = []
    for name, spec in explorer._x12_variants().items():
        errs = [wn.holdout(panel, z, target, spec, train=explorer.X12_TRAIN, test=explorer.X12_TEST) for z in SERIES]
        mape.append((float(wn.holdout_mape(pl.concat(errs))["mape_relevel"].mean()), name))
    return min(mape)[1]


def x12_panel(ctx: MartContext) -> tuple[pl.DataFrame, pl.DataFrame]:
    """X12's daily panel and weather features, from the hourly frames ``explorer`` reads once per run."""
    from basecast_pipelines.models import weather_normalized as wn

    return ctx.cached("x12.panel", lambda: wn.build_panel(*explorer._hourly(ctx)))


def x12_variant(ctx: MartContext, target: str) -> str:
    """The chosen spec's name for ``target``; the daily peak's is the one ``explorer`` chose for the summer peak."""
    if target == explorer.X12_TARGET:
        return explorer.normalized_summer_peak(ctx)[1]
    return ctx.cached(f"x12.variant.{target}", lambda: choose_variant(x12_panel(ctx)[0], target))


def x12_days(ctx: MartContext, target: str) -> pl.DataFrame:
    """X12 cell 3 for one target: every zone-day of the nine series, normalized with its rolling window."""

    def build() -> pl.DataFrame:
        from basecast_pipelines.models import weather_normalized as wn

        panel, feats = x12_panel(ctx)
        normals = wn.normal_weather(feats, wn.NORMAL_YEARS)
        spec = explorer._x12_variants()[x12_variant(ctx, target)]
        return pl.concat([wn.normalize_zone(panel, z, target, normals, spec)[0] for z in SERIES])

    return ctx.cached(f"x12.days.{target}", build)


def monthly_rows(energy_days: pl.DataFrame, peak_days: pl.DataFrame) -> pl.DataFrame:
    """X12 cell 6 (``weather_normalized.monthly``) in the contract's names, plus X12 cell 8's 12-month change of the
    normalized (``yoy_norm_pct``) and actual (``yoy_pct``) average MW. The change reads the same month a year
    before by date, so a missing month gives null, not a shifted comparison."""
    from basecast_pipelines.models import weather_normalized as wn

    m = wn.monthly(energy_days, peak_days)
    prev = m.select("weather_zone", pl.col("month").dt.offset_by("12mo").alias("month"),
                    pl.col("avg_norm_mw").alias("_prev_norm"), pl.col("avg_mw").alias("_prev"))
    return (
        m.join(prev, on=["weather_zone", "month"], how="left")
        .with_columns(
            pl.col("days").cast(pl.Int64),
            pl.col("t_mean").alias("t_mean_c"),
            (100 * (pl.col("avg_norm_mw") / pl.col("_prev_norm") - 1)).alias("yoy_norm_pct"),
            (100 * (pl.col("avg_mw") / pl.col("_prev") - 1)).alias("yoy_pct"),
        )
        .select(MONTHLY_COLUMNS)
        .sort("weather_zone", "month")
    )


def ytd_change(energy_days: pl.DataFrame, year: int, through: date) -> pl.DataFrame:
    """X12 §2 "2026 so far": per zone, energy actual and normalized from Jan 1 through ``through``'s day of the
    year, ``year`` vs ``year - 1`` (%): ``yoy_pct``, ``yoy_norm_pct``."""
    cut = through.timetuple().tm_yday
    d = energy_days.filter(pl.col("date").dt.year().is_in([year - 1, year]) & (pl.col("date").dt.ordinal_day() <= cut))
    agg = d.group_by("weather_zone", pl.col("date").dt.year().alias("year")).agg(
        (pl.col("dmean") * pl.col("n_hours")).sum().alias("e"),
        (pl.col("normalized") * pl.col("n_hours")).sum().alias("n"),
    )
    now, before = agg.filter(pl.col("year") == year), agg.filter(pl.col("year") == year - 1)
    return now.join(before, on="weather_zone", suffix="_prev").select(
        "weather_zone",
        pl.lit(year).cast(pl.Int32).alias("year"),
        (100 * (pl.col("e") / pl.col("e_prev") - 1)).alias("yoy_pct"),
        (100 * (pl.col("n") / pl.col("n_prev") - 1)).alias("yoy_norm_pct"),
    )


def annual_rows(month_rows: pl.DataFrame, energy_days: pl.DataFrame, peak_days: pl.DataFrame,
                peaks: pl.DataFrame, *, ref_years: tuple[int, int] = YOY_REF) -> pl.DataFrame:
    """Per zone and year: X12 cell 6's annual energy (``weather_normalized.annual``), the YoY of full years (12
    months, as X12's ``full``) and its z-score against ``ref_years`` (cell 8), the running year's same-days change
    (``yoy_basis = same_days_ytd``), the actual June–September daily max and the normalized summer-peak quantiles
    (cell 7; ``peaks`` = ``peak_quantiles`` rows). ``month_rows`` carries ``energy_gwh`` / ``energy_norm_gwh`` /
    ``days`` per month (:func:`monthly_rows`)."""
    from basecast_pipelines.models import weather_normalized as wn

    year = wn.annual(month_rows).with_columns(
        pl.col("year").cast(pl.Int32), pl.col("months").cast(pl.Int64), pl.col("days").cast(pl.Int64),
        (pl.col("months") == 12).alias("complete"),
    )
    full = year.filter(pl.col("complete"))
    prev = full.select("weather_zone", (pl.col("year") + 1).alias("year"),
                       pl.col("energy_gwh").alias("_prev"), pl.col("energy_norm_gwh").alias("_prev_norm"))
    year = year.join(prev, on=["weather_zone", "year"], how="left").with_columns(
        pl.when(pl.col("complete")).then(100 * (pl.col("energy_norm_gwh") / pl.col("_prev_norm") - 1))
        .alias("yoy_norm_pct"),
        pl.when(pl.col("complete")).then(100 * (pl.col("energy_gwh") / pl.col("_prev") - 1)).alias("yoy_pct"),
    )
    ref = year.filter(pl.col("year").is_between(*ref_years)).group_by("weather_zone").agg(
        pl.col("yoy_norm_pct").mean().alias("_mu"), pl.col("yoy_norm_pct").std().alias("_sd"))
    year = year.join(ref, on="weather_zone", how="left").with_columns(
        ((pl.col("yoy_norm_pct") - pl.col("_mu")) / pl.col("_sd")).alias("yoy_norm_z"),
        pl.when(pl.col("yoy_norm_pct").is_not_null()).then(pl.lit(CALENDAR)).alias("yoy_basis"),
    )
    through = energy_days.group_by("weather_zone", pl.col("date").dt.year().cast(pl.Int32).alias("year")).agg(
        pl.col("date").max().alias("complete_through"))
    year = year.join(through, on=["weather_zone", "year"], how="left")
    partial = year.filter(~pl.col("complete") & (pl.col("year") == pl.col("year").max()))
    if partial.height:
        y = int(partial["year"][0])
        ytd = ytd_change(energy_days, y, partial["complete_through"].max())
        year = year.join(ytd, on=["weather_zone", "year"], how="left", suffix="_ytd").with_columns(
            pl.coalesce("yoy_norm_pct", "yoy_norm_pct_ytd").alias("yoy_norm_pct"),
            pl.coalesce("yoy_pct", "yoy_pct_ytd").alias("yoy_pct"),
            pl.when(pl.col("yoy_norm_pct_ytd").is_not_null()).then(pl.lit(YTD)).otherwise(pl.col("yoy_basis"))
            .alias("yoy_basis"),
        )
    actual = peak_days.filter(pl.col("date").dt.month().is_in(SUMMER_MONTHS)).group_by(
        "weather_zone", pl.col("date").dt.year().cast(pl.Int32).alias("year")).agg(
        pl.col("dmax").max().alias("summer_peak_mw"))
    band = peaks.select(
        "weather_zone", pl.col("year").cast(pl.Int32), pl.col("p10").alias("summer_peak_norm_p10"),
        pl.col("p50").alias("summer_peak_norm_p50"), pl.col("p90").alias("summer_peak_norm_p90"),
        pl.col("weather_years").cast(pl.Int64).alias("summer_weather_years"))
    return (
        year.join(actual, on=["weather_zone", "year"], how="left")
        .join(band, on=["weather_zone", "year"], how="left")
        .select(ANNUAL_COLUMNS)
        .sort("weather_zone", "year")
    )


def yoy_reference(annual: pl.DataFrame, ref_years: tuple[int, int] = YOY_REF) -> dict[str, dict[str, float]]:
    """Per zone, the mean and SD of the normalized calendar-year YoY over ``ref_years`` (the z-score's base)."""
    ref = annual.filter(pl.col("year").is_between(*ref_years) & (pl.col("yoy_basis") == CALENDAR)).group_by(
        "weather_zone").agg(pl.col("yoy_norm_pct").mean().alias("mean_pct"),
                            pl.col("yoy_norm_pct").std().alias("sd_pct"))
    return {r["weather_zone"]: {"mean_pct": r["mean_pct"], "sd_pct": r["sd_pct"]}
            for r in ref.sort("weather_zone").iter_rows(named=True)}


def p50_bias(annual: pl.DataFrame, years: tuple[int, int] = BIAS_YEARS) -> dict[str, float]:
    """Per zone, the mean of P50 ÷ actual summer peak − 1 (%) over ``years`` (X12 §2 bias check)."""
    b = annual.filter(pl.col("year").is_between(*years)).group_by("weather_zone").agg(
        (100 * (pl.col("summer_peak_norm_p50") / pl.col("summer_peak_mw") - 1)).mean().alias("bias"))
    return dict(b.sort("weather_zone").iter_rows())


def load_monthly(ctx: MartContext) -> pl.DataFrame:
    return ctx.cached("load.monthly", lambda: monthly_rows(x12_days(ctx, "dmean"), x12_days(ctx, "dmax")))


def load_annual(ctx: MartContext) -> pl.DataFrame:
    def build() -> pl.DataFrame:
        peaks, _ = explorer.normalized_summer_peak(ctx)
        return annual_rows(load_monthly(ctx), x12_days(ctx, "dmean"), x12_days(ctx, "dmax"), peaks)

    return ctx.cached("load.annual", build)


def _x12_meta(frame: pl.DataFrame, ctx: MartContext) -> dict[str, Any]:
    from basecast_pipelines.models import weather_normalized as wn

    days = x12_days(ctx, "dmean")
    return {
        "normal_period": f"{wn.NORMAL_YEARS[0]}-{wn.NORMAL_YEARS[1]}",
        "weather_source": WEATHER_SOURCE,
        "model": X12_MODEL,
        "variants": {t: x12_variant(ctx, t) for t in TARGETS},
        "variant_choice": "lowest mean re-leveled MAPE of the nine series, trained 2020-2022, validated on 2023",
        "window_years": 3,
        "excluded_days": [d.isoformat() for d in wn.EXCLUDED_DAYS],
        "complete_through": days["date"].max(),
        "first_day": days["date"].min(),
        "summer_months": list(SUMMER_MONTHS),
        "peak_norm_mw": "weather-adjusted daily peak, not an expected peak",
    }


def _annual_meta(frame: pl.DataFrame, ctx: MartContext) -> dict[str, Any]:
    return _x12_meta(frame, ctx) | {
        "yoy_reference": {"years": f"{YOY_REF[0]}-{YOY_REF[1]}", "by_zone": yoy_reference(frame)},
        "yoy_basis": {CALENDAR: "calendar year vs the year before (full years only)",
                      YTD: "the running year from Jan 1 through complete_through vs the same days a year before"},
        "summer_peak_band": "P10 / P50 / P90 of the June-September maximum over the normal weather years, each day "
                            "keeping its residual; uncorrected",
        "p50_bias_pct": {"years": f"{BIAS_YEARS[0]}-{BIAS_YEARS[1]}", "by_zone": p50_bias(frame)},
    }


# --- X2: the queue's stage curves ---------------------------------------------------------------------------

STAGE_NAMES = {"pre_ia": "entry", "ia_signed": "ia"}  # queue_adjusted stage -> the contract's stage
STRATUM_NAMES = {"gas+other": "gas_other"}  # the contract's stratum codes (get-data schemas/queue.py)
WEIGHTINGS: dict[str, str | None] = {"mw": "capacity_mw", "count": None}  # X2's primary variant is MW-weighted
GRID_MONTHS = tuple(range(97))  # monthly, 0-96: the cohort (first listed since 2018-08) has at most ~97 months
CURVE_COLUMNS = (
    "as_of_month", "stage", "stratum", "weighting", "month", "at_risk", "cif_cod", "cif_withdrawn", "survival",
    "supported", "cif_ia", "curve", "n", "support_end_months",
)
CURVE_SCHEMA = {
    "stage": pl.Utf8, "stratum": pl.Utf8, "weighting": pl.Utf8, "month": pl.Int64, "at_risk": pl.Int64,
    "cif_cod": pl.Float64, "cif_withdrawn": pl.Float64, "survival": pl.Float64, "supported": pl.Boolean,
    "cif_ia": pl.Float64, "curve": pl.Utf8, "n": pl.Int64, "support_end_months": pl.Float64,
}


def composed_from_entry(pre: CifFit, ia: CifFit, months: Sequence[float], cause: int) -> np.ndarray:
    """P(``cause`` by each month | at queue entry), semi-Markov through the IA: ``pre``'s own CIF of ``cause`` plus
    the IA CIF increments convolved with the IA-stage CIF of ``cause``. ``queue_adjusted.composed_cod`` at elapsed 0
    for COD, written for any cause (withdrawal)."""
    import numpy as np

    from basecast_pipelines.models import queue_adjusted as qa

    t = np.asarray(months, dtype=float)
    direct = pre.at(t, cause)
    if qa.IA_EVENT not in pre.causes or not pre.times.size:
        return np.clip(direct, 0.0, 1.0)
    d_ia = np.diff(np.concatenate([[0.0], pre.cif[:, pre.causes.index(qa.IA_EVENT)]]))
    window = pre.times[None, :] <= t[:, None]
    rest = np.clip(t[:, None] - pre.times[None, :], 0.0, None)
    g = ia.at(rest.ravel(), cause).reshape(rest.shape)
    return np.clip(direct + (window * d_ia[None, :] * g).sum(axis=1), 0.0, 1.0)


def _cut(ok: np.ndarray) -> np.ndarray:
    """``supported`` as a cut: true up to the first point that fails, false from there on."""
    import numpy as np

    return np.logical_and.accumulate(ok)


def _count_fits(outcomes: pl.DataFrame, stage: str, weight: str | None) -> dict[str, CifFit]:
    """The project-count fits behind ``fit_stage_curves``'s frames (the same rows: weight > 0 when weighted), per
    stratum and pooled, for ``at_risk`` in projects."""
    from basecast_pipelines.models import queue_adjusted as qa
    from basecast_pipelines.models import survival as sv

    frame = qa.pre_ia_frame(outcomes) if stage == qa.PRE_IA else qa.stage_frame(outcomes, stage)
    if weight is not None:
        frame = frame.filter(pl.col(weight).is_not_null() & (pl.col(weight) > 0))
    groups = [(qa.POOLED, frame), *[(k[0], g) for k, g in frame.group_by("stratum")]]
    return {name: sv.fit_cif(g["time"].to_numpy(), g["event"].to_numpy(), g["entry"].to_numpy()) for name, g in groups}


def stage_curve_rows(outcomes: pl.DataFrame, stages: Sequence[str], *, months: Sequence[int] = GRID_MONTHS,
                     strata: Iterable[str] | None = None, **fit_kw: Any) -> pl.DataFrame:
    """X2's curves on ``months`` for both weightings: one row per stage × stratum (plus the pooled ``all``) ×
    weighting × month. ``outcomes`` is ``truncate_at``'s frame with ``stratum``; ``stages`` is ``fit_stages`` of a
    semi-Markov stage set (``pre_ia`` and ``ia_signed``). A stratum ``fit_stage_curves`` did not fit takes the pooled
    curve, as ``queue_adjusted.score`` does (``curve = pooled``). ``fit_kw`` goes to ``fit_stage_curves``."""
    import numpy as np

    from basecast_pipelines.models import queue_adjusted as qa
    from basecast_pipelines.models import survival as sv

    if set(stages) != {qa.PRE_IA, "ia_signed"}:
        raise NotImplementedError(f"stage curves are built for the semi-Markov entry + IA set, not {stages}")
    min_at_risk = fit_kw.get("min_at_risk", qa.MIN_AT_RISK)
    m = np.asarray(months, dtype=float)
    names = [qa.POOLED, *sorted(set(strata) if strata is not None else set(qa.STRATA.values()))]
    parts = []
    for weighting, weight in WEIGHTINGS.items():
        curves = qa.fit_stage_curves(outcomes, stages, weight=weight, **fit_kw)
        counts = {s: _count_fits(outcomes, s, weight) for s in stages}
        for (stage, name), c in curves.items():  # the count fits here must be the ones fit_stage_curves cut with
            if qa.support_end(counts[stage][name], min_at_risk) != c.end:
                raise AssertionError(f"count fit of {stage}/{name} differs from fit_stage_curves'")

        def pick(stage: str, name: str) -> tuple[qa.StageCurve, CifFit, bool]:
            own = (stage, name) in curves
            key = name if own else qa.POOLED
            return curves[(stage, key)], counts[stage][key], own

        for name in names:
            ia, ia_count, ia_own = pick("ia_signed", name)
            pre, pre_count, pre_own = pick(qa.PRE_IA, name)
            ia_risk = ia_count.n_at_risk(m)
            pre_risk = pre_count.n_at_risk(m)
            cod = np.array([qa.composed_cod(pre.fit, ia.fit, np.zeros(1), h)[0][0] for h in m])
            wd = composed_from_entry(pre.fit, ia.fit, m, sv.WITHDRAWN)
            blocks = {
                "ia": dict(at_risk=ia_risk, cif_cod=ia.fit.at(m, sv.COD), cif_withdrawn=ia.fit.at(m, sv.WITHDRAWN),
                           survival=qa.survival_at(ia.fit, m), cif_ia=np.full(m.shape, np.nan),
                           supported=_cut((m <= ia.end) & (ia_risk >= min_at_risk)),
                           curve="own" if ia_own else "pooled", n=ia.n, support_end_months=ia.end),
                # the entry value at month t reads the IA curve up to t: both fits must still be supported there
                "entry": dict(at_risk=pre_risk, cif_cod=cod, cif_withdrawn=wd, survival=1.0 - cod - wd,
                              cif_ia=pre.fit.at(m, qa.IA_EVENT),
                              supported=_cut((m <= pre.end) & (m <= ia.end) & (pre_risk >= min_at_risk)),
                              curve="own" if pre_own and ia_own else "pooled", n=pre.n,
                              support_end_months=min(pre.end, ia.end)),
            }
            for stage, b in blocks.items():
                parts.append(pl.DataFrame({
                    "stage": stage, "stratum": STRATUM_NAMES.get(name, name), "weighting": weighting,
                    "month": np.asarray(months, dtype=np.int64),
                    "at_risk": np.asarray(b["at_risk"]).round().astype(np.int64),
                    "cif_cod": b["cif_cod"], "cif_withdrawn": b["cif_withdrawn"], "survival": b["survival"],
                    "supported": b["supported"], "cif_ia": b["cif_ia"], "curve": b["curve"], "n": b["n"],
                    "support_end_months": float(b["support_end_months"]),
                }, schema=CURVE_SCHEMA).with_columns(pl.col("cif_ia").fill_nan(None)))
    return pl.concat(parts).sort("stage", "stratum", "weighting", "month")


def build_stage_curves(ctx: MartContext) -> pl.DataFrame:
    from basecast_pipelines.models import queue_adjusted as qa
    from basecast_pipelines.models import survival as sv

    model = marts_config.value(ctx.config, "queue.model")
    if model != "entry_ia_sm":
        raise NotImplementedError(f"queue.model {model!r}: only entry_ia_sm is built (R4 default)")
    q = marts_queue.adjusted_queue(ctx)  # the same events and as-of date the queue marts are scored with
    cohort = q.events.filter(pl.col("first_seen_month") >= pl.lit(sv.COHORT_START))
    rows = stage_curve_rows(qa.truncate_at(cohort, q.as_of), qa.fit_stages(model))
    return rows.select(pl.lit(q.report_month, dtype=pl.Date).alias("as_of_month"), pl.all()).select(CURVE_COLUMNS)


def _curves_meta(frame: pl.DataFrame, ctx: MartContext) -> dict[str, Any]:
    from basecast_pipelines.models import queue_adjusted as qa
    from basecast_pipelines.models import survival as sv

    q = marts_queue.adjusted_queue(ctx)
    return {
        "report_month": q.report_month.isoformat(),
        "queue_as_of": q.as_of.isoformat(),
        "model": marts_config.value(ctx.config, "queue.model"),
        "cohort_first_listed_from": sv.COHORT_START.isoformat(),
        "min_at_risk": qa.MIN_AT_RISK,
        "supported": f"at least {qa.MIN_AT_RISK} projects at risk and within the fit's support end (the last event "
                     f"time with {qa.MIN_AT_RISK} at risk), as a cut; entry also needs the IA curve's support, since "
                     "its value at month t reads the IA curve up to t. at_risk is in projects for both weightings",
        "strata_rule": f"a stratum with n < {sv.MIN_N} or < {sv.MIN_EVENTS} events at a stage takes the pooled curve",
        "stages": {
            "ia": "months since the IA was signed: Aalen-Johansen CIF of COD and of withdrawal (delayed entry at the "
                  "first listed month)",
            "entry": "months since queue entry, semi-Markov: time to the IA (competing with COD and withdrawal) "
                     "convolved with the IA-stage curve; cif_ia is the entry -> IA step",
        },
        "weightings": {"mw": "capacity_mw (X2's primary variant)", "count": "projects (X2's count variant)"},
        "event": "ERCOT commercial operation approval; competing event: cancellation, or vanishing from the report",
    }


# --- annotations --------------------------------------------------------------------------------------------

ANNOTATIONS_PATH = PROJECT_ROOT / "config" / "annotations.yaml"
ANNOTATION_KINDS = {"grid_event", "demand_record", "official_forecast", "large_load_policy"}
ANNOTATION_FIELDS = ("id", "date", "date_precision", "kind", "title", "detail", "source_url", "source_title",
                     "source_page", "doc_ref", "verified")
REQUIRED_FIELDS = {"id", "date", "kind", "title", "detail", "source_url", "doc_ref", "verified"}
ANNOTATION_SCHEMA = {
    "id": pl.Utf8, "date": pl.Date, "date_precision": pl.Utf8, "kind": pl.Utf8, "title": pl.Utf8, "detail": pl.Utf8,
    "source_url": pl.Utf8, "source_title": pl.Utf8, "source_page": pl.Int64, "doc_ref": pl.Utf8,
    "verified": pl.Boolean,
}


def load_annotations(path: Path = ANNOTATIONS_PATH) -> list[dict[str, Any]]:
    """``config/annotations.yaml``'s events, validated: known fields only, unique ids, a real date, a known kind,
    and ``verified`` true only with a ``source_url`` (null URL = not verified)."""
    items = (yaml.safe_load(path.read_text()) or {}).get("annotations") or []
    problems, seen, out = [], set(), []
    for i, item in enumerate(items):
        where = f"annotations[{i}] ({item.get('id', '?') if isinstance(item, Mapping) else '?'})"
        if not isinstance(item, Mapping):
            problems.append(f"{where}: expected a mapping")
            continue
        if missing := REQUIRED_FIELDS - set(item):
            problems.append(f"{where}: missing {sorted(missing)}")
        if unknown := set(item) - set(ANNOTATION_FIELDS):
            problems.append(f"{where}: unknown fields {sorted(unknown)}")
        if item.get("id") in seen:
            problems.append(f"{where}: duplicate id")
        seen.add(item.get("id"))
        if not isinstance(item.get("date"), date):
            problems.append(f"{where}: date must be YYYY-MM-DD")
        if item.get("kind") not in ANNOTATION_KINDS:
            problems.append(f"{where}: kind {item.get('kind')!r} not in {sorted(ANNOTATION_KINDS)}")
        if item.get("date_precision", "day") not in ("day", "month"):
            problems.append(f"{where}: date_precision must be day or month")
        if not isinstance(item.get("verified"), bool):
            problems.append(f"{where}: verified must be true or false")
        elif item["verified"] and not item.get("source_url"):
            problems.append(f"{where}: verified without a source_url")
        out.append({f: item.get(f) for f in ANNOTATION_FIELDS} | {"date_precision": item.get("date_precision", "day")})
    if problems:
        raise ValueError(f"{path.name}: " + "; ".join(problems))
    return out


def annotation_rows(items: Sequence[Mapping[str, Any]], as_of: date) -> pl.DataFrame:
    """The events dated on or before ``as_of``, oldest first."""
    rows = [dict(r) for r in items if r["date"] <= as_of]
    return pl.DataFrame(rows, schema=ANNOTATION_SCHEMA).select(ANNOTATION_FIELDS).sort("date", "id")


def build_annotations(ctx: MartContext) -> pl.DataFrame:
    return annotation_rows(load_annotations(), ctx.as_of)


def _annotations_meta(frame: pl.DataFrame, ctx: MartContext) -> dict[str, Any]:
    return {"source": "config/annotations.yaml",
            "rule": "source_url is a URL written in the repo doc named in doc_ref; null = not verified",
            "unverified": frame.filter(~pl.col("verified"))["id"].to_list()}


# --- checks --------------------------------------------------------------------------------------------------


def _golden(name, get, expected, **kw):
    return value_check(name, get, expected, as_of=GOLDEN_AS_OF, **kw)


def _zone(frame: pl.DataFrame, zone: str = TOTAL) -> pl.DataFrame:
    return frame.filter(pl.col("weather_zone") == zone)


def _year(frame: pl.DataFrame, year: int, col: str, zone: str = TOTAL):
    return _zone(frame, zone).filter(pl.col("year") == year)[col].item()


def _month(frame: pl.DataFrame, month: date, col: str, zone: str):
    return _zone(frame, zone).filter(pl.col("month") == month)[col].item()


def _years(frame: pl.DataFrame, first: int, last: int, zone: str = TOTAL) -> pl.DataFrame:
    return _zone(frame, zone).filter(pl.col("year").is_between(first, last)).sort("year")


def _p50_rose_every_year(frame: pl.DataFrame) -> bool:
    p = _years(frame, 2011, 2026)["summer_peak_norm_p50"]
    return p.len() == 16 and bool((p.diff().drop_nulls() > 0).all())


def _raw_falls(frame: pl.DataFrame) -> int:
    p = _years(frame, 2011, 2026)["summer_peak_mw"]
    return int((p.diff().drop_nulls() < 0).sum())


def _bias(frame: pl.DataFrame) -> float:
    return p50_bias(frame)[TOTAL]


def _ref(stat: str):
    return lambda f: yoy_reference(f)[TOTAL][stat]


def _running_before_last(frame: pl.DataFrame) -> int:
    """Incomplete months other than each zone's last (the running month)."""
    return frame.filter(~pl.col("complete") & (pl.col("month") < pl.col("month").max().over("weather_zone"))).height


def _series_complete(frame: pl.DataFrame) -> bool:
    return set(frame["weather_zone"].unique().to_list()) == set(SERIES)


def _key_unique(*cols: str):
    return lambda f: f.select(cols).is_duplicated().any()


MONTHLY_CHECKS = (
    value_check("unique weather_zone × month", _key_unique("weather_zone", "month"), False),
    value_check("the eight zones and ERCOT", _series_complete, True),
    value_check("every month has normalized energy", lambda f: f["energy_norm_gwh"].null_count(), 0),
    value_check("only a zone's last month may be running", _running_before_last, 0),
    _golden("NORTH Dec 2021 normalized YoY +24%", lambda f: _month(f, date(2021, 12, 1), "yoy_norm_pct", "NORTH"),
            24, tol=0.5),
    _golden("WEST Aug 2026 normalized YoY +39%", lambda f: _month(f, date(2026, 8, 1), "yoy_norm_pct", "WEST"),
            39, tol=0.5),
)

PCT1, Z1, GW = 0.05, 0.05, 50.0  # the doc's precision: one decimal of a %, of a z-score, of a GW
ANNUAL_CHECKS = (
    value_check("unique weather_zone × year", _key_unique("weather_zone", "year"), False),
    value_check("the eight zones and ERCOT", _series_complete, True),
    value_check("P10 <= P50 <= P90", lambda f: f.filter(
        (pl.col("summer_peak_norm_p10") > pl.col("summer_peak_norm_p50"))
        | (pl.col("summer_peak_norm_p50") > pl.col("summer_peak_norm_p90"))).height, 0),
    _golden("ERCOT 2010-21 normalized YoY mean 2.16%", _ref("mean_pct"), 2.16, tol=0.005),
    _golden("ERCOT 2010-21 normalized YoY SD 0.57 pp", _ref("sd_pct"), 0.57, tol=0.005),
    _golden("ERCOT 2021 normalized YoY +3.4%", lambda f: _year(f, 2021, "yoy_norm_pct"), 3.4, tol=PCT1),
    _golden("ERCOT 2022 normalized YoY +4.4%", lambda f: _year(f, 2022, "yoy_norm_pct"), 4.4, tol=PCT1),
    _golden("ERCOT 2022 z 3.9", lambda f: _year(f, 2022, "yoy_norm_z"), 3.9, tol=Z1),
    _golden("ERCOT 2022-25 z from 3.9", lambda f: _years(f, 2022, 2025)["yoy_norm_z"].min(), 3.9, tol=Z1),
    _golden("ERCOT 2022-25 z to 5.7", lambda f: _years(f, 2022, 2025)["yoy_norm_z"].max(), 5.7, tol=Z1),
    _golden("SCENT 2022 z 4.7", lambda f: _year(f, 2022, "yoy_norm_z", "SCENT"), 4.7, tol=Z1),
    _golden("NORTH 2022 z 12.2", lambda f: _year(f, 2022, "yoy_norm_z", "NORTH"), 12.2, tol=Z1),
    _golden("ERCOT 2026 so far (same days) +3.8% normalized", lambda f: _year(f, 2026, "yoy_norm_pct"), 3.8, tol=PCT1),
    _golden("normalized summer peak rose every year 2012-2026", _p50_rose_every_year, True),
    _golden("normalized summer peak 64.8 GW in 2011", lambda f: _year(f, 2011, "summer_peak_norm_p50"), 64_800, tol=GW),
    _golden("normalized summer peak 91.1 GW in 2026", lambda f: _year(f, 2026, "summer_peak_norm_p50"), 91_100, tol=GW),
    _golden("raw summer peak fell in 7 of the 15 years 2012-2026", _raw_falls, 7),
    _golden("2025 actual summer peak 83.7 GW", lambda f: _year(f, 2025, "summer_peak_mw"), 83_700, tol=GW),
    _golden("2025 at normal weather 88.0 GW (P50)", lambda f: _year(f, 2025, "summer_peak_norm_p50"), 88_000, tol=GW),
    _golden("P50 +0.85% over the actual peak, 2003-2022 (ERCOT)", _bias, 0.85, tol=0.005),
)


def _curve(frame: pl.DataFrame, stage: str, stratum: str, weighting: str, month: int, col: str):
    return frame.filter((pl.col("stage") == stage) & (pl.col("stratum") == stratum)
                        & (pl.col("weighting") == weighting) & (pl.col("month") == month))[col].item()


def _curves_ok(frame: pl.DataFrame, rule) -> bool:
    return all(rule(g.sort("month")) for _, g in frame.group_by("stage", "stratum", "weighting"))


def _sums_to_one(frame: pl.DataFrame) -> bool:
    gap = frame.select((pl.col("cif_cod") + pl.col("cif_withdrawn") + pl.col("survival") - 1).abs().max()).item()
    return float(gap) < 1e-9


def _is_cut(g: pl.DataFrame) -> bool:
    s = g["supported"].cast(pl.Int8)
    return bool((s.diff().drop_nulls() <= 0).all())


CIF = 0.0005  # Q6 gives CIFs as percentages with one decimal
CURVE_CHECKS = (
    value_check("unique curve point", _key_unique("as_of_month", "stage", "stratum", "weighting", "month"), False),
    value_check("stages entry | ia", lambda f: set(f["stage"].unique().to_list()) <= {"entry", "ia"}, True),
    value_check("contract strata", lambda f: set(f["stratum"].unique().to_list())
                <= {"all", "solar", "storage", "wind", "gas_other"}, True),
    value_check("probabilities in [0, 1]", lambda f: bool(f.select(pl.all_horizontal(
        pl.col("cif_cod", "cif_withdrawn", "survival").is_between(-1e-12, 1 + 1e-12)).all()).item()), True),
    value_check("COD + withdrawn + event-free = 1", _sums_to_one, True),
    value_check("CIF of COD never falls", lambda f: _curves_ok(f, lambda g: bool(
        (g["cif_cod"].diff().drop_nulls() >= -1e-12).all())), True),
    value_check("supported is a cut", lambda f: _curves_ok(f, _is_cut), True),
    _golden("IA signed, all: 44.0% reach COD within 36 months (Q6, count)",
            lambda f: _curve(f, "ia", "all", "count", 36, "cif_cod"), 0.440, tol=CIF),
    _golden("IA signed, all: 30.4% MW-weighted (Q6)", lambda f: _curve(f, "ia", "all", "mw", 36, "cif_cod"), 0.304,
            tol=CIF),
    _golden("IA signed, all: 46.6% by 48 months, MW-weighted (Q6)",
            lambda f: _curve(f, "ia", "all", "mw", 48, "cif_cod"), 0.466, tol=CIF),
    _golden("IA signed, storage: 59.1% (Q6, count)", lambda f: _curve(f, "ia", "storage", "count", 36, "cif_cod"),
            0.591, tol=CIF),
    _golden("IA signed, solar: 22.2% (Q6, count)", lambda f: _curve(f, "ia", "solar", "count", 36, "cif_cod"),
            0.222, tol=CIF),
    _golden("IA signed, all: 7.4% withdrawn by 36 months (Q6, count)",
            lambda f: _curve(f, "ia", "all", "count", 36, "cif_withdrawn"), 0.074, tol=CIF),
    _golden("IA signed, all: 226 at risk at 36 months (Q6)", lambda f: _curve(f, "ia", "all", "count", 36, "at_risk"),
            226),
    _golden("IA signed, gas_other: no supported CIF48 (Q6 †)",
            lambda f: _curve(f, "ia", "gas_other", "count", 48, "supported"), False),
)

ANNOTATION_CHECKS = (
    value_check("unique id", lambda f: f["id"].is_duplicated().any(), False),
    value_check("verified only with a source URL",
                lambda f: f.filter(pl.col("verified") & pl.col("source_url").is_null()).height, 0),
    _golden("the 2026-08-03 pause, with the findings URL", lambda f: f.filter(
        pl.col("date") == date(2026, 8, 3))["source_url"].to_list(),
        ["https://www.ercot.com/files/docs/2026/08/25/10.-Large-Load-Issues.zip"]),
)


# --- marts ---------------------------------------------------------------------------------------------------

X12_INPUTS = ("ercot_load_hourly_wz", "weather_hourly_wz")

LOAD_NORMALIZED_MONTHLY = Mart(
    name="mart_load_normalized_monthly",
    build=load_monthly,
    key=("weather_zone", "month"),
    inputs=X12_INPUTS,
    description="Load by weather zone and month, actual and at normal weather (ERA5 2003-2022): average, energy, "
    "daily peak and its 12-month change (X12).",
    caveats=("preliminary_actuals",),
    checks=MONTHLY_CHECKS,
    meta=_x12_meta,
)

LOAD_NORMALIZED_ANNUAL = Mart(
    name="mart_load_normalized_annual",
    build=load_annual,
    key=("weather_zone", "year"),
    inputs=X12_INPUTS,
    description="Load by weather zone and year: energy actual and at normal weather with its YoY and z-score, the "
    "actual summer peak and its P10/P50/P90 under the 20 normal weather years (X12).",
    caveats=("preliminary_actuals", "band_uncalibrated"),
    checks=ANNUAL_CHECKS,
    meta=_annual_meta,
)

QUEUE_STAGE_CURVES = Mart(
    name="mart_queue_stage_curves",
    build=build_stage_curves,
    key=("as_of_month", "stage", "stratum", "weighting", "month"),
    inputs=("gis_project_events",),
    description="Generation-queue survival by stage (queue entry, IA signed), stratum and weighting: the share "
    "reaching COD and withdrawing by month, cut where fewer than 10 projects remain at risk (X2's curves, Q6).",
    caveats=("beyond_backtested_window",),
    checks=CURVE_CHECKS,
    meta=_curves_meta,
)

ANNOTATIONS = Mart(
    name="mart_annotations",
    build=build_annotations,
    key=("id",),
    inputs=(),
    description="Dated events for the Forecast charts (records, official forecasts, large-load policy), each with "
    "its source URL; null = not verified.",
    caveats=("policy_pause_2026", "preliminary_actuals"),
    checks=ANNOTATION_CHECKS,
    meta=_annotations_meta,
)

MARTS = (LOAD_NORMALIZED_MONTHLY, LOAD_NORMALIZED_ANNUAL, QUEUE_STAGE_CURVES, ANNOTATIONS)
