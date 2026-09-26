"""Large-load queue series from the ERCOT decks: inventory, consistency checks and a realization ratio.

Phase 0, question Q5 (``docs/PHASE0_ANALYSIS.md``). The inputs are ``large_load_chart_values`` (values Gemini
read off chart images, ``verified=false``) and ``large_load_headlines`` (numbers matched by regex in the decks'
sentences). Everything below is a pure function on DataFrames except the thin ``load_*`` helpers.

Two series matter:

- **In-service series** (``series_kind="in_service_year"``): the stacked bar chart "Current Large Load
  Interconnection Queue" / "Actual and Projected Large Load Growth 20xx-20yy". Each bar is a *stock*: MW with a
  projected in-service date on or before the end of that year, split by status. The last bar is the whole
  queue, so its total is the deck's "MW tracked" and its approved-to-energize segments are the deck's approved
  stock.
- **Status series** (``series_kind="status_by_month"``): "ERCOT Approvals - Past 12 Months" / "Approved Large
  Load - Growth in the Past Year". A *stock* by month of MW approved to energize (and planning studies
  approved).

Realization ratio, for a vintage ``v`` (a deck) and a target year ``Y``:

- ``promised(v, Y)``: the vintage's in-service bar for ``Y`` (all statuses; the "firm" variant drops "No
  Studies Submitted").
- ``realized(Y)``: the approved-to-energize stock at the end of ``Y`` (status series), and the observed
  energized stock at the end of ``Y`` (in-service series of the first deck published after ``Y``).
- ``gross = realized / promised``; ``incremental = (realized - base_v) / (promised - base_v)``, where
  ``base_v`` is the vintage's own approved-to-energize stock (MW already approved when the deck was made).

Both sides are stocks, not a tracked cohort: the decks carry no project ids, so ``realized`` also counts loads
that entered the queue after the vintage. The ratio is a stock-to-stock comparison, not a cohort survival rate.
"""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path

import polars as pl

SOURCE_ID = "ercot_large_load_decks"
TOLERANCE = 0.05
PHASE0_SINCE = date(2023, 5, 1)

A2E_BUCKETS = ("approved_to_energize", "approved_to_energize_not_operational", "observed_energized")
PIPELINE_BUCKETS = ("planning_studies_approved", "under_ercot_review", "no_studies_submitted")
STATUS_BUCKETS = A2E_BUCKETS + PIPELINE_BUCKETS

_EXCLUDED_YEAR_CHARTS = re.compile(r"(?i)RPG|Planning Models|Excluding")
_APPROVALS = re.compile(r"(?i)approved large load|approvals")
_OBSERVATIONS = re.compile(r"(?i)observations")
_QUEUE = re.compile(r"(?i)queue|tracked|requests submitted")
_YEAR = r"^\d{4}$"
_MONTH = r"^\d{4}-\d{2}$"


# --------------------------------------------------------------------------------------------------- loaders


def load_chart_values() -> pl.DataFrame:
    from basecast_pipelines.models.db import read_sql

    return read_sql("select * from large_load_chart_values")


def load_headlines() -> pl.DataFrame:
    from basecast_pipelines.models.db import read_sql

    return read_sql("select * from large_load_headlines")


def load_status() -> pl.DataFrame:
    from basecast_pipelines.models.db import read_sql

    return read_sql("select * from large_load_status")


def load_manifests(lake_root: Path) -> pl.DataFrame:
    """One row per raw file of the deck source: ``source_file``, public ``url``, ``doc_type``, ``posted_date``."""
    rows = []
    for manifest in sorted((lake_root / "raw" / f"source={SOURCE_ID}").glob("dt=*/_manifest.json")):
        body = json.loads(manifest.read_text())
        for entry in body.get("entries", []):
            meta = entry.get("meta") or {}
            rows.append(
                {
                    "source_file": f"raw/source={SOURCE_ID}/dt={body['dt']}/{entry['file']}",
                    "url": entry.get("url"),
                    "doc_type": meta.get("type"),
                    "posted_date": meta.get("posted_date"),
                }
            )
    schema = {"source_file": pl.String, "url": pl.String, "doc_type": pl.String, "posted_date": pl.String}
    return pl.DataFrame(rows, schema=schema)


# ------------------------------------------------------------------------------------------------- inventory


def series_kind() -> pl.Expr:
    """Classify a chart row into the series Q5 cares about (by title and category dimension)."""
    title = pl.col("chart_title")
    ctype = pl.col("category_type")
    return (
        pl.when((ctype == "year") & ~title.str.contains(_EXCLUDED_YEAR_CHARTS.pattern))
        .then(pl.lit("in_service_year"))
        .when((ctype == "month") & title.str.contains(_APPROVALS.pattern))
        .then(pl.lit("status_by_month"))
        .when((ctype == "month") & title.str.contains(_OBSERVATIONS.pattern))
        .then(pl.lit("energized_by_month"))
        .when((ctype == "month") & title.str.contains(_QUEUE.pattern))
        .then(pl.lit("queue_by_month"))
        .when(ctype.is_in(["load_zone", "project_type", "tsp"]))
        .then(pl.lit("by_") + ctype)
        .otherwise(pl.lit("other"))
    )


AS_OF_MAX_LAG_DAYS = 90
AS_OF_MAX_LEAD_DAYS = 31


def with_vintage(cv: pl.DataFrame) -> pl.DataFrame:
    """Add ``vintage`` (the data date) and ``series_kind``.

    The vintage is the chart's as-of date when it is plausible (at most 90 days before and 31 days after the
    report date), else the report date: Gemini read "2024-05-31" off the May 2026 deck and "2024-08-20" off an
    Aug 2026 deck.
    """
    lag = (pl.col("report_date") - pl.col("as_of")).dt.total_days()
    plausible = lag.is_between(-AS_OF_MAX_LEAD_DAYS, AS_OF_MAX_LAG_DAYS)
    return cv.with_columns(
        pl.when(plausible).then(pl.col("as_of")).otherwise(pl.col("report_date")).alias("vintage"),
        series_kind().alias("series_kind"),
    )


MAX_AXIS_LAG_MONTHS = 3
MAX_AXIS_LEAD_MONTHS = 1
_AXIS_KEY = ("vintage", "document", "source_file", "page", "chart_title")


def _month_number(col: str) -> pl.Expr:
    """``"YYYY-MM"`` (or the ``vintage`` date) as a running month count, so two months subtract to a lag."""
    c = pl.col(col)
    if col == "vintage":
        return c.dt.year().cast(pl.Int32) * 12 + c.dt.month().cast(pl.Int32)
    return c.str.slice(0, 4).cast(pl.Int32) * 12 + c.str.slice(5, 2).cast(pl.Int32)


def misdated_month_axes(cv: pl.DataFrame, max_lag_months: int = MAX_AXIS_LAG_MONTHS,
                        max_lead_months: int = MAX_AXIS_LEAD_MONTHS) -> pl.DataFrame:
    """Monthly-axis charts whose axis was misread: the chart's last month sits more than ``max_lag_months``
    before its deck, or more than ``max_lead_months`` after it.

    The rolling "past 12 months" charts end in the deck's month or the one before (lag 0-1 in every deck but
    one). Gemini read the May 2026 deck's axes two years early (2023-07..2024-05 for 2025-07..2026-05, lag 24).
    The rule is per chart and looks at the *last* month: several charts legitimately open with an old anchor bar
    (April 2022 in the 2023 queue charts, October 2022 in the approvals charts), which a per-value age limit
    would drop. A 3-month tolerance also catches a one-year misread. Returns one row per flagged chart.
    """
    months = with_vintage(cv).filter(pl.col("category").str.contains(_MONTH))
    last = months.group_by(_AXIS_KEY).agg(_month_number("category").max().alias("_last"),
                                          pl.col("category").max().alias("last_month"))
    return (
        last.with_columns((_month_number("vintage") - pl.col("_last")).alias("lag_months"))
        .filter((pl.col("lag_months") > max_lag_months) | (pl.col("lag_months") < -max_lead_months))
        .drop("_last")
        .sort("vintage", "document", "page")
    )


def drop_misdated_months(cv: pl.DataFrame, max_lag_months: int = MAX_AXIS_LAG_MONTHS,
                         max_lead_months: int = MAX_AXIS_LEAD_MONTHS) -> pl.DataFrame:
    """Chart values with ``vintage`` and ``series_kind``, minus the monthly values of misdated charts
    (:func:`misdated_month_axes`). Every other row is kept; applying it twice changes nothing.

    Dropped rather than re-dated: shifting the May 2026 axes by +24 months matches the June 2026 deck's reading
    of the same months 10/10, so dropping loses nothing, while a re-date would be a guess for a deck that has no
    later reading to confirm it.
    """
    df = with_vintage(cv)
    bad = misdated_month_axes(df, max_lag_months, max_lead_months).select(_AXIS_KEY)
    if bad.is_empty():
        return df
    flagged = df.join(bad.with_columns(pl.lit(True).alias("_misdated")), on=list(_AXIS_KEY), how="left")
    return flagged.filter(
        ~(pl.col("_misdated").fill_null(False) & pl.col("category").str.contains(_MONTH))
    ).drop("_misdated")


def series_inventory(cv: pl.DataFrame) -> pl.DataFrame:
    """Distinct (vintage, document, page, chart, series kind, dimension) with counts, statuses and span."""
    return (
        with_vintage(cv)
        .group_by("vintage", "report_date", "document", "source_file", "page", "chart_title", "series_kind",
                  "category_type")
        .agg(
            pl.len().alias("n_values"),
            pl.col("status_bucket").unique().sort().str.join(",").alias("status_buckets"),
            pl.col("category").min().alias("first_category"),
            pl.col("category").max().alias("last_category"),
        )
        .sort("vintage", "document", "page", "chart_title")
    )


def deck_coverage(inventory: pl.DataFrame, since: date = PHASE0_SINCE) -> pl.DataFrame:
    """Per document since ``since``: whether it has the in-service series and the status series."""
    return (
        inventory.filter(pl.col("vintage") >= since)
        .group_by("vintage", "report_date", "document", "source_file")
        .agg(
            (pl.col("series_kind") == "in_service_year").any().alias("has_in_service"),
            (pl.col("series_kind") == "status_by_month").any().alias("has_status_by_month"),
            (pl.col("series_kind") == "energized_by_month").any().alias("has_energized_by_month"),
        )
        .sort("vintage", "document")
    )


# --------------------------------------------------------------------------------------- in-service series


_CHART_KEY = ("vintage", "report_date", "document", "source_file", "page", "chart_title")


def in_service_wide(cv: pl.DataFrame) -> pl.DataFrame:
    """One row per in-service chart x year: MW per status bucket, the chart's own total and the segment sum.

    Two labels in one bucket (June 2026 splits "Section 9.4/9.5 met" and "9.4 only" inside planning studies
    approved) are summed. ``a2e`` = approved to energize + approved not operational + observed energized.
    """
    rows = (
        with_vintage(cv)
        .filter((pl.col("series_kind") == "in_service_year") & pl.col("category").str.contains(_YEAR))
        .with_columns(pl.col("category").cast(pl.Int32).alias("year"))
    )
    key = [*_CHART_KEY, "year"]
    totals = (
        rows.filter(pl.col("status_bucket") == "total")
        .group_by(key)
        .agg(pl.col("value_mw").max().alias("total"))
    )
    segments = (
        rows.filter(pl.col("status_bucket").is_in(STATUS_BUCKETS))
        .group_by([*key, "status_bucket"])
        .agg(pl.col("value_mw").sum())
        .pivot(on="status_bucket", index=key, values="value_mw")
    )
    for bucket in STATUS_BUCKETS:
        if bucket not in segments.columns:
            segments = segments.with_columns(pl.lit(None, dtype=pl.Float64).alias(bucket))
    wide = segments.join(totals, on=key, how="full", coalesce=True)
    has_segments = pl.any_horizontal([pl.col(b).is_not_null() for b in STATUS_BUCKETS])
    return (
        wide.with_columns(
            pl.when(has_segments).then(pl.sum_horizontal(STATUS_BUCKETS)).alias("segment_sum"),
            pl.when(has_segments).then(pl.sum_horizontal(A2E_BUCKETS)).alias("a2e"),
        )
        .with_columns(pl.coalesce("total", "segment_sum").alias("total_mw"))
        .sort(*key)
    )


def check_bars_sum(wide: pl.DataFrame, tol: float = TOLERANCE) -> pl.DataFrame:
    """Per chart x year: does the stack of status segments add up to the chart's printed total?"""
    return wide.with_columns(
        (pl.col("segment_sum") / pl.col("total") - 1).alias("bars_rel_diff"),
    ).with_columns(
        pl.when(pl.col("bars_rel_diff").is_null())
        .then(None)
        .otherwise(pl.col("bars_rel_diff").abs() <= tol)
        .alias("bars_ok")
    )


def chart_checks(wide: pl.DataFrame, tol: float = TOLERANCE) -> pl.DataFrame:
    """Per chart: the last bar (whole queue), bars-sum result over all years and whether totals rise by year."""
    checked = check_bars_sum(wide, tol)
    per_chart = checked.group_by(_CHART_KEY).agg(
        pl.col("year").min().alias("first_year"),
        pl.col("year").max().alias("last_year"),
        pl.col("bars_ok").drop_nulls().len().alias("bars_checked"),
        (pl.col("bars_ok").drop_nulls().not_()).sum().alias("bars_failed"),
        pl.col("bars_rel_diff").abs().max().alias("bars_worst_rel_diff"),
        (pl.col("total_mw").sort_by("year").diff().drop_nulls() >= -1).all().alias("monotone"),
    )
    last = (
        wide.sort("year")
        .group_by(_CHART_KEY)
        .agg(
            pl.col("total_mw").last().alias("queue_total_mw"),
            pl.col("a2e").last().alias("a2e_stock_mw"),
            pl.col("no_studies_submitted").last().alias("no_studies_mw"),
            pl.col("segment_sum").last().is_not_null().alias("has_segments"),
        )
    )
    return per_chart.join(last, on=list(_CHART_KEY)).sort("vintage", "document", "page")


def same_deck_headlines(headlines: pl.DataFrame, max_lag_days: int = 45) -> pl.DataFrame:
    """Headline totals that describe the deck itself (drops historical references such as "56 GW in Sep 2024").

    Returns ``source_file``, ``document`` (zip member or file name), ``metric``, ``headline_mw``.
    """
    metrics = ("total_mw_tracked", "approved_to_energize_mw")
    return (
        headlines.filter(pl.col("metric").is_in(metrics) & pl.col("dimension").is_null())
        .filter(
            pl.col("as_of_date").is_null()
            | ((pl.col("report_date") - pl.col("as_of_date")).dt.total_days() <= max_lag_days)
        )
        .with_columns(pl.coalesce("member", "file_name").alias("document"))
        .group_by("source_file", "document", "metric")
        .agg(pl.col("value").max().alias("headline_mw"), pl.col("value_text").first(), pl.col("page").first())
    )


def check_headlines(charts: pl.DataFrame, headlines: pl.DataFrame, tol: float = TOLERANCE) -> pl.DataFrame:
    """Compare each chart's queue total and approved stock with the same deck's headline sentences."""
    hl = same_deck_headlines(headlines)
    total = hl.filter(pl.col("metric") == "total_mw_tracked").select(
        "source_file", "document", pl.col("headline_mw").alias("headline_total_mw")
    )
    a2e = hl.filter(pl.col("metric") == "approved_to_energize_mw").select(
        "source_file", "document", pl.col("headline_mw").alias("headline_a2e_mw")
    )
    out = charts.join(total, on=["source_file", "document"], how="left").join(
        a2e, on=["source_file", "document"], how="left"
    )
    return out.with_columns(
        (pl.col("queue_total_mw") / pl.col("headline_total_mw") - 1).alias("total_rel_diff"),
        (pl.col("a2e_stock_mw") / pl.col("headline_a2e_mw") - 1).alias("a2e_rel_diff"),
    ).with_columns(
        pl.when(pl.col("total_rel_diff").is_null()).then(None)
        .otherwise(pl.col("total_rel_diff").abs() <= tol).alias("total_ok"),
        pl.when(pl.col("a2e_rel_diff").is_null()).then(None)
        .otherwise(pl.col("a2e_rel_diff").abs() <= tol).alias("a2e_ok"),
    ).with_columns(
        (pl.col("total_ok").is_not_null() | pl.col("a2e_ok").is_not_null()).alias("headline_checked"),
        (
            (pl.col("bars_failed") == 0)
            & pl.col("monotone")
            & pl.col("total_ok").fill_null(True)
            & pl.col("a2e_ok").fill_null(True)
        ).alias("consistent"),
    )


def pick_vintages(checked: pl.DataFrame, doc_types: pl.DataFrame | None = None) -> pl.DataFrame:
    """One in-service chart per calendar month of data: prefer consistent charts with a full status stack from
    a status deck, then the most years, then the latest date in the month.

    Revised copies and board/monthly reprints of the same data (often dated a few days apart) collapse into one
    vintage, so the count is of independent snapshots, not of files.
    """
    df = checked
    if doc_types is not None and "doc_type" not in df.columns:
        df = df.join(doc_types.select("source_file", "doc_type"), on="source_file", how="left")
    elif "doc_type" not in df.columns:
        df = df.with_columns(pl.lit(None, dtype=pl.String).alias("doc_type"))
    return (
        df.with_columns(
            pl.col("vintage").dt.strftime("%Y-%m").alias("vintage_month"),
            (pl.col("doc_type") == "status_deck").fill_null(False).alias("_status_deck"),
            (pl.col("last_year") - pl.col("first_year")).alias("_span"),
        )
        .sort(["vintage_month", "consistent", "has_segments", "_status_deck", "_span", "vintage"],
              descending=[False, True, True, True, True, True])
        .group_by("vintage_month", maintain_order=True)
        .first()
        .drop("_status_deck", "_span")
        .sort("vintage")
        # a board update often reprints last month's deck: same queue total and approved stock -> same vintage
        .filter(
            (pl.col("queue_total_mw") != pl.col("queue_total_mw").shift(1)).fill_null(True)
            | (pl.col("a2e_stock_mw") != pl.col("a2e_stock_mw").shift(1)).fill_null(True)
        )
    )


# ------------------------------------------------------------------------------------------- status series


def a2e_by_month(cv: pl.DataFrame, max_lag_months: int = MAX_AXIS_LAG_MONTHS) -> pl.DataFrame:
    """Approved-to-energize stock by month from the status series, keeping the latest deck's reading.

    Misdated axes are dropped first (:func:`drop_misdated_months`): the latest deck wins each month, so one deck
    read two years early would otherwise overwrite 11 good months (Dec 2023 showed the May 2026 deck's 8,786 MW
    instead of 4,479 MW, the Apr 2024 deck's reading).

    "Latest reading" means restated history wins: the Apr 2024 deck restates Jul-Dec 2023 upward (Dec 2023:
    4,479 MW vs 3,188 MW in the Dec 2023 deck, which matches that deck's own sentence as of 11 Dec).
    """
    return (
        drop_misdated_months(cv, max_lag_months)
        .filter(
            (pl.col("series_kind") == "status_by_month")
            & (pl.col("status_bucket") == "approved_to_energize")
            & pl.col("category").str.contains(_MONTH)
        )
        .sort("vintage")
        .group_by("category")
        .agg(
            pl.col("value_mw").last().alias("a2e_mw"),
            pl.col("value_mw").n_unique().alias("n_readings"),
            pl.col("value_mw").min().alias("min_reading"),
            pl.col("value_mw").max().alias("max_reading"),
            pl.col("document").last().alias("document"),
            pl.col("page").last().alias("page"),
            pl.col("vintage").last().alias("read_from_vintage"),
        )
        .rename({"category": "month"})
        .sort("month")
    )


def stock_at_year_end(monthly: pl.DataFrame, year: int, value: str = "a2e_mw") -> tuple[float | None, str | None]:
    """The stock at December of ``year`` (or the latest month before it, for a partial year)."""
    rows = monthly.filter(pl.col("month") <= f"{year}-12").sort("month")
    if rows.is_empty():
        return None, None
    last = rows.row(-1, named=True)
    return last[value], last["month"]


def energized_at_year_end(wide: pl.DataFrame, year: int) -> tuple[float | None, date | None]:
    """Observed-energized stock at the end of ``year``: the bar for ``year`` in the first deck after it ends.

    For a year still in progress this falls back to the latest deck (a partial value).
    """
    rows = wide.filter((pl.col("year") == year) & pl.col("observed_energized").is_not_null()).sort("vintage")
    if rows.is_empty():
        return None, None
    after = rows.filter(pl.col("vintage") > date(year, 12, 31))
    row = (after if not after.is_empty() else rows).row(0 if not after.is_empty() else -1, named=True)
    return row["observed_energized"], row["vintage"]


# -------------------------------------------------------------------------------------- realization ratio


def realization_ratios(vintage_bars: pl.DataFrame, realized: pl.DataFrame) -> pl.DataFrame:
    """Realization ratios per vintage x target year.

    ``vintage_bars``: the picked vintages' in-service rows (``in_service_wide`` joined to ``pick_vintages``),
    with ``vintage``, ``year``, ``total_mw``, ``no_studies_submitted`` and ``base_a2e_mw`` (the vintage's own
    approved stock). ``realized``: ``target_year``, ``realized_a2e_mw``, ``realized_energized_mw``,
    ``realized_as_of`` (the date the realized stock refers to). Only vintages made before ``realized_as_of``
    and before the end of the target year count.
    """
    df = vintage_bars.join(realized, left_on="year", right_on="target_year", how="inner").rename(
        {"year": "target_year"}
    )
    promised = pl.col("total_mw")
    firm = pl.col("total_mw") - pl.col("no_studies_submitted").fill_null(0)
    base = pl.col("base_a2e_mw")
    real = pl.col("realized_a2e_mw")
    return (
        df.filter(
            (pl.col("vintage") < pl.col("realized_as_of"))
            & (pl.col("vintage") < pl.date(pl.col("target_year"), 12, 31))
        )
        .with_columns(
            promised.alias("promised_mw"),
            pl.when(pl.col("no_studies_submitted").is_not_null() | pl.col("has_segments"))
            .then(firm).alias("promised_firm_mw"),
            ((pl.date(pl.col("target_year"), 12, 31) - pl.col("vintage")).dt.total_days() / 30.44)
            .round(0).cast(pl.Int32).alias("horizon_months"),
        )
        .with_columns(
            (real / pl.col("promised_mw")).alias("gross_a2e"),
            (real / pl.col("promised_firm_mw")).alias("gross_a2e_firm"),
            (pl.col("realized_energized_mw") / pl.col("promised_mw")).alias("gross_energized"),
            pl.when(pl.col("promised_mw") - base > 0)
            .then((real - base) / (pl.col("promised_mw") - base)).alias("incremental_a2e"),
            pl.when(pl.col("promised_firm_mw") - base > 0)
            .then((real - base) / (pl.col("promised_firm_mw") - base)).alias("incremental_a2e_firm"),
        )
        .sort("target_year", "vintage")
    )


RATIO_COLUMNS = ("gross_a2e", "gross_a2e_firm", "gross_energized", "incremental_a2e", "incremental_a2e_firm")


def summarize_ratios(ratios: pl.DataFrame, columns: tuple[str, ...] = RATIO_COLUMNS) -> pl.DataFrame:
    """Median and range across vintages, per target year and ratio definition."""
    long = ratios.unpivot(index=["target_year", "vintage"], on=list(columns), variable_name="ratio",
                          value_name="value").drop_nulls("value")
    return (
        long.group_by("target_year", "ratio")
        .agg(
            pl.len().alias("n_vintages"),
            pl.col("value").median().alias("median"),
            pl.col("value").quantile(0.1, "linear").alias("p10"),
            pl.col("value").quantile(0.9, "linear").alias("p90"),
            pl.col("value").min().alias("min"),
            pl.col("value").max().alias("max"),
        )
        .sort("target_year", "ratio")
    )


# ---------------------------------------------------------------------------------------------- cross-checks


def crosscheck_status_vs_headlines(cv: pl.DataFrame, headlines: pl.DataFrame, tol: float = 0.01) -> pl.DataFrame:
    """Gemini's last month of the status series vs the same deck's "approved to energize" sentence
    (misdated axes left out)."""
    last = (
        drop_misdated_months(cv)
        .filter((pl.col("series_kind") == "status_by_month") & (pl.col("status_bucket") == "approved_to_energize")
                & pl.col("category").str.contains(_MONTH))
        .sort("category")
        .group_by("source_file", "document", "page", "chart_title")
        .agg(pl.col("category").last().alias("month"), pl.col("value_mw").last().alias("gemini_mw"))
    )
    hl = same_deck_headlines(headlines).filter(pl.col("metric") == "approved_to_energize_mw").select(
        "source_file", "document", "headline_mw", pl.col("page").alias("headline_page")
    )
    return (
        last.join(hl, on=["source_file", "document"], how="inner")
        .with_columns((pl.col("gemini_mw") / pl.col("headline_mw") - 1).alias("rel_diff"))
        .with_columns((pl.col("rel_diff").abs() <= tol).alias("agree"))
        .sort("month")
    )


_AMBIGUOUS_SERIES = ("remaining approved to energize", "remaining approved to energize load")


def crosscheck_native(cv: pl.DataFrame, status: pl.DataFrame, tol: float = 0.01,
                      max_days_after: int = 70) -> pl.DataFrame:
    """Native PPTX monthly values vs Gemini's reading of the same months in the next deck(s).

    Gemini did not read the charts of the one deck that has native charts (Oct 2024), so the comparison is
    with the rolling 12-month charts of the decks that follow it (same series kind, series and month). ERCOT
    can restate a month between decks, so disagreement is an upper bound on Gemini's error.
    "Remaining approved" appears in both the simultaneous and non-simultaneous charts and is left out.
    """
    native = (
        status.filter(pl.col("dimension") == "snapshot_date")
        .with_columns(pl.lit("month").alias("category_type"))
        .with_columns(series_kind().alias("series_kind"))
        .select(
            pl.coalesce("member", "file_name").alias("native_document"),
            pl.col("report_date").alias("native_date"),
            "series_kind",
            pl.col("series_name").fill_null("Total").str.to_lowercase().alias("series"),
            pl.col("snapshot_date").dt.strftime("%Y-%m").alias("month"),
            pl.col("mw").alias("native_mw"),
        )
        .filter(~pl.col("series").is_in(_AMBIGUOUS_SERIES))
    )
    gem = drop_misdated_months(cv).filter(pl.col("category_type") == "month").select(
        "document", "page", "vintage", "series_kind",
        pl.col("status_label").str.to_lowercase().alias("series"),
        pl.col("category").alias("month"), pl.col("value_mw").alias("gemini_mw"),
    )
    days_after = (pl.col("vintage") - pl.col("native_date")).dt.total_days()
    return (
        native.join(gem, on=["series_kind", "series", "month"], how="inner")
        .filter((days_after > 0) & (days_after <= max_days_after))
        .with_columns((pl.col("gemini_mw") / pl.col("native_mw") - 1).alias("rel_diff"))
        .with_columns((pl.col("rel_diff").abs() <= tol).alias("agree"))
        .sort("series_kind", "series", "month")
    )


def duplicate_agreement(wide: pl.DataFrame, tol: float = 0.01) -> pl.DataFrame:
    """Same vintage read twice (revised copies, reprints): do Gemini's two readings of the bars agree?"""
    cols = ["total_mw", "a2e", *PIPELINE_BUCKETS]
    long = wide.unpivot(index=["vintage", "document", "page", "year"], on=cols, variable_name="field",
                        value_name="mw").drop_nulls("mw")
    pairs = long.join(long, on=["vintage", "year", "field"], suffix="_b").filter(
        pl.col("document") < pl.col("document_b")
    )
    return pairs.with_columns(
        ((pl.col("mw") - pl.col("mw_b")).abs() <= tol * pl.max_horizontal(pl.col("mw").abs(), 1.0)).alias("agree")
    )


# ------------------------------------------------------------------------------------------------ spot check


def slide_link(url: str | None, document: str, page: int) -> str:
    """Public ERCOT URL plus the page: ``#page=N`` for a PDF, "› member, slide N" inside a zip."""
    if not url:
        return f"not verified (no URL in the manifest) — {document}, p. {page}"
    if url.lower().endswith(".zip"):
        return f"{url} › {document}, slide {page}"
    if url.lower().endswith(".pdf"):
        return f"{url}#page={page}"
    return f"{url}, slide {page}"


def spread(df: pl.DataFrame, n: int) -> pl.DataFrame:
    """``n`` rows spread evenly over ``df`` (sorted as given)."""
    if df.height <= n:
        return df
    step = (df.height - 1) / (n - 1)
    return df[[round(i * step) for i in range(n)]]
