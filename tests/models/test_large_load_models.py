"""Q5 large-load helpers on tiny synthetic decks (no database)."""

from datetime import date

import polars as pl
import pytest

from basecast_pipelines.models import large_load as ll

SRC = "raw/source=ercot_large_load_decks/dt=2024-06-03/deck.pdf"


def _row(document, report_date, category, bucket, mw, *, title="Current Large Load Interconnection Queue",
         ctype="year", as_of=None, label=None, source_file=SRC, page=3):
    return {
        "report_date": report_date, "document": document, "as_of": as_of, "page": page, "chart_title": title,
        "status_label": label or bucket, "status_bucket": bucket, "category": category, "category_type": ctype,
        "value_mw": float(mw), "source_file": source_file,
    }


def _deck(document, report_date, bars, *, source_file=SRC, as_of=None):
    """bars: {year: (a2e, psa, review, no_studies, printed_total)}"""
    rows = []
    for year, (a2e, psa, rev, nss, total) in bars.items():
        for bucket, mw in (("approved_to_energize", a2e), ("planning_studies_approved", psa),
                           ("under_ercot_review", rev), ("no_studies_submitted", nss), ("total", total)):
            rows.append(_row(document, report_date, str(year), bucket, mw, source_file=source_file, as_of=as_of))
    return rows


def _cv(rows):
    return pl.DataFrame(rows, schema_overrides={"as_of": pl.Date, "report_date": pl.Date})


def test_in_service_wide_sums_segments_and_flags_bad_bars():
    cv = _cv(_deck("d1", date(2024, 6, 3), {2024: (100, 50, 30, 20, 200), 2025: (100, 80, 60, 60, 250)}))
    checked = ll.check_bars_sum(ll.in_service_wide(cv)).sort("year")
    assert checked["segment_sum"].to_list() == [200.0, 300.0]
    assert checked["a2e"].to_list() == [100.0, 100.0]
    assert checked["bars_ok"].to_list() == [True, False]  # 300 vs 250 is +20%


def test_series_kind_ignores_rpg_and_classifies_months():
    cv = _cv([
        _row("d", date(2026, 9, 14), "2025", "other", 1, title="RPG Project Evaluations"),
        _row("d", date(2026, 9, 14), "2025-01", "approved_to_energize", 1, title="ERCOT Approvals – Past 12 Months",
             ctype="month"),
    ])
    kinds = ll.with_vintage(cv)["series_kind"].to_list()
    assert kinds == ["other", "status_by_month"]


def test_implausible_as_of_falls_back_to_report_date():
    cv = _cv([
        _row("a", date(2026, 5, 21), "2025", "total", 1, as_of=date(2024, 5, 31)),
        _row("b", date(2024, 2, 26), "2025", "total", 1, as_of=date(2024, 1, 31)),
    ])
    assert ll.with_vintage(cv)["vintage"].to_list() == [date(2026, 5, 21), date(2024, 1, 31)]


def test_headline_check_uses_same_deck_and_drops_historical_references():
    cv = _cv(_deck("d1", date(2024, 6, 3), {2024: (100, 50, 30, 20, 200), 2025: (100, 80, 60, 60, 300)}))
    headlines = pl.DataFrame({
        "report_date": [date(2024, 6, 3)] * 3,
        "as_of_date": [None, date(2023, 1, 1), None],
        "metric": ["total_mw_tracked", "total_mw_tracked", "approved_to_energize_mw"],
        "dimension": [None, None, None],
        "member": [None, None, None],
        "file_name": ["d1", "d1", "d1"],
        "source_file": [SRC] * 3,
        "value": [305.0, 50.0, 130.0],
        "value_text": ["305 MW", "50 MW", "130 MW"],
        "page": [2, 2, 5],
    }, schema_overrides={"as_of_date": pl.Date, "dimension": pl.String, "member": pl.String})
    out = ll.check_headlines(ll.chart_checks(ll.in_service_wide(cv)), headlines).row(0, named=True)
    assert out["headline_total_mw"] == 305.0  # the 2023 reference (50 MW) is not this deck's total
    assert out["total_ok"] is True
    assert out["a2e_ok"] is False  # 100 vs 130
    assert out["consistent"] is False


def test_pick_vintages_collapses_reprints_and_same_month():
    decks = (
        _deck("a", date(2024, 6, 3), {2025: (100, 0, 0, 0, 300)})
        + _deck("b", date(2024, 6, 20), {2025: (100, 0, 0, 0, 310)}, source_file="s2")
        + _deck("c", date(2024, 7, 2), {2025: (100, 0, 0, 0, 310)}, source_file="s3")  # reprint of b
        + _deck("d", date(2024, 8, 5), {2025: (120, 0, 0, 0, 330)}, source_file="s4")
    )
    charts = ll.chart_checks(ll.in_service_wide(_cv(decks))).with_columns(pl.lit(True).alias("consistent"))
    picked = ll.pick_vintages(charts)
    assert picked["document"].to_list() == ["b", "d"]


def test_realization_ratios_gross_and_incremental():
    bars = pl.DataFrame({
        "vintage": [date(2024, 1, 15), date(2024, 1, 15)],
        "year": [2024, 2025],
        "total_mw": [20_000.0, 30_000.0],
        "no_studies_submitted": [2_000.0, 5_000.0],
        "has_segments": [True, True],
        "base_a2e_mw": [4_000.0, 4_000.0],
    }).with_columns(pl.col("year").cast(pl.Int32))
    realized = pl.DataFrame({
        "target_year": [2024, 2025],
        "realized_a2e_mw": [6_000.0, 9_000.0],
        "realized_energized_mw": [4_000.0, 6_000.0],
        "realized_as_of": [date(2025, 1, 1), date(2026, 1, 1)],
    }).with_columns(pl.col("target_year").cast(pl.Int32))
    r = ll.realization_ratios(bars, realized).sort("target_year")
    assert r["gross_a2e"].to_list() == pytest.approx([0.3, 0.3])
    assert r["gross_a2e_firm"].to_list() == pytest.approx([6 / 18, 9 / 25])
    assert r["incremental_a2e"].to_list() == pytest.approx([2 / 16, 5 / 26])
    assert r["gross_energized"].to_list() == pytest.approx([0.2, 0.2])
    assert r["horizon_months"].to_list() == [12, 24]  # 350 and 715 days


def test_realization_skips_vintages_after_the_target_year():
    bars = pl.DataFrame({
        "vintage": [date(2025, 2, 1)], "year": [2024], "total_mw": [5.0], "no_studies_submitted": [0.0],
        "has_segments": [True], "base_a2e_mw": [1.0],
    }).with_columns(pl.col("year").cast(pl.Int32))
    realized = pl.DataFrame({"target_year": [2024], "realized_a2e_mw": [3.0], "realized_energized_mw": [2.0],
                             "realized_as_of": [date(2025, 1, 1)]}).with_columns(pl.col("target_year").cast(pl.Int32))
    assert ll.realization_ratios(bars, realized).is_empty()


def test_a2e_by_month_keeps_latest_reading_and_year_end_stock():
    title = "ERCOT Approvals – Past 12 Months"
    cv = _cv([
        _row("old", date(2024, 11, 20), "2024-11", "approved_to_energize", 6000, title=title, ctype="month"),
        _row("new", date(2025, 1, 22), "2024-11", "approved_to_energize", 6100, title=title, ctype="month"),
        _row("new", date(2025, 1, 22), "2024-12", "approved_to_energize", 6300, title=title, ctype="month"),
        _row("new", date(2025, 1, 22), "2025-01", "approved_to_energize", 6306, title=title, ctype="month"),
    ])
    monthly = ll.a2e_by_month(cv)
    assert monthly.filter(pl.col("month") == "2024-11")["a2e_mw"].item() == 6100
    assert ll.stock_at_year_end(monthly, 2024) == (6300.0, "2024-12")
    assert ll.stock_at_year_end(monthly, 2025) == (6306.0, "2025-01")  # partial year: latest month


def test_slide_link_forms():
    assert ll.slide_link("https://x/a.pdf", "a.pdf", 3) == "https://x/a.pdf#page=3"
    assert ll.slide_link("https://x/r.zip", "m.pdf", 4) == "https://x/r.zip › m.pdf, slide 4"
    assert ll.slide_link("https://x/d.pptx", "d.pptx", 5) == "https://x/d.pptx, slide 5"
    assert ll.slide_link(None, "d.pdf", 5).startswith("not verified")
