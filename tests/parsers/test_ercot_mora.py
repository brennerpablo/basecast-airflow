"""MORA parser. Fixture: cell-for-cell copy of four sheets of the November 2026 edition (trimmed with
xlsxwriter; the Monthly Outlook keeps only its deterministic table)."""

from datetime import date

import polars as pl

from basecast_pipelines.parsers.ercot.mora import DATASETS, parse_outlook

URL = "https://www.ercot.com/files/docs/2026/09/03/MORA_November2026.xlsx"


def _file(raw_file):
    return raw_file("ercot_mora", "ercot_mora/MORA_November2026_trimmed.xlsx", url=URL,
                    meta={"edition_month": "2026-11-01", "link_text": "MORA November 2026"})


def test_deterministic_table(raw_file):
    df = parse_outlook(_file(raw_file))
    assert df["edition"].unique().to_list() == ["MORA November 2026"]
    assert df["target_month"].unique().to_list() == [date(2026, 11, 1)]
    assert df["published_date"].unique().to_list() == [date(2026, 9, 3)]
    outlook = df.filter(pl.col("sheet") == "Monthly Outlook")
    load = outlook.filter(pl.col("line_item") == "Load Based on Average Weather")
    assert load["value"].item() == 65445.872832
    assert load["hour_ending"].item() == 19  # "Hour Ending 7:00 p.m."
    assert outlook.filter(pl.col("line_item") == "Planned")["section"].item() == "Generation Resource Stack"
    assert set(outlook["unit"]) == {"MW"}


def test_capacity_and_percentiles(raw_file):
    df = parse_outlook(_file(raw_file))
    total = df.filter(pl.col("line_item") == "Total Resources, MW")
    assert sorted(total["value"].to_list()) == [99621.842646, 194132.54]
    prrm = df.filter(pl.col("sheet") == "PRRM Percentile Results")
    demand = prrm.filter(pl.col("line_item").str.starts_with("Gross Demand"))
    assert demand.height == 11 * 24
    assert demand.filter((pl.col("percentile") == 0.5) & (pl.col("hour_ending") == 19))["value"].item() == 66698.286246
    outages = prrm.filter(pl.col("line_item").str.starts_with("Unplanned Thermal Outages"))
    assert outages["hour_ending"].null_count() == outages.height and outages.height == 11
    assert set(prrm["unit"]) == {"MW"}


def test_pdf_twin_is_skipped(raw_file):
    pdf = raw_file("ercot_mora", "ercot_mora/MORA_November2026_trimmed.xlsx", name="MORA_November2026.pdf")
    assert parse_outlook(pdf) is None
    assert DATASETS[0].inputs(pdf)
