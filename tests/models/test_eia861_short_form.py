"""EIA-861 short form reader and long + short combination (``basecast_pipelines/models/eia861_short_form.py``),
on tiny synthetic workbooks and frames (no database, no real zip)."""

from __future__ import annotations

import io
import zipfile

import polars as pl
import pytest

from basecast_pipelines.models import eia861_short_form as S

HEADER_2024 = [
    "Data Year", "Utility Number", "Utility Name", "Ownership", "State", "BA Code",
    "Total Revenue (Thousand Dollars)", "Total Sales (MWh)", "Total Customers", "Water Heater", "Net Metering",
    "Demand Side Management", "Time Based Programs",
]  # fmt: skip


def _xlsx(rows: list[list[object]], sheet: str = "861S") -> bytes:
    xlsxwriter = pytest.importorskip("xlsxwriter")
    buf = io.BytesIO()
    book = xlsxwriter.Workbook(buf, {"in_memory": True})
    ws = book.add_worksheet(sheet)
    for r, row in enumerate(rows):
        for c, value in enumerate(row):
            if value is not None:
                ws.write(r, c, value)
    book.close()
    return buf.getvalue()


def _zip(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, data in members.items():
            z.writestr(name, data)
    return buf.getvalue()


def test_parse_short_form_types_columns_and_keeps_texas():
    book = _xlsx(
        [
            HEADER_2024,
            ["2024", "1287", "City of Acme - (TX)", "Municipal", "TX", "ERCO", "1200.5", "9832", "1500", ".", "Y", "N",
             "N"],  # fmt: skip
            ["2024", "19952", "Zeta Electric Department", "Municipal", "TX", "ERCO", ".", ".", ".", ".", "N", "N", "N"],
            ["2024", "555", "Okla Town", "Municipal", "OK", "SWPP", "10", "20", "30", ".", "N", "N", "N"],
        ]
    )
    df = S.parse_short_form(book, data_year=2024, early_release=False)
    assert df.schema == pl.Schema(S.SHORT_FORM_SCHEMA)
    assert df["utility_id"].to_list() == ["1287", "19952"]
    acme = df.row(0, named=True)
    assert acme["revenue_thousand_usd"] == 1200.5 and acme["sales_mwh"] == 9832.0 and acme["customers"] == 1500
    assert acme["net_metering"] is True and acme["demand_side_management"] is False
    blank = df.row(1, named=True)  # EIA's "." is missing, not zero
    assert blank["customers"] is None and blank["sales_mwh"] is None
    assert S.parse_short_form(book, data_year=2024, early_release=False, states=None).height == 3


def test_parse_short_form_old_and_early_release_layouts():
    old = _xlsx(  # 2013-2014: no Ownership, BA_CODE
        [
            ["Data Year", "Utility Number", "Utility Name", "State", "BA_CODE", "Total Revenue (Thousand Dollars)",
             "Total Sales (MWh)", "Total Customers", "Water Heater", "Net Metering", "Demand Side Management",
             "Time Based Programs"],
            ["2013", "1287", "Acme", "TX", "ERCO", "900", "8818", "1400", ".", "N", "N", "N"],
        ]
    )  # fmt: skip
    df = S.parse_short_form(old, data_year=2013, early_release=False)
    assert df["ownership"].to_list() == [None] and df["ba_code"].to_list() == ["ERCO"]
    early = _xlsx(  # early release: a note row above the header and a leading note column
        [
            [None, "This is an early release of the final EIA-861 data."],
            [None, *HEADER_2024],
            ["Early release data. Not fully edited.", "2025", "1287", "Acme", "Municipal", "TX", "ERCO", "1300",
             "9900", "1510", ".", "N", "N", "N"],
        ]
    )  # fmt: skip
    er = S.parse_short_form(early, data_year=2025, early_release=True)
    assert er.select("data_year", "early_release", "utility_id", "customers").rows() == [(2025, True, "1287", 1510)]


def test_parse_short_form_fails_loudly_on_text_in_numbers_and_wrong_year():
    bad = _xlsx([HEADER_2024, ["2024", "1", "Acme", "Municipal", "TX", "ERCO", "12", "oops", "3", ".", "N", "N", "N"]])
    with pytest.raises(ValueError, match="sales_mwh"):
        S.parse_short_form(bad, data_year=2024, early_release=False)
    good = _xlsx([HEADER_2024, ["2023", "1", "Acme", "Municipal", "TX", "ERCO", "12", "4", "3", ".", "N", "N", "N"]])
    with pytest.raises(ValueError, match="expected 2024"):
        S.parse_short_form(good, data_year=2024, early_release=False)


def test_read_short_form_zip_uses_the_zip_name_and_skips_zips_without_it():
    book = _xlsx([HEADER_2024, ["2024", "1", "Acme", "Municipal", "TX", "ERCO", "12", "4", "3", ".", "N", "N", "N"]])
    data = _zip({"Short_Form_2024.xlsx": book, "Sales_Ult_Cust_2024.xlsx": b"not read"})
    df = S.read_short_form_zip(data, "f8612024.zip")
    assert df["source_file"].to_list() == ["f8612024.zip/Short_Form_2024.xlsx"]
    assert S.read_short_form_zip(_zip({"Sales_Ult_Cust_2019.xlsx": b"x"}), "f8612019.zip") is None
    assert S.release_of("f8612025er.zip") == (2025, True)
    with pytest.raises(ValueError):
        S.release_of("Short_Form_2024.xlsx")


def _long_rows() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "data_year": [2024, 2024, 2024, 2024],
            "early_release": [False] * 4,
            "utility_id": ["10", "10", "10", "20"],
            "part": ["A", "D", "C", "A"],
            "ba_code": ["ERCO"] * 4,
            "sector": ["total", "total", "total", "total"],
            "revenue_thousand_usd": [100.0, 5.0, 50.0, None],
            "sales_mwh": [1000.0, 50.0, 500.0, None],
            "customers": [900, 0, 400, None],
        }
    )


def test_long_form_totals_follow_the_part_c_rule_and_carry_revenue():
    tot = S.long_form_totals(_long_rows()).sort("utility_id")
    assert tot.schema == pl.Schema(S.TOTALS_SCHEMA)
    ten = tot.row(0, named=True)
    assert ten["customers"] == 900.0  # part C customers left out
    assert ten["sales_mwh"] == 1550.0 and ten["revenue_thousand_usd"] == 155.0
    twenty = tot.row(1, named=True)
    assert twenty["customers"] is None and twenty["sales_mwh"] is None  # blank is not zero


def _totals(rows: list[tuple]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=S.TOTALS_SCHEMA, orient="row")


def test_combine_forms_prefers_the_long_form_and_flags_overlaps():
    long = _totals([("10", 2024, False, 900.0, 1000.0, 100.0)])
    short = _totals([("10", 2024, False, 1.0, 1.0, 1.0), ("30", 2024, False, 50.0, 400.0, 48.0)])
    out = S.combine_forms(long, short)
    assert out.select("utility_id", "form", "customers", "in_both").rows() == [
        ("10", "long", 900.0, True),
        ("30", "short", 50.0, False),
    ]
    short_sum = S.short_form_totals(
        pl.DataFrame(
            {"utility_id": ["30"], "data_year": [2024], "early_release": [False], "customers": [None],
             "sales_mwh": [None], "revenue_thousand_usd": [None]},
            schema_overrides={"customers": pl.Int64, "sales_mwh": pl.Float64, "revenue_thousand_usd": pl.Float64},
        )
    )  # fmt: skip
    assert short_sum["customers"].to_list() == [None]


def test_add_delivery_and_wires_customers():
    combined = S.combine_forms(
        _totals(
            [
                ("L", 2019, False, 100.0, 1.0, 1.0),
                ("L", 2024, False, 5.0, 1.0, 1.0),
                ("N", 2019, False, 50.0, 1.0, 1.0),
                ("N", 2024, False, 60.0, 1.0, 1.0),
            ]
        ),
        _totals([]),
    )
    delivery = pl.DataFrame(
        {"utility_id": ["L", "N", "N"], "data_year": [2024, 2020, 2024], "early_release": [False] * 3,
         "customers": [101.0, 3.0, 2.0]}
    )  # fmt: skip
    out = S.add_delivery(combined, delivery, first_year=2020).sort("utility_id", "data_year")
    assert out.select("utility_id", "data_year", "delivery_customers", "wires_customers").rows() == [
        ("L", 2019, 0.0, 100.0),  # no delivery customers in the first file year: 0 before it
        ("L", 2024, 101.0, 106.0),
        ("N", 2019, None, None),  # already had delivery customers in 2020: unknown before
        ("N", 2024, 2.0, 62.0),
    ]
    sig = S.utility_signals(out, year=2024, base_year=2019, customers="wires_customers").sort("utility_id")
    lub, nue = sig.row(0, named=True), sig.row(1, named=True)
    assert lub["eia_customers"] == 106.0
    assert lub["eia_customer_cagr"] == pytest.approx((106 / 100) ** (1 / 5) - 1)
    assert nue["eia_customers"] == 62.0
    assert nue["eia_customer_cagr"] == pytest.approx((60 / 50) ** (1 / 5) - 1)  # falls back to bundled


def test_utility_signals_price_growth_and_final_releases_only():
    combined = S.combine_forms(
        _totals([("10", 2019, False, 100.0, 1000.0, 100.0), ("10", 2024, False, 121.0, 1000.0, 150.0)]),
        _totals([("10", 2025, True, 999.0, 1.0, 1.0), ("30", 2024, False, 50.0, 0.0, 48.0)]),
    )
    sig = S.utility_signals(combined, year=2024, base_year=2019).sort("utility_id")
    ten = sig.row(0, named=True)
    assert ten["eia_price"] == pytest.approx(0.15)  # thousand USD / MWh = USD / kWh
    assert ten["eia_price_cagr"] == pytest.approx((0.15 / 0.10) ** (1 / 5) - 1)
    assert ten["eia_customer_cagr"] == pytest.approx((121 / 100) ** (1 / 5) - 1)
    assert ten["eia_form"] == "long" and ten["eia_base_form"] == "long"
    thirty = sig.row(1, named=True)
    assert thirty["eia_price"] is None and thirty["eia_customer_cagr"] is None and thirty["eia_form"] == "short"


def test_percentile_rank_averages_ties_and_keeps_nulls():
    df = pl.DataFrame({"v": [1.0, 2.0, 2.0, None]}).select(S.percentile_rank(pl.col("v")).alias("r"))
    assert df["r"].to_list() == [pytest.approx(1 / 3), pytest.approx(2.5 / 3), pytest.approx(2.5 / 3), None]
