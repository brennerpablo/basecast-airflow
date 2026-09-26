"""Fuel Mix Report parser. Fixtures are trimmed from the real workbooks: IntGenbyFuel2025.xlsx (current
layout, spring and fall DST days, "(DST)" columns, a header-only future month) and a zip with the 2007
(``Date - Fuel``, Gas before the CC split, no repeated hour) and 2015 (``Date-Fuel``, DST1-DST4 columns)
workbooks, rewritten as .xlsx with the same cells."""

from datetime import date, datetime, timezone

import fastexcel
import polars as pl

from basecast_pipelines.parsers.ercot.fuel_mix import DATASETS, _sheet, _slots, parse_fuel_mix
from tests.parsers.conftest import FIXTURES

CURRENT = "ercot_fuel_mix/IntGenbyFuel2025.xlsx"
OLD = "ercot_fuel_mix/FuelMixReport_PreviousYears.zip"


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def day(df: pl.DataFrame, d: date, fuel: str) -> pl.DataFrame:
    return df.filter(pl.col("local_date") == d, pl.col("fuel") == fuel).sort("interval_start_utc")


def assert_continuous(rows: pl.DataFrame, n: int) -> None:
    assert rows.height == n
    steps = rows["interval_start_utc"].diff().drop_nulls().dt.total_minutes().unique().to_list()
    assert steps == [15]


def test_current_layout(raw_file):
    f = raw_file("ercot_fuel_mix", CURRENT, meta={"link_text": "Fuel Mix Report: 2025"})
    df = parse_fuel_mix(f)
    assert df.columns == [
        "interval_start_utc", "interval_end_utc", "local_date", "interval_ending", "dst_flag", "fuel",
        "fuel_label", "settlement_type", "generation_mwh", "published_label",
    ]
    assert df.schema["interval_start_utc"] == pl.Datetime("us", "UTC")
    assert df.schema["generation_mwh"] == pl.Float64
    assert set(df["fuel"].cast(pl.String)) == {"coal", "solar", "wsl"}
    assert set(df["settlement_type"].cast(pl.String)) == {"FINAL"}
    assert (df.filter(pl.col("fuel") == "wsl")["generation_mwh"] <= 0).all()  # storage charging
    assert df.select(list(DATASETS[0].key)).is_unique().all()
    assert DATASETS[0].inputs(f)

    normal = day(df, date(2025, 3, 8), "coal")
    assert_continuous(normal, 96)
    assert normal["interval_start_utc"][0] == utc(2025, 3, 8, 6, 0)
    assert normal["interval_ending"].to_list()[-1] == "24:00"


def test_current_layout_dst_days(raw_file):
    df = parse_fuel_mix(raw_file("ercot_fuel_mix", CURRENT))

    spring = day(df, date(2025, 3, 9), "coal")
    assert_continuous(spring, 92)
    assert "02:15" not in spring["interval_ending"].to_list()
    assert spring.filter(pl.col("interval_ending") == "03:15")["interval_start_utc"][0] == utc(2025, 3, 9, 8, 0)

    fall = day(df, date(2025, 11, 2), "coal")
    assert_continuous(fall, 100)
    repeated = fall.filter(pl.col("dst_flag"))
    assert repeated["published_label"].cast(pl.String).to_list() == ["01:15 (DST)", "01:30 (DST)", "01:45 (DST)", "02:00 (DST)"]
    assert repeated["interval_ending"].cast(pl.String).to_list() == ["01:15", "01:30", "01:45", "02:00"]
    assert repeated["interval_start_utc"][0] == utc(2025, 11, 2, 7, 0)  # 01:00 CST
    first_pass = fall.filter(pl.col("interval_ending") == "01:15", ~pl.col("dst_flag"))
    assert first_pass["interval_start_utc"][0] == utc(2025, 11, 2, 6, 0)  # 01:00 CDT


def test_daily_totals_match_the_published_total(raw_file):
    """The ``Total`` column of each row is the sum of its intervals: values are MWh per interval."""
    df = parse_fuel_mix(raw_file("ercot_fuel_mix", CURRENT))
    sheet = fastexcel.read_excel(FIXTURES / CURRENT).load_sheet("Nov", header_row=0).to_polars()
    published = {(r["Date"].date(), r["Fuel"].lower()): r["Total"] for r in sheet.iter_rows(named=True)}
    ours = df.group_by("local_date", pl.col("fuel").cast(pl.String)).agg(pl.col("generation_mwh").sum())
    for d, fuel, total in ours.iter_rows():
        if (d, fuel) in published:
            assert abs(total - published[(d, fuel)]) < 1e-3


def test_old_layouts(raw_file):
    df = parse_fuel_mix(raw_file("ercot_fuel_mix", OLD))
    assert df.select(list(DATASETS[0].key)).is_unique().all()
    y2007 = df.filter(pl.col("local_date").dt.year() == 2007)
    assert set(y2007["fuel"].cast(pl.String)) == {"coal", "gas_total"}  # Gas before the CC split
    assert set(y2007["settlement_type"].to_list()) == {None}
    assert set(df.filter(pl.col("local_date").dt.year() == 2015)["fuel"].cast(pl.String)) == {"coal", "gas", "gas_cc", "solar"}


def test_old_layouts_dst_days(raw_file):
    df = parse_fuel_mix(raw_file("ercot_fuel_mix", OLD))

    # 2007: the placeholder block sits at 01:00-01:45; the next published values move up by 4 slots
    spring07 = day(df, date(2007, 3, 11), "coal")
    assert_continuous(spring07, 92)
    moved = spring07.filter(pl.col("published_label") == "02:00")
    assert moved["interval_ending"][0] == "01:00"
    assert (spring07["generation_mwh"] > 0).all()  # the zero placeholders are gone
    # 2007: the repeated hour was never published
    fall07 = day(df, date(2007, 11, 4), "coal")
    assert fall07.height == 96 and not fall07["dst_flag"].any()

    # 2015: the spring block sits at 23:15-24:00; values from 02:15 on belong one hour later
    spring15 = day(df, date(2015, 3, 8), "coal")
    assert_continuous(spring15, 92)
    moved = spring15.filter(pl.col("published_label") == "02:15")
    assert moved["interval_ending"][0] == "03:15"
    assert moved["interval_start_utc"][0] == utc(2015, 3, 8, 8, 0)  # 03:00 CDT
    # 2015: DST1-DST4 hold the day's last four intervals; the column labelled 02:15 is the repeated 01:15
    fall15 = day(df, date(2015, 11, 1), "coal")
    assert_continuous(fall15, 100)
    assert fall15.filter(pl.col("published_label") == "DST1")["interval_ending"][0] == "23:15"
    repeated = fall15.filter(pl.col("dst_flag"))
    assert repeated["published_label"].cast(pl.String).to_list() == ["02:15", "02:30", "02:45", "03:00"]
    assert repeated["interval_start_utc"][0] == utc(2015, 11, 1, 7, 0)
    # the next day's DST columns are zero placeholders and are ignored
    assert day(df, date(2015, 11, 2), "coal").height == 96


def test_newest_workbook_per_link_is_selected(raw_file):
    old = raw_file("ercot_fuel_mix", CURRENT, dt=date(2026, 8, 1), name="a.xlsx", meta={"link_text": "Fuel Mix Report: 2026"})
    new = raw_file("ercot_fuel_mix", CURRENT, dt=date(2026, 9, 1), name="b.xlsx", meta={"link_text": "Fuel Mix Report: 2026"})
    other = raw_file("ercot_fuel_mix", OLD, dt=date(2026, 8, 1), meta={"link_text": "Fuel Mix Report: 2007 - 2024"})
    assert DATASETS[0].files([old, other, new]) == [other, new]


def _grid(header: list, rows: list[list]) -> pl.DataFrame:
    data = [header, *rows]
    return pl.DataFrame([[None if v is None else str(v) for v in col] for col in zip(*data)],
                        schema=[f"c{i}" for i in range(len(header))], orient="col")


def _times() -> list[str]:
    return [f"{m // 60}:{m % 60:02d}" for m in range(15, 1440, 15)] + ["0:00"]


def test_header_typo_is_read_by_position():
    times = _times()
    typo = times[:33] + [times[32]] + times[33:95]  # 08:15 twice, 24:00 missing (Aug 2009)
    slots = _slots(["Date", "Fuel", "Total", *typo], [f"c{i}" for i in range(99)], "x")
    ends = [s.end for s in slots if s.kind == "regular"]
    assert ends == list(range(15, 1441, 15))


def test_all_zero_duplicate_fuel_row_is_dropped():
    """Nov 2011 lists an all-zero "Solar" row next to "Sun" on the same days."""
    header = ["Date", "Fuel", "Total", *_times()]
    rows = [
        ["2011-11-23 00:00:00", "Sun", "96", *["1"] * 96],
        ["2011-11-23 00:00:00", "Solar", "0", *["0"] * 96],
        ["2011-11-23 00:00:00", "Coal", "960", *["10"] * 96],
    ]
    out = _sheet(_grid(header, rows), "x")
    assert out.group_by("fuel").len().sort("fuel").rows() == [("coal", 96), ("solar", 96)]
    assert out.filter(pl.col("fuel") == "solar")["fuel_label"].unique().to_list() == ["Sun"]
