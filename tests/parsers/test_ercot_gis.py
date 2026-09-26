"""ercot_gis parsers, on workbooks trimmed from the real Aug 2017 (old layout) and Aug 2026 (GIM layout)
GIS reports and the Aug 2026 co-located battery report (see tests/fixtures/ercot_gis/make_fixtures.py)."""

from datetime import date

import polars as pl
import pytest

from basecast_pipelines.parsers.ercot.gis import (
    BATTERY_SCHEMA,
    CELL_SCHEMA,
    DATASETS,
    SNAPSHOT_SCHEMA,
    find_blocks,
    norm_inr,
    one_file_per_month,
    parse_cells,
    parse_colocated_battery,
    parse_snapshots,
)

SOURCE = "ercot_gis"
OLD = "ercot_gis/gis_report_2017_08_trimmed.xlsx"
NEW = "ercot_gis/gis_report_2026_08_trimmed.xlsx"
BATTERY = "ercot_gis/colocated_battery_2026_08_trimmed.xlsx"
GIS_META = {"family": "gis_report"}


def row(df: pl.DataFrame, inr: str) -> dict:
    rows = df.filter(pl.col("inr") == inr).to_dicts()
    assert len(rows) == 1, inr
    return rows[0]


@pytest.fixture
def old(raw_file):
    return parse_snapshots(raw_file(SOURCE, OLD, dt=date(2017, 8, 1), meta=GIS_META))


@pytest.fixture
def new(raw_file):
    return parse_snapshots(raw_file(SOURCE, NEW, dt=date(2026, 8, 1), meta=GIS_META))


def test_snapshot_schema_is_stable_across_layouts(old, new):
    expected = list(SNAPSHOT_SCHEMA.items())
    assert list(old.schema.items()) == expected
    assert list(new.schema.items()) == expected
    for df in (old, new):
        assert df["inr"].is_unique().all()
        assert df["inr"].str.contains(r"^\d{2}INR\d{4}").all()


def test_recent_layout_milestones_and_merge(new):
    assert new["report_month"].unique().to_list() == [date(2026, 8, 1)]
    wind = row(new, "15INR0064b")  # Project Details - Large Gen, wrapped five-row header
    assert wind["status"] == "active"
    assert wind["size_category"] == "Large"
    assert wind["county"] == "Glasscock"
    assert wind["cdr_reporting_zone"] == "WEST"
    assert (wind["fuel"], wind["technology"], wind["fuel_type"]) == ("WIN", "WT", "wind")
    assert wind["capacity_mw"] == pytest.approx(162.1)
    assert wind["study_phase"] == "SS Completed, FIS Completed, IA"
    assert wind["screening_study_started"] == date(2013, 11, 27)
    assert wind["fis_requested"] == date(2014, 4, 15)
    assert wind["ia_signed"] == date(2018, 5, 30)
    assert wind["financial_security_provided"] is True
    assert wind["approved_energization"] == date(2020, 2, 10)
    assert wind["approved_synchronization"] == date(2020, 5, 12)
    assert wind["projected_cod"] == date(2026, 12, 3)
    assert (wind["air_permit"], wind["air_permit_date"]) == ("Not Required", None)

    solar = row(new, "16INR0049")
    assert solar["site_control_approved"] == date(2014, 7, 7)  # "Approval Date for Submission of Proof of ..."
    assert solar["change_indicators"] is None

    # in Large Gen and in the month's Commissioning Update: one row, project table wins, event added
    bess = row(new, "22INR0467")
    assert bess["status"] == "active"
    assert bess["sheets"] == '["Project Details - Large Gen", "Commissioning Update"]'
    assert bess["sections"] == '["project_details", "commissioning"]'
    assert bess["fuel_type"] == "storage"
    assert bess["approved_energization"] == date(2026, 8, 5)
    assert bess["approved_synchronization"] == date(2026, 8, 28)
    assert "Energization Approved by ERCOT" in bess["commissioning_categories"]


def test_recent_layout_update_tables(new):
    cod = row(new, "24INR0294")  # only in Commissioning Update: left the queue this month
    assert cod["status"] == "commissioning_update"
    assert cod["commercial_operation_date"] == date(2026, 8, 7)
    assert cod["fuel"] == "Battery Storage" and cod["fuel_type"] == "storage"

    cancelled = row(new, "21INR0280")
    assert cancelled["status"] == "cancelled"
    assert cancelled["cancel_date"] == date(2026, 8, 11)  # "2026-08-11 14:29:05"
    assert cancelled["size_category"] == "Large"

    inactive = row(new, "13INR0010a")
    assert inactive["status"] == "inactive"
    assert inactive["inactive_date"] == date(2019, 9, 9)

    small = row(new, "22INR0596")
    assert small["size_category"] == "Small"
    assert small["model_ready_date"] == date(2022, 9, 1)
    assert small["financial_security_required_dist"] is True


def test_old_layout(old):
    assert old.height == 11
    ia = row(old, "13INR0049")  # IA Table: header on row 5 under title rows, m/Y projected date
    assert ia["status"] == "active"
    assert ia["sections"] == '["ia_table"]'
    assert ia["projected_cod"] == date(2017, 9, 1)  # "9/2017"
    assert ia["financial_security_provided"] is True
    assert ia["meets_planning_6_9_1bd_met"] is True
    assert (ia["air_permit"], ia["water_availability"], ia["fis_status"]) == ("Yes", "Yes", "Complete")

    # IA Table + Solar Chart (IA signed date) + "New Resources Approved for Synchronization"
    pearl = row(old, "15INR0070_1b")
    assert pearl["sections"] == '["ia_table", "fuel_chart", "synchronization"]'
    assert pearl["ia_signed"] == date(2014, 12, 17)
    assert pearl["approved_synchronization"] == date(2017, 8, 24)

    upton = row(old, "16INR0065b")  # only in the monthly sub-tables
    assert upton["status"] == "commissioning_update"
    assert upton["commercial_operation_date"] == date(2017, 8, 25)

    fis = row(old, "12INR0055")  # Full Study Table
    assert fis["sections"] == '["full_study"]'
    assert fis["fis_status"] == "Incomplete"
    assert fis["poi_location"].startswith("tap 69kV")

    assert row(old, "13INR0006")["status"] == "cancelled"  # Full Study + "Projects Cancelled by the Developer"
    assert row(old, "19INR0024")["cancel_date"] == date(2017, 8, 31)


def test_zip_wrapped_workbook_parses_like_the_workbook(raw_file, old):
    f = raw_file(SOURCE, "ercot_gis/gis_report_2017_08_trimmed.zip", dt=date(2017, 8, 1), meta=GIS_META)
    assert DATASETS[1].inputs(f)
    assert parse_snapshots(f).equals(old)


def test_cells_keep_every_value_with_its_composite_header(raw_file):
    df = parse_cells(raw_file(SOURCE, NEW, dt=date(2026, 8, 1), meta=GIS_META))
    assert list(df.schema.items()) == list(CELL_SCHEMA.items())
    assert (df["value"].str.strip_chars() != "").all()
    assert set(df["sheet"]) == {"Summary", "Project Details - Large Gen", "Project Details - Small Gen",
                                "Commissioning Update", "Inactive Projects", "Cancellation Update"}
    large = df.filter(pl.col("sheet") == "Project Details - Large Gen")
    site = large.filter(pl.col("value") == "2014-07-07 00:00:00")
    assert site["header_label"].to_list() == ["Approval Date for Submission of Proof of Site Control"]
    title = large.filter(pl.col("value") == "GIM Project Details - Large Generators")
    assert title["header_label"].to_list() == [None] and title["header_row_index"].to_list() == [None]
    inr = large.filter(pl.col("value") == "15INR0064b").to_dicts()[0]
    header = large.filter(pl.col("value") == "INR").to_dicts()[0]
    assert inr["header_row_index"] == header["row_index"]
    assert inr["column_index"] == header["column_index"] == 0
    assert inr["row_index"] > header["row_index"] + 4  # the header wraps over five rows
    # absolute coordinates: the used range of the real sheet starts below Excel row 1
    assert large["row_index"].min() > 0
    assert df.filter(pl.col("sheet") == "Summary")["header_label"].is_null().all()


def test_colocated_battery(raw_file):
    f = raw_file(SOURCE, BATTERY, dt=date(2026, 8, 1), meta={"family": "co_located_battery"})
    assert DATASETS[2].inputs(f) and not DATASETS[1].inputs(f)
    df = parse_colocated_battery(f)
    assert list(df.schema.items()) == list(BATTERY_SCHEMA.items())
    assert df.group_by("colocation").len().sort("colocation").rows() == [
        ("operational", 3), ("solar", 3), ("stand_alone", 2)]
    solar = df.filter(pl.col("colocation") == "solar")
    assert solar["inr"].str.contains(r"^\d{2}INR\d{4}").all()
    assert row(df, "29INR0108")["is_battery"] is True  # "7L Storage SLF", OTH / BA
    assert row(df, "29INR0107")["is_battery"] is False
    units = df.filter(pl.col("colocation") == "operational")
    assert units["inr"].is_null().all() and units["unit_code"].is_not_null().all()
    anchor = units.filter(pl.col("unit_code") == "ANCHOR_BESS1").to_dicts()[0]
    assert (anchor["unit_name"], anchor["in_service_year"], anchor["capacity_mw"]) == ("ANCHOR BESS U1", 2022, 35.2)


def test_one_file_per_month_prefers_the_latest_correction(raw_file):
    def make(name, dt, publish=None):
        meta = {**GIS_META, **({"publish_date": publish} if publish else {})}
        return raw_file(SOURCE, OLD, dt=dt, name=name, meta=meta)

    listed = [make("GIS_Report_July_2020_CORRECTION.xlsx", date(2020, 7, 1), "2020-08-04T09:10:17-05:00"),
              make("GIS_Report_July_2020.xlsx", date(2020, 7, 1), "2020-08-03T14:57:17-05:00"),
              make("GIS_Report_July_2020_CORRECTION2.xlsx", date(2020, 7, 1), "2020-08-06T14:02:48-05:00")]
    year_page = [make("GIS_REPORT__March_2017_Revised.xlsx", date(2017, 3, 1)),
                 make("GIS_REPORT__March_2017.xlsx", date(2017, 3, 1))]
    chosen = one_file_per_month(listed + year_page)
    assert [f.name for f in chosen] == ["GIS_REPORT__March_2017_Revised.xlsx", "GIS_Report_July_2020_CORRECTION2.xlsx"]


def test_find_blocks_joins_wrapped_headers_and_splits_sub_tables():
    rows = [
        ("New Resources Approved for Synchronization", None, None),
        ("INR", "Project", "Part2 Synch"),
        (None, None, "Apprv"),
        ("17INR0001", "A", "2017-08-01 00:00:00"),
        (None, None, None),
        ("Projects Cancelled by the Developer*", None, None),
        ("INR", "Project", "Cancellation Date*"),
        ("17INR0002", "B", "2017-08-02 00:00:00"),
    ]
    first, second = find_blocks(rows)
    assert first.labels == ("INR", "Project", "Part2 Synch Apprv")
    assert (first.header_row, first.data_start, first.data_end) == (1, 3, 6)
    assert (second.header_row, second.data_start) == (6, 7)
    assert norm_inr(" 22inr0373*** ") == "22INR0373"
