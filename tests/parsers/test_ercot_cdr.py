"""CDR parser. Fixtures: the April 2002 working paper as published (52 KB xls, per-TDSP blocks) and cell-for-cell
copies of a few sheets of the December 2019 and December 2025 editions (trimmed with xlsxwriter)."""

from datetime import date

import polars as pl

from basecast_pipelines.parsers.ercot._adequacy_common import extract_tables, period_token
from basecast_pipelines.parsers.ercot.cdr import DATASETS, parse_capacity, parse_forecasts, select_editions

SOURCE = "ercot_cdr"


def test_old_working_paper_has_per_utility_blocks(raw_file):
    f = raw_file(SOURCE, "ercot_cdr/ERCOT02CDR04092002.xls",
                 meta={"page_year": 2002, "link_text": "Capacity, Demand and Reserves Report - 2002"})
    forecasts = parse_forecasts(f)
    assert forecasts["vintage"].unique().to_list() == ["CDR Apr 2002"]  # from the file name 04092002
    ercot = forecasts.filter((pl.col("region_type") == "ercot") & (pl.col("season") == "summer"))
    peak = dict(ercot.filter(pl.col("metric") == "peak_mw").select("target_year", "value").iter_rows())
    assert peak == {2003: 60727.0, 2004: 63190.0, 2005: 64965.0, 2006: 66857.0, 2007: 68715.0}
    brazos = forecasts.filter(
        (pl.col("region_id") == "Brazos Electric Power Coop") & (pl.col("season") == "summer")
        & (pl.col("metric") == "firm_peak_mw")
    )
    assert brazos.filter(pl.col("target_year") == 2003)["value"].item() == 1828.9
    assert set(forecasts["region_type"]) == {"ercot", "utility"}

    capacity = parse_capacity(f)
    margin = capacity.filter(pl.col("line_item").str.contains("Percent Reserve Margin")).head(1)
    assert margin["unit"].item() == "fraction"
    assert capacity.schema["value"] == pl.Float64 and capacity.schema["target_year"] == pl.Int64


def test_december_2025_seasons_scenarios_and_net_load_hour(raw_file):
    f = raw_file(SOURCE, "ercot_cdr/CapacityDemandandReservesReport_December2025_trimmed.xlsx",
                 meta={"edition_month": "2025-12-01", "link_text": "Capacity Demand and Reserves Report December 2025"})
    fc = parse_forecasts(f)
    assert fc["vintage"].unique().to_list() == ["CDR Dec 2025"]
    assert fc["vintage_date"].unique().to_list() == [date(2025, 12, 1)]
    base = fc.filter(pl.col("scenario").is_null())
    assert set(base["season"]) == {"summer", "winter", "spring", "fall"}
    summer_peak = base.filter((pl.col("season") == "summer") & (pl.col("metric") == "peak_mw"))
    assert summer_peak.filter(pl.col("target_year") == 2026)["value"].item() == 95419.42689
    # winter "2026/2027" → target year 2026, published label kept
    winter = base.filter((pl.col("season") == "winter") & (pl.col("metric") == "firm_peak_mw"))
    assert winter.filter(pl.col("target_year") == 2026)["column_label"].item().startswith("Winter | 2026/2027")
    # the Peak Net Load Hour columns become their own scenario; "Difference" columns are dropped
    assert fc.filter(pl.col("scenario") == "peak_net_load_hour").height == 40
    assert not fc["column_label"].str.contains("Difference").any()
    # scenario rows keep their published label, even where they equal the base case (2026)
    tsp = fc.filter(pl.col("scenario").str.starts_with("Scenario 3") & (pl.col("season") == "summer"))
    assert tsp.height == 5 and tsp.filter(pl.col("target_year") == 2030)["value"].item() == 100347.721196
    # hidden input sheets ("Load") are not read
    assert set(parse_capacity(f)["sheet"]) == {"Load-Resource Scenarios", "Seasonal Summary"}


def test_december_2019_summary_and_unlabelled_fraction_table(raw_file):
    f = raw_file(SOURCE, "ercot_cdr/CapacityDemandandReserveReport-Dec2019_trimmed.xlsx",
                 meta={"edition_month": "2019-12-01", "link_text": "Capacity Demand and Reserves Report December 2019"})
    fc = parse_forecasts(f)
    assert sorted(set(fc["metric"])) == ["firm_peak_mw", "peak_before_ee_mw", "peak_mw"]
    assert fc.filter((pl.col("metric") == "peak_mw") & (pl.col("target_year") == 2020))["value"].item() == 76695.56895
    cap = parse_capacity(f)
    fuel = cap.filter(pl.col("sheet") == "SummerFuelTypes")
    assert fuel.filter(pl.col("unit") == "MW")["value"].max() > 1000
    assert fuel.filter(pl.col("unit") == "fraction")["value"].max() <= 1.0
    assert fuel.filter(pl.col("column_label") == "Capacity_Pct")["unit"].unique().to_list() == ["fraction"]


def test_companion_workbooks_and_reposts(raw_file):
    companion = raw_file(SOURCE, "ercot_cdr/CapacityDemandandReserveReport-Dec2019_trimmed.xlsx",
                         name="CDR_Summer_PeakAveWindCapacityPercentages_11-26-2019.xlsx")
    assert parse_capacity(companion) is None and parse_forecasts(companion) is None
    link = "Capacity, Demand and Reserves Report - May 2013"
    own = raw_file(SOURCE, "ercot_cdr/ERCOT02CDR04092002.xls", name="a.xls",
                   meta={"page_year": 2013, "edition_month": "2013-05-01", "link_text": link})
    repost = raw_file(SOURCE, "ercot_cdr/ERCOT02CDR04092002.xls", name="b.xls",
                      meta={"page_year": 2014, "edition_month": "2013-05-01", "link_text": link})
    assert [x.name for x in select_editions([own, repost])] == ["a.xls"]
    assert all(d.inputs(own) for d in DATASETS)


def test_extract_tables_stacked_headers_and_side_by_side():
    rows = [
        ["Title", None, None, None, None, None],
        [None, "Summer", "Summer", None, "Net", "Net"],
        ["Table 1", 2026, 2027, "Table 1", 2026, 2027],
        ["Firm Peak Load, MW", 100.5, 110.5, "Firm Peak Net Load", 90.5, 95.5],
        ["Reserve Margins (%)", None, None, "Reserve Margins (%)", None, None],
        ["Protocol", 0.1, 0.2, "Protocol", 0.3, 0.4],
        ["Mothballed Capacity, MW", 2006.0, 2006.0, None, None, None],
    ]
    values = extract_tables(rows)
    left = [v for v in values if v.col == 2 and v.row == 3][0]
    assert left.label == "Firm Peak Load, MW" and left.headers == ("Summer", "2027")
    right = [v for v in values if v.col == 5 and v.row == 3][0]
    assert right.label == "Firm Peak Net Load" and right.headers == ("Net", "2027")
    margin = [v for v in values if v.row == 5 and v.col == 4][0]
    assert margin.section == "Reserve Margins (%)"
    # a data row whose values look like years is still data
    assert [v.value for v in values if v.row == 6] == [2006.0, 2006.0]


def test_period_tokens():
    assert period_token(2026.0).year == 2026
    assert period_token("2026/2027").season == "winter"
    assert period_token("2025/26").year == 2025
    assert period_token("Summer 2025").season == "summer"
    assert period_token(2024.5) is None and period_token("Total") is None
