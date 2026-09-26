"""ERCOT LTLF parsers, on real workbooks: small ones copied whole, big ones trimmed to a few days with every
column kept (the hourly xlsb is represented by an xlsx trim: fastexcel reads both through the same code)."""

from datetime import date, datetime, timezone

import polars as pl
import pytest

from basecast_pipelines.parsers.ercot import _ltlf_hourly, ltlf

SOURCE = "ercot_ltlf"
ZONES = {"COAST", "EAST", "FWEST", "NCENT", "NORTH", "SCENT", "SOUTH", "WEST"}
LTLF_2025 = {"section": "Long-Term Load Forecast > 2025 Load Forecast Scenarios", "link_text": ""}


def url(day: str, name: str) -> str:
    return f"https://www.ercot.com/files/docs/{day}/{name}"


@pytest.fixture
def ltlf_file(raw_file):
    def make(fixture: str, *, day: str, meta: dict, name: str | None = None):
        return raw_file(SOURCE, f"ercot_ltlf/{fixture}", meta=meta, url=url(day, name or fixture), name=name)

    return make


def one(df: pl.DataFrame, **where) -> float:
    rows = df.filter(pl.all_horizontal([pl.col(k) == v for k, v in where.items()]))
    assert rows.height == 1, (where, rows)
    return rows["value"][0]


# --- ltlf_forecasts -----------------------------------------------------------------------------------------


def test_summer_and_winter_peaks_2025(ltlf_file):
    f = ltlf_file("Summer-and-Winter-Peaks.xlsx", day="2025/04/08", meta={**LTLF_2025, "link_text": "Summer and Winter Peaks"})
    df = ltlf.parse_forecasts(f)
    assert df.columns == list(ltlf.FORECAST_SCHEMA)
    assert set(df["vintage"]) == {"LTLF 2025"} and set(df["vintage_date"]) == {date(2025, 4, 8)}
    assert set(df["scenario"]) == {"ercot_adjusted", "tsp_provided"}
    # the 2025 LTLF report (Table 4) quotes the ERCOT Adjusted summer peaks: 94,650 MW in 2026, 104,295 MW in 2027
    kw = dict(region_id="ERCOT", season="summer", metric="peak_mw", scenario="ercot_adjusted")
    assert round(one(df, target_year=2026, **kw)) == 94650
    assert round(one(df, target_year=2027, **kw)) == 104295
    assert round(one(df, target_year=2031, region_id="ERCOT", season="summer", metric="peak_mw", scenario="tsp_provided")) == 218420
    # the ERCOT column of a non-coincident table is the sum of the zones' own peaks, not a system peak
    ncp = df.filter((pl.col("season") == "summer") & (pl.col("scenario") == "tsp_provided") & (pl.col("target_year") == 2025))
    zone_sum = ncp.filter(pl.col("metric") == "ncp_mw")["value"].sum()
    assert zone_sum == one(ncp, metric="ncp_zone_sum_mw")
    # winters are labelled by the year they start in
    winter = df.filter(pl.col("season") == "winter")
    assert winter["target_year"].min() == 2025 and set(winter["row_label"]) >= {"2025-2026", "2031-2032"}
    assert set(df.filter(pl.col("region_type") == "weather_zone")["region_id"]) == ZONES
    assert df.select("target_year", "season", "region_id", "metric", "scenario").is_unique().all()


def test_monthly_2025_pairs_the_offset_tsp_labels_by_position(ltlf_file):
    f = ltlf_file("2025-ERCOT-Monthly-Peak-Demand-and-Energy-Forecast.xlsx", day="2025/04/08", meta=LTLF_2025)
    df = ltlf.parse_forecasts(f)
    assert df.group_by("scenario", "metric").len().sort("scenario", "metric")["len"].to_list() == [240] * 4
    assert set(df["season"]) == {"month"}
    # TSP's year/month labels sit one row below its values; paired by position the August peaks match the
    # summer peaks of Summer-and-Winter-Peaks.xlsx
    aug = dict(target_year=2026, target_month=8, metric="peak_mw")
    assert round(one(df, scenario="tsp_provided", **aug), 3) == 109031.492
    assert round(one(df, scenario="ercot_adjusted", **aug), 3) == 94650.257
    energy_2026 = df.filter((pl.col("metric") == "energy_mwh") & (pl.col("scenario") == "ercot_adjusted") & (pl.col("target_year") == 2026))
    assert round(energy_2026["value"].sum() / 1e6) == 558  # TWh, 2025 LTLF report Table 4


def test_ncp_workbook_skips_the_historical_table(ltlf_file):
    f = ltlf_file("Summer_NCP_forecast_by_weather_zone_2017.xls", day="2016/12/12",
                  meta={"page_year": 2017, "section": "Load Forecast Scenarios", "link_text": "Weather Zone Non-Coincident Peak Forecast"})
    df = ltlf.parse_forecasts(f)
    assert df["target_year"].min() == 2017 and df["target_year"].max() == 2026
    assert set(df["metric"]) == {"ncp_mw", "ncp_zone_sum_mw"}
    assert set(df["scenario"]) == {"base"} and set(df["season"]) == {"summer"}
    assert df.height == 10 * 9


def test_peak_scenarios_by_weather_year(ltlf_file):
    f = ltlf_file("ercot_peak_demand_scenarios.xls", day="2015/05/08",
                  meta={"page_year": 2014, "section": "2014 Long-Term Load Forecast", "link_text": "ERCOT Peak Demand Scenarios"})
    df = ltlf.parse_forecasts(f)
    assert set(df["region_id"]) == {"ERCOT"} and set(df["metric"]) == {"peak_mw"}
    assert set(df["scenario"]) == {"base_p50", "base_p90"} | {f"base_weather_year_{y}" for y in range(2002, 2014)}
    assert one(df, target_year=2014, scenario="base_p50") == 68096
    assert set(df["vintage"]) == {"LTLF 2014"}


def test_2013_table_keeps_forecast_years_and_converts_gwh(ltlf_file):
    f = ltlf_file("historical_and_forecasted_energy_and_coincident_peak_demand.xls", day="2014/05/02",
                  meta={"page_year": 2013, "section": "2013 Long-Term Load Forecast", "link_text": ""})
    df = ltlf.parse_forecasts(f)
    assert df["target_year"].min() == 2013  # 2003-2012 are history
    assert one(df, target_year=2013, region_id="ERCOT", metric="energy_mwh") == pytest.approx(331_876_720)
    assert one(df, target_year=2013, region_id="ERCOT", metric="peak_mw") == 67998
    assert set(df["season"]) == {"annual"}
    assert set(df.filter(pl.col("region_type") == "weather_zone")["region_id"]) == ZONES


def test_weekly_p90_peaks(ltlf_file):
    f = ltlf_file("p90_peaks_may.xlsx", day="2025/02/05", meta={**LTLF_2025, "link_text": "2025 Weekly P90 Peaks"})
    df = ltlf.parse_forecasts(f)
    assert set(df["season"]) == {"week"} and set(df["scenario"]) == {"base_p90"}
    first = df.filter((pl.col("region_id") == "ERCOT")).sort("target_year", "target_month").row(0, named=True)
    assert first["row_label"] == "week 2025-05-04..2025-05-10, peak 2025-05-09 HE17"
    assert df.select("row_label", "region_id").is_unique().all()


def test_p90_hint_from_the_file_when_titles_omit_it(ltlf_file):
    f = ltlf_file("90th_Percentile_Summer_NCP_by_weather_zone_2022_2031.xlsx", day="2022/02/24",
                  meta={"page_year": 2022, "section": "2022 Load Forecast Scenarios",
                        "link_text": "90th Percentile Summer Non-Coincident Peak by Weather Zone"})
    df = ltlf.parse_forecasts(f)
    assert set(df["scenario"]) == {"base_p90"}
    assert {"gross_ncp_mw", "ncp_mw", "rooftop_pv_ncp_mw", "rooftop_pv_ncp_zone_sum_mw"} <= set(df["metric"])


def test_winter_peak_loads_ignores_a_stray_header_cell(ltlf_file):
    f = ltlf_file("Winter-Peak-Loads.xlsx", day="2024/01/18",
                  meta={"page_year": 2024, "section": "2024 Load Forecast Scenarios", "link_text": "Winter Peak Loads"})
    df = ltlf.parse_forecasts(f)
    assert set(df["season"]) == {"winter"} and df["target_year"].min() == 2024
    assert set(df["metric"]) == {"peak_mw", "ncp_mw", "ncp_zone_sum_mw"}
    assert df.height == 2 * 6 * 9


def test_documents_and_premise_tables_stay_in_raw(ltlf_file):
    premise = ltlf_file("forecast_average_usage_per_premise_by_weather_zone.xls", day="2014/05/13",
                        meta={"page_year": 2014, "link_text": "Forecast average usage per premise by weather zone"})
    assert ltlf.parse_forecasts(premise) is None
    report = ltlf_file("Winter-Peak-Loads.xlsx", day="2025/04/08", meta=LTLF_2025, name="2025_LTLF_Report.docx")
    assert ltlf.parse_forecasts(report) is None


def test_winter_rs_peak_by_transmission_operator(ltlf_file):
    f = ltlf_file("ERCOT-Adjusted-Load-Forecast-Winter-2025-2026-for-RS-Magnitude_trimmed.xlsx", day="2025/10/06",
                  meta={"section": "Winter 2025-2026 Load Forecast for Reliability Standard Magnitude",
                        "link_text": "ERCOT Adjusted Load Forecast Winter 2025-2026 for RS Magnitude"})
    df = ltlf.parse_forecasts(f)
    assert set(df["season"]) == {"winter"} and set(df["target_year"]) == {2025}
    ercot = df.filter(pl.col("region_type") == "ercot")
    assert one(ercot, metric="large_load_additions_mw") == 4817
    with_ll = one(ercot, metric="peak_mw", scenario="ercot_adjusted_p75")
    no_ll = one(ercot, metric="peak_mw", scenario="ercot_adjusted_p75_no_large_loads")
    assert with_ll - no_ll == pytest.approx(4817)
    tos = df.filter(pl.col("region_type") == "transmission_operator")
    assert tos.height == 21 and set(tos["scenario"]) == {"ercot_adjusted_p75_no_large_loads"}
    assert tos["value"].sum() == pytest.approx(no_ll)  # the operators split the load without large loads


# --- ltlf_hourly --------------------------------------------------------------------------------------------


@pytest.mark.parametrize("big", [False, True])
def test_hourly_2025_components_and_dst(ltlf_file, monkeypatch, big):
    if big:  # the path taken by the 48 MB workbooks: one typed read with the header on the first row
        monkeypatch.setattr(_ltlf_hourly, "BIG_WORKBOOK_BYTES", 0)
    f = ltlf_file("ErcotAdjustedForecast_trimmed.xlsx", day="2025/04/08",
                  meta={"section": "Long-Term Load Forecast > 2025 Long-Term Load Forecast Reports", "link_text": "ERCOT Adjusted Forecast"})
    df = ltlf.parse_hourly(f)
    assert df.columns == list(ltlf.HOURLY_SCHEMA)
    assert set(df["scenario"].cast(pl.String)) == {"ercot_adjusted"}
    assert set(df["region_id"].cast(pl.String)) == ZONES | {"ERCOT"}
    assert df.height == 9 * (24 + 23 + 25)
    # net load is the sum of its components
    parts = pl.sum_horizontal("base_economic_mw", "ev_mw", "pv_mw", "lfl_mw", "contracts_mw", "officer_letters_mw")
    assert df.select((parts - pl.col("mw")).abs().max()).item() < 1e-6
    ercot = df.filter(pl.col("region_id") == "ERCOT").sort("ts_utc")
    assert ercot["ts_utc"].null_count() == 0 and ercot["ts_utc"].is_unique().all()
    spring = ercot.filter(pl.col("operating_date") == date(2025, 3, 9))
    assert spring["hour_ending"].to_list()[:2] == [1, 3]
    assert spring["ts_utc"].to_list()[:2] == [datetime(2025, 3, 9, 7, tzinfo=timezone.utc), datetime(2025, 3, 9, 8, tzinfo=timezone.utc)]
    fall = ercot.filter(pl.col("operating_date") == date(2025, 11, 2)).head(4)
    assert fall["hour_ending_local"].to_list() == ["2025-11-02 01:00", "2025-11-02 02:00", "2025-11-02 02:00", "2025-11-02 03:00"]
    assert fall["dst_flag"].to_list() == [False, False, True, False]
    assert [t.hour for t in fall["ts_utc"]] == [6, 7, 8, 9]
    assert ercot.filter(pl.col("operating_date") == date(2025, 1, 1))["ts_utc"].max() == datetime(2025, 1, 2, 6, tzinfo=timezone.utc)


def test_hourly_2022_impossible_labels_keep_values_without_utc(ltlf_file):
    f = ltlf_file("2022_LTLF_Hourly_trimmed.xlsx", day="2022/02/10",
                  meta={"page_year": 2022, "section": "2022 Long-Term Load Forecast Reports", "link_text": "2022 ERCOT Hourly Forecast"})
    df = ltlf.parse_hourly(f)
    assert set(df["scenario"].cast(pl.String)) == {"base"}
    ercot = df.filter(pl.col("region_id") == "ERCOT")
    assert ercot.height == 24 + 25 + 25
    null_ts = ercot.filter(pl.col("ts_utc").is_null())
    # hour ending 2 on the spring-forward day, and the repeated hour on 2022-11-02 (not a DST day)
    assert sorted(null_ts["operating_date"].to_list()) == [date(2022, 3, 13), date(2022, 11, 2)]
    assert ercot.filter(pl.col("ts_utc").is_not_null())["ts_utc"].is_unique().all()
    # the workbook leaves gross and net empty on that spring hour (PV is filled), and nowhere else
    empty = ercot.filter(pl.col("gross_mw").is_null())
    assert empty.select("operating_date", "hour_ending").rows() == [(date(2022, 3, 13), 2)]
    assert ercot["pv_mw"].null_count() == 0


def test_hourly_rs_winter_by_transmission_operator(ltlf_file):
    f = ltlf_file("ERCOT-Adjusted-Load-Forecast-Winter-2025-2026-for-RS-Magnitude_trimmed.xlsx", day="2025/10/06",
                  meta={"section": "Winter 2025-2026 Load Forecast for Reliability Standard Magnitude",
                        "link_text": "ERCOT Adjusted Load Forecast Winter 2025-2026 for RS Magnitude"})
    df = ltlf.parse_hourly(f)
    assert df.height == 48 * (2 + 21)
    tos = df.filter(pl.col("region_type") == "transmission_operator").group_by("ts_utc").agg(pl.col("mw").sum())
    no_ll = df.filter(pl.col("scenario") == "ercot_adjusted_p75_no_large_loads", pl.col("region_type") == "ercot")
    joined = tos.join(no_ll.select("ts_utc", pl.col("mw").alias("ercot")), on="ts_utc")
    assert joined.height == 48 and joined.select((pl.col("mw") - pl.col("ercot")).abs().max()).item() < 1e-6


def test_peak_workbooks_have_no_hourly_rows(ltlf_file):
    f = ltlf_file("Summer-and-Winter-Peaks.xlsx", day="2025/04/08", meta=LTLF_2025)
    assert ltlf.parse_hourly(f) is None


# --- ltlf_weather_scenarios ---------------------------------------------------------------------------------


def test_weather_scenarios_long_format(ltlf_file):
    meta = {"section": "Long-Term Load Forecast > Weather Year Scenario Forecast - By Weather Region", "link_text": "Coast"}
    f = ltlf_file("Coast_trimmed.xlsx", day="2025/08/22", meta=meta)
    df = ltlf.parse_weather_scenarios(f)
    assert df.columns == list(ltlf.WEATHER_SCHEMA)
    hours = 24 + 25
    assert df.height == hours * (45 + 5)
    assert set(df["vintage"].cast(pl.String)) == {"LTLF 2025"}
    assert set(df["weather_zone"].cast(pl.String)) == {"COAST"}
    assert set(df["scenario"].cast(pl.String)) == {"ercot_adjusted"}
    pred = df.filter(pl.col("component") == "pred")
    assert pred["weather_year"].min() == 1980 and pred["weather_year"].max() == 2024
    other = df.filter(pl.col("component") != "pred")
    assert other["weather_year"].null_count() == other.height
    assert set(other["component"].cast(pl.String)) == {"ev", "pv", "lfl", "contracts", "officer_letters"}
    assert pred.filter(pl.col("weather_year") == 2011).select("ts_utc").is_unique().all()
    # all 8 workbooks route here and nowhere else
    assert [d.name for d in ltlf.DATASETS if d.inputs(f)] == ["ltlf_weather_scenarios"]


def test_vintage_from_page_section_or_url(ltlf_file):
    f = ltlf_file("Coast_trimmed.xlsx", day="2025/08/22", meta={"section": "Weather Year Scenario Forecast", "link_text": "Coast"})
    assert ltlf.vintage_year(f) == 2025 and ltlf.vintage_date(f) == date(2025, 8, 22)
    g = ltlf_file("p90_peaks_may.xlsx", day="2016/12/13", meta={"page_year": 2017, "section": "Long-Term Load Forecast Reports"})
    assert ltlf.vintage_year(g) == 2017 and ltlf.vintage_date(g) == date(2016, 12, 13)
