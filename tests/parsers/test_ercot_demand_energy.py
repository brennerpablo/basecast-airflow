"""Demand and Energy report parser, on three trimmed real workbooks: 2026 (current layout, partial year, final
settlement flags), 2008 (zonal market: congestion zones, day-of-month dates, ``0800`` times, long weather-zone
names) and 2016 (``*`` / ``**`` / ``***`` footnotes)."""

from datetime import date, datetime, timezone

import polars as pl

from basecast_pipelines.parsers.ercot.demand_energy import DATASETS, KEY, SCHEMA, parse_demand_energy

WEATHER_ZONES = {"COAST", "EAST", "FWEST", "NORTH", "NCENT", "SOUTH", "SCENT", "WEST"}


def row(df: pl.DataFrame, month: date, region_id: str, metric: str) -> dict:
    out = df.filter((pl.col("month") == month) & (pl.col("region_id") == region_id) & (pl.col("metric") == metric))
    assert out.height == 1
    return out.row(0, named=True)


def test_current_layout(raw_file):
    f = raw_file("ercot_demand_energy", "ercot_demand_energy/DemandandEnergy2026_trimmed.xlsx",
                 name="DemandandEnergy2026-for-Corp-Comms.xlsx", meta={"link_text": "2026 Demand and Energy Report"})
    assert DATASETS[0].inputs(f)
    df = parse_demand_energy(f)
    assert dict(df.schema) == SCHEMA
    assert df.select(pl.struct(*KEY).is_unique().all()).item()
    # months not published yet are dropped; the forecasts cover the whole year
    assert df.filter(pl.col("metric") == "peak_hourly_mw")["month"].max() == date(2026, 8, 1)
    assert df.filter(pl.col("metric") == "forecast_energy_mwh")["month"].max() == date(2026, 12, 1)

    july = row(df, date(2026, 7, 1), "ERCOT", "peak_hourly_mw")
    assert july["value"] == 91133.727696
    assert july["peak_local"] == datetime(2026, 7, 22, 18)  # hour ending 18:00 CDT
    assert july["peak_ts_utc"] == datetime(2026, 7, 22, 23, tzinfo=timezone.utc)
    assert july["interval_minutes"] == 60
    assert not july["final_settlement"]
    assert row(df, date(2026, 1, 1), "ERCOT", "peak_hourly_mw")["final_settlement"]  # "Jan*"

    assert row(df, date(2026, 1, 1), "ERCOT", "peak_15min_mw")["peak_local"] == datetime(2026, 1, 26, 8, 15)
    assert row(df, date(2026, 8, 1), "ERCOT", "min_15min_mw")["peak_local"] == datetime(2026, 8, 2, 7, 15)
    assert row(df, date(2026, 12, 1), "ERCOT", "forecast_peak_hourly_mw")["value"] == 80898.905

    lz = df.filter(pl.col("region_type") == "load_zone")
    assert set(lz["region_id"]) == {"LZ_AEN", "LZ_CPS", "LZ_HOUSTON", "LZ_LCRA", "LZ_NORTH", "LZ_RAYBN", "LZ_SOUTH",
                                   "LZ_WEST"}
    assert set(df.filter(pl.col("region_type") == "weather_zone")["region_id"]) == WEATHER_ZONES
    aen = row(df, date(2026, 1, 1), "LZ_AEN", "noncoincident_peak_15min_mw")
    assert aen["peak_local"] == datetime(2026, 1, 26, 8, 0)
    # coincident zone demand sums to the ERCOT 15-minute peak and carries its time
    coincident = df.filter((pl.col("metric") == "coincident_peak_15min_mw") & (pl.col("region_type") == "weather_zone")
                           & (pl.col("month") == date(2026, 1, 1)))
    assert abs(coincident["value"].sum() - row(df, date(2026, 1, 1), "ERCOT", "peak_15min_mw")["value"]) < 1
    assert coincident["peak_local"].unique().to_list() == [datetime(2026, 1, 26, 8, 15)]
    energy = df.filter((pl.col("metric") == "energy_mwh") & (pl.col("region_type") == "load_zone")
                       & (pl.col("month") == date(2026, 3, 1)))["value"].sum()
    assert abs(energy / row(df, date(2026, 3, 1), "ERCOT", "energy_mwh")["value"] - 1) < 1e-6


def test_zonal_market_layout(raw_file):
    f = raw_file("ercot_demand_energy", "ercot_demand_energy/ercot_2008_demand_and_energy_trimmed.xlsx")
    df = parse_demand_energy(f)
    assert df.height == 12 * (5 + 4 * 3 + 8 * 3)  # no minimum demand in 2008
    assert set(df["region_type"]) == {"ercot", "congestion_zone", "weather_zone"}
    assert set(df.filter(pl.col("region_type") == "congestion_zone")["region_id"]) == {"NORTH", "SOUTH", "HOUSTON",
                                                                                     "WEST"}
    assert set(df.filter(pl.col("region_type") == "weather_zone")["region_id"]) == WEATHER_ZONES
    aug = row(df, date(2008, 8, 1), "ERCOT", "peak_hourly_mw")
    assert aug["value"] == 62174.3766
    assert aug["peak_local"] == datetime(2008, 8, 4, 17)  # "Date 4", "Interval Ending 1700"
    coast = row(df, date(2008, 1, 1), "COAST", "noncoincident_peak_15min_mw")
    assert coast["value"] == 12302.1388 and coast["peak_local"] == datetime(2008, 1, 3, 18, 45)
    assert row(df, date(2008, 1, 1), "COAST", "energy_mwh")["value"] > 6e6  # "Coastal" in the energy table
    assert not df["final_settlement"].any()  # "all values are from initial settlement data"
    assert df["report_year"].unique().to_list() == [2008]


def test_star_footnotes_per_year(raw_file):
    f = raw_file("ercot_demand_energy", "ercot_demand_energy/ERCOT2016D_E_trimmed.xlsx")
    df = parse_demand_energy(f)
    demand = df.filter(pl.col("metric") == "peak_hourly_mw").sort("month")
    # Demand sheet: '*' flags 2015 only, '**' 2016, '***' both; Jan is "Jan**", Aug "Aug***"
    assert demand.filter(pl.col("month") == date(2016, 1, 1))["final_settlement"].item()
    assert demand.filter(pl.col("month") == date(2016, 8, 1))["final_settlement"].item()
    assert row(df, date(2016, 1, 1), "ERCOT", "peak_hourly_mw")["peak_local"] == datetime(2016, 1, 11, 8)


def test_newest_upload_per_report_year(raw_file):
    select = DATASETS[0].select
    partial = raw_file("ercot_demand_energy", "ercot_demand_energy/ercot_2008_demand_and_energy_trimmed.xlsx",
                       name="ercot2011d_e_thru_nov.xls", meta={"link_text": "2011 Demand and Energy Values"},
                       url="https://www.ercot.com/files/docs/2011/12/08/ercot2011d_e_thru_nov.xls")
    final = raw_file("ercot_demand_energy", "ercot_demand_energy/ercot_2008_demand_and_energy_trimmed.xlsx",
                     name="ercot2011d_e.xls", meta={"link_text": "2011 Demand and Energy Values"},
                     url="https://www.ercot.com/files/docs/2012/01/09/ercot2011d_e.xls")
    other = raw_file("ercot_demand_energy", "ercot_demand_energy/DemandandEnergy2026_trimmed.xlsx",
                     name="DemandandEnergy2026.xlsx", url="https://www.ercot.com/files/docs/2026/02/09/x.xlsx")
    assert {f.name for f in select([final, other, partial])} == {"ercot2011d_e.xls", "DemandandEnergy2026.xlsx"}
