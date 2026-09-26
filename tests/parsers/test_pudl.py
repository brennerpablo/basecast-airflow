"""PUDL fixtures: a few rows sliced from three v2026.9.0 tables (ERCOT plus another balancing authority
or state, to check the filters)."""

import polars as pl

from basecast_pipelines.parsers.eia.pudl import DATASETS

BY_NAME = {d.name: d for d in DATASETS}


def _file(raw_file, table):
    return raw_file("pudl", f"pudl/{table}.parquet", meta={"release": "v2026.9.0", "table": table})


def test_one_dataset_per_table():
    assert len(DATASETS) == 8
    assert all(d.name.startswith("pudl_") and len(d.name) <= 63 for d in DATASETS)


def test_eia930_keeps_ercot_with_weather_zones(raw_file):
    table = "core_eia930__hourly_subregion_demand"
    ds = BY_NAME[f"pudl_{table}"]
    f = _file(raw_file, table)
    assert ds.inputs(f)
    df = ds.parse(f)
    assert set(df["balancing_authority_code_eia"]) == {"ERCO"}
    assert df.height == 16
    assert df.schema["balancing_authority_subregion_code_eia"] == pl.String  # was categorical
    assert df.schema["datetime_utc"] == pl.Datetime("us", "UTC")
    assert df["weather_zone"].null_count() == 0
    assert set(df["weather_zone"]) <= {"COAST", "EAST", "FWEST", "NORTH", "NCENT", "SOUTH", "SCENT", "WEST"}
    assert df["pudl_release"].unique().to_list() == ["v2026.9.0"]


def test_small_tables_are_kept_whole(raw_file):
    table = "core_ferc714__yearly_planning_area_demand_forecast"
    df = BY_NAME[f"pudl_{table}"].parse(_file(raw_file, table))
    assert set(df["respondent_id_ferc714"]) == {58, 146}
    ercot_2025 = df.filter((pl.col("respondent_id_ferc714") == 58) & (pl.col("report_year") == 2025))
    assert ercot_2025.height == 10


def test_service_territory_texas_with_contract_keys(raw_file):
    table = "core_eia861__yearly_service_territory"
    df = BY_NAME[f"pudl_{table}"].parse(_file(raw_file, table))
    assert set(df["state"]) == {"TX"}
    assert {"utility_id", "county_fips"} <= set(df.columns)
    assert not {"utility_id_eia", "county_id_fips"} & set(df.columns)
    assert df.schema["utility_id"] == pl.String and set(df["utility_id"]) == {"1015"}


def test_inputs_match_their_table_only(raw_file):
    f = _file(raw_file, "core_eia930__hourly_subregion_demand")
    assert [d.name for d in DATASETS if d.inputs(f)] == ["pudl_core_eia930__hourly_subregion_demand"]
