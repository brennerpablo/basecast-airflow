"""NP4-160-SG parser, on the newest real zip trimmed to a few rows of each of its five csv files."""

from datetime import date

import polars as pl

from basecast_pipelines.parsers.ercot.settlement_points import (
    DATASETS,
    parse_ccp,
    parse_hub_dc_tie,
    parse_noie,
    parse_resource_node_unit,
    parse_settlement_points,
)

ZIP = "RPT.00010008.0000000000000000.20260923.000527097.CIM_Sep_ML3_1_09302026_SP_List_and_EB_Mapping.zip"
META = {"friendly_name": "CIM_Sep_ML3_1_09302026_SP_List_and_EB_Mapping"}


def test_settlement_point_map_with_noie_zones(raw_file):
    f = raw_file("ercot_settlement_points", f"ercot_settlement_points/{ZIP}", meta=META)
    assert all(ds.inputs(f) for ds in DATASETS)
    df = parse_settlement_points(f)
    assert df["electrical_bus"].is_unique().all()
    assert df.schema["voltage_kv"] == pl.Float64 and df.schema["psse_bus_number"] == pl.Int64
    assert df["voltage_kv"].null_count() == 0 and df["psse_bus_number"].null_count() == 0
    assert set(df["load_zone"]) <= {"LZ_HOUSTON", "LZ_NORTH", "LZ_SOUTH", "LZ_WEST"}
    assert set(df["noie_load_zone"].drop_nulls()) == {"LZ_AEN", "LZ_CPS", "LZ_LCRA", "LZ_RAYBN"}
    assert df.filter(pl.col("hub").is_not_null())["hub"].str.starts_with("HB_").all()
    assert df.filter(pl.col("resource_node").is_not_null()).height >= 3
    assert df["network_model"].unique().to_list() == ["CIM_Sep_ML3_1_09302026"]
    first = df.row(0, named=True)
    assert first["electrical_bus"] == "0001" and first["voltage_kv"] == 138.0 and first["psse_bus_number"] == 7103


def test_other_tables(raw_file):
    f = raw_file("ercot_settlement_points", f"ercot_settlement_points/{ZIP}", meta=META)
    noie = parse_noie(f)
    assert noie.columns == ["physical_load", "noie_load_zone", "voltage_kv", "substation", "electrical_bus",
                            "network_model"]
    assert set(noie["noie_load_zone"]) == {"LZ_AEN", "LZ_CPS", "LZ_LCRA", "LZ_RAYBN"}
    # every NOIE bus is also in the settlement point list, with the same NOIE zone
    sp = parse_settlement_points(f).select("electrical_bus", pl.col("noie_load_zone").alias("sp_noie"))
    joined = noie.join(sp, on="electrical_bus", how="left")
    assert (joined["noie_load_zone"] == joined["sp_noie"]).all()

    assert parse_resource_node_unit(f).columns == ["resource_node", "unit_substation", "unit_name", "network_model"]
    assert parse_ccp(f).columns == ["ccp_name", "logical_resource_node", "network_model"]
    hubs = parse_hub_dc_tie(f)
    assert set(hubs["kind"]) == {"hub", "dc_tie"}
    assert "HB_NORTH" in hubs["settlement_point"].to_list()


def test_only_the_newest_snapshot(raw_file):
    old = raw_file("ercot_settlement_points", f"ercot_settlement_points/{ZIP}", dt=date(2026, 9, 16), name="old.zip")
    new = raw_file("ercot_settlement_points", f"ercot_settlement_points/{ZIP}", dt=date(2026, 9, 23))
    assert [f.name for f in DATASETS[0].files([old, new])] == [ZIP]
