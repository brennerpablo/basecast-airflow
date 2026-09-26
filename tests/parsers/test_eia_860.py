"""EIA-860 fixture: the 2025 zip with the Plant workbook trimmed to three Texas plants (plus one
out-of-state) and the Generator workbook trimmed to a few rows per sheet."""

import polars as pl

from basecast_pipelines.parsers.eia.eia_860 import parse_generators, parse_plants

ZIP = "eia_860/eia8602025.zip"


def test_plants_keep_texas_with_td_owner(raw_file):
    df = parse_plants(raw_file("eia_860", ZIP, meta={"data_year": 2025}))
    assert set(df["state"]) == {"TX"}
    assert df["data_year"].unique().to_list() == [2025]
    laredo = df.filter(pl.col("plant_id_eia") == 3439).row(0, named=True)
    assert laredo["td_owner"] == "AEP Texas Central Company" and laredo["td_owner_id"] == "3278"
    assert laredo["county_fips"] == "48479"  # Webb
    assert laredo["balancing_authority_code"] == "ERCO"
    assert df.schema["utility_id"] == pl.String and df.schema["latitude"] == pl.Float64
    assert df.schema["grid_voltage_kv"] == pl.Float64 and df.schema["energy_storage"] == pl.Boolean


def test_generators_from_every_sheet(raw_file):
    df = parse_generators(raw_file("eia_860", ZIP))
    assert set(df["sheet"]) == {"operable", "proposed", "retired_and_canceled"}
    assert set(df["state"]) == {"TX"}
    assert df.schema["nameplate_capacity_mw"] == pl.Float64
    assert df.schema["operating_year"] == pl.Int64 and df.schema["current_year"] == pl.Int64
    assert df.schema["uprate_or_derate_completed_during_year"] == pl.Boolean
    assert "rto_iso_location_designation" in df.columns
    assert max(len(c) for c in df.columns) <= 63
    proposed = df.filter(pl.col("sheet") == "proposed")
    assert proposed["status"].is_in(["P", "L", "T", "U", "V", "TS", "OT"]).all()
