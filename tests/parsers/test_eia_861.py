"""EIA-861 fixtures: the 2024 final and 2025 early-release zips trimmed to a few utilities (Austin Energy,
Bailey County EC and one out-of-state utility), keeping each workbook's header block."""

import json

import polars as pl

from basecast_pipelines.parsers.territories.eia_861 import DATASETS, parse_sales, parse_territory, parse_utility

FINAL = "eia_861/f8612024.zip"
EARLY = "eia_861/f8612025er.zip"


def test_sales_are_long_by_sector_texas_only(raw_file):
    f = raw_file("eia_861", FINAL, meta={"data_year": 2024, "early_release": False})
    df = parse_sales(f)
    assert set(df["state"]) == {"TX"}
    assert set(df["sector"]) == {"residential", "commercial", "industrial", "transportation", "total"}
    assert df.schema["utility_id"] == pl.String
    assert df.schema["revenue_thousand_usd"] == pl.Float64 and df.schema["customers"] == pl.Int64
    austin = df.filter((pl.col("utility_id") == "1015") & (pl.col("sector") == "total")).row(0, named=True)
    assert austin["customers"] == 562004
    assert austin["sales_mwh"] == 14200574
    assert austin["revenue_thousand_usd"] == 1475140.4
    assert austin["ba_code"] == "ERCO" and austin["ownership"] == "Municipal" and austin["part"] == "A"
    assert austin["data_year"] == 2024 and austin["early_release"] is False
    # "." (not applicable) becomes null, not zero
    bailey = df.filter((pl.col("utility_id") == "1169") & (pl.col("sector") == "transportation"))
    assert bailey["sales_mwh"].to_list() == [None]


def test_early_release_has_a_leading_note_column_and_is_flagged(raw_file):
    f = raw_file("eia_861", EARLY)  # no meta: year and flag from the file name
    df = parse_sales(f)
    assert df["early_release"].unique().to_list() == [True]
    assert df["data_year"].unique().to_list() == [2025]
    assert "col_0" not in df.columns
    assert df.filter((pl.col("utility_id") == "1015") & (pl.col("sector") == "total"))["customers"].item() == 577889


def test_utility_flags_and_balancing_authority(raw_file):
    df = parse_utility(raw_file("eia_861", FINAL))
    assert set(df["state"]) == {"TX"}
    austin = df.filter(pl.col("utility_id") == "1015").row(0, named=True)
    assert austin["ownership_type"] == "Municipal"
    assert austin["nerc_region"] == "TRE"
    assert austin["rto_ercot"] is True
    assert austin["ba_code"] == "ERCO" and json.loads(austin["ba_codes"]) == ["ERCO"]
    bailey = df.filter(pl.col("utility_id") == "1169").row(0, named=True)
    assert bailey["ownership_type"] == "Cooperative" and bailey["ba_code"] == "SWPP"
    # the SPP column appears twice (NERC region and RTO): the group row keeps them apart
    assert {"nerc_spp", "rto_spp", "activity_distribution"} <= set(df.columns)
    assert all(df.schema[c] == pl.Boolean for c in df.columns if c.startswith(("rto_", "activity_")))


def test_service_territory_maps_county_fips(raw_file):
    df = parse_territory(raw_file("eia_861", FINAL))
    assert set(df["state"]) == {"TX"}
    austin = df.filter(pl.col("utility_id") == "1015")
    assert dict(zip(austin["county_name"], austin["county_fips"])) == {"Travis": "48453", "Williamson": "48491"}
    assert df["county_fips"].null_count() == 0
    assert df["county_fips"].str.len_chars().unique().to_list() == [5]


def test_inputs_take_release_zips_only(raw_file):
    assert DATASETS[0].inputs(raw_file("eia_861", FINAL))
    assert not DATASETS[0].inputs(raw_file("eia_861", FINAL, name="notes.zip"))
