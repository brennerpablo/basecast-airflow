import polars as pl

from basecast_pipelines.parsers.census.zcta_county import DATASETS, SQL_DATASETS, parse_zcta_county

FIXTURE = "census_zcta_county/tab20_zcta520_county20_natl.txt"  # Rockwall, Camp, Aransas (no ZCTA), 2 AL rows


def test_zcta_county_keeps_texas_parts(raw_file):
    f = raw_file("census_zcta_county", FIXTURE)
    df = parse_zcta_county(f)
    assert set(df["county_fips"]) == {"48007", "48063", "48397"}
    assert df.height == 12
    assert df.schema["part_land_m2"] == pl.Int64 and df.schema["share_of_county_land"] == pl.Float64
    assert df["vintage"].unique().to_list() == [2020]
    assert df.filter(pl.col("zcta5").is_not_null())["zcta5"].str.len_chars().unique().to_list() == [5]
    # Aransas: the part of the county outside any ZCTA
    aransas = df.filter(pl.col("county_fips") == "48007").row(0, named=True)
    assert aransas["zcta5"] is None and aransas["share_of_zcta_land"] is None
    assert aransas["county_name_full"] == "Aransas County"
    # a county's parts add up to its land area
    camp = df.filter(pl.col("county_fips") == "48063")
    assert camp["part_land_m2"].sum() == camp["county_land_m2"][0]
    assert DATASETS[0].inputs(f)


def test_county_weather_zone_is_declared():
    [sd] = SQL_DATASETS
    assert sd.name == "county_weather_zone"
    for table in ("census_zcta_county", "ercot_zip_weather_zone", "county_iso_share"):
        assert table in sd.sql
