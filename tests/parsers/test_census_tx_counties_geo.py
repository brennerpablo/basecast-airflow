import json

import polars as pl
import pytest

from basecast_pipelines.parsers.census.counties_geo import DATASETS, SHAPEFILE_SRID, parse_counties, srid_from_prj

FIXTURE = "census_tx_counties_geo/cb_2025_us_county_500k.zip"  # Rockwall, Camp and Falls Church (VA)


def test_counties_keeps_texas_only(raw_file):
    f = raw_file("census_tx_counties_geo", FIXTURE)
    df = parse_counties(f)
    assert df["county_fips"].to_list() == ["48063", "48397"]
    assert df["county_name"].to_list() == ["Camp", "Rockwall"]
    assert df["county_name_full"].to_list() == ["Camp County", "Rockwall County"]
    assert df.schema["aland_m2"] == pl.Int64 and df.schema["vintage"] == pl.Int32
    assert df["vintage"].to_list() == [2025, 2025]  # from the file name when discovery recorded none
    assert df.columns[-1] == "geom"
    for text in df["geom"]:
        geometry = json.loads(text)
        assert geometry["type"] in {"Polygon", "MultiPolygon"}
        lon, lat = geometry["coordinates"][0][0] if geometry["type"] == "Polygon" else geometry["coordinates"][0][0][0]
        assert -107 < lon < -93 and 25 < lat < 37
    assert DATASETS[0].inputs(f) and DATASETS[0].geometry == {"geom": SHAPEFILE_SRID}


def test_counties_vintage_from_discovery(raw_file):
    f = raw_file("census_tx_counties_geo", FIXTURE, name="counties.zip", meta={"vintage": 2024})
    assert parse_counties(f)["vintage"].unique().to_list() == [2024]


def test_srid_from_prj():
    nad83 = 'GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137,298.25]]]'
    wgs84 = 'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]]]'
    assert srid_from_prj(nad83) == 4269
    assert srid_from_prj(wgs84) == 4326
    with pytest.raises(ValueError):
        srid_from_prj('PROJCS["NAD83 / Texas Centric Albers",GEOGCS["NAD83",DATUM["North_American_Datum_1983"]]]')
