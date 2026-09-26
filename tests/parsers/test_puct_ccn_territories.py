"""PUCT CCN parser, plus the geo overlays run end to end on the trimmed fixtures (Rockwall and Camp counties,
territories clipped around them). The overlay test needs a PostGIS: it runs only when BASECAST_TEST_DB_URL
is set, and works on session temp tables that shadow the real ones, so it never writes a real table."""

import json
import os
from datetime import date

import polars as pl
import pytest

from basecast_pipelines.parsers.puct.ccn_territories import DATASETS, SQL_DATASETS, parse_ccn_page

SOURCE = "puct_ccn_territories"
DROPPED = {"main_phone", "outage_phone1", "outage_phone2", "company_electric_outages_maplink"}


def test_ccn_coop_page(raw_file):
    raw_file(SOURCE, "puct_ccn_territories/coop_dist_layer.json")
    f = raw_file(SOURCE, "puct_ccn_territories/coop_dist_features_00000.geojson")
    df = parse_ccn_page(f).sort("objectid")
    assert df["territory_id"].to_list() == ["coop_dist_17", "coop_dist_64", "coop_dist_68"]
    assert set(df["utility_type"]) == {"coop"} and set(df["layer"]) == {"COOP_DIST"}
    wood = df.filter(pl.col("territory_id") == "coop_dist_68").row(0, named=True)
    assert wood["iso_rto"] == "ERCOT,SPP" and wood["in_ercot"] and not wood["ercot_only"]
    farmers = df.filter(pl.col("territory_id") == "coop_dist_17").row(0, named=True)
    assert farmers["ercot_only"] and farmers["ccn_no"] == "30055"
    assert json.loads(farmers["ccn_numbers"]) == ["30055"]
    assert df["layer_edited_on"].unique().to_list() == [date(2026, 6, 29)]  # from coop_dist_layer.json
    assert df.schema["data_source_date"] == pl.Date
    assert not DROPPED & set(df.columns)
    assert df.columns[-1] == "geom" and json.loads(df["geom"][0])["type"] in {"Polygon", "MultiPolygon"}
    assert DATASETS[0].inputs(f)


def test_ccn_iou_page_lists_every_ccn(raw_file):
    f = raw_file(SOURCE, "puct_ccn_territories/iou_features_00000.geojson")
    df = parse_ccn_page(f).sort("objectid")
    assert df["utility_type"].unique().to_list() == ["iou"]
    oncor = df.filter(pl.col("company_name").str.starts_with("Oncor")).row(0, named=True)
    assert oncor["ccn_no"] == "30026" and json.loads(oncor["ccn_numbers"]) == ["30026", "30198"]
    swepco = df.filter(pl.col("company_name").str.starts_with("Southwestern Electric")).row(0, named=True)
    assert swepco["iso_rto"] == "SPP" and not swepco["in_ercot"]
    assert df["gt_cooperative"].is_null().all()  # the IOU layer has no G&T field
    assert df["layer_edited_on"].is_null().all()  # no iou_layer.json next to this page


def test_sql_datasets_order():
    # overlays first, then the union and the weather zones that read them
    assert [d.name for d in SQL_DATASETS] == [
        "county_utility_overlap_puct", "county_iso_share", "county_utility_overlap", "county_weather_zone",
    ]


# --- overlays on PostGIS -------------------------------------------------------------------------------


@pytest.fixture
def geo_db(raw_file):
    """A connection whose temp tables hold every geo input parsed from the fixtures."""
    url = os.environ.get("BASECAST_TEST_DB_URL")
    if not url:
        pytest.skip("BASECAST_TEST_DB_URL not set")
    from basecast_pipelines.common.db import connect
    from basecast_pipelines.parsers.census.counties_geo import parse_counties
    from basecast_pipelines.parsers.census.zcta_county import parse_zcta_county
    from basecast_pipelines.parsers.territories.eia_atlas import parse_territory_page
    from tests.parsers.conftest import FIXTURES

    counties = parse_counties(raw_file("census_tx_counties_geo", "census_tx_counties_geo/cb_2025_us_county_500k.zip"))
    zcta = parse_zcta_county(raw_file("census_zcta_county", "census_zcta_county/tab20_zcta520_county20_natl.txt"))
    zips = pl.read_csv(FIXTURES / "census_zcta_county/zip_weather_zone.csv", schema_overrides={"zip_code": pl.String})
    raw_file(SOURCE, "puct_ccn_territories/coop_dist_layer.json")
    puct = pl.concat([
        parse_ccn_page(raw_file(SOURCE, f"puct_ccn_territories/{name}"))
        for name in ("coop_dist_features_00000.geojson", "iou_features_00000.geojson")
    ], how="diagonal_relaxed")
    eia = parse_territory_page(raw_file("eia_territories", "eia_territories/features_00000.geojson"))

    conn = connect(url)
    for name, df, geometry in [
        ("tx_counties", counties, {"geom": 4269}),
        ("census_zcta_county", zcta, {}),
        ("ercot_zip_weather_zone", zips, {}),
        ("puct_ccn_territories", puct, {"geom": 4326}),
        ("eia_utility_territories", eia, {"geom": 4326}),
    ]:
        load_temp_table(conn, name, df.with_columns(pl.lit(f"fixture/{name}").alias("source_file")), geometry)
    yield conn
    conn.close()


def load_temp_table(conn, name: str, df: pl.DataFrame, geometry: dict[str, int]) -> None:
    """Load ``df`` into a session temp table named like the real one (it shadows it), turning GeoJSON text
    columns into EPSG:4326 geometries the way the writer does."""
    from psycopg import sql

    from basecast_pipelines.common.db import column_types, copy_frame

    stage = f"_fixture_{name}"
    columns = column_types(df)
    conn.execute(sql.SQL("CREATE TEMP TABLE {} ({})").format(
        sql.Identifier(stage),
        sql.SQL(", ").join(sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(t)) for c, t in columns.items()),
    ))
    copy_frame(conn, stage, df)
    select = [
        sql.SQL("ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON({c}), {srid}), 4326) AS {c}").format(
            c=sql.Identifier(c), srid=sql.Literal(geometry[c]))
        if c in geometry else sql.Identifier(c)
        for c in columns
    ]
    conn.execute(sql.SQL("CREATE TEMP TABLE {} AS SELECT {} FROM {}").format(
        sql.Identifier(name), sql.SQL(", ").join(select), sql.Identifier(stage)))


def build(conn, name: str, query: str) -> list[dict]:
    from psycopg import sql
    from psycopg.rows import dict_row

    conn.execute(sql.SQL("CREATE TEMP TABLE {} AS ").format(sql.Identifier(name)) + sql.SQL(query))
    with conn.cursor(row_factory=dict_row) as cur:
        return cur.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(name))).fetchall()


def test_overlays_and_weather_zones(geo_db):
    from basecast_pipelines.parsers.territories.eia_atlas import SQL_DATASETS as EIA_SQL

    conn = geo_db
    assert conn.execute("SELECT bool_and(ST_IsValid(geom)), min(ST_SRID(geom)) FROM tx_counties").fetchone() == (
        True, 4326)
    sql = {d.name: d.sql for d in [*SQL_DATASETS, *EIA_SQL]}
    puct = build(conn, "county_utility_overlap_puct", sql["county_utility_overlap_puct"])
    iso = {r["county_fips"]: r for r in build(conn, "county_iso_share", sql["county_iso_share"])}
    eia = build(conn, "county_utility_overlap_eia", sql["county_utility_overlap_eia"])
    both = build(conn, "county_utility_overlap", sql["county_utility_overlap"])
    zones = {r["county_fips"]: r for r in build(conn, "county_weather_zone", sql["county_weather_zone"])}

    assert len(both) == len(puct) + len(eia) and {r["source"] for r in both} == {"puct_ccn", "eia_territories"}
    rockwall = {r["utility_name"]: r["county_share"] for r in puct if r["county_fips"] == "48397"}
    assert rockwall.keys() == {"Oncor Electric Delivery Company LLC", "Farmers Electric Cooperative, Inc."}
    assert rockwall["Oncor Electric Delivery Company LLC"] == pytest.approx(0.88, abs=0.03)
    assert all(0 < r["county_share"] <= 1.0001 and 0 < r["territory_share"] <= 1.0001 for r in both)

    assert iso["48397"]["ercot_listed_share"] == pytest.approx(1, abs=0.01)
    assert iso["48063"]["non_ercot_share"] == pytest.approx(0.95, abs=0.03)

    # Rockwall: ERCOT, North Central. Camp: ZipToZone says EAST but SPP utilities serve it.
    assert (zones["48397"]["weather_zone"], zones["48397"]["in_ercot"]) == ("NCENT", True)
    assert zones["48397"]["top_share"] == pytest.approx(1) and zones["48397"]["n_zones"] == 1
    assert (zones["48063"]["weather_zone"], zones["48063"]["in_ercot"]) == (None, False)
    assert zones["48063"]["ziptozone_zone"] == "EAST"
    # Aransas has no ZCTA in the fixture: kept, without a zone
    assert (zones["48007"]["weather_zone"], zones["48007"]["in_ercot"], zones["48007"]["n_zctas"]) == (None, False, 0)
    assert zones["48397"]["county_name"] == "Rockwall"


def test_equal_area_reads_nested_shells_even_odd(geo_db):
    """ArcGIS GeoJSON writes some holes as separate polygon parts; repaired, they must stay holes."""
    from basecast_pipelines.parsers._geo_common import equal_area

    nested = ("ST_GeomFromText('MULTIPOLYGON(((0 0,1000 0,1000 1000,0 1000,0 0)),"
              "((200 200,400 200,400 400,200 400,200 200)))', 3083)")
    area = geo_db.execute(f"SELECT ST_Area({equal_area(nested)})").fetchone()[0]
    assert area == pytest.approx(1000 * 1000 - 200 * 200)
