"""The map GeoJSON export (``basecast_pipelines/marts/geo.py``) on synthetic rows (no database)."""

from __future__ import annotations

import json

import polars as pl
import pytest

from basecast_pipelines.marts import geo

SQUARE = json.dumps({"type": "Polygon", "coordinates": [[[-97.0, 30.0], [-96.9, 30.0], [-96.9, 30.1], [-97.0, 30.0]]]})
ZONES = ["COAST", "EAST", "FWEST", "NCENT", "NORTH", "SCENT", "SOUTH", "WEST"]


def _counties() -> pl.DataFrame:
    rows = []
    for i in range(geo.N_COUNTIES):
        ercot = i < geo.N_ERCOT_COUNTIES
        rows.append({"county_fips": f"48{2 * i + 1:03d}", "county_name": f"County {i}",
                     "weather_zone": ZONES[i % 8] if ercot else None, "in_ercot": ercot, "geometry": SQUARE})
    return pl.DataFrame(rows)


def _zones() -> pl.DataFrame:
    return pl.DataFrame({"weather_zone": ZONES, "geometry": [SQUARE] * 8})


def _reader(counties: pl.DataFrame, zones: pl.DataFrame):
    return lambda query, params: counties if query == geo.COUNTIES_SQL else zones


def test_counties_carry_the_four_properties_and_parsed_geometry():
    fc = geo.feature_collection(_counties().head(2).iter_rows(named=True), geo.COUNTY_PROPERTIES)
    first = fc["features"][0]
    assert first["properties"] == {"county_fips": "48001", "county_name": "County 0", "weather_zone": "COAST",
                                   "in_ercot": True}
    assert first["geometry"]["type"] == "Polygon"
    assert b" " not in geo.encode(fc).replace(b"County ", b"")  # compact JSON


def test_county_checks():
    geo.check_counties(geo.feature_collection(_counties().iter_rows(named=True), geo.COUNTY_PROPERTIES))
    bad_fips = _counties().with_columns(pl.when(pl.int_range(pl.len()) == 0).then(pl.lit("4801"))
                                        .otherwise(pl.col("county_fips")).alias("county_fips"))
    zone_outside = _counties().with_columns(pl.lit("WEST").alias("weather_zone"))
    for frame, message in ((bad_fips, "5 digits"), (zone_outside, "outside ERCOT"),
                           (_counties().head(253), "253 counties")):
        with pytest.raises(ValueError, match=message):
            geo.check_counties(geo.feature_collection(frame.iter_rows(named=True), geo.COUNTY_PROPERTIES))


def test_zone_checks():
    geo.check_zones(geo.feature_collection(_zones().iter_rows(named=True), geo.ZONE_PROPERTIES))
    with pytest.raises(ValueError, match="8 distinct"):
        geo.check_zones(geo.feature_collection(_zones().head(7).iter_rows(named=True), geo.ZONE_PROPERTIES))


def test_export_takes_the_smallest_tolerance_under_the_size_limit(tmp_path, monkeypatch):
    size = len(geo.encode(geo.feature_collection(_counties().iter_rows(named=True), geo.COUNTY_PROPERTIES)))
    calls: list[float] = []

    def read_sql(query, params):
        calls.append(params["tol"])
        if query != geo.COUNTIES_SQL:
            return _zones()
        # finer tolerances return a longer name, i.e. a bigger file, until 200 m
        return _counties() if params["tol"] >= 200 else _counties().with_columns(
            (pl.col("county_name") + "x" * 50).alias("county_name"))

    monkeypatch.setattr(geo, "MAX_BYTES", size + 1)
    tol, sizes = geo.export(tmp_path, read_sql=read_sql)
    assert tol == 200 and sorted(set(calls)) == [100, 150, 200]
    assert sizes[geo.COUNTIES_FILE] == size == (tmp_path / geo.COUNTIES_FILE).stat().st_size
    assert json.loads((tmp_path / geo.ZONES_FILE).read_text())["features"][0]["properties"] == {"weather_zone": "COAST"}
    # a forced tolerance is written even above the limit
    tol, sizes = geo.export(tmp_path, tolerance_m=100, read_sql=read_sql)
    assert tol == 100 and sizes[geo.COUNTIES_FILE] > size


def test_export_fails_when_no_tolerance_fits(tmp_path, monkeypatch):
    monkeypatch.setattr(geo, "MAX_BYTES", 10)
    with pytest.raises(ValueError, match="no tolerance"):
        geo.export(tmp_path, read_sql=_reader(_counties(), _zones()))
    assert not list(tmp_path.iterdir())
