"""Map geometry for the app (A-M2): Texas counties and ERCOT weather zones as GeoJSON.

Read from PostGIS as ``basecast_reader`` (``tx_counties`` joined to ``county_weather_zone``), simplified in
EPSG:3083 (Texas-centric Albers, metres) with ``ST_SimplifyPreserveTopology``, written in WGS84 with 5 decimals.
The zones are the ``in_ercot`` counties dissolved by weather zone before simplifying. The files go to
``basecast-app/public/geo/``; the app commits them and joins its data by ``county_fips`` (MapLibre ``promoteId``).

Each polygon is simplified on its own, so neighbours can show slivers up to the tolerance along shared edges;
the smallest tolerance that keeps every file under :data:`MAX_BYTES` is used (:func:`export`).
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

COUNTIES_FILE = "tx-counties.geojson"
ZONES_FILE = "ercot-weather-zones.geojson"
MAX_BYTES = 1_000_000
TOLERANCES_M = (100, 150, 200, 300, 500)
N_COUNTIES, N_ERCOT_COUNTIES, N_ZONES = 254, 204, 8
COUNTY_PROPERTIES = ("county_fips", "county_name", "weather_zone", "in_ercot")
ZONE_PROPERTIES = ("weather_zone",)

_SIMPLIFIED = "ST_AsGeoJSON(ST_Transform(ST_SimplifyPreserveTopology({geom}, %(tol)s), 4326), 5)"
COUNTIES_SQL = f"""
select c.county_fips, c.county_name,
       case when z.in_ercot then z.weather_zone end as weather_zone,
       coalesce(z.in_ercot, false) as in_ercot,
       {_SIMPLIFIED.format(geom="ST_Transform(c.geom, 3083)")} as geometry
from tx_counties c
left join county_weather_zone z using (county_fips)
order by c.county_fips
"""
ZONES_SQL = f"""
select z.weather_zone,
       {_SIMPLIFIED.format(geom="ST_Union(ST_Transform(c.geom, 3083))")} as geometry
from tx_counties c
join county_weather_zone z using (county_fips)
where z.in_ercot
group by z.weather_zone
order by z.weather_zone
"""


def feature_collection(rows: Iterable[Mapping[str, Any]], properties: Sequence[str]) -> dict[str, Any]:
    """A FeatureCollection from rows carrying ``properties`` and ``geometry`` (GeoJSON text)."""
    return {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "properties": {p: row[p] for p in properties}, "geometry": json.loads(row["geometry"])}
            for row in rows
        ],
    }


def encode(collection: Mapping[str, Any]) -> bytes:
    """Compact UTF-8 JSON (no spaces), as served to the browser."""
    return json.dumps(collection, separators=(",", ":"), ensure_ascii=False).encode()


def check_counties(collection: Mapping[str, Any]) -> None:
    """Raise unless there are 254 counties with unique 5-digit ``county_fips``, 204 in ERCOT, and a weather zone
    exactly on the ERCOT ones."""
    props = [f["properties"] for f in collection["features"]]
    fips = [p["county_fips"] for p in props]
    problems = []
    if len(props) != N_COUNTIES:
        problems.append(f"{len(props)} counties, expected {N_COUNTIES}")
    if len(set(fips)) != len(fips):
        problems.append("duplicate county_fips")
    if bad := [f for f in fips if not (isinstance(f, str) and len(f) == 5 and f.isdigit())]:
        problems.append(f"county_fips not 5 digits: {bad[:5]}")
    if (n := sum(p["in_ercot"] is True for p in props)) != N_ERCOT_COUNTIES:
        problems.append(f"{n} ERCOT counties, expected {N_ERCOT_COUNTIES}")
    if bad := [p["county_fips"] for p in props if (p["weather_zone"] is not None) != (p["in_ercot"] is True)]:
        problems.append(f"weather_zone set outside ERCOT or missing inside it: {bad[:5]}")
    if problems:
        raise ValueError("; ".join(problems))


def check_zones(collection: Mapping[str, Any]) -> None:
    """Raise unless there are 8 distinct weather zones."""
    zones = [f["properties"]["weather_zone"] for f in collection["features"]]
    if len(zones) != N_ZONES or len(set(zones)) != N_ZONES or None in zones:
        raise ValueError(f"expected {N_ZONES} distinct weather zones, got {zones}")


def build(read_sql: Callable[..., Any], tolerance_m: float) -> dict[str, bytes]:
    """Both files at one tolerance, checked. ``read_sql(query, params)`` returns a polars DataFrame."""
    params = {"tol": float(tolerance_m)}
    counties = feature_collection(read_sql(COUNTIES_SQL, params).iter_rows(named=True), COUNTY_PROPERTIES)
    zones = feature_collection(read_sql(ZONES_SQL, params).iter_rows(named=True), ZONE_PROPERTIES)
    check_counties(counties)
    check_zones(zones)
    return {COUNTIES_FILE: encode(counties), ZONES_FILE: encode(zones)}


def export(out_dir: Path, *, tolerance_m: float | None = None,
           read_sql: Callable[..., Any] | None = None) -> tuple[float, dict[str, int]]:
    """Write both files to ``out_dir`` at ``tolerance_m``, or at the smallest of :data:`TOLERANCES_M` that keeps
    each under :data:`MAX_BYTES`. Returns the tolerance used and the bytes per file."""
    if read_sql is None:
        from basecast_pipelines.models.db import read_sql
    for tol in (tolerance_m,) if tolerance_m is not None else TOLERANCES_M:
        files = build(read_sql, tol)
        if tolerance_m is not None or all(len(b) < MAX_BYTES for b in files.values()):
            break
    else:
        raise ValueError(f"no tolerance in {TOLERANCES_M} m keeps both files under {MAX_BYTES:,} bytes")
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, data in files.items():
        (out_dir / name).write_bytes(data)
    return tol, {name: len(data) for name, data in files.items()}
