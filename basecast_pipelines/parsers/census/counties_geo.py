"""Texas county polygons from the Census cartographic boundary shapefile (counties, 1:500k, national zip).

The zip is read in memory with pyshp; only ``STATEFP = 48`` is kept (254 counties). Coordinates stay in
the shapefile's datum (NAD83, EPSG:4269, checked against the ``.prj``) and the writer transforms them to
EPSG:4326. The newest snapshot wins."""

from __future__ import annotations

import io
import re
import zipfile

import polars as pl
import shapefile

from basecast_pipelines.parsers._geo_common import geojson_text
from basecast_pipelines.processing.core import Dataset, RawFile, latest_dt

SOURCE_ID = "census_tx_counties_geo"
TEXAS_STATEFP = "48"
# Datum of the Census cartographic boundary files; ``parse_counties`` refuses a .prj that says otherwise.
SHAPEFILE_SRID = 4269

_PRJ_SRID = {
    "north_american_1983": 4269,
    "wgs_1984": 4326,
}
_VINTAGE = re.compile(r"cb_(\d{4})_", re.IGNORECASE)
# The national county zip only. The source also fetches the Texas place zip (``cb_<year>_48_place_500k.zip``, read
# from the lake by ``marts/muni_places.py``): it is not a county file, and its ``dt`` must not hide the county zip's
# from ``latest_dt``.
_COUNTY_ZIP = re.compile(r"^cb_\d{4}_us_county_500k(?:__[0-9a-f]{8})?\.zip$", re.IGNORECASE)


def srid_from_prj(prj: str) -> int:
    """EPSG code of a geographic ``.prj`` (Esri WKT), from its datum name."""
    datum = re.search(r'DATUM\["D?_?([^"]+)"', prj)
    if datum is None or re.search(r"PROJCS\[", prj):
        raise ValueError(f"unsupported .prj (expected geographic coordinates): {prj[:120]!r}")
    name = datum.group(1).lower()
    for token, srid in _PRJ_SRID.items():
        if token in name:
            return srid
    raise ValueError(f"unknown datum in .prj: {datum.group(1)!r}")


def _member(z: zipfile.ZipFile, suffix: str, *, required: bool = True) -> bytes | None:
    names = [n for n in z.namelist() if n.lower().endswith(suffix)]
    if len(names) > 1:
        raise ValueError(f"more than one {suffix} in the zip: {names}")
    if not names:
        if required:
            raise ValueError(f"no {suffix} in the zip")
        return None
    return z.read(names[0])


def parse_counties(f: RawFile) -> pl.DataFrame | None:
    with zipfile.ZipFile(io.BytesIO(f.read_bytes())) as z:
        shp, shx, dbf = (_member(z, s) for s in (".shp", ".shx", ".dbf"))
        prj = _member(z, ".prj").decode("utf-8", "replace")
        cpg = _member(z, ".cpg", required=False)
    srid = srid_from_prj(prj)
    if srid != SHAPEFILE_SRID:
        raise ValueError(f"{f.key}: .prj is EPSG:{srid}, the dataset declares EPSG:{SHAPEFILE_SRID}")
    encoding = cpg.decode("ascii", "replace").strip() if cpg else "utf-8"
    reader = shapefile.Reader(shp=io.BytesIO(shp), shx=io.BytesIO(shx), dbf=io.BytesIO(dbf), encoding=encoding)

    fields = {name.lower(): name for name, *_ in reader.fields[1:]}
    missing = {"statefp", "countyfp", "geoid", "name", "aland", "awater"} - set(fields)
    if missing:
        raise ValueError(f"{f.key}: shapefile lacks fields {sorted(missing)}; has {sorted(fields)}")

    rows = []
    for record in reader.iterShapeRecords():
        attrs = {k.lower(): v for k, v in record.record.as_dict().items()}
        if str(attrs["statefp"]).strip() != TEXAS_STATEFP:
            continue
        geometry = record.shape.__geo_interface__
        if geometry.get("type") not in {"Polygon", "MultiPolygon"}:
            raise ValueError(f"{f.key}: county {attrs['geoid']} has geometry {geometry.get('type')}")
        rows.append({
            "county_fips": str(attrs["geoid"]).strip(),
            "county_name": str(attrs["name"]).strip(),
            "county_name_full": str(attrs.get("namelsad") or "").strip() or None,
            "statefp": str(attrs["statefp"]).strip(),
            "countyfp": str(attrs["countyfp"]).strip(),
            "countyns": str(attrs.get("countyns") or "").strip() or None,
            "aland_m2": int(attrs["aland"]),
            "awater_m2": int(attrs["awater"]),
            "geom": geojson_text(geometry),
        })
    if not rows:
        raise ValueError(f"{f.key}: no Texas counties (STATEFP={TEXAS_STATEFP}) in the shapefile")

    vintage = f.meta.get("vintage")
    if vintage is None and (m := _VINTAGE.search(f.name)):
        vintage = m.group(1)
    df = pl.DataFrame(rows, schema_overrides={"aland_m2": pl.Int64, "awater_m2": pl.Int64})
    bad = df.filter(
        ~pl.col("county_fips").str.contains(r"^\d{5}$")
        | (pl.col("county_fips") != pl.col("statefp") + pl.col("countyfp"))
    )
    if bad.height:
        raise ValueError(f"{f.key}: malformed county GEOIDs {bad['county_fips'].to_list()[:5]}")
    if df["county_fips"].is_duplicated().any():
        raise ValueError(f"{f.key}: duplicated county GEOIDs")
    vintage_col = pl.lit(int(vintage) if vintage is not None else None, dtype=pl.Int32).alias("vintage")
    return df.select(pl.exclude("geom"), vintage_col, pl.col("geom")).sort("county_fips")


DATASETS = [
    Dataset(
        name="tx_counties",
        target="postgres",
        mode="replace",
        description="Texas counties (254): FIPS, names, Census land/water area and the 1:500k cartographic "
                    "boundary polygon (EPSG:4326).",
        parse=parse_counties,
        inputs=lambda f: bool(_COUNTY_ZIP.match(f.name)),
        select=latest_dt,
        geometry={"geom": SHAPEFILE_SRID},
    ),
]
