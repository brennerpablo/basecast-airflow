"""Helpers shared by the geometry parsers: GeoJSON pages saved from ArcGIS feature layers, geometry as
GeoJSON text (the writer turns it into PostGIS), and the SQL snippets the overlay datasets share.

No geometry library here on purpose: validity, areas and overlays are computed in PostGIS."""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timezone
from typing import Any

from basecast_pipelines.processing.core import RawFile, SqlDataset

# Coordinates the ArcGIS pages were requested in (``outSR=4326``).
WGS84 = 4326
# NAD83 / Texas Centric Albers Equal Area: areas in m² for the overlays.
EQUAL_AREA_SRID = 3083
# County boundaries differ slightly between sources (1:500k cartographic vs TIGER-based territories), which
# leaves slivers along shared edges. An overlap is kept when it is at least this share of the county or of
# the territory (so a small town's utility inside a large county stays).
SLIVER_COUNTY_SHARE = 0.001
SLIVER_TERRITORY_SHARE = 0.01

_CRS_4326 = re.compile(r"(EPSG:+4326|CRS84)$", re.IGNORECASE)


def geojson_text(geometry: dict[str, Any]) -> str:
    """Compact GeoJSON text for a geometry object (tuples become arrays)."""
    return json.dumps(geometry, separators=(",", ":"))


def read_feature_page(f: RawFile) -> list[dict[str, Any]]:
    """The features of one GeoJSON page from an ArcGIS ``query?f=geojson``, checking it is a
    FeatureCollection in WGS84. An ArcGIS error payload (``{"error": ...}``) raises."""
    payload = json.loads(f.read_bytes())
    if "error" in payload:
        raise ValueError(f"{f.key}: ArcGIS error payload {payload['error']}")
    if payload.get("type") != "FeatureCollection":
        raise ValueError(f"{f.key}: not a GeoJSON FeatureCollection")
    crs = ((payload.get("crs") or {}).get("properties") or {}).get("name")
    if crs is not None and not _CRS_4326.search(str(crs)):
        raise ValueError(f"{f.key}: expected WGS84 coordinates, got CRS {crs!r}")
    return payload.get("features") or []


def esri_date(value: Any) -> date | None:
    """ArcGIS date fields come as epoch milliseconds (UTC)."""
    if value in (None, ""):
        return None
    return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).date()


def clean_text(value: Any, *, nulls: tuple[str, ...] = ("", "NONE", "NA", "N/A", "NOT AVAILABLE")) -> str | None:
    """Stripped text, with the placeholder values these layers use turned into null."""
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return None if text.upper() in nulls else text


def split_list(value: str | None, separators: str) -> list[str]:
    """``"30028,30170"`` → ``["30028", "30170"]`` (empty parts dropped)."""
    if not value:
        return []
    return [part.strip() for part in re.split(f"[{re.escape(separators)}]", value) if part.strip()]


# --- SQL snippets for the overlays (PostGIS) ---------------------------------------------------------

def equal_area(column: str) -> str:
    """A geometry in the equal-area projection, repaired when invalid (after projecting, since projecting
    can itself break validity) and reduced to its polygons. ``ST_MakeValid``'s default (linework) method
    reads rings even-odd, which matches the areas ArcGIS reports for layers that write holes as separate
    polygon parts ("nested shells"); valid inputs are returned unchanged."""
    return f"ST_CollectionExtract(ST_MakeValid(ST_Transform({column}, {EQUAL_AREA_SRID})), 3)"


def county_overlap_sql(*, source: str, territories: str, key: str, utility_id: str, ccn_no: str,
                       name: str, utility_type: str, iso: str, in_ercot: str) -> str:
    """County × territory overlap for one territory table: the share of each county's area inside the
    territory and of the territory's area inside the county, in an equal-area projection, without the
    boundary slivers (see ``SLIVER_COUNTY_SHARE``).

    The keyword arguments are SQL expressions over the territory table (alias ``t``)."""
    return f"""
WITH county AS MATERIALIZED (
    SELECT c.county_fips, c.county_name,
           {equal_area('c.geom')} AS g
    FROM tx_counties c
),
county_area AS MATERIALIZED (
    SELECT county_fips, county_name, g, ST_Area(g) AS area_m2 FROM county
),
territory AS MATERIALIZED (
    SELECT {key} AS territory_key, {utility_id} AS utility_id, {ccn_no} AS ccn_no, {name} AS utility_name,
           {utility_type} AS utility_type, {iso} AS iso_rto, {in_ercot} AS in_ercot,
           {equal_area('t.geom')} AS g,
           t.source_file
    FROM {territories} t
),
territory_area AS MATERIALIZED (
    SELECT territory.*, ST_Area(g) AS area_m2 FROM territory
),
pairs AS (
    SELECT c.county_fips, c.county_name, c.area_m2 AS county_area_m2,
           t.territory_key, t.utility_id, t.ccn_no, t.utility_name, t.utility_type, t.iso_rto, t.in_ercot,
           t.area_m2 AS territory_area_m2, t.source_file,
           CASE WHEN ST_CoveredBy(c.g, t.g) THEN c.area_m2
                WHEN ST_CoveredBy(t.g, c.g) THEN t.area_m2
                ELSE ST_Area(ST_Intersection(c.g, t.g)) END AS overlap_m2
    FROM county_area c
    JOIN territory_area t ON ST_Intersects(c.g, t.g)
)
SELECT '{source}'::text AS source, county_fips, county_name, territory_key, utility_id, ccn_no, utility_name,
       utility_type, iso_rto, in_ercot,
       overlap_m2 / 1e6 AS overlap_km2,
       county_area_m2 / 1e6 AS county_area_km2,
       territory_area_m2 / 1e6 AS territory_area_km2,
       overlap_m2 / NULLIF(county_area_m2, 0) AS county_share,
       overlap_m2 / NULLIF(territory_area_m2, 0) AS territory_share,
       'ST_Intersection in EPSG:{EQUAL_AREA_SRID}'::text AS method,
       source_file AS territory_source_file,
       now() AS built_at
FROM pairs
WHERE overlap_m2 >= {SLIVER_COUNTY_SHARE} * county_area_m2
   OR overlap_m2 >= {SLIVER_TERRITORY_SHARE} * territory_area_m2
"""


_OVERLAP_COLUMNS = (
    "source, county_fips, county_name, territory_key, utility_id, ccn_no, utility_name, utility_type, iso_rto, "
    "in_ercot, overlap_km2, county_area_km2, territory_area_km2, county_share, territory_share, method, "
    "territory_source_file, built_at"
)

# Both sources side by side; declared by both territory parsers, so it is rebuilt when either changes (and
# skipped until both per-source tables exist).
COUNTY_UTILITY_OVERLAP_SQL = f"""
SELECT {_OVERLAP_COLUMNS} FROM county_utility_overlap_puct
UNION ALL
SELECT {_OVERLAP_COLUMNS} FROM county_utility_overlap_eia
ORDER BY county_fips, source, county_share DESC
"""

COUNTY_UTILITY_OVERLAP = SqlDataset(
    name="county_utility_overlap",
    sql=COUNTY_UTILITY_OVERLAP_SQL,
    description="Texas county × utility territory overlap from both sources (source = puct_ccn | eia_territories).",
    indexes=(("county_fips",), ("source", "territory_key")),
)
