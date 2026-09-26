"""PUCT electric CCN service areas: co-op, municipal and IOU polygons from the PUCT's Electric Service
Territory viewer (three ArcGIS layers saved as GeoJSON pages), plus the county overlays built on them.

The layers call themselves unofficial (the official maps are county mylars at PUCT Central Records) and
their areas overlap (dual certification), so a county's utility shares can add up to more than 1. Company
phone numbers and outage/efficiency links are dropped; nothing here is about individuals.

SQL datasets (Postgres, need ``tx_counties`` from ``census_tx_counties_geo``):

- ``county_utility_overlap_puct``: county × PUCT territory with area shares (equal-area EPSG:3083);
- ``county_iso_share``: share of each county's area in territories whose ISO field is ERCOT only, ERCOT
  and another ISO (mixed), or not ERCOT; with ZipToZone it decides ``county_weather_zone.in_ercot``;
- ``county_utility_overlap``: the PUCT and EIA overlaps together (needs ``eia_territories`` processed);
- ``county_weather_zone``: rebuilt here too, because its ``in_ercot`` comes from ``county_iso_share``."""

from __future__ import annotations

import json
import re
from datetime import date, datetime

import polars as pl

from basecast_pipelines.parsers._geo_common import (
    COUNTY_UTILITY_OVERLAP,
    EQUAL_AREA_SRID,
    WGS84,
    clean_text,
    county_overlap_sql,
    equal_area,
    esri_date,
    geojson_text,
    read_feature_page,
    split_list,
)
from basecast_pipelines.parsers.census.zcta_county import COUNTY_WEATHER_ZONE
from basecast_pipelines.processing.core import Dataset, RawFile, SqlDataset, latest_dt

SOURCE_ID = "puct_ccn_territories"

_TYPES = {"COOP-DIST": "coop", "COOP": "coop", "MUNI": "muni", "IOU": "iou"}
_LAYER = re.compile(r"^(?P<layer>[a-z_]+?)_features_\d+\.geojson$", re.IGNORECASE)


def _date(value) -> date | None:
    text = clean_text(value, nulls=("", "NONE", "NA", "N/A", "UNKNOWN"))
    if text is None:
        return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"unexpected DATA_SOURCE_DATE {text!r}")


def _layer_edit_date(f: RawFile, layer: str) -> date | None:
    """``dataLastEditDate`` from the layer's metadata file saved next to the page (null if absent)."""
    key = f.key.rsplit("/", 1)[0] + f"/{layer.lower()}_layer.json"
    if not f.storage.exists(key):
        return None
    info = json.loads(f.storage.read_bytes(key)).get("editingInfo") or {}
    return esri_date(info.get("dataLastEditDate") or info.get("lastEditDate"))


def parse_ccn_page(f: RawFile) -> pl.DataFrame | None:
    layer = f.meta.get("layer") or (m.group("layer").upper() if (m := _LAYER.match(f.name)) else None)
    if layer is None:
        raise ValueError(f"{f.key}: cannot tell which layer (COOP_DIST, MUNI, IOU) the page belongs to")
    rows = []
    for feature in read_feature_page(f):
        p = {k.upper(): v for k, v in (feature.get("properties") or {}).items()}
        geometry = feature.get("geometry")
        if not geometry:
            raise ValueError(f"{f.key}: feature {p.get('OBJECTID')} has no geometry")
        company_type = clean_text(p.get("COMPANY_TYPE"))
        utility_type = _TYPES.get((company_type or "").upper())
        if utility_type is None:
            raise ValueError(f"{f.key}: unknown COMPANY_TYPE {company_type!r}")
        ccn_numbers = [c for c in split_list(clean_text(p.get("CCN_NO")), ",;/") if re.fullmatch(r"\d+", c)]
        iso = clean_text(p.get("INDEPENDENT_SYSTEM_OPERATOR_REGIONAL_TRANSMISSION_ORGANIZATION"))
        isos = [i.upper() for i in split_list(iso, ",;/")]
        gt = clean_text(p.get("GENERATION_TRANSMISSION_COOPERATIVE"))
        rows.append({
            "territory_id": f"{layer.lower()}_{int(p['OBJECTID'])}",
            "layer": layer.upper(),
            "objectid": int(p["OBJECTID"]),
            "ccn_no": ccn_numbers[0] if ccn_numbers else None,
            "ccn_numbers": json.dumps(ccn_numbers),
            "company_name": clean_text(p.get("COMPANY_NAME")),
            "company_abbreviation": clean_text(p.get("COMPANY_ABBREVIATION")),
            "company_type": company_type,
            "utility_type": utility_type,
            "iso_rto": ",".join(isos) or None,
            "in_ercot": "ERCOT" in isos,
            "ercot_only": isos == ["ERCOT"],
            "gt_cooperative": gt,
            "gt_cooperatives": json.dumps(split_list(gt, ";")),
            "company_website": clean_text(p.get("COMPANY_WEBSITE")),
            "comments": clean_text(p.get("COMMENTS")),
            "data_source": clean_text(p.get("DATA_SOURCE")),
            "data_source_date": _date(p.get("DATA_SOURCE_DATE")),
            "geom": geojson_text(geometry),
        })
    if not rows:
        return None  # an empty trailing page: nothing to add
    return pl.DataFrame(rows, schema_overrides={"data_source_date": pl.Date}).with_columns(
        pl.lit(_layer_edit_date(f, layer), dtype=pl.Date).alias("layer_edited_on"),
    ).select(pl.exclude("geom"), pl.col("geom"))


DATASETS = [
    Dataset(
        name="puct_ccn_territories",
        target="postgres",
        mode="replace",
        description="PUCT electric CCN service areas (co-op, muni, IOU polygons; unofficial per the PUCT) with "
                    "CCN numbers, ISO/RTO and G&T co-op.",
        parse=parse_ccn_page,
        inputs=lambda f: f.suffix == ".geojson" and "_features_" in f.name,
        select=latest_dt,
        geometry={"geom": WGS84},
    ),
]


COUNTY_UTILITY_OVERLAP_PUCT_SQL = county_overlap_sql(
    source="puct_ccn",
    territories="puct_ccn_territories",
    key="t.territory_id",
    utility_id="NULL::text",
    ccn_no="t.ccn_no",
    name="t.company_name",
    utility_type="t.utility_type",
    iso="t.iso_rto",
    in_ercot="t.in_ercot",
)

# Union per ISO class and county, so overlapping territories (dual certification) are not counted twice.
# The ISO field is per utility and many co-ops list two ISOs ("mixed"), so it cannot say on its own which
# part of a mixed territory is in ERCOT; county_weather_zone combines it with ERCOT's ZipToZone list.
COUNTY_ISO_SHARE_SQL = f"""
WITH county AS MATERIALIZED (
    SELECT county_fips, county_name, {equal_area('geom')} AS g FROM tx_counties
),
territory AS MATERIALIZED (
    SELECT CASE WHEN ercot_only THEN 'ercot' WHEN in_ercot THEN 'mixed' ELSE 'non_ercot' END AS iso_class,
           iso_rto, {equal_area('geom')} AS g
    FROM puct_ccn_territories
),
clipped AS MATERIALIZED (
    SELECT c.county_fips, t.iso_class, t.iso_rto, ST_Intersection(c.g, t.g) AS g
    FROM county c JOIN territory t ON ST_Intersects(c.g, t.g)
),
by_county AS (
    SELECT county_fips,
           ST_Area(ST_Union(g) FILTER (WHERE iso_class = 'ercot')) AS ercot_m2,
           ST_Area(ST_Union(g) FILTER (WHERE iso_class = 'mixed')) AS mixed_m2,
           ST_Area(ST_Union(g) FILTER (WHERE iso_class = 'non_ercot')) AS non_ercot_m2,
           ST_Area(ST_Union(g) FILTER (WHERE iso_class <> 'non_ercot')) AS ercot_listed_m2,
           ST_Area(ST_Union(g)) AS covered_m2
    FROM clipped GROUP BY county_fips
),
by_iso AS (
    SELECT county_fips, iso_rto,
           row_number() OVER (PARTITION BY county_fips ORDER BY ST_Area(ST_Union(g)) DESC, iso_rto) AS rk
    FROM clipped GROUP BY county_fips, iso_rto
)
SELECT c.county_fips, c.county_name,
       coalesce(b.ercot_m2, 0) / ST_Area(c.g) AS ercot_only_share,
       coalesce(b.mixed_m2, 0) / ST_Area(c.g) AS mixed_share,
       coalesce(b.non_ercot_m2, 0) / ST_Area(c.g) AS non_ercot_share,
       coalesce(b.ercot_listed_m2, 0) / ST_Area(c.g) AS ercot_listed_share,
       coalesce(b.covered_m2, 0) / ST_Area(c.g) AS covered_share,
       i.iso_rto AS dominant_iso_rto,
       'union of PUCT CCN territories per ISO class in EPSG:{EQUAL_AREA_SRID}; ercot_listed = territories whose '
       'ISO field includes ERCOT (ERCOT only or mixed)'::text AS method,
       now() AS built_at
FROM county c
LEFT JOIN by_county b USING (county_fips)
LEFT JOIN by_iso i ON i.county_fips = c.county_fips AND i.rk = 1
ORDER BY c.county_fips
"""


SQL_DATASETS = [
    SqlDataset(
        name="county_utility_overlap_puct",
        sql=COUNTY_UTILITY_OVERLAP_PUCT_SQL,
        description="Texas county × PUCT CCN territory: overlap area and its share of the county and of the "
                    "territory (EPSG:3083).",
        indexes=(("county_fips",), ("territory_key",)),
    ),
    SqlDataset(
        name="county_iso_share",
        sql=COUNTY_ISO_SHARE_SQL,
        description="Share of each Texas county's area in PUCT territories whose ISO is ERCOT only, ERCOT and "
                    "another ISO (mixed), or not ERCOT; overlapping territories are unioned.",
        indexes=(("county_fips",),),
    ),
    COUNTY_UTILITY_OVERLAP,
    COUNTY_WEATHER_ZONE,
]
