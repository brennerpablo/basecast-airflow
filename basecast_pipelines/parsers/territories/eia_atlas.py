"""Electric Retail Service Territories (HIFLD layer, ORNL-hosted copy frozen 2025-08-21): every utility
polygon intersecting Texas' bounding box, from the GeoJSON pages, plus its county overlay.

``state`` is the utility's mailing state, so utilities headquartered in Oklahoma, New Mexico, Louisiana
or Arkansas that serve Texas appear, as do out-of-state neighbours caught by the bounding box (the county
overlay keeps only real overlaps). ``-999999`` means "no data" in the numeric fields and becomes null;
``NOT AVAILABLE`` becomes null in the text fields. The street address and telephone are dropped. Units of
the peak, capacity and energy fields follow EIA-861 (MW, MWh) per the HIFLD documentation: not verified
in the layer metadata, which carries no units.

SQL datasets (Postgres, need ``tx_counties`` from ``census_tx_counties_geo``):

- ``county_utility_overlap_eia``: county × EIA territory with area shares (equal-area EPSG:3083);
- ``county_utility_overlap``: the PUCT and EIA overlaps together (needs ``puct_ccn_territories``)."""

from __future__ import annotations

import polars as pl

from basecast_pipelines.parsers._geo_common import (
    COUNTY_UTILITY_OVERLAP,
    WGS84,
    clean_text,
    county_overlap_sql,
    esri_date,
    geojson_text,
    read_feature_page,
    split_list,
)
from basecast_pipelines.processing.core import Dataset, RawFile, SqlDataset, latest_dt

SOURCE_ID = "eia_territories"
MISSING = -999999

_TYPES = {
    "COOPERATIVE": "coop",
    "MUNICIPAL": "muni",
    "INVESTOR OWNED": "iou",
    "POLITICAL SUBDIVISION": "political_subdivision",
    "STATE": "state",
    "FEDERAL": "federal",
    "MUNICIPAL MKTG AUTHORITY": "muni_marketing_authority",
    "RETAIL POWER MARKETER": "retail_marketer",
    "WHOLESALE POWER MARKETER": "wholesale_marketer",
    "TRANSMISSION": "transmission",
}

# Output column → source field, for the numeric fields (``-999999`` = no data).
_NUMBERS = {
    "customers": "CUSTOMERS",
    "summer_peak": "SUMMR_PEAK",
    "winter_peak": "WINTR_PEAK",
    "summer_cap": "SUMMER_CAP",
    "winter_cap": "WINTER_CAP",
    "net_gen": "NET_GEN",
    "purchased": "PURCHASED",
    "net_ex": "NET_EX",
    "retail_mwh": "RETAIL_MWH",
    "wholesale_mwh": "WSALE_MWH",
    "total_mwh": "TOTAL_MWH",
    "trans_mwh": "TRANS_MWH",
}


def _number(value) -> float | None:
    if value is None or value == "":
        return None
    number = float(value)
    return None if number == MISSING else number


def _in_ercot(control_area: str | None, planning_area: str | None) -> bool:
    """ERCO among the balancing authorities; wires-only TDSPs (Oncor, CenterPoint, AEP Texas, TNMP) have no
    control area and say ERCOT in the planning area. A planning area of ERCOT next to another control area
    (Entergy Texas: MISO) does not count."""
    if control_area:
        return "ERCO" in [c.upper() for c in split_list(control_area, ",;/")]
    return (planning_area or "").upper() == "ERCOT"


def parse_territory_page(f: RawFile) -> pl.DataFrame | None:
    rows = []
    for feature in read_feature_page(f):
        p = {k.upper(): v for k, v in (feature.get("properties") or {}).items()}
        geometry = feature.get("geometry")
        utility_id = clean_text(p.get("ID"))
        if not geometry or utility_id is None:
            raise ValueError(f"{f.key}: feature {p.get('OBJECTID')} lacks a geometry or an ID")
        raw_type = clean_text(p.get("TYPE"))
        utility_type = _TYPES.get(raw_type.upper()) if raw_type else None
        if raw_type and utility_type is None:
            raise ValueError(f"{f.key}: unknown TYPE {raw_type!r}")
        control_area = clean_text(p.get("CNTRL_AREA"))
        planning_area = clean_text(p.get("PLAN_AREA"))
        year = clean_text(p.get("YEAR"))
        rows.append({
            "utility_id": utility_id,
            "objectid": int(p["OBJECTID"]),
            "name": clean_text(p.get("NAME")),
            "type": raw_type,
            "utility_type": utility_type,
            "state": clean_text(p.get("STATE")),
            "city": clean_text(p.get("CITY")),
            "country": clean_text(p.get("COUNTRY")),
            "holding_company": clean_text(p.get("HOLDING_CO")),
            "control_area": control_area,
            "planning_area": planning_area,
            "in_ercot": _in_ercot(control_area, planning_area),
            "regulated": clean_text(p.get("REGULATED")),
            "naics_code": clean_text(p.get("NAICS_CODE")),
            **{out: _number(p.get(src)) for out, src in _NUMBERS.items()},
            "data_year": int(year) if year and year.isdigit() else None,
            "data_source": clean_text(p.get("SOURCE")),
            "source_date": esri_date(p.get("SOURCEDATE")),
            "val_method": clean_text(p.get("VAL_METHOD")),
            "val_date": esri_date(p.get("VAL_DATE")),
            "website": clean_text(p.get("WEBSITE")),
            "geom": geojson_text(geometry),
        })
    if not rows:
        return None  # an empty trailing page: nothing to add
    schema = {out: pl.Float64 for out in _NUMBERS} | {
        "customers": pl.Float64, "data_year": pl.Int32, "source_date": pl.Date, "val_date": pl.Date,
    }
    return pl.DataFrame(rows, schema_overrides=schema).with_columns(
        pl.col("customers").round(0).cast(pl.Int64),
    )


def _unique_ids(df: pl.DataFrame) -> pl.DataFrame:
    dupes = df.filter(pl.col("utility_id").is_duplicated())["utility_id"].unique().to_list()
    if dupes:
        raise ValueError(f"eia_utility_territories: duplicated utility_id {sorted(dupes)[:10]}")
    return df.sort("utility_id")


DATASETS = [
    Dataset(
        name="eia_utility_territories",
        target="postgres",
        mode="replace",
        description="Electric retail service territories (HIFLD, frozen 2025-08-21) intersecting Texas' bounding "
                    "box, one polygon per EIA utility id, with EIA-861 attributes (2022).",
        parse=parse_territory_page,
        inputs=lambda f: f.suffix == ".geojson" and f.name.startswith("features_"),
        select=latest_dt,
        finalize=_unique_ids,
        geometry={"geom": WGS84},
    ),
]

COUNTY_UTILITY_OVERLAP_EIA_SQL = county_overlap_sql(
    source="eia_territories",
    territories="eia_utility_territories",
    key="t.utility_id",
    utility_id="t.utility_id",
    ccn_no="NULL::text",
    name="t.name",
    utility_type="t.utility_type",
    iso="t.control_area",
    in_ercot="t.in_ercot",
)

SQL_DATASETS = [
    SqlDataset(
        name="county_utility_overlap_eia",
        sql=COUNTY_UTILITY_OVERLAP_EIA_SQL,
        description="Texas county × EIA/HIFLD retail service territory: overlap area and its share of the county "
                    "and of the territory (EPSG:3083).",
        indexes=(("county_fips",), ("utility_id",)),
    ),
    COUNTY_UTILITY_OVERLAP,
]
