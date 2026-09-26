"""Census 2020 ZCTA ↔ county relationship file (national, pipe-delimited), Texas counties only, and the
county → ERCOT weather zone table derived from it (KICKOFF A1).

One row per (ZCTA, county) part with land and water areas in m². Rows with an empty ZCTA are the parts of
a county that lie in no ZCTA (kept with ``zcta5`` null, so a county's parts add up to its land area). The
record layout PDF is not parsed: headers are read from the file itself.

``county_weather_zone`` applies the largest-area rule: each ZCTA takes the weather zone of the ZIP code
with the same five digits in ERCOT's ZipToZone sheet (ZIP ≈ ZCTA is an approximation: ZCTAs are built
from ZIP delivery areas but are not ZIP codes, and PO-box-only ZIPs have no ZCTA), a county's ZCTA land
area is summed per zone, and the zone with the most land wins (``ziptozone_zone``). Land in ZCTAs absent
from ZipToZone is ``share_outside``.

ZipToZone lists every ZIP "identified as having ESI IDs served by ERCOT", so it also covers ZIPs that are
mostly outside ERCOT (El Paso, Beaumont, Texarkana). ``in_ercot`` therefore also asks the PUCT territories
(``county_iso_share``, built by the ``puct_ccn_territories`` parser) whether utilities that list ERCOT
cover most of the county; ``weather_zone`` is ``ziptozone_zone`` for counties in ERCOT and null elsewhere.
Use ``weather_zone`` to map county-level signals to zones, and ``ziptozone_zone`` to place an ERCOT
project that sits in a border county. The dataset is declared here and in the PUCT parser, so it is
rebuilt when either input changes and skipped until both exist."""

from __future__ import annotations

import io
import re

import polars as pl

from basecast_pipelines.processing.core import Dataset, RawFile, SqlDataset, latest_dt

SOURCE_ID = "census_zcta_county"
TEXAS_STATEFP = "48"
WEATHER_ZONES = ("COAST", "EAST", "FWEST", "NORTH", "NCENT", "SOUTH", "SCENT", "WEST")

# Output column → regex over the file's header (the ``_20`` suffix is the vintage).
_COLUMNS = {
    "zcta5": r"^GEOID_ZCTA5_\d{2}$",
    "county_fips": r"^GEOID_COUNTY_\d{2}$",
    "county_name_full": r"^NAMELSAD_COUNTY_\d{2}$",
    "zcta_land_m2": r"^AREALAND_ZCTA5_\d{2}$",
    "zcta_water_m2": r"^AREAWATER_ZCTA5_\d{2}$",
    "county_land_m2": r"^AREALAND_COUNTY_\d{2}$",
    "county_water_m2": r"^AREAWATER_COUNTY_\d{2}$",
    "part_land_m2": r"^AREALAND_PART$",
    "part_water_m2": r"^AREAWATER_PART$",
}
_AREAS = [c for c in _COLUMNS if c.endswith("_m2")]


def _find(header: list[str], pattern: str, key: str) -> str:
    matches = [h for h in header if re.match(pattern, h.strip(), re.IGNORECASE)]
    if len(matches) != 1:
        raise ValueError(f"{key}: expected one header matching {pattern!r}, found {matches} in {header}")
    return matches[0]


def parse_zcta_county(f: RawFile) -> pl.DataFrame | None:
    text = f.read_bytes().decode("utf-8-sig")
    raw = pl.read_csv(io.StringIO(text), separator="|", infer_schema=False, quote_char=None)
    cols = {out: _find(raw.columns, pattern, f.key) for out, pattern in _COLUMNS.items()}
    vintage = 2000 + int(re.search(r"_(\d{2})$", cols["county_fips"]).group(1))

    def blank_to_null(name: str) -> pl.Expr:
        s = pl.col(name).str.strip_chars()
        return pl.when(s == "").then(None).otherwise(s)

    df = raw.select(
        blank_to_null(cols["zcta5"]).alias("zcta5"),
        blank_to_null(cols["county_fips"]).alias("county_fips"),
        blank_to_null(cols["county_name_full"]).alias("county_name_full"),
        *(blank_to_null(cols[c]).cast(pl.Int64).alias(c) for c in _AREAS),
    ).filter(pl.col("county_fips").str.starts_with(TEXAS_STATEFP))
    if not df.height:
        raise ValueError(f"{f.key}: no Texas counties in the relationship file")
    bad = df.filter(
        ~pl.col("county_fips").str.contains(r"^\d{5}$")
        | (pl.col("zcta5").is_not_null() & ~pl.col("zcta5").str.contains(r"^\d{5}$"))
    )
    if bad.height:
        raise ValueError(f"{f.key}: malformed ZCTA or county codes, e.g. {bad.head(3).to_dicts()}")
    return df.with_columns(
        (pl.col("part_land_m2") / pl.col("county_land_m2")).alias("share_of_county_land"),
        (pl.col("part_land_m2") / pl.col("zcta_land_m2")).alias("share_of_zcta_land"),
        pl.lit(vintage, dtype=pl.Int32).alias("vintage"),
    ).sort("county_fips", "zcta5", nulls_last=True)


DATASETS = [
    Dataset(
        name="census_zcta_county",
        target="postgres",
        mode="replace",
        description="Census 2020 ZCTA ↔ county parts for Texas counties, with ZCTA, county and part land/water "
                    "areas (m²); zcta5 is null for county land outside any ZCTA.",
        parse=parse_zcta_county,
        inputs=lambda f: f.suffix == ".txt",
        select=latest_dt,
    ),
]


def _zone_shares() -> str:
    return ",\n       ".join(
        f"coalesce(sum(land_m2) FILTER (WHERE zone = '{z}'), 0) / NULLIF(t.total_m2, 0) AS share_{z.lower()}"
        for z in WEATHER_ZONES
    )


# A county is in ERCOT when both sources say so for most of it: ERCOT lists ESI IDs in most of its ZCTA
# land (ZipToZone), and PUCT territories whose ISO includes ERCOT cover most of its area. ZipToZone alone
# keeps El Paso, Beaumont and Texarkana; the PUCT field alone cannot split co-ops that list two ISOs.
IN_ERCOT = "(iso.ercot_listed_share >= 0.5 AND s.share_outside < 0.5)"

COUNTY_WEATHER_ZONE_SQL = f"""
WITH county AS (
    SELECT county_fips, regexp_replace(min(county_name_full), ' County$', '') AS county_name,
           min(county_land_m2) AS county_land_m2,
           min(source_file) AS zcta_source_file
    FROM census_zcta_county
    GROUP BY county_fips
),
part AS (
    SELECT p.county_fips, p.zcta5, p.part_land_m2 AS land_m2, z.weather_zone AS zone
    FROM census_zcta_county p
    LEFT JOIN ercot_zip_weather_zone z ON z.zip_code = p.zcta5
    WHERE p.zcta5 IS NOT NULL
),
totals AS (
    SELECT county_fips, sum(land_m2) AS total_m2, count(*) AS n_zctas, count(zone) AS n_zctas_matched
    FROM part
    GROUP BY county_fips
),
ranked AS (
    SELECT p.county_fips, p.zone, sum(p.land_m2) / NULLIF(t.total_m2, 0) AS share,
           row_number() OVER (PARTITION BY p.county_fips ORDER BY sum(p.land_m2) DESC, p.zone) AS rk
    FROM part p JOIN totals t USING (county_fips)
    WHERE p.zone IS NOT NULL
    GROUP BY p.county_fips, p.zone, t.total_m2
),
shares AS (
    SELECT p.county_fips,
       {_zone_shares()},
       coalesce(sum(land_m2) FILTER (WHERE zone IS NULL), 0) / NULLIF(t.total_m2, 0) AS share_outside,
       count(DISTINCT zone) FILTER (WHERE land_m2 > 0) AS n_zones
    FROM part p JOIN totals t USING (county_fips)
    GROUP BY p.county_fips, t.total_m2
),
zip_source AS (
    SELECT min(source_file) AS zip_zone_source_file FROM ercot_zip_weather_zone
)
SELECT c.county_fips,
       c.county_name,
       CASE WHEN {IN_ERCOT} THEN top.zone END AS weather_zone,
       coalesce({IN_ERCOT}, false) AS in_ercot,
       top.zone AS ziptozone_zone,
       coalesce(top.share, 0) AS top_share,
       second.zone AS second_zone,
       coalesce(second.share, 0) AS second_share,
       coalesce(s.n_zones, 0) AS n_zones,
       {", ".join(f"s.share_{z.lower()}" for z in WEATHER_ZONES)},
       s.share_outside,
       iso.ercot_listed_share AS puct_ercot_listed_share,
       iso.ercot_only_share AS puct_ercot_only_share,
       iso.non_ercot_share AS puct_non_ercot_share,
       coalesce(t.n_zctas, 0) AS n_zctas,
       coalesce(t.n_zctas_matched, 0) AS n_zctas_matched,
       t.total_m2 / 1e6 AS zcta_land_km2,
       c.county_land_m2 / 1e6 AS county_land_km2,
       'zone: largest ZCTA land area, ZIP (ERCOT ZipToZone) ~ ZCTA (Census 2020); in_ercot: most ZCTA land '
       'in ZipToZone and most county area in PUCT territories listing ERCOT'::text AS method,
       c.zcta_source_file,
       zs.zip_zone_source_file,
       now() AS built_at
FROM county c
CROSS JOIN zip_source zs
LEFT JOIN totals t USING (county_fips)
LEFT JOIN shares s USING (county_fips)
LEFT JOIN county_iso_share iso USING (county_fips)
LEFT JOIN ranked top ON top.county_fips = c.county_fips AND top.rk = 1
LEFT JOIN ranked second ON second.county_fips = c.county_fips AND second.rk = 2
ORDER BY c.county_fips
"""

COUNTY_WEATHER_ZONE = SqlDataset(
    name="county_weather_zone",
    sql=COUNTY_WEATHER_ZONE_SQL,
    description="Texas county → ERCOT weather zone by the largest ZCTA land area (ZIP ≈ ZCTA), with the share of "
                "the county's ZCTA land in each zone; null zone and in_ercot = false outside ERCOT (PUCT territories).",
    indexes=(("county_fips",), ("weather_zone",)),
)

SQL_DATASETS = [COUNTY_WEATHER_ZONE]
