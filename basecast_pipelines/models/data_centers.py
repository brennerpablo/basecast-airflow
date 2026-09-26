"""New data-center sites (TCEQ air permits) by territory type and metro status (phase 0, Q4).

``docs/PHASE0_ANALYSIS.md`` §2 Q4 asks whether the data centers permitted since 2025 land in co-op territory
outside the big metros. Everything here works **by county, not by point**: ``tceq_data_center_sites`` has
no coordinates (street addresses are dropped by the parser), so a site takes the territory mix of its whole
county from ``county_utility_overlap_puct``.

The logic is pure functions on DataFrames (tested in ``tests/models/test_data_centers.py``); the ``load_*``
functions are thin read-only queries through ``models.db``.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from basecast_pipelines.models.db import read_sql

TYPES = ("coop", "muni", "iou")
SINCE = date(2025, 1, 1)

# The "big metros" behind docs/decisions.md 2026-09-26 ("37 sites since 2025 … 34 of them outside the big
# metros"). The list is not in the repo: it was typed inline in that session's one-off query (Claude Code
# transcript of 2026-09-26 06:35 UTC) and matched on TCEQ's upper-case county name. It is an ad hoc list,
# not an OMB/MSA definition: it has the DFW, Houston, San Antonio and Austin cores plus some suburbs, and
# leaves out e.g. Bell (Killeen–Temple), Lubbock, El Paso, Montgomery and Galveston.
LEGACY_METRO_COUNTIES = frozenset(
    {
        "DALLAS",
        "TARRANT",
        "COLLIN",
        "DENTON",
        "ELLIS",
        "HARRIS",
        "BEXAR",
        "TRAVIS",
        "WILLIAMSON",
        "FORT BEND",
        "ROCKWALL",
        "KAUFMAN",
        "HAYS",
    }
)

# Fallback metro rule, explicit because it is ours: county population (Census PEP) per km² of land.
# 100/km² (~259/sq mi) takes the 29 densest Texas counties in PEP vintage 2025.
DENSITY_THRESHOLD_PER_KM2 = 100.0

COOP_SHARE_FOR_VIDEO = 0.60


def select_new_sites(sites: pl.DataFrame, since: date = SINCE) -> pl.DataFrame:
    """Sites whose earliest known permit date is on or after ``since``.

    Sites with no known date at all (``first_affil_begin_dt`` null) are left out; the caller reports them
    apart, together with the selected sites that also carry an undated row (``has_undated_affiliation``).
    """
    return sites.filter(pl.col("first_affil_begin_dt") >= since).sort("first_affil_begin_dt", "ref_num_txt")


def fully_undated_sites(sites: pl.DataFrame) -> pl.DataFrame:
    """Sites with no dated permit row at all: they cannot be placed before or after ``SINCE``."""
    return sites.filter(pl.col("first_affil_begin_dt").is_null()).sort("ref_num_txt")


def county_territory_types(overlap: pl.DataFrame) -> pl.DataFrame:
    """One row per county: the area and the county_share weight of each territory type, and the largest type.

    ``overlap`` has one row per (county, CCN territory) with ``utility_type`` in ``TYPES``, ``overlap_km2``
    and ``county_share``. PUCT territories overlap (dual certification) and leave some land uncovered, so a
    county's shares do not sum to 1: the weights ``<type>_w`` are normalized by the county's own sum
    (``share_sum`` keeps the raw sum, ``<type>_raw`` the raw per-type sum, whose value clipped to 1 is an upper
    bound on the land the type covers). ``largest_type`` is the type with the most summed overlap area; ties
    go to the order of ``TYPES``.
    """
    typed = overlap.filter(pl.col("utility_type").is_in(TYPES))
    wide = typed.group_by("county_fips").agg(
        *[
            pl.col("overlap_km2").filter(pl.col("utility_type") == t).sum().alias(f"{t}_km2")
            for t in TYPES
        ],
        *[
            pl.col("county_share").filter(pl.col("utility_type") == t).sum().alias(f"{t}_raw")
            for t in TYPES
        ],
        pl.col("county_share").sum().alias("share_sum"),
    )
    areas = pl.concat_list([pl.col(f"{t}_km2") for t in TYPES])
    largest = pl.concat_list(
        [pl.when(pl.col(f"{t}_km2") == areas.list.max()).then(pl.lit(t)) for t in TYPES]
    ).list.drop_nulls().list.first()
    return wide.select(
        "county_fips",
        *[f"{t}_km2" for t in TYPES],
        largest.alias("largest_type"),
        *[
            pl.when(pl.col("share_sum") > 0)
            .then(pl.col(f"{t}_raw") / pl.col("share_sum"))
            .otherwise(None)
            .alias(f"{t}_w")
            for t in TYPES
        ],
        *[f"{t}_raw" for t in TYPES],
        "share_sum",
    ).sort("county_fips")


def classify_sites(sites: pl.DataFrame, county_types: pl.DataFrame) -> pl.DataFrame:
    """Attach each site's county territory mix (by county, not by point); unmatched counties stay null."""
    return sites.join(county_types, on="county_fips", how="left")


def city_muni_match(sites: pl.DataFrame, overlap: pl.DataFrame) -> pl.DataFrame:
    """Add ``city_muni``: a muni of the site's county whose name contains the site's city, else null.

    A cheap point-level hint that the county attribution can miss (a site inside a city with its own
    utility). Name containment only; whether the site is inside the muni's territory is not verified.
    """
    munis = overlap.filter(pl.col("utility_type") == "muni").select(
        "county_fips", pl.col("utility_name").alias("city_muni")
    )
    city = pl.col("city").str.to_uppercase().str.strip_chars()
    matched = (
        sites.select("ref_num_txt", "county_fips", "city")
        .filter(city.is_not_null() & (city != ""))
        .join(munis, on="county_fips", how="inner")
        .filter(pl.col("city_muni").str.to_uppercase().str.contains(city, literal=True))
        .group_by("ref_num_txt")
        .agg(pl.col("city_muni").sort().first())
    )
    return sites.join(matched, on="ref_num_txt", how="left")


def type_summary(classified: pl.DataFrame) -> pl.DataFrame:
    """Sites per territory type, two ways: largest-area type (count) and county_share weights (sum).

    Shares are over all sites, so a site whose county has no territory row counts in the denominator
    as unclassified (it is listed in the ``unclassified`` row).
    """
    rows = []
    for t in TYPES:
        by_area = classified.filter(pl.col("largest_type") == t).height
        weighted = float(classified[f"{t}_w"].fill_null(0).sum())
        rows.append({"type": t, "sites_largest_area": by_area, "sites_weighted": weighted})
    missing = classified.filter(pl.col("largest_type").is_null()).height
    rows.append({"type": "unclassified", "sites_largest_area": missing, "sites_weighted": float(missing)})
    schema = {"type": pl.String, "sites_largest_area": pl.Int64, "sites_weighted": pl.Float64}
    n = max(classified.height, 1)
    return pl.DataFrame(rows, schema=schema).with_columns(
        (pl.col("sites_largest_area") / n).alias("share_largest_area"),
        (pl.col("sites_weighted") / n).alias("share_weighted"),
    )


def county_density(population: pl.DataFrame, counties: pl.DataFrame) -> pl.DataFrame:
    """People per km² of land: ``population`` (county_fips, population) ÷ ``counties`` (… aland_m2)."""
    return counties.join(population, on="county_fips", how="inner").select(
        "county_fips",
        "population",
        (pl.col("aland_m2") / 1e6).alias("land_km2"),
        (pl.col("population") / (pl.col("aland_m2") / 1e6)).alias("density_per_km2"),
    )


def flag_metro(
    sites: pl.DataFrame,
    density: pl.DataFrame,
    *,
    threshold: float = DENSITY_THRESHOLD_PER_KM2,
    legacy_counties: frozenset[str] = LEGACY_METRO_COUNTIES,
) -> pl.DataFrame:
    """Add ``metro_legacy`` (the decision's 13-county list, by upper-case name) and ``metro_density``.

    ``metro_density`` is null when the county has no density row.
    """
    name = pl.col("county_name").str.to_uppercase().str.strip_chars()
    return sites.join(density.select("county_fips", "density_per_km2"), on="county_fips", how="left").with_columns(
        name.is_in(sorted(legacy_counties)).alias("metro_legacy"),
        (pl.col("density_per_km2") >= threshold).alias("metro_density"),
    )


def decide(
    coop_weighted_share: float, outside_metro_share: float, *, coop_min: float = COOP_SHARE_FOR_VIDEO
) -> str:
    """The handoff rule: ≥ 60% of sites (weighted) in co-op territory **and** most outside the metros."""
    return "in video" if coop_weighted_share >= coop_min and outside_metro_share > 0.5 else "signal only"


# --- thin loaders (read-only) -------------------------------------------------------------------------------


def load_sites() -> pl.DataFrame:
    return read_sql(
        """
        SELECT ref_num_txt, reg_ent_name, county_name, county_fips, city, first_affil_begin_dt,
               has_undated_affiliation, has_pending, has_active, statuses, matched_by_name, matched_by_naics
        FROM tceq_data_center_sites
        """
    )


def load_overlap() -> pl.DataFrame:
    return read_sql(
        """
        SELECT county_fips, utility_name, utility_type, iso_rto, in_ercot, overlap_km2, county_share
        FROM county_utility_overlap_puct
        """
    )


def load_population(year: int = 2025, series: str = "postcensal_v2025") -> pl.DataFrame:
    return read_sql(
        """
        SELECT county_fips, value AS population
        FROM census_population_county
        WHERE series = %(series)s AND measure = 'population' AND year = %(year)s
        """,
        {"series": series, "year": year},
    )


def load_counties() -> pl.DataFrame:
    return read_sql("SELECT county_fips, county_name AS county, aland_m2 FROM tx_counties")


def load_iso_share() -> pl.DataFrame:
    return read_sql("SELECT county_fips, ercot_only_share, dominant_iso_rto FROM county_iso_share")
