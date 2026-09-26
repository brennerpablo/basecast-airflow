"""Census place (city) signals for the municipal utilities (X10, ``docs/analysis/x10_muni_places.md``).

A muni's territory is roughly its city, so place-level Census data (PEP sub-county population, Building
Permits Survey by place, ACS 5-year B25032 by place) should describe it better than county data apportioned by
area. This module holds the pure pieces: parsers for the three Census files, the muni → place crosswalk, the
place signals, and the place / city trigger mappings. Nothing here downloads or writes; the analysis script
fetches the files into ``analysis/out/x10_cache/`` and reads the database read-only.

Validation lock: nothing here knows which accounts are Base's partners; the script holds them out before any
function is called (as ``analysis/q3_signals.py``).
"""

from __future__ import annotations

import csv
import io
import json
import re
from collections.abc import Iterable
from datetime import date

import polars as pl

from basecast_pipelines.models.accounts import core_name
from basecast_pipelines.models.triggers import EVENT_COLUMNS

STATE_FIPS = "48"
INCORPORATED_PLACE = "162"  # SUB-EST2025 SUMLEV key (SUB-EST2025.pdf)
UNINCORPORATED = "99990"  # BPS "FIPS Place" of a county's unincorporated area
_PLACE_SUFFIX = re.compile(r"\s+(city|town|village|cdp)\s*$", re.IGNORECASE)
_CITY_GOV = re.compile(r"\b(city|town|village)\b", re.IGNORECASE)
# Overlap rule for the crosswalk check (Claude's thresholds, see the doc).
CONFIRM_SHARE = 0.5  # ≥ half of the place in the CCN territory, or half of the territory in the place
PARTIAL_SHARE = 0.1


# --- names ---------------------------------------------------------------------------------------------------


def place_core(name: str | None) -> str:
    """``'Boerne city'`` → ``'boerne'``; ``'La Grange city'`` → ``'la grange'`` (same rule as utility names)."""
    return core_name(_PLACE_SUFFIX.sub("", name or ""))


def city_core(name: str | None) -> str:
    """A city written by another agency (``'SAN ANTONIO'``, ``'Waxahachie City'``, ``'City of Paris'``)."""
    return core_name(name)


# --- parsers -------------------------------------------------------------------------------------------------


def parse_pep_places(text: str, *, state: str = STATE_FIPS) -> pl.DataFrame:
    """``sub-est<vintage>_<state>.csv`` → incorporated places (SUMLEV 162), long by year.

    Returns ``place_fips`` (state + place, 7 characters), ``place_name``, ``funcstat``, ``year``,
    ``population`` (the July 1 ``POPESTIMATE<year>`` columns; the April base is left out).
    """
    df = pl.read_csv(io.StringIO(text), infer_schema=False)
    years = [c for c in df.columns if re.fullmatch(r"POPESTIMATE\d{4}", c)]
    places = df.filter(pl.col("SUMLEV") == INCORPORATED_PLACE, pl.col("STATE") == state).select(
        (pl.col("STATE") + pl.col("PLACE")).alias("place_fips"),
        pl.col("NAME").alias("place_name"),
        pl.col("FUNCSTAT").alias("funcstat"),
        *years,
    )
    return (
        places.unpivot(index=["place_fips", "place_name", "funcstat"], on=years, variable_name="_y")
        .select(
            "place_fips",
            "place_name",
            "funcstat",
            pl.col("_y").str.slice(len("POPESTIMATE")).cast(pl.Int32).alias("year"),
            pl.col("value").cast(pl.Float64).alias("population"),
        )
        .sort("place_fips", "year")
    )


def _bps_columns(top: list[str], sub: list[str]) -> list[str]:
    """Join the two BPS header rows: ``'1-unit|Units'`` for the grouped counts, ``'FIPS Place Code'`` else.

    A group label (``1-unit``) sits above any one of its three columns (Bldgs, Units, Value; in the files
    seen, above Units), and the top row can be shorter than the second, so each triple takes the first
    non-empty label found above it.
    """
    top = [t.strip() for t in top] + [""] * max(0, len(sub) - len(top))
    sub = [s.strip() for s in sub]
    names: list[str] = []
    i = 0
    while i < len(sub):
        if sub[i : i + 3] == ["Bldgs", "Units", "Value"]:
            group = next((t for t in top[i : i + 3] if t), f"group{i}")
            names += [f"{group}|Bldgs", f"{group}|Units", f"{group}|Value"]
            i += 3
        else:
            names.append(f"{top[i]} {sub[i]}".strip())
            i += 1
    return names


def parse_bps_places(text: str, *, state: str = STATE_FIPS) -> pl.DataFrame:
    """A BPS place file (``so<yy>a.txt`` annual, ``so<yymm>y.txt`` year to date) → one row per permit office.

    Headers are read from the file's two header rows, not assumed. Returns ``period`` (the survey date as
    written: ``2025`` or ``202608``), ``bps_id``, ``county_code``, ``place_fips`` (null for a county's
    unincorporated area, FIPS place 99990), ``place_name``, ``months_rep``, ``units_total`` (all structure types,
    with the Census imputation), ``units_1`` (single-unit) and ``units_total_rep`` (reported only).
    """
    rows = list(csv.reader(io.StringIO(text)))
    names = _bps_columns(rows[0], rows[1])
    col = {n: i for i, n in enumerate(names)}
    groups = ("1-unit", "2-units", "3-4 units", "5+ units")
    need = ["Survey Date", "State Code", "6-Digit ID", "County Code", "FIPS Place Code", "Number of Months Rep",
            "Place Name", *(f"{g}|Units" for g in groups), *(f"{g} rep|Units" for g in groups)]
    missing = [n for n in need if n not in col]
    if missing:
        raise ValueError(f"BPS place file: columns not found {missing}")
    out = []
    for r in rows[2:]:
        if len(r) != len(names) or r[col["State Code"]].strip() != state:
            continue

        def num(name: str, r: list[str] = r) -> float:
            v = r[col[name]].strip()
            return float(v) if v else 0.0

        place = r[col["FIPS Place Code"]].strip()
        out.append(
            {
                "period": r[col["Survey Date"]].strip(),
                "bps_id": r[col["6-Digit ID"]].strip(),
                "county_code": r[col["County Code"]].strip(),
                "place_fips": None if place in {"", UNINCORPORATED} else state + place,
                "place_name": r[col["Place Name"]].strip(),
                "months_rep": int(num("Number of Months Rep")),
                "units_total": sum(num(f"{g}|Units") for g in groups),
                "units_1": num("1-unit|Units"),
                "units_total_rep": sum(num(f"{g} rep|Units") for g in groups),
            }
        )
    schema = {"period": pl.Utf8, "bps_id": pl.Utf8, "county_code": pl.Utf8, "place_fips": pl.Utf8,
              "place_name": pl.Utf8, "months_rep": pl.Int32, "units_total": pl.Float64, "units_1": pl.Float64,
              "units_total_rep": pl.Float64}
    return pl.DataFrame(out, schema=schema)


def parse_acs_places(lines: Iterable[str], *, state: str = STATE_FIPS, table: str = "B25032") -> pl.DataFrame:
    """ACS table-based summary file (``acsdt5y<year>-b25032.dat``, ``|``-separated) → places of ``state``.

    ``lines``: the header line first, then any rows (only ``GEO_ID`` ``1600000US<state>…`` are kept). Returns
    ``place_fips``, ``occupied_units`` (line 1), ``owner_units`` (line 2) and ``owner_sf_homes`` (lines 3 + 4,
    owner-occupied 1-unit detached + attached; the same lines as ``accounts.owner_single_family``).
    """
    it = iter(lines)
    header = next(it).rstrip("\n").split("|")
    idx = {h: i for i, h in enumerate(header)}
    cells = {k: f"{table}_E{n:03d}" for k, n in (("occ", 1), ("own", 2), ("d", 3), ("a", 4))}
    prefix = f"1600000US{state}"
    out = []
    for line in it:
        if not line.startswith(prefix):
            continue
        r = line.rstrip("\n").split("|")

        def v(key: str, r: list[str] = r) -> float | None:
            s = r[idx[cells[key]]].strip()
            return float(s) if s not in {"", "null"} else None

        d, a = v("d"), v("a")
        out.append(
            {
                "place_fips": r[idx["GEO_ID"]][len("1600000US"):],
                "occupied_units": v("occ"),
                "owner_units": v("own"),
                "owner_sf_homes": None if d is None or a is None else d + a,
            }
        )
    return pl.DataFrame(
        out,
        schema={"place_fips": pl.Utf8, "occupied_units": pl.Float64, "owner_units": pl.Float64,
                "owner_sf_homes": pl.Float64},
    )


# --- crosswalk -----------------------------------------------------------------------------------------------


def match_places(candidates: pl.DataFrame, places: pl.DataFrame) -> pl.DataFrame:
    """Muni → incorporated place by exact core name, trying each account's names in ``priority`` order.

    ``candidates``: ``account_id``, ``name``, ``matched_on`` (e.g. ``puct``, ``eia``), ``priority`` (lower
    first). ``places``: ``place_fips``, ``place_name`` (one row per place). A core name shared by two places
    is ambiguous and never matched. Returns one row per account: ``account_id``, ``place_fips``,
    ``place_name``, ``matched_on`` (null when no name matched).
    """
    pc = places.select("place_fips", "place_name").unique().with_columns(
        pl.col("place_name").map_elements(place_core, return_dtype=pl.Utf8).alias("_core")
    )
    unique_core = pc.group_by("_core").agg(pl.len().alias("_n")).filter(pl.col("_n") == 1).select("_core")
    pc = pc.join(unique_core, on="_core", how="semi")
    cand = candidates.with_columns(pl.col("name").map_elements(core_name, return_dtype=pl.Utf8).alias("_core"))
    hits = (
        cand.filter(pl.col("_core") != "")
        .join(pc, on="_core", how="inner")
        .sort("account_id", "priority")
        .unique(subset=["account_id"], keep="first", maintain_order=True)
        .select("account_id", "place_fips", "place_name", "matched_on")
    )
    return candidates.select("account_id").unique().join(hits, on="account_id", how="left").sort("account_id")


def classify_overlap(
    df: pl.DataFrame, *, confirm: float = CONFIRM_SHARE, partial: float = PARTIAL_SHARE
) -> pl.DataFrame:
    """Add ``place_in_territory`` (intersection / place area), ``territory_in_place`` (intersection / territory
    area), ``overlap_status`` and ``fit``.

    ``overlap_status`` checks the crosswalk: ``confirmed`` when either share is ≥ ``confirm`` (the city and the
    CCN territory are mostly the same land from at least one side), ``partial`` when the larger share is ≥
    ``partial``, ``rejected`` below, ``no_place`` without a place. ``fit`` says how well the place describes the
    territory: ``same`` (both shares ≥ ``confirm``), ``city_larger`` (the territory sits inside a bigger city:
    place data over-counts it), ``territory_larger`` (the territory runs past the city limits: place data
    under-counts it), else ``weak``.

    ``df``: ``place_km2``, ``territory_km2``, ``inter_km2`` (null when the account has no place).
    """
    pin = pl.when(pl.col("place_km2") > 0).then(pl.col("inter_km2") / pl.col("place_km2"))
    tin = pl.when(pl.col("territory_km2") > 0).then(pl.col("inter_km2") / pl.col("territory_km2"))
    best = pl.max_horizontal("place_in_territory", "territory_in_place")
    p_ok, t_ok = pl.col("place_in_territory") >= confirm, pl.col("territory_in_place") >= confirm
    return df.with_columns(pin.alias("place_in_territory"), tin.alias("territory_in_place")).with_columns(
        pl.when(pl.col("place_km2").is_null())
        .then(pl.lit("no_place"))
        .when(best >= confirm)
        .then(pl.lit("confirmed"))
        .when(best >= partial)
        .then(pl.lit("partial"))
        .otherwise(pl.lit("rejected"))
        .alias("overlap_status"),
        pl.when(pl.col("place_km2").is_null())
        .then(None)
        .when(p_ok & t_ok)
        .then(pl.lit("same"))
        .when(t_ok)
        .then(pl.lit("city_larger"))
        .when(p_ok)
        .then(pl.lit("territory_larger"))
        .otherwise(pl.lit("weak"))
        .alias("fit"),
    )


# --- signals -------------------------------------------------------------------------------------------------


def place_signals(
    population: pl.DataFrame,
    permits: pl.DataFrame,
    housing: pl.DataFrame,
    *,
    start: int,
    end: int,
    permit_years: list[int],
) -> pl.DataFrame:
    """The Q3 signals, measured on the place instead of apportioned counties.

    ``population``: :func:`parse_pep_places` rows. ``permits``: :func:`parse_bps_places` annual rows with an
    integer ``year`` column (several years stacked). ``housing``: :func:`parse_acs_places`. Returns
    ``place_fips``, ``population`` (``end``), ``pop_growth`` (``start`` → ``end``), ``permit_units`` (sum over
    ``permit_years``, null when the place has no BPS row in those years), ``permits_per_1k`` (per resident in
    ``end``, Q3's definition), ``occupied_units``, ``owner_sf_homes`` and ``owner_sf_share``.
    """
    pop = population.filter(pl.col("year").is_in([start, end])).pivot(
        on="year", index="place_fips", values="population", aggregate_function="first"
    ).rename({str(start): "_p0", str(end): "_p1"})
    units = (
        permits.filter(pl.col("year").is_in(permit_years), pl.col("place_fips").is_not_null())
        .group_by("place_fips")
        .agg(pl.col("units_total").sum().alias("permit_units"))
    )
    return (
        pop.join(units, on="place_fips", how="left")
        .join(housing, on="place_fips", how="left")
        .select(
            "place_fips",
            pl.col("_p1").alias("population"),
            pl.when(pl.col("_p0") > 0).then(pl.col("_p1") / pl.col("_p0") - 1).alias("pop_growth"),
            "permit_units",
            pl.when(pl.col("_p1") > 0).then(pl.col("permit_units") / pl.col("_p1") * 1000).alias("permits_per_1k"),
            "occupied_units",
            "owner_sf_homes",
            pl.when(pl.col("occupied_units") > 0)
            .then(pl.col("owner_sf_homes") / pl.col("occupied_units"))
            .alias("owner_sf_share"),
        )
    )


def permit_change(
    recent: pl.DataFrame, prior: pl.DataFrame, *, growth: float = 0.25, min_units: float = 50.0
) -> pl.DataFrame:
    """Permit units of the same months in two years per place (e.g. year to date August 2026 vs August 2025).

    ``recent`` / ``prior``: ``place_fips``, ``units_total`` (several rows per place are summed). Returns
    ``place_fips``, ``recent``, ``prior``, ``change`` (null when ``prior`` is 0) and ``surge`` (``change`` ≥
    ``growth`` and ``recent`` ≥ ``min_units``, the X5 ``permit_surge`` thresholds).
    """

    def per(df: pl.DataFrame, name: str) -> pl.DataFrame:
        return df.filter(pl.col("place_fips").is_not_null()).group_by("place_fips").agg(
            pl.col("units_total").sum().alias(name)
        )

    both = per(recent, "recent").join(per(prior, "prior"), on="place_fips", how="full", coalesce=True)
    both = both.with_columns(pl.col("recent", "prior").fill_null(0.0))
    change = pl.when(pl.col("prior") > 0).then(pl.col("recent") / pl.col("prior") - 1)
    return both.with_columns(change.alias("change")).with_columns(
        ((pl.col("change") >= growth) & (pl.col("recent") >= min_units)).fill_null(False).alias("surge")
    )


# --- triggers mapped by city ----------------------------------------------------------------------------------


def _muni_cities(muni_places: pl.DataFrame) -> pl.DataFrame:
    """``account_id``, ``_core`` of the matched place (one row per muni with a place)."""
    return muni_places.filter(pl.col("place_fips").is_not_null()).select(
        "account_id", pl.col("place_name").map_elements(place_core, return_dtype=pl.Utf8).alias("_core")
    )


def dc_city_events(dc_events: pl.DataFrame, sites: pl.DataFrame, muni_places: pl.DataFrame) -> pl.DataFrame:
    """Data-center events (``triggers.dc_permit_events``) whose TCEQ site ``city`` is the muni's city.

    ``sites``: ``ref_num_txt``, ``city``. The TCEQ city is the site's address city (whether it is the legal
    jurisdiction is not verified). Exposure 1, like the other name mappings.
    """
    city = sites.filter(pl.col("city").is_not_null()).select(
        pl.col("ref_num_txt").alias("source_ref"),
        pl.col("city").map_elements(city_core, return_dtype=pl.Utf8).alias("_core"),
    )
    return (
        dc_events.join(city, on="source_ref", how="inner")
        .join(_muni_cities(muni_places), on="_core", how="inner")
        .select("account_id", *EVENT_COLUMNS, pl.lit(1.0).alias("exposure"))
    )


def city_units(taxing_units: str | None, local_government_name: str | None) -> list[str]:
    """The city taxing units of a Ch. 312 agreement: from the ``taxing_units`` JSON list when it parses, else
    the ``local_government_name``; only names that say City / Town / Village."""
    names: list[str] = []
    if taxing_units:
        try:
            parsed = json.loads(taxing_units)
            names = [str(x) for x in parsed] if isinstance(parsed, list) else []
        except ValueError:
            names = []
    if not names and local_government_name:
        names = [local_government_name]
    return [n for n in names if _CITY_GOV.search(n)]


def ch312_city_events(
    agreement_events: pl.DataFrame, agreements: pl.DataFrame, muni_places: pl.DataFrame
) -> pl.DataFrame:
    """Ch. 312 abatements granted by the muni's own city (X5 maps Ch. 312 by county only).

    ``agreement_events``: ``triggers.dev_agreement_events`` output (value and status filters already applied).
    ``agreements``: the raw rows with ``program``, ``agreement_id``, ``taxing_units``, ``local_government_name``.
    """
    units = agreements.filter(pl.col("program") == "ch312").select(
        pl.format("ch312:{}", pl.col("agreement_id")).alias("source_ref"),
        pl.struct("taxing_units", "local_government_name")
        .map_elements(
            lambda s: [city_core(n) for n in city_units(s["taxing_units"], s["local_government_name"])],
            return_dtype=pl.List(pl.Utf8),
        )
        .alias("_core"),
    ).explode("_core", empty_as_null=True).drop_nulls("_core").unique()
    return (
        agreement_events.filter(pl.col("source_ref").str.starts_with("ch312"))
        .join(units, on="source_ref", how="inner")
        .join(_muni_cities(muni_places), on="_core", how="inner")
        .unique(subset=["account_id", "source_ref"], keep="first")
        .select("account_id", *EVENT_COLUMNS, pl.lit(1.0).alias("exposure"))
    )


def place_permit_surge_events(changes: pl.DataFrame, muni_places: pl.DataFrame, *, as_of_month: date,
                              window: str) -> pl.DataFrame:
    """``permit_surge`` events from the muni's own place (:func:`permit_change` rows with ``surge``)."""
    hits = changes.filter(pl.col("surge")).join(
        muni_places.filter(pl.col("place_fips").is_not_null()).select("account_id", "place_fips", "place_name"),
        on="place_fips",
        how="inner",
    )
    return hits.select(
        "account_id",
        pl.lit("permit_surge").alias("trigger"),
        pl.lit(as_of_month).alias("event_date"),
        pl.lit(None, pl.Utf8).alias("county_fips"),
        pl.format("{} permits +{}% ({})", pl.col("place_name"), (pl.col("change") * 100).round(0),
                  pl.lit(window)).alias("title"),
        pl.format("{} → {} units (BPS place, imputed)", pl.col("prior").round(0), pl.col("recent").round(0))
        .alias("detail"),
        pl.lit("census_bps_place").alias("source"),
        pl.format("{}:{}", pl.col("place_fips"), pl.lit(as_of_month.isoformat())).alias("source_ref"),
        pl.lit(1.0).alias("exposure"),
    )
