"""City (Census place) facts and the city permit-surge trigger for the munis (X10 extras, R15; A-M3 P1).

Port of ``analysis/x10_muni_places.py`` §1 and §3 (logic in ``models/muni_places.py``), for the account marts:

- :func:`crosswalk`: muni → incorporated place by core name (the PUCT name, else the EIA name), checked against the
  CCN polygon in PostGIS (read-only), with the ``fit`` of the city to the territory;
- :func:`city_blocks`: the ``city`` block of ``mart_account_detail`` for every muni whose place is confirmed: PEP
  population and growth, ACS owner-occupied single-family homes and share, BPS permits, and the city's homes per
  EIA meter, each labelled as the city, not the territory;
- :func:`place_permit_surge_events`: ``permit_surge`` from the city's own BPS series (the same months year to date
  vs a year earlier, ≥ +25 %, ≥ 50 units, monthly reporters only: X10's recommended rule), in the account events
  shape of ``marts/accounts.py``.

The Census place files are read from the local lake, where X10's spec puts them (the existing census sources,
extended; no new source id): ``census_pep`` ``sub-est<vintage>_48.csv``, ``census_bps`` ``so<yyyy>a.txt`` and
``so<yymm>y.txt``, ``census_acs`` ``acsdt5y<year>-b25032.dat`` and ``census_tx_counties_geo``
``cb_<year>_48_place_500k.zip``, fetched by the census_acs, census_pep, census_bps and census_tx_counties_geo sources
(not parsed into tables); a missing file raises
:class:`FileNotFoundError` naming it.

Validation lock: every function takes the account universe as an argument (already without the held-out accounts)
and computes nothing for an account outside it; the database queries are limited to its munis.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import zipfile
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from basecast_pipelines.config import PROJECT_ROOT
from basecast_pipelines.marts import config as marts_config
from basecast_pipelines.marts.core import MartContext, value_check

GOLDEN_AS_OF = date(2026, 9, 26)  # X10's run date
RAW = PROJECT_ROOT / "data" / "raw"
POP_START, POP_END = 2020, 2025
PERMIT_YEARS = [2023, 2024, 2025]
EIA_YEAR, EIA_BASE = 2024, 2019
BEST_MIN_KM2 = 0.5  # the best spatial place needs this much overlap (X10)

# (source id, file name pattern) in the lake, as X10's spec proposes.
PEP_FILE = ("census_pep", re.compile(r"sub-est(\d{4})_48\.csv"))
BPS_ANNUAL = ("census_bps", re.compile(r"so(\d{4})a\.txt"))
BPS_YTD = ("census_bps", re.compile(r"so(\d{2})(\d{2})y\.txt"))
ACS_FILE = ("census_acs", re.compile(r"acsdt5y(\d{4})-b25032\.dat"))
PLACE_GEO = ("census_tx_counties_geo", re.compile(r"cb_(\d{4})_48_place_500k\.zip"))

EVENT_SCHEMA = {
    "account_id": pl.Utf8, "trigger": pl.Utf8, "event_date": pl.Date, "county_fips": pl.Utf8, "title": pl.Utf8,
    "detail": pl.Utf8, "source": pl.Utf8, "source_ref": pl.Utf8, "exposure": pl.Float64,
}
CITY_NOTE = "city, not territory"
# X10 §1 and review items 4–5: what the fit means for the city's counts.
FIT_NOTE = {
    "same": None,
    "city_larger": "the city is larger than the territory (other utilities serve part of it): counts over-count it",
    "territory_larger": "the territory runs past the city limits: counts are a floor",
}
FITS = tuple(FIT_NOTE)


# --- the lake files ------------------------------------------------------------------------------------------


def lake_files(raw: Path, source: str, pattern: re.Pattern[str]) -> dict[str, Path]:
    """File name → path of the files of ``raw/source=<source>/dt=*/`` whose name matches ``pattern``; the newest
    snapshot wins for a name found in several."""
    latest: dict[str, Path] = {}
    for path in sorted((Path(raw) / f"source={source}").glob("dt=*/*")):
        if pattern.fullmatch(path.name):
            latest[path.name] = path  # sorted by dt, so the last one wins
    return latest


def ytd_pair(names: Iterable[str], *, as_of: date) -> tuple[str, str, date]:
    """The newest BPS year-to-date file whose month has started by ``as_of`` and has the same month a year
    earlier: ``(recent file, prior file, first day of its last month)``."""
    months = {}
    for n in names:
        m = BPS_YTD[1].fullmatch(n)
        if m:
            months[date(2000 + int(m.group(1)), int(m.group(2)), 1)] = n
    for month in sorted((d for d in months if d <= as_of), reverse=True):
        prior = date(month.year - 1, month.month, 1)
        if prior in months:
            return months[month], months[prior], month
    raise FileNotFoundError(f"census_bps: no year-to-date place file pair (so<yymm>y.txt) up to {as_of}")


def ytd_window(month: date) -> str:
    """``'Jan–Aug 2026 vs Jan–Aug 2025'`` for August 2026."""
    span = "Jan" if month.month == 1 else f"Jan–{month:%b}"
    return f"{span} {month.year} vs {span} {month.year - 1}"


@dataclass(frozen=True)
class PlaceTables:
    """The parsed Census place files. ``ytd_month``: the first day of the last month of the year-to-date window."""

    pep: pl.DataFrame
    bps_annual: pl.DataFrame
    acs: pl.DataFrame
    acs_year: int
    ytd_now: pl.DataFrame
    ytd_prev: pl.DataFrame
    ytd_month: date
    geo_zip: Path
    files: tuple[Path, ...]


def load_tables(raw: Path, *, as_of: date) -> PlaceTables:
    """Parse the place files of the lake with ``models/muni_places``'s parsers (X10 §Place tables)."""
    from basecast_pipelines.models import muni_places as M

    pep_files = lake_files(raw, *PEP_FILE)
    annual = lake_files(raw, *BPS_ANNUAL)
    ytd = lake_files(raw, *BPS_YTD)
    acs_files = lake_files(raw, *ACS_FILE)
    geo_files = lake_files(raw, *PLACE_GEO)
    missing = [f"census_bps/so{y}a.txt" for y in PERMIT_YEARS if f"so{y}a.txt" not in annual]
    missing += [f"{s}/{p.pattern}" for (s, p), found in ((PEP_FILE, pep_files), (ACS_FILE, acs_files),
                                                         (PLACE_GEO, geo_files)) if not found]
    if missing:
        raise FileNotFoundError(f"Census place files not in the lake ({raw}): {missing}")
    pep_path = pep_files[max(pep_files)]  # the newest vintage
    acs_path = acs_files[max(acs_files)]
    geo_path = geo_files[max(geo_files)]
    now_name, prev_name, month = ytd_pair(ytd, as_of=as_of)

    def bps(path: Path) -> pl.DataFrame:
        return M.parse_bps_places(path.read_text(encoding="latin-1"))

    pep = M.parse_pep_places(pep_path.read_text(encoding="latin-1"))
    if not {POP_START, POP_END} <= set(pep["year"].unique().to_list()):
        raise ValueError(f"{pep_path.name}: needs July 1 estimates for {POP_START} and {POP_END}")
    with acs_path.open(encoding="latin-1") as f:
        acs = M.parse_acs_places(f)
    return PlaceTables(
        pep=pep,
        bps_annual=pl.concat([bps(annual[f"so{y}a.txt"]).with_columns(pl.lit(y).alias("year")) for y in PERMIT_YEARS]),
        acs=acs,
        acs_year=int(ACS_FILE[1].fullmatch(acs_path.name).group(1)),
        ytd_now=bps(ytd[now_name]),
        ytd_prev=bps(ytd[prev_name]),
        ytd_month=month,
        geo_zip=geo_path,
        files=(pep_path, *(annual[f"so{y}a.txt"] for y in PERMIT_YEARS), ytd[now_name], ytd[prev_name], acs_path,
               geo_path),
    )


def tables(ctx: MartContext) -> PlaceTables:
    return ctx.cached("muni_places.tables", lambda: load_tables(RAW, as_of=ctx.as_of))


def place_signals(ctx: MartContext) -> pl.DataFrame:
    """X10's place signals (Q3's definitions on the city), one row per incorporated place."""
    from basecast_pipelines.models import muni_places as M

    def build() -> pl.DataFrame:
        t = tables(ctx)
        return M.place_signals(t.pep, t.bps_annual, t.acs, start=POP_START, end=POP_END, permit_years=PERMIT_YEARS)

    return ctx.cached("muni_places.signals", build)


# --- the crosswalk (X10 §1) ----------------------------------------------------------------------------------


def munis_of(accounts: pl.DataFrame) -> pl.DataFrame:
    """The munis of the universe: ``account_id``, ``name``, ``eia_utility_id`` (from the Q2 crosswalk when
    ``accounts`` does not carry it; only the given accounts are looked up)."""
    munis = accounts.filter(pl.col("account_type") == "muni")
    if "eia_utility_id" not in munis.columns:
        from basecast_pipelines.models import accounts as A

        crosswalk = A.read_crosswalk(PROJECT_ROOT / "config" / "utility_crosswalk.yaml")
        munis = A.build_universe(munis.select("account_id", "name", "account_type"), crosswalk, intersect=False)
    return munis.select(pl.col("account_id").cast(pl.Utf8), "name", pl.col("eia_utility_id").cast(pl.Utf8))


def name_candidates(munis: pl.DataFrame, eia_names: pl.DataFrame) -> pl.DataFrame:
    """The names to match on, in order: the PUCT name (``puct``, priority 0), then the EIA-861 utility name (``eia``,
    priority 1). ``eia_names``: ``eia_utility_id``, ``utility_name``."""
    return pl.concat(
        [
            munis.select("account_id", "name", pl.lit("puct").alias("matched_on"), pl.lit(0).alias("priority")),
            munis.join(eia_names, on="eia_utility_id", how="inner").select(
                "account_id", pl.col("utility_name").alias("name"), pl.lit("eia").alias("matched_on"),
                pl.lit(1).alias("priority"),
            ),
        ]
    )


def place_polygons(zip_bytes: bytes, boxes: list[Mapping[str, float]], incorporated: set[str]) -> list[dict]:
    """The incorporated places of the cartographic-boundary zip whose bounding box touches one of ``boxes``
    (``x0``, ``y0``, ``x1``, ``y1``) as ``{place_fips, gj}`` (GeoJSON text; NAD83 read as WGS84, a sub-metre
    difference for areas)."""
    import shapefile  # pyshp

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:

        def member(suffix: str) -> io.BytesIO:
            (name,) = [n for n in z.namelist() if n.lower().endswith(suffix)]
            return io.BytesIO(z.read(name))

        sf = shapefile.Reader(shp=member(".shp"), shx=member(".shx"), dbf=member(".dbf"))
        fields = [f[0] for f in sf.fields[1:]]
        out = []
        for sr in sf.iterShapeRecords():
            rec = dict(zip(fields, sr.record, strict=True))
            x0, y0, x1, y1 = sr.shape.bbox
            if rec["GEOID"] in incorporated and any(
                x0 <= b["x1"] and x1 >= b["x0"] and y0 <= b["y1"] and y1 >= b["y0"] for b in boxes
            ):
                out.append({"place_fips": rec["GEOID"], "gj": json.dumps(sr.shape.__geo_interface__)})
    return out


EXTENT_SQL = """
    select ccn_no as account_id, st_xmin(e) x0, st_ymin(e) y0, st_xmax(e) x1, st_ymax(e) y1
    from (select ccn_no, st_extent(geom)::geometry e from puct_ccn_territories
          where ccn_no = any(%(ids)s) group by ccn_no) s
"""
OVERLAP_SQL = """
    with p as (
        select place_fips, st_makevalid(st_setsrid(st_geomfromgeojson(gj), 4326)) g
        from jsonb_to_recordset(%(p)s::jsonb) as x(place_fips text, gj text)
    ), t as (
        select ccn_no, st_makevalid(st_union(geom)) g from puct_ccn_territories
        where ccn_no = any(%(ids)s) group by ccn_no
    )
    select t.ccn_no as account_id, p.place_fips,
           st_area(t.g::geography) / 1e6 as territory_km2,
           st_area(p.g::geography) / 1e6 as place_km2,
           st_area(st_intersection(t.g, p.g)::geography) / 1e6 as inter_km2
    from t join p on st_intersects(t.g, p.g)
"""
TERRITORY_SQL = (
    "select ccn_no as account_id, st_area(st_union(geom)::geography) / 1e6 as territory_km2 "
    "from puct_ccn_territories where ccn_no = any(%(ids)s) group by ccn_no"
)
_OVERLAP_SCHEMA = {"account_id": pl.Utf8, "place_fips": pl.Utf8, "territory_km2": pl.Float64,
                   "place_km2": pl.Float64, "inter_km2": pl.Float64}


def check_crosswalk(names: pl.DataFrame, overlap: pl.DataFrame, territory: pl.DataFrame,
                    places: pl.DataFrame) -> pl.DataFrame:
    """The name matches with the polygon check (X10 §1): ``classify_overlap`` of the matched place, plus the best
    spatial place (the largest overlap of ≥ ``BEST_MIN_KM2``) and whether the name match is it.

    ``names``: :func:`models.muni_places.match_places` rows. ``overlap``: account × intersecting place with
    ``territory_km2``, ``place_km2``, ``inter_km2``. ``territory``: ``account_id``, ``territory_km2``.
    """
    from basecast_pipelines.models import muni_places as M

    ov = overlap.select([pl.col(c).cast(t) for c, t in _OVERLAP_SCHEMA.items()])
    terr = territory.select(pl.col("account_id").cast(pl.Utf8), pl.col("territory_km2").cast(pl.Float64))
    pk = ov.select("place_fips", "place_km2").unique()
    checked = M.classify_overlap(
        names.join(terr, on="account_id", how="left")
        .join(pk, on="place_fips", how="left")
        .join(ov.select("account_id", "place_fips", "inter_km2"), on=["account_id", "place_fips"], how="left")
        .with_columns(
            pl.when(pl.col("place_fips").is_not_null()).then(pl.col("inter_km2").fill_null(0.0)).alias("inter_km2")
        )
    )
    best = (
        M.classify_overlap(ov)
        .filter(pl.col("inter_km2") >= BEST_MIN_KM2)
        .sort("inter_km2", descending=True)
        .unique(subset=["account_id"], keep="first")
        .join(places.select("place_fips", "place_name"), on="place_fips")
        .select("account_id", pl.col("place_fips").alias("best_fips"), pl.col("place_name").alias("best_place"))
    )
    return checked.join(best, on="account_id", how="left").with_columns(
        (pl.col("best_fips") == pl.col("place_fips")).alias("name_is_best_spatial")
    )


def _universe_key(prefix: str, ids: list[str]) -> str:
    return f"{prefix}:{hashlib.sha1(','.join(sorted(ids)).encode()).hexdigest()[:12]}"


def crosswalk(ctx: MartContext, accounts: pl.DataFrame) -> pl.DataFrame:
    """One row per muni of ``accounts``: ``account_id``, ``name``, ``eia_utility_id``, ``place_fips``,
    ``place_name``, ``matched_on``, ``territory_km2``, ``place_km2``, ``inter_km2``, ``place_in_territory``,
    ``territory_in_place``, ``overlap_status``, ``fit``, ``best_fips``, ``best_place``, ``name_is_best_spatial``."""
    munis = munis_of(accounts)
    ids = munis["account_id"].to_list()
    if not ids:
        raise ValueError("no muni in the accounts given")

    def build() -> pl.DataFrame:
        from basecast_pipelines.models import accounts as A
        from basecast_pipelines.models import muni_places as M

        t = tables(ctx)
        places = t.pep.select("place_fips", "place_name", "funcstat").unique()
        eia_names = ctx.cached("muni_places.eia_names", lambda: A.load_eia_utilities().select(
            pl.col("utility_id").cast(pl.Utf8).alias("eia_utility_id"), "utility_name"))
        names = M.match_places(name_candidates(munis, eia_names), places).join(munis, on="account_id")
        boxes = ctx.read_sql(EXTENT_SQL, {"ids": ids}).rows(named=True)
        geo = place_polygons(t.geo_zip.read_bytes(), boxes, set(places["place_fips"]))
        overlap = ctx.read_sql(OVERLAP_SQL, {"p": json.dumps(geo), "ids": ids})
        territory = ctx.read_sql(TERRITORY_SQL, {"ids": ids})
        return check_crosswalk(names, overlap, territory, places)

    return ctx.cached(_universe_key("muni_places.crosswalk", ids), build)


def confirmed(xw: pl.DataFrame) -> pl.DataFrame:
    """The munis whose city is confirmed by the polygon (X10: only these carry place data)."""
    return xw.filter(pl.col("overlap_status") == "confirmed")


# --- the city block (X10 §2, recommendation 1) ---------------------------------------------------------------


def meters(eia: pl.DataFrame, munis: pl.DataFrame) -> pl.DataFrame:
    """``account_id``, ``eia_customers``: X4's wires customers (incl. delivery-only) in ``EIA_YEAR``, as X10."""
    from basecast_pipelines.models import eia861_short_form as S

    sig = S.utility_signals(eia, year=EIA_YEAR, base_year=EIA_BASE, customers="wires_customers").select(
        pl.col("utility_id").cast(pl.Utf8).alias("eia_utility_id"), "eia_customers"
    )
    return munis.select("account_id", "eia_utility_id").join(sig, on="eia_utility_id", how="left").select(
        "account_id", pl.col("eia_customers").cast(pl.Float64)
    )


def load_eia(raw: Path | None = None) -> pl.DataFrame:
    """The EIA-861 long + short form with delivery-only customers (X4), as ``marts/accounts.py`` builds it (the
    861S zips from the lake at ``raw``, default :data:`RAW`)."""
    from basecast_pipelines.models import eia861_short_form as S

    raw = raw or RAW
    long_tot = S.long_form_totals(S.load_long_sales())
    short_tot = S.short_form_totals(S.load_short_form(raw))
    delivery = S.load_delivery(raw)
    return S.add_delivery(
        S.with_price(S.combine_forms(long_tot, short_tot)), delivery, first_year=int(delivery["data_year"].min())
    )


def _note(*parts: str | None) -> str:
    return "; ".join(p for p in (CITY_NOTE, *parts) if p)


def city_block(row: Mapping[str, Any], *, acs_year: int) -> dict:
    """The ``city`` block of one muni (JSON-ready): place, fit, the two area shares and the city facts.

    ``row``: a confirmed crosswalk row joined to its place signals and ``eia_customers``. Every note says "city,
    not territory"; counts also carry what the fit means for them, and each fact X10's own qualifier.
    """
    from basecast_pipelines.models import diagnosis as D

    fit = row["fit"]
    if fit not in FIT_NOTE:
        raise ValueError(f"{row['account_id']}: fit {fit!r} is not one of {FITS}")
    fit_note = FIT_NOTE[fit]
    pep = D.Source("census_pep_place", date(POP_END, 7, 1))
    bps = D.Source("census_bps_place", date(max(PERMIT_YEARS), 12, 31))
    acs = D.Source("census_acs_place", date(acs_year, 12, 31))
    y0, y1 = min(PERMIT_YEARS), max(PERMIT_YEARS)
    homes, cust, units = row.get("owner_sf_homes"), row.get("eia_customers"), row.get("permit_units")
    no_bps = None if units is not None else f"no BPS row for the place in {y0}–{y1}"
    facts = [
        D.fact("city_population", f"Population, {row['place_name']} (Census place)", row.get("population"),
               "people", pep, _note(f"PEP vintage {POP_END}, July 1", fit_note)),
        D.fact("city_pop_growth", f"Population growth {POP_START}→{POP_END} (city)", row.get("pop_growth"), "share",
               pep, _note(f"boundaries of {POP_END} for every year; a fast city can grow past its muni's wires "
                          "(X10): meter growth stays the growth signal")),
        D.fact("city_owner_sf_homes", "Owner-occupied single-family homes (city)", homes, "homes", acs,
               _note(f"ACS {acs_year} 5-year B25032, owner-occupied 1-unit detached + attached", fit_note)),
        D.fact("city_owner_sf_share", "Owner-occupied single-family share (city)", row.get("owner_sf_share"), "share",
               acs, _note(f"ACS {acs_year} 5-year B25032, lines 3 + 4 over occupied units")),
        D.fact("city_permit_units", f"Permit units {y0}–{y1} (city)", units, "units", bps,
               _note("BPS annual, with the Census imputation", no_bps, fit_note)),
        D.fact("city_permits_per_1k", "Permits per 1,000 residents (city)", row.get("permits_per_1k"), "units/1k",
               bps, _note(f"BPS {y0}–{y1} units per resident of {POP_END}", no_bps)),
        D.fact("city_homes_per_meter", "City owner-occupied single-family homes per EIA meter",
               (homes / cust) if homes is not None and cust else None, "ratio", acs,
               _note(f"EIA-861 meters {EIA_YEAR} incl. delivery-only; meters also count businesses", fit_note)),
    ]
    for f in facts:
        f["simulated"] = False
        f["verified"] = True
    return D.to_jsonable({
        "place_name": row["place_name"],
        "place_fips": row["place_fips"],
        "fit": fit,
        "place_in_territory": row.get("place_in_territory"),
        "territory_in_place": row.get("territory_in_place"),
        "facts": facts,
    })


def city_rows(ctx: MartContext, accounts: pl.DataFrame, *, eia: pl.DataFrame | None = None) -> pl.DataFrame:
    """The confirmed munis of ``accounts`` with their place signals and EIA meters (one row per muni)."""
    munis = munis_of(accounts)
    if eia is None:
        eia = ctx.cached("muni_places.eia", load_eia)
    return (
        confirmed(crosswalk(ctx, accounts))
        .select("account_id", "place_fips", "place_name", "matched_on", "fit", "place_in_territory",
                "territory_in_place")
        .join(place_signals(ctx), on="place_fips", how="left")
        .join(meters(eia, munis), on="account_id", how="left")
        .sort("account_id")
    )


def city_blocks(ctx: MartContext, accounts: pl.DataFrame, *, eia: pl.DataFrame | None = None) -> dict[str, dict]:
    """``account_id`` → the ``city`` block, for the munis of ``accounts`` whose city is confirmed (co-ops and
    unconfirmed munis have none: the payload's ``city`` stays null). ``eia``: the EIA frame ``marts/accounts.py``
    already built (loaded here when None)."""
    if munis_of(accounts).is_empty():
        return {}
    acs_year = tables(ctx).acs_year
    return {r["account_id"]: city_block(r, acs_year=acs_year)
            for r in city_rows(ctx, accounts, eia=eia).iter_rows(named=True)}


# --- the city permit surge (X10 §3, recommendation 2) --------------------------------------------------------


def reporting_months(ytd_now: pl.DataFrame, ytd_prev: pl.DataFrame) -> pl.DataFrame:
    """``place_fips``, ``m_now``, ``m_prev``: the most months any permit office of the place reported in each
    year-to-date file (0 = the place's numbers are Census imputation only)."""

    def per(df: pl.DataFrame, name: str) -> pl.DataFrame:
        return df.filter(pl.col("place_fips").is_not_null()).group_by("place_fips").agg(
            pl.col("months_rep").max().alias(name)
        )

    return per(ytd_now, "m_now").join(per(ytd_prev, "m_prev"), on="place_fips", how="full", coalesce=True)


def reported_changes(ytd_now: pl.DataFrame, ytd_prev: pl.DataFrame, *, months: int) -> pl.DataFrame:
    """:func:`models.muni_places.permit_change` of the two year-to-date files, kept for the places that reported
    every month of the window in both years (X10's monthly-reporter rule)."""
    from basecast_pipelines.models import muni_places as M

    changes = M.permit_change(ytd_now, ytd_prev)
    return changes.join(reporting_months(ytd_now, ytd_prev), on="place_fips").filter(
        pl.col("m_now") >= months, pl.col("m_prev") >= months
    )


def surge_events(changes: pl.DataFrame, muni_places: pl.DataFrame, ids: list[str], *, month: date) -> pl.DataFrame:
    """``models.muni_places.place_permit_surge_events`` in the account events shape (:data:`EVENT_SCHEMA`), kept
    to ``ids`` (validation lock)."""
    from basecast_pipelines.models import muni_places as M

    ev = M.place_permit_surge_events(changes, muni_places, as_of_month=month, window=ytd_window(month))
    return ev.select([pl.col(c).cast(t) for c, t in EVENT_SCHEMA.items()]).filter(pl.col("account_id").is_in(ids))


def place_permit_surge_events(ctx: MartContext, accounts: pl.DataFrame) -> pl.DataFrame:
    """``permit_surge`` events of the munis of ``accounts`` from their confirmed city's BPS series: the newest
    year to date vs the same months a year earlier, ≥ +25 % and ≥ 50 units, monthly reporters only. Dated at the
    window's last month, exposure 1, no county (a name mapping). Columns: :data:`EVENT_SCHEMA`."""
    if munis_of(accounts).is_empty():
        return pl.DataFrame(schema=EVENT_SCHEMA)
    t = tables(ctx)
    mp = confirmed(crosswalk(ctx, accounts)).select("account_id", "place_fips", "place_name")
    changes = reported_changes(t.ytd_now, t.ytd_prev, months=t.ytd_month.month)
    return surge_events(changes, mp, mp["account_id"].to_list(), month=t.ytd_month)


# --- golden checks (X10, as_of 2026-09-26), for the integrator to attach --------------------------------------


def _city(payload: str) -> dict | None:
    return json.loads(payload).get("city")


def _fit_count(fit: str):
    return lambda f: sum(1 for p in f["payload"] if (c := _city(p)) is not None and c["fit"] == fit)


def _place_facts_on(config: Mapping[str, Any]) -> bool:
    return bool(marts_config.value(config, "munis.place_facts"))


def _as_x10(config: Mapping[str, Any]) -> bool:
    """X10's switches for the trigger count: X5's strong triggers (the IA one strong) plus the city permits."""
    v = marts_config.value
    return v(config, "triggers.gen_storage_ia") == "strong" and bool(v(config, "munis.place_permit_trigger"))


# ``mart_account_detail`` with ``munis.place_facts: true`` (X10 §1: 59 of 59 munis confirmed; 31 / 7 / 21).
DETAIL_CHECKS = tuple(
    value_check(name, get, expected, as_of=GOLDEN_AS_OF, applies=_place_facts_on)
    for name, get, expected in (
        ("59 munis with a city block (X10)", lambda f: sum(_city(p) is not None for p in f["payload"]), 59),
        ("city fit same (X10)", _fit_count("same"), 31),
        ("city fit city_larger (X10)", _fit_count("city_larger"), 7),
        ("city fit territory_larger (X10)", _fit_count("territory_larger"), 21),
    )
)
# ``mart_accounts`` with ``munis.place_permit_trigger: true`` (X10 §3: munis with an active strong trigger 10 → 13).
ACCOUNT_CHECKS = (
    value_check("munis with an active strong trigger, city permits on (X10)",
                lambda f: f.filter(pl.col("account_type") == "muni", pl.col("n_strong") >= 1).height, 13,
                as_of=GOLDEN_AS_OF, applies=_as_x10),
)
