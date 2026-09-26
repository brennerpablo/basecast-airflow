"""EIA-860 annual release (zip of workbooks) → Texas plants and generators.

- ``eia860_plants``: Schedule 2 (``2___Plant_Y<year>.xlsx``), Texas plants with their location,
  balancing authority and the transmission or distribution system owner they connect to (``td_owner``,
  ``td_owner_id`` = that utility's EIA id) — the link from plants to co-ops and munis.
- ``eia860_generators``: Schedule 3.1 (``3_1_Generator_Y<year>.xlsx``), Texas generators from the
  Operable, Proposed and Retired and Canceled sheets; ``sheet`` says which, ``status`` keeps EIA's code
  (OP, SB, P, L, T, U, V, TS, OT, RE, CN, ...).

Every column of those sheets is kept (snake_case), found by header text; a title row sits above the
header. Capacities are MW floats, months and years integers, "?" columns and Y/N flags booleans (X = not
applicable and U = unknown → null). ``data_year`` comes from the zip name."""

from __future__ import annotations

import re

import polars as pl

from basecast_pipelines.parsers.eia._common import (
    check_identifiers,
    clean_strings,
    id_text,
    tx_county_fips,
    type_columns,
    zip_member,
)
from basecast_pipelines.processing.core import Dataset, RawFile, latest_by_url
from basecast_pipelines.processing.tabular import clean_label, find_header_row, read_grid, sheet_names, snake

SOURCE_ID = "eia_860"

_ZIP = re.compile(r"^eia860(\d{4})\.zip$", re.IGNORECASE)
_RENAME = {
    "plant_code": "plant_id_eia",
    "transmission_or_distribution_system_owner": "td_owner",
    "transmission_or_distribution_system_owner_id": "td_owner_id",
    "transmission_or_distribution_system_owner_state": "td_owner_state",
    "rto_iso_location_designation_for_reporting_wholesale_sales_data_to_ferc": "rto_iso_location_designation",
    "primary_purpose_naics_code": "primary_purpose_naics",
}
# Y/N columns whose header does not end with "?"
_FLAGS = {
    "energy_storage", "natural_gas_storage", "liquefied_natural_gas_storage", "ferc_cogeneration_status",
    "ferc_small_power_producer_status", "ferc_exempt_wholesale_generator_status", "duct_burners",
    "uprate_or_derate_completed_during_year", "synchronized_to_transmission_grid",
    "associated_with_combined_heat_and_power_system", "previously_canceled",
}  # fmt: skip


def _data_year(f: RawFile) -> int:
    if "data_year" in f.meta:
        return int(f.meta["data_year"])
    match = _ZIP.match(f.name)
    if not match:
        raise ValueError(f"{f.key}: not an EIA-860 zip name")
    return int(match.group(1))


def _sheet_table(book: bytes, sheet: str, key: str) -> tuple[pl.DataFrame, set[str]]:
    """A schedule sheet with its header (found under the title row) as snake_case names; header cells
    ending in "?" are returned as flag columns."""
    grid = read_grid(book, sheet)
    header = find_header_row(grid, [r"^utility id$", r"^plant (code|id)$", r"^state$"])
    if header is None:
        raise ValueError(f"{key}: no header in sheet {sheet!r}")
    labels = list(grid.row(header))
    names, seen, flags = [], set(), set()
    for i, raw in enumerate(labels):
        name = _RENAME.get(snake(raw), snake(raw)) if clean_label(raw) else f"col_{i}"
        if name in seen:
            raise ValueError(f"{key}: duplicate column {name!r} in sheet {sheet!r}")
        seen.add(name)
        names.append(name)
        if clean_label(raw).endswith("?"):
            flags.add(name)
    body = grid.slice(header + 1)
    body.columns = names
    body = clean_strings(body).filter(pl.col("utility_id").is_not_null() | pl.col("plant_id_eia").is_not_null())
    return body.drop([c for c in body.columns if c.startswith("col_") and body[c].is_null().all()]), flags


def _typed(df: pl.DataFrame, flags: set[str]) -> pl.DataFrame:
    cols = df.columns
    floats = [c for c in cols if re.search(r"(^|_)(mw|mwh|kv)$", c) or c in {"latitude", "longitude",
              "nameplate_power_factor"}]  # fmt: skip
    ints = [c for c in cols if re.search(r"(^|_)(month|year)(_|$)", c) and c not in _FLAGS]
    ints += [c for c in ("plant_id_eia", "sector") if c in cols]
    flag_cols = sorted((flags | _FLAGS) & set(cols))
    df = type_columns(df, floats=floats, ints=ints, flags=flag_cols)
    return df.with_columns(
        id_text("utility_id").alias("utility_id"),
        *([id_text("td_owner_id").alias("td_owner_id")] if "td_owner_id" in cols else []),
    )


def _book(f: RawFile, pattern: str) -> bytes:
    found = zip_member(f.read_bytes(), pattern)
    if found is None:
        raise ValueError(f"{f.key}: no member matching {pattern!r}")
    return found[1]


def parse_plants(f: RawFile) -> pl.DataFrame:
    book = _book(f, r"^2___Plant_Y\d{4}\.xlsx?$")
    sheet = next((s for s in sheet_names(book) if s.strip().lower() == "plant"), None)
    if sheet is None:
        raise ValueError(f"{f.key}: no Plant sheet")
    body, flags = _sheet_table(book, sheet, f.key)
    body = _typed(body.filter(pl.col("state") == "TX"), flags)
    first = ["data_year", "utility_id", "utility_name", "plant_id_eia", "plant_name", "state", "county",
             "county_fips"]  # fmt: skip
    body = body.with_columns(
        pl.lit(_data_year(f), pl.Int64).alias("data_year"), tx_county_fips("county").alias("county_fips")
    )
    return check_identifiers(body.select(*first, *[c for c in body.columns if c not in first]), "eia860_plants")


def parse_generators(f: RawFile) -> pl.DataFrame:
    book = _book(f, r"^3_1_Generator_Y\d{4}\.xlsx?$")
    frames = []
    for sheet in sheet_names(book):
        body, flags = _sheet_table(book, sheet, f.key)
        body = _typed(body.filter(pl.col("state") == "TX"), flags)
        frames.append(body.with_columns(pl.lit(snake(sheet)).alias("sheet")))
    if not frames:
        raise ValueError(f"{f.key}: no generator sheets")
    df = pl.concat(frames, how="diagonal_relaxed").with_columns(
        pl.lit(_data_year(f), pl.Int64).alias("data_year"), tx_county_fips("county").alias("county_fips")
    )
    first = ["data_year", "sheet", "utility_id", "utility_name", "plant_id_eia", "plant_name", "state", "county",
             "county_fips", "generator_id", "status"]  # fmt: skip
    return check_identifiers(df.select(*first, *[c for c in df.columns if c not in first]), "eia860_generators")


def _is_release_zip(f: RawFile) -> bool:
    return bool(_ZIP.match(f.name))


DATASETS = [
    Dataset(
        name="eia860_plants",
        target="postgres",
        mode="by_file",
        description="EIA-860 Schedule 2 plants in Texas: location, county_fips, balancing authority, NERC region, "
        "sector, grid voltage and the transmission or distribution system owner (td_owner, td_owner_id).",
        parse=parse_plants,
        inputs=_is_release_zip,
        select=latest_by_url,
    ),
    Dataset(
        name="eia860_generators",
        target="postgres",
        mode="by_file",
        description="EIA-860 Schedule 3.1 generators in Texas from the Operable, Proposed and Retired and "
        "Canceled sheets (sheet column), with EIA status code, capacities (MW), technology, energy sources "
        "and operating, planned and retirement months/years.",
        parse=parse_generators,
        inputs=_is_release_zip,
        select=latest_by_url,
    ),
]
