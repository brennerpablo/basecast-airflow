"""EIA-860M monthly generator inventory → Texas and ERCOT generators, one report month per file.

Each workbook has one sheet per status (Operating, Planned, Retired, Canceled or Postponed, plus Puerto
Rico copies), a title row ("... as of August 2026") and the header a row or two below it. Columns were
added over the years (county and balancing authority moved, nameplate and winter capacity, storage MWh,
DC capacity...), so the header is found by what it says and every column is kept (snake_case).

Rows kept: plant state TX, or balancing authority ERCO (the 2015 files have no balancing authority
column, so they are filtered by state only). ``sheet_status`` is operating, planned, retired or
canceled_or_postponed; ``status_code`` / ``status`` split EIA's "(V) Under construction, more than 50
percent complete" (only the operating and planned sheets carry it). ``report_month`` is the first day of
the inventory month. The map-link columns (Google Map, Bing Map) are dropped."""

from __future__ import annotations

import re
from datetime import date

import polars as pl

from basecast_pipelines.parsers.eia._common import (
    check_identifiers,
    clean_strings,
    id_text,
    tx_county_fips,
    type_columns,
)
from basecast_pipelines.processing.core import Dataset, RawFile, latest_by_url
from basecast_pipelines.processing.tabular import clean_label, find_header_row, read_grid, sheet_names, snake

SOURCE_ID = "eia_860m"

_MONTHS = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october",
           "november", "december"]  # fmt: skip
_FILE = re.compile(r"^([a-z]+)_generator(\d{4})\.xlsx?$", re.IGNORECASE)
_TITLE = re.compile(r"as of\s+([A-Za-z]+)\s+(\d{4})", re.IGNORECASE)
_SHEETS = (
    (r"^operating", "operating"),
    (r"^planned", "planned"),
    (r"^retired", "retired"),
    (r"^cancel", "canceled_or_postponed"),
)
_RENAME = {
    "entity_id": "utility_id",
    "entity_name": "utility_name",
    "plant_id": "plant_id_eia",
    "plant_state": "state",
    "sector": "sector_name",
    "status": "status_raw",
}
_DROP = {"google_map", "bing_map"}


def _report_month(f: RawFile) -> date:
    meta = f.meta
    if "year" in meta and "month" in meta:
        return date(int(meta["year"]), int(meta["month"]), 1)
    match = _FILE.match(f.name)
    if not match or match.group(1).lower() not in _MONTHS:
        raise ValueError(f"{f.key}: not an EIA-860M file name")
    return date(int(match.group(2)), _MONTHS.index(match.group(1).lower()) + 1, 1)


def _sheet_status(sheet: str) -> str | None:
    name = sheet.strip().lower()
    return next((status for pattern, status in _SHEETS if re.search(pattern, name)), None)


def _parse_sheet(grid: pl.DataFrame, key: str, sheet: str, month: date) -> pl.DataFrame:
    title = next((clean_label(c) for c in grid.row(0) if c), "") if grid.height else ""
    match = _TITLE.search(title)
    if match and date(int(match.group(2)), _MONTHS.index(match.group(1).lower()) + 1, 1) != month:
        raise ValueError(f"{key}: sheet {sheet!r} says {title!r}, expected {month:%B %Y}")
    header = find_header_row(grid, [r"^entity id$", r"^plant id$", r"^generator id$"])
    if header is None:
        raise ValueError(f"{key}: no header in sheet {sheet!r}")
    names = []
    for i, raw in enumerate(grid.row(header)):
        name = snake(raw) if clean_label(raw) else f"col_{i}"
        names.append(_RENAME.get(name, name))
    if len(set(names)) != len(names):
        raise ValueError(f"{key}: duplicate columns in sheet {sheet!r}: {names}")
    body = grid.slice(header + 1)
    body.columns = names
    body = clean_strings(body.drop([c for c in names if c in _DROP or c.startswith("col_")]))
    keep = pl.col("state") == "TX"
    if "balancing_authority_code" in body.columns:
        keep = keep | (pl.col("balancing_authority_code") == "ERCO")
    body = body.filter(pl.col("plant_id_eia").is_not_null() & keep)
    cols = body.columns
    floats = [c for c in cols if re.search(r"(^|_)(mw|mwh)$", c) or c in {"latitude", "longitude"}]
    ints = [c for c in cols if re.search(r"(^|_)(month|year)$", c)] + ["plant_id_eia"]
    body = type_columns(body, floats=floats, ints=ints)
    if "status_raw" in cols:
        parts = pl.col("status_raw").str.extract_groups(r"^\(([A-Z]+)\)\s*(.*)$")
        body = body.with_columns(
            parts.struct.field("1").alias("status_code"),
            pl.coalesce(parts.struct.field("2"), pl.col("status_raw")).alias("status"),
        ).drop("status_raw")
    return body.with_columns(id_text("utility_id").alias("utility_id"))


def parse_generators(f: RawFile) -> pl.DataFrame:
    month = _report_month(f)
    frames = []
    with f.local_path() as path:
        for sheet in sheet_names(path):
            status = _sheet_status(sheet)
            if status is None:
                raise ValueError(f"{f.key}: unknown sheet {sheet!r}")
            df = _parse_sheet(read_grid(path, sheet), f.key, sheet, month)
            frames.append(df.with_columns(pl.lit(status).alias("sheet_status")))
    df = pl.concat(frames, how="diagonal_relaxed").with_columns(
        pl.lit(month).alias("report_month"),
        tx_county_fips("county", "state").alias("county_fips"),
    )
    for c in ("status_code", "status", "balancing_authority_code"):
        if c not in df.columns:
            df = df.with_columns(pl.lit(None, pl.String).alias(c))
    first = ["report_month", "sheet_status", "status_code", "status", "utility_id", "utility_name", "plant_id_eia",
             "plant_name", "generator_id", "state", "county", "county_fips", "balancing_authority_code"]  # fmt: skip
    return check_identifiers(df.select(*first, *[c for c in df.columns if c not in first]), "eia860m_generators")


DATASETS = [
    Dataset(
        name="eia860m_generators",
        target="postgres",
        mode="by_file",
        description="EIA-860M monthly generator inventory, Texas or ERCOT (BA = ERCO) rows: one report_month per "
        "file, sheet_status (operating, planned, retired, canceled_or_postponed), EIA status, capacities (MW), "
        "technology, energy source, operating / planned / retirement months, county_fips, coordinates.",
        parse=parse_generators,
        inputs=lambda f: bool(_FILE.match(f.name)),
        select=latest_by_url,
    ),
]
