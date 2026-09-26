"""EIA-861 annual files → Texas utilities: sales, revenue and customers by sector (``Sales_Ult_Cust``),
utility characteristics (``Utility_Data``) and the counties each utility serves (``Service_Territory``).

One raw file is one data year's zip: a final release, or the early release of the latest year (flagged
``early_release``; EIA says it is not fully edited). When a year's final release arrives, its early
release rows stay in the table (``by_file``): filter ``early_release = false`` when both exist.

Layouts change across years (``.xls`` until 2014, a "Short Form" column in some years, a leading note
column in the early release, ``BA_CODE`` vs ``BA Code``), so every header is found by what it says:

- ``Sales_Ult_Cust``: a three-row header block (sector, measure, unit). Sectors become rows (``sector`` =
  residential, commercial, industrial, transportation, total), with ``revenue_thousand_usd``,
  ``sales_mwh`` and ``customers``. EIA's state-adjustment rows (utility 99999) are kept; per EIA, a state
  total sums parts A-D but not part C customers.
- ``Utility_Data``: a group row (NERC regions, RTOs, other activities) over the labels; flags become
  booleans prefixed ``nerc_``, ``rto_`` and ``activity_`` (Y → true, N → false, blank → null). The
  balancing authority is not in this sheet: ``ba_code`` (the one with most Texas customers) and
  ``ba_codes`` (JSON array) come from the same year's ``Sales_Ult_Cust`` Texas rows.
- ``Service_Territory``: one row per utility and county; ``county_fips`` from the county name.

Only Texas rows (``state = 'TX'``) are kept."""

from __future__ import annotations

import json
import re

import polars as pl

from basecast_pipelines.parsers.eia._common import (
    body_with_names,
    clean_strings,
    group_header,
    id_text,
    tx_county_fips,
    type_columns,
    zip_member,
)
from basecast_pipelines.processing.core import Dataset, RawFile, latest_by_url
from basecast_pipelines.processing.tabular import (
    clean_label,
    find_header_row,
    integer,
    read_grid,
    sheet_names,
    snake,
    with_header,
)

SOURCE_ID = "eia_861"

_ZIP = re.compile(r"^f861(\d{4})(er)?\.zip$", re.IGNORECASE)
_SECTORS = ("residential", "commercial", "industrial", "transportation", "total")
_MEASURES = {"revenues": ("revenue_thousand_usd", "thousand dollars"), "sales": ("sales_mwh", "megawatthours"),
             "customers": ("customers", "count")}  # fmt: skip
_BASE = {"data_year", "utility_number", "utility_name", "state"}


def _release(f: RawFile) -> tuple[int, bool]:
    """(data year, early release) from what discovery recorded, else from the file name."""
    meta = f.meta
    if "data_year" in meta:
        return int(meta["data_year"]), bool(meta.get("early_release"))
    match = _ZIP.match(f.name)
    if not match:
        raise ValueError(f"{f.key}: not an EIA-861 zip name")
    return int(match.group(1)), bool(match.group(2))


def _workbook(data: bytes, stem: str, key: str) -> bytes:
    """The zip's ``<stem>_<year>...xls[x]`` workbook (``Sales_Ult_Cust_2024.xlsx``, not ``..._CS_2024``)."""
    found = zip_member(data, rf"^{stem}_\d{{4}}\w*\.xlsx?$")
    if found is None:
        raise ValueError(f"{key}: no {stem} workbook in the zip")
    return found[1]


def _common_columns(body: pl.DataFrame, data_year: int, early: bool) -> list[pl.Expr]:
    missing = _BASE - set(body.columns)
    if missing:
        raise ValueError(f"missing columns {sorted(missing)} in {body.columns}")
    exprs = [
        integer(pl.col("data_year")).fill_null(data_year).alias("data_year"),
        pl.lit(early).alias("early_release"),
        id_text("utility_number").alias("utility_id"),
        pl.col("utility_name"),
    ]
    if "short_form" in body.columns:
        exprs.append((pl.col("short_form").str.to_uppercase() == "Y").fill_null(False).alias("short_form"))
    else:
        exprs.append(pl.lit(None, pl.Boolean).alias("short_form"))
    return exprs


# --- Sales_Ult_Cust -------------------------------------------------------------------------------------


def _row_above(grid: pl.DataFrame, start: int, pattern: str) -> int:
    rx = re.compile(pattern, re.IGNORECASE)
    for i in range(start - 1, -1, -1):
        if any(rx.search(clean_label(c)) for c in grid.row(i) if c is not None):
            return i
    raise ValueError(f"no row matching {pattern!r} above row {start}")


def _sales_sheet(grid: pl.DataFrame, data_year: int, early: bool) -> pl.DataFrame | None:
    header = find_header_row(grid, [r"utility number", r"megawatthours"])
    if header is None:
        return None  # not a sales table (the "Decoupled" sheet lists rate-decoupling flags)
    measure_row = _row_above(grid, header, r"^customers$")
    sector_row = _row_above(grid, measure_row, r"^residential$")
    units, measures, sectors = grid.row(header), grid.row(measure_row), grid.row(sector_row)
    names, current = [], None
    for i, unit in enumerate(units):
        sector = clean_label(sectors[i]).lower()
        if sector in _SECTORS:
            current = sector
        measure = clean_label(measures[i]).lower()
        if measure in _MEASURES and current:
            name, expected_unit = _MEASURES[measure]
            if clean_label(unit).lower() != expected_unit:
                raise ValueError(f"{current} {measure}: unit {unit!r}, expected {expected_unit!r}")
            names.append(f"{current}__{name}")
        else:
            names.append(snake(unit) if clean_label(unit) else f"col_{i}")
    body = body_with_names(grid, header, names)
    body = body.rename({c: "data_type" for c in body.columns if c.startswith("data_type")})
    body = clean_strings(body).filter(pl.col("state") == "TX")
    found = [s for s in _SECTORS if f"{s}__sales_mwh" in body.columns]
    if not found:
        raise ValueError("no sector columns under the sales header")
    body = type_columns(
        body,
        floats=[f"{s}__{m}" for s in found for m in ("revenue_thousand_usd", "sales_mwh")],
        ints=[f"{s}__customers" for s in found],
    )
    common = [
        *_common_columns(body, data_year, early),
        *(pl.col(c) if c in body.columns else pl.lit(None, pl.String).alias(c)
          for c in ("part", "service_type", "data_type", "state", "ownership", "ba_code")),
    ]  # fmt: skip
    return pl.concat(
        [
            body.select(
                *common,
                pl.lit(sector).alias("sector"),
                pl.col(f"{sector}__revenue_thousand_usd").alias("revenue_thousand_usd"),
                pl.col(f"{sector}__sales_mwh").alias("sales_mwh"),
                pl.col(f"{sector}__customers").alias("customers"),
            )
            for sector in found
        ],
        how="vertical",
    )


def _sales(data: bytes, key: str, data_year: int, early: bool) -> pl.DataFrame:
    book = _workbook(data, "Sales_Ult_Cust", key)
    frames = [df for s in sheet_names(book) if (df := _sales_sheet(read_grid(book, s), data_year, early)) is not None]
    if not frames:
        raise ValueError(f"{key}: no sales sheet in Sales_Ult_Cust")
    return pl.concat(frames, how="vertical")


def parse_sales(f: RawFile) -> pl.DataFrame:
    data_year, early = _release(f)
    return _sales(f.read_bytes(), f.key, data_year, early)


# --- Utility_Data ---------------------------------------------------------------------------------------

_GROUPS = ((r"\bNERC\b", "nerc_"), (r"\bRTOs?\b", "rto_"), (r"activit", "activity_"))


def _ba_codes(sales: pl.DataFrame) -> pl.DataFrame:
    """Per utility: the balancing authority with most Texas customers, and all of them (JSON array)."""
    totals = (
        sales.filter((pl.col("sector") == "total") & pl.col("ba_code").is_not_null())
        .group_by("utility_id", "ba_code")
        .agg(pl.col("customers").sum().alias("n"))
    )
    out = totals.group_by("utility_id").agg(
        pl.col("ba_code").sort_by("n", "ba_code", descending=[True, False]).first().alias("ba_code"),
        pl.col("ba_code").unique().sort().alias("ba_codes"),
    )
    return out.with_columns(
        pl.col("ba_codes").map_elements(lambda codes: json.dumps(list(codes)), return_dtype=pl.String)
    )


def parse_utility(f: RawFile) -> pl.DataFrame:
    data_year, early = _release(f)
    data = f.read_bytes()
    book = _workbook(data, "Utility_Data", f.key)
    frames = []
    for sheet in sheet_names(book):
        grid = read_grid(book, sheet)
        header = find_header_row(grid, [r"utility number", r"ownership", r"nerc region"])
        if header is None:
            raise ValueError(f"{f.key}: no Utility_Data header in sheet {sheet!r}")
        body = body_with_names(grid, header, group_header(grid, header, _GROUPS))
        body = clean_strings(body).filter(pl.col("state") == "TX")
        flags = [c for c in body.columns if c.startswith(("nerc_", "rto_", "activity_")) and c != "nerc_region"]
        body = type_columns(body, flags=flags)
        frames.append(
            body.select(
                *_common_columns(body, data_year, early),
                pl.col("state"),
                pl.col("ownership_type"),
                pl.col("nerc_region"),
                *flags,
            )
        )
    df = pl.concat(frames, how="diagonal_relaxed")
    return df.join(_ba_codes(_sales(data, f.key, data_year, early)), on="utility_id", how="left")


# --- Service_Territory ----------------------------------------------------------------------------------


def parse_territory(f: RawFile) -> pl.DataFrame:
    data_year, early = _release(f)
    book = _workbook(f.read_bytes(), "Service_Territory", f.key)
    frames = []
    for sheet in sheet_names(book):
        grid = read_grid(book, sheet)
        header = find_header_row(grid, [r"utility number", r"^county$"])
        if header is None:
            raise ValueError(f"{f.key}: no Service_Territory header in sheet {sheet!r}")
        body = with_header(grid, header)
        if body.height == 0:
            continue  # a sheet with a header and no rows (drops every column)
        body = clean_strings(body).filter(pl.col("state") == "TX")
        frames.append(
            body.select(
                *_common_columns(body, data_year, early),
                pl.col("state"),
                pl.col("county").alias("county_name"),
                tx_county_fips("county").alias("county_fips"),
            )
        )
    if not frames:
        raise ValueError(f"{f.key}: no rows in Service_Territory")
    return pl.concat(frames, how="vertical").unique(maintain_order=True)


def _is_release_zip(f: RawFile) -> bool:
    return bool(_ZIP.match(f.name))


DATASETS = [
    Dataset(
        name="eia861_sales",
        target="postgres",
        mode="by_file",
        description="EIA-861 Sales_Ult_Cust, Texas rows: revenue (thousand USD), sales (MWh) and customers by "
        "utility, part, balancing authority and sector (residential, commercial, industrial, transportation, "
        "total), one data year per file; early releases flagged.",
        parse=parse_sales,
        inputs=_is_release_zip,
        select=latest_by_url,
    ),
    Dataset(
        name="eia861_utility",
        target="postgres",
        mode="by_file",
        description="EIA-861 Utility_Data, Texas rows: ownership type, NERC region, other NERC regions, RTOs "
        "and activities (booleans), plus the balancing authority from Sales_Ult_Cust; one data year per file.",
        parse=parse_utility,
        inputs=_is_release_zip,
        select=latest_by_url,
    ),
    Dataset(
        name="eia861_service_territory",
        target="postgres",
        mode="by_file",
        description="EIA-861 Service_Territory, Texas rows: the counties each utility serves (name and "
        "county_fips), one data year per file.",
        parse=parse_territory,
        inputs=_is_release_zip,
        select=latest_by_url,
    ),
]
