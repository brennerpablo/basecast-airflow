"""Hourly LTLF workbooks: the published hourly forecasts (one column per zone and component) and the
weather-year scenario workbooks (one workbook per zone, one ``Pred_<year>`` column per weather year).

Time: the workbooks give ``year, month, day, hour`` with ``hour`` = hour ending 1-24 in Central Prevailing
Time. In ERCOT's convention the spring-forward day has no hour ending 2 (23 rows) and the fall-back day
repeats hour ending 2 (25 rows). Each row keeps its local labels (``operating_date``, ``hour_ending``,
``hour_ending_local`` like ``2025-11-02 02:00``) plus ``dst_flag`` (true on the second occurrence of a
repeated hour) and ``ts_utc``, the UTC instant at which the hour ends. Rows whose label cannot exist
(hour ending 2 on a spring-forward day, or a repeated hour on any day other than a fall-back day, both seen
in the 2022 workbook) keep their labels and values with a null ``ts_utc``.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import fastexcel
import polars as pl

from basecast_pipelines.parsers.ercot._ltlf_tables import is_hourly_header, norm, region_of
from basecast_pipelines.processing.tabular import num

log = logging.getLogger(__name__)

TIME_ZONE = "America/Chicago"
TIME_COLUMNS = ["operating_date", "hour_ending", "hour_ending_local", "dst_flag", "ts_utc"]

# component columns of ltlf_hourly; ``mw`` is the published total (net load when components are given)
COMPONENTS = [
    "mw",
    "gross_mw",
    "base_economic_mw",
    "ev_mw",
    "pv_mw",
    "lfl_mw",
    "contracts_mw",
    "officer_letters_mw",
    "contracted_lfl_mw",
    "officer_letters_lfl_mw",
]
_COMPONENT_TOKENS = {
    "": "mw",
    "net": "mw",
    "net15": "mw",
    "gross": "gross_mw",
    "base": "base_economic_mw",
    "baseeconomic": "base_economic_mw",
    "ev": "ev_mw",
    "pv": "pv_mw",
    "pvpred": "pv_mw",
    "lfl": "lfl_mw",
    "lfl15pct": "lfl_mw",
    "lflforecast": "lfl_mw",
    "contracts": "contracts_mw",
    "officerletters": "officer_letters_mw",
    "contractedlfl": "contracted_lfl_mw",
    "officerletterslfl": "officer_letters_lfl_mw",
}
_REGION_TOKENS = {"coast", "east", "fwest", "ncent", "north", "scent", "south", "west", "ercot", "total"}
# weather-scenario components that do not depend on the weather year
WEATHER_COMPONENTS = {"ev": "ev", "pv": "pv", "lflforecast": "lfl", "lfl": "lfl", "contracts": "contracts",
                      "officerletters": "officer_letters"}


# Parsing an hourly sheet takes most of a task's memory (calamine holds every cell: ~0.9 GB for the 48 MB
# xlsb), so big workbooks are read once with the header on the first row, and only scanned first when not.
BIG_WORKBOOK_BYTES = 5_000_000
_TIME_NAMES = {"year", "month", "day", "hour"}


def header_row(path: Path, sheet: str) -> int | None:
    """Index of the ``year | month | day | hour`` header in the first rows of a sheet, or None."""
    grid = fastexcel.read_excel(path).load_sheet(sheet, header_row=None, dtypes="string", n_rows=10).to_polars()
    rows = [[(str(v).strip() or None) if v is not None else None for v in r] for r in grid.iter_rows()]
    if not is_hourly_header(rows):
        return None
    for i, row in enumerate(rows):
        if _TIME_NAMES <= {norm(v) for v in row if v}:
            return i
    return None


def load_hourly(path: Path, sheet: str, *, big: bool) -> pl.DataFrame | None:
    """The sheet with typed columns (fastexcel infers numbers and dates), ``year, month, day, hour``
    renamed ``_year...`` and the time columns added; None when the sheet has no hourly header."""
    reader = fastexcel.read_excel(path)
    df = None
    if big:
        df = reader.load_sheet(sheet, header_row=0).to_polars()
        if not _TIME_NAMES <= {norm(c) for c in df.columns}:
            df = None
    if df is None:
        header = header_row(path, sheet)
        if header is None:
            return None
        df = reader.load_sheet(sheet, header_row=header).to_polars()
    df = df.rename({c: f"_{norm(c)}" for c in df.columns if norm(c) in _TIME_NAMES})
    missing = {"_year", "_month", "_day", "_hour"} - set(df.columns)
    if missing:
        raise ValueError(f"{sheet}: hourly sheet without {sorted(missing)}")
    return with_time(df.filter(pl.col("_year").is_not_null() & pl.col("_hour").is_not_null()))


def with_time(df: pl.DataFrame) -> pl.DataFrame:
    """Add the time columns (see the module docstring) from ``_year, _month, _day, _hour``."""
    y, m, d, he = (pl.col(c).cast(pl.Float64).round(0).cast(pl.Int32) for c in ("_year", "_month", "_day", "_hour"))
    df = df.with_columns(pl.date(y, m, d).alias("operating_date"), he.cast(pl.Int8).alias("hour_ending"))
    local_midnight = pl.col("operating_date").cast(pl.Datetime("us")).dt.replace_time_zone(TIME_ZONE)
    df = df.with_columns(
        (pl.int_range(pl.len()).over(["operating_date", "hour_ending"]) > 0).alias("dst_flag"),
        local_midnight.dt.convert_time_zone("UTC").alias("_day_start"),
        (pl.col("operating_date").cast(pl.Datetime("us")) + pl.duration(days=1))
        .dt.replace_time_zone(TIME_ZONE).dt.convert_time_zone("UTC").alias("_next_start"),
    )
    hours = ((pl.col("_next_start") - pl.col("_day_start")).dt.total_minutes() // 60).cast(pl.Int32)
    h = pl.col("hour_ending").cast(pl.Int32)
    offset = (
        pl.when(pl.col("dst_flag") & ~((hours == 25) & (h == 2))).then(None)  # repeated hour on a normal day
        .when(hours == 23).then(pl.when(h == 2).then(None).when(h > 2).then(h - 1).otherwise(h))
        .when(hours == 25).then(pl.when((h > 2) | pl.col("dst_flag")).then(h + 1).otherwise(h))
        .otherwise(h)
    )
    df = df.with_columns(
        (pl.col("_day_start") + pl.duration(hours=offset)).alias("ts_utc"),
        (pl.col("operating_date").dt.strftime("%Y-%m-%d") + pl.lit(" ")
         + pl.col("hour_ending").cast(pl.String).str.zfill(2) + pl.lit(":00")).alias("hour_ending_local"),
    )
    nulls = df.filter(pl.col("ts_utc").is_null()).height
    if nulls:
        log.warning("%d hourly rows carry hour labels that cannot exist in %s; ts_utc left null", nulls, TIME_ZONE)
    return df.drop("_day_start", "_next_start")


def split_column(name: str) -> tuple[str, str] | None:
    """``base_economic_coast`` → (COAST, base_economic_mw); ``Total_PV`` → (ERCOT, pv_mw); None if the
    column is not a zone/component column."""
    tokens = [t for t in re.split(r"[^0-9a-z]+", name.lower().replace("%", "pct")) if t]
    region_token = next((t for t in tokens if t in _REGION_TOKENS), None)
    if region_token is None:
        return None
    rest = "".join(t for t in tokens if t != region_token)
    component = _COMPONENT_TOKENS.get(rest)
    if component is None:
        raise ValueError(f"unknown hourly column {name!r}")
    region = "ERCOT" if region_token == "total" else region_of(region_token)
    assert region is not None
    return region, component


def as_float(df: pl.DataFrame, column: str) -> pl.Expr:
    dtype = df.schema[column]
    return (num(column) if dtype == pl.String else pl.col(column).cast(pl.Float64)).alias(column)


def zone_component_frames(df: pl.DataFrame, constants: list[pl.Expr]) -> list[pl.DataFrame]:
    """One frame per region: ``constants`` (vintage, scenario...), region, the time columns and every
    component column (null when the workbook has none). Frames share the time columns, no copies."""
    by_region: dict[str, dict[str, str]] = {}
    for c in df.columns:
        if c.startswith("_") or c in TIME_COLUMNS or norm(c) == "date":
            continue
        parsed = split_column(c)
        if parsed is None:
            raise ValueError(f"unexpected hourly column {c!r}")
        region, component = parsed
        if component in by_region.setdefault(region, {}):
            raise ValueError(f"two hourly columns for {region} {component}: {by_region[region][component]!r}, {c!r}")
        by_region[region][component] = c
    frames = []
    for region, cols in by_region.items():
        values = [
            as_float(df, cols[comp]).alias(comp) if comp in cols else pl.lit(None, pl.Float64).alias(comp)
            for comp in COMPONENTS
        ]
        frames.append(
            df.select(
                *constants,
                pl.lit("ercot" if region == "ERCOT" else "weather_zone").alias("region_type"),
                pl.lit(region).alias("region_id"),
                *TIME_COLUMNS,
                *values,
            )
        )
    return frames
