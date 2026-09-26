"""Historical settlement point prices for hubs and load zones → ``prices_rtm_hub_lz`` (real time, 15-minute,
EMIL NP6-785-ER) and ``prices_dam_hub_lz`` (day-ahead, hourly, EMIL NP4-180-ER).

Each raw file is a zip holding one yearly workbook, a sheet per month (``Dec_1`` in 2010; ``Jan_1``,
``Jan_2`` ... in 2012-2016, where a month over ~65k rows is split in two sheets; ``Jan`` ... ``Dec`` from
2017; the current year's future months are empty sheets). Headers, read by name:

- RTM: Delivery Date, Delivery Hour, Delivery Interval, Repeated Hour Flag, Settlement Point Name,
  Settlement Point Type, Settlement Point Price;
- DAM: Delivery Date, Hour Ending (``01:00`` ... ``24:00``), Repeated Hour Flag, Settlement Point,
  Settlement Point Price.

Settlement point types in the RTM files: ``HU`` (hubs), ``SH`` (HB_BUSAVG), ``AH`` (HB_HUBAVG), ``LZ`` and
``LZEW`` (every load zone is published twice, as ``LZ`` and as ``LZEW``; the two prices differ by up to a
few $/MWh; the LZEW definition, energy-weighted per its name, is not verified). So the RTM key includes
``settlement_point_type``. The DAM files carry no type column. Some RTM workbooks end a sheet with a
stray footer row (a date, a count and a time, no settlement point); it is dropped.

Time: hour ending in America/Chicago. Hour ending 03:00 is absent on spring-forward days and hour
ending 02:00 appears twice on fall-back days, the second pass with ``Repeated Hour Flag = Y``. The
interval start is ``delivery_date + (hour - 1) h + (interval - 1) × 15 min`` in local time, then UTC.

The yearly file of the current year is republished with every update, so only the newest file per
friendly name (``RTMLZHBSPP_<year>``) is read; ``by_key`` lets it replace the rows of the older one.
"""

from __future__ import annotations

import re

import fastexcel
import polars as pl

from basecast_pipelines.parsers.ercot._market_common import check_repeated, sort_unique, to_utc, workbooks
from basecast_pipelines.processing.core import Dataset, RawFile, latest_by
from basecast_pipelines.processing.tabular import excel_date, find_header_row, num, with_header

SOURCE_ID = "ercot_spp_hist"

_NAME = re.compile(r"(RTM|DAM)LZHBSPP_(\d{4})", re.IGNORECASE)


def _market(f: RawFile) -> str | None:
    market = f.meta.get("market")
    if market:
        return str(market).lower()
    m = _NAME.search(f.name)
    return m.group(1).lower() if m else None


def _friendly_name(f: RawFile) -> str:
    if f.meta.get("friendly_name"):
        return str(f.meta["friendly_name"])
    m = _NAME.search(f.name)
    return m.group(0).upper() if m else f.url


def _column(columns: list[str], pattern: str, where: str, *, required: bool = True) -> str | None:
    found = [c for c in columns if re.search(pattern, c)]
    if len(found) > 1 or (required and not found):
        raise ValueError(f"{where}: expected one column matching {pattern!r}, found {found} in {columns}")
    return found[0] if found else None


def _typed(body: pl.DataFrame, market: str, where: str) -> pl.DataFrame:
    cols = body.columns
    date_c = _column(cols, r"^delivery_date$", where)
    hour_c = _column(cols, r"^(hour_ending|delivery_hour)$", where)
    interval_c = _column(cols, r"^delivery_interval$", where, required=market == "rtm")
    flag_c = _column(cols, r"repeated_hour|dst", where)
    point_c = _column(cols, r"^settlement_point(_name)?$", where)
    type_c = _column(cols, r"^settlement_point_type$", where, required=False)
    price_c = _column(cols, r"price", where)

    body = body.filter(pl.col(point_c).is_not_null())  # footer rows carry no settlement point
    flag = pl.col(flag_c).str.strip_chars().str.to_uppercase()
    exprs = [
        excel_date(date_c, formats=("%m/%d/%Y", "%m/%d/%y")).alias("delivery_date"),
        pl.col(hour_c).str.extract(r"^\s*(\d{1,2})(?::00)?\s*$", 1).cast(pl.Int8, strict=False).alias("delivery_hour"),
    ]
    if interval_c:
        exprs.append(pl.col(interval_c).str.strip_chars().cast(pl.Int8, strict=False).alias("delivery_interval"))
    exprs += [
        pl.when(flag == "Y").then(True).when(flag == "N").then(False).alias("dst_flag"),
        pl.col(point_c).str.strip_chars().str.to_uppercase().alias("settlement_point"),
    ]
    if type_c:
        exprs.append(pl.col(type_c).str.strip_chars().str.to_uppercase().alias("settlement_point_type"))
    exprs.append(num(price_c).alias("price_usd_mwh"))
    out = body.select(exprs)

    bad = ~pl.col("delivery_hour").is_between(1, 24) | pl.col("delivery_date").is_null() | pl.col("dst_flag").is_null()
    if interval_c:
        bad = bad | ~pl.col("delivery_interval").is_between(1, 4)
    bad_rows = out.filter(bad.fill_null(True))
    if bad_rows.height:
        raise ValueError(f"{where}: {bad_rows.height} rows with an unreadable date, hour, interval or flag, "
                         f"e.g. {bad_rows.row(0)}")
    return out


def _parse(f: RawFile, market: str) -> pl.DataFrame | None:
    if _market(f) != market:
        return None  # the other market's file
    frames = []
    for member, data in workbooks(f):
        reader = fastexcel.read_excel(data)
        for sheet in reader.sheet_names:
            grid = reader.load_sheet(sheet, header_row=None, dtypes="string").to_polars()
            grid = grid.rename({old: f"c{i}" for i, old in enumerate(grid.columns)})
            if grid.height == 0:
                continue  # future months of the current year's workbook
            where = f"{f.key}:{member}:{sheet}"
            header = find_header_row(grid, [r"delivery date", r"settlement point"], max_scan=20)
            if header is None:
                raise ValueError(f"{where}: no settlement point price header")
            body = with_header(grid, header)
            if body.height:
                frames.append(_typed(body, market, where))
    if not frames:
        raise ValueError(f"{f.key}: no price rows")
    df = pl.concat(frames)
    minutes = (pl.col("delivery_hour").cast(pl.Int32) - 1) * 60
    if market == "rtm":
        minutes = minutes + (pl.col("delivery_interval").cast(pl.Int32) - 1) * 15
    local = pl.col("delivery_date").cast(pl.Datetime("us")) + pl.duration(minutes=minutes)
    df = df.with_columns(local.alias("_local"))
    check_repeated(df, "_local", "dst_flag", f.key)
    start = "interval_start_utc" if market == "rtm" else "hour_start_utc"
    df = df.with_columns(to_utc(pl.col("_local"), pl.col("dst_flag")).alias(start)).drop("_local")
    key = [start, "settlement_point", *(["settlement_point_type"] if market == "rtm" else [])]
    return sort_unique(df, key, f.key)


def parse_rtm(f: RawFile) -> pl.DataFrame | None:
    return _parse(f, "rtm")


def parse_dam(f: RawFile) -> pl.DataFrame | None:
    return _parse(f, "dam")


_newest_per_year = latest_by(_friendly_name)

DATASETS = [
    Dataset(
        name="prices_rtm_hub_lz",
        target="bigquery",
        mode="by_key",
        description="Real-time settlement point prices for hubs and load zones, every 15-minute interval since "
                    "Dec 2010 (NP6-785-ER), $/MWh; load zones appear as LZ and LZEW.",
        parse=parse_rtm,
        inputs=lambda f: f.suffix == ".zip" and _market(f) == "rtm",
        select=_newest_per_year,
        key=("interval_start_utc", "settlement_point", "settlement_point_type"),
        partition=("interval_start_utc", "MONTH"),
        cluster=("settlement_point",),
    ),
    Dataset(
        name="prices_dam_hub_lz",
        target="bigquery",
        mode="by_key",
        description="Day-ahead settlement point prices for hubs and load zones, every hour since Dec 2010 "
                    "(NP4-180-ER), $/MWh.",
        parse=parse_dam,
        inputs=lambda f: f.suffix == ".zip" and _market(f) == "dam",
        select=_newest_per_year,
        key=("hour_start_utc", "settlement_point"),
        partition=("hour_start_utc", "MONTH"),
        cluster=("settlement_point",),
    ),
]
