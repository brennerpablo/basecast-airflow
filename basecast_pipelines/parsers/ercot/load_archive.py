"""Hourly Load Data Archives → ``ercot_load_hourly_wz`` (hourly native load by weather zone plus the ERCOT
total, long format).

One workbook per year: xls for 2003-2015, a zip holding one xlsx since 2016. Two layouts, both read by header:

- 2003-2016: ``Hour_End, COAST, EAST, FAR_WEST, NORTH, NORTH_C, SOUTHERN, SOUTH_C, WEST, ERCOT``; the hour is
  an Excel datetime with float noise (``01:59:59.997``), midnight is hour ending 24 of the previous day, the
  spring-forward day skips ``02:00`` and the repeated fall hour is a plain duplicate ``02:00`` row;
- 2017+: ``Hour Ending`` (or ``HourEnding``), ``COAST … WEST, ERCOT`` with contract codes; the hour is text
  ``MM/DD/YYYY HH:MM`` with ``24:00``, the spring-forward day skips ``03:00`` and the repeated fall hour reads
  ``02:00 DST``.

``ts_utc`` is the **end** of the hour-ending interval in UTC; ``operating_date`` + ``hour_ending`` (1–24) and
``hour_ending_local`` (the same label as a naive wall-clock datetime, HE 24 → next midnight) keep the published
local labels, and ``dst_flag`` marks the repeated November hour (see ``_load_common``). Hours with no value
are dropped (the 2016 file has one: HE 24 of 2016-11-06).

ERCOT overwrites the current-year file monthly, so the same year arrives in several ``dt=`` snapshots. The
table is written ``by_key`` on (``ts_utc``, ``weather_zone``) and files are processed oldest snapshot first, so
the latest snapshot wins. ``ercot_load_wz_daily`` (NP6-345-CD) writes the same table for the days after the
last archive update; ``source`` says which one wrote each row.
"""

from __future__ import annotations

import io
import zipfile

import polars as pl

from basecast_pipelines.parsers.ercot._load_common import ARCHIVE_TABLE, KEY, combined_load_table, hourly_long, parse_hour_labels, zone_columns
from basecast_pipelines.processing.core import Dataset, RawFile
from basecast_pipelines.processing.tabular import find_header_row, read_grid, sheet_names, with_header

SOURCE_ID = "ercot_native_load"
_EXCEL = (".xls", ".xlsx")


def _workbook(f: RawFile) -> bytes:
    """The workbook bytes: the file itself, or the single spreadsheet inside the zip."""
    data = f.read_bytes()
    if f.suffix != ".zip":
        return data
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        members = [n for n in z.namelist() if n.lower().endswith(_EXCEL) and not n.startswith("__MACOSX")]
        if len(members) != 1:
            raise ValueError(f"{f.key}: expected one spreadsheet in the zip, found {members}")
        return z.read(members[0])


def parse_native_load(f: RawFile) -> pl.DataFrame:
    book = _workbook(f)
    for sheet in sheet_names(book):
        grid = read_grid(book, sheet)
        header = find_header_row(grid, [r"^hour", r"^coast$", r"^ercot$"], max_scan=20)
        if header is not None:
            break
    else:
        raise ValueError(f"{f.key}: no sheet with an hour / COAST / ERCOT header")
    body = with_header(grid, header, rename=lambda c: str(c).strip())
    hour_col = next(c for c in body.columns if c.lower().startswith("hour"))
    zones = zone_columns(body.columns)
    body = body.filter(pl.col(hour_col).is_not_null())
    labels = parse_hour_labels(body[hour_col])
    frame = labels.hstack(body.select(list(zones)).get_columns())
    return hourly_long(frame, zones, source=SOURCE_ID)


DATASETS = [
    Dataset(
        name=ARCHIVE_TABLE,
        target="postgres",
        mode="by_key",
        key=KEY,
        description=(
            "Hourly native load by ERCOT weather zone plus the ERCOT total (MW), long format; ts_utc is the end "
            "of the hour-ending interval. Hourly Load Data Archives 2003+, completed by NP6-345-CD."
        ),
        parse=parse_native_load,
        inputs=lambda f: f.suffix in {".xls", ".xlsx", ".zip"},
    ),
]

SQL_DATASETS = [combined_load_table()]
