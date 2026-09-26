"""Shared helpers for the ERCOT market parsers (fuel mix, settlement point prices).

ERCOT publishes these series in local time (America/Chicago) by *interval ending*: hour ending ``24:00``
closes the operating day; on the spring-forward day hour ending 03:00 does not exist (the published day
goes HE 01, 02, 04, ...), and on the fall-back day hour ending 02:00 happens twice, the second pass
flagged as repeated. Parsers turn the published label into the local wall-clock *start* of the interval
and hand it to ``to_utc`` with the repeated flag.
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Iterator, Sequence
from datetime import date, datetime, timedelta
from functools import lru_cache

import polars as pl

from basecast_pipelines.config import LOCAL_TZ
from basecast_pipelines.processing.core import RawFile

LOCAL_TZ_NAME = LOCAL_TZ.key
EXCEL_SUFFIXES = (".xlsx", ".xlsm", ".xls")


def to_utc(local_start: pl.Expr, repeated: pl.Expr) -> pl.Expr:
    """Naive local wall-clock start → ``Datetime("us", "UTC")``. At an ambiguous local time (the repeated
    hour) an unflagged row is the first pass (CDT) and a flagged one the second (CST). Non-existent local
    times (the skipped spring hour) raise: they mean the label was misread."""
    ambiguous = pl.when(repeated).then(pl.lit("latest")).otherwise(pl.lit("earliest"))
    return (
        local_start.dt.replace_time_zone(LOCAL_TZ_NAME, ambiguous=ambiguous, non_existent="raise")
        .dt.convert_time_zone("UTC")
        .dt.cast_time_unit("us")
    )


def check_repeated(df: pl.DataFrame, local_start: str, flag: str, where: str) -> None:
    """Rows flagged as the repeated hour must sit at an ambiguous local time (the fall-back hour)."""
    flagged = df.filter(pl.col(flag)).select(local_start)
    if not flagged.height:
        return
    first = pl.col(local_start).dt.replace_time_zone(LOCAL_TZ_NAME, ambiguous="earliest", non_existent="null")
    second = pl.col(local_start).dt.replace_time_zone(LOCAL_TZ_NAME, ambiguous="latest", non_existent="null")
    bad = flagged.filter(first.eq_missing(second))
    if bad.height:
        raise ValueError(f"{where}: {bad.height} rows flagged as repeated hour outside the fall-back hour, "
                         f"e.g. {bad.row(0)[0]}")


@lru_cache(maxsize=None)
def dst_days(year: int) -> tuple[date | None, date | None]:
    """(spring-forward day, fall-back day) of ``year`` in America/Chicago."""
    spring = fall = None
    d = date(year, 1, 1)
    while d.year == year:
        start = datetime(d.year, d.month, d.day, tzinfo=LOCAL_TZ).utcoffset()
        noon = datetime(d.year, d.month, d.day, 12, tzinfo=LOCAL_TZ).utcoffset()
        if noon > start:
            spring = d
        elif noon < start:
            fall = d
        d += timedelta(days=1)
    return spring, fall


def workbooks(f: RawFile) -> Iterator[tuple[str, bytes]]:
    """(name, bytes) of every spreadsheet in a raw file: the file itself, or each member of a zip."""
    if f.suffix == ".zip":
        with zipfile.ZipFile(io.BytesIO(f.read_bytes())) as z:
            for name in sorted(z.namelist()):
                if name.lower().endswith(EXCEL_SUFFIXES) and not name.startswith("__MACOSX/"):
                    yield name, z.read(name)
    elif f.suffix in EXCEL_SUFFIXES:
        yield f.name, f.read_bytes()


def sort_unique(df: pl.DataFrame, key: Sequence[str], where: str) -> pl.DataFrame:
    """``df`` sorted by ``key``; raises when two rows share a key (adjacent once sorted, which costs far
    less memory than hashing the key)."""
    df = df.sort(list(key))
    same = pl.all_horizontal([pl.col(c).eq_missing(pl.col(c).shift(1)) for c in key])
    dup = df.filter(same)
    if dup.height:
        raise ValueError(f"{where}: {dup.height} rows repeat a key {tuple(key)}, e.g. {dup.select(key).row(0)}")
    return df
