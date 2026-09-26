"""Helpers for messy spreadsheets and text tables: read every cell as text, find the header row by what it
says (ERCOT moves headers around between vintages), then type the columns explicitly."""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import date, timedelta
from pathlib import Path

import fastexcel
import polars as pl

_NULL_TOKENS = ["", "-", "--", "—", "n/a", "N/A", "NA", "na", "#N/A", "null", "NULL", "TBD", "tbd", "*"]


def sheet_names(source: Path | bytes) -> list[str]:
    return fastexcel.read_excel(source).sheet_names


def read_grid(source: Path | bytes, sheet: str | int, *, n_rows: int | None = None) -> pl.DataFrame:
    """One sheet as a grid of strings (no header), columns ``c0..cN``. Works for xlsx, xlsm, xlsb and xls."""
    reader = fastexcel.read_excel(source)
    df = reader.load_sheet(sheet, header_row=None, dtypes="string", n_rows=n_rows).to_polars()
    return df.rename({old: f"c{i}" for i, old in enumerate(df.columns)})


def read_grids(source: Path | bytes, *, sheets: Sequence[str] | None = None) -> dict[str, pl.DataFrame]:
    reader = fastexcel.read_excel(source)
    out = {}
    for name in reader.sheet_names:
        if sheets is not None and name not in sheets:
            continue
        df = reader.load_sheet(name, header_row=None, dtypes="string").to_polars()
        out[name] = df.rename({old: f"c{i}" for i, old in enumerate(df.columns)})
    return out


def clean_label(value: object) -> str:
    """Collapse whitespace and line breaks in a header cell."""
    return re.sub(r"\s+", " ", str(value or "")).strip()


def snake(value: object) -> str:
    """``'Projected COD (mm/dd/yyyy)'`` → ``'projected_cod_mm_dd_yyyy'``."""
    text = clean_label(value).lower().replace("%", " pct ").replace("#", " num ").replace("&", " and ")
    text = re.sub(r"[^0-9a-z]+", "_", text).strip("_")
    return text or "col"


def find_header_row(grid: pl.DataFrame, patterns: Sequence[str], *, max_scan: int = 80) -> int | None:
    """Index of the first row where every regex in ``patterns`` matches some cell (case-insensitive)."""
    compiled = [re.compile(p, re.IGNORECASE) for p in patterns]
    for i, row in enumerate(grid.head(max_scan).iter_rows()):
        cells = [clean_label(c) for c in row if c is not None]
        if all(any(p.search(c) for c in cells) for p in compiled):
            return i
    return None


def with_header(grid: pl.DataFrame, header_row: int, *, rename=snake) -> pl.DataFrame:
    """Rows below ``header_row`` with its cells as column names (empty and duplicate names made unique);
    columns and rows that are entirely empty are dropped."""
    header = list(grid.row(header_row))
    names, seen = [], {}
    for i, raw in enumerate(header):
        name = rename(raw) if raw is not None and clean_label(raw) else f"col_{i}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        names.append(name)
    body = grid.slice(header_row + 1)
    body.columns = names
    body = body.select([c for c in body.columns if body[c].is_not_null().any()])
    return body.filter(~pl.all_horizontal(pl.all().is_null()))


def num(expr: pl.Expr | str) -> pl.Expr:
    """Text → Float64: strips thousands separators, currency, percent signs and spaces; placeholders
    such as ``-``, ``N/A`` or ``TBD`` become null; anything else unparseable becomes null."""
    e = pl.col(expr) if isinstance(expr, str) else expr
    s = e.cast(pl.String).str.strip_chars()
    s = pl.when(s.is_in(_NULL_TOKENS)).then(None).otherwise(s)
    s = s.str.replace_all(r"[,$\s]", "").str.replace(r"^\((.*)\)$", "-$1").str.replace(r"%$", "")
    return s.cast(pl.Float64, strict=False)


def integer(expr: pl.Expr | str) -> pl.Expr:
    return num(expr).round(0).cast(pl.Int64, strict=False)


_EXCEL_EPOCH = date(1899, 12, 30)


def excel_date(expr: pl.Expr | str, *, formats: Sequence[str] = ("%m/%d/%y", "%m/%d/%Y")) -> pl.Expr:
    """Text cell → Date: Excel serial numbers, ISO strings (what fastexcel gives for date cells) or the
    listed formats; anything else becomes null."""
    e = pl.col(expr) if isinstance(expr, str) else expr
    s = e.cast(pl.String).str.strip_chars()
    serial = s.cast(pl.Float64, strict=False)
    from_serial = pl.lit(_EXCEL_EPOCH) + pl.duration(days=serial.floor().cast(pl.Int64, strict=False))
    iso = s.str.slice(0, 10).str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    parsed = [iso, *(s.str.strptime(pl.Date, f, strict=False) for f in formats)]
    return (
        pl.when(serial.is_between(1, 80000)).then(from_serial.cast(pl.Date)).otherwise(pl.coalesce(parsed))
    )


def excel_serial_to_date(serial: float) -> date:
    return _EXCEL_EPOCH + timedelta(days=int(serial))
