"""Shared helpers for the resource-adequacy parsers (CDR, MORA, RTP, TPIT).

ERCOT publishes these reports as presentation-style workbooks: titles, notes and several small tables per
sheet, with the header of each table somewhere in the middle of the sheet. :func:`extract_tables` finds
every labelled table in a sheet without assuming positions:

- a *data row* has a text label and, to its right, at least one number that is not a year;
- a *header row* has no such numbers, and has header cells (text, years, ``2026/2027``, ``Summer 2025``)
  above columns where the data rows below hold numbers;
- consecutive header rows stack (``Summer`` / ``2026`` / ``Peak Load Hour:``); cells of a header row are
  carried to the right over merged-looking gaps, but never across a label column (side-by-side tables);
- text-only rows between tables are titles and section labels.

Each number becomes a :class:`TableValue` with its row label, the stacked header texts of its column and
the surrounding titles, so a parser can map it to its own long format.

Legacy ``.xls`` files are read with ``xlrd``: calamine (``fastexcel``) returns wrong cached values for some
BIFF formula cells in the older CDR workbooks (e.g. zeros in the 2006 CDR summary header)."""

from __future__ import annotations

import io
import re
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

import fastexcel
import polars as pl

Cell = str | float | None

_NULL_TEXT = {"", "-", "--", "—", "n/a", "na", "#n/a", "null", "tbd", "*", "#value!", "#ref!", "#div/0!", "nan"}
_NUMBER = re.compile(r"^[-+]?(\d[\d,]*\.?\d*|\.\d+)([eE][-+]?\d+)?$")
_YEAR = re.compile(
    r"^(?:(summer|winter|spring|fall)\s+)?((?:19|20)\d{2})(?:\s*/\s*((?:19|20)?\d{2}))?$", re.IGNORECASE
)
_SEASON_WORD = re.compile(r"\b(summer|winter|spring|fall)\b", re.IGNORECASE)
MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october",
     "november", "december"], start=1)}
_MONTH_YEAR = re.compile(
    r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|june?|july?|aug(?:ust)?|sep(?:t(?:ember)?)?|"
    r"oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)[\s_\-.,]*((?:19|20)\d{2})\b",
    re.IGNORECASE,
)


# --- reading ------------------------------------------------------------------------------------------


def clean_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value)).strip()


def _xls_cell(sheet, book, r: int, c: int) -> Cell:
    import xlrd

    kind = sheet.cell_type(r, c)
    value = sheet.cell_value(r, c)
    if kind in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK, xlrd.XL_CELL_ERROR):
        return None
    if kind == xlrd.XL_CELL_DATE:
        try:
            return xlrd.xldate_as_datetime(value, book.datemode).isoformat(sep=" ")
        except (ValueError, OverflowError):
            return float(value)
    if kind == xlrd.XL_CELL_NUMBER:
        return float(value)
    if kind == xlrd.XL_CELL_BOOLEAN:
        return str(bool(value)).upper()
    return str(value)


class Workbook:
    """A workbook opened once; sheets come back as lists of rows of raw cells (``str``, ``float`` for xls
    numbers, ``None`` for empty). xlsx cells come back as text (numbers included) from calamine."""

    def __init__(self, source: Path | bytes, *, name: str = "") -> None:
        self.name = name or (source.name if isinstance(source, Path) else "")
        self.is_xls = self.name.lower().endswith(".xls")
        if self.is_xls:
            import xlrd

            data = source.read_bytes() if isinstance(source, Path) else source
            self._book = xlrd.open_workbook(file_contents=data, on_demand=True, logfile=io.StringIO())
            self.sheet_names = self._book.sheet_names()
        else:
            self._reader = fastexcel.read_excel(source)
            self.sheet_names = self._reader.sheet_names

    def rows(self, sheet: str) -> list[list[Cell]]:
        if self.is_xls:
            s = self._book.sheet_by_name(sheet)
            return [[_xls_cell(s, self._book, r, c) for c in range(s.ncols)] for r in range(s.nrows)]
        df = self._reader.load_sheet(sheet, header_row=None, dtypes="string").to_polars()
        return [list(row) for row in df.iter_rows()]

    def frame(self, sheet: str) -> pl.DataFrame:
        """The sheet as a grid of strings ``c0..cN`` (like ``tabular.read_grid``)."""
        if self.is_xls:
            rows = self.rows(sheet)
            width = max((len(r) for r in rows), default=0)
            cols = {f"c{i}": [_as_text(r[i]) if i < len(r) else None for r in rows] for i in range(width)}
            return pl.DataFrame(cols, schema={k: pl.String for k in cols})
        df = self._reader.load_sheet(sheet, header_row=None, dtypes="string").to_polars()
        return df.rename({old: f"c{i}" for i, old in enumerate(df.columns)})


def _as_text(value: Cell) -> str | None:
    if value is None:
        return None
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else repr(value)
    return value


def as_number(value: Cell) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = value.strip()
    if text.lower() in _NULL_TEXT:
        return None
    text = text.replace("$", "").replace(" ", "")
    negative = text.startswith("(") and text.endswith(")")
    text = text.strip("()")
    if not _NUMBER.match(text):
        return None
    try:
        number = float(text.replace(",", ""))
    except ValueError:
        return None
    return -number if negative else number


def as_label(value: Cell) -> str | None:
    """Cleaned text of a non-numeric, non-empty cell."""
    if value is None or isinstance(value, (int, float)):
        return None
    text = clean_text(value)
    if text.lower() in _NULL_TEXT or as_number(text) is not None:
        return None
    return text


def iter_zip_members(data: bytes, *, suffixes: tuple[str, ...]) -> Iterator[tuple[str, bytes]]:
    """Every member of a zip (recursing into nested zips) whose name ends with one of ``suffixes``."""
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for info in sorted(zf.infolist(), key=lambda i: i.filename):
            if info.is_dir():
                continue
            name = info.filename
            lower = name.lower()
            if lower.endswith(".zip"):
                for inner, payload in iter_zip_members(zf.read(info), suffixes=suffixes):
                    yield f"{name}/{inner}", payload
            elif lower.endswith(suffixes) and not Path(name).name.startswith(("~$", "._")):
                yield name, zf.read(info)


# --- periods ------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Period:
    year: int
    season: str | None  # summer | winter | spring | fall
    split: bool  # "2026/2027" (a winter spanning two years)
    label: str


def period_token(value: Cell) -> Period | None:
    """A header cell that names a year: ``2026``, ``2026.0``, ``2026/2027``, ``2025/26``, ``Summer 2025``."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        value = float(value)
        if value.is_integer() and 1990 <= value <= 2070:
            return Period(int(value), None, False, str(int(value)))
        return None
    text = clean_text(value)
    number = as_number(text)
    if number is not None:
        return period_token(number)
    m = _YEAR.match(text)
    if not m:
        return None
    season = m.group(1).lower() if m.group(1) else None
    split = m.group(3) is not None
    if split and season is None:
        season = "winter"
    return Period(int(m.group(2)), season, split, text)


def season_in(text: str | None) -> str | None:
    if not text:
        return None
    m = _SEASON_WORD.search(text)
    return m.group(1).lower() if m else None


def month_year(text: str | None) -> date | None:
    """First ``<Month> <YYYY>`` in a text (``December 2025``, ``May2024``, ``Dec_2019``)."""
    if not text:
        return None
    m = _MONTH_YEAR.search(text)
    if not m:
        return None
    word = m.group(1).lower()
    month = next(i for name, i in MONTHS.items() if name.startswith(word[:3]))
    return date(int(m.group(2)), month, 1)


# --- table extraction ---------------------------------------------------------------------------------


@dataclass
class TableValue:
    row: int  # 0-based row in the sheet
    col: int
    value: float
    label: str  # the row label (nearest text cell to the left)
    label_col: int  # column of that label (tables can sit side by side)
    headers: tuple[str, ...]  # stacked header texts over the column, top to bottom
    periods: tuple[Period, ...]  # year tokens among the headers
    section: str | None  # last text-only row inside the table
    title: str | None  # text rows right above the table and the header row's own label
    context_season: str | None  # last season word seen in titles above (sticky within the sheet)
    entity: str | None  # e.g. the utility of a per-utility block ("AEP | Year | 2002 ...")
    table_index: int


@dataclass
class _Table:
    index: int
    header_rows: list[int]
    title_rows: list[dict[int, str]]
    has_data: bool = False
    section: dict[int, str] | None = None


@dataclass
class _Row:
    cells: dict[int, Cell]
    texts: dict[int, str] = field(default_factory=dict)
    numbers: dict[int, float] = field(default_factory=dict)
    periods: dict[int, Period] = field(default_factory=dict)


def _analyse(raw: list[Cell]) -> _Row:
    row = _Row(cells={})
    for c, v in enumerate(raw):
        if v is None:
            continue
        number = as_number(v)
        text = as_label(v)
        if number is None and text is None:
            continue
        row.cells[c] = v
        period = period_token(v)
        if period is not None:
            row.periods[c] = period
        if number is not None:
            row.numbers[c] = number
        elif text is not None:
            row.texts[c] = text
    return row


def _orderly_years(row: _Row) -> bool:
    """Numeric year cells that look like a header: steps of 0..5 years, allowing a reset per season."""
    years = [row.periods[c].year for c in sorted(row.numbers) if c in row.periods]
    steps = [b - a for a, b in zip(years, years[1:])]
    return all(-40 <= s <= 5 for s in steps)


def _data_numbers(row: _Row) -> dict[int, float]:
    """The numbers of a row that are values, or nothing when the row is a year header. A year header names
    at least two different years (or a period such as ``2026/2027``), in order, and may carry stray zeros
    (formula cells next to it, e.g. ``Fuel Type | 2007 … 2012 | 0``). A single year-like number is a value
    (``2000`` MW in a one-column table)."""
    if not row.numbers:
        return {}
    others = [c for c in row.numbers if c not in row.periods]
    years = {p.year for p in row.periods.values()}
    text_periods = [c for c in row.periods if c not in row.numbers]
    if not (len(years) >= 2 or text_periods) or not _orderly_years(row):
        return row.numbers
    if others and (len(row.periods) < 2 or any(row.numbers[c] != 0 for c in others)):
        return row.numbers
    return {}


def _is_data(row: _Row) -> bool:
    nums = _data_numbers(row)
    return bool(nums) and any(t < max(nums) for t in row.texts)


_YEAR_LABEL = re.compile(r"\(?[a-z]{0,2}\)?\s*year", re.IGNORECASE)


def _texts_between(cells: dict[int, str], lo: int, hi: int) -> list[str]:
    return [t for k, t in sorted(cells.items()) if lo < k <= hi]


def extract_tables(rows: list[list[Cell]], *, lookahead: int = 12) -> list[TableValue]:
    """Every number in a labelled table of the sheet (see the module docstring)."""
    info = [_analyse(r) for r in rows]
    data = [_is_data(r) for r in info]
    n = len(info)

    def header_cols(r: int) -> set[int]:
        row = info[r]
        candidates = set(row.texts) | set(row.periods)
        found: set[int] = set()
        for d in range(r + 1, min(n, r + 1 + lookahead)):
            if not data[d]:
                continue
            nums = _data_numbers(info[d])
            left = min(info[d].texts)
            found.update(c for c in candidates if c in nums and left < c)
        return found

    header = [bool(info[r].cells) and not data[r] and not _data_numbers(info[r]) and bool(header_cols(r))
              for r in range(n)]

    out: list[TableValue] = []
    table: _Table | None = None
    pending: list[dict[int, str]] = []
    context_season: str | None = None
    count = 0

    def note_season(text: str) -> None:
        nonlocal context_season
        s = season_in(text)
        if s:
            context_season = s

    for r in range(n):
        row = info[r]
        if not row.cells:
            continue
        if header[r]:
            if table is None or table.has_data:
                table = _Table(count, [r], pending)
                count += 1
            else:
                table.header_rows.append(r)
                table.title_rows.extend(pending)
            first = min(header_cols(r))
            for t in [*(t for p in pending for t in p.values()), *(t for c, t in row.texts.items() if c < first)]:
                note_season(t)
            pending = []
            continue
        if data[r] and table is not None:
            nums = _data_numbers(row)
            for c, value in sorted(nums.items()):
                left = [k for k in row.texts if k < c]
                if not left:
                    continue
                seg = max(left)
                prev = max((k for k in nums if k < seg), default=-1)
                parts: list[str] = []
                periods: list[Period] = []
                own: list[str] = []
                entity = None
                for h in table.header_rows:
                    hr = info[h]
                    k = c
                    while k > seg and k not in hr.cells:
                        k -= 1
                    if k > seg:
                        cell = hr.cells[k]
                        if k in hr.periods:
                            periods.append(hr.periods[k])
                            text = hr.periods[k].label
                        else:
                            text = hr.texts.get(k) or _as_text(cell)
                        if text and text not in parts:
                            parts.append(text)
                    labels = _texts_between(hr.texts, prev, seg)
                    if len(labels) >= 2 and _YEAR_LABEL.fullmatch(labels[-1]):
                        entity = labels[0]
                    own.extend(t for t in labels if not _YEAR_LABEL.fullmatch(t) and t not in own)
                if not parts:
                    continue
                titles = [" ".join(_texts_between(tr, prev, c)) for tr in table.title_rows]
                title = " | ".join(t for t in [*titles, *own] if t) or None
                section = " ".join(_texts_between(table.section, prev, c)) if table.section else None
                out.append(
                    TableValue(
                        row=r, col=c, value=value, label=row.texts[seg], label_col=seg, headers=tuple(parts),
                        periods=tuple(periods), section=section or None, title=title,
                        context_season=context_season, entity=entity, table_index=table.index,
                    )
                )
            # a label with no numbers in its own segment is a section label for that segment only
            # (side-by-side tables: "Reserve Margins (%)" on the left while the right table has data)
            texts = sorted(row.texts)
            for i, t in enumerate(texts):
                end = texts[i + 1] if i + 1 < len(texts) else 10**9
                if not any(t < k < end for k in nums):
                    table.section = {**(table.section or {}), t: row.texts[t]}
            table.has_data = True
            pending = []
            continue
        if row.texts and not row.numbers:
            for t in row.texts.values():
                note_season(t)
            if table is not None and table.has_data:
                table.section = dict(row.texts)
            pending.append(dict(row.texts))
    return out


def to_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None
