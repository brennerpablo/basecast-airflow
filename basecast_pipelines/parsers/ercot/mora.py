"""Monthly Outlook for Resource Adequacy (MORA), editions December 2023 → November 2026 (xlsx; the PDF twin
of each edition returns ``None``, its numbers are in the workbook).

Each edition covers one target month (``MORA_November2026`` is published in early September 2026). Read:

- ``Monthly Outlook``: the deterministic "Loads and Resources (MW)" table, one column per scenario hour
  (``Hour with the Highest Reserve Shortage Risk``, or the expected peak / minimum load hours in the first
  editions);
- ``Capacity by Resource Category``: installed capacity rating and expected available capacity by resource
  category;
- ``PRRM Percentile Results``: the probabilistic model's percentiles (0–100 %) of gross demand, solar and
  wind by hour ending, and of unplanned thermal outages;
- any other sheet with a labelled table (the risk-profile sheets are charts and text in most editions).

Not read: ``Resource Details`` (unit list), ``Historical Comparisons`` (an unfilled template in every
edition checked), ``Cover``, ``Background``, ``CB_DATA_`` (Crystal Ball add-in metadata).

``published_date`` is the date folder of the download URL (``/files/docs/2026/09/03/``), which matches the
posting date on the Resource Adequacy page; ``target_month`` comes from the link text or file name."""

from __future__ import annotations

import re
from datetime import date

import polars as pl

from basecast_pipelines.parsers.ercot._adequacy_common import (
    TableValue,
    Workbook,
    as_label,
    as_number,
    extract_tables,
    month_year,
)
from basecast_pipelines.processing.core import Dataset, RawFile

SOURCE_ID = "ercot_mora"

_SKIP_SHEETS = re.compile(r"^(cb_data_|cover|background|resource details|historical comparisons)$", re.IGNORECASE)
_PRRM = re.compile(r"prrm|percentile", re.IGNORECASE)
_URL_DATE = re.compile(r"/(20\d{2})/(\d{2})/(\d{2})/")
_VERSION = re.compile(r"_v(\d+)\b", re.IGNORECASE)
_FOOTNOTE = re.compile(r"\s*\[\d+\]\s*$")
_FRACTION = re.compile(r"\bratio\b|percent(?!ile)|\(%\)|margin", re.IGNORECASE)
_HOUR = re.compile(r"\b(\d{1,2})(?::00)?\s*([ap])\.?\s*m\b", re.IGNORECASE)


def _hour_ending(text: str | None) -> int | None:
    """``Hour Ending 9 p.m.`` → 21, ``(8 a.m.)`` → 8, ``12 a.m.`` → 24 (ERCOT hour-ending convention)."""
    m = _HOUR.search(text or "")
    if not m:
        return None
    hour = int(m.group(1)) % 12 + (12 if m.group(2).lower() == "p" else 0)
    return hour or 24

SCHEMA = {
    "edition": pl.String,
    "target_month": pl.Date,
    "published_date": pl.Date,
    "sheet": pl.String,
    "table_title": pl.String,
    "section": pl.String,
    "line_item": pl.String,
    "column_label": pl.String,
    "scenario": pl.String,
    "percentile": pl.Float64,
    "hour_ending": pl.Int64,
    "value": pl.Float64,
    "unit": pl.String,
    "row_number": pl.Int64,
    "column_number": pl.Int64,
}


def _edition(f: RawFile) -> tuple[str, date, date | None]:
    target = None
    if f.meta.get("edition_month"):
        target = date.fromisoformat(f.meta["edition_month"])
    target = target or month_year(re.sub(r"[_\-]", " ", f.name))
    if target is None:
        raise ValueError(f"{f.key}: cannot tell the MORA target month")
    m = _URL_DATE.search(f.url or "")
    published = date(int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else None
    version = _VERSION.search(f.name)
    label = f"MORA {target.strftime('%B %Y')}" + (f" v{version.group(1)}" if version else "")
    return label, target, published


def _clean(text: str | None) -> str | None:
    return _FOOTNOTE.sub("", text).strip() if text else text


def _percentile(value) -> float | None:
    if isinstance(value, str) and value.strip().endswith("%"):
        number = as_number(value.strip().rstrip("%"))
        return None if number is None else number / 100
    number = as_number(value)
    return number if number is not None and 0 <= number <= 1 else None


def _prrm_records(rows: list[list]) -> list[dict]:
    """``Percentiles | 1 | 2 … 24`` blocks (or ``Percentiles | <quantity>``) under a title row."""
    out: list[dict] = []
    title = None
    r = 0
    while r < len(rows):
        row = rows[r]
        labels = {c: as_label(v) for c, v in enumerate(row) if as_label(v)}
        key_col = next((c for c, t in labels.items() if re.fullmatch(r"percentiles?", t, re.IGNORECASE)), None)
        if key_col is None:
            if labels and all(as_number(v) is None for v in row):
                title = " ".join(t for _, t in sorted(labels.items()))
            r += 1
            continue
        columns = {}
        for c in range(key_col + 1, len(row)):
            number = as_number(row[c])
            text = as_label(row[c])
            if number is not None and number.is_integer() and 1 <= number <= 24:
                columns[c] = (int(number), None)
            elif text:
                columns[c] = (None, text)
        r += 1
        while r < len(rows) and key_col < len(rows[r]) and _percentile(rows[r][key_col]) is not None:
            p = _percentile(rows[r][key_col])
            for c, (hour, text) in columns.items():
                value = as_number(rows[r][c]) if c < len(rows[r]) else None
                if value is None:
                    continue
                out.append(
                    {
                        "table_title": title,
                        "section": None,
                        "line_item": _clean(title) or text,
                        "column_label": text if text else f"HE{hour}",
                        "scenario": None,
                        "percentile": round(p, 6),
                        "hour_ending": hour,
                        "value": value,
                        "row_number": r + 1,
                        "column_number": c + 1,
                    }
                )
            r += 1
    return out


def _table_records(values: list[TableValue]) -> list[dict]:
    out = []
    for v in values:
        column = " | ".join(v.headers)
        titles = [t for t in (v.title or "").split(" | ") if t]
        out.append(
            {
                "table_title": titles[-1] if titles else None,
                "section": v.section,
                "line_item": _clean(v.label),
                "column_label": column,
                "scenario": _clean(v.headers[-1]) if v.headers else None,
                "percentile": None,
                "hour_ending": _hour_ending(column),
                "value": v.value,
                "row_number": v.row + 1,
                "column_number": v.col + 1,
            }
        )
    return out


def parse_outlook(f: RawFile) -> pl.DataFrame | None:
    if f.suffix == ".pdf":
        return None  # the PDF restates the workbook's tables (and charts); the xlsx twin is parsed
    edition, target, published = _edition(f)
    records: list[dict] = []
    with f.local_path() as path:
        wb = Workbook(path, name=f.name)
        for sheet in wb.sheet_names:
            if _SKIP_SHEETS.match(sheet.strip()):
                continue
            rows = wb.rows(sheet)
            found = _prrm_records(rows) if _PRRM.search(sheet) else _table_records(extract_tables(rows))
            for rec in found:
                context = " ".join(x for x in (rec["line_item"], rec["column_label"], rec["section"]) if x)
                rec.update(
                    edition=edition, target_month=target, published_date=published, sheet=sheet,
                    value=round(rec["value"], 6),
                    unit="fraction" if _FRACTION.search(context) and abs(rec["value"]) <= 5 else "MW",
                )
            records.extend(found)
    if not any(r["sheet"].lower() == "monthly outlook" for r in records):
        raise ValueError(f"{f.key}: no Loads and Resources table in 'Monthly Outlook'")
    return pl.DataFrame(records, schema=SCHEMA)


DATASETS = [
    Dataset(
        name="mora_outlook",
        target="postgres",
        mode="by_file",
        description=(
            "MORA editions Dec 2023 → Nov 2026 in long format: deterministic loads and resources by scenario "
            "hour, capacity by resource category, and PRRM percentiles (demand, wind, solar by hour ending; "
            "thermal outages), with sheet/row/column provenance."
        ),
        parse=parse_outlook,
        inputs=lambda f: f.suffix in {".xlsx", ".xls", ".pdf"},
    ),
]
