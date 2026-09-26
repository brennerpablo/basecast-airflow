"""Regional Transmission Plan (RTP), public version: the reliability-case load by weather zone from each plan's
input-assumptions workbook (Appendix A2 in 2017–2018, Appendix B from 2019), sheet
``Reliability Case-Load Forecast``. Four tables per plan, each ``Year | Coast | East | … | West | Total``:

- ``tsp_submitted``: SSWG (TSP-submitted) load, less self-served load, non-coincident zone peaks;
- ``ercot_forecast_p90``: ERCOT's 90th-percentile load forecast (coincident peak from 2025);
- ``rtp_load``: the load level the plan studied (TSP load bounded by ERCOT's forecast, plus adjustments);
- ``rtp_load_off_peak``: the plan's off-peak case.

The raw files are zips: the 2014–2023 archive (a zip of yearly zips), the 2024 and 2025 packages, and the
2022 addendum. Plans before 2017 ship no input-assumptions workbook, and the addendum and the 765-kV PDF
carry no load tables, so those contribute no rows (the PDF returns ``None``). Staff names and emails that
appear on other sheets of these workbooks are never read."""

from __future__ import annotations

import re
from datetime import date

import polars as pl

from basecast_pipelines.parsers.ercot._adequacy_common import (
    Workbook,
    as_label,
    as_number,
    clean_text,
    iter_zip_members,
    period_token,
)
from basecast_pipelines.processing.core import Dataset, RawFile

SOURCE_ID = "ercot_rtp"

_LOAD_SHEET = re.compile(r"reliability case.?load forecast", re.IGNORECASE)
_PLAN_YEAR = re.compile(r"(?<!\d)(20\d{2})(?!\d)")
ZONES = {
    "coast": "COAST",
    "east": "EAST",
    "far west": "FWEST",
    "north": "NORTH",
    "north central": "NCENT",
    "south central": "SCENT",
    "south": "SOUTH",
    "southern": "SOUTH",
    "west": "WEST",
}

SCHEMA = {
    "rtp_year": pl.Int64,
    "workbook": pl.String,
    "sheet": pl.String,
    "status": pl.String,
    "date_last_updated": pl.Date,
    "series": pl.String,
    "series_title": pl.String,
    "target_year": pl.Int64,
    "region_type": pl.String,
    "region_id": pl.String,
    "column_label": pl.String,
    "value": pl.Float64,
    "unit": pl.String,
    "row_number": pl.Int64,
    "column_number": pl.Int64,
}


def series_of(title: str) -> str:
    t = title.lower()
    if t.startswith("sswg"):
        return "tsp_submitted"
    if "90th percentile" in t:
        return "ercot_forecast_p90"
    if "off-peak" in t or "off peak" in t:
        return "rtp_load_off_peak"
    if "rtp" in t:
        return "rtp_load"
    raise ValueError(f"unknown RTP load table: {title!r}")


def _region(label: str) -> tuple[str, str] | None:
    key = clean_text(label).lower()
    if key in ZONES:
        return "weather_zone", ZONES[key]
    if "total" in key:
        return "ercot", "ERCOT"
    return None


def _date(value) -> date | None:
    if isinstance(value, str) and re.match(r"\d{4}-\d{2}-\d{2}", value):
        return date.fromisoformat(value[:10])
    return None


def load_tables(rows: list[list]) -> tuple[list[dict], str | None, date | None]:
    """The ``Year | <zones> | Total`` tables of the sheet, with the title row above each."""
    out: list[dict] = []
    status = updated = title = None
    r = 0
    while r < len(rows):
        row = rows[r]
        first = as_label(row[0]) if row else None
        if first and re.match(r"date last updated", first, re.IGNORECASE):
            updated = next((d for d in (_date(v) for v in row[1:]) if d), None)
        elif first and re.match(r"status", first, re.IGNORECASE):
            status = next((as_label(v) for v in row[1:] if as_label(v)), None)
        if first and clean_text(first).lower() == "year":
            columns = {c: (as_label(v), _region(as_label(v) or "")) for c, v in enumerate(row) if c and as_label(v)}
            if title is None:
                raise ValueError(f"RTP load table at row {r + 1} has no title")
            series = series_of(title)
            r += 1
            while r < len(rows) and rows[r] and period_token(rows[r][0]) is not None:
                target = period_token(rows[r][0]).year
                for c, (label, region) in columns.items():
                    value = as_number(rows[r][c]) if c < len(rows[r]) else None
                    if value is None or region is None:
                        continue
                    out.append(
                        {
                            "series": series,
                            "series_title": title,
                            "target_year": target,
                            "region_type": region[0],
                            "region_id": region[1],
                            "column_label": label,
                            "value": round(value, 6),
                            "unit": "MW",
                            "row_number": r + 1,
                            "column_number": c + 1,
                        }
                    )
                r += 1
            title = None
            continue
        if first and all(as_number(v) is None for v in row) and not re.match(r"(date last|status)", first, re.I):
            title = first
        r += 1
    return out, status, updated


def parse_rtp_load(f: RawFile) -> pl.DataFrame | None:
    if f.suffix != ".zip":
        return None  # the 765-kV plan PDF: text and maps, no load tables
    records: list[dict] = []
    for member, payload in iter_zip_members(f.read_bytes(), suffixes=(".xlsx", ".xls")):
        wb = Workbook(payload, name=member)
        sheet = next((s for s in wb.sheet_names if _LOAD_SHEET.search(s)), None)
        if sheet is None:
            continue  # project lists, fuel prices, economic assumptions
        years = _PLAN_YEAR.findall(member.rsplit("/", 1)[-1]) or _PLAN_YEAR.findall(member)
        if not years:
            raise ValueError(f"{f.key}: no plan year in {member!r}")
        tables, status, updated = load_tables(wb.rows(sheet))
        if not tables:
            raise ValueError(f"{f.key}: {member} / {sheet} has no load table")
        for rec in tables:
            rec.update(rtp_year=int(years[0]), workbook=member, sheet=sheet, status=status, date_last_updated=updated)
        records.extend(tables)
    return pl.DataFrame(records, schema=SCHEMA) if records else None


DATASETS = [
    Dataset(
        name="rtp_load",
        target="postgres",
        mode="by_file",
        description=(
            "Regional Transmission Plan reliability-case load by weather zone and study year, 2017–2025 plans: "
            "TSP-submitted (SSWG) load, ERCOT's 90th-percentile forecast, the plan's load and its off-peak case."
        ),
        parse=parse_rtp_load,
        inputs=lambda f: f.suffix in {".zip", ".pdf"},
    ),
]
