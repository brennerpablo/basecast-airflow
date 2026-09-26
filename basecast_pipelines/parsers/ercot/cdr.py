"""Capacity, Demand and Reserves (CDR) reports, 2000 → December 2025, and the May 2026 Generation Resource
Capacity Forecast that replaced that edition.

The workbook layout changes by edition family, so the tables are found by what they say rather than
where they sit (``_adequacy_common.extract_tables``). The sheets read per family:

- 2000–2002 working papers: ``Summer Summary`` / ``Winter Summary`` hold one block per load-serving entity
  or TDSP (``AEP | Year | 2002 …``) plus the ERCOT totals;
- 2003 – May 2024: ``SummerSummary`` / ``WinterSummary`` (``Summary`` in the December 2011 update), the
  fuel-type sheets (per CDR zone until 2010), ``Supplemental`` (years 6–10, 2016–2019),
  ``MidTermProjections`` (2012) and the scenario sheets (``Generation/Planned Resource Scenarios``,
  ``Load Scenario - COVID-19 Impact``, ``Load Forecast, HB5066``);
- December 2024 – December 2025: ``Summer/Winter Summary`` or ``Seasonal Summary`` with Peak Load Hour and
  Peak Net Load Hour columns per year and season, ``Load-Resource Scenarios`` and
  ``Capacity by Resource Type``;
- May 2026 Generation Resource Capacity Forecast: ``Capacity by Resource Category``.

Not read: unit lists (``*Capacities``, ``Unit Details``, ``New CDR-Eligible Resources``), per-county sheets
(2003–2011), ``Executive_Summary``, the single-year ``Peak v High Net Load Hour`` comparisons, the hidden
calculation inputs of December 2025 (``Load``, ``LongTermLoadForecast``, ``LoadResourceScenarios``, …),
chart-only sheets (``LongTermProjections``, ``ReserveMargin``) and text sheets. The wind/solar
capacity-percentage companion workbooks (2014–2020) hold percentages, not capacity lines: they return
``None``.

Outputs:

- ``cdr_capacity``: every value of those tables in long format, as published (line item, column label,
  season, target year, region), with provenance;
- ``cdr_forecasts``: the peak demand forecast lines in the forecast shape shared with the LTLF parser.

Winter periods: ``2026/2027`` → ``target_year`` 2026 (the year the winter starts), ``period_label`` keeps
the published text. Early editions label winters with one year; that year is kept as published (not
verified whether it is the starting or ending year)."""

from __future__ import annotations

import re
from datetime import date

import polars as pl

from basecast_pipelines.parsers.ercot._adequacy_common import (
    MONTHS,
    TableValue,
    Workbook,
    as_label,
    as_number,
    extract_tables,
    month_year,
    period_token,
    season_in,
)
from basecast_pipelines.processing.core import Dataset, RawFile
from basecast_pipelines.parsers._forecasts import OFFICIAL_FORECASTS

SOURCE_ID = "ercot_cdr"
PUBLISHER = "ERCOT"

_SHEETS = re.compile(
    r"summary|fuel ?types|fuel type capacity mix|capacity by resource|scenario|supplemental|projections|"
    r"hb ?5066",
    re.IGNORECASE,
)
_SKIP_SHEETS = re.compile(r"executive|peak v high|^loadresourcescenarios$", re.IGNORECASE)
_COMPANION = re.compile(r"capacitypercentages", re.IGNORECASE)
_REVISED = re.compile(r"revised[\s_\-]*(\d*)", re.IGNORECASE)
_DIGITS_DATE = re.compile(r"(?<!\d)(\d{2})(\d{2})?((?:19|20)\d{2})(?!\d)")

# Editions whose month is neither in the link text, the file name nor the title page; the month comes from
# the next edition's "Changes" sheet ("Changes from 2012 CDR (May Release)").
_EDITION_OVERRIDES = {
    "capacitydemandandreservereport_2012.xls": date(2012, 5, 1),
}

CAPACITY_SCHEMA = {
    "edition": pl.String,
    "edition_date": pl.Date,
    "report": pl.String,
    "sheet": pl.String,
    "table_title": pl.String,
    "section": pl.String,
    "line_group": pl.String,
    "line_item": pl.String,
    "column_label": pl.String,
    "season": pl.String,
    "target_year": pl.Int64,
    "period_label": pl.String,
    "scenario": pl.String,
    "region_type": pl.String,
    "region_id": pl.String,
    "value": pl.Float64,
    "unit": pl.String,
    "row_number": pl.Int64,
    "column_number": pl.Int64,
}

FORECAST_SCHEMA = {
    "vintage": pl.String,
    "vintage_date": pl.Date,
    "publisher": pl.String,
    "product": pl.String,
    "target_year": pl.Int64,
    "target_month": pl.Int64,
    "season": pl.String,
    "region_type": pl.String,
    "region_id": pl.String,
    "metric": pl.String,
    "scenario": pl.String,
    "value": pl.Float64,
    "unit": pl.String,
    "sheet": pl.String,
    "row_label": pl.String,
    "column_label": pl.String,
}


# --- edition ------------------------------------------------------------------------------------------


def _title_page_month(wb: Workbook) -> date | None:
    sheet = next((s for s in wb.sheet_names if s.lower() == "titlepage"), None)
    if sheet is None:
        return None
    for row in wb.rows(sheet)[:40]:
        for cell in row:
            found = month_year(as_label(cell))
            if found:
                return found
    return None


def _digits_month(name: str) -> date | None:
    """``CDR_Summer_02082000`` → Feb 2000, ``ERCOTCDR082001`` → Aug 2001."""
    for m in _DIGITS_DATE.finditer(name):
        month = int(m.group(1))
        if 1 <= month <= 12:
            return date(int(m.group(3)), month, 1)
    return None


def edition_of(f: RawFile, wb: Workbook) -> tuple[date, str, str]:
    """(edition date, vintage label, report name)."""
    meta = f.meta
    text = f"{meta.get('link_text') or ''} {f.name}"
    report = "Generation Resource Capacity Forecast" if "generation_resource" in f.name.lower() else "CDR"
    when = (
        _EDITION_OVERRIDES.get(f.name.lower())
        or _title_page_month(wb)
        or (date.fromisoformat(meta["edition_month"]) if meta.get("edition_month") else None)
        or month_year(f.name.replace("_", " "))
        or _loose_month(wb_title(wb))
        or _digits_month(f.name)
    )
    if when is None:
        raise ValueError(f"{f.key}: cannot tell the CDR edition month")
    label = f"{'CDR' if report == 'CDR' else 'GRCF'} {when.strftime('%b %Y')}"
    revised = _REVISED.search(text)
    if revised:
        label += " Revised" + (f" {revised.group(1)}" if revised.group(1) else "")
    return when, label, report


def _loose_month(text: str) -> date | None:
    """``2012 Report on the Capacity … (December Update)`` → Dec 2012."""
    year = re.search(r"\b((?:19|20)\d{2})\b", text)
    month = re.search(r"\b(" + "|".join(MONTHS) + r")\b", text, re.IGNORECASE)
    if not (year and month):
        return None
    return date(int(year.group(1)), MONTHS[month.group(1).lower()], 1)


def wb_title(wb: Workbook) -> str:
    """Title rows of the first summary sheet (``2012 Report … (December Update)``)."""
    sheet = next((s for s in wb.sheet_names if "summary" in s.lower() and "exec" not in s.lower()), None)
    if sheet is None:
        return ""
    texts = []
    for row in wb.rows(sheet)[:3]:
        texts.extend(t for t in (as_label(c) for c in row) if t)
    return " ".join(texts)


# --- values -------------------------------------------------------------------------------------------

_ZONE = re.compile(r"fuel types\s*-\s*([a-z ]+?)(?:\s+zone)?\s*$", re.IGNORECASE)
_NOT_A_UTILITY = re.compile(r"^(ercot|generation capacity|margins|total)\b", re.IGNORECASE)
_FRACTION_LABEL = re.compile(r"margin|percent(?!ile)|pct|\(%\)|\bratio\b", re.IGNORECASE)
_FRACTION_CONTEXT = re.compile(r"percent(?!ile)|pct|\(%\)", re.IGNORECASE)
_MW = re.compile(r"\bMW\b")
_FOOTNOTE = re.compile(r"^\[\d+\]$")
_SCENARIO_ROW = re.compile(r"\bscenario\b", re.IGNORECASE)


def _region(v: TableValue) -> tuple[str, str]:
    if v.entity and not _NOT_A_UTILITY.match(v.entity):
        return "utility", v.entity.strip()
    for part in (v.title or "").split(" | "):
        m = _ZONE.search(part.strip())
        if m and m.group(1).strip().upper() != "ERCOT":
            return "cdr_zone", m.group(1).strip().upper()
    return "ercot", "ERCOT"


def _season(v: TableValue, sheet: str) -> str | None:
    for text in (*v.headers, *(p.season or "" for p in v.periods)):
        s = season_in(text)
        if s:
            return s
    if any(p.split for p in v.periods):
        return "winter"
    return (
        season_in(v.title and v.title.split(" | ")[-1])
        or v.context_season
        or season_in(sheet)
        or season_in(v.label)
    )


def _unit(v: TableValue) -> str:
    if _MW.search(v.label):
        return "MW"
    if _FRACTION_LABEL.search(v.label):
        return "fraction"
    titles = [t for t in (v.title or "").split(" | ") if len(t) <= 40]
    context = " ".join([*v.headers, v.section or "", *titles])
    if _FRACTION_CONTEXT.search(context):
        return "fraction"
    return "MW"


def _fraction_tables(values: list[TableValue]) -> set[int]:
    """Tables whose every value lies within [-1, 1]: unlabelled percentage tables (e.g. the second
    fuel-type table of May 2016, published without its "In Percentages" caption)."""
    peak: dict[int, float] = {}
    count: dict[int, int] = {}
    for v in values:
        peak[v.table_index] = max(peak.get(v.table_index, 0.0), abs(v.value))
        count[v.table_index] = count.get(v.table_index, 0) + 1
    return {t for t, m in peak.items() if m <= 1.0 and count[t] >= 3}


def _scenario(headers: tuple[str, ...], group: str | None, label: str) -> str | None:
    text = " ".join([*headers, group or "", label]).lower()
    if "net load" in text:
        return "peak_net_load_hour"
    return None


def _vertical_values(rows: list[list]) -> list[TableValue]:
    """Year-in-rows tables (``Load Forecast, HB5066``): a period in one column, its value in the next,
    under a header cell above the period column."""
    out: list[TableValue] = []
    for r, row in enumerate(rows):
        for c in range(len(row) - 1):
            period = period_token(row[c])
            value = as_number(row[c + 1])
            if period is None or value is None:
                continue
            header = None
            for up in range(r - 1, max(-1, r - 25), -1):
                if c < len(rows[up]) and period_token(rows[up][c]) is None and as_label(rows[up][c]):
                    header = as_label(rows[up][c])
                    break
            if header is None:
                continue
            title = next(
                (as_label(rows[up][0]) for up in range(r - 1, -1, -1)
                 if as_label(rows[up][0]) and all(as_number(x) is None for x in rows[up])),
                None,
            )
            out.append(
                TableValue(
                    row=r, col=c + 1, value=value, label=header, label_col=c, headers=(period.label,),
                    periods=(period,),
                    section=None, title=title, context_season=None, entity=None, table_index=c,
                )
            )
    return out


def _sheet_values(wb: Workbook, sheet: str) -> list[TableValue]:
    rows = wb.rows(sheet)
    if re.search(r"hb ?5066", sheet, re.IGNORECASE):
        return _vertical_values(rows)
    return extract_tables(rows)


def capacity_records(f: RawFile) -> list[dict] | None:
    if _COMPANION.search(f.name):
        return None  # wind/solar capacity-percentage companions: percentages by unit, not CDR lines
    with f.local_path() as path:
        wb = Workbook(path, name=f.name)
        when, edition, report = edition_of(f, wb)
        records: list[dict] = []
        for sheet in wb.sheet_names:
            if not _SHEETS.search(sheet) or _SKIP_SHEETS.search(sheet):
                continue
            values = _sheet_values(wb, sheet)
            fraction_tables = _fraction_tables(values)
            groups: dict[tuple[int, int], str] = {}
            units: dict[tuple[int, int], str] = {}
            for v in values:
                if not _SCENARIO_ROW.search(v.label):
                    groups[(v.table_index, v.label_col)] = v.label
                group = groups.get((v.table_index, v.label_col))
                if any(re.match(r"^difference\b", h, re.IGNORECASE) for h in v.headers):
                    continue  # derived "Difference [1] less [2]" columns
                period = v.periods[-1] if v.periods else None
                region_type, region_id = _region(v)
                if v.table_index in fraction_tables:
                    unit = "fraction"
                elif re.match(r"^\s*difference\b", v.label, re.IGNORECASE):
                    unit = units.get((v.table_index, v.label_col), _unit(v))  # same unit as the lines above
                else:
                    unit = _unit(v)
                units[(v.table_index, v.label_col)] = unit
                records.append(
                    {
                        "edition": edition,
                        "edition_date": when,
                        "report": report,
                        "sheet": sheet,
                        "table_title": v.title,
                        "section": v.section,
                        "line_group": group,
                        "line_item": v.label,
                        "column_label": " | ".join(h for h in v.headers if not _FOOTNOTE.match(h)),
                        "season": _season(v, sheet),
                        "target_year": period.year if period else None,
                        "period_label": period.label if period else None,
                        "scenario": _scenario(v.headers, group, v.label),
                        "region_type": region_type,
                        "region_id": region_id,
                        "value": round(v.value, 6),
                        "unit": unit,
                        "row_number": v.row + 1,
                        "column_number": v.col + 1,
                    }
                )
    if not records:
        raise ValueError(f"{f.key}: no CDR table found")
    return records


def parse_capacity(f: RawFile) -> pl.DataFrame | None:
    records = capacity_records(f)
    if records is None:
        return None
    return pl.DataFrame(records, schema=CAPACITY_SCHEMA)


# --- forecasts ----------------------------------------------------------------------------------------

_LESS_PLUS = re.compile(r"^\s*(less|plus)\b|\bless:|\bplus:|\bplus$", re.IGNORECASE)
_FIRM = re.compile(r"\bfirm\b.*\b(load|demand)\b", re.IGNORECASE)
_PEAK = re.compile(
    r"\bpeak (demand|load)\b|\b(summer|winter|seasonal) load\b|\bload based on\b|\bload forecast\b",
    re.IGNORECASE,
)
_NOT_PEAK = re.compile(
    r"interruptible|energy efficiency|reserve|margin|capacity|resources|\bnet load\b|contracted|"
    r"officer letter|large load|difference|laar|\bdate\b|hour ending",
    re.IGNORECASE,
)
_BEFORE_EE = re.compile(r"before reductions", re.IGNORECASE)
_LOAD_SHEETS = re.compile(r"summary|supplemental|scenario|hb ?5066|projections", re.IGNORECASE)
_BASE_SHEETS = re.compile(r"summary|supplemental", re.IGNORECASE)


def _metric(rec: dict) -> str | None:
    label = rec["line_item"]
    if rec["unit"] != "MW" or _LESS_PLUS.search(label):
        return None
    group = rec["line_group"] or ""
    if _SCENARIO_ROW.search(label):
        if _FIRM.search(group):
            return "firm_peak_mw"
        if _PEAK.search(group) and not _NOT_PEAK.search(group):
            return "peak_mw"
        return None
    if _FIRM.search(label):
        return "firm_peak_mw"
    if _BEFORE_EE.search(label):
        return "peak_before_ee_mw"
    if _PEAK.search(label) and not _NOT_PEAK.search(label):
        return "peak_mw"
    return None


def _forecast_row(rec: dict, scenario: str | None) -> dict:
    return {
        "vintage": rec["edition"],
        "vintage_date": rec["edition_date"],
        "publisher": PUBLISHER,
        "product": "CDR",
        "target_year": rec["target_year"],
        "target_month": None,
        "season": rec["season"],
        "region_type": rec["region_type"],
        "region_id": rec["region_id"],
        "metric": rec["metric"],
        "scenario": scenario,
        "value": rec["value"],
        "unit": "MW",
        "sheet": rec["sheet"],
        "row_label": rec["line_item"],
        "column_label": rec["column_label"],
    }


def parse_forecasts(f: RawFile) -> pl.DataFrame | None:
    """Peak demand lines. The summary sheets give the base case (``scenario`` null, or
    ``peak_net_load_hour`` for that column family); lines on the scenario sheets that restate a base value
    are dropped, the others keep their published label as ``scenario``."""
    records = capacity_records(f)
    if records is None:
        return None
    base: dict[tuple, float] = {}
    base_labels: dict[tuple, set[str]] = {}
    out: list[dict] = []
    others: list[dict] = []
    for rec in records:
        if not _LOAD_SHEETS.search(rec["sheet"]) or rec["target_year"] is None:
            continue
        if "chart" in (rec["column_label"] or "").lower():
            continue  # hidden "Data for Chart" blocks restate the summary
        metric = _metric(rec)
        if metric is None:
            continue
        rec = {**rec, "metric": metric}
        line = (rec["season"], rec["region_type"], rec["region_id"], metric, rec["scenario"])
        key = (rec["target_year"], *line)
        if _BASE_SHEETS.search(rec["sheet"]):
            if key not in base:
                base[key] = rec["value"]
                base_labels.setdefault(line, set()).add(rec["line_item"].lower())
                out.append(_forecast_row(rec, rec["scenario"]))
        else:
            others.append({**rec, "_key": key, "_line": line})
    seen = set()
    for rec in others:
        key, line = rec["_key"], rec["_line"]
        restated = key in base and abs(base[key] - rec["value"]) < 0.5
        if restated and not _SCENARIO_ROW.search(rec["line_item"]):
            continue  # a scenario table restating the base case
        if key not in base and rec["line_item"].lower() in base_labels.get(line, set()):
            base[key] = rec["value"]  # the base line continued for later years (years 6-10 tables)
            out.append(_forecast_row(rec, rec["scenario"]))
            continue
        scenario = f"{rec['scenario']}: {rec['line_item']}" if rec["scenario"] else rec["line_item"]
        if (key, scenario) in seen:
            continue
        seen.add((key, scenario))
        out.append(_forecast_row(rec, scenario))
    return pl.DataFrame(out, schema=FORECAST_SCHEMA) if out else None


def _is_spreadsheet(f: RawFile) -> bool:
    return f.suffix in {".xls", ".xlsx", ".xlsb"}


def select_editions(files: list[RawFile]) -> list[RawFile]:
    """Drop re-posts: when two files carry the same link text, keep the one listed on its own year page
    (the May 2013 CDR is posted again, with identical tables, on the 2014 page)."""
    by_link: dict[str, list[RawFile]] = {}
    for f in files:
        by_link.setdefault((f.meta.get("link_text") or f.key).strip().lower(), []).append(f)
    keep = []
    for group in by_link.values():
        if len(group) > 1:
            own = [f for f in group if str(f.meta.get("page_year")) in (f.meta.get("edition_month") or "")[:4]]
            group = own or group
        keep.extend(group)
    return sorted(keep, key=lambda f: (f.dt, f.entry.fetched_at, f.key))


DATASETS = [
    Dataset(
        name="cdr_capacity",
        target="postgres",
        mode="by_file",
        description=(
            "CDR (2000 → Dec 2025) and May 2026 Generation Resource Capacity Forecast tables in long format: "
            "every published line (load, resources, reserve margins, fuel types, scenarios) per edition, "
            "season, target year and region, with sheet/row/column provenance."
        ),
        parse=parse_capacity,
        inputs=_is_spreadsheet,
        select=select_editions,
    ),
    Dataset(
        name="cdr_forecasts",
        target="postgres",
        mode="by_file",
        description=(
            "CDR peak demand forecasts (peak_mw, firm_peak_mw, peak_before_ee_mw) per vintage, target year, "
            "season and region, in the forecast shape shared with the LTLF (union → official_forecasts)."
        ),
        parse=parse_forecasts,
        inputs=_is_spreadsheet,
        select=select_editions,
    ),
]

SQL_DATASETS = [OFFICIAL_FORECASTS]
