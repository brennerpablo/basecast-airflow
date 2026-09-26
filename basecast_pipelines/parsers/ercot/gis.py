"""GIS Report (EMIL PG7-200-ER, Report Type 15933): ERCOT's monthly generator interconnection status.

Raw: ``raw/source=ercot_gis/dt=<first day of the report month>/`` holds one workbook per month (xls,
xlsx, or a zip with one xlsx inside, May 2014 → today) plus, since Oct 2020, the co-located battery
identification report. Some months also have a revised or corrected workbook; the newest one wins
(``one_file_per_month``). See ``docs/gis-columns.md`` for the sheets and headers of every vintage.

Two layouts:

- **May 2014 – Jul 2018** ("old"): ``IA Table`` (projects with a signed interconnection agreement),
  ``Full Study Table`` (projects in the full interconnection study without an IA, confidential ones
  with only INR / county / fuel / MW), ``Wind Chart`` / ``Solar Chart`` (the data behind the charts,
  with the IA signed date), ``QSA Units`` (May–Jul 2018) and a ``New Units`` sheet (renamed ``New and
  Cancelled Units`` and ``Newly Operational & Cancelled``) with small tables of the month's new IAs,
  synchronization approvals, commercial operation approvals and (since Apr 2017) cancellations.
- **Aug 2018 → today** ("GINR/GIM"): ``Project Details`` (``Project Details - Large Gen`` since Feb
  2022), ``Project Details - Small Gen`` (since Jan 2022), ``Commissioning Update``, ``Cancellation
  Update`` and ``Inactive Projects`` (since Aug 2019; the first two were ``Project ... Update`` before).

Every table is found by its header row: the row with an ``INR`` / ``GINR Reference Number`` cell and
another header word (the position varies, and a sheet may hold several tables). Headers wrap over up to
five rows; the label of a column is the text of all its header rows joined (``composite label``).

Datasets:

- ``gis_cells`` (BigQuery): every non-empty cell of every sheet, long format, with the composite header
  label of its table. ``row_index`` / ``column_index`` are 0-based absolute sheet coordinates (Excel row
  = ``row_index + 1``, column A = 0); fastexcel drops leading empty rows, so the offset is read from the
  sheet XML (xlsx) or with xlrd (xls).
- ``gis_snapshots`` (Postgres): one row per INR × report month, the INR's rows of all sheets merged
  (the project tables win over the monthly update tables), with stable column names
  (``HEADER_MAP``) and a status derived from the sheets: ``cancelled`` > ``inactive`` > ``active``
  (listed in a project table) > ``commissioning_update`` (only in the month's commissioning approvals).
- ``gis_project_events`` (SQL over ``gis_snapshots``): per INR, entry, milestones, exit and exit status
  for the survival model.
- ``gis_colocated_battery`` (Postgres): the co-located battery identification reports, one row per
  project (or operational unit) and sheet.

Conventions and caveats:

- ``inr`` is the text as printed (suffixes such as ``a`` or ``_2`` kept); only spaces are removed and
  ``INR`` upper-cased.
- ``1900-01-01`` marks "milestone date not available" (report note) and becomes null; datetimes with
  Excel float noise (``09:57:59.999``) are rounded to the second before taking the date.
- ``Air Permit``, ``GHG Permit`` and ``Water Availability`` hold either a date or a word (``Not
  Required``, ``Yes``, ``No``, ``N/A``): the date goes to ``*_date``, the word to the column itself.
- ``fuel`` and ``technology`` are as printed (codes such as ``WIN`` in the project tables, words such as
  ``Wind`` in the update tables and old sheets); ``fuel_type`` is a derived, normalized family.
- The monthly update tables list the approvals and cancellations *during* the report month, while
  ``Inactive Projects`` is cumulative ("as of end of month").
- No sheet in any vintage has personal contact fields; ``interconnecting_entity`` is a company name.
- Not verified: the equivalence of the old Yes/No planning flags with the later dates; the column
  labelled ``Meets Planning Guide QSA (Section 6.9) Prerequisites`` in Sep–Oct 2018 is read as the
  "meets all 6.9 requirements" date (it sits where that column sits in the other months); rows in the
  old ``Full Study Table`` without a name are confidential projects (report note).
"""

from __future__ import annotations

import io
import json
import logging
import re
import zipfile
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import date
from xml.etree import ElementTree

import fastexcel
import polars as pl

from basecast_pipelines.processing.core import Dataset, RawFile, SqlDataset
from basecast_pipelines.processing.tabular import clean_label, num

log = logging.getLogger(__name__)

SOURCE_ID = "ercot_gis"

# --- workbook access ---------------------------------------------------------------------------------


def workbook_bytes(f: RawFile) -> bytes:
    """The workbook of a raw file; the 2014-10 → 2015-02 reports are zips holding one xlsx."""
    data = f.read_bytes()
    if f.suffix != ".zip":
        return data
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        members = [n for n in z.namelist() if n.lower().endswith((".xlsx", ".xlsm", ".xls"))]
        if len(members) != 1:
            raise ValueError(f"{f.key}: expected one workbook in the zip, found {members}")
        return z.read(members[0])


@dataclass(frozen=True)
class Sheet:
    name: str
    rows: list[tuple[str | None, ...]]
    row0: int  # absolute sheet row of rows[0]
    cols: list[int]  # absolute sheet column of each position in a row


def read_sheets(data: bytes) -> list[Sheet]:
    """Every non-empty sheet as rows of strings, with the absolute position of the used range."""
    reader = fastexcel.read_excel(data)
    offsets = _row_offsets(data)
    out = []
    for name in reader.sheet_names:
        sheet = reader.load_sheet(name, header_row=None, dtypes="string")
        if not sheet.width or not sheet.height:
            continue
        rows = list(sheet.to_polars().iter_rows())
        cols = [c.absolute_index for c in sheet.available_columns()]
        out.append(Sheet(name, rows, offsets.get(name, 0), cols))
    return out


_CELL = re.compile(rb"<(?:\w+:)?c\b([^>]*?)(/>|>(.*?)</(?:\w+:)?c>)", re.S)
_CELL_REF = re.compile(rb'\br="[A-Z]+(\d+)"')
_HAS_VALUE = re.compile(rb"<(?:\w+:)?(?:v|is)\b[^>]*>")


def _row_offsets(data: bytes) -> dict[str, int]:
    """First row holding a value in each sheet (calamine's used range starts there)."""
    try:
        if data[:2] == b"PK":
            return _xlsx_row_offsets(data)
        return _xls_row_offsets(data)
    except Exception as exc:  # positions are provenance only; never fail a parse on them
        log.warning("could not read sheet row offsets (%s); row_index is relative to the used range", exc)
        return {}


def _xlsx_row_offsets(data: bytes) -> dict[str, int]:
    out = {}
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        workbook = ElementTree.fromstring(z.read("xl/workbook.xml"))
        rels = ElementTree.fromstring(z.read("xl/_rels/workbook.xml.rels"))
        targets = {r.get("Id"): r.get("Target", "") for r in rels}
        for sheet in workbook.iter():
            if not sheet.tag.endswith("}sheet"):
                continue
            rid = next((v for k, v in sheet.attrib.items() if k.endswith("}id")), None)
            target = targets.get(rid, "")
            path = target.lstrip("/") if target.startswith("/") else "xl/" + target
            if path not in z.namelist():
                continue
            with z.open(path) as fh:
                xml = fh.read()
            for m in _CELL.finditer(xml):
                inner = m.group(3)
                if inner and _HAS_VALUE.search(inner):
                    ref = _CELL_REF.search(m.group(1))
                    if ref:
                        out[sheet.get("name")] = int(ref.group(1)) - 1
                    break
    return out


def _xls_row_offsets(data: bytes) -> dict[str, int]:
    import xlrd

    book = xlrd.open_workbook(file_contents=data, on_demand=True)
    out = {}
    for name in book.sheet_names():
        sh = book.sheet_by_name(name)
        for i in range(sh.nrows):
            if any(c.ctype not in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK) and str(c.value).strip()
                   for c in sh.row(i)):
                out[name] = i
                break
        book.unload_sheet(name)
    return out


# --- tables inside a sheet ---------------------------------------------------------------------------

_KEY_HEADER = re.compile(r"^(g?inr( reference number| number| no\.?| #)?|unit name)$", re.I)
_PEER_HEADER = re.compile(r"project|county|fuel|\bmw\b|capacity|unit code", re.I)
_INR_CODE = re.compile(r"^\d{2}\s*INR\s*\d{3,5}", re.I)
_NUMBER_OR_DATE = re.compile(r"^-?[\d.,]+%?$|^\d{4}-\d{2}-\d{2}|^\d{1,2}/\d{1,2}/\d{2,4}$")
MAX_HEADER_ROWS = 6


@dataclass(frozen=True)
class Block:
    """One table: its header row, the rows its header wraps over, and its data rows."""

    header_row: int  # index into Sheet.rows
    data_start: int
    data_end: int  # exclusive
    key_col: int  # position of the INR (or unit name) column
    labels: tuple[str, ...]  # composite label of each column ('' when none)


def _text(value: object) -> str:
    return clean_label(value) if value is not None else ""


def _is_header(row: tuple) -> int | None:
    cells = [_text(c) for c in row]
    for j, c in enumerate(cells):
        if c and _KEY_HEADER.match(c) and any(_PEER_HEADER.search(o) for k, o in enumerate(cells) if k != j and o):
            return j
    return None


def find_blocks(rows: Sequence[tuple]) -> list[Block]:
    heads = [(i, j) for i, row in enumerate(rows) if (j := _is_header(row)) is not None]
    blocks = []
    for n, (h, key) in enumerate(heads):
        stop = heads[n + 1][0] if n + 1 < len(heads) else len(rows)
        labels = [_text(c) for c in rows[h]]
        end = h + 1
        while end < stop and end - h < MAX_HEADER_ROWS:
            row = rows[end]
            filled = [(k, _text(c)) for k, c in enumerate(row) if _text(c)]
            if not filled or _text(row[key]) or any(_NUMBER_OR_DATE.match(t) for _, t in filled):
                break
            for k, t in filled:
                labels[k] = f"{labels[k]} {t}".strip()
            end += 1
        blocks.append(Block(h, end, stop, key, tuple(labels)))
    return blocks


def norm_label(label: str) -> str:
    """Header text as a lookup key: lower case, single spaces, footnote stars dropped."""
    text = clean_label(label).lower()
    text = re.sub(r"\s+,", ",", text).rstrip("* ").strip()
    return re.sub(r"\s+", " ", text)


def norm_inr(value: str) -> str:
    text = re.sub(r"\s+", "", value).rstrip("*")
    return re.sub(r"inr", "INR", text, count=1, flags=re.I)


# --- header → stable column name --------------------------------------------------------------------

# Every header seen from May 2014 to Aug 2026 (normalized with ``norm_label``), with the vintages that
# use it. ``None`` means "known, deliberately not kept" (the cells stay in ``gis_cells``). A header not
# listed here goes to ``extra_fields`` (JSON) with a warning, so a new vintage never loses data silently.
HEADER_MAP: dict[str, str | None] = {
    # identity
    "inr": "inr",  # all INR tables
    "ginr reference number": "inr",  # IA Table, Full Study Table 2014-05..2018-07
    "project name": "project_name",  # IA Table 2014-05.., Full Study 2017-08.., all new-layout tables
    "project": "project_name",  # New Units tables 2014-05..2018-07
    "projectname": "project_name",  # Wind/Solar Chart 2014-05..2018-07
    "ginr study phase": "study_phase",  # Project Details 2018-08..2022-01
    "gim study phase": "study_phase",  # Project Details - Large Gen 2022-02..
    "interconnecting entity": "interconnecting_entity",  # IA/Full Study 2017-08..2018-07, new layout
    "poi location": "poi_location",  # new layout
    "point of interconnection (poi)": "poi_location",  # Full Study 2017-08..2018-07, IA Table 2018-06..07
    "county": "county",
    "cdr reporting zone": "cdr_reporting_zone",  # new layout
    "fuel": "fuel",
    "fuel type": "fuel",  # Newly Operational & Cancelled 2018-06
    "technology": "technology",  # new layout
    "typecode": "technology",  # Wind/Solar Chart
    "size category": "size_category",  # update tables 2022-02..
    # capacity
    "capacity (mw)": "capacity_mw",  # new layout
    "capacity to grid (mw)": "capacity_mw",  # Full Study Table
    "mw for grid": "capacity_mw",  # IA Table
    "mwforgrid": "capacity_mw",  # Wind/Solar Chart
    "mw": "capacity_mw",  # New Units tables; "MW **" in the update tables
    # projected commercial operation date
    "projected cod": "projected_cod",  # IA Table 2014-05..12; new layout
    "projected date": "projected_cod",  # IA Table 2015-01..2018-07, charts 2015-01..
    "cod projection": "projected_cod",  # Wind Chart 2014-05..12
    "projected cod (as specified by the resource developer)": "projected_cod",  # Full Study 2014
    "projected date (as specified by the resource developer)": "projected_cod",  # Full Study 2015-01..2017-07
    "projected month/year (as specified by the resource developer)": "projected_cod",  # Full Study 2017-08
    "projected cod month/year (as specified by the resource developer)": "projected_cod",  # 2017-09..2018-07
    "year": None,  # Wind/Solar Chart: year of the projected date
    # change flags
    "changes from last report": "change_indicators",  # IA Table
    "change indicators: proj name, mw size, cod, sfs/ntp, fis request": "change_indicators",  # 2018-08..2019-07
    "change indicators: proj name, mw size, cod, sfs/ntp, fis request status change ina-to-pln":
        "change_indicators",  # 2019-08..2020-05 (some months)
    "change indicators: proj name, mw size, cod, sfs/ntp, fis request status change sus-to-pln":
        "change_indicators",  # 2019-09..2020-03
    "change indicators: proj name, mw size, cod, sfs/ntp, fis request, status change ina-to-pln":
        "change_indicators",  # 2020-06..
    "change indicators: proj name, mw size, cod, sfs, status change ina-to-pln": "change_indicators",  # small 2022-01
    "change indicators: proj name, mw size, cod, sfs. status change ina-to-pln": "change_indicators",  # small 2022-02..
    # milestones (new layout, 2018-08..)
    "approval date for submission of proof of site control": "site_control_approved",  # 2020-07..
    "screening study started": "screening_study_started",
    "screening study complete": "screening_study_complete",
    "fis requested": "fis_requested",
    "fis approved": "fis_approved",
    "economic study required": "economic_study_required",  # 2020-06..
    "ia signed": "ia_signed",  # also Wind/Solar Chart 2014-05..2018-07
    "signed ia date": "ia_signed",  # New Units "New Executed Interconnection Agreements" 2014-05..06
    "financial security and notice to proceed provided": "financial_security_provided",
    "sufficient financial security received by tsp": "financial_security_provided",  # IA Table
    "sufficient financial security received by tsp/notice to proceed given": "financial_security_provided",  # 2016-03
    "fs posted": "financial_security_provided",  # Wind/Solar Chart 2017-03..2018-07 (true/false)
    "financial security required to fund dist. upgrades": "financial_security_required_dist",  # Small Gen
    "air permit": "air_permit",
    "air permit(s)": "air_permit",  # IA Table 2017-08
    "ghg permit": "ghg_permit",
    "water availability": "water_availability",
    "water rights": "water_availability",  # IA Table 2014-12..2018-07
    "proof of adequate water supplies": "water_availability",  # IA Table 2017-08
    "meets planning guide section 6.9(1) requirements for inclusion in planning models": "meets_planning_6_9_1",
    "meets all planning guide section 6.9 requirements for inclusion in planning models":
        "meets_all_planning_6_9",  # 2019-01..
    "meets planning guide section 6.9 requirements for inclusion in planning models":
        "meets_all_planning_6_9",  # 2018-08..12
    "meets planning guide qsa (section 6.9) prerequisites": "meets_all_planning_6_9",  # 2018-09..10, mislabelled
    "meets planning guide qsa (section 5.9) prerequisites": "meets_qsa_prerequisites",
    "meets requirements of pg section 6.9": "meets_planning_6_9_met",  # IA Table 2014-05..11 (Yes/No)
    "meets all requirements": "meets_planning_6_9_met",  # IA Table 2014-12..2016-02 (Yes/No)
    "meets section 6.9 requirements (1)(b) through (1)(d)": "meets_planning_6_9_1bd_met",  # 2016-03..2018-07
    "construction start": "construction_start",
    "construction end": "construction_end",
    "approved for energization": "approved_energization",
    "approved for synchronization": "approved_synchronization",
    "model ready date": "model_ready_date",  # Small Gen
    # old study status
    "fis completion": "fis_status",  # IA Table 2015-04..2018-07 (Complete/Incomplete)
    "fis report status": "fis_status",  # Full Study Table 2017-09..2018-07
    "fis status": "fis_status",  # Full Study Table 2018-01
    "public letter": "public_letter_date",  # Full Study Table 2014-05..2016-12
    # monthly events
    "commissioning category": "commissioning_category",  # Commissioning Update 2018-09..
    "approval date": "commissioning_approval_date",  # Commissioning Update, routed by category
    "part2 synch apprv": "approved_synchronization",  # New Units 2014-05..2018-06
    "part 2 synch approval": "approved_synchronization",  # 2018-07
    "part3 commercial apprv": "commercial_operation_date",  # New Units 2014-05..2016-04
    "resource commissioning date": "commercial_operation_date",  # 2016-01..2018-07
    "cancel date": "cancel_date",  # Cancellation Update 2018-08..
    "cancellation date": "cancel_date",  # New Units 2017-04..2018-07
    "inactive date": "inactive_date",  # Inactive Projects 2019-08..
    "comment": "comment",
    "comments": "comment",
    "notes": "comment",  # Cancellation Update 2019-12
    # co-located battery report
    "project status": "project_status",
    "unit name": "unit_name",
    "unit code": "unit_code",
    "in service": "in_service_year",
}

# Headers whose meaning depends on the table.
SECTION_HEADER_MAP: dict[tuple[str, str], str | None] = {
    ("full_study", "status"): "fis_status",  # Full Study Table 2017-02..08 (Complete/Incomplete)
    ("fuel_chart", "status"): None,  # Wind/Solar Chart: always PLANNED
}

_MISSING = object()


def stable_name(label: str, section: str | None) -> object:
    key = norm_label(label)
    if (section, key) in SECTION_HEADER_MAP:
        return SECTION_HEADER_MAP[(section, key)]
    return HEADER_MAP.get(key, _MISSING)


# --- sheet kinds -------------------------------------------------------------------------------------

# Merge priority: the project tables describe the project; the update tables only add their event.
SECTION_PRIORITY = [
    "project_details", "small_gen", "ia_table", "full_study", "qsa", "fuel_chart",
    "new_ia", "synchronization", "commercial_operation", "commissioning", "inactive", "cancellation",
]
ACTIVE_SECTIONS = {"project_details", "small_gen", "ia_table", "full_study", "qsa", "fuel_chart", "new_ia"}


def section_of(sheet: str, columns: set[str]) -> str | None:
    s = clean_label(sheet).lower()
    if "small gen" in s:
        return "small_gen"
    if s.startswith("project details"):
        return "project_details"
    if "commissioning" in s:
        return "commissioning"
    if "cancellation update" in s:
        return "cancellation"
    if s.startswith("inactive"):
        return "inactive"
    if s == "ia table":
        return "ia_table"
    if s == "full study table":
        return "full_study"
    if s.endswith(" chart"):
        return "fuel_chart"
    if s == "qsa units":
        return "qsa"
    if s.startswith(("new ", "newly ")):  # New Units, New and Cancelled Units, Newly Operational & Cancelled
        for col, section in (("cancel_date", "cancellation"), ("commercial_operation_date", "commercial_operation"),
                             ("approved_synchronization", "synchronization"), ("ia_signed", "new_ia")):
            if col in columns:
                return section
    return None


# --- typing ------------------------------------------------------------------------------------------

_EXCEL_EPOCH = date(1899, 12, 30)
_PLACEHOLDER_DATES = [date(1900, 1, 1), date(1899, 12, 31), date(1899, 12, 30)]


def to_date(col: str) -> pl.Expr:
    """Text → Date: ISO datetimes (rounded to the second), Excel serials, m/d/Y, m/Y; placeholders → null."""
    s = pl.col(col).str.strip_chars()
    stamp = s.str.strptime(pl.Datetime("ms"), "%Y-%m-%d %H:%M:%S%.f", strict=False).dt.round("1s").dt.date()
    iso = s.str.slice(0, 10).str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    serial = s.cast(pl.Float64, strict=False)
    from_serial = pl.lit(_EXCEL_EPOCH) + pl.duration(days=serial.round(0).cast(pl.Int64, strict=False))
    month_year = pl.concat_str(pl.lit("1/"), s).str.strptime(pl.Date, "%d/%m/%Y", strict=False)
    parsed = pl.coalesce(
        stamp, iso,
        pl.when(serial.is_between(1, 80000)).then(from_serial.cast(pl.Date)),
        s.str.strptime(pl.Date, "%m/%d/%Y", strict=False),
        s.str.strptime(pl.Date, "%m/%d/%y", strict=False),
        month_year,
    )
    return pl.when(parsed.is_in(_PLACEHOLDER_DATES)).then(None).otherwise(parsed)


def capacity(col: str = "capacity_mw") -> pl.Expr:
    return num(pl.col(col).str.replace(r"\*+$", ""))


def to_bool(col: str) -> pl.Expr:
    s = pl.col(col).str.strip_chars().str.to_lowercase()
    return pl.when(s.is_in(["yes", "true", "y"])).then(True).when(s.is_in(["no", "false", "n"])).then(False)


FUEL_TYPES = {
    "storage": ["BAT", "BATTERY", "BATTERY STORAGE", "STORAGE", "ENERGY STORAGE", "STORAGE/BATTERY", "MWH"],
    "wind": ["WIN", "WIND"],
    "solar": ["SOL", "SOLAR"],
    "gas": ["GAS", "GAS/CE", "NATURAL GAS", "GAS-ALLOTHER", "GAS-COMBINEDCYCLE"],
    "coal": ["COA", "COAL", "LIG", "LIGNITE"],
    "nuclear": ["NUC", "NUCLEAR"],
    "hydro": ["HYD", "HYDRO", "WAT", "WATER"],
    "biomass": ["BIO", "BIOMASS"],
    "oil": ["OIL", "FUEL OIL", "PETROLEUM COKE", "PETCOKE"],
    "other": ["OTH", "OTHER"],
}
_FUEL_LOOKUP = {code: family for family, codes in FUEL_TYPES.items() for code in codes}


def fuel_type(fuel: str = "fuel", technology: str = "technology") -> pl.Expr:
    """Normalized fuel family (derived): battery technology codes win over the ``OTH`` fuel code."""
    code = pl.col(fuel).str.strip_chars().str.to_uppercase()
    tech = pl.col(technology).str.strip_chars().str.to_uppercase()
    family = code.replace_strict(_FUEL_LOOKUP, default="other", return_dtype=pl.String)
    return (
        pl.when(tech.is_in(["BA", "EN"])).then(pl.lit("storage"))
        .when(code.is_null()).then(None)
        .otherwise(family)
    )


# --- gis_cells ---------------------------------------------------------------------------------------

CELL_SCHEMA = {
    "report_month": pl.Date,
    "sheet": pl.String,
    "row_index": pl.Int32,
    "column_index": pl.Int32,
    "header_row_index": pl.Int32,
    "header_label": pl.String,
    "value": pl.String,
}


def cells_frame(sheets: Sequence[Sheet], report_month) -> pl.DataFrame:
    cols: dict[str, list] = {k: [] for k in CELL_SCHEMA if k != "report_month"}
    for sheet in sheets:
        row_block: dict[int, Block] = {}
        for b in find_blocks(sheet.rows):
            for i in range(b.data_start, b.data_end):
                row_block[i] = b
        for i, row in enumerate(sheet.rows):
            block = row_block.get(i)
            for j, value in enumerate(row):
                if value is None or not value.strip():
                    continue
                cols["sheet"].append(sheet.name)
                cols["row_index"].append(sheet.row0 + i)
                cols["column_index"].append(sheet.cols[j])
                cols["header_row_index"].append(sheet.row0 + block.header_row if block else None)
                cols["header_label"].append((block.labels[j] or None) if block else None)
                cols["value"].append(value)
    df = pl.DataFrame(cols, schema={k: v for k, v in CELL_SCHEMA.items() if k != "report_month"})
    return df.with_columns(pl.lit(report_month, pl.Date).alias("report_month")).select(list(CELL_SCHEMA))


def parse_cells(f: RawFile) -> pl.DataFrame | None:
    df = cells_frame(read_sheets(workbook_bytes(f)), f.dt)
    if not df.height:
        raise ValueError(f"{f.key}: no cells read")
    return df


# --- records from the INR tables ---------------------------------------------------------------------


def iter_records(sheets: Sequence[Sheet], *, key: str = "inr",
                 section_fn=section_of, unknown: set[str] | None = None) -> Iterator[dict]:
    """One dict per data row of every recognised table: stable column → cleaned text, plus ``sheet``,
    ``section`` and ``extra_fields`` (unmapped headers)."""
    for sheet in sheets:
        for b in find_blocks(sheet.rows):
            mapped = {j: stable_name(label, None) for j, label in enumerate(b.labels) if label}
            section = section_fn(sheet.name, {v for v in mapped.values() if isinstance(v, str)})
            if section is None:
                if key == "inr" and norm_label(b.labels[b.key_col]) != "unit name":
                    log.warning("GIS sheet %r has an INR table of unknown kind; kept in gis_cells only", sheet.name)
                continue
            names = {j: stable_name(label, section) for j, label in enumerate(b.labels)}
            for row in sheet.rows[b.data_start:b.data_end]:
                ident = _text(row[b.key_col])
                if not ident:
                    continue
                if key == "inr" and not _INR_CODE.match(ident):
                    continue  # notes, "NONE.", sub-table titles
                rec: dict = {"sheet": sheet.name, "section": section}
                extra: dict[str, str] = {}
                for j, value in enumerate(row):
                    text = _text(value)
                    if not text:
                        continue
                    name = names.get(j, _MISSING)
                    if name is _MISSING and not b.labels[j] and text == ident:
                        continue  # an unlabelled copy of the INR column (Cancellation Update 2020-04)
                    if name is _MISSING:
                        label = b.labels[j] or f"col_{sheet.cols[j]}"
                        extra[label] = text
                        if unknown is not None:
                            unknown.add(label)
                    elif name is not None and name not in rec:
                        rec[name] = text
                if "inr" in rec:
                    rec["inr"] = norm_inr(rec["inr"])
                rec["extra_fields"] = json.dumps(extra, ensure_ascii=False) if extra else None
                yield rec


# --- gis_snapshots -----------------------------------------------------------------------------------

DATE_COLUMNS = [
    "projected_cod", "site_control_approved", "screening_study_started", "screening_study_complete",
    "fis_requested", "fis_approved", "ia_signed", "meets_planning_6_9_1", "meets_all_planning_6_9",
    "meets_qsa_prerequisites", "construction_start", "construction_end", "model_ready_date",
    "approved_energization", "approved_synchronization", "commercial_operation_date", "cancel_date",
    "inactive_date", "public_letter_date",
]
BOOL_COLUMNS = [
    "economic_study_required", "financial_security_provided", "financial_security_required_dist",
    "meets_planning_6_9_met", "meets_planning_6_9_1bd_met",
]
PERMIT_COLUMNS = ["air_permit", "ghg_permit", "water_availability"]

SNAPSHOT_SCHEMA: dict[str, pl.DataType] = {
    "inr": pl.String,
    "report_month": pl.Date,
    "status": pl.String,
    "sheets": pl.String,
    "sections": pl.String,
    "size_category": pl.String,
    "project_name": pl.String,
    "interconnecting_entity": pl.String,
    "poi_location": pl.String,
    "county": pl.String,
    "cdr_reporting_zone": pl.String,
    "fuel": pl.String,
    "technology": pl.String,
    "fuel_type": pl.String,
    "capacity_mw": pl.Float64,
    "study_phase": pl.String,
    "fis_status": pl.String,
    "projected_cod": pl.Date,
    "change_indicators": pl.String,
    "site_control_approved": pl.Date,
    "screening_study_started": pl.Date,
    "screening_study_complete": pl.Date,
    "fis_requested": pl.Date,
    "fis_approved": pl.Date,
    "economic_study_required": pl.Boolean,
    "ia_signed": pl.Date,
    "financial_security_provided": pl.Boolean,
    "financial_security_required_dist": pl.Boolean,
    "air_permit": pl.String,
    "air_permit_date": pl.Date,
    "ghg_permit": pl.String,
    "ghg_permit_date": pl.Date,
    "water_availability": pl.String,
    "water_availability_date": pl.Date,
    "meets_planning_6_9_1": pl.Date,
    "meets_all_planning_6_9": pl.Date,
    "meets_qsa_prerequisites": pl.Date,
    "meets_planning_6_9_met": pl.Boolean,
    "meets_planning_6_9_1bd_met": pl.Boolean,
    "construction_start": pl.Date,
    "construction_end": pl.Date,
    "model_ready_date": pl.Date,
    "approved_energization": pl.Date,
    "approved_synchronization": pl.Date,
    "commercial_operation_date": pl.Date,
    "commissioning_categories": pl.String,
    "cancel_date": pl.Date,
    "inactive_date": pl.Date,
    "public_letter_date": pl.Date,
    "comment": pl.String,
    "extra_fields": pl.String,
}
# Every column a table row can carry (stable names), all text until typed.
RAW_FIELDS = sorted({v for v in [*HEADER_MAP.values(), *SECTION_HEADER_MAP.values()] if v})


def _json_list(col: str) -> pl.Expr:
    return pl.col(col).map_elements(lambda v: json.dumps(list(v), ensure_ascii=False), return_dtype=pl.String)


def _route_commissioning(df: pl.DataFrame) -> pl.DataFrame:
    """A Commissioning Update row says which approval its date is: energization, synchronization or
    commercial operation ("... Revoked" rows keep only their category)."""
    cat = pl.col("commissioning_category").str.to_lowercase()
    approval = pl.col("commissioning_approval_date")
    revoked = cat.str.contains("revok")
    return df.with_columns(
        pl.coalesce("approved_energization", pl.when(cat.str.contains("energiz") & ~revoked).then(approval))
        .alias("approved_energization"),
        pl.coalesce("approved_synchronization", pl.when(cat.str.contains("synchroniz") & ~revoked).then(approval))
        .alias("approved_synchronization"),
        pl.coalesce("commercial_operation_date", pl.when(cat.str.contains("commercial") & ~revoked).then(approval))
        .alias("commercial_operation_date"),
    )


def snapshot_frame(records: list[dict], report_month) -> pl.DataFrame:
    schema = {c: pl.String for c in ["sheet", "section", "extra_fields", *RAW_FIELDS]}
    raw = pl.DataFrame(records, schema=schema) if records else pl.DataFrame(schema=schema)
    raw = _route_commissioning(raw).with_columns(
        pl.col("section").replace_strict({s: i for i, s in enumerate(SECTION_PRIORITY)}, return_dtype=pl.Int32)
        .alias("_priority"),
    )
    value_cols = [c for c in RAW_FIELDS if c != "inr"]
    merged = (
        raw.sort("_priority", maintain_order=True)
        .group_by("inr", maintain_order=True)
        .agg(
            *[pl.col(c).drop_nulls().first() for c in value_cols],
            pl.col("sheet").unique(maintain_order=True).alias("_sheets"),
            pl.col("section").unique(maintain_order=True).alias("_sections"),
            pl.col("commissioning_category").drop_nulls().str.strip_chars_end("*").str.strip_chars()
            .unique(maintain_order=True).alias("_categories"),
            pl.col("extra_fields").drop_nulls().alias("_extra"),
        )
    )
    sections = pl.col("_sections")
    status = (
        pl.when(sections.list.contains("cancellation")).then(pl.lit("cancelled"))
        .when(sections.list.contains("inactive")).then(pl.lit("inactive"))
        .when(pl.any_horizontal([sections.list.contains(s) for s in ACTIVE_SECTIONS])).then(pl.lit("active"))
        .otherwise(pl.lit("commissioning_update"))
    )
    size = pl.coalesce(
        pl.col("size_category"),
        pl.when(sections.list.contains("small_gen") & ~sections.list.contains("project_details"))
        .then(pl.lit("Small")),
        pl.when(pl.col("_sheets").list.eval(pl.element().str.to_lowercase().str.contains("large gen")).list.any())
        .then(pl.lit("Large")),
    )
    out = merged.with_columns(
        pl.lit(report_month, pl.Date).alias("report_month"),
        status.alias("status"),
        size.alias("size_category"),
        _json_list("_sheets").alias("sheets"),
        _json_list("_sections").alias("sections"),
        pl.when(pl.col("_categories").list.len() > 0).then(_json_list("_categories")).alias("commissioning_categories"),
        pl.when(pl.col("_extra").list.len() > 0)
        .then(pl.col("_extra").map_elements(
            lambda v: json.dumps({k: x for d in v for k, x in json.loads(d).items()}, ensure_ascii=False),
            return_dtype=pl.String))
        .alias("extra_fields"),
        capacity().alias("capacity_mw"),
        *[to_date(c).alias(c) for c in DATE_COLUMNS],
        *[to_bool(c).alias(c) for c in BOOL_COLUMNS],
        *[to_date(c).alias(f"{c}_date") for c in PERMIT_COLUMNS],
    ).with_columns(
        *[pl.when(~pl.col(c).str.contains(r"^\d{4}-\d{2}-\d{2}")).then(pl.col(c)).alias(c) for c in PERMIT_COLUMNS],
        fuel_type().alias("fuel_type"),
    )
    return out.select([pl.col(c).cast(t) for c, t in SNAPSHOT_SCHEMA.items()]).sort("inr")


def parse_snapshots(f: RawFile) -> pl.DataFrame | None:
    unknown: set[str] = set()
    sheets = read_sheets(workbook_bytes(f))
    records = list(iter_records(sheets, unknown=unknown))
    if unknown:
        log.warning("%s: headers not in HEADER_MAP kept in extra_fields: %s", f.key, sorted(unknown))
    if not any(r["section"] in ACTIVE_SECTIONS for r in records):
        raise ValueError(f"{f.key}: no project table found (sheets: {[s.name for s in sheets]})")
    return snapshot_frame(records, f.dt)


# --- gis_colocated_battery ---------------------------------------------------------------------------

BATTERY_SCHEMA: dict[str, pl.DataType] = {
    "report_month": pl.Date,
    "sheet": pl.String,
    "colocation": pl.String,
    "inr": pl.String,
    "unit_name": pl.String,
    "unit_code": pl.String,
    "project_name": pl.String,
    "project_status": pl.String,
    "interconnecting_entity": pl.String,
    "poi_location": pl.String,
    "county": pl.String,
    "cdr_reporting_zone": pl.String,
    "projected_cod": pl.Date,
    "fuel": pl.String,
    "technology": pl.String,
    "fuel_type": pl.String,
    "is_battery": pl.Boolean,
    "capacity_mw": pl.Float64,
    "ia_signed": pl.Date,
    "financial_security_provided": pl.Boolean,
    "approved_energization": pl.Date,
    "approved_synchronization": pl.Date,
    "in_service_year": pl.Int32,
    "comment": pl.String,
    "extra_fields": pl.String,
}


def colocation_of(sheet: str, columns: set[str]) -> str | None:
    s = clean_label(sheet).lower()
    if s.startswith("co-located with "):
        return s.removeprefix("co-located with ").strip()  # solar | wind | thermal
    if s == "stand-alone":
        return "stand_alone"
    if s.startswith("co-located") and ("operational" in s or "commercial approved" in s):
        return "operational"  # units, no INR (Co-located Commercial Approved 2021-08..2022-06, then Operational)
    return None


def parse_colocated_battery(f: RawFile) -> pl.DataFrame | None:
    unknown: set[str] = set()
    sheets = read_sheets(workbook_bytes(f))
    records = []
    for rec in iter_records(sheets, key="any", section_fn=colocation_of, unknown=unknown):
        if "inr" not in rec and "unit_code" not in rec:
            continue  # notes under the table
        if "inr" in rec and not _INR_CODE.match(rec["inr"]):
            continue
        rec["colocation"] = rec.pop("section")
        records.append(rec)
    if unknown:
        log.warning("%s: headers not in HEADER_MAP kept in extra_fields: %s", f.key, sorted(unknown))
    if not records:
        raise ValueError(f"{f.key}: no co-located battery table found (sheets: {[s.name for s in sheets]})")
    schema = {c: pl.String for c in ["sheet", "colocation", "extra_fields", *RAW_FIELDS]}
    df = pl.DataFrame(records, schema=schema)
    tech = pl.col("technology").str.strip_chars().str.to_uppercase()
    return df.with_columns(
        pl.lit(f.dt, pl.Date).alias("report_month"),
        capacity().alias("capacity_mw"),
        *[to_date(c).alias(c) for c in ("projected_cod", "ia_signed", "approved_energization",
                                         "approved_synchronization")],
        to_bool("financial_security_provided").alias("financial_security_provided"),
        num("in_service_year").cast(pl.Int32, strict=False).alias("in_service_year"),
        fuel_type().alias("fuel_type"),
        (tech.is_in(["BA", "EN"]) | (pl.col("fuel").str.to_uppercase() == "BAT")).alias("is_battery"),
    ).select([pl.col(c).cast(t) for c, t in BATTERY_SCHEMA.items()])


# --- file selection ----------------------------------------------------------------------------------

_REVISION = re.compile(r"correct|revis|update", re.I)


def _version_key(f: RawFile) -> tuple:
    return (f.meta.get("publish_date") or "", bool(_REVISION.search(f.name)), f.entry.fetched_at, f.key)


def one_file_per_month(files: list[RawFile]) -> list[RawFile]:
    """The newest workbook of each report month: latest listing publish date, else a revised/corrected
    name (the Resource Adequacy pages carry no date). Corrections are complete reports (their
    ``CORRECTIONS`` sheet says what changed)."""
    best: dict = {}
    for f in files:
        if f.dt not in best or _version_key(f) > _version_key(best[f.dt]):
            best[f.dt] = f
    return sorted(best.values(), key=lambda f: (f.dt, f.entry.fetched_at, f.key))


def _family(name: str):
    return lambda f: f.meta.get("family") == name and f.suffix in {".xlsx", ".xls", ".zip"}


DATASETS = [
    Dataset(
        name="gis_cells",
        target="bigquery",
        mode="by_file",
        description="Every non-empty cell of every sheet of the monthly GIS report (long format, raw text), "
                    "with the composite header label of its table; 0-based absolute sheet coordinates.",
        parse=parse_cells,
        inputs=_family("gis_report"),
        select=one_file_per_month,
        partition=("report_month", "MONTH"),
        cluster=("sheet",),
    ),
    Dataset(
        name="gis_snapshots",
        target="postgres",
        mode="by_file",
        description="Generator interconnection queue, one row per INR and report month (all sheets merged), "
                    "stable column names across the 2014-2026 layouts, milestone dates and a derived status.",
        parse=parse_snapshots,
        inputs=_family("gis_report"),
        select=one_file_per_month,
    ),
    Dataset(
        name="gis_colocated_battery",
        target="postgres",
        mode="by_file",
        description="Co-located battery identification reports: one row per battery or co-located project "
                    "(INR) and colocation sheet, plus the operational co-located units.",
        parse=parse_colocated_battery,
        inputs=_family("co_located_battery"),
        select=one_file_per_month,
    ),
]

# Survival events per INR. Only the newest source file of each report month is read (a correction that
# arrives after its original leaves the original's rows in gis_snapshots; see by_file mode).
GIS_PROJECT_EVENTS_SQL = r"""
WITH files AS (
    SELECT report_month, source_file,
           row_number() OVER (PARTITION BY report_month ORDER BY max(ingested_at) DESC, source_file DESC) AS rn
    FROM gis_snapshots
    GROUP BY report_month, source_file
),
s AS (
    SELECT g.* FROM gis_snapshots g JOIN files f USING (report_month, source_file) WHERE f.rn = 1
),
latest AS (SELECT max(report_month) AS latest_report_month FROM s),
agg AS (
    SELECT inr,
           min(report_month) AS first_seen_month,
           max(report_month) AS last_seen_month,
           count(*) AS months_seen,
           min(screening_study_started) AS screening_study_started,
           min(screening_study_complete) AS screening_study_complete,
           min(fis_requested) AS fis_requested,
           min(fis_approved) AS fis_approved,
           min(ia_signed) AS ia_signed,
           min(construction_start) AS construction_start,
           min(approved_energization) AS approved_energization,
           min(approved_synchronization) AS approved_synchronization,
           min(commercial_operation_date) AS commercial_operation_date,
           min(cancel_date) AS cancel_date,
           min(inactive_date) AS inactive_date,
           min(report_month) FILTER (
               WHERE ia_signed IS NOT NULL OR sections LIKE '%"ia_table"%' OR sections LIKE '%"new_ia"%'
                  OR study_phase ~ ',\s*IA$') AS ia_first_month,
           min(report_month) FILTER (WHERE fis_requested IS NOT NULL OR sections LIKE '%"full_study"%')
               AS fis_first_month,
           min(report_month) FILTER (WHERE approved_energization IS NOT NULL) AS energization_first_month,
           min(report_month) FILTER (WHERE approved_synchronization IS NOT NULL) AS synchronization_first_month,
           min(report_month) FILTER (WHERE commercial_operation_date IS NOT NULL) AS cod_first_month,
           min(report_month) FILTER (WHERE status = 'cancelled') AS cancelled_month,
           max(report_month) FILTER (WHERE status = 'cancelled') AS last_cancelled_month,
           max(report_month) FILTER (WHERE status = 'active') AS last_active_month,
           min(report_month) FILTER (WHERE status = 'inactive') AS inactive_first_month
    FROM s
    GROUP BY inr
),
first_row AS (
    SELECT DISTINCT ON (inr) inr, status AS first_status, sections AS first_sections
    FROM s ORDER BY inr, report_month
),
last_row AS (
    SELECT DISTINCT ON (inr) inr, status AS last_status FROM s ORDER BY inr, report_month DESC
),
-- descriptive attributes: the latest row of a project table (the update tables print fuel as words and
-- have no technology), else the latest row
attrs AS (
    SELECT DISTINCT ON (inr) inr, project_name, interconnecting_entity, county, cdr_reporting_zone, fuel,
           technology, fuel_type, capacity_mw, size_category, projected_cod
    FROM s ORDER BY inr, (status = 'active') DESC, (technology IS NOT NULL) DESC, report_month DESC
),
labelled AS (
    SELECT a.*, f.first_status, f.first_sections, l.last_status, t.project_name, t.interconnecting_entity,
           t.county, t.cdr_reporting_zone, t.fuel, t.technology, t.fuel_type, t.capacity_mw, t.size_category,
           t.projected_cod, x.latest_report_month,
           CASE
               WHEN a.cod_first_month IS NOT NULL THEN 'operational'
               -- a cancellation counts unless the project is listed as active again afterwards
               WHEN a.last_cancelled_month >= coalesce(a.last_active_month, DATE '0001-01-01') THEN 'cancelled'
               WHEN a.last_seen_month = x.latest_report_month AND l.last_status = 'active' THEN 'active'
               WHEN a.last_seen_month = x.latest_report_month AND l.last_status = 'inactive' THEN 'inactive'
               WHEN a.synchronization_first_month IS NOT NULL THEN 'operational'
               WHEN l.last_status = 'inactive' THEN 'inactive'
               WHEN a.last_seen_month = x.latest_report_month THEN 'active'
               ELSE 'dropped'
           END AS exit_status
    FROM agg a
    JOIN first_row f USING (inr)
    JOIN last_row l USING (inr)
    JOIN attrs t USING (inr)
    CROSS JOIN latest x
)
SELECT inr, project_name, interconnecting_entity, county, cdr_reporting_zone, fuel, technology, fuel_type,
       capacity_mw, size_category, projected_cod,
       first_seen_month, first_status, first_sections, last_seen_month, last_status, months_seen,
       ((EXTRACT(YEAR FROM age(last_seen_month, first_seen_month)) * 12
         + EXTRACT(MONTH FROM age(last_seen_month, first_seen_month)))::int + 1 - months_seen) AS months_missing,
       latest_report_month,
       coalesce(screening_study_started, first_seen_month) AS entry_date,
       screening_study_started, screening_study_complete, fis_requested, fis_approved, ia_signed,
       construction_start, approved_energization, approved_synchronization, commercial_operation_date,
       cancel_date, inactive_date,
       fis_first_month, ia_first_month, energization_first_month, synchronization_first_month,
       cod_first_month, cancelled_month, inactive_first_month, last_active_month,
       exit_status,
       CASE exit_status
           WHEN 'operational' THEN coalesce(cod_first_month, (last_seen_month + interval '1 month')::date)
           WHEN 'cancelled' THEN last_cancelled_month
           WHEN 'inactive' THEN CASE WHEN last_seen_month = latest_report_month THEN NULL
                                     ELSE inactive_first_month END
           WHEN 'dropped' THEN (last_seen_month + interval '1 month')::date
       END AS exit_month,
       (exit_status = 'dropped' OR (exit_status = 'operational' AND cod_first_month IS NULL)) AS exit_inferred
FROM labelled
"""

SQL_DATASETS = [
    SqlDataset(
        name="gis_project_events",
        sql=GIS_PROJECT_EVENTS_SQL,
        description="Per INR: entry, milestone dates and the first report month each appears, last month "
                    "seen, and exit status (operational / cancelled / inactive / active / dropped) for the "
                    "generation-queue survival model.",
        indexes=(("inr",), ("exit_status",)),
    ),
]
