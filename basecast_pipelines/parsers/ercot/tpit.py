"""Transmission Project and Information Tracking (TPIT): TSP-submitted transmission projects, one row per
project per snapshot, 2007 → July 2026.

Two raw files: the current workbook (ERCOT reuses an old ``/files/docs/`` path for it) and the archive zip
with every published snapshot since 2009, plus cumulative 1999–2010 files (one zip nested inside). Each
workbook has one sheet per status (``Future``, ``Planned``/``Current Year Planned``, ``Completed``,
``Cancelled``; ``ERCOTTPITPast2Years`` in the cumulative files) with the header on row 2 or 3. Columns are
matched by their header text (``COLUMNS``); unknown columns are ignored.

- The snapshot date comes from the sheet names (``FutureTPIT071326NoCost``) or, when they carry none, from
  the file name; the cumulative 1999–20xx files have no snapshot date (null).
- In the archive, a snapshot published in several workbooks (``UPDATED`` re-issues, ``Comparison_Results``
  copies, ``_old`` files) is read once, from the preferred workbook (update > plain > old > comparison).
  The current workbook is also inside the archive, so its snapshot appears in both raw files;
  ``tpit_project_history`` (SQL) reads each snapshot from one raw file only.
- Personal data: the ``TSP/Company Contact`` column (names, emails, phones) is dropped, and emails and phone
  numbers are masked in the free-text columns.
- Not read: ``RTPProjects``/``TPIT5YearPlanProjects`` (RTP project lists), ``ImprovementCostSummary`` (cost
  by year and kV, not by project: the public TPIT has no per-project cost), ``TransmissionOwnerProjContac*``
  (contacts), ``TSPResponsibility``, ``Cost Summary Info`` and ``Read Me``.

Counties: ``county_start`` / ``county_end`` as published, with ``county_start_fips`` / ``county_end_fips``
when the name is a Texas county."""

from __future__ import annotations

import re
from datetime import date
from pathlib import PurePosixPath

import polars as pl

from basecast_pipelines.parsers.ercot._adequacy_common import Workbook, iter_zip_members
from basecast_pipelines.parsers.ercot._tx_counties import county_fips
from basecast_pipelines.processing.core import Dataset, RawFile, SqlDataset
from basecast_pipelines.processing.tabular import excel_date, find_header_row, num, with_header

SOURCE_ID = "ercot_tpit"

# (output column, header regex, kind)
COLUMNS: list[tuple[str, str, str]] = [
    ("project_id", r"^ercot project number", "text"),
    ("project_title", r"^project title|^project\s+name", "text"),
    ("project_description", r"^project description", "text"),
    ("comments", r"^comments", "text"),
    ("terminal_from", r"^terminal\W+from", "text"),
    ("terminal_to", r"^terminal\W+to", "text"),
    ("transmission_status", r"^transmission status", "text"),
    ("associated_projects", r"^associated projects", "text"),
    ("tsp", r"^transmission owner(\s*\(text\))?$", "text"),
    ("tsp_project_number", r"^transmission owner project number", "text"),
    ("projected_in_service_date", r"^projected in.?service", "date"),
    ("actual_in_service_date", r"^actual in.?service", "date"),
    ("kv", r"service level kv", "number"),
    ("circuit_miles_new", r"circuit miles new", "number"),
    ("circuit_miles_upgraded", r"circuit miles rebuilt", "number"),
    ("autotransformer_mva", r"autotransformer capacity", "number"),
    ("reactive_mvar", r"reactive capability", "number"),
    ("county_start", r"^county location (for|of) (substation|starting)", "text"),
    ("county_end", r"^county location (for|of) ending", "text"),
    ("planning_charter_tier", r"planning charter tier", "text"),
    ("rpg_number", r"^rpg number", "text"),
    ("rpg_submitted_date", r"submitted to ercot for rpg", "date"),
    ("rpg_completed_date", r"rpg review completed", "date"),
    ("bod_review_date", r"bod review completed", "date"),
    ("sswg_bus_numbers", r"sswg base case related bus", "text"),
    ("in_sswg_case", r"reflected in sswg", "text"),
    ("part_of_interface", r"part of interface", "text"),
    ("requested_additional_information", r"^requested additional information", "text"),
    ("other_info", r"^other\b", "text"),
    ("phase_number", r"^phase number", "text"),
    ("mod_project_number", r"^mod project number", "text"),
    ("rtp_project_number", r"^rtp project number", "text"),
]
_PERSONAL = re.compile(r"contact", re.IGNORECASE)
_FREE_TEXT = ("project_title", "project_description", "comments", "requested_additional_information", "other_info")
_EMAIL = r"[\w.+'-]+@[\w-]+(\.[\w-]+)+"
_PHONE = r"\(?\b\d{3}\)?[-.\s]*\d{3}[-.\s]*\d{4}\b"

_STATUS_SHEETS = [
    ("future", re.compile(r"future", re.IGNORECASE)),
    ("planned", re.compile(r"planned", re.IGNORECASE)),
    ("completed", re.compile(r"completed", re.IGNORECASE)),
    ("cancelled", re.compile(r"cancel", re.IGNORECASE)),
    ("past_2_years", re.compile(r"past\s*2\s*years", re.IGNORECASE)),
]
_NOT_PROJECT_SHEETS = re.compile(r"5\s*year|rtpprojects|contact|improvement\s*cost|cost summary|responsibility|read me", re.IGNORECASE)

SCHEMA: dict[str, pl.DataType] = {
    "snapshot_date": pl.Date,
    "workbook": pl.String,
    "sheet": pl.String,
    "sheet_status": pl.String,
    **{name: {"text": pl.String, "date": pl.Date, "number": pl.Float64}[kind] for name, _, kind in COLUMNS},
    "county_start_fips": pl.String,
    "county_end_fips": pl.String,
}


def snapshot_of(text: str) -> date | None:
    """``FutureTPIT071326NoCost`` → 2026-07-13, ``TPITFutureNoCost11012009`` → 2009-11-01."""
    for run in re.findall(r"(?<!\d)(\d{8}|\d{6})(?!\d)", text):
        month, day = int(run[:2]), int(run[2:4])
        year = int(run[4:]) if len(run) == 8 else 2000 + int(run[4:])
        if 1 <= month <= 12 and 1 <= day <= 31 and 1999 <= year <= 2099:
            return date(year, month, day)
    return None


def _sheet_status(sheet: str) -> str | None:
    if _NOT_PROJECT_SHEETS.search(sheet):
        return None
    return next((status for status, pattern in _STATUS_SHEETS if pattern.search(sheet)), None)


def _date_expr(col: str) -> pl.Expr:
    """Excel serials, ISO strings, ``m/d/yy``, ``m/d/yyyy``, ``m/yyyy`` and ``Mon-yy``/``Mon yyyy``."""
    s = pl.col(col).str.strip_chars()
    month_year = pl.concat_str([pl.lit("1/"), s]).str.strptime(pl.Date, "%d/%m/%Y", strict=False)
    mon = pl.concat_str([pl.lit("1 "), s.str.replace_all(r"[-/]", " ")])
    return pl.coalesce(
        excel_date(s, formats=("%m/%d/%y", "%m/%d/%Y")),
        pl.when(s.str.contains(r"^\d{1,2}/\d{4}$")).then(month_year),
        mon.str.strptime(pl.Date, "%d %b %Y", strict=False),
        mon.str.strptime(pl.Date, "%d %b %y", strict=False),
        mon.str.strptime(pl.Date, "%d %B %Y", strict=False),
    )


def _text_expr(col: str) -> pl.Expr:
    s = pl.col(col).str.replace_all(r"\s+", " ").str.strip_chars()
    s = s.str.replace(r"^(-?\d+)\.0$", "$1")
    return pl.when(s.is_in(["", "-", "N/A", "n/a", "NA"])).then(None).otherwise(s)


def parse_sheet(frame: pl.DataFrame, sheet: str) -> pl.DataFrame | None:
    header = find_header_row(frame, [r"ercot project number", r"transmission owner"], max_scan=10)
    if header is None:
        return None
    body = with_header(frame, header, rename=lambda v: re.sub(r"\s+", " ", str(v)).strip().lower())
    exprs = []
    for name, pattern, kind in COLUMNS:
        source = next((c for c in body.columns if re.search(pattern, c) and not _PERSONAL.search(c)), None)
        if source is None:
            exprs.append(pl.lit(None, dtype=SCHEMA[name]).alias(name))
        elif kind == "date":
            exprs.append(_date_expr(source).alias(name))
        elif kind == "number":
            exprs.append(num(source).alias(name))
        else:
            exprs.append(_text_expr(source).alias(name))
    out = body.select(exprs).filter(pl.col("project_id").is_not_null() | pl.col("project_title").is_not_null())
    # rows that repeat the header or carry notes: no TSP and no in-service date
    out = out.filter(
        pl.col("tsp").is_not_null()
        | pl.col("projected_in_service_date").is_not_null()
        | pl.col("actual_in_service_date").is_not_null()
    )
    out = out.with_columns(
        [pl.col(c).str.replace_all(_EMAIL, "[email removed]").str.replace_all(_PHONE, "[phone removed]") for c in _FREE_TEXT]
    )
    return out


def parse_workbook(payload: bytes, name: str) -> pl.DataFrame | None:
    wb = Workbook(payload, name=name)
    status_sheets = [(s, _sheet_status(s)) for s in wb.sheet_names]
    status_sheets = [(s, st) for s, st in status_sheets if st]
    if not status_sheets:
        return None
    snapshot = next((d for d in (snapshot_of(s) for s, _ in status_sheets) if d), None) or snapshot_of(
        PurePosixPath(name).name
    )
    frames = []
    for sheet, status in status_sheets:
        df = parse_sheet(wb.frame(sheet), sheet)
        if df is None:
            raise ValueError(f"{name} / {sheet}: no 'ERCOT Project Number' header")
        frames.append(
            df.with_columns(
                pl.lit(snapshot, dtype=pl.Date).alias("snapshot_date"),
                pl.lit(name).alias("workbook"),
                pl.lit(sheet).alias("sheet"),
                pl.lit(status).alias("sheet_status"),
            )
        )
    out = pl.concat(frames, how="vertical_relaxed")
    return out.with_columns(
        pl.col("county_start").map_elements(county_fips, return_dtype=pl.String).alias("county_start_fips"),
        pl.col("county_end").map_elements(county_fips, return_dtype=pl.String).alias("county_end_fips"),
    ).select(list(SCHEMA))


def _preference(name: str) -> int:
    lower = PurePosixPath(name).name.lower()
    if "comparison" in lower or "comparion" in lower:
        return 3
    if re.search(r"_old\b|_old\.", lower):
        return 2
    if "update" in lower:
        return 0
    return 1


def parse_projects(f: RawFile) -> pl.DataFrame | None:
    if f.suffix in {".xlsx", ".xls"}:
        return parse_workbook(f.read_bytes(), f.name)
    if f.suffix != ".zip":
        return None
    members = list(iter_zip_members(f.read_bytes(), suffixes=(".xlsx", ".xls")))
    parsed = []
    for name, payload in members:
        df = parse_workbook(payload, name)
        if df is not None and df.height:
            parsed.append((name, df))
    if not parsed:
        raise ValueError(f"{f.key}: no TPIT workbook in the archive")
    best: dict[object, tuple[int, str]] = {}
    for name, df in parsed:
        snapshot = df["snapshot_date"][0] or name  # cumulative files (no date) stand alone
        rank = (_preference(name), name)
        if snapshot not in best or rank < best[snapshot]:
            best[snapshot] = rank
    keep = {name for _, name in best.values()}
    return pl.concat([df for name, df in parsed if name in keep], how="vertical_relaxed")


DATASETS = [
    Dataset(
        name="tpit_projects",
        target="postgres",
        mode="by_file",
        description=(
            "ERCOT TPIT transmission projects, one row per project per snapshot (2007 → Jul 2026): TSP, "
            "status, projected/actual in-service dates, kV, miles, counties with FIPS, RPG/SSWG references. "
            "Contacts dropped at parse."
        ),
        parse=parse_projects,
        inputs=lambda f: f.suffix in {".xlsx", ".xls", ".zip"},
    ),
]

SQL_DATASETS = [
    SqlDataset(
        name="tpit_project_history",
        sql=(
            "SELECT t.* FROM tpit_projects t "
            "JOIN (SELECT snapshot_date, min(source_file) AS source_file FROM tpit_projects "
            "      WHERE snapshot_date IS NOT NULL GROUP BY snapshot_date) k "
            "  ON t.snapshot_date = k.snapshot_date AND t.source_file = k.source_file "
            "UNION ALL SELECT * FROM tpit_projects WHERE snapshot_date IS NULL"
        ),
        description=(
            "tpit_projects with each snapshot read from one raw file only (the current workbook repeats the "
            "archive's latest snapshot); the undated cumulative 1999-20xx workbooks are kept as published."
        ),
        indexes=(("project_id",), ("snapshot_date",), ("county_start_fips",)),
    ),
]
