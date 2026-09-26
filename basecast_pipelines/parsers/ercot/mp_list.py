"""List of Market Participants in the ERCOT Region (NP12-215-ER) → ``ercot_market_participants``.

The zip holds one workbook with a sheet per registration type (CRRAH, IMRE, LSE, QSE, RE, TDSP), each with
a title, sometimes a note, and a header row found by what it says. Only the newest snapshot is kept (ERCOT
lists just the latest file; each run keeps one in raw).

Personal data is dropped at parse: the authorized and backup authorized representatives' names and emails
(``AR ...`` / ``BAR ...`` columns). Kept: ``name`` (as published, with the "(TDSP)" style suffix),
``entity_name`` (without it), ``short_name``, ``duns_number`` (text, leading zeros kept),
``market_participant_type``, ``sfa_effective_date`` (Standard Form Agreement), ``sheet`` and
``snapshot_date`` (the file's publish date)."""

from __future__ import annotations

import logging
import re
from datetime import date

import polars as pl

from basecast_pipelines.parsers.eia._common import clean_strings, zip_member
from basecast_pipelines.processing.core import Dataset, RawFile, latest_dt
from basecast_pipelines.processing.tabular import excel_date, find_header_row, read_grid, sheet_names, with_header

log = logging.getLogger(__name__)

SOURCE_ID = "ercot_mp_list"

_PERSONAL = re.compile(r"^b?ar_|email|phone|first_name|last_name")
_KEEP = ("name", "short_name", "duns_number", "market_participant_type", "sfa_effective_date")
_STAMP = re.compile(r"\.(\d{8})\.\d+\.")


def _snapshot_date(f: RawFile) -> date:
    published = f.meta.get("publish_date")
    if published:
        return date.fromisoformat(published[:10])
    match = _STAMP.search(f.name)
    return date(int(match.group(1)[:4]), int(match.group(1)[4:6]), int(match.group(1)[6:])) if match else f.dt


def parse_participants(f: RawFile) -> pl.DataFrame:
    data = f.read_bytes()
    if f.suffix == ".zip":
        found = zip_member(data, r"\.xlsx?$")
        if found is None:
            raise ValueError(f"{f.key}: no workbook in the zip")
        data = found[1]
    frames = []
    for sheet in sheet_names(data):
        grid = read_grid(data, sheet)
        header = find_header_row(grid, [r"^name$", r"^short name$", r"market participant type"])
        if header is None:
            raise ValueError(f"{f.key}: no participant header in sheet {sheet!r}")
        body = clean_strings(with_header(grid, header))
        body = body.drop([c for c in body.columns if _PERSONAL.search(c)])
        missing = set(_KEEP) - set(body.columns)
        if missing:
            raise ValueError(f"{f.key}: sheet {sheet!r} lacks {sorted(missing)}")
        extra = set(body.columns) - set(_KEEP)
        if extra:  # only reviewed columns are kept: a new one could carry personal data
            log.warning("%s: sheet %r has new columns %s, not kept", f.key, sheet, sorted(extra))
        frames.append(
            # the last row of each sheet is a footer with the report date in the name column
            body.filter(pl.col("name").is_not_null() & pl.col("market_participant_type").is_not_null()).select(
                pl.lit(sheet.strip()).alias("sheet"),
                pl.col("name"),
                pl.col("name").str.replace(r"\s*\([A-Z]+\)$", "").alias("entity_name"),
                "short_name",
                "duns_number",
                "market_participant_type",
                excel_date("sfa_effective_date").alias("sfa_effective_date"),
            )
        )
    return pl.concat(frames, how="vertical").with_columns(pl.lit(_snapshot_date(f)).alias("snapshot_date"))


DATASETS = [
    Dataset(
        name="ercot_market_participants",
        target="postgres",
        mode="replace",
        description="ERCOT registered market participants (NP12-215-ER), newest snapshot: one row per "
        "registration (CRRAH, IMRE, LSE, QSE, RE, TDSP) with name, short name, DUNS, type and SFA effective "
        "date; representatives' names and emails dropped.",
        parse=parse_participants,
        inputs=lambda f: f.suffix in {".zip", ".xlsx", ".xls"},
        select=latest_dt,
    ),
]
