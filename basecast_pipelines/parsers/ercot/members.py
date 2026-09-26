"""ERCOT corporate members by market segment, one workbook per membership year → ``ercot_members``.

Each workbook has a sheet per segment (Cooperatives, Municipals, Investor-Owned Utilities, Consumers...)
titled "<year> Members - ... Segment", with the members listed under membership-type columns
(Corporate, Associate; Adjunct for non-segment members) and, in Consumers, a Subsegment column whose
label only appears on the first row of each block. From 2024 the Adjunct sheet has "Member" and "Assigned
Segment for PRS Voting" columns instead. One row per member, segment and membership type; placeholder
cells ("None") are skipped, and so are stray cells above the header (2023 Consumers repeats "City of
Mesquite", listed under Large Commercial, in its title row). ``subsegment`` keeps the label and
``subsegment_code`` aligns the 2026 renaming ("Small Consumer" for "Small Commercial"...): residential,
small, large, industrial.

The files list organizations (and a few positions such as "TAC Residential Consumer"), not
representatives: no personal names were found in the 2013-2026 files, and no contact columns exist.
Membership is voluntary, so this is a subset of ERCOT's market participants."""

from __future__ import annotations

import logging
import re

import polars as pl

from basecast_pipelines.processing.core import Dataset, RawFile, latest_by_url
from basecast_pipelines.processing.tabular import clean_label, read_grid, sheet_names, snake

log = logging.getLogger(__name__)

SOURCE_ID = "ercot_members"

_SEGMENTS = (
    (r"adjunct|non-segment", "adjunct"),
    (r"consumer", "consumer"),
    (r"cooperative", "cooperative"),
    (r"independent generator", "independent_generator"),
    (r"power marketer", "independent_power_marketer"),
    (r"retail electric provider", "independent_retail_electric_provider"),
    (r"investor", "investor_owned_utility"),
    (r"municipal", "municipal"),
)
_TYPE = re.compile(r"^(corporate|associate|adjunct)$", re.IGNORECASE)
_MEMBER = re.compile(r"^members?$", re.IGNORECASE)
_SUBSEGMENT = re.compile(r"^sub\s*seg", re.IGNORECASE)
_ASSIGNED = re.compile(r"assigned segment", re.IGNORECASE)
_TITLE_YEAR = re.compile(r"\b(20\d\d)\s+Members\b", re.IGNORECASE)
_PLACEHOLDER = {"none", "n/a", "-"}


def _segment(sheet: str) -> str:
    name = sheet.strip().lower()
    for pattern, code in _SEGMENTS:
        if re.search(pattern, name):
            return code
    log.warning("unknown ERCOT membership sheet %r", sheet)
    return snake(sheet)


def _year(f: RawFile, titles: list[str]) -> int:
    for title in titles:
        match = _TITLE_YEAR.search(title)
        if match:
            return int(match.group(1))
    if f.meta.get("page_year"):
        return int(f.meta["page_year"])
    match = re.search(r"(20\d\d)", f.name)
    if not match:
        raise ValueError(f"{f.key}: no membership year in titles, page or file name")
    return int(match.group(1))


def _header(grid: pl.DataFrame) -> int | None:
    for i, row in enumerate(grid.head(15).iter_rows()):
        if any(_TYPE.match(clean_label(c)) or _MEMBER.match(clean_label(c)) for c in row if c is not None):
            return i
    return None


def _member(value: object) -> str | None:
    text = clean_label(value)
    return None if not text or text.lower() in _PLACEHOLDER else text


def _sheet_rows(grid: pl.DataFrame, segment: str) -> tuple[list[dict], str]:
    header = _header(grid)
    if header is None:
        raise ValueError("no membership-type header")
    labels = [clean_label(c) for c in grid.row(header)]
    types = {i: ("adjunct" if _MEMBER.match(lab) else lab.lower()) for i, lab in enumerate(labels)
             if _TYPE.match(lab) or _MEMBER.match(lab)}  # fmt: skip
    sub_col = next((i for i, lab in enumerate(labels) if _SUBSEGMENT.match(lab)), None)
    assigned_col = next((i for i, lab in enumerate(labels) if _ASSIGNED.search(lab)), None)
    title = clean_label(grid.row(0)[0]) if grid.height else ""
    rows = []
    subsegment = None
    for row in grid.slice(header + 1).iter_rows():
        if sub_col is not None and _member(row[sub_col]):
            subsegment = clean_label(row[sub_col])
        for i, kind in types.items():
            name = _member(row[i])
            if name:
                assigned = _member(row[assigned_col]) if assigned_col is not None else None
                rows.append({"subsegment": subsegment if sub_col is not None else None, "membership_type": kind,
                             "member_name": name, "assigned_segment": assigned})  # fmt: skip
    return rows, title


def parse_members(f: RawFile) -> pl.DataFrame:
    rows, titles = [], []
    with f.local_path() as path:
        for sheet in sheet_names(path):
            grid = read_grid(path, sheet)
            segment = _segment(sheet)
            try:
                sheet_rows, title = _sheet_rows(grid, segment)
            except ValueError as exc:
                raise ValueError(f"{f.key}: sheet {sheet!r}: {exc}") from None
            titles.append(title)
            rows += [{"segment": segment, "segment_sheet": sheet.strip(), **r} for r in sheet_rows]
    schema = {"segment": pl.String, "segment_sheet": pl.String, "subsegment": pl.String,
              "membership_type": pl.String, "member_name": pl.String, "assigned_segment": pl.String}  # fmt: skip
    df = pl.DataFrame(rows, schema=schema)
    sub = pl.col("subsegment").str.to_lowercase()
    code = (
        pl.when(sub.str.contains("residential")).then(pl.lit("residential"))
        .when(sub.str.contains("small")).then(pl.lit("small"))
        .when(sub.str.contains("large")).then(pl.lit("large"))
        .when(sub.str.contains("industrial")).then(pl.lit("industrial"))
        .otherwise(sub)
    )
    return df.select(
        pl.lit(_year(f, titles), pl.Int64).alias("year"), "segment", "segment_sheet", "subsegment",
        code.alias("subsegment_code"), "membership_type", "member_name", "assigned_segment",
    ).unique(maintain_order=True)


DATASETS = [
    Dataset(
        name="ercot_members",
        target="postgres",
        mode="by_file",
        description="ERCOT corporate members by year and market segment (cooperative, municipal, investor-owned "
        "utility, consumer...), with subsegment (consumers) and membership type (corporate, associate, "
        "adjunct); organizations only.",
        parse=parse_members,
        inputs=lambda f: f.suffix in {".xlsx", ".xls"},
        select=latest_by_url,
    ),
]
