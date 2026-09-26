"""ACS 5-year table B25032 (tenure by units in structure) → ``census_housing_county`` (Texas counties).

Raw per vintage: the table-based Summary File ``acsdt5y<year>-b25032.dat`` (pipe-delimited, every
geography; ``GEO_ID`` plus ``B25032_E<nnn>``/``B25032_M<nnn>`` estimate and margin-of-error columns) and the
``ACS<year>5YR_Table_Shells.txt`` that labels each line. Counties are ``GEO_ID`` ``0500000US48xxx``.

One row per county × vintage × table line (long format), labelled from the shell of the same vintage:
``tenure`` is the indent-1 parent (``owner_occupied``/``renter_occupied``, ``total`` for line 1) and
``units_in_structure`` the indent-2 label (``1, detached``, ``1, attached``, ``2``, ... ``Mobile home``).
Owner-occupied single-family homes are lines 3 (detached) and 4 (attached). 5-year estimates overlap:
use one ``vintage`` per analysis. Negative ACS annotation codes (e.g. ``-555555555``) would become null
with the code kept in ``estimate_annotation``/``moe_annotation``; the 2024 Texas county rows have none."""

from __future__ import annotations

import re

import polars as pl

from basecast_pipelines.processing.core import Dataset, RawFile, latest_by

SOURCE_ID = "census_acs"
TABLE = "B25032"
TEXAS_COUNTY_PREFIX = "0500000US48"

_DATA = re.compile(r"^acsdt5y(\d{4})-([a-z0-9]+)(?:__[0-9a-f]{8})?\.dat$", re.IGNORECASE)
_SHELLS = re.compile(r"^ACS(\d{4})5YR_Table_Shells(?:__[0-9a-f]{8})?\.txt$", re.IGNORECASE)
_CELL = re.compile(rf"^{TABLE}_([EM])(\d{{3}})$", re.IGNORECASE)


def _kind(f: RawFile) -> tuple[str, int] | None:
    if m := _DATA.match(f.name):
        return ("data", int(m.group(1))) if m.group(2).upper() == TABLE else None
    if m := _SHELLS.match(f.name):
        return "shells", int(m.group(1))
    return None


def _shell(f: RawFile) -> pl.DataFrame:
    """The table's lines from the shells file: line, unique id, label, indent, tenure, units in structure."""
    with f.local_path() as path:
        shells = pl.read_csv(path, separator="|", infer_schema=False, quote_char=None, encoding="utf8-lossy")
    shells = shells.rename({c: c.strip().lower().replace(" ", "_") for c in shells.columns})
    needed = {"table_id", "line", "indent", "unique_id", "label"}
    if not needed <= set(shells.columns):
        raise ValueError(f"{f.key}: table shells lack {sorted(needed - set(shells.columns))}")
    optional = lambda c: pl.col(c) if c in shells.columns else pl.lit(None, pl.String)  # noqa: E731
    rows = shells.filter(pl.col("table_id").str.strip_chars() == TABLE).select(
        pl.col("line").cast(pl.Float64).cast(pl.Int32).alias("line"),
        pl.col("unique_id").str.strip_chars(),
        pl.col("label").str.strip_chars(),
        pl.col("indent").cast(pl.Int32),
        optional("title").str.strip_chars().alias("table_title"),
        optional("universe").str.strip_chars().alias("universe"),
    ).sort("line")
    if rows.is_empty():
        raise ValueError(f"{f.key}: no {TABLE} lines in the table shells")
    tenure, labels = [], []
    parent = "total"
    for label, indent in rows.select("label", "indent").iter_rows():
        clean = label.rstrip(":").strip()
        if indent == 0:
            parent, unit = "total", None
        elif indent == 1:
            parent, unit = re.sub(r"[^a-z]+", "_", clean.lower().removesuffix(" housing units")).strip("_"), None
        else:
            unit = clean
        tenure.append(parent)
        labels.append(unit)
    return rows.with_columns(
        pl.Series("tenure", tenure, pl.String),
        pl.Series("units_in_structure", labels, pl.String),
    )


def _data(f: RawFile, vintage: int) -> pl.DataFrame:
    with f.local_path() as path:
        wide = (
            pl.scan_csv(path, separator="|", infer_schema=False)
            .filter(pl.col("GEO_ID").str.starts_with(TEXAS_COUNTY_PREFIX))
            .collect()
        )
    cells = [c for c in wide.columns if _CELL.match(c)]
    if "GEO_ID" not in wide.columns or not cells:
        raise ValueError(f"{f.key}: no GEO_ID / {TABLE} estimate columns")
    if wide.is_empty():
        raise ValueError(f"{f.key}: no Texas county rows")
    long = wide.unpivot(index="GEO_ID", on=cells, variable_name="cell", value_name="value").with_columns(
        pl.col("cell").str.extract(_CELL.pattern, 1).str.to_uppercase().alias("stat"),
        pl.col("cell").str.extract(_CELL.pattern, 2).cast(pl.Int32).alias("line"),
        pl.col("value").str.strip_chars().cast(pl.Float64),  # strict: a non-number is a format change
    )
    est = long.filter(pl.col("stat") == "E").select("GEO_ID", "line", pl.col("value").alias("estimate"))
    moe = long.filter(pl.col("stat") == "M").select("GEO_ID", "line", pl.col("value").alias("moe"))
    return est.join(moe, on=["GEO_ID", "line"], how="full", coalesce=True).with_columns(
        pl.lit(vintage, pl.Int32).alias("vintage"),
        pl.col("GEO_ID").str.slice(len("0500000US"), 5).alias("county_fips"),
    )


def _annotated(col: str) -> list[pl.Expr]:
    """Negative values are ACS annotation codes, not numbers: null the value, keep the code."""
    v = pl.col(col)
    return [
        pl.when(v < 0).then(None).otherwise(v).round(0).cast(pl.Int64).alias(col),
        pl.when(v < 0).then(v.cast(pl.Int64).cast(pl.String)).otherwise(None).alias(f"{col}_annotation"),
    ]


def build_housing(files: list[RawFile]) -> pl.DataFrame | None:
    """Every vintage that has both its data file and its table shells; one frame for all vintages."""
    by_vintage: dict[int, dict[str, RawFile]] = {}
    for f in files:
        kind = _kind(f)
        if kind:
            by_vintage.setdefault(kind[1], {})[kind[0]] = f
    frames = []
    for vintage, pair in sorted(by_vintage.items()):
        if "data" not in pair:
            continue  # shells alone carry no numbers
        if "shells" not in pair:
            raise ValueError(f"{pair['data'].key}: no table shells for vintage {vintage}")
        shell = _shell(pair["shells"])
        data = _data(pair["data"], vintage)
        unknown = set(data["line"].unique()) - set(shell["line"])
        if unknown:
            raise ValueError(f"{pair['data'].key}: lines {sorted(unknown)} missing from the table shells")
        frames.append(
            data.join(shell, on="line", how="left")
            .with_columns(*_annotated("estimate"), *_annotated("moe"), pl.lit(pair["data"].key).alias("source_file"))
            .select(
                "vintage", "county_fips", pl.lit(TABLE).alias("table_id"), "line", "unique_id", "label", "indent",
                "tenure", "units_in_structure", "estimate", "moe", "estimate_annotation", "moe_annotation",
                "table_title", "universe", "source_file",
            )
            .sort("county_fips", "line")
        )
    return pl.concat(frames) if frames else None


DATASETS = [
    Dataset(
        name="census_housing_county",
        target="postgres",
        mode="replace",
        description=(
            "ACS 5-year B25032 tenure by units in structure, Texas counties, long format (one row per county, "
            "vintage and table line) with estimate and margin of error; owner-occupied 1-unit detached/attached "
            "are lines 3 and 4."
        ),
        build=build_housing,
        inputs=lambda f: _kind(f) is not None,
        select=latest_by(_kind),
    ),
]
