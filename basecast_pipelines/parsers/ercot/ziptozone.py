"""ZIP code → ERCOT weather zone, from the ``ZipToZone`` sheet of Load Profiling Guide Appendix D.

The newest snapshot wins: ERCOT republishes the whole appendix, so the table is rebuilt from it."""

from __future__ import annotations

import polars as pl

from basecast_pipelines.processing.core import Dataset, RawFile, latest_dt
from basecast_pipelines.processing.tabular import find_header_row, read_grid, sheet_names, with_header

SOURCE_ID = "ercot_ziptozone"


def parse_zip_zone(f: RawFile) -> pl.DataFrame | None:
    with f.local_path() as path:
        sheet = next((s for s in sheet_names(path) if s.strip().lower() == "ziptozone"), None)
        if sheet is None:
            return None
        grid = read_grid(path, sheet)
    header = find_header_row(grid, [r"zip code", r"weather zone code"])
    if header is None:
        raise ValueError(f"{f.key}: no ZIP / weather zone header in sheet {sheet!r}")
    body = with_header(grid, header)
    zip_col = next(c for c in body.columns if "zip" in c)
    code_col = next(c for c in body.columns if "zone_code" in c)
    name_col = next((c for c in body.columns if "zone_name" in c), None)
    return (
        body.select(
            pl.col(zip_col).str.strip_chars().str.zfill(5).alias("zip_code"),
            pl.col(code_col).str.strip_chars().str.to_uppercase().alias("weather_zone"),
            (pl.col(name_col).str.strip_chars() if name_col else pl.lit(None, pl.String)).alias("weather_zone_name"),
        )
        .filter(pl.col("zip_code").str.contains(r"^\d{5}$") & pl.col("weather_zone").is_not_null())
        .unique("zip_code", keep="first", maintain_order=True)
    )


DATASETS = [
    Dataset(
        name="ercot_zip_weather_zone",
        target="postgres",
        mode="replace",
        description="ZIP code → ERCOT weather zone (Load Profiling Guide Appendix D, ZipToZone sheet).",
        parse=parse_zip_zone,
        inputs=lambda f: f.suffix in {".xlsx", ".xls"},
        select=latest_dt,
    ),
]
