"""EAGLE-I side files (ORNL / DOE CESER, figshare article 24237376, CC BY 4.0) → one small Postgres table
each, rebuilt from the newest snapshot:

- ``eaglei_mcc`` (``MCC.csv``): modeled customer count per county (the denominator for outage shares);
- ``eaglei_coverage_history`` (``coverage_history.csv``): per state and year, customers covered by EAGLE-I;
- ``eaglei_dqi`` (``DQI.csv``): data quality index per FEMA region and year.

All three are national; they are kept whole (a few thousand rows). Headers are read by name, not position.

The national yearly outage files (``eaglei_outages_<year>.csv``, 15-minute customers out per county,
1.1-1.4 GB each) are not in the raw lake yet and have no parser: a future one would stream each year,
keep Texas counties (FIPS ``48xxx``) only and write to BigQuery. Such files match no dataset here, so
they are ignored if they show up."""

from __future__ import annotations

import io
from collections.abc import Callable

import polars as pl

from basecast_pipelines.processing.core import Dataset, RawFile, latest_dt
from basecast_pipelines.processing.tabular import integer, num, snake

SOURCE_ID = "ornl_eaglei"


def _read_csv(f: RawFile) -> pl.DataFrame:
    """Every cell as text, headers in snake_case (the files carry a UTF-8 BOM)."""
    df = pl.read_csv(io.BytesIO(f.read_bytes()), infer_schema=False, encoding="utf8-lossy")
    df.columns = [snake(c.lstrip("﻿")) for c in df.columns]
    return df.filter(~pl.all_horizontal(pl.all().is_null()))


def _require(f: RawFile, df: pl.DataFrame, columns: list[str]) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"{f.key}: columns {missing} missing (has {df.columns})")


def _rest(df: pl.DataFrame, known: set[str]) -> list[pl.Expr]:
    """Columns a later version may add: numbers when every value is numeric, text otherwise."""
    out = []
    for c in df.columns:
        if c in known:
            continue
        values = df[c].drop_nulls()
        numeric = values.len() > 0 and df.select(num(c)).to_series().drop_nulls().len() == values.len()
        out.append(num(c).alias(c) if numeric else pl.col(c).str.strip_chars().alias(c))
    return out


def parse_mcc(f: RawFile) -> pl.DataFrame | None:
    df = _read_csv(f)
    _require(f, df, ["county_fips", "customers"])
    fips = pl.col("county_fips").str.strip_chars()
    out = (
        # The last row is "Grand Total", not a county.
        df.filter(fips.str.contains(r"^\d{1,5}$"))
        .select(
            fips.str.zfill(5).alias("county_fips"),
            integer("customers").alias("customers"),
            *_rest(df, {"county_fips", "customers"}),
        )
    )
    total = df.filter(fips.str.to_lowercase() == "grand total").select(integer("customers")).to_series()
    if total.len() and total[0] != out["customers"].sum():
        raise ValueError(f"{f.key}: counties sum to {out['customers'].sum()}, the Grand Total row says {total[0]}")
    return out


def parse_coverage_history(f: RawFile) -> pl.DataFrame | None:
    df = _read_csv(f)
    counts = ["total_customers", "min_covered", "max_covered"]
    shares = ["min_pct_covered", "max_pct_covered"]
    _require(f, df, ["year", "state", *counts, *shares])
    # "year" holds the first day of the year as m/d/yy ("1/1/18"); plain years are accepted too.
    text = pl.col("year").str.strip_chars()
    year = pl.coalesce(
        text.str.strptime(pl.Date, "%m/%d/%y", strict=False).dt.year(),
        text.str.strptime(pl.Date, "%m/%d/%Y", strict=False).dt.year(),
        text.str.extract(r"^(\d{4})$").cast(pl.Int32, strict=False),
    )
    out = df.select(
        year.cast(pl.Int32).alias("year"),
        pl.col("state").str.strip_chars().str.to_uppercase().alias("state"),
        *(integer(c).alias(c) for c in counts),
        *(num(c).alias(c) for c in shares),
        *_rest(df, {"year", "state", *counts, *shares}),
    )
    if out["year"].null_count():
        raise ValueError(f"{f.key}: unreadable year values {df.filter(year.is_null())['year'].to_list()[:5]}")
    return out


def parse_dqi(f: RawFile) -> pl.DataFrame | None:
    df = _read_csv(f)
    shares = ["success_rate", "percent_enabled", "spatial_precision", "cust_coverage", "dqi"]
    counts = ["max_covered", "total_customers"]
    _require(f, df, ["fema", "year", *shares, *counts])
    return df.select(
        integer("fema").cast(pl.Int32).alias("fema_region"),
        integer("year").cast(pl.Int32).alias("year"),
        *(num(c).alias(c) for c in shares),
        *(integer(c).alias(c) for c in counts),
        *_rest(df, {"fema", "year", *shares, *counts}),
    )


def _named(filename: str) -> Callable[[RawFile], bool]:
    return lambda f: f.name.lower() == filename.lower()


DATASETS = [
    Dataset(
        name="eaglei_mcc",
        target="postgres",
        mode="replace",
        description="EAGLE-I modeled customer count (MCC) per county, national (county_fips 5-char).",
        parse=parse_mcc,
        inputs=_named("MCC.csv"),
        select=latest_dt,
    ),
    Dataset(
        name="eaglei_coverage_history",
        target="postgres",
        mode="replace",
        description="EAGLE-I coverage per state and year: total customers, min/max customers covered and share.",
        parse=parse_coverage_history,
        inputs=_named("coverage_history.csv"),
        select=latest_dt,
    ),
    Dataset(
        name="eaglei_dqi",
        target="postgres",
        mode="replace",
        description="EAGLE-I data quality index (DQI) and its components per FEMA region and year.",
        parse=parse_dqi,
        inputs=_named("DQI.csv"),
        select=latest_dt,
    ),
]
