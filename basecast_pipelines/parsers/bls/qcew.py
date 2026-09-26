"""BLS QCEW annual industry files (``qcew_<year>_annual_<naics>.csv``, national) → ``bls_qcew_county`` (Texas).

One row per Texas area × ownership × industry × year. Areas: every county (``48001``..``48507``, agglvl 78),
``48999`` (establishments whose county is unknown, often larger than most counties) and the statewide
``48000`` row (agglvl 58); ``area_type`` tells them apart and ``county_fips`` is null for the last two.
The files hold only private ownership (``own_code`` 5) for NAICS 518210 today; the code is kept as is.

Disclosure: rows with ``disclosure_code = 'N'`` are suppressed; the file writes 0 for their employment,
wages, contributions and pay. Those become null (``disclosed = false``); establishment counts are still
published and kept. The same rule applies to the location quotients (``lq_disclosure_code``) and the
over-the-year changes (``oty_disclosure_code``). Headers are read by name."""

from __future__ import annotations

import io
import re

import polars as pl

from basecast_pipelines.processing.core import Dataset, RawFile, latest_by_url

SOURCE_ID = "bls_qcew"
TEXAS = "48"

_FILE = re.compile(r"^qcew_(\d{4})_annual_(\d+)(?:__[0-9a-f]{8})?\.csv$", re.IGNORECASE)
_RENAME = {
    "annual_avg_estabs": "establishments",
    "annual_avg_emplvl": "employment",
    "total_annual_wages": "total_wages",
    "taxable_annual_wages": "taxable_wages",
    "annual_contributions": "contributions",
    "annual_avg_wkly_wage": "avg_weekly_wage",
}
_KEYS = ("area_fips", "own_code", "industry_code", "agglvl_code", "size_code", "year", "qtr", "disclosure_code")
_ESTABS = {"annual_avg_estabs", "lq_annual_avg_estabs", "oty_annual_avg_estabs_chg", "oty_annual_avg_estabs_pct_chg"}


def _suppressed(prefix: str, columns: list[str]) -> list[str]:
    """Measures of one block (main, ``lq_``, ``oty_``) that the file zeroes when the block is not disclosed."""
    block = [c for c in columns if c not in _KEYS and not c.endswith("disclosure_code")]
    if prefix:
        block = [c for c in block if c.startswith(prefix)]
    else:
        block = [c for c in block if not c.startswith(("lq_", "oty_"))]
    return [c for c in block if c not in _ESTABS]


def parse_qcew(f: RawFile) -> pl.DataFrame | None:
    df = pl.read_csv(io.BytesIO(f.read_bytes()), infer_schema=False)
    df = df.rename({c: c.strip().lower() for c in df.columns})
    missing = (set(_KEYS) | set(_RENAME) | {"avg_annual_pay"}) - set(df.columns)
    if missing:
        raise ValueError(f"{f.key}: missing columns {sorted(missing)}")
    df = df.filter(pl.col("area_fips").str.strip_chars().str.starts_with(TEXAS))
    if df.is_empty():
        raise ValueError(f"{f.key}: no Texas rows")
    if set(df["qtr"].unique()) != {"A"}:
        raise ValueError(f"{f.key}: expected annual rows (qtr = A), got {df['qtr'].unique().to_list()}")

    measures = [c for c in df.columns if c not in _KEYS and not c.endswith("disclosure_code")]
    integers = [c for c in measures if not c.endswith("pct_chg") and not c.startswith("lq_")]
    typed = df.with_columns(
        *[pl.col(c).str.strip_chars().cast(pl.Int64) for c in integers],
        *[pl.col(c).str.strip_chars().cast(pl.Float64) for c in measures if c not in integers],
        *[pl.col(c).str.strip_chars().replace("", None) for c in df.columns if c.endswith("disclosure_code")],
    )
    for prefix in ("", "lq_", "oty_"):
        flag = pl.col(f"{prefix}disclosure_code") == "N"
        typed = typed.with_columns(
            [pl.when(flag).then(None).otherwise(pl.col(c)).alias(c) for c in _suppressed(prefix, typed.columns)]
        )

    area = pl.col("area_fips").str.strip_chars()
    return typed.select(
        pl.col("year").cast(pl.Int32),
        area.alias("area_fips"),
        pl.when(area.str.contains(r"^48\d{3}$") & ~area.is_in(["48000", "48999"])).then(area).otherwise(None)
        .alias("county_fips"),
        pl.when(area == "48000").then(pl.lit("state")).when(area == "48999").then(pl.lit("county_unknown"))
        .otherwise(pl.lit("county")).alias("area_type"),
        pl.col("own_code").cast(pl.Int32),
        pl.col("industry_code").str.strip_chars(),
        pl.col("agglvl_code").cast(pl.Int32),
        pl.col("size_code").cast(pl.Int32),
        pl.col("disclosure_code"),
        pl.col("disclosure_code").is_null().alias("disclosed"),
        *[pl.col(c).alias(_RENAME.get(c, c)) for c in measures],
        pl.col("lq_disclosure_code"),
        pl.col("oty_disclosure_code"),
    ).sort("area_fips", "own_code", "industry_code")


DATASETS = [
    Dataset(
        name="bls_qcew_county",
        target="postgres",
        mode="by_file",
        description=(
            "BLS QCEW annual averages for Texas counties (plus statewide 48000 and unknown-county 48999) by "
            "industry (NAICS 518210 data processing and hosting): establishments, employment, wages, pay, "
            "location quotients and over-the-year changes; suppressed cells are null with disclosed = false."
        ),
        parse=parse_qcew,
        inputs=lambda f: bool(_FILE.match(f.name)),
        select=latest_by_url,
    ),
]
