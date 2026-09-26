"""Census Population Estimates Program → ``census_population_county`` (Texas counties, long format).

Four data files, kept side by side and never mixed (``series`` + ``vintage`` on every row):

- ``co-est00int-tot.csv`` → ``intercensal_2000_2010``: ``ESTIMATESBASE2000``, ``POPESTIMATE2000..2010``,
  ``CENSUS2010POP`` (all counties; Texas kept);
- ``cc-est2020int-agesex-48.csv`` → ``intercensal_2010_2020`` (Texas only): one row per county and coded
  ``YEAR`` (layout PDF ``CC-EST2020INT-AGESEX.pdf``: 1 = 4/1/2010 estimates base, 2..11 = 7/1/2010..7/1/2019,
  12 = 4/1/2020 Census); only ``POPESTIMATE`` is kept (the age/sex columns are out of scope);
- ``co-est2020-alldata.csv`` → ``postcensal_v2020`` and ``co-est<YYYY>-alldata.csv`` → ``postcensal_v<YYYY>``:
  population plus components of change (births, deaths, natural change, international/domestic/net migration,
  residual, group quarters) and their rates.

The series name comes from the file name, so it stays stable when a newer vintage arrives (the manifest's
``postcensal_latest`` would move). Rows: ``measure`` / ``value`` with ``year`` and ``ref_date`` (April 1 for
census counts and estimates bases, July 1 for estimates; components cover the year ending June 30, the first
one starting April 1). Measures are normalized across vintages (``NATURALINC`` in V2020 = ``NATURALCHG`` in
V2025 → ``natural_change``; ``NPOPCHG_2010`` → ``npopchg``). Rates (``r_*``) are per 1,000 people. Files are
Latin-1. The layout PDFs are not parsed (they are not inputs)."""

from __future__ import annotations

import io
import re
from datetime import date

import polars as pl

from basecast_pipelines.processing.core import Dataset, RawFile, latest_by_url

SOURCE_ID = "census_pep"
TEXAS = "48"

_INTERCENSAL_00 = re.compile(r"^co-est00int-tot(?:__[0-9a-f]{8})?\.csv$", re.IGNORECASE)
_INTERCENSAL_10 = re.compile(r"^cc-est2020int-agesex-48(?:__[0-9a-f]{8})?\.csv$", re.IGNORECASE)
_POSTCENSAL = re.compile(r"^co-est(\d{4})-alldata(?:__[0-9a-f]{8})?\.csv$", re.IGNORECASE)

# cc-est2020int-agesex layout (CC-EST2020INT-AGESEX.pdf, "The key for YEAR"): code → (measure, reference date)
_INTERCENSAL_10_YEAR = {
    1: ("estimates_base", date(2010, 4, 1)),
    **{code: ("population", date(2008 + code, 7, 1)) for code in range(2, 12)},
    12: ("census_population", date(2020, 4, 1)),
}

# column prefix (upper case, before the year) → measure; order matters (longest first where prefixes overlap)
_MEASURES = (
    ("ESTIMATESBASE", "estimates_base"),
    ("GQESTIMATESBASE", "gq_estimates_base"),
    ("GQESTIMATES", "gq_estimates"),
    ("POPESTIMATE", "population"),
    ("CENSUS", "census_population"),  # CENSUS2010POP
    ("NPOPCHG", "npopchg"),
    ("BIRTHS", "births"),
    ("DEATHS", "deaths"),
    ("NATURALINC", "natural_change"),
    ("NATURALCHG", "natural_change"),
    ("INTERNATIONALMIG", "international_mig"),
    ("DOMESTICMIG", "domestic_mig"),
    ("NETMIG", "net_mig"),
    ("RESIDUAL", "residual"),
    ("RBIRTH", "r_birth"),
    ("RDEATH", "r_death"),
    ("RNATURALINC", "r_natural_change"),
    ("RNATURALCHG", "r_natural_change"),
    ("RINTERNATIONALMIG", "r_international_mig"),
    ("RDOMESTICMIG", "r_domestic_mig"),
    ("RNETMIG", "r_net_mig"),
)
_MEASURE_BY_PREFIX = dict(_MEASURES)
_YEAR_COLUMN = re.compile(r"^([A-Z]+?)_?(\d{4})(POP)?$")
_APRIL_MEASURES = {"estimates_base", "gq_estimates_base", "census_population"}
_ID_COLUMNS = {"SUMLEV", "REGION", "DIVISION", "STATE", "COUNTY", "STNAME", "CTYNAME", "YEAR"}


def _series(f: RawFile) -> tuple[str, str] | None:
    """(series, vintage) from the file name; the manifest's vintage label wins when present."""
    vintage = f.meta.get("vintage")
    if _INTERCENSAL_00.match(f.name):
        return "intercensal_2000_2010", vintage or "2000-2010 intercensal"
    if _INTERCENSAL_10.match(f.name):
        return "intercensal_2010_2020", vintage or "2010-2020 intercensal"
    if m := _POSTCENSAL.match(f.name):
        return f"postcensal_v{m.group(1)}", f"Vintage {m.group(1)}"
    return None


def _read(f: RawFile) -> pl.DataFrame:
    df = pl.read_csv(io.BytesIO(f.read_bytes()), infer_schema=False, encoding="latin1")
    df = df.rename({c: c.strip().upper() for c in df.columns})
    missing = {"SUMLEV", "STATE", "COUNTY", "CTYNAME"} - set(df.columns)
    if missing:
        raise ValueError(f"{f.key}: no {sorted(missing)} columns")
    return df.filter(
        (pl.col("STATE").str.strip_chars().str.zfill(2) == TEXAS)
        & (pl.col("SUMLEV").str.strip_chars().cast(pl.Int32) == 50)
    ).with_columns(
        (pl.lit(TEXAS) + pl.col("COUNTY").str.strip_chars().str.zfill(3)).alias("county_fips"),
        pl.col("CTYNAME").str.strip_chars().alias("county_name"),
    )


def _wide_measures(f: RawFile, df: pl.DataFrame) -> pl.DataFrame:
    """Files with one column per measure and year (``POPESTIMATE2021``, ``NPOPCHG_2010``, ``CENSUS2010POP``)."""
    columns = {}
    for c in df.columns:
        if c in _ID_COLUMNS or c in ("county_fips", "county_name"):
            continue
        m = _YEAR_COLUMN.match(c)
        if not m or m.group(1) not in _MEASURE_BY_PREFIX:
            raise ValueError(f"{f.key}: unknown column {c!r}")
        columns[c] = (_MEASURE_BY_PREFIX[m.group(1)], int(m.group(2)))
    mapping = pl.DataFrame(
        [(c, measure, year) for c, (measure, year) in columns.items()],
        schema={"column": pl.String, "measure": pl.String, "year": pl.Int32},
        orient="row",
    )
    return (
        df.unpivot(index=["county_fips", "county_name"], on=list(columns), variable_name="column", value_name="value")
        .join(mapping, on="column", how="left")
        .with_columns(
            pl.when(pl.col("measure").is_in(list(_APRIL_MEASURES)))
            .then(pl.date(pl.col("year"), 4, 1))
            .otherwise(pl.date(pl.col("year"), 7, 1))
            .alias("ref_date")
        )
    )


def _coded_years(f: RawFile, df: pl.DataFrame) -> pl.DataFrame:
    """cc-est2020int-agesex: one row per county and coded YEAR; keep POPESTIMATE."""
    if not {"YEAR", "POPESTIMATE"} <= set(df.columns):
        raise ValueError(f"{f.key}: no YEAR / POPESTIMATE columns")
    codes = df["YEAR"].str.strip_chars().cast(pl.Int32)
    unknown = set(codes.unique()) - set(_INTERCENSAL_10_YEAR)
    if unknown:
        raise ValueError(f"{f.key}: YEAR codes {sorted(unknown)} not in the layout")
    mapping = pl.DataFrame(
        [(code, measure, ref.year, ref) for code, (measure, ref) in _INTERCENSAL_10_YEAR.items()],
        schema={"year_code": pl.Int32, "measure": pl.String, "year": pl.Int32, "ref_date": pl.Date},
        orient="row",
    )
    return (
        df.select("county_fips", "county_name", codes.alias("year_code"), pl.col("POPESTIMATE").alias("value"))
        .join(mapping, on="year_code", how="left")
        .drop("year_code")
    )


def parse_population(f: RawFile) -> pl.DataFrame | None:
    series = _series(f)
    if series is None:
        return None  # layout PDFs and anything else in the folder
    name, vintage = series
    df = _read(f)
    if df.is_empty():
        raise ValueError(f"{f.key}: no Texas county rows")
    long = _coded_years(f, df) if name == "intercensal_2010_2020" else _wide_measures(f, df)
    out = long.select(
        pl.lit(name).alias("series"),
        pl.lit(vintage).alias("vintage"),
        "county_fips",
        "county_name",
        "year",
        "ref_date",
        "measure",
        pl.col("value").str.strip_chars().cast(pl.Float64),  # strict: text here means the layout changed
    ).filter(pl.col("value").is_not_null())
    dupes = out.group_by("county_fips", "measure", "ref_date").len().filter(pl.col("len") > 1)
    if dupes.height:
        raise ValueError(f"{f.key}: {dupes.height} duplicated county/measure/date cells")
    return out.sort("county_fips", "measure", "ref_date")


DATASETS = [
    Dataset(
        name="census_population_county",
        target="postgres",
        mode="by_file",
        description=(
            "Census PEP county population for Texas, four series side by side (intercensal 2000-2010, "
            "intercensal 2010-2020, Vintage 2020, latest postcensal), long format: population, census/base "
            "counts and components of change (births, deaths, migration, rates)."
        ),
        parse=parse_population,
        inputs=lambda f: _series(f) is not None,
        select=latest_by_url,
    ),
]
