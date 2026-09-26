"""TCEQ Central Registry, air permit rows (program AIRNSR) → ``tceq_air_permits`` and ``tceq_data_center_sites``.

Raw: CSV pages of 50,000 rows from the five regional Central Registry datasets on data.texas.gov, all with
the same 35 columns. Each row joins a regulated entity (site, ``ref_num_txt`` = RN), a principal (customer,
``princ_ref_num_txt`` = CN, source column ``ref_num_txt_1``) and an additional id (the air permit or
registration number, ``additional_id_text``) with its status and affiliation dates. So there is one CSV kind
and one table, one row per affiliation; the site-level view is the SQL dataset.

The registry is regenerated daily and exported in pages, so only the newest snapshot (``dt``) is used and
each page is checked against the dataset's row count recorded at download time. Exact duplicate rows (97 in
the 2026-09-26 export) are dropped.

Cleaning:
- dates → ``Date``; the placeholders ``0001-01-01``/``1800-01-01``/``1900-01-01`` (unknown) and ``3000-…``
  (open-ended) become null. Six begin dates in 2033/2086 are kept as published (data-entry errors at the
  source);
- ``county_fips`` from ``re_phys_loc_addr_county`` (all 254 counties match);
- ``re_naics_code`` is the leading code of ``indus_type_cd_name`` (the site's NAICS, often wrong per TCEQ);
- the two free-text driving-direction columns (``*_phys_loc_desc``) are dropped;
- personal data: the principal's name is nulled when the principal is a person (``princ_type`` INDIVIDUAL
  OWNER TYPE, SOLE PROPRIETORSHIP, ESTATE, TRUST, or OTHER without an organization word in the name, see
  ``_privacy.py``); on those rows the street address lines are nulled too (city, ZIP and county stay). The
  site name (``reg_ent_name``) is kept: it names the facility, and the data-center filter needs it.

Memory: the ~350 MB export is built one page at a time with repeated values as categoricals (text in
Postgres); the table is ~165 MB in memory and the process peaks at ~0.9 GB (2 threads) to ~1.2 GB.
"""

from __future__ import annotations

import logging
import re

import polars as pl

from basecast_pipelines.processing.core import Dataset, RawFile, SqlDataset, latest_by, latest_dt
from basecast_pipelines.parsers.texas._counties import county_fips_expr
from basecast_pipelines.parsers.texas._privacy import is_org_name

log = logging.getLogger(__name__)

SOURCE_ID = "tceq_air_permits"

_PAGE = re.compile(r"^([a-z0-9]{4}-[a-z0-9]{4})_(.+)_([a-z]+)_(\d{7})(?:__[0-9a-f]{8})?\.csv$", re.IGNORECASE)
_REQUIRED = {
    "ref_num_txt", "reg_ent_name", "re_phys_loc_addr_county", "ref_num_txt_1", "name_txt", "princ_name",
    "program_code", "additional_id_text", "additional_id_status", "indus_typ_cd", "affil_begin_dt",
}  # fmt: skip
_RENAME = {"ref_num_txt_1": "princ_ref_num_txt", "name_txt": "princ_type"}
_DATES = ("affil_begin_dt", "affil_end_dt", "status_dt")
_PERSON_TYPES = ("INDIVIDUAL OWNER TYPE", "SOLE PROPRIETORSHIP", "ESTATE", "TRUST")
_ADDRESS_COLUMNS = ("re_phys_loc_addr_line_1", "re_phys_loc_addr_line_2", "ai_phys_loc_addr_line_1",
                    "ai_phys_loc_addr_line_2")
_DROP = ("re_phys_loc_desc", "ai_phys_loc_desc")  # free-text driving directions: no use here, may name people
_CATEGORICAL = (
    "re_phys_loc_city", "re_phys_loc_addr_county", "re_phys_loc_addr_state", "re_phys_loc_addr_zip",
    "re_phys_loc_addr_zip_ext", "reg_ent_desc_txt", "indus_type_cd_name", "princ_type", "program_code",
    "additional_id_status", "ai_phys_loc_city", "ai_phys_loc_addr_state", "indus_typ_cd", "indus_typ_name",
    "indus_typ_cd_1", "indus_typ_name_1", "reg_ent_status_txt", "princ_status_txt", "county_fips", "re_naics_code",
    "dataset_id", "registry_region", "source_file",
)  # fmt: skip


def _page_key(f: RawFile) -> tuple[str, int]:
    """(dataset id, offset) from the manifest, or from the file name."""
    if f.meta.get("dataset_id") is not None and f.meta.get("offset") is not None:
        return str(f.meta["dataset_id"]), int(f.meta["offset"])
    m = _PAGE.match(f.name)
    if not m:
        raise ValueError(f"{f.key}: not a Central Registry page")
    return m.group(1), int(m.group(4))


def _select_pages(files: list[RawFile]) -> list[RawFile]:
    """The newest snapshot only (pages of different days never mix), one file per page."""
    return latest_by(_page_key)(latest_dt(files))


def _date(col: str) -> pl.Expr:
    d = pl.col(col).str.slice(0, 10).str.to_date("%Y-%m-%d", strict=False)
    return pl.when((d <= pl.date(1900, 1, 1)) | (d >= pl.date(2999, 1, 1))).then(None).otherwise(d).alias(col)


def _page(f: RawFile, path) -> pl.LazyFrame:
    """One page, cleaned, as a lazy frame (nothing is read until the whole table is collected)."""
    dataset_id, offset = _page_key(f)
    columns = [c.strip().lower() for c in pl.read_csv(path, n_rows=0, infer_schema=False).columns]
    missing = _REQUIRED - set(columns)
    if missing:
        raise ValueError(f"{f.key}: missing columns {sorted(missing)}")
    lf = pl.scan_csv(path, infer_schema=False, low_memory=True).rename(lambda c: c.strip().lower())
    lf = lf.drop([c for c in _DROP if c in columns]).rename({k: v for k, v in _RENAME.items() if k in columns})
    kept = [_RENAME.get(c, c) for c in columns if c not in _DROP]
    strings = [c for c in kept if c not in _DATES and c != "tceq_region_number"]
    person = pl.col("princ_type").is_in(_PERSON_TYPES) | (
        (pl.col("princ_type").is_null() | (pl.col("princ_type") == "OTHER"))
        & ~is_org_name("princ_name")
    )
    personal = [c for c in ("princ_name", "princ_legal_name", *_ADDRESS_COLUMNS) if c in kept]
    return (
        lf.with_columns([pl.col(c).str.strip_chars().replace("", None) for c in strings])
        .with_columns(
            *[pl.when(person).then(None).otherwise(pl.col(c)).alias(c) for c in personal],
            *[_date(c) for c in _DATES if c in kept],
            pl.col("tceq_region_number").str.strip_chars().cast(pl.Int32, strict=False),
            pl.col("re_phys_loc_addr_county").str.to_uppercase(),
            county_fips_expr("re_phys_loc_addr_county").alias("county_fips"),
            pl.col("indus_type_cd_name").str.extract(r"^\s*(\d{2,6})\b").alias("re_naics_code"),
            pl.lit(dataset_id).alias("dataset_id"),
            pl.lit(f.meta.get("region")).cast(pl.String).alias("registry_region"),
            pl.lit(offset, pl.Int64).alias("page_offset"),
            pl.lit(f.meta.get("row_count"), pl.Int64).alias("expected_rows"),
            pl.lit(f.key).alias("source_file"),
        )
        .with_columns([pl.col(c).cast(pl.Categorical) for c in _CATEGORICAL])
    )


def _drop_exact_duplicates(df: pl.DataFrame, subset: list[str]) -> pl.DataFrame:
    """Exact duplicates on ``subset``: a row hash finds the few candidates, an exact comparison decides, and one
    filter drops them (a full ``unique`` over ~40 text columns would double the memory peak)."""
    candidates = df.select(pl.struct(subset).hash().is_duplicated()).to_series()
    if not candidates.any():
        return df
    indexed = df.with_row_index("_row")
    suspects = indexed.filter(candidates)
    kept = suspects.unique(subset=subset, keep="first", maintain_order=True)["_row"]
    drop = suspects.filter(~pl.col("_row").is_in(kept.implode()))["_row"]
    return indexed.filter(~pl.col("_row").is_in(drop.implode())).drop("_row")


def build_air_permits(files: list[RawFile]) -> pl.DataFrame | None:
    """Every page of the snapshot, one page at a time: each is cleaned and stored with categoricals (~12 MB per
    50,000 rows instead of ~30 MB of CSV), then the pages are stacked without copying."""
    if not files:
        return None
    frames = []
    for f in files:
        with f.local_path() as path:
            frames.append(_page(f, path).collect())
    df = pl.concat(frames, how="vertical_relaxed", rechunk=False)
    del frames

    programs = set(df["program_code"].cast(pl.String).unique())
    if programs != {"AIRNSR"}:
        raise ValueError(f"tceq: unexpected program codes {sorted(programs, key=str)}")
    counts = df.group_by("dataset_id").agg(pl.len().alias("rows"), pl.col("expected_rows").max().alias("expected"))
    for dataset_id, rows, expected in counts.iter_rows():
        if expected is None:
            continue
        if rows < 0.99 * expected:
            raise ValueError(f"tceq {dataset_id}: {rows} rows, {expected} expected; a page is missing")
        if rows != expected:
            log.warning("tceq %s: %d rows, %d at download time", dataset_id, rows, expected)
    if df["county_fips"].null_count():
        unknown = df.filter(pl.col("county_fips").is_null())["re_phys_loc_addr_county"].unique().to_list()
        log.warning("tceq: counties not matched to a Texas FIPS (county_fips left null): %s", unknown[:20])
    df = df.drop("expected_rows")
    return _drop_exact_duplicates(df, [c for c in df.columns if c not in ("source_file", "page_offset")])


DATASETS = [
    Dataset(
        name="tceq_air_permits",
        target="postgres",
        mode="replace",
        description=(
            "TCEQ Central Registry air permits (program AIRNSR), one row per site × principal × permit id, "
            "newest daily export; site, county_fips, permit status and affiliation dates. Individuals' names "
            "and street addresses removed."
        ),
        build=build_air_permits,
        inputs=lambda f: f.suffix == ".csv",
        select=_select_pages,
    ),
]

# docs/decisions.md 2026-09-26: the reproducible data-center filter. Placeholder begin dates (1800-01-01) are
# null in tceq_air_permits, so the earliest *known* date is first_affil_begin_dt and has_undated_affiliation
# flags sites that also have an undated row (the decision counted 1800-01-01 as the earliest date).
DATA_CENTER_SQL = """
SELECT
    ref_num_txt,
    (array_agg(reg_ent_name ORDER BY affil_begin_dt DESC NULLS LAST))[1] AS reg_ent_name,
    min(re_phys_loc_addr_county) AS county_name,
    min(county_fips) AS county_fips,
    min(re_phys_loc_city) AS city,
    min(tceq_region_number) AS tceq_region_number,
    min(affil_begin_dt) AS first_affil_begin_dt,
    max(affil_begin_dt) AS last_affil_begin_dt,
    bool_or(affil_begin_dt IS NULL) AS has_undated_affiliation,
    count(*) AS permit_rows,
    count(DISTINCT additional_id_text) AS permit_ids,
    count(*) FILTER (WHERE additional_id_status = 'PENDING') AS pending_rows,
    count(*) FILTER (WHERE additional_id_status = 'ACTIVE') AS active_rows,
    bool_or(additional_id_status = 'PENDING') AS has_pending,
    bool_or(additional_id_status = 'ACTIVE') AS has_active,
    (SELECT json_agg(s ORDER BY s)::text FROM unnest(array_agg(DISTINCT additional_id_status)) AS s
        WHERE s IS NOT NULL) AS statuses,
    (SELECT json_agg(p ORDER BY p)::text FROM unnest(array_agg(DISTINCT princ_name)) AS p
        WHERE p IS NOT NULL) AS principals,
    bool_or(reg_ent_name ~* 'DATA ?CENTER|DATACENTER|DATA CTR') AS matched_by_name,
    bool_or(indus_typ_cd = '518210') AS matched_by_naics
FROM tceq_air_permits
WHERE reg_ent_name ~* 'DATA ?CENTER|DATACENTER|DATA CTR' OR indus_typ_cd = '518210'
GROUP BY ref_num_txt
"""

SQL_DATASETS = [
    SqlDataset(
        name="tceq_data_center_sites",
        sql=DATA_CENTER_SQL,
        description=(
            "Data-center sites in the TCEQ air permits (docs/decisions.md filter: site name matches DATA ?CENTER|"
            "DATACENTER|DATA CTR or NAICS 518210), one row per site (RN) dated by its earliest affiliation; "
            "a lower bound with no MW."
        ),
        indexes=(("county_fips",), ("ref_num_txt",)),
    ),
]
