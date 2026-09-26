"""PUCT market directories (``/bulkcopy/<kind>.csv``, regenerated daily) → one table per kind,
``puct_directory_<kind>``, rebuilt from the newest snapshot.

Organization kinds (``coop``, ``muni``, ``iou``, ``tdu``, ``ra``, ``rep``, ``agg``, ``pgc``, ``sg``): the
CSV has one row per contact and address. Personal data is dropped at parse (``docs/decisions.md``): the
contact's name and job title (``Title1``/``Title2``/``textbox17``; ``Title1`` sometimes holds a person's
name), phones, fax and emails, and every row whose ``MailGroupDesc`` is not an organization address
(``Contact``, ``Regulatory Rep``, ``Authorized Rep``...). What is left is one row per organization and
address: ``primary_id_no`` (the CCN number for co-ops and munis), ``track_no``, names, ``org_type``,
``service_type``, the address, website, ``address_type`` (Company / Physical, Mailing / PO Box) and
the primary / complaint address flags (true when any contact row at that address had them).

Facility kinds (``pgc_facility``, ``sg_facility``): one row per generation facility with its county
(``county_fips``), host utility service area, control area and capacity (MW).

Sole proprietors (``CoTypeDesc`` = Sole Proprietor in pgc.csv / sg.csv) are registered under a person's
name: their names, street address, zip and website are blanked (``redacted`` = true), in the
organization tables and in the facility tables (whose ``CompanyName``/``FacilityName`` repeat the
person's name), which is why the facility tables are built together with their organization file.

Not verified: the unlabeled ``textbox`` columns. Where they hold True/False they are read as
``primary_address`` (organization files; it sits where coop.csv has ``PrimaryAddress``) or
``self_generator`` (facility files; true on every sg_facility row); otherwise they hold job titles and
are dropped."""

from __future__ import annotations

import io
import logging
import re

import polars as pl

from basecast_pipelines.parsers.eia._common import clean_strings, tx_county_fips, type_columns
from basecast_pipelines.processing.core import Dataset, RawFile, latest_dt

log = logging.getLogger(__name__)

SOURCE_ID = "puct_directories"

ORG_KINDS = ("agg", "coop", "iou", "muni", "pgc", "ra", "rep", "sg", "tdu")
FACILITY_KINDS = {"pgc_facility": "pgc", "sg_facility": "sg"}

_COLUMNS = {
    "primaryidno": "primary_id_no",
    "trackno": "track_no",
    "companyname": "company_name",
    "dba": "dba",
    "cotypedesc": "org_type",
    "coservicedesc": "service_type",
    "address1": "address1",
    "address2": "address2",
    "city": "city",
    "state": "state",
    "zip": "zip",
    "website": "website",
    "mailgroupdesc": "address_type",
    "primaryaddress": "primary_address",
    "complaintaddress": "complaint_address",
    "selfgen": "self_generator",
    "primaryidtype": "primary_id_type",
    "facilityname": "facility_name",
    "generationtype": "generation_type",
    "county": "county",
    "serviceareaco": "service_area_utility",
    "controlarea": "control_area",
    "generatingcapacity": "generating_capacity_mw",
    "recdate": "rec_date",
    "nonrenfuels": "non_renewable_fuels",
}
_PERSONAL = {"title1", "title2", "phone", "altphone", "alt", "fax", "email"}
_ORG_ADDRESS = r"(?i)^(company|mailing)\b"  # organization addresses; Contact / ... Rep rows are people
_BOOLEAN = {"true", "false"}
_REDACT_ORG = ("company_name", "dba", "address1", "address2", "zip", "website")
_REDACT_FACILITY = ("company_name", "facility_name")


def _kind(f: RawFile) -> str:
    return f.name.removesuffix(".csv").lower()


def _read_csv(f: RawFile) -> pl.DataFrame:
    data = f.read_bytes()
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = data.decode("cp1252")
    return pl.read_csv(io.StringIO(text), infer_schema_length=0, truncate_ragged_lines=True)


def _columns(raw: pl.DataFrame, f: RawFile, *, facility: bool) -> pl.DataFrame:
    """Known columns renamed, personal ones dropped, ``textbox`` columns resolved by what they hold."""
    out = {}
    for col in raw.columns:
        key = re.sub(r"[^a-z0-9]", "", col.lower())
        if key in _COLUMNS:
            out[col] = _COLUMNS[key]
        elif key.startswith("textbox"):
            values = set(raw[col].drop_nulls().str.strip_chars().str.to_lowercase().unique()) - {""}
            if values and values <= _BOOLEAN:
                out[col] = "self_generator" if facility else "primary_address"
        elif key not in _PERSONAL:
            log.warning("%s: unknown column %r dropped", f.key, col)
    if len(set(out.values())) != len(out):
        raise ValueError(f"{f.key}: two columns map to the same name: {out}")
    df = clean_strings(raw.select(pl.col(c).alias(n) for c, n in out.items()))
    missing = {"track_no", "company_name"} - set(df.columns)
    if missing:
        raise ValueError(f"{f.key}: missing columns {sorted(missing)} in {raw.columns}")
    return df.filter(pl.col("track_no").is_not_null() | pl.col("company_name").is_not_null())


def _redact(df: pl.DataFrame, sole: pl.Expr, columns: tuple[str, ...]) -> pl.DataFrame:
    return df.with_columns(
        *(pl.when(sole).then(None).otherwise(pl.col(c)).alias(c) for c in columns if c in df.columns),
        sole.alias("redacted"),
    )


def parse_org(f: RawFile) -> pl.DataFrame:
    df = _columns(_read_csv(f), f, facility=False)
    if "address_type" not in df.columns:
        raise ValueError(f"{f.key}: no MailGroupDesc column")
    df = df.filter(pl.col("address_type").str.contains(_ORG_ADDRESS))
    df = type_columns(df, flags=["primary_address", "complaint_address", "self_generator"])
    sole = pl.lit(False)
    if "org_type" in df.columns:
        sole = pl.col("org_type").str.to_lowercase().str.contains("sole proprietor").fill_null(False)
    df = _redact(df, sole, _REDACT_ORG)
    flags = [c for c in ("primary_address", "complaint_address") if c in df.columns]
    keys = [c for c in df.columns if c not in flags]
    return (
        df.group_by(keys, maintain_order=True).agg(pl.col(c).any() for c in flags)
        if flags else df.unique(maintain_order=True)
    )


def _sole_proprietors(f: RawFile) -> set[str]:
    raw = _columns(_read_csv(f), f, facility=False)
    if "org_type" not in raw.columns:
        raise ValueError(f"{f.key}: no CoTypeDesc column to find sole proprietors")
    sole = raw.filter(pl.col("org_type").str.to_lowercase().str.contains("sole proprietor"))
    return set(sole["track_no"].drop_nulls())


def _facility_builder(kind: str, parent: str):
    def build(files: list[RawFile]) -> pl.DataFrame | None:
        by_kind = {_kind(f): f for f in files}
        if kind not in by_kind:
            return None
        if parent not in by_kind:
            raise ValueError(f"{kind}: no {parent}.csv in the snapshot (needed to redact sole proprietors)")
        f = by_kind[kind]
        sole_tracks = _sole_proprietors(by_kind[parent])
        df = _columns(_read_csv(f), f, facility=True)
        df = type_columns(df, floats=["generating_capacity_mw"], flags=["self_generator"])
        if "rec_date" in df.columns:
            df = df.with_columns(pl.col("rec_date").str.strptime(pl.Date, "%m/%d/%Y", strict=False))
        if "county" in df.columns:
            df = df.with_columns(tx_county_fips("county").alias("county_fips"))
        df = _redact(df, pl.col("track_no").is_in(sorted(sole_tracks)), _REDACT_FACILITY)
        return df.with_columns(pl.lit(f.key).alias("source_file"))

    return build


DATASETS = [
    *(
        Dataset(
            name=f"puct_directory_{kind}",
            target="postgres",
            mode="replace",
            description=f"PUCT {kind} directory (bulkcopy/{kind}.csv), newest snapshot: one row per organization "
            "and address; contact names, titles, phones and emails dropped.",
            parse=parse_org,
            inputs=lambda f, kind=kind: f.suffix == ".csv" and _kind(f) == kind,
            select=latest_dt,
        )
        for kind in ORG_KINDS
    ),
    *(
        Dataset(
            name=f"puct_directory_{kind}",
            target="postgres",
            mode="replace",
            description=f"PUCT {kind} directory (bulkcopy/{kind}.csv), newest snapshot: generation facilities with "
            "county_fips, host service area, control area and capacity (MW); sole proprietors' names blanked.",
            build=_facility_builder(kind, parent),
            inputs=lambda f, kind=kind, parent=parent: f.suffix == ".csv" and _kind(f) in {kind, parent},
            select=latest_dt,
        )
        for kind, parent in FACILITY_KINDS.items()
    ),
]
