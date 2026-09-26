"""PUCT directory fixtures: coop.csv, pgc.csv and pgc_facility.csv trimmed to a few organizations.
Contact names, titles, phones and emails in the fixtures were replaced with placeholders, and the sole
proprietor is a fictitious "JANE Q SAMPLE" at a fictitious address (simulated, not the real record)."""

import polars as pl

from basecast_pipelines.parsers.puct.directories import DATASETS, parse_org

BY_NAME = {d.name: d for d in DATASETS}
PERSONAL = {"title1", "title2", "textbox17", "phone", "altphone", "fax", "email"}


def test_coop_keeps_organization_addresses_only(raw_file):
    df = parse_org(raw_file("puct_directories", "puct_directories/coop.csv"))
    assert not PERSONAL & set(df.columns)
    text = " ".join(str(v) for row in df.iter_rows() for v in row)
    assert "PERSON NAME" not in text and "example.test" not in text and "555" not in text
    assert set(df["address_type"]) == {"Company / Physical", "Mailing / PO Box"}
    assert df["primary_id_no"].to_list().count("30005") == 2  # physical + mailing, contact row dropped
    bandera = df.filter((pl.col("primary_id_no") == "30005") & (pl.col("address_type") == "Mailing / PO Box"))
    assert bandera["primary_address"].to_list() == [True]
    assert df.schema["complaint_address"] == pl.Boolean
    assert df["redacted"].sum() == 0
    assert df["track_no"].null_count() == 0  # the trailing blank row is gone


def test_pgc_redacts_sole_proprietors(raw_file):
    df = parse_org(raw_file("puct_directories", "puct_directories/pgc.csv"))
    sole = df.filter(pl.col("track_no") == "PG090058").row(0, named=True)
    assert sole["redacted"] is True
    assert sole["company_name"] is None and sole["address1"] is None and sole["zip"] is None
    assert sole["org_type"] == "Sole Proprietor"
    # textbox17 holds True/False in pgc.csv: the primary-address flag
    assert df.schema["primary_address"] == pl.Boolean
    # two contact rows at one address collapse into one organization address
    assert df.filter(pl.col("track_no") == "PG260082").height == 1
    assert df.filter(pl.col("track_no") == "PG260082")["company_name"].item() == "ARROYO PROJECTCO 3 LLC"


def test_facilities_are_built_with_their_organization_file(raw_file):
    ds = BY_NAME["puct_directory_pgc_facility"]
    files = [
        raw_file("puct_directories", "puct_directories/pgc_facility.csv"),
        raw_file("puct_directories", "puct_directories/pgc.csv"),
        raw_file("puct_directories", "puct_directories/coop.csv"),
    ]
    chosen = ds.files(files)
    assert sorted(f.name for f in chosen) == ["pgc.csv", "pgc_facility.csv"]
    df = ds.build(chosen)
    facility_key = next(f.key for f in chosen if f.name == "pgc_facility.csv")
    assert df["source_file"].unique().to_list() == [facility_key]
    sole = df.filter(pl.col("track_no") == "PG090058").row(0, named=True)
    assert sole["redacted"] is True and sole["company_name"] is None and sole["facility_name"] is None
    assert sole["county_fips"] == "48085"  # Collin
    bexar = df.filter(pl.col("track_no") == "PG250099").row(0, named=True)
    assert bexar["generating_capacity_mw"] == 100.0 and bexar["county_fips"] == "48029"
    assert bexar["control_area"] == "ERCOT" and bexar["service_area_utility"] == "CPS ENERGY"
    assert df.schema["self_generator"] == pl.Boolean


def test_one_table_per_kind():
    kinds = {d.name.removeprefix("puct_directory_") for d in DATASETS}
    assert kinds == {"agg", "coop", "iou", "muni", "pgc", "pgc_facility", "ra", "rep", "sg", "sg_facility", "tdu"}
    assert {d.mode for d in DATASETS} == {"replace"}
