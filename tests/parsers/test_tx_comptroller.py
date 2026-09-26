"""tx_comptroller: trimmed CSVs and HTML pages. Officers, form preparers, contacts, authorized users, taxpayer
ids and individuals' names in the fixtures were replaced with placeholders before committing."""

import json
from datetime import date

import polars as pl

from basecast_pipelines.parsers.texas.comptroller import (
    DATASETS,
    parse_agreements,
    parse_data_centers,
    parse_jeti,
)

BY_NAME = {d.name: d for d in DATASETS}


def test_ch380_counties_recipients_and_no_personal_data(raw_file):
    f = raw_file("tx_comptroller", "tx_comptroller/ch380.csv")
    df = parse_agreements(f)
    assert df.height == 5 and set(df["program"]) == {"ch380"}
    rows = {r["agreement_id"]: r for r in df.iter_rows(named=True)}
    assert rows["0002181"]["county_fips"] == "48371"  # Pecos County government, no county listed
    assert rows["0002181"]["recipient_name"] == "Lancium LLC"
    assert rows["0000128"]["county_fips"] == "48181"  # Grayson, then Denton
    assert json.loads(rows["0000128"]["counties_fips"]) == ["48181", "48121"]
    assert rows["0000047"]["county_fips"] is None  # a city with no county listed
    assert rows["0013834"]["recipient_name"] is None and rows["0013834"]["recipient_name_redacted"]
    text = df.write_csv()
    for leak in ("Jane Doe", "John Doe", "Jane Roe", "example.test", "555010", "Example St"):
        assert leak not in text
    assert BY_NAME["cpa_local_dev_agreements"].inputs(f)


def test_ch312_one_row_per_agreement(raw_file):
    df = parse_agreements(raw_file("tx_comptroller", "tx_comptroller/ch312-abatement-detail.csv"))
    assert set(df["program"]) == {"ch312"} and df["agreement_id"].n_unique() == df.height == 3
    sharka = df.filter(pl.col("recipient_name").str.starts_with("Sharka")).row(0, named=True)
    assert sharka["county_fips"] == "48139" and sharka["county_name"] == "Ellis"
    assert sharka["n_taxing_units"] >= 1 and isinstance(json.loads(sharka["taxing_units"]), list)
    assert isinstance(sharka["effective_date"], date)
    amarillo = df.filter(pl.col("reporting_entity") == "Potter Randall CAD").row(0, named=True)
    assert amarillo["county_fips"] is None and amarillo["counties_fips"] == "[]"  # two-county CAD, city lead
    person = df.filter(pl.col("recipient_name_redacted")).height
    assert person == 1
    assert "Jane" not in df.write_csv() and "00000000000" not in df.write_csv()


def test_data_center_tables(raw_file):
    f = raw_file("tx_comptroller", "tx_comptroller/data-center-lists.html")
    df = parse_data_centers(f)
    assert set(df["program"]) == {"qualifying_data_center", "qualifying_large_data_center_project"}
    rows = {r["data_center_name"]: r for r in df.iter_rows(named=True)}
    hibbetts = rows["3301 Hibbetts Road Data Center"]
    operators = json.loads(hibbetts["operator_names"])
    assert operators[0] == "Core Scientific, Inc." and len(operators) == 3
    assert hibbetts["owner_names"] is None and hibbetts["registration_id"] == "DC481494"
    assert rows["Fort Haskell Data Center"]["registration_id"] == "DC112125"
    assert json.loads(rows["Fort Haskell Data Center"]["registration_ids"]) == ["DC112125", "DC1125125"]
    lancium = rows["Lancium Abilene Clean Campus IX"]
    assert lancium["effective_date"] == date(2026, 6, 17) and lancium["effective_date_text"] == "06/172026"
    assert rows["ECX AUS31-36"]["registration_id"] == "LD790989"  # "LLC LD790989-OP1" typo on the page
    assert rows["Borden County Data Center"]["county_fips"] == "48033"
    assert rows["Borden County Data Center"]["county_method"] == "name_hint"
    assert BY_NAME["cpa_data_centers"].inputs(f)


def test_jeti_table(raw_file):
    df = parse_jeti(raw_file("tx_comptroller", "tx_comptroller/jeti-current-agreements.html"))
    rows = {r["application_number"]: r for r in df.iter_rows(named=True)}
    assert set(rows) == {"J0001", "J0007", "J0035", "J0039"}
    assert rows["J0001"]["proposed_investment_usd"] == 1.7e9 and rows["J0001"]["minimum_required_jobs"] == 75
    assert rows["J0007"]["minimum_required_jobs"] is None and rows["J0007"]["jobs_requirement_waived"]
    assert rows["J0035"]["proposed_investment_usd"] == 1e10 and rows["J0035"]["limitation_pct"] == 50.0
    assert rows["J0039"]["school_district"] == "Iola ISD" and rows["J0039"]["county_fips"] is None


def test_newest_snapshot_only(raw_file):
    old = raw_file("tx_comptroller", "tx_comptroller/ch380.csv", dt=date(2026, 9, 20))
    new = raw_file("tx_comptroller", "tx_comptroller/ch380.csv", dt=date(2026, 9, 26))
    assert BY_NAME["cpa_local_dev_agreements"].files([old, new]) == [new]
