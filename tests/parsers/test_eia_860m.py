"""EIA-860M fixtures: the August 2026 and December 2015 workbooks with every sheet trimmed to a few
Texas rows, a couple of out-of-state rows and (2026) one ERCOT plant outside Texas."""

from datetime import date

import polars as pl
import pytest

from basecast_pipelines.parsers.eia.eia_860m import parse_generators


def test_latest_month_keeps_texas_and_ercot(raw_file):
    f = raw_file("eia_860m", "eia_860m/august_generator2026.xlsx", meta={"year": 2026, "month": 8})
    df = parse_generators(f)
    assert df["report_month"].unique().to_list() == [date(2026, 8, 1)]
    assert set(df["sheet_status"]) == {"operating", "planned", "retired", "canceled_or_postponed"}
    assert ((df["state"] == "TX") | (df["balancing_authority_code"] == "ERCO")).all()
    assert df.filter(pl.col("state") != "TX").height == 1  # the ERCOT plant in Oklahoma
    assert "PR" not in set(df["state"])
    assert not {"google_map", "bing_map"} & set(df.columns)
    planned = df.filter(pl.col("sheet_status") == "planned")
    assert planned["status_code"].is_in(["P", "L", "T", "U", "V", "TS", "OT"]).all()
    assert planned.schema["planned_operation_year"] == pl.Int64
    assert df.schema["net_summer_capacity_mw"] == pl.Float64 and df.schema["utility_id"] == pl.String
    tx = df.filter(pl.col("state") == "TX")
    assert tx["county_fips"].null_count() == 0


def test_2015_layout_without_balancing_authority(raw_file):
    df = parse_generators(raw_file("eia_860m", "eia_860m/december_generator2015.xlsx"))
    assert df["report_month"].unique().to_list() == [date(2015, 12, 1)]
    assert set(df["state"]) == {"TX"}
    assert df["balancing_authority_code"].null_count() == df.height
    assert df.filter(pl.col("sheet_status") == "operating")["status"].unique().to_list() == ["Operating"]


def test_title_month_must_match_the_file(raw_file):
    f = raw_file("eia_860m", "eia_860m/december_generator2015.xlsx", name="november_generator2015.xlsx")
    with pytest.raises(ValueError, match="expected November 2015"):
        parse_generators(f)
