from pathlib import Path

import polars as pl
import pytest

from basecast_pipelines.parsers.outages.eaglei import DATASETS, parse_coverage_history, parse_dqi, parse_mcc


def test_mcc_county_fips_and_total(raw_file):
    f = raw_file("ornl_eaglei", "ornl_eaglei/MCC.csv")
    df = parse_mcc(f)
    assert df.columns == ["county_fips", "customers"]
    assert df["county_fips"].to_list() == ["01001", "01003", "01005", "48001", "48003", "48005", "48453"]
    assert df.schema["customers"] == pl.Int64
    assert df.filter(pl.col("county_fips") == "48453")["customers"].item() == 641926
    datasets = {d.name: d for d in DATASETS}
    assert datasets["eaglei_mcc"].inputs(f) and not datasets["eaglei_dqi"].inputs(f)


def test_mcc_grand_total_must_match(raw_file, tmp_path):
    text = (Path(__file__).parent.parent / "fixtures" / "ornl_eaglei/MCC.csv").read_text(encoding="utf-8-sig")
    bad = tmp_path / "MCC.csv"
    bad.write_text(text.replace("48453,641926", "48453,1"))
    with pytest.raises(ValueError, match="Grand Total"):
        parse_mcc(raw_file("ornl_eaglei", bad))


def test_coverage_history_years(raw_file):
    df = parse_coverage_history(raw_file("ornl_eaglei", "ornl_eaglei/coverage_history.csv"))
    assert df.columns == ["year", "state", "total_customers", "min_covered", "max_covered", "min_pct_covered",
                          "max_pct_covered"]
    tx = df.filter(pl.col("state") == "TX").sort("year")
    assert tx["year"].to_list() == [2018, 2019, 2020, 2021, 2022]
    assert tx["max_pct_covered"].to_list() == [0.9, 0.93, 0.63, 0.94, 0.93]
    assert df.schema["total_customers"] == pl.Int64


def test_dqi_by_fema_region(raw_file):
    df = parse_dqi(raw_file("ornl_eaglei", "ornl_eaglei/DQI.csv"))
    assert df.columns[:2] == ["fema_region", "year"] and "dqi" in df.columns
    region6 = df.filter(pl.col("fema_region") == 6).sort("year")
    assert region6["year"].to_list() == [2018, 2019, 2020, 2021, 2022]
    assert region6["dqi"][0] == pytest.approx(78.94663535)
    assert df.schema["max_covered"] == pl.Int64 and df.schema["dqi"] == pl.Float64


def test_yearly_outage_files_have_no_dataset(raw_file):
    f = raw_file("ornl_eaglei", "ornl_eaglei/DQI.csv", name="eaglei_outages_2021.csv")
    assert not any(d.inputs(f) for d in DATASETS)
