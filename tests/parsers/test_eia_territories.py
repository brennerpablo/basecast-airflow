import polars as pl
import pytest

from basecast_pipelines.parsers.territories.eia_atlas import DATASETS, SQL_DATASETS, parse_territory_page

FIXTURE = "eia_territories/features_00000.geojson"  # Oncor, Farmers EC, SWEPCO, Upshur-Rural (clipped)


def test_territories_page(raw_file):
    f = raw_file("eia_territories", FIXTURE)
    df = parse_territory_page(f).sort("utility_id")
    assert df["utility_id"].to_list() == ["17698", "19579", "44372", "6182"]
    assert df.schema["utility_id"] == pl.String and df.schema["customers"] == pl.Int64
    by_id = {r["utility_id"]: r for r in df.iter_rows(named=True)}
    oncor = by_id["44372"]
    # wires-only TDSP: no control area, planning area ERCOT; -999999 and NOT AVAILABLE become null
    assert oncor["control_area"] is None and oncor["planning_area"] == "ERCOT" and oncor["in_ercot"]
    assert oncor["customers"] is None and oncor["utility_type"] == "iou"
    assert by_id["6182"]["in_ercot"] and by_id["6182"]["utility_type"] == "coop"
    assert not by_id["17698"]["in_ercot"] and by_id["17698"]["control_area"] == "SWPP"
    assert df["data_year"].unique().to_list() == [2022]
    assert df.schema["source_date"] == pl.Date
    assert not {"address", "telephone", "zip"} & set(df.columns)
    assert DATASETS[0].inputs(f)


def test_duplicate_ids_are_refused(raw_file):
    df = parse_territory_page(raw_file("eia_territories", FIXTURE))
    with pytest.raises(ValueError, match="duplicated utility_id"):
        DATASETS[0].finalize(pl.concat([df, df.head(1)]))


def test_sql_datasets():
    assert [d.name for d in SQL_DATASETS] == ["county_utility_overlap_eia", "county_utility_overlap"]
