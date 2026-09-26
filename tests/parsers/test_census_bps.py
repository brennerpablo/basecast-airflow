"""census_bps: fixtures are the two header lines plus a few rows of real county files (national → Texas)."""

from datetime import date

from basecast_pipelines.parsers.census.permits import DATASETS, parse_permits


def test_annual_1990_layout_and_texas_filter(raw_file):
    f = raw_file("census_bps", "census_bps/co1990a.txt", meta={"kind": "annual", "year": 1990})
    df = parse_permits(f)
    assert df.height == 3  # the Alabama rows are dropped
    assert df["county_fips"].to_list() == ["48001", "48003", "48005"]
    assert set(df["period_type"]) == {"annual"} and set(df["year"]) == {1990} and df["month"].null_count() == 3
    assert df["period_start"].to_list() == [date(1990, 1, 1)] * 3
    anderson = df.row(0, named=True)
    assert anderson["county_name"] == "Anderson County"
    assert (anderson["bldgs_1_unit"], anderson["units_1_unit"], anderson["value_1_unit"]) == (10, 10, 364500)
    assert anderson["units_1_unit_rep"] == 10
    assert DATASETS[0].inputs(f)


def test_annual_2025_imputed_vs_reported_and_totals(raw_file):
    df = parse_permits(raw_file("census_bps", "census_bps/co2025a.txt"))  # period from the file name
    assert df["county_fips"].to_list() == ["48001", "48003", "48005", "48453"]
    andrews = df.filter(df["county_fips"] == "48003").row(0, named=True)
    assert andrews["units_1_unit"] == 67 and andrews["units_1_unit_rep"] == 48  # imputed ≥ reported
    assert andrews["units_5_plus_units"] == 26
    assert andrews["units_total"] == 67 + 26
    assert (df["units_total"] >= df["units_total_rep"]).all()


def test_monthly_blank_reported_block_is_null(raw_file):
    df = parse_permits(raw_file("census_bps", "census_bps/co0012c.txt"))
    assert set(df["period_type"]) == {"monthly"} and set(df["year"]) == {2000} and set(df["month"]) == {12}
    ector = df.filter(df["county_fips"] == "48135").row(0, named=True)
    assert ector["units_1_unit"] == 7 and ector["units_1_unit_rep"] is None and ector["units_total_rep"] is None
    brazos = df.filter(df["county_fips"] == "48041").row(0, named=True)
    assert brazos["units_5_plus_units"] == 96 and brazos["units_5_plus_units_rep"] == 96
