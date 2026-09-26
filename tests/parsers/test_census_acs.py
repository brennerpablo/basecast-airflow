"""census_acs: a trimmed B25032 summary file (US, Texas, one Alabama county, two Texas counties, a tract) and
trimmed table shells."""

from basecast_pipelines.parsers.census.housing import DATASETS, build_housing


def test_b25032_long_format_with_shell_labels(raw_file):
    data = raw_file("census_acs", "census_acs/acsdt5y2024-b25032.dat", meta={"vintage": 2024, "table": "b25032"})
    shells = raw_file("census_acs", "census_acs/ACS20245YR_Table_Shells.txt", meta={"vintage": 2024})
    ds = DATASETS[0]
    assert ds.files([data, shells]) == [data, shells]
    df = build_housing([data, shells])
    assert df.height == 2 * 23
    assert sorted(df["county_fips"].unique()) == ["48301", "48453"]
    travis = {r["line"]: r for r in df.filter(df["county_fips"] == "48453").iter_rows(named=True)}
    assert travis[1]["tenure"] == "total" and travis[1]["estimate"] == 583747 and travis[1]["moe"] == 1810
    assert travis[3]["tenure"] == "owner_occupied" and travis[3]["units_in_structure"] == "1, detached"
    assert travis[3]["estimate"] == 260824
    assert travis[4]["units_in_structure"] == "1, attached"
    assert travis[14]["tenure"] == "renter_occupied"
    assert travis[2]["estimate"] == sum(travis[i]["estimate"] for i in range(3, 13))
    assert set(df["vintage"]) == {2024} and df["estimate_annotation"].null_count() == df.height
    assert set(df["source_file"]) == {data.key}
