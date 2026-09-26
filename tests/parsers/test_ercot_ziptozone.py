from basecast_pipelines.parsers.ercot.ziptozone import DATASETS, parse_zip_zone


def test_zip_zone_reads_the_ziptozone_sheet(raw_file):
    f = raw_file("ercot_ziptozone", "ercot_ziptozone/appendix_d_trimmed.xlsx")
    df = parse_zip_zone(f)
    assert df.columns == ["zip_code", "weather_zone", "weather_zone_name"]
    assert df.height == 4
    assert df["zip_code"].str.len_chars().to_list() == [5] * 4
    assert set(df["weather_zone"]) <= {"COAST", "EAST", "FWEST", "NORTH", "NCENT", "SOUTH", "SCENT", "WEST"}
    assert DATASETS[0].inputs(f)
