"""RTP parser. Fixture: a zip of yearly zips like the 2014–2023 archive, holding a cell-for-cell copy of the
2025 Appendix B load sheet, a project-list workbook (header only) and a plan with no input-assumptions
workbook (simulated placeholder PDF)."""

import polars as pl

from basecast_pipelines.parsers.ercot.rtp import parse_rtp_load


def test_reliability_case_load(raw_file):
    df = parse_rtp_load(raw_file("ercot_rtp", "ercot_rtp/2025_RTP_Report_Public_Archive_trimmed.zip"))
    assert set(df["rtp_year"]) == {2025}
    assert set(df["series"]) == {"tsp_submitted", "ercot_forecast_p90", "rtp_load", "rtp_load_off_peak"}
    totals = df.filter((pl.col("region_type") == "ercot") & (pl.col("target_year") == 2027))
    by_series = dict(totals.select("series", "value").iter_rows())
    assert by_series == {"tsp_submitted": 127724.38, "ercot_forecast_p90": 86577.422128, "rtp_load": 125862.15}
    zones = set(df.filter(pl.col("region_type") == "weather_zone")["region_id"])
    assert zones == {"COAST", "EAST", "FWEST", "NORTH", "NCENT", "SCENT", "SOUTH", "WEST"}
    assert df["status"].unique().to_list() == ["Final"]
    assert df["date_last_updated"].null_count() == 0


def test_files_without_load_tables(raw_file):
    pdf = raw_file("ercot_rtp", "ercot_rtp/2025_RTP_Report_Public_Archive_trimmed.zip", name="plan.pdf")
    assert parse_rtp_load(pdf) is None
