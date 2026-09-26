"""ERCOT membership fixtures: the 2024 workbook (Adjunct sheet with "Assigned Segment for PRS Voting",
Consumers with subsegments) and the 2013 one ("None" placeholders), trimmed to a few rows per sheet."""

import polars as pl

from basecast_pipelines.parsers.ercot.members import parse_members


def test_2024_members_by_segment(raw_file):
    df = parse_members(raw_file("ercot_members", "ercot_members/2024-ERCOT-Members.xlsx", meta={"page_year": 2024}))
    assert df["year"].unique().to_list() == [2024]
    assert set(df["segment"]) == {"adjunct", "consumer", "cooperative", "municipal"}
    adjunct = df.filter(pl.col("segment") == "adjunct")
    assert set(adjunct["assigned_segment"]) == {"Investor-Owned Utilities"}
    assert set(adjunct["membership_type"]) == {"adjunct"}
    coops = df.filter(pl.col("segment") == "cooperative")
    assert "Bandera Electric Cooperative, Inc." in coops["member_name"].to_list()
    assert set(coops["membership_type"]) == {"corporate"}
    consumers = df.filter(pl.col("segment") == "consumer")
    assert consumers["subsegment"].null_count() == 0  # forward-filled within each block
    assert set(consumers["subsegment_code"]) <= {"residential", "small", "large", "industrial"}
    assert df.filter(pl.col("segment") != "consumer")["subsegment"].null_count() == df.filter(
        pl.col("segment") != "consumer"
    ).height


def test_2013_placeholders_are_skipped(raw_file):
    df = parse_members(raw_file("ercot_members", "ercot_members/2013-ERCOTMembers.xlsx"))
    assert df["year"].unique().to_list() == [2013]  # from the sheet titles
    assert "None" not in df["member_name"].to_list()
    assert df.filter(pl.col("segment") == "adjunct").height == 0
    munis = df.filter(pl.col("segment") == "municipal")
    assert set(munis["membership_type"]) == {"corporate", "associate"}
