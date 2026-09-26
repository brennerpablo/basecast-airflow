"""Hourly Load Data Archives parser, on two trimmed real archives: 2016 (old layout, Excel datetimes, both DST
days, one hour without values) and 2025 (new layout, text labels with ``24:00`` and ``02:00 DST``)."""

from datetime import date, datetime, timezone

import polars as pl

from basecast_pipelines.parsers.ercot._load_common import WEATHER_ZONES
from basecast_pipelines.parsers.ercot.load_archive import DATASETS, parse_native_load


def utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def ercot(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("weather_zone") == "ERCOT").sort("ts_utc")


def test_old_layout_dst_days(raw_file):
    f = raw_file("ercot_native_load", "ercot_native_load/native_Load_2016_trimmed.zip", name="native_Load_2016.zip")
    assert DATASETS[0].inputs(f)
    df = parse_native_load(f)
    assert set(df["weather_zone"]) == {*WEATHER_ZONES, "ERCOT"}
    assert df.schema["ts_utc"] == pl.Datetime("us", "UTC")
    assert df.select(pl.struct("ts_utc", "weather_zone").is_unique().all()).item()

    total = ercot(df)
    # spring forward (2016-03-13): 23 hours; the old layout labels the second hour 03:00 (wall clock)
    spring = total.filter(pl.col("operating_date") == date(2016, 3, 13))
    assert spring.height == 23
    assert spring["hour_ending"].head(3).to_list() == [1, 3, 4]
    assert spring["ts_utc"].head(3).to_list() == [utc(2016, 3, 13, 7), utc(2016, 3, 13, 8), utc(2016, 3, 13, 9)]
    assert spring["ts_utc"][-1] == utc(2016, 3, 14, 5)  # HE 24 ends at local midnight (CDT)

    # fall back (2016-11-06): hour ending 02:00 twice, the second is the repeated hour; HE 24 has no values
    fall = total.filter(pl.col("operating_date") == date(2016, 11, 6))
    assert fall.height == 24
    assert fall["hour_ending"].head(4).to_list() == [1, 2, 2, 3]
    assert fall["dst_flag"].head(4).to_list() == [False, False, True, False]
    assert fall["ts_utc"].head(4).to_list() == [utc(2016, 11, 6, h) for h in (6, 7, 8, 9)]
    assert fall["hour_ending_local"][2] == datetime(2016, 11, 6, 2)
    assert fall["hour_ending"].max() == 23

    # each trimmed stretch is hourly-continuous in UTC, except the empty HE 24 of 2016-11-06
    for month, gaps in ((3, 0), (11, 1)):
        steps = total.filter(pl.col("operating_date").dt.month() == month)["ts_utc"].diff().drop_nulls()
        assert steps.dt.total_hours().to_list().count(1) == steps.len() - gaps


def test_new_layout_text_labels(raw_file):
    f = raw_file("ercot_native_load", "ercot_native_load/Native_Load_2025_trimmed.zip", name="Native_Load_2025.zip")
    df = parse_native_load(f)
    total = ercot(df)

    spring = total.filter(pl.col("operating_date") == date(2025, 3, 9))
    assert spring.height == 23
    assert spring["hour_ending"].head(3).to_list() == [1, 2, 4]  # the new layout skips 03:00
    assert spring["ts_utc"].head(3).to_list() == [utc(2025, 3, 9, 7), utc(2025, 3, 9, 8), utc(2025, 3, 9, 9)]

    fall = total.filter(pl.col("operating_date") == date(2025, 11, 2))
    assert fall.height == 25
    assert fall.filter(pl.col("dst_flag"))["hour_ending"].to_list() == [2]
    assert fall["ts_utc"].head(4).to_list() == [utc(2025, 11, 2, h) for h in (6, 7, 8, 9)]

    year_end = total.filter(pl.col("operating_date") == date(2025, 12, 31)).sort("hour_ending")
    assert year_end["hour_ending"].to_list() == [23, 24]
    assert year_end["hour_ending_local"][-1] == datetime(2026, 1, 1, 0)
    assert year_end["ts_utc"][-1] == utc(2026, 1, 1, 6)

    zones = df.filter(pl.col("weather_zone") != "ERCOT").group_by("ts_utc").agg(pl.col("mw").sum())
    check = zones.join(total.select("ts_utc", pl.col("mw").alias("total")), on="ts_utc")
    assert (check["mw"] - check["total"]).abs().max() < 0.01
    assert df["source"].unique().to_list() == ["ercot_native_load"]
