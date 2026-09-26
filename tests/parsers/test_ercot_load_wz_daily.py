"""NP6-345-CD parser, on one real daily file and on a SIMULATED fall-back day: the real 2026-08-24 csv relabeled
as 2026-11-01 with hour ending 02:00 repeated and flagged ``DSTFlag = Y`` (no real DST file was in the
31-day listing when this was written)."""

from datetime import date, datetime, timedelta, timezone

import polars as pl

from basecast_pipelines.parsers.ercot._load_common import SCHEMA
from basecast_pipelines.parsers.ercot.load_archive import parse_native_load
from basecast_pipelines.parsers.ercot.load_wz_daily import DATASETS, parse_load_wz_daily

REAL = "cdr.00013101.0000000000000000.20260825.055001399.ACTUALSYSLOADWZNP6345_csv.zip"


def utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def test_daily_file(raw_file):
    f = raw_file("ercot_load_wz_daily", f"ercot_load_wz_daily/{REAL}")
    assert DATASETS[0].inputs(f)
    df = parse_load_wz_daily(f)
    assert df.height == 24 * 9
    assert dict(df.schema) == SCHEMA
    total = df.filter(pl.col("weather_zone") == "ERCOT").sort("ts_utc")
    assert total["operating_date"].unique().to_list() == [date(2026, 8, 24)]
    assert total["ts_utc"][0] == utc(2026, 8, 24, 6)  # HE 1 CDT ends 01:00 CDT = 06:00 UTC
    assert total["ts_utc"][-1] == utc(2026, 8, 25, 5)
    assert total["mw"][0] == 69371.92
    assert not df["dst_flag"].any()


def test_same_schema_as_the_archive(raw_file):
    daily = parse_load_wz_daily(raw_file("ercot_load_wz_daily", f"ercot_load_wz_daily/{REAL}"))
    archive = parse_native_load(
        raw_file("ercot_native_load", "ercot_native_load/Native_Load_2025_trimmed.zip", name="Native_Load_2025.zip")
    )
    assert daily.schema == archive.schema  # both feed ercot_load_hourly_wz


def test_simulated_fall_back_day(raw_file):
    f = raw_file("ercot_load_wz_daily", "ercot_load_wz_daily/simulated_dst_fall_back_20261101.zip")
    total = parse_load_wz_daily(f).filter(pl.col("weather_zone") == "ERCOT").sort("ts_utc")
    assert total.height == 25
    assert total["hour_ending"].head(4).to_list() == [1, 2, 2, 3]
    assert total["dst_flag"].head(4).to_list() == [False, False, True, False]
    # 25 consecutive UTC hours from 06:00 UTC (HE 1 CDT) to 06:00 UTC next day (HE 24 CST)
    assert total["ts_utc"].to_list() == [utc(2026, 11, 1, 5) + timedelta(hours=h) for h in range(1, 26)]
