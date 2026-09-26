"""Settlement point price parsers. Fixtures are trimmed from the real yearly files: RTMLZHBSPP_2024 (spring
and fall DST days, HB_NORTH and LZ_HOUSTON as LZ and LZEW, plus the footer row some RTM workbooks carry
and an empty future-month sheet) and DAMLZHBSPP_2011 (DST days, two points)."""

from datetime import date, datetime, timezone

import polars as pl
import pytest

from basecast_pipelines.parsers.ercot._market_common import dst_days
from basecast_pipelines.parsers.ercot.spp_hist import DATASETS, parse_dam, parse_rtm

RTM = "ercot_spp_hist/rpt.00013061.0000000000000000.20250101.081829672.RTMLZHBSPP_2024.zip"
DAM = "ercot_spp_hist/rpt.00013060.0000000000000000.20130301.132053000.DAMLZHBSPP_2011.zip"


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def test_dst_days():
    assert dst_days(2024) == (date(2024, 3, 10), date(2024, 11, 3))
    assert dst_days(2011) == (date(2011, 3, 13), date(2011, 11, 6))


def test_rtm_types_and_footer(raw_file):
    f = raw_file("ercot_spp_hist", RTM, meta={"market": "rtm", "friendly_name": "RTMLZHBSPP_2024"})
    df = parse_rtm(f)
    assert df.schema == {
        "delivery_date": pl.Date,
        "delivery_hour": pl.Int8,
        "delivery_interval": pl.Int8,
        "dst_flag": pl.Boolean,
        "settlement_point": pl.String,
        "settlement_point_type": pl.String,
        "price_usd_mwh": pl.Float64,
        "interval_start_utc": pl.Datetime("us", "UTC"),
    }
    # 3 series x (spring: 3 hours + fall: 4 hour passes) x 4 intervals; footer row and empty sheet dropped
    assert df.height == 3 * 7 * 4
    assert set(df.select("settlement_point", "settlement_point_type").unique().rows()) == {
        ("HB_NORTH", "HU"), ("LZ_HOUSTON", "LZ"), ("LZ_HOUSTON", "LZEW"),
    }
    assert parse_dam(f) is None
    assert DATASETS[0].inputs(f) and not DATASETS[1].inputs(f)


def test_rtm_dst_days_are_continuous(raw_file):
    df = parse_rtm(raw_file("ercot_spp_hist", RTM))  # market read from the file name
    hub = df.filter(pl.col("settlement_point") == "HB_NORTH").sort("interval_start_utc")

    spring = hub.filter(pl.col("delivery_date") == date(2024, 3, 10))
    assert spring["delivery_hour"].unique().sort().to_list() == [1, 2, 4]  # hour ending 03 does not exist
    assert spring["interval_start_utc"][0] == utc(2024, 3, 10, 6, 0)  # 00:00 CST
    assert spring.filter(pl.col("delivery_hour") == 4)["interval_start_utc"][0] == utc(2024, 3, 10, 8, 0)  # 03:00 CDT

    fall = hub.filter(pl.col("delivery_date") == date(2024, 11, 3))
    assert fall.height == 16 and fall["dst_flag"].sum() == 4
    assert fall["interval_start_utc"][0] == utc(2024, 11, 3, 5, 0)  # 00:00 CDT
    assert fall.filter(pl.col("delivery_hour") == 2, ~pl.col("dst_flag"))["interval_start_utc"][0] == utc(2024, 11, 3, 6, 0)
    assert fall.filter(pl.col("dst_flag"))["interval_start_utc"][0] == utc(2024, 11, 3, 7, 0)  # 01:00 CST
    steps = fall["interval_start_utc"].diff().drop_nulls().dt.total_minutes().unique().to_list()
    assert steps == [15]


def test_rtm_key_is_unique(raw_file):
    df = parse_rtm(raw_file("ercot_spp_hist", RTM))
    key = list(DATASETS[0].key)
    assert df.select(key).is_unique().all()
    assert df.select(pl.col(key).null_count()).sum_horizontal().item() == 0


def test_dam_hour_ending_and_repeated_hour(raw_file):
    f = raw_file("ercot_spp_hist", DAM, meta={"market": "dam"})
    df = parse_dam(f)
    assert parse_rtm(f) is None
    assert df.columns == ["delivery_date", "delivery_hour", "dst_flag", "settlement_point", "price_usd_mwh", "hour_start_utc"]
    north = df.filter(pl.col("settlement_point") == "HB_NORTH").sort("hour_start_utc")

    spring = north.filter(pl.col("delivery_date") == date(2011, 3, 13), pl.col("delivery_hour") <= 4)
    assert spring.select("delivery_hour", "hour_start_utc").rows() == [
        (1, utc(2011, 3, 13, 6)), (2, utc(2011, 3, 13, 7)), (4, utc(2011, 3, 13, 8)),
    ]
    fall = north.filter(pl.col("delivery_date") == date(2011, 11, 6), pl.col("delivery_hour") <= 4)
    assert fall.select("delivery_hour", "dst_flag", "hour_start_utc").rows() == [
        (1, False, utc(2011, 11, 6, 5)), (2, False, utc(2011, 11, 6, 6)), (2, True, utc(2011, 11, 6, 7)),
        (3, False, utc(2011, 11, 6, 8)), (4, False, utc(2011, 11, 6, 9)),
    ]
    assert north.filter(pl.col("delivery_hour") == 24)["hour_start_utc"].to_list() == [
        utc(2011, 3, 14, 4), utc(2011, 11, 7, 5),
    ]
    assert df["price_usd_mwh"].is_not_null().all()


def test_newest_file_per_year_is_selected(raw_file):
    old = raw_file("ercot_spp_hist", RTM, dt=date(2026, 1, 1), name="rpt.a.RTMLZHBSPP_2026.zip",
                   meta={"market": "rtm", "friendly_name": "RTMLZHBSPP_2026"})
    new = raw_file("ercot_spp_hist", RTM, dt=date(2026, 9, 20), name="rpt.b.RTMLZHBSPP_2026.zip",
                   meta={"market": "rtm", "friendly_name": "RTMLZHBSPP_2026"})
    assert DATASETS[0].files([old, new]) == [new]


def test_flag_outside_the_repeated_hour_raises():
    from basecast_pipelines.parsers.ercot._market_common import check_repeated

    df = pl.DataFrame({"t": [datetime(2024, 7, 1, 1, 0)], "f": [True]})
    with pytest.raises(ValueError, match="repeated hour"):
        check_repeated(df, "t", "f", "x")
