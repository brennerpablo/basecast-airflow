from dataclasses import replace
from datetime import datetime, timezone

import polars as pl

from basecast_pipelines.parsers.weather.ghcnh import DATASETS, parse_ghcnh

FIXTURE = "noaa_ghcnh/GHCNh_USW00013972_2026_trimmed.parquet"


def _file(raw_file):
    """19 real rows of Tyler 2026, fetched 2026-09-26 (so the row stamped 2026-10-02 is in the future)."""
    f = raw_file("noaa_ghcnh", FIXTURE, name="GHCNh_USW00013972_2026.parquet",
                 meta={"point_id": "east_tyler", "station": "USW00013972", "year": 2026})
    return replace(f, entry=replace(f.entry, fetched_at="2026-09-26T06:09:22+00:00"))


def test_ghcnh_keeps_observations_with_core_variables(raw_file):
    f = _file(raw_file)
    df = parse_ghcnh(f)
    # 5-minute precipitation rows and the future-dated row are dropped; SPECI (FM16) rows stay.
    assert df.height == 5
    assert df["ts_utc"].dtype == pl.Datetime("us", "UTC")
    assert df["ts_utc"].to_list()[0] == datetime(2026, 1, 2, 23, 53, tzinfo=timezone.utc)
    assert df["ts_utc"].is_sorted() and df.select("station_id", "ts_utc").is_unique().all()
    assert df["temperature_report_type"].to_list() == ["FM15", None, "FM15", "FM16", "FM16"]
    assert set(df["station_id"]) == {"USW00013972"}
    assert set(df["point_id"]) == {"east_tyler"} and set(df["weather_zone"]) == {"EAST"}
    assert DATASETS[0].inputs(f)


def test_ghcnh_types_and_sentinels(raw_file):
    df = parse_ghcnh(_file(raw_file))
    assert df.schema["temperature_c"] == pl.Float64
    assert df.schema["relative_humidity_pct"] == pl.Float64
    assert df.schema["wind_direction_deg"] == pl.Int32
    assert df["temperature_c"].to_list() == [20.0, None, 18.3, 12.2, 14.4]
    # 999 is calm (C) or variable (V), not a bearing: null, with the measurement code kept.
    assert df["wind_direction_deg"].to_list() == [None, None, None, 30, None]
    assert df["wind_direction_measurement_code"].to_list() == ["C", None, "C", "N", "V"]
    # The hourly precipitation row at :59 carries only precipitation.
    precip_row = df.filter(pl.col("temperature_c").is_null())
    assert precip_row["precipitation_mm"].to_list() == [0.0]
    for var in ("temperature", "dew_point", "precipitation"):
        for flag in ("measurement_code", "quality_code", "report_type", "source_code"):
            assert df.schema[f"{var}_{flag}"] == pl.String
    assert not [c for c, t in df.schema.items() if isinstance(t, (pl.List, pl.Struct))]
