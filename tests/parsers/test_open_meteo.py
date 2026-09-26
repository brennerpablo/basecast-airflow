import json
import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from pathlib import Path

import polars as pl
import pytest

from basecast_pipelines.parsers.weather.open_meteo import DATASETS, SQL_DATASETS, parse_open_meteo

FIXTURES = Path(__file__).parent.parent / "fixtures"
FIXTURE = "open_meteo/south_mcallen_2026.json"
URL = ("https://archive-api.open-meteo.com/v1/archive?latitude=26.179&longitude=-98.245&start_date=2026-01-01"
       "&end_date=2026-09-19&hourly=temperature_2m%2Cdew_point_2m&timezone=GMT&models=era5")
META = {"point_id": "south_mcallen", "weather_zone": "SOUTH", "year": 2026, "model": "era5"}


def test_open_meteo_hourly_rows(raw_file):
    f = raw_file("open_meteo", FIXTURE, meta=META, url=URL)
    df = parse_open_meteo(f)
    assert df.columns == ["point_id", "weather_zone", "ts_utc", "temperature_c", "dew_point_c", "model",
                          "grid_latitude", "grid_longitude", "grid_elevation_m"]
    assert df.height == 48
    assert df["ts_utc"].dtype == pl.Datetime("us", "UTC")
    assert df["ts_utc"][0] == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert df["ts_utc"].diff().drop_nulls().unique().to_list() == [timedelta(hours=1)]
    assert set(df["point_id"]) == {"south_mcallen"} and set(df["weather_zone"]) == {"SOUTH"}
    assert set(df["model"]) == {"era5"}
    assert (df["grid_latitude"][0], df["grid_longitude"][0]) == (26.25, -98.25)
    assert df["temperature_c"].null_count() == 0 and df["dew_point_c"].null_count() == 0
    assert DATASETS[0].inputs(f)


def test_open_meteo_falls_back_to_the_url_and_config(raw_file):
    """Without discovery metadata the point comes from the file name, the zone from the config, the model
    from the URL."""
    df = parse_open_meteo(raw_file("open_meteo", FIXTURE, url=URL))
    assert (df["point_id"][0], df["weather_zone"][0], df["model"][0]) == ("south_mcallen", "SOUTH", "era5")


def test_open_meteo_rejects_local_time(raw_file, tmp_path):
    payload = json.loads((FIXTURES / FIXTURE).read_text())
    payload["utc_offset_seconds"] = -18000
    bad = tmp_path / "south_mcallen_2026.json"
    bad.write_text(json.dumps(payload))
    f = raw_file("open_meteo", bad, meta=META, url=URL)
    with pytest.raises(ValueError, match="utc_offset_seconds"):
        parse_open_meteo(f)


def test_weather_hourly_wz_sql():
    """Runs the SQL dataset on a throwaway schema; needs BASECAST_TEST_DB_URL (e.g. the local PostGIS)."""
    url = os.environ.get("BASECAST_TEST_DB_URL")
    if not url:
        pytest.skip("BASECAST_TEST_DB_URL not set")
    import psycopg

    schema = f"_test_weather_{uuid4().hex[:8]}"
    rows = [
        ("south_mcallen", "SOUTH", "2026-01-01 00:00+00", 20.0, 10.0, "era5"),
        ("south_brownsville", "SOUTH", "2026-01-01 00:00+00", 22.0, 12.0, "era5"),
        ("south_corpus_christi", "SOUTH", "2026-01-01 00:00+00", 18.0, None, "era5"),
        ("south_mcallen", "SOUTH", "2026-01-01 01:00+00", 19.0, 9.0, "era5"),
        ("ncent_dfw", "NCENT", "2026-01-01 00:00+00", 5.0, 1.0, "era5"),
    ]
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute(f"CREATE SCHEMA {schema}")
        try:
            conn.execute(f"SET search_path TO {schema}")
            conn.execute("CREATE TABLE open_meteo_hourly (point_id text, weather_zone text, ts_utc timestamptz, "
                         "temperature_c double precision, dew_point_c double precision, model text)")
            with conn.cursor() as cur:
                cur.executemany("INSERT INTO open_meteo_hourly VALUES (%s, %s, %s, %s, %s, %s)", rows)
            out = conn.execute(
                f"SELECT weather_zone, extract(hour FROM ts_utc AT TIME ZONE 'UTC')::int, temperature_c, dew_point_c, "
                f"n_points, model FROM ({SQL_DATASETS[0].sql}) wz ORDER BY 1, 2"
            ).fetchall()
        finally:
            conn.execute(f"DROP SCHEMA {schema} CASCADE")
    assert out == [
        ("NCENT", 0, 5.0, 1.0, 1, "era5"),
        ("SOUTH", 0, 20.0, 11.0, 3, "era5"),
        ("SOUTH", 1, 19.0, 9.0, 1, "era5"),
    ]
