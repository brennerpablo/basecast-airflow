"""Open-Meteo archive API (ERA5) JSON → ``open_meteo_hourly`` (one row per weather point and UTC hour), plus
``weather_hourly_wz``: the per-weather-zone hourly mean of those points.

Each raw file is one point and one year, requested with ``timezone=GMT``: ``hourly.time`` is ISO text
without an offset, in UTC (``utc_offset_seconds`` is checked to be 0). ERA5 temperature and dew point are
instantaneous values at the top of the hour. The current year is re-fetched with each run and its newest
months are preliminary ERA5; ``by_key`` on (point_id, ts_utc) makes the latest fetch win.

``weather_hourly_wz`` averages the points of each zone with equal weights (``docs/decisions.md``: ERCOT does
not publish its stations or weights), over the points that have a value that hour; ``n_points`` says how
many there were, so a consumer can drop incomplete hours."""

from __future__ import annotations

import json
from urllib.parse import parse_qs, urlparse

import polars as pl

from basecast_pipelines.processing.core import Dataset, RawFile, SqlDataset, latest_by
from basecast_pipelines.sources.weather.open_meteo import load_points

SOURCE_ID = "open_meteo"
VARIABLES = {"temperature_2m": "temperature_c", "dew_point_2m": "dew_point_c"}
UNIT = "°C"


def _point_meta(f: RawFile) -> tuple[str, str, str]:
    """(point_id, weather_zone, model) from what discovery recorded, else from the URL and the config."""
    meta = f.meta
    query = parse_qs(urlparse(f.url).query)
    model = meta.get("model") or (query.get("models") or [None])[0]
    point_id = meta.get("point_id") or f.name.rsplit("_", 1)[0]
    zone = meta.get("weather_zone") or {p.id: p.weather_zone for p in load_points()}.get(point_id)
    if not (point_id and zone and model):
        raise ValueError(f"{f.key}: cannot tell the weather point, zone or model")
    return point_id, zone, model


def parse_open_meteo(f: RawFile) -> pl.DataFrame | None:
    payload = json.loads(f.read_bytes())
    if payload.get("error"):
        raise ValueError(f"{f.key}: Open-Meteo error payload: {payload.get('reason')}")
    if payload.get("utc_offset_seconds") != 0:
        raise ValueError(f"{f.key}: expected UTC times, got utc_offset_seconds={payload.get('utc_offset_seconds')}")
    hourly, units = payload["hourly"], payload.get("hourly_units", {})
    for var in VARIABLES:
        if var not in hourly:
            raise ValueError(f"{f.key}: hourly.{var} missing (has {sorted(hourly)})")
        if units.get(var) != UNIT:
            raise ValueError(f"{f.key}: hourly.{var} in {units.get(var)!r}, expected {UNIT}")
    point_id, zone, model = _point_meta(f)
    df = pl.DataFrame(
        {"time": hourly["time"], **{var: hourly[var] for var in VARIABLES}},
        schema={"time": pl.String, **{var: pl.Float64 for var in VARIABLES}},
    )
    if not df.height:
        return None  # an empty year (e.g. a request made before ERA5 covers it) has nothing to add
    return df.select(
        pl.lit(point_id).alias("point_id"),
        pl.lit(zone).alias("weather_zone"),
        pl.col("time").str.strptime(pl.Datetime("us"), "%Y-%m-%dT%H:%M").dt.replace_time_zone("UTC").alias("ts_utc"),
        *(pl.col(var).alias(name) for var, name in VARIABLES.items()),
        pl.lit(model).alias("model"),
        pl.lit(float(payload["latitude"])).alias("grid_latitude"),
        pl.lit(float(payload["longitude"])).alias("grid_longitude"),
        pl.lit(payload.get("elevation"), dtype=pl.Float64).alias("grid_elevation_m"),
    ).filter(pl.any_horizontal(pl.col(list(VARIABLES.values())).is_not_null()))


DATASETS = [
    Dataset(
        name="open_meteo_hourly",
        target="postgres",
        mode="by_key",
        key=("point_id", "ts_utc"),
        description="Open-Meteo ERA5 hourly 2 m temperature and dew point (°C, UTC) at the weather points of "
                    "config/weather_points.yaml, 2003 onward, with the ERA5 grid cell the API snapped to.",
        parse=parse_open_meteo,
        inputs=lambda f: f.suffix == ".json",
        # One file per point and year; a re-fetched year supersedes the older copy.
        select=latest_by(lambda f: f.name),
    ),
]

SQL_DATASETS = [
    SqlDataset(
        name="weather_hourly_wz",
        sql="""
            SELECT weather_zone,
                   ts_utc,
                   avg(temperature_c) AS temperature_c,
                   avg(dew_point_c) AS dew_point_c,
                   count(temperature_c)::integer AS n_points,
                   string_agg(DISTINCT model, ',' ORDER BY model) AS model
            FROM open_meteo_hourly
            GROUP BY weather_zone, ts_utc
        """,
        description="ERA5 hourly temperature and dew point per ERCOT weather zone (UTC): equal-weight mean of "
                    "the zone's weather points that have a value that hour (n_points of them).",
        indexes=(("weather_zone", "ts_utc"),),
    ),
]
