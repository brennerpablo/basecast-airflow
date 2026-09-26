"""NOAA GHCNh station-year Parquet → ``noaa_ghcnh_hourly``: the core surface variables of the 12 airport
stations, one row per published observation, with their GHCNh flags.

GHCNh (v1.1.0 documentation, ``ghcnh_DOCUMENTATION.pdf``) publishes each variable with five attribute
columns: ``_Measurement_Code``, ``_Quality_Code``, ``_Report_Type``, ``_Source_Code`` and
``_Source_Station_ID``. The first four are kept as published text, per variable (a row can mix reports:
e.g. temperature from a METAR and precipitation from another feed); ``_Source_Station_ID`` is kept for
temperature only, because GHCNh publishes one fixed lat/lon per station and the source id is the only trace
of a station change (e.g. Austin's 1980s rows come only from ``ICAO-KAUS``, which was likely Mueller airport
before 1999: not verified). ``DATE`` is the observation time in UTC.

Observations are kept as published, including sub-hourly SPECI (FM16) reports: ``ts_utc`` is the
observation time and nothing is aggregated here. Rows are dropped only when they carry none of the core
variables (e.g. the 5-minute precipitation rows of recent years) or when they are stamped after the file
was fetched (seen: 49 rows without temperature dated 2026-10-01 to 10-04 in the 2026 files fetched on
2026-09-26).

Value conventions: units are the documented ones (°C, %, m/s, degrees, hPa, mm). ``wind_direction`` 999
is not a direction but a sentinel that GHCNh uses with measurement code ``C`` (calm) or ``V`` (variable),
so it becomes null (the measurement code keeps the reason); 0 (the documented calm value) is kept.
GHCNh's own QC does not catch every outlier (a few temperatures of 50-222 °C carry quality code 1 or 5,
"passed all checks"), so consumers must range-check; this table does not filter values.

``point_id`` and ``weather_zone`` come from ``config/weather_points.yaml`` (``ghcnh_id``).
The current-year file is re-downloaded with each fetch; ``by_key`` makes the latest version win."""

from __future__ import annotations

import logging
from datetime import datetime
from functools import cache

import polars as pl
import yaml

from basecast_pipelines.processing.core import Dataset, RawFile, latest_by
from basecast_pipelines.sources.weather.open_meteo import POINTS_FILE

SOURCE_ID = "noaa_ghcnh"
log = logging.getLogger(__name__)

# GHCNh column → (our column, cast). The cast turns GHCNh's text columns into numbers.
VARIABLES: dict[str, tuple[str, pl.DataType]] = {
    "temperature": ("temperature_c", pl.Float64),
    "dew_point_temperature": ("dew_point_c", pl.Float64),
    "relative_humidity": ("relative_humidity_pct", pl.Float64),
    "wind_speed": ("wind_speed_ms", pl.Float64),
    "wind_direction": ("wind_direction_deg", pl.Int32),
    "sea_level_pressure": ("sea_level_pressure_hpa", pl.Float64),
    "precipitation": ("precipitation_mm", pl.Float64),
}
# Prefix of each variable's flag columns in our table.
FLAG_PREFIX = {
    "temperature": "temperature",
    "dew_point_temperature": "dew_point",
    "relative_humidity": "relative_humidity",
    "wind_speed": "wind_speed",
    "wind_direction": "wind_direction",
    "sea_level_pressure": "sea_level_pressure",
    "precipitation": "precipitation",
}
FLAGS = {
    "Measurement_Code": "measurement_code",
    "Quality_Code": "quality_code",
    "Report_Type": "report_type",
    "Source_Code": "source_code",
}
STATION_COLUMNS = {"STATION": "station_id", "Station_name": "station_name", "LATITUDE": "latitude",
                   "LONGITUDE": "longitude", "ELEVATION": "elevation_m"}
KEY = ("station_id", "ts_utc")
WIND_DIRECTION_NONE = 999  # calm (measurement code C) or variable (V): no direction


@cache
def station_points() -> dict[str, tuple[str, str]]:
    """GHCNh station id → (weather point id, weather zone), from ``config/weather_points.yaml``."""
    points = yaml.safe_load(POINTS_FILE.read_text())["points"]
    return {p["ghcnh_id"]: (p["id"], p["weather_zone"]) for p in points if p.get("ghcnh_id")}


def _columns() -> list[str]:
    cols = list(STATION_COLUMNS) + ["DATE", "temperature_Source_Station_ID"]
    for var in VARIABLES:
        cols += [var, *(f"{var}_{flag}" for flag in FLAGS)]
    return cols


def _timestamp(schema: pl.Schema) -> pl.Expr:
    dtype = schema["DATE"]
    if dtype == pl.String:
        return pl.col("DATE").str.strptime(pl.Datetime("us"), "%Y-%m-%dT%H:%M:%S").dt.replace_time_zone("UTC")
    if isinstance(dtype, pl.Datetime):
        col = pl.col("DATE").cast(pl.Datetime("us", dtype.time_zone))
        return col.dt.replace_time_zone("UTC") if dtype.time_zone is None else col.dt.convert_time_zone("UTC")
    raise ValueError(f"unexpected GHCNh DATE type {dtype}")


def parse_ghcnh(f: RawFile) -> pl.DataFrame | None:
    wanted = _columns()
    with f.local_path() as path:
        schema = pl.read_parquet_schema(path)
        missing = [c for c in wanted if c not in schema]
        if missing:
            raise ValueError(f"{f.key}: GHCNh columns missing {missing}")
        raw = pl.read_parquet(path, columns=wanted)

    values = []
    for var, (name, dtype) in VARIABLES.items():
        col = pl.col(var)
        if raw.schema[var] == pl.String:
            col = col.str.strip_chars().cast(pl.Float64, strict=False)
        col = col.cast(dtype, strict=False)
        if var == "wind_direction":
            col = pl.when(col == WIND_DIRECTION_NONE).then(None).otherwise(col)
        values.append(col.alias(name))
        values += [
            pl.col(f"{var}_{flag}").cast(pl.String).str.strip_chars().alias(f"{FLAG_PREFIX[var]}_{suffix}")
            for flag, suffix in FLAGS.items()
        ]

    df = raw.select(
        pl.col("STATION").str.strip_chars().alias("station_id"),
        pl.col("Station_name").str.strip_chars().alias("station_name"),
        pl.col("LATITUDE").cast(pl.Float64).alias("latitude"),
        pl.col("LONGITUDE").cast(pl.Float64).alias("longitude"),
        pl.col("ELEVATION").cast(pl.Float64).alias("elevation_m"),
        _timestamp(raw.schema).alias("ts_utc"),
        *values,
        pl.col("temperature_Source_Station_ID").cast(pl.String).str.strip_chars()
        .alias("temperature_source_station_id"),
    )
    value_cols = [name for name, _ in VARIABLES.values()]
    fetched = datetime.fromisoformat(f.entry.fetched_at)
    df = df.filter(
        pl.any_horizontal(pl.col(value_cols).is_not_null())
        & (pl.col("ts_utc") <= pl.lit(fetched).cast(pl.Datetime("us", "UTC")))
    )
    if df.select(pl.col(list(KEY)).is_null().any()).row(0) != (False, False):
        raise ValueError(f"{f.key}: GHCNh rows without STATION or DATE")
    duplicates = df.height - df.select(KEY).n_unique()
    if duplicates:
        # Never seen in 1980-2026; BigQuery's by_key insert does not dedupe, so keep the last one.
        log.warning("%s: %d duplicate (station_id, ts_utc) rows, keeping the last", f.key, duplicates)
        df = df.unique(subset=list(KEY), keep="last", maintain_order=True)

    points = station_points()
    stations = df["station_id"].unique().to_list()
    unknown = [s for s in stations if s not in points]
    if unknown:
        log.warning("%s: stations %s are not in config/weather_points.yaml; point_id left null", f.key, unknown)
    mapping = pl.DataFrame(
        {"station_id": list(points), "point_id": [p for p, _ in points.values()],
         "weather_zone": [z for _, z in points.values()]},
        schema={"station_id": pl.String, "point_id": pl.String, "weather_zone": pl.String},
    )
    df = df.join(mapping, on="station_id", how="left")
    head = ["station_id", "point_id", "weather_zone", "ts_utc", *value_cols]
    return df.select(*head, *[c for c in df.columns if c not in head]).sort("ts_utc")


DATASETS = [
    Dataset(
        name="noaa_ghcnh_hourly",
        target="bigquery",
        mode="by_key",
        key=KEY,
        partition=("ts_utc", "MONTH"),
        cluster=("station_id",),
        description="NOAA GHCNh observations (UTC, sub-hourly as published) at the 12 weather-point airports, "
                    "1980 onward: temperature, dew point, relative humidity, wind, sea-level pressure and "
                    "precipitation with their GHCNh measurement, quality, report-type and source codes.",
        parse=parse_ghcnh,
        inputs=lambda f: f.suffix == ".parquet" and f.name.startswith("GHCNh_"),
        # One file per station and year; a re-fetched current year supersedes the older copy.
        select=latest_by(lambda f: f.name),
    ),
]
