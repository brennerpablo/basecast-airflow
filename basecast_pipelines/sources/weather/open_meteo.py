"""Open-Meteo historical weather (archive API, ERA5 reanalysis): hourly temperature and dew point in UTC for
the weather points in ``config/weather_points.yaml``, one request per point and year since 2003.
ERA5 lags ~5 days and its latest months are preliminary, so past years are only treated as final from
April of the following year. ``dt`` is the fetch date. Free tier: non-commercial use only."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import httpx
import yaml

from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import PROJECT_ROOT, local_today

SOURCE_ID = "open_meteo"
ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"
POINTS_FILE = PROJECT_ROOT / "config" / "weather_points.yaml"
HOURLY = "temperature_2m,dew_point_2m"
ARCHIVE_LAG = timedelta(days=6)


@dataclass(frozen=True)
class WeatherPoint:
    id: str
    weather_zone: str
    latitude: float
    longitude: float


def load_points(path=POINTS_FILE) -> list[WeatherPoint]:
    payload = yaml.safe_load(path.read_text())
    return [
        WeatherPoint(p["id"], p["weather_zone"], float(p["latitude"]), float(p["longitude"]))
        for p in payload["points"]
    ]


def discover(http: HttpClient, *, start_year: int = 2003, model: str = "era5") -> list[RemoteFile]:
    today = local_today()
    last_day = today - ARCHIVE_LAG
    files = []
    for point in load_points():
        for year in range(int(start_year), last_day.year + 1):
            end = min(date(year, 12, 31), last_day)
            params = {
                "latitude": point.latitude,
                "longitude": point.longitude,
                "start_date": date(year, 1, 1).isoformat(),
                "end_date": end.isoformat(),
                "hourly": HOURLY,
                "timezone": "GMT",
                "models": model,
            }
            files.append(
                RemoteFile(
                    url=str(httpx.URL(ENDPOINT, params=params)),
                    dt=today,
                    filename=f"{point.id}_{year}.json",
                    source_page=ENDPOINT,
                    immutable=end == date(year, 12, 31) and today >= date(year + 1, 4, 1),
                    meta={"point_id": point.id, "weather_zone": point.weather_zone, "year": year, "model": model},
                )
            )
    return files


run = source_runner(SOURCE_ID, discover)
