"""NOAA GHCNh (Global Historical Climatology Network hourly, successor of ISD, which stopped updating in
2025): station-year Parquet files for the airport stations in ``config/weather_points.yaml``
(``ghcnh_id``), 1980 onward, listed from NCEI's public bucket. U.S. federal data, no license limits, so this
is the license-free alternative to Open-Meteo. ``dt`` is the fetch date; the current year changes daily."""

from __future__ import annotations

from datetime import date

import yaml

from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.common.s3 import list_objects
from basecast_pipelines.config import local_today
from basecast_pipelines.sources.weather.open_meteo import POINTS_FILE

SOURCE_ID = "noaa_ghcnh"
BUCKET = "https://www.ncei.noaa.gov/oa/global-historical-climatology-network"


def discover(http: HttpClient, *, start_year: int = 1980, end_year: int | None = None) -> list[RemoteFile]:
    today = local_today()
    points = yaml.safe_load(POINTS_FILE.read_text())["points"]
    files = []
    for year in range(int(start_year), int(end_year or today.year) + 1):
        for point in points:
            prefix = f"hourly/access/by-year/{year}/parquet/GHCNh_{point['ghcnh_id']}_"
            for obj in list_objects(http, BUCKET, prefix):
                files.append(
                    RemoteFile(
                        url=f"{BUCKET}/{obj.key}",
                        dt=today,
                        source_page=f"{BUCKET}?list-type=2&prefix={prefix}",
                        immutable=today >= date(year + 1, 4, 1),
                        meta={"point_id": point["id"], "station": point["ghcnh_id"], "year": year,
                              "listed_bytes": obj.size, "listed_etag": obj.etag},
                    )
                )
    return files


run = source_runner(SOURCE_ID, discover)
