"""BLS QCEW open data: annual county establishments, employment and wages for selected NAICS industries
(518210 data processing & hosting = data centers), from the documented CSV API
``data.bls.gov/cew/data/api/<year>/a/industry/<naics>.csv`` (national files; Texas at parse). Many small
counties are suppressed. ``dt`` is the fetch date."""

from __future__ import annotations

from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "bls_qcew"
API = "https://data.bls.gov/cew/data/api/{year}/a/industry/{naics}.csv"
INDUSTRIES = ("518210",)


def discover(http: HttpClient, *, start_year: int = 2014) -> list[RemoteFile]:
    today = local_today()
    return [
        RemoteFile(
            url=API.format(year=year, naics=naics),
            dt=today,
            filename=f"qcew_{year}_annual_{naics}.csv",
            immutable=year <= today.year - 2,
            meta={"year": year, "naics": naics},
        )
        for naics in INDUSTRIES
        for year in range(int(start_year), today.year)
    ]


run = source_runner(SOURCE_ID, discover)
