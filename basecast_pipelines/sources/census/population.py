"""Census Population Estimates Program, county totals with components of change (latest vintage), from
the popest datasets directory (newest ``YYYY-YYYY`` folder, ``counties/totals/co-est*-alldata.csv``).
The file is Latin-1. ``dt`` is the fetch date."""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import fetch_links
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "census_pep"
DATASETS = "https://www2.census.gov/programs-surveys/popest/datasets/"

_PERIOD = re.compile(r"/datasets/(\d{4})-(\d{4})/$")
_ALLDATA = re.compile(r"^co-est\d{4}-alldata\.csv$", re.IGNORECASE)


def discover(http: HttpClient) -> list[RemoteFile]:
    periods = {}
    for link in fetch_links(http, DATASETS):
        match = _PERIOD.search(link.url)
        if match:
            periods[(int(match.group(2)), int(match.group(1)))] = link.url
    if not periods:
        return []
    totals = max(periods.items())[1] + "counties/totals/"
    return [
        RemoteFile(url=link.url, dt=local_today(), source_page=totals)
        for link in fetch_links(http, totals)
        if _ALLDATA.match(link.filename)
    ]


run = source_runner(SOURCE_ID, discover)
