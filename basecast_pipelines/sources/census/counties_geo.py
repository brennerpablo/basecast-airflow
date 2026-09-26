"""Census cartographic boundary file for counties at 1:500k (national shapefile zip, latest vintage linked
from the Cartographic Boundary Files page). Texas (STATEFP=48) is filtered at parse time. ``dt`` is the
fetch date."""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import fetch_links, file_links
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "census_tx_counties_geo"
PAGE = "https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html"

_COUNTY_500K = re.compile(r"^cb_(\d{4})_us_county_500k\.zip$", re.IGNORECASE)


def discover(http: HttpClient) -> list[RemoteFile]:
    candidates = []
    for link in file_links(fetch_links(http, PAGE), (".zip",)):
        match = _COUNTY_500K.match(link.filename)
        if match and "/shp/" in link.url:
            candidates.append((int(match.group(1)), link))
    if not candidates:
        return []
    vintage, link = max(candidates, key=lambda c: c[0])
    return [
        RemoteFile(url=link.url, dt=local_today(), source_page=PAGE, meta={"vintage": vintage, "link_text": link.text})
    ]


run = source_runner(SOURCE_ID, discover)
