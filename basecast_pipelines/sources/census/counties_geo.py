"""Census cartographic boundary files at 1:500k, each the latest vintage linked from the Cartographic Boundary Files
page (shapefile links, not KML):

- counties: the national zip ``cb_<year>_us_county_500k.zip``; Texas (STATEFP=48) is filtered at parse time;
- places: the Texas zip ``cb_<year>_48_place_500k.zip`` (incorporated places and CDPs), read from the lake by
  ``marts/muni_places.py`` to check the muni → city crosswalk against the CCN polygons. No parser reads it.

``dt`` is the fetch date."""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import Link, fetch_links, file_links
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "census_tx_counties_geo"
PAGE = "https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html"

_COUNTY_500K = re.compile(r"^cb_(\d{4})_us_county_500k\.zip$", re.IGNORECASE)
_TX_PLACE_500K = re.compile(r"^cb_(\d{4})_48_place_500k\.zip$", re.IGNORECASE)  # state file, 48 = Texas


def _newest(links: list[Link], pattern: re.Pattern[str]) -> tuple[int, Link] | None:
    """(vintage, link) of the newest shapefile link whose file name matches ``pattern``."""
    candidates = [
        (int(m.group(1)), link) for link in links if (m := pattern.match(link.filename)) and "/shp/" in link.url
    ]
    return max(candidates, key=lambda c: c[0]) if candidates else None


def discover(http: HttpClient, *, include_places: bool = True) -> list[RemoteFile]:
    today = local_today()
    links = file_links(fetch_links(http, PAGE), (".zip",))
    files = []
    if county := _newest(links, _COUNTY_500K):
        vintage, link = county
        files.append(
            RemoteFile(url=link.url, dt=today, source_page=PAGE, meta={"vintage": vintage, "link_text": link.text})
        )
    if include_places and (place := _newest(links, _TX_PLACE_500K)):
        vintage, link = place
        meta = {"kind": "place", "state_fips": "48", "vintage": vintage, "link_text": link.text}
        files.append(RemoteFile(url=link.url, dt=today, source_page=PAGE, meta=meta))
    return files


run = source_runner(SOURCE_ID, discover)
