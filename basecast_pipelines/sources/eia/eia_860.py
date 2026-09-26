"""EIA-860 annual detailed data (utilities, plants with their transmission/distribution system owner,
generators incl. proposed ones, storage, owners). Latest final release by default, from the EIA-860 page;
``--opt min_year=<yyyy>`` takes older years too. ``dt`` is the fetch date."""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import fetch_links, file_links
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "eia_860"
PAGE = "https://www.eia.gov/electricity/data/eia860/"

_ZIP = re.compile(r"^eia860(\d{4})\.zip$", re.IGNORECASE)


def discover(http: HttpClient, *, min_year: int | None = None) -> list[RemoteFile]:
    today = local_today()
    found = {}
    for link in file_links(fetch_links(http, PAGE), (".zip",)):
        match = _ZIP.match(link.filename)
        if match:
            found.setdefault(int(match.group(1)), link)
    if not found:
        return []
    first = int(min_year) if min_year else max(found)
    return [
        RemoteFile(url=link.url, dt=today, source_page=PAGE, meta={"data_year": year})
        for year, link in sorted(found.items())
        if year >= first
    ]


run = source_runner(SOURCE_ID, discover)
