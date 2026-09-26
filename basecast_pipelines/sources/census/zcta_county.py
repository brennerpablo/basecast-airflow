"""Census 2020 ZCTA-to-county relationship file (national, pipe-delimited, with land/water areas) and its
record layout PDF, discovered on the 2020 Relationship Files page. ``dt`` is the fetch date."""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import fetch_links, file_links
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "census_zcta_county"
PAGE = "https://www.census.gov/geographies/reference-files/time-series/geo/relationship-files.2020.html"

_FILES = re.compile(r"^(explanation_)?tab20_zcta520_county20_natl\.(txt|pdf)$", re.IGNORECASE)


def discover(http: HttpClient) -> list[RemoteFile]:
    today = local_today()
    return [
        RemoteFile(url=link.url, dt=today, source_page=PAGE, meta={"link_text": link.text})
        for link in file_links(fetch_links(http, PAGE), (".txt", ".pdf"))
        if _FILES.match(link.filename)
    ]


run = source_runner(SOURCE_ID, discover)
