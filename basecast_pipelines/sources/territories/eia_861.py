"""EIA-861 annual electric power industry files (utility-level sales, customers, ownership), used to list
ERCOT co-ops and munis and their growth. Zips linked from the EIA-861 page, final releases plus the latest
early release, since ``min_year``. ``dt`` is the fetch date."""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import fetch_links, file_links
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "eia_861"
PAGE = "https://www.eia.gov/electricity/data/eia861/"

_ZIP = re.compile(r"^f861(\d{4})(er)?\.zip$", re.IGNORECASE)


def discover(http: HttpClient, *, min_year: int = 2013) -> list[RemoteFile]:
    today = local_today()
    files = []
    for link in file_links(fetch_links(http, PAGE), (".zip",)):
        match = _ZIP.match(link.filename)
        if not match or int(match.group(1)) < int(min_year):
            continue
        files.append(
            RemoteFile(
                url=link.url,
                dt=today,
                source_page=PAGE,
                meta={"data_year": int(match.group(1)), "early_release": bool(match.group(2))},
            )
        )
    return files


run = source_runner(SOURCE_ID, discover)
