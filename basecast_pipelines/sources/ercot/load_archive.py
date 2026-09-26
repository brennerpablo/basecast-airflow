"""Hourly Load Data Archives (native load by weather zone), one file per year on the load_hist page.

Weather zones exist since April 2003, so earlier years (control areas) are skipped by default. ERCOT
overwrites the current-year file monthly at the same URL; each changed download lands in a new ``dt=``
(the fetch date), never over the previous one.
"""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import fetch_links, file_links
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "ercot_native_load"
PAGE = "https://www.ercot.com/gridinfo/load/load_hist"

_YEAR = re.compile(r"(?<!\d)((?:19|20)\d{2})(?!\d)")


def discover(http: HttpClient, *, min_year: int = 2003) -> list[RemoteFile]:
    today = local_today()
    files = []
    for link in file_links(fetch_links(http, PAGE), (".zip", ".xls", ".xlsx")):
        match = _YEAR.search(link.filename)
        if match is None or int(match.group(1)) < int(min_year):
            continue
        files.append(
            RemoteFile(
                url=link.url,
                dt=today,
                source_page=PAGE,
                meta={"data_year": int(match.group(1)), "link_text": link.text},
            )
        )
    return files


run = source_runner(SOURCE_ID, discover)
