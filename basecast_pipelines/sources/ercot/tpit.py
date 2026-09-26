"""Transmission Project and Information Tracking (TPIT): TSP-submitted transmission projects with status,
counties, projected/actual in-service dates and SSWG bus numbers; the current workbook plus the archive zip
(2009 onward), both linked from the Transmission Planning page. ERCOT reuses old ``/files/docs/`` paths for
new content, so changes are detected by ETag/sha256. Contact columns hold personal data: drop at parse.
``dt`` is the fetch date."""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import fetch_links, file_links
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "ercot_tpit"
PAGE = "https://www.ercot.com/gridinfo/planning"

_TPIT = re.compile(r"Transmission Project and Information Tracking", re.IGNORECASE)


def discover(http: HttpClient) -> list[RemoteFile]:
    today = local_today()
    return [
        RemoteFile(url=link.url, dt=today, source_page=PAGE, meta={"link_text": link.text})
        for link in file_links(fetch_links(http, PAGE), (".xlsx", ".xls", ".zip"))
        if _TPIT.search(link.text)
    ]


run = source_runner(SOURCE_ID, discover)
