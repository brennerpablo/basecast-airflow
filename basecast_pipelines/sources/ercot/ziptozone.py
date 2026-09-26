"""Load Profiling Guide Appendix D (Profile Decision Tree), whose ZipToZone sheet maps ZIP codes to weather
zones. Discovered on the current Load Profiling Guide page; ``dt`` is the fetch date."""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import fetch_links, file_links
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "ercot_ziptozone"
PAGE = "https://www.ercot.com/mktrules/guides/loadprofiling/current"

_APPENDIX_D = re.compile(r"Appendix_D", re.IGNORECASE)


def discover(http: HttpClient) -> list[RemoteFile]:
    today = local_today()
    return [
        RemoteFile(url=link.url, dt=today, source_page=PAGE, meta={"link_text": link.text})
        for link in file_links(fetch_links(http, PAGE), (".xlsx", ".xls"))
        if _APPENDIX_D.search(link.filename)
    ]


run = source_runner(SOURCE_ID, discover)
