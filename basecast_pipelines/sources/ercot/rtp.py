"""Regional Transmission Plan, public version (EMIL PG7-048-M): the yearly report packages from the product
listing (``dt`` = publish date) and the RTP archive/addendum files linked from the Transmission Planning page
(``dt`` = fetch date). Appendix B compares TSP-submitted load, ERCOT's forecast and the plan's load by
weather zone, and lists planned generators by INR and county."""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import fetch_links, file_links
from basecast_pipelines.common.ercot_mis import listing_files
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "ercot_rtp"
EMIL_ID = "pg7-048-m"
PLANNING_PAGE = "https://www.ercot.com/gridinfo/planning"

_RTP = re.compile(r"Regional Transmission Plan|\bRTP\b", re.IGNORECASE)


def discover(http: HttpClient) -> list[RemoteFile]:
    files = listing_files(http, EMIL_ID)
    today = local_today()
    for link in file_links(fetch_links(http, PLANNING_PAGE), (".zip", ".pdf", ".xlsx")):
        if _RTP.search(link.text):
            files.append(RemoteFile(url=link.url, dt=today, source_page=PLANNING_PAGE, meta={"link_text": link.text}))
    return files


run = source_runner(SOURCE_ID, discover)
