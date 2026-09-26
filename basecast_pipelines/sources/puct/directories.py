"""PUCT market directories (regenerated daily): electric co-ops, municipal utilities, IOUs, power generation
companies and their facilities (county and host service area), aggregators, as CSV from the PUCT
directories page. ``PrimaryIDNo`` of co-ops/munis is the CCN number. Contact rows name people: drop at parse.
``dt`` is the fetch date."""

from __future__ import annotations

from basecast_pipelines.common.discovery import fetch_links, file_links
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today

SOURCE_ID = "puct_directories"
PAGE = "https://www.puc.texas.gov/industry/electric/directories/Default.aspx"


def discover(http: HttpClient) -> list[RemoteFile]:
    today = local_today()
    return [
        RemoteFile(url=link.url, dt=today, source_page=PAGE, meta={"link_text": link.text})
        for link in file_links(fetch_links(http, PAGE), (".csv",))
        if "/bulkcopy/" in link.url
    ]


run = source_runner(SOURCE_ID, discover)
