"""ERCOT corporate members by segment (co-ops, munis, IOUs, retail providers...), one workbook per year
from the Membership page and its year pages (2013 onward). Membership is voluntary, so this is a subset of
market participants. ``dt`` is the fetch date."""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import file_links
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today
from basecast_pipelines.sources.ercot._web import crawl_with_year_pages, year_of_page

SOURCE_ID = "ercot_members"
PAGE = "https://www.ercot.com/about/governance/members"

_MEMBERS = re.compile(r"\bMembers\b", re.IGNORECASE)


def discover(http: HttpClient) -> list[RemoteFile]:
    today = local_today()
    files: list[RemoteFile] = []
    seen: set[str] = set()
    for page, links in crawl_with_year_pages(http, PAGE):
        for link in file_links(links, (".xlsx", ".xls")):
            if link.url in seen or not _MEMBERS.search(link.text):
                continue
            seen.add(link.url)
            files.append(
                RemoteFile(url=link.url, dt=today, source_page=page,
                           meta={"link_text": link.text, "page_year": year_of_page(page)})
            )
    return files


run = source_runner(SOURCE_ID, discover)
