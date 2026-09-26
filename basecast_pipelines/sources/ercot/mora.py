"""Monthly Outlook for Resource Adequacy (MORA): the xlsx + pdf pair of every edition (first edition covered
December 2023) from the Resource Adequacy page and its year pages. ``dt`` is the fetch date; the edition
month is kept in the manifest."""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import file_links, parse_month_year
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today
from basecast_pipelines.sources.ercot._web import RESOURCE_ADEQUACY_PAGE, crawl_with_year_pages

SOURCE_ID = "ercot_mora"

_MORA = re.compile(r"\bMORA\b|Monthly Outlook for Resource Adequacy", re.IGNORECASE)


def discover(http: HttpClient, *, min_year: int = 2023) -> list[RemoteFile]:
    today = local_today()
    files: list[RemoteFile] = []
    seen: set[str] = set()
    for page, links in crawl_with_year_pages(http, RESOURCE_ADEQUACY_PAGE, min_year=int(min_year)):
        for link in file_links(links, (".xlsx", ".xls", ".pdf")):
            if link.url in seen or not (_MORA.search(link.text) or _MORA.search(link.filename)):
                continue
            seen.add(link.url)
            edition = parse_month_year(link.text) or parse_month_year(link.filename)
            files.append(
                RemoteFile(
                    url=link.url,
                    dt=today,
                    source_page=page,
                    meta={"link_text": link.text, "edition_month": edition.isoformat() if edition else None},
                )
            )
    return files


run = source_runner(SOURCE_ID, discover)
