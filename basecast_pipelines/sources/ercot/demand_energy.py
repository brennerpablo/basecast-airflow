"""ERCOT Demand and Energy report: one workbook per year with monthly peak demand (date, hour ending),
energy, and coincident / non-coincident peaks by load zone (incl. LZ_AEN, LZ_CPS, LZ_LCRA, LZ_RAYBN) and by
weather zone. Linked as "Demand and Energy" from Helpful Resources and its year pages (2009 onward). The
current-year file is overwritten monthly at the same URL. ``dt`` is the fetch date."""

from __future__ import annotations

import re

from basecast_pipelines.common.discovery import file_links
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.raw import RemoteFile, source_runner
from basecast_pipelines.config import local_today
from basecast_pipelines.sources.ercot._web import crawl_with_year_pages, year_of_page

SOURCE_ID = "ercot_demand_energy"
PAGE = "https://www.ercot.com/news/presentations"

_DEMAND_ENERGY = re.compile(r"Demand (and|&) Energy|\bd_?e\b", re.IGNORECASE)


def discover(http: HttpClient) -> list[RemoteFile]:
    today = local_today()
    files: list[RemoteFile] = []
    seen: set[str] = set()
    for page, links in crawl_with_year_pages(http, PAGE):
        for link in file_links(links, (".xlsx", ".xls", ".zip")):
            if link.url in seen or not (_DEMAND_ENERGY.search(link.text) or _DEMAND_ENERGY.search(link.filename)):
                continue
            seen.add(link.url)
            files.append(
                RemoteFile(url=link.url, dt=today, source_page=page,
                           meta={"link_text": link.text, "page_year": year_of_page(page)})
            )
    return files


run = source_runner(SOURCE_ID, discover)
